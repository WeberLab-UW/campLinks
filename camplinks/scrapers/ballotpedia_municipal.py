"""Ballotpedia scraper for US mayoral elections in the 100 largest cities.

Unlike the Wikipedia-based MunicipalScraper, this scraper directly targets
Ballotpedia election pages for the top 100 US cities by population.
Handles standard votebox, RCV (ranked-choice), and runoff table formats.

URL pattern: https://ballotpedia.org/Mayoral_election_in_{City},_{State}_({year})
"""

from __future__ import annotations

import logging
import sqlite3

import requests
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

from camplinks.db import upsert_candidate, upsert_election
from camplinks.http import fetch_soup
from camplinks.models import Candidate, Election
from camplinks.scrapers import register_scraper
from camplinks.scrapers.ballotpedia_parsing import (
    BALLOTPEDIA_BASE,
    BALLOTPEDIA_DELAY_S,
    detect_election_stage,
    parse_rcv_votebox,
    parse_votebox,
)
from camplinks.scrapers.base import BaseScraper
from camplinks.wiki_parsing import candidates_from_parsed

logger = logging.getLogger(__name__)

TOP_100_URL = f"{BALLOTPEDIA_BASE}/Largest_cities_in_the_United_States_by_population"

# fmt: off
_FALLBACK_CITIES: tuple[tuple[str, str], ...] = (
    ("New York", "New York"), ("Los Angeles", "California"),
    ("Chicago", "Illinois"), ("Houston", "Texas"),
    ("Phoenix", "Arizona"), ("Philadelphia", "Pennsylvania"),
    ("San Antonio", "Texas"), ("San Diego", "California"),
    ("Dallas", "Texas"), ("San Jose", "California"),
    ("Austin", "Texas"), ("Jacksonville", "Florida"),
    ("Fort Worth", "Texas"), ("Columbus", "Ohio"),
    ("Charlotte", "North Carolina"), ("Indianapolis", "Indiana"),
    ("San Francisco", "California"), ("Seattle", "Washington"),
    ("Denver", "Colorado"), ("Washington", "District of Columbia"),
    ("Nashville", "Tennessee"), ("Oklahoma City", "Oklahoma"),
    ("El Paso", "Texas"), ("Boston", "Massachusetts"),
    ("Portland", "Oregon"), ("Las Vegas", "Nevada"),
    ("Memphis", "Tennessee"), ("Louisville", "Kentucky"),
    ("Baltimore", "Maryland"), ("Milwaukee", "Wisconsin"),
    ("Albuquerque", "New Mexico"), ("Tucson", "Arizona"),
    ("Fresno", "California"), ("Mesa", "Arizona"),
    ("Sacramento", "California"), ("Atlanta", "Georgia"),
    ("Kansas City", "Missouri"), ("Omaha", "Nebraska"),
    ("Colorado Springs", "Colorado"), ("Raleigh", "North Carolina"),
    ("Long Beach", "California"), ("Virginia Beach", "Virginia"),
    ("Miami", "Florida"), ("Oakland", "California"),
    ("Minneapolis", "Minnesota"), ("Tampa", "Florida"),
    ("Tulsa", "Oklahoma"), ("Arlington", "Texas"),
    ("New Orleans", "Louisiana"), ("Wichita", "Kansas"),
    ("Cleveland", "Ohio"), ("Bakersfield", "California"),
    ("Aurora", "Colorado"), ("Anaheim", "California"),
    ("Honolulu", "Hawaii"), ("Santa Ana", "California"),
    ("Riverside", "California"), ("Corpus Christi", "Texas"),
    ("Lexington", "Kentucky"), ("Henderson", "Nevada"),
    ("Stockton", "California"), ("St. Paul", "Minnesota"),
    ("Cincinnati", "Ohio"), ("St. Louis", "Missouri"),
    ("Pittsburgh", "Pennsylvania"), ("Greensboro", "North Carolina"),
    ("Lincoln", "Nebraska"), ("Orlando", "Florida"),
    ("Irvine", "California"), ("Newark", "New Jersey"),
    ("Durham", "North Carolina"), ("Chula Vista", "California"),
    ("Toledo", "Ohio"), ("Fort Wayne", "Indiana"),
    ("St. Petersburg", "Florida"), ("Laredo", "Texas"),
    ("Jersey City", "New Jersey"), ("Chandler", "Arizona"),
    ("Madison", "Wisconsin"), ("Lubbock", "Texas"),
    ("Scottsdale", "Arizona"), ("Reno", "Nevada"),
    ("Buffalo", "New York"), ("Gilbert", "Arizona"),
    ("Glendale", "Arizona"), ("North Las Vegas", "Nevada"),
    ("Winston-Salem", "North Carolina"), ("Chesapeake", "Virginia"),
    ("Norfolk", "Virginia"), ("Fremont", "California"),
    ("Garland", "Texas"), ("Irving", "Texas"),
    ("Hialeah", "Florida"), ("Richmond", "Virginia"),
    ("Boise", "Idaho"), ("Spokane", "Washington"),
    ("Baton Rouge", "Louisiana"), ("Tacoma", "Washington"),
    ("San Bernardino", "California"), ("Modesto", "California"),
)
# fmt: on


class BallotpediaMunicipalScraper(BaseScraper):
    """Scraper for US mayoral elections from Ballotpedia (top 100 cities)."""

    race_type = "Mayor"

    def build_index_url(self, year: int) -> str:
        """Return the Ballotpedia top-100 cities page URL.

        Args:
            year: Election year (unused; the city list is static).

        Returns:
            URL of the largest cities page.
        """
        return TOP_100_URL

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract (city_state, election_url) pairs from the top-100 table.

        Parses the Ballotpedia largest-cities table and constructs
        per-city election URLs for the given year.

        Args:
            soup: Parsed top-100 cities page.
            year: Election year for URL construction.

        Returns:
            List of (``"City, State"``, url) tuples.
        """
        results: list[tuple[str, str]] = []

        # Find the main sortable table
        table = soup.find("table", class_=lambda c: c and "sortable" in c)
        if not table:
            # Fallback: find the first large table
            tables = soup.find_all("table")
            for t in tables:
                if len(t.find_all("tr")) > 50:
                    table = t
                    break

        if not table:
            logger.warning("Could not find top-100 cities table; using fallback list.")
            return self._build_urls_from_fallback(year)

        if not isinstance(table, Tag):
            return self._build_urls_from_fallback(year)

        rows = table.find_all("tr")
        for row in rows[1:]:  # Skip header row
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            # The city column (usually column 1) contains a link like
            # "New York, New York" or "Los Angeles, California"
            city_cell = cells[1] if len(cells) > 1 else cells[0]
            link = city_cell.find("a")
            if link:
                city_state = link.get_text(strip=True)
            else:
                city_state = city_cell.get_text(strip=True)

            if "," not in city_state:
                continue

            city, state = city_state.rsplit(",", maxsplit=1)
            city = city.strip()
            state = state.strip()

            url = self._build_election_url(city, state, year)
            results.append((f"{city}, {state}", url))

            if len(results) >= 100:
                break

        if not results:
            logger.warning("Parsed 0 cities from table; using fallback list.")
            return self._build_urls_from_fallback(year)

        return results

    def parse_state_page(
        self,
        state: str,
        soup: BeautifulSoup,
        year: int,
    ) -> list[tuple[Election, list[Candidate]]]:
        """Parse Ballotpedia mayoral election results for one city.

        Handles standard votebox, RCV votebox, and multiple election
        stages (general, runoff) on a single page.

        Args:
            state: City display name (e.g. ``"Houston, Texas"``).
            soup: Parsed Ballotpedia election page.
            year: Election year.

        Returns:
            List of (Election, candidates) tuples, one per stage.
        """
        results: list[tuple[Election, list[Candidate]]] = []

        # Find all votebox containers (standard + RCV)
        voteboxes = soup.find_all(
            "div",
            class_=lambda c: c and ("votebox" in c or "rcvvotebox" in c),
        )

        for vbox in voteboxes:
            if not isinstance(vbox, Tag):
                continue

            classes = vbox.get("class") or []
            stage = detect_election_stage(vbox)

            if "rcvvotebox" in classes:
                parsed = parse_rcv_votebox(vbox)
            else:
                parsed = parse_votebox(vbox)

            candidates = candidates_from_parsed(parsed)
            if candidates:
                election = Election(
                    state=state,
                    race_type="Mayor",
                    year=year,
                    district=None,
                    election_stage=stage,
                )
                results.append((election, candidates))

        return results

    def scrape_all(self, year: int, conn: sqlite3.Connection) -> int:
        """Scrape mayoral elections from Ballotpedia for top 100 cities.

        Overrides ``BaseScraper.scrape_all()`` because the index page
        is not year-specific, 404 responses are expected (not every
        city has an election every year), and Ballotpedia requires
        a longer crawl delay.

        Args:
            year: Election year to scrape.
            conn: Open database connection.

        Returns:
            Total number of elections inserted/updated.
        """
        logger.info("Fetching top-100 cities list from Ballotpedia...")
        try:
            index_soup = fetch_soup(TOP_100_URL, delay_s=BALLOTPEDIA_DELAY_S)
            city_urls = self.collect_state_urls(index_soup, year)
        except requests.RequestException as exc:
            logger.error("Failed to fetch top-100 page: %s", exc)
            logger.info("Using fallback city list.")
            city_urls = self._build_urls_from_fallback(year)

        logger.info(
            "Checking %d cities for %d mayoral elections...",
            len(city_urls),
            year,
        )

        total_elections = 0
        for city_state, url in tqdm(
            city_urls,
            desc=f"Scraping Mayor (Ballotpedia) {year}",
            unit="city",
        ):
            try:
                soup = fetch_soup(url, delay_s=BALLOTPEDIA_DELAY_S)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    continue
                logger.error("Failed to fetch %s: %s", city_state, exc)
                continue
            except requests.RequestException as exc:
                logger.error("Failed to fetch %s: %s", city_state, exc)
                continue

            try:
                page_results = self.parse_state_page(city_state, soup, year)
                for election, candidates in page_results:
                    election.wikipedia_url = url
                    eid = upsert_election(conn, election)
                    for cand in candidates:
                        upsert_candidate(conn, cand, eid)
                    total_elections += 1
                conn.commit()
            except (AttributeError, KeyError, ValueError, TypeError) as exc:
                logger.error("Error parsing %s: %s", city_state, exc)

        logger.info(
            "Scraped %d mayoral elections for %d from Ballotpedia.",
            total_elections,
            year,
        )
        return total_elections

    @staticmethod
    def _build_election_url(city: str, state: str, year: int) -> str:
        """Construct a Ballotpedia mayoral election page URL.

        Args:
            city: City name (e.g. ``"San Francisco"``).
            state: State name (e.g. ``"California"``).
            year: Election year.

        Returns:
            Full Ballotpedia URL.
        """
        city_slug = city.replace(" ", "_")
        state_slug = state.replace(" ", "_")
        return (
            f"{BALLOTPEDIA_BASE}/Mayoral_election_in_{city_slug},_{state_slug}_({year})"
        )

    def _build_urls_from_fallback(self, year: int) -> list[tuple[str, str]]:
        """Build (city_state, url) pairs from the hardcoded fallback list.

        Args:
            year: Election year for URL construction.

        Returns:
            List of (``"City, State"``, url) tuples.
        """
        return [
            (
                f"{city}, {state}",
                self._build_election_url(city, state, year),
            )
            for city, state in _FALLBACK_CITIES
        ]


register_scraper("bp_municipal", BallotpediaMunicipalScraper)
