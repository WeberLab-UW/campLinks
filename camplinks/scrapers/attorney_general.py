"""Wikipedia scraper for US state attorney general elections.

Uses the same ``wikitable plainrowheaders`` + ``vcard`` row format as
gubernatorial and Senate races. Statewide races (no district).
"""

from __future__ import annotations

import logging
import re
import sqlite3

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from camplinks.db import upsert_candidate, upsert_election
from camplinks.http import BASE_URL, fetch_soup
from camplinks.models import Candidate, Election
from camplinks.scrapers import register_scraper
from camplinks.scrapers.base import BaseScraper
from camplinks.wiki_parsing import (
    candidates_from_parsed,
    is_general_election_table,
    parse_candidate_row,
)

logger = logging.getLogger(__name__)


class AttorneyGeneralScraper(BaseScraper):
    """Scraper for US state attorney general elections."""

    race_type = "Attorney General"

    def build_index_url(self, year: int) -> str:
        """Build Wikipedia index URL for attorney general elections.

        Args:
            year: Election year.

        Returns:
            Index page URL.
        """
        return f"{BASE_URL}/wiki/{year}_United_States_attorney_general_elections"

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract state page URLs from the attorney general elections index.

        Falls back to searching for links matching the state-level
        pattern if the index page is sparse.

        Args:
            soup: Parsed index page.
            year: Election year.

        Returns:
            De-duplicated list of (state, url) tuples.
        """
        pattern = re.compile(rf"/wiki/{year}_\w+_[Aa]ttorney_[Gg]eneral_election$")
        seen: set[str] = set()
        results: list[tuple[str, str]] = []
        for anchor in soup.find_all("a", href=pattern):
            href = str(anchor["href"])
            if href in seen:
                continue
            seen.add(href)
            page_part = href.split(f"/wiki/{year}_", maxsplit=1)[-1]
            state_name = re.sub(
                r"_[Aa]ttorney_[Gg]eneral_election$", "", page_part
            ).replace("_", " ")
            results.append((state_name, f"{BASE_URL}{href}"))

        if not results:
            logger.warning(
                "No AG election links found on index page for %d. "
                "The index page may not exist for this year.",
                year,
            )

        return results

    def scrape_all(self, year: int, conn: sqlite3.Connection) -> int:
        """Orchestrate AG scrape with fallback for missing index page.

        The AG index page does not exist for all years. When the index
        returns 404, falls back to searching the gubernatorial elections
        page for AG election links.

        Args:
            year: Election year to scrape.
            conn: Open database connection.

        Returns:
            Total number of elections inserted/updated.
        """
        logger.info("Fetching %s %d index page...", self.race_type, year)
        index_url = self.build_index_url(year)

        try:
            index_soup = fetch_soup(index_url)
        except requests.RequestException:
            logger.warning(
                "AG index page not found for %d, trying gubernatorial page.",
                year,
            )
            fallback_url = (
                f"{BASE_URL}/wiki/{year}_United_States_gubernatorial_elections"
            )
            try:
                index_soup = fetch_soup(fallback_url)
            except requests.RequestException:
                logger.error("Fallback page also not found for %d.", year)
                return 0

        state_urls = self.collect_state_urls(index_soup, year)
        logger.info("Found %d AG state pages to scrape.", len(state_urls))

        total_elections = 0
        for state, url in tqdm(
            state_urls,
            desc=f"Scraping {self.race_type} {year}",
            unit="state",
        ):
            try:
                soup = fetch_soup(url)
                results = self.parse_state_page(state, soup, year)
                for election, candidates in results:
                    election.wikipedia_url = url
                    eid = upsert_election(conn, election)
                    for cand in candidates:
                        upsert_candidate(conn, cand, eid)
                    total_elections += 1
                conn.commit()
            except requests.RequestException as exc:
                logger.error("Failed to fetch %s: %s", state, exc)
            except (AttributeError, KeyError, ValueError, TypeError) as exc:
                logger.error("Error parsing %s: %s", state, exc)

        logger.info(
            "Scraped %d %s elections for %d.",
            total_elections,
            self.race_type,
            year,
        )
        return total_elections

    def parse_state_page(
        self,
        state: str,
        soup: BeautifulSoup,
        year: int,
    ) -> list[tuple[Election, list[Candidate]]]:
        """Parse general election results from an attorney general state page.

        AG races are statewide (district=None).

        Args:
            state: Human-readable state name.
            soup: Parsed state elections page.
            year: Election year.

        Returns:
            List containing a single (Election, candidates) tuple,
            or empty list if no general election table found.
        """
        tables = soup.find_all(
            "table",
            class_=lambda c: c and "wikitable" in c and "plainrowheaders" in c,
        )

        for table in tables:
            if not is_general_election_table(table):
                continue

            parsed: list[dict[str, str | float | bool | None]] = []
            for row in table.find_all("tr", class_="vcard"):
                cand = parse_candidate_row(row)
                if cand:
                    parsed.append(cand)

            candidates = candidates_from_parsed(parsed)
            if candidates:
                election = Election(
                    state=state,
                    race_type="Attorney General",
                    year=year,
                    district=None,
                )
                return [(election, candidates)]

        return []


register_scraper("attorney_general", AttorneyGeneralScraper)
