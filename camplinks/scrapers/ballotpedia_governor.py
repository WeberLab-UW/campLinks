"""Ballotpedia scraper for US gubernatorial elections.

Scrapes gubernatorial election results directly from Ballotpedia for all
states with governor races. Handles standard votebox, RCV, and runoff formats.

URL pattern: https://ballotpedia.org/{State}_gubernatorial_election,_{year}
"""

from __future__ import annotations

import logging
import re
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

_LINK_RE = re.compile(r"_gubernatorial_election,_\d{4}$")

# fmt: off
_FALLBACK_STATES: tuple[str, ...] = (
    "Alabama", "Alaska", "Arizona", "Arkansas", "California",
    "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
    "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
    "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
    "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
)
# fmt: on


class BallotpediaGovernorScraper(BaseScraper):
    """Scraper for US gubernatorial elections from Ballotpedia."""

    race_type = "Governor"

    def build_index_url(self, year: int) -> str:
        """Build Ballotpedia index URL for gubernatorial elections.

        Args:
            year: Election year.

        Returns:
            Index page URL.
        """
        return f"{BALLOTPEDIA_BASE}/Gubernatorial_elections,_{year}"

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract state page URLs from the gubernatorial index page.

        Parses the sortable table on the Ballotpedia gubernatorial
        elections index and constructs per-state election URLs.
        Excludes lieutenant governor bundled pages.

        Args:
            soup: Parsed index page.
            year: Election year.

        Returns:
            De-duplicated list of (state, url) tuples.
        """
        results: list[tuple[str, str]] = []
        seen: set[str] = set()

        # Find links matching the gubernatorial election URL pattern
        for anchor in soup.find_all("a", href=_LINK_RE):
            href = str(anchor.get("href", ""))

            # Skip lieutenant governor pages
            if "lieutenant" in href.lower():
                continue

            # Extract state name from URL
            # Pattern: /State_gubernatorial_election,_2026
            path = href.lstrip("/")
            state_part = path.split("_gubernatorial_election")[0]
            state = state_part.replace("_", " ")

            if state in seen:
                continue
            seen.add(state)

            url = self._build_election_url(state, year)
            results.append((state, url))

        if not results:
            logger.warning("No states found in index table; using fallback list.")
            return self._build_urls_from_fallback(year)

        return results

    def parse_state_page(
        self,
        state: str,
        soup: BeautifulSoup,
        year: int,
    ) -> list[tuple[Election, list[Candidate]]]:
        """Parse Ballotpedia gubernatorial election results for one state.

        Handles standard votebox, RCV votebox, and multiple election
        stages (general, primary, runoff) on a single page.

        Args:
            state: State name (e.g. ``"Texas"``).
            soup: Parsed Ballotpedia election page.
            year: Election year.

        Returns:
            List of (Election, candidates) tuples, one per stage.
        """
        results: list[tuple[Election, list[Candidate]]] = []

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
                    race_type="Governor",
                    year=year,
                    district=None,
                    election_stage=stage,
                )
                results.append((election, candidates))

        return results

    def scrape_all(self, year: int, conn: sqlite3.Connection) -> int:
        """Scrape gubernatorial elections from Ballotpedia.

        Overrides ``BaseScraper.scrape_all()`` for Ballotpedia-specific
        delay, 404 handling (not every state has a governor race every
        cycle), and index page parsing.

        Args:
            year: Election year to scrape.
            conn: Open database connection.

        Returns:
            Total number of elections inserted/updated.
        """
        index_url = self.build_index_url(year)
        logger.info("Fetching gubernatorial elections index from Ballotpedia...")
        try:
            index_soup = fetch_soup(index_url, delay_s=BALLOTPEDIA_DELAY_S)
            state_urls = self.collect_state_urls(index_soup, year)
        except requests.RequestException as exc:
            logger.error("Failed to fetch index page: %s", exc)
            logger.info("Using fallback state list.")
            state_urls = self._build_urls_from_fallback(year)

        logger.info(
            "Checking %d states for %d gubernatorial elections...",
            len(state_urls),
            year,
        )

        total_elections = 0
        for state, url in tqdm(
            state_urls,
            desc=f"Scraping Governor (Ballotpedia) {year}",
            unit="state",
        ):
            try:
                soup = fetch_soup(url, delay_s=BALLOTPEDIA_DELAY_S)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    continue
                logger.error("Failed to fetch %s: %s", state, exc)
                continue
            except requests.RequestException as exc:
                logger.error("Failed to fetch %s: %s", state, exc)
                continue

            try:
                page_results = self.parse_state_page(state, soup, year)
                for election, candidates in page_results:
                    election.wikipedia_url = url
                    eid = upsert_election(conn, election)
                    for cand in candidates:
                        upsert_candidate(conn, cand, eid)
                    total_elections += 1
                conn.commit()
            except (AttributeError, KeyError, ValueError, TypeError) as exc:
                logger.error("Error parsing %s: %s", state, exc)

        logger.info(
            "Scraped %d gubernatorial elections for %d from Ballotpedia.",
            total_elections,
            year,
        )
        return total_elections

    @staticmethod
    def _build_election_url(state: str, year: int) -> str:
        """Construct a Ballotpedia gubernatorial election page URL.

        Args:
            state: State name (e.g. ``"New Hampshire"``).
            year: Election year.

        Returns:
            Full Ballotpedia URL.
        """
        state_slug = state.replace(" ", "_")
        return f"{BALLOTPEDIA_BASE}/{state_slug}_gubernatorial_election,_{year}"

    def _build_urls_from_fallback(self, year: int) -> list[tuple[str, str]]:
        """Build (state, url) pairs from the hardcoded fallback list.

        Args:
            year: Election year for URL construction.

        Returns:
            List of (state, url) tuples.
        """
        return [
            (state, self._build_election_url(state, year)) for state in _FALLBACK_STATES
        ]


register_scraper("bp_governor", BallotpediaGovernorScraper)
