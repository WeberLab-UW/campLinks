"""Abstract base class for Wikipedia election scrapers."""

from __future__ import annotations

import logging
import sqlite3
from abc import ABC, abstractmethod

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from camplinks.db import upsert_candidate, upsert_election
from camplinks.http import fetch_soup
from camplinks.models import Candidate, Election

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Base class for all race-specific Wikipedia scrapers.

    Subclasses implement URL construction and page parsing logic.
    The shared ``scrape_all`` method handles orchestration, progress
    tracking, and database writes.
    """

    race_type: str  # e.g. "US House", "US Senate"

    @abstractmethod
    def build_index_url(self, year: int) -> str:
        """Return the Wikipedia index page URL for this race type and year.

        Args:
            year: Election year.

        Returns:
            Fully-qualified Wikipedia URL.
        """

    @abstractmethod
    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract (state_name, page_url) pairs from the index page.

        Args:
            soup: Parsed index page.
            year: Election year.

        Returns:
            De-duplicated list of (state, url) tuples.
        """

    @abstractmethod
    def parse_state_page(
        self,
        state: str,
        soup: BeautifulSoup,
        year: int,
    ) -> list[tuple[Election, list[Candidate]]]:
        """Parse a single state page into elections and their candidates.

        Args:
            state: Human-readable state name.
            soup: Parsed state elections page.
            year: Election year.

        Returns:
            List of (Election, [Candidate, ...]) tuples.
        """

    def scrape_all(self, year: int, conn: sqlite3.Connection) -> int:
        """Orchestrate a full scrape: index -> states -> DB.

        Args:
            year: Election year to scrape.
            conn: Open database connection.

        Returns:
            Total number of elections inserted/updated.
        """
        logger.info("Fetching %s %d index page...", self.race_type, year)
        index_url = self.build_index_url(year)
        index_soup = fetch_soup(index_url)
        state_urls = self.collect_state_urls(index_soup, year)
        logger.info("Found %d state pages to scrape.", len(state_urls))

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
