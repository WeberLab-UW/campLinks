"""Wikipedia scraper for US gubernatorial elections.

Gubernatorial pages use the same ``wikitable plainrowheaders`` + ``vcard``
row format as Senate races. The key difference is the URL pattern and
race type. Races are statewide (no district).
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from camplinks.http import BASE_URL
from camplinks.models import Candidate, Election
from camplinks.scrapers import register_scraper
from camplinks.scrapers.base import BaseScraper
from camplinks.wiki_parsing import (
    candidates_from_parsed,
    is_general_election_table,
    parse_candidate_row,
)


class GovernorScraper(BaseScraper):
    """Scraper for US gubernatorial elections."""

    race_type = "Governor"

    def build_index_url(self, year: int) -> str:
        """Build Wikipedia index URL for gubernatorial elections.

        Args:
            year: Election year.

        Returns:
            Index page URL.
        """
        return f"{BASE_URL}/wiki/{year}_United_States_gubernatorial_elections"

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract state page URLs from the gubernatorial elections index.

        Args:
            soup: Parsed index page.
            year: Election year.

        Returns:
            De-duplicated list of (state, url) tuples.
        """
        pattern = re.compile(rf"/wiki/{year}_\w+_gubernatorial_election$")
        seen: set[str] = set()
        results: list[tuple[str, str]] = []
        for anchor in soup.find_all("a", href=pattern):
            href = str(anchor["href"])
            if href in seen:
                continue
            seen.add(href)
            state_name = (
                href.split(f"/wiki/{year}_", maxsplit=1)[-1]
                .replace("_gubernatorial_election", "")
                .replace("_", " ")
            )
            results.append((state_name, f"{BASE_URL}{href}"))
        return results

    def parse_state_page(
        self,
        state: str,
        soup: BeautifulSoup,
        year: int,
    ) -> list[tuple[Election, list[Candidate]]]:
        """Parse general election results from a gubernatorial state page.

        Gubernatorial races are statewide (district=None).

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
                    race_type="Governor",
                    year=year,
                    district=None,
                )
                return [(election, candidates)]

        return []


register_scraper("governor", GovernorScraper)
