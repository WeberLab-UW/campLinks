"""Wikipedia scraper for US House special elections.

Special election pages are linked from the main House elections index but
use a per-district URL pattern instead of per-state pages. Each page
contains a single district race using the standard ``wikitable
plainrowheaders`` + ``vcard`` row format.
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
    classify_election_table,
    extract_district_number,
    extract_primary_party,
    parse_candidate_row,
)

_SPECIAL_LINK_RE = re.compile(
    r"/wiki/\d{4}_[A-Z][\w%']+congressional_district_special_election"
)

_STATE_FROM_URL_RE = re.compile(r"/wiki/\d{4}_([A-Z][\w]+?)(?:%27s|'s)_")

_DISTRICT_FROM_URL_RE = re.compile(r"(\d+)(?:st|nd|rd|th)_congressional_district")


class SpecialHouseScraper(BaseScraper):
    """Scraper for US House of Representatives special elections."""

    race_type = "US House"

    def build_index_url(self, year: int) -> str:
        """Build Wikipedia index URL for House elections.

        Args:
            year: Election year.

        Returns:
            Index page URL (same page as regular House elections).
        """
        return (
            f"{BASE_URL}/wiki/{year}_United_States_House_of_Representatives_elections"
        )

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract special election page URLs from the House elections index.

        Args:
            soup: Parsed index page.
            year: Election year.

        Returns:
            De-duplicated list of (state, url) tuples where each entry
            represents a single special election district.
        """
        pattern = re.compile(
            rf"/wiki/{year}_[\w%']+"
            r"congressional_district_special_election"
        )
        seen: set[str] = set()
        results: list[tuple[str, str]] = []
        for anchor in soup.find_all("a", href=pattern):
            href = str(anchor["href"])
            if href in seen:
                continue
            seen.add(href)
            state_match = _STATE_FROM_URL_RE.search(href)
            state_name = (
                state_match.group(1).replace("_", " ") if state_match else "Unknown"
            )
            results.append((state_name, f"{BASE_URL}{href}"))
        return results

    def parse_state_page(
        self,
        state: str,
        soup: BeautifulSoup,
        year: int,
    ) -> list[tuple[Election, list[Candidate]]]:
        """Parse a single special election page.

        Each page covers one congressional district. The district number
        is extracted from the page title or URL.

        Args:
            state: State name extracted from URL.
            soup: Parsed special election page.
            year: Election year.

        Returns:
            List containing a single (Election, candidates) tuple,
            or empty list if no results table found.
        """
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""
        district = extract_district_number(title) if title else "At-Large"

        results: list[tuple[Election, list[Candidate]]] = []

        tables = soup.find_all(
            "table",
            class_=lambda c: c and "wikitable" in c and "plainrowheaders" in c,
        )

        for table in tables:
            stage = classify_election_table(table)
            if stage is None:
                continue

            primary_party = extract_primary_party(table) if stage != "general" else ""

            parsed: list[dict[str, str | float | bool | None]] = []
            for row in table.find_all("tr", class_="vcard"):
                cand = parse_candidate_row(row)
                if cand:
                    if primary_party and not cand.get("party"):
                        cand["party"] = primary_party
                    parsed.append(cand)

            candidates = candidates_from_parsed(parsed)
            if candidates:
                election = Election(
                    state=state,
                    race_type="US House",
                    year=year,
                    district=district,
                    election_stage=stage,
                )
                results.append((election, candidates))

        if results:
            return results

        # Fallback: try any wikitable with vcard rows
        for table in soup.find_all("table", class_=lambda c: c and "wikitable" in c):
            parsed = []
            for row in table.find_all("tr", class_="vcard"):
                cand = parse_candidate_row(row)
                if cand:
                    parsed.append(cand)
            candidates = candidates_from_parsed(parsed)
            if candidates:
                election = Election(
                    state=state,
                    race_type="US House",
                    year=year,
                    district=district,
                )
                return [(election, candidates)]

        return []


register_scraper("special_house", SpecialHouseScraper)
