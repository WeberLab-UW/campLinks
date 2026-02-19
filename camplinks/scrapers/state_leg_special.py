"""Wikipedia scraper for US state legislative special elections.

Collects links to individual special election pages from the main state
legislative elections index page. Many of the 90+ annual specials may
lack dedicated Wikipedia pages; missing pages are logged and skipped.
"""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from camplinks.http import BASE_URL
from camplinks.models import Candidate, Election
from camplinks.scrapers import register_scraper
from camplinks.scrapers.base import BaseScraper
from camplinks.wiki_parsing import (
    candidates_from_parsed,
    extract_district_number,
    is_general_election_table,
    parse_candidate_row,
)

logger = logging.getLogger(__name__)

# Keywords indicating upper vs lower chamber
_UPPER_KEYWORDS = {"senate", "state_senate"}
_LOWER_KEYWORDS = {
    "house",
    "assembly",
    "general_assembly",
    "house_of_delegates",
    "house_of_representatives",
}


def _classify_from_url(href: str) -> str:
    """Determine race_type from a special election URL.

    Args:
        href: URL path fragment.

    Returns:
        "State Senate" or "State House".
    """
    lower = href.lower()
    for kw in _UPPER_KEYWORDS:
        if kw in lower:
            return "State Senate"
    return "State House"


def _extract_state_from_url(href: str, year: int) -> str:
    """Extract state name from a special election URL.

    Args:
        href: URL path like /wiki/2025_Georgia_House_..._special_election.
        year: Election year for stripping prefix.

    Returns:
        Human-readable state name.
    """
    page_part = href.split(f"/wiki/{year}_", maxsplit=1)[-1]
    # Find the first chamber keyword boundary
    for kw in (
        "House_of_Representatives",
        "House_of_Delegates",
        "General_Assembly",
        "State_Senate",
        "Senate",
        "Assembly",
        "House",
    ):
        if f"_{kw}" in page_part:
            state = page_part.split(f"_{kw}", maxsplit=1)[0]
            return state.replace("_", " ")
    return page_part.split("_")[0].replace("_", " ")


class StateLegSpecialScraper(BaseScraper):
    """Scraper for US state legislative special elections."""

    race_type = "State House"

    def build_index_url(self, year: int) -> str:
        """Build Wikipedia index URL for state legislative elections.

        Args:
            year: Election year.

        Returns:
            Index page URL (same as regular state leg page).
        """
        return f"{BASE_URL}/wiki/{year}_United_States_state_legislative_elections"

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract state legislative special election page URLs.

        Looks for links containing "special_election" in the context of
        state legislative bodies.

        Args:
            soup: Parsed index page.
            year: Election year.

        Returns:
            De-duplicated list of (state, url) tuples.
        """
        pattern = re.compile(rf"/wiki/{year}_\w+.*special_election")
        seen: set[str] = set()
        results: list[tuple[str, str]] = []
        for anchor in soup.find_all("a", href=pattern):
            href = str(anchor["href"])
            if href in seen:
                continue
            lower_href = href.lower()
            # Skip non-legislative specials (House of Reps federal, etc.)
            if "congressional_district" in lower_href:
                continue
            if "gubernatorial" in lower_href:
                continue
            if "attorney_general" in lower_href:
                continue
            if "supreme_court" in lower_href:
                continue
            if "mayoral" in lower_href:
                continue
            # Must contain a state legislative chamber keyword
            has_leg_keyword = any(
                kw in lower_href
                for kw in (
                    "house",
                    "senate",
                    "assembly",
                    "delegates",
                    "representatives",
                )
            )
            if not has_leg_keyword:
                continue
            seen.add(href)
            state_name = _extract_state_from_url(href, year)
            results.append((state_name, f"{BASE_URL}{href}"))

        logger.info("Found %d state legislative special election pages.", len(results))
        return results

    def parse_state_page(
        self,
        state: str,
        soup: BeautifulSoup,
        year: int,
    ) -> list[tuple[Election, list[Candidate]]]:
        """Parse a state legislative special election page.

        Args:
            state: State name from URL.
            soup: Parsed special election page.
            year: Election year.

        Returns:
            List of (Election, candidates) tuples.
        """
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""
        race_type = _classify_from_url(title.replace(" ", "_"))

        district = extract_district_number(title) if title else "At-Large"

        results: list[tuple[Election, list[Candidate]]] = []

        # Standard tables
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
                    race_type=race_type,
                    year=year,
                    district=district,
                )
                results.append((election, candidates))

        if results:
            return results

        # Fallback: any wikitable with vcard rows
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
                    race_type=race_type,
                    year=year,
                    district=district,
                )
                results.append((election, candidates))

        return results


register_scraper("state_leg_special", StateLegSpecialScraper)
