"""Wikipedia scraper for US state legislative elections (regular).

Handles state House/Assembly and Senate elections. Each state has
differently-named chambers (e.g. NJ "General Assembly", VA "House of
Delegates"), so a chamber-name mapping determines the race_type.

Results are per-district, similar to federal House elections.
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
    find_preceding_heading,
    is_general_election_table,
    parse_candidate_row,
)

logger = logging.getLogger(__name__)

# Chamber keywords that map to race_type
_LOWER_CHAMBERS: set[str] = {
    "general assembly",
    "house of delegates",
    "house of representatives",
    "assembly",
    "house",
}
_UPPER_CHAMBERS: set[str] = {
    "senate",
    "state senate",
}

# URL patterns for state legislative election pages
_STATE_LEG_PATTERNS: tuple[str, ...] = (
    r"_House_of_Delegates_election",
    r"_General_Assembly_election",
    r"_House_of_Representatives_election",
    r"_State_Senate_election",
    r"_Assembly_election",
    r"_State_Assembly_election",
)


def _classify_chamber(text: str) -> str:
    """Determine race_type from chamber name text.

    Args:
        text: Text containing a chamber name (URL, title, or heading).

    Returns:
        "State House" or "State Senate".
    """
    lower = text.lower()
    for keyword in _UPPER_CHAMBERS:
        if keyword in lower:
            return "State Senate"
    return "State House"


class StateLegislativeScraper(BaseScraper):
    """Scraper for US state legislative elections (regular)."""

    race_type = "State House"

    def build_index_url(self, year: int) -> str:
        """Build Wikipedia index URL for state legislative elections.

        Args:
            year: Election year.

        Returns:
            Index page URL.
        """
        return f"{BASE_URL}/wiki/{year}_United_States_state_legislative_elections"

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract state legislative election page URLs.

        Finds links matching known chamber name patterns like
        ``{year}_{State}_House_of_Delegates_election``.

        Args:
            soup: Parsed index page.
            year: Election year.

        Returns:
            De-duplicated list of (state_chamber, url) tuples.
        """
        combined_pattern = "|".join(
            rf"/wiki/{year}_\w+{pat}" for pat in _STATE_LEG_PATTERNS
        )
        pattern = re.compile(combined_pattern)
        seen: set[str] = set()
        results: list[tuple[str, str]] = []
        for anchor in soup.find_all("a", href=pattern):
            href = str(anchor["href"])
            if href in seen:
                continue
            if "special" in href.lower():
                continue
            seen.add(href)
            page_part = href.split(f"/wiki/{year}_", maxsplit=1)[-1]
            page_part = page_part.replace("_election", "")
            state_name = page_part.split("_")[0]
            for pat in _STATE_LEG_PATTERNS:
                cleaned = pat.replace(r"_election", "").lstrip("_")
                if (
                    cleaned.replace("\\", "").replace("_", " ").lower()
                    in page_part.replace("_", " ").lower()
                ):
                    state_name = (
                        page_part.rsplit(cleaned.replace("\\", ""), maxsplit=1)[0]
                        .rstrip("_")
                        .replace("_", " ")
                    )
                    break
            else:
                state_name = page_part.replace("_", " ")
            results.append((state_name, f"{BASE_URL}{href}"))
        return results

    def parse_state_page(
        self,
        state: str,
        soup: BeautifulSoup,
        year: int,
    ) -> list[tuple[Election, list[Candidate]]]:
        """Parse district-level results from a state legislative page.

        Determines the race_type (State House vs State Senate) from the
        page title, then extracts per-district election results.

        Args:
            state: State name or state-chamber identifier.
            soup: Parsed state legislative elections page.
            year: Election year.

        Returns:
            List of (Election, candidates) tuples, one per district.
        """
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""
        race_type = _classify_chamber(title)

        # Extract the actual state name from the title
        state_name = state
        if title:
            m = re.match(
                rf"{year}\s+(.+?)\s+"
                r"(?:House|General|State|Assembly|Senate)",
                title,
            )
            if m:
                state_name = m.group(1).strip()

        results: list[tuple[Election, list[Candidate]]] = []

        tables = soup.find_all(
            "table",
            class_=lambda c: c and "wikitable" in c and "plainrowheaders" in c,
        )
        for table in tables:
            if not is_general_election_table(table):
                continue

            # Look for h2 district heading first, fall back to h3
            h2 = find_preceding_heading(table, ("h2",))
            if h2 is None:
                h2 = find_preceding_heading(table, ("h3",))
            heading_text = h2.get_text(strip=True) if h2 else ""
            district = extract_district_number(heading_text)

            parsed: list[dict[str, str | float | bool | None]] = []
            for row in table.find_all("tr", class_="vcard"):
                cand = parse_candidate_row(row)
                if cand:
                    parsed.append(cand)

            candidates = candidates_from_parsed(parsed)
            if candidates:
                election = Election(
                    state=state_name,
                    race_type=race_type,
                    year=year,
                    district=district,
                )
                results.append((election, candidates))

        # Fallback: try any wikitable with vcard rows
        if not results:
            for table in soup.find_all(
                "table", class_=lambda c: c and "wikitable" in c
            ):
                h2 = find_preceding_heading(table, ("h2", "h3"))
                heading_text = h2.get_text(strip=True) if h2 else ""

                if not heading_text or "district" not in heading_text.lower():
                    continue

                district = extract_district_number(heading_text)
                parsed = []
                for row in table.find_all("tr", class_="vcard"):
                    cand = parse_candidate_row(row)
                    if cand:
                        parsed.append(cand)

                candidates = candidates_from_parsed(parsed)
                if candidates:
                    election = Election(
                        state=state_name,
                        race_type=race_type,
                        year=year,
                        district=district,
                    )
                    results.append((election, candidates))

        return results


register_scraper("state_leg", StateLegislativeScraper)
