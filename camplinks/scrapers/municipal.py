"""Wikipedia scraper for US municipal/mayoral elections.

Uses the Wikipedia category page as the index to discover all city
election pages. Handles two distinct HTML patterns:

- **Pattern B** (preferred): ``wikitable plainrowheaders`` + ``vcard``
  rows with party in ``class="org"`` cell.
- **Pattern A** (fallback): basic ``wikitable`` with ``<th scope="row">``
  for candidate names, no vcard or party in the results table.
"""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup, Tag

from camplinks.http import BASE_URL
from camplinks.models import Candidate, Election
from camplinks.scrapers import register_scraper
from camplinks.scrapers.base import BaseScraper
from camplinks.wiki_parsing import (
    candidates_from_parsed,
    parse_basic_wikitable_row,
    parse_candidate_row,
)

logger = logging.getLogger(__name__)

# Pages to skip from the category listing
_SKIP_TITLES: set[str] = {
    "united states local elections",
    "united states mayoral elections",
    "city of starbase",
}


def _extract_city_name(title: str, year: int) -> str:
    """Extract city name from a Wikipedia article title.

    Args:
        title: Page title like "2025 Boston mayoral election".
        year: Election year for stripping prefix.

    Returns:
        City name like "Boston".
    """
    name = title
    prefix = f"{year} "
    if name.startswith(prefix):
        name = name[len(prefix) :]
    for suffix in (
        " mayoral election",
        " municipal election",
        " municipal elections",
        " mayoral special election",
    ):
        if name.lower().endswith(suffix):
            name = name[: -len(suffix)]
            break
    name = re.sub(r"\s*[-–]\s*Wikipedia$", "", name).strip()
    return name


def _is_results_table(table: Tag) -> bool:
    """Heuristic: does this table contain election results?

    Args:
        table: A ``<table>`` element.

    Returns:
        True if the table appears to contain vote results.
    """
    headers = table.find_all("th")
    header_text = " ".join(h.get_text(strip=True).lower() for h in headers)
    return "votes" in header_text or "%" in header_text


def _parse_pattern_a(
    table: Tag,
) -> list[dict[str, str | float | bool | None]]:
    """Parse a basic wikitable without vcard formatting (Pattern A).

    Args:
        table: A ``<table>`` element.

    Returns:
        List of parsed candidate dicts.
    """
    parsed: list[dict[str, str | float | bool | None]] = []
    for row in table.find_all("tr"):
        cand = parse_basic_wikitable_row(row)
        if cand:
            parsed.append(cand)
    return parsed


class MunicipalScraper(BaseScraper):
    """Scraper for US municipal/mayoral elections."""

    race_type = "Mayor"

    def build_index_url(self, year: int) -> str:
        """Build Wikipedia category URL for mayoral elections.

        Args:
            year: Election year.

        Returns:
            Category page URL.
        """
        return f"{BASE_URL}/wiki/Category:{year}_United_States_mayoral_elections"

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract city election page URLs from the Wikipedia category.

        The category page uses a ``<div class="mw-category">`` container
        with nested ``<ul>/<li>/<a>`` elements.

        Args:
            soup: Parsed category page.
            year: Election year.

        Returns:
            De-duplicated list of (city_name, url) tuples.
        """
        seen: set[str] = set()
        results: list[tuple[str, str]] = []

        cat_div_result = soup.find("div", class_="mw-category")
        cat_div: BeautifulSoup | Tag = (
            cat_div_result if isinstance(cat_div_result, Tag) else soup
        )

        pattern = re.compile(
            rf"/wiki/{year}_\w+_mayoral_(?:election|special_election)"
            r"|"
            rf"/wiki/{year}_\w+_municipal_elections?"
        )
        for anchor in cat_div.find_all("a", href=pattern):
            href = str(anchor["href"])
            if href in seen:
                continue

            title = anchor.get_text(strip=True)
            if any(skip in title.lower() for skip in _SKIP_TITLES):
                continue

            seen.add(href)
            city_name = _extract_city_name(title, year)
            if not city_name:
                continue
            results.append((city_name, f"{BASE_URL}{href}"))

        # Also try direct links not matching the regex
        for anchor in cat_div.find_all("a"):
            href = str(anchor.get("href", ""))
            if not href.startswith("/wiki/") or href in seen:
                continue
            title = anchor.get_text(strip=True)
            if "mayoral" not in title.lower() and "municipal" not in title.lower():
                continue
            if any(skip in title.lower() for skip in _SKIP_TITLES):
                continue
            seen.add(href)
            city_name = _extract_city_name(title, year)
            if city_name:
                results.append((city_name, f"{BASE_URL}{href}"))

        logger.info("Found %d municipal election pages for %d.", len(results), year)
        return results

    def parse_state_page(
        self,
        state: str,
        soup: BeautifulSoup,
        year: int,
    ) -> list[tuple[Election, list[Candidate]]]:
        """Parse a mayoral election page.

        Tries Pattern B (vcard) first, then falls back to Pattern A
        (basic wikitable). The ``state`` field stores the city name.

        Args:
            state: City name.
            soup: Parsed election page.
            year: Election year.

        Returns:
            List containing a single (Election, candidates) tuple,
            or empty list if no results table found.
        """
        # Pattern B: wikitable plainrowheaders with vcard
        tables_b = soup.find_all(
            "table",
            class_=lambda c: c and "wikitable" in c and "plainrowheaders" in c,
        )
        for table in tables_b:
            if not _is_results_table(table):
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
                    race_type="Mayor",
                    year=year,
                    district=None,
                )
                return [(election, candidates)]

        # Pattern A: basic wikitable without vcard
        for table in soup.find_all("table", class_=lambda c: c and "wikitable" in c):
            if not _is_results_table(table):
                continue
            parsed = _parse_pattern_a(table)
            candidates = candidates_from_parsed(parsed)
            if candidates:
                election = Election(
                    state=state,
                    race_type="Mayor",
                    year=year,
                    district=None,
                )
                return [(election, candidates)]

        return []


register_scraper("municipal", MunicipalScraper)
