"""Wikipedia scraper for US state supreme court elections.

Handles two election formats:
- **Contested elections** (e.g. Wisconsin): standard ``wikitable
  plainrowheaders`` + ``vcard`` rows with candidate-vs-candidate results.
- **Retention elections** (e.g. Pennsylvania): Yes/No vote format where
  a single justice is either retained or not.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, Tag

from camplinks.http import BASE_URL
from camplinks.models import Candidate, Election
from camplinks.scrapers import register_scraper
from camplinks.scrapers.base import BaseScraper
from camplinks.wiki_parsing import (
    candidates_from_parsed,
    is_general_election_table,
    parse_basic_wikitable_row,
    parse_candidate_row,
)

_SUPREME_COURT_LINK_RE = re.compile(r"/wiki/\d{4}_\w+_Supreme_Court_election")


def _parse_retention_table(
    table: Tag,
) -> list[dict[str, str | float | bool | None]]:
    """Parse a retention election table (Yes/No vote format).

    Retention tables list a single justice with Yes and No vote counts
    and percentages.

    Args:
        table: A ``<table>`` element.

    Returns:
        List of parsed candidate dicts (one per justice).
    """
    results: list[dict[str, str | float | bool | None]] = []
    rows = table.find_all("tr")

    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 3:
            continue

        name_cell = None
        for cell in cells:
            link = cell.find("a")
            if link and link.get("href", "").startswith("/wiki/"):
                text = cell.get_text(strip=True)
                if not text:
                    continue
                # Skip cells that are just "Yes" or "No" vote labels
                stripped = text.strip().lower()
                if stripped in ("yes", "no"):
                    continue
                name_cell = cell
                break

        if name_cell is None:
            continue

        candidate_name = name_cell.get_text(strip=True)
        wiki_url = ""
        link = name_cell.find("a")
        if link:
            href_val = str(link.get("href", ""))
            if href_val.startswith("/wiki/"):
                wiki_url = f"{BASE_URL}{href_val}"

        yes_pct: float | None = None
        for cell in cells:
            text = cell.get_text(strip=True).lower()
            if "%" in text and ("yes" in text or cell.find("b")):
                pct_str = text.replace(",", "").replace("%", "")
                for part in pct_str.split():
                    try:
                        yes_pct = float(part)
                        break
                    except ValueError:
                        continue

        is_retained = yes_pct is not None and yes_pct > 50.0

        results.append(
            {
                "party": "",
                "name": candidate_name,
                "wiki_url": wiki_url,
                "vote_pct": yes_pct,
                "is_winner": is_retained,
            }
        )

    return results


class JudicialScraper(BaseScraper):
    """Scraper for US state supreme court elections."""

    race_type = "State Supreme Court"

    def build_index_url(self, year: int) -> str:
        """Build Wikipedia index URL for judicial elections.

        Args:
            year: Election year.

        Returns:
            Index page URL.
        """
        return f"{BASE_URL}/wiki/{year}_United_States_judicial_elections"

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract state supreme court election page URLs.

        Args:
            soup: Parsed index page.
            year: Election year.

        Returns:
            De-duplicated list of (state, url) tuples.
        """
        pattern = re.compile(rf"/wiki/{year}_\w+_Supreme_Court_election")
        seen: set[str] = set()
        results: list[tuple[str, str]] = []
        for anchor in soup.find_all("a", href=pattern):
            href = str(anchor["href"])
            if href in seen:
                continue
            seen.add(href)
            state_name = (
                href.split(f"/wiki/{year}_", maxsplit=1)[-1]
                .rsplit("_Supreme_Court_election", maxsplit=1)[0]
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
        """Parse state supreme court election results.

        Handles both contested (candidate-vs-candidate) and retention
        (Yes/No vote) election formats.

        Args:
            state: Human-readable state name.
            soup: Parsed state elections page.
            year: Election year.

        Returns:
            List of (Election, candidates) tuples.
        """
        results: list[tuple[Election, list[Candidate]]] = []

        # Try contested election tables first
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
                    race_type="State Supreme Court",
                    year=year,
                    district=None,
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
                    race_type="State Supreme Court",
                    year=year,
                    district=None,
                )
                results.append((election, candidates))

        if results:
            return results

        # Pattern A: basic wikitable with <th scope="row"> (no vcard)
        for table in soup.find_all("table", class_=lambda c: c and "wikitable" in c):
            caption = table.find("caption")
            if not caption:
                continue
            cap_text = caption.get_text(strip=True).lower()
            if "election" not in cap_text:
                continue
            parsed = []
            for row in table.find_all("tr"):
                cand = parse_basic_wikitable_row(row)
                if cand:
                    parsed.append(cand)
            candidates = candidates_from_parsed(parsed)
            if candidates:
                election = Election(
                    state=state,
                    race_type="State Supreme Court",
                    year=year,
                    district=None,
                )
                results.append((election, candidates))

        if results:
            return results

        # Retention elections: look for tables with Yes/No patterns
        for table in soup.find_all("table", class_=lambda c: c and "wikitable" in c):
            table_text = table.get_text(strip=True).lower()
            if "retention" not in table_text and "yes" not in table_text:
                continue
            parsed = _parse_retention_table(table)
            candidates = candidates_from_parsed(parsed)
            if candidates:
                election = Election(
                    state=state,
                    race_type="State Supreme Court",
                    year=year,
                    district=None,
                )
                results.append((election, candidates))

        return results


register_scraper("judicial", JudicialScraper)
