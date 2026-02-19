"""Wikipedia scraper for US House of Representatives elections.

Handles three distinct table formats:
- Standard: ``wikitable plainrowheaders`` (most states)
- California: combined primary+general tables
- RCV: ``wikitable sortable`` for ranked-choice voting states (Alaska)
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, Tag

from camplinks.http import BASE_URL
from camplinks.models import Candidate, Election
from camplinks.scrapers import register_scraper
from camplinks.scrapers.base import BaseScraper
from camplinks.wiki_parsing import (
    INCUMBENT_RE,
    candidates_from_parsed,
    extract_district_number,
    find_preceding_heading,
    is_general_election_table,
    parse_candidate_row,
)

_STATE_LINK_RE = re.compile(
    r"/wiki/\d{4}_United_States_House_of_Representatives_elections?_in_"
)


def _parse_california_table(
    table: Tag,
    district: str,
    state: str,
    year: int,
) -> tuple[Election, list[Candidate]] | None:
    """Parse California's combined primary+general table format.

    Args:
        table: A ``<table>`` element from the California page.
        district: District identifier string.
        state: State name.
        year: Election year.

    Returns:
        (Election, candidates) tuple or None.
    """
    rows = table.find_all("tr")
    in_general = False
    parsed: list[dict[str, str | float | bool | None]] = []

    for row in rows:
        th = row.find("th")
        if th and "general election" in th.get_text(strip=True).lower():
            colspan = int(str(th.get("colspan", "1")))
            if colspan >= 3:
                in_general = True
                parsed = []
                continue

        if not in_general:
            continue

        if "vcard" in (row.get("class") or []):
            cand = parse_candidate_row(row)
            if cand:
                parsed.append(cand)

    if not parsed:
        return None

    candidates = candidates_from_parsed(parsed)
    if not candidates:
        return None

    election = Election(
        state=state,
        race_type="US House",
        year=year,
        district=district,
    )
    return election, candidates


def _parse_rcv_tables(
    soup: BeautifulSoup,
    state: str,
    year: int,
) -> list[tuple[Election, list[Candidate]]]:
    """Parse ranked-choice voting tables (e.g. Alaska).

    Args:
        soup: Parsed state page.
        state: State name.
        year: Election year.

    Returns:
        List of (Election, candidates) tuples.
    """
    results: list[tuple[Election, list[Candidate]]] = []
    tables = soup.find_all(
        "table",
        class_=lambda c: c and "wikitable" in c and "sortable" in c,
    )
    for table in tables:
        caption = table.find("caption")
        if not caption:
            continue
        cap_text = caption.get_text(strip=True).lower()
        if "election" not in cap_text:
            continue
        if "congressional district" not in cap_text and "at-large" not in cap_text:
            continue

        district = extract_district_number(cap_text)
        parsed: list[dict[str, str | float | bool | None]] = []

        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 4:
                continue
            vcard_span = row.find("span", class_="vcard")
            if not vcard_span:
                continue

            party_text = ""
            for cell in cells:
                link = cell.find("a")
                if link:
                    text = link.get_text(strip=True).lower()
                    if "republican" in text:
                        party_text = "Republican"
                        break
                    if "democrat" in text:
                        party_text = "Democratic"
                        break

            name_link = vcard_span.find("a")
            if name_link:
                name = name_link.get_text(strip=True)
                wiki_url = f"{BASE_URL}{name_link['href']}"
            else:
                name = vcard_span.get_text(strip=True)
                wiki_url = ""

            name = INCUMBENT_RE.sub("", name).strip()

            pct_cells = [c for c in cells if "%" in c.get_text(strip=True)]
            vote_pct: float | None = None
            if pct_cells:
                last_pct = pct_cells[-1].get_text(strip=True)
                last_pct = last_pct.replace(",", "").replace("%", "")
                try:
                    vote_pct = float(last_pct)
                except ValueError:
                    pass

            is_winner = False
            if pct_cells and pct_cells[-1].find("b"):
                is_winner = True

            parsed.append(
                {
                    "party": party_text,
                    "name": name,
                    "wiki_url": wiki_url,
                    "vote_pct": vote_pct,
                    "is_winner": is_winner,
                }
            )

        candidates = candidates_from_parsed(parsed)
        if candidates:
            election = Election(
                state=state,
                race_type="US House",
                year=year,
                district=district,
            )
            results.append((election, candidates))

    return results


class HouseScraper(BaseScraper):
    """Scraper for US House of Representatives elections."""

    race_type = "US House"

    def build_index_url(self, year: int) -> str:
        """Build Wikipedia index URL for House elections.

        Args:
            year: Election year.

        Returns:
            Index page URL.
        """
        return (
            f"{BASE_URL}/wiki/{year}_United_States_House_of_Representatives_elections"
        )

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract state page URLs from the House elections index.

        Args:
            soup: Parsed index page.
            year: Election year.

        Returns:
            De-duplicated list of (state, url) tuples.
        """
        pattern = re.compile(
            rf"/wiki/{year}_United_States_House_of_Representatives_elections?_in_"
        )
        seen: set[str] = set()
        results: list[tuple[str, str]] = []
        for anchor in soup.find_all("a", href=pattern):
            href = str(anchor["href"])
            if href in seen:
                continue
            seen.add(href)
            state_name = href.rsplit("_in_", maxsplit=1)[-1].replace("_", " ")
            results.append((state_name, f"{BASE_URL}{href}"))
        return results

    def parse_state_page(
        self,
        state: str,
        soup: BeautifulSoup,
        year: int,
    ) -> list[tuple[Election, list[Candidate]]]:
        """Parse all general-election results from a single state page.

        Args:
            state: Human-readable state name.
            soup: Parsed state elections page.
            year: Election year.

        Returns:
            List of (Election, candidates) tuples, one per district.
        """
        results: list[tuple[Election, list[Candidate]]] = []
        is_california = state.lower() == "california"

        if is_california:
            tables = soup.find_all(
                "table",
                class_=lambda c: c and "wikitable" in c,
            )
            for table in tables:
                caption = table.find("caption")
                if not caption:
                    continue
                cap_text = caption.get_text(strip=True).lower()
                if (
                    "congressional district" not in cap_text
                    and "at-large" not in cap_text
                ):
                    continue

                h2 = find_preceding_heading(table, ("h2",))
                district = extract_district_number(
                    h2.get_text(strip=True) if h2 else cap_text
                )

                result = _parse_california_table(table, district, state, year)
                if result:
                    results.append(result)
        else:
            tables = soup.find_all(
                "table",
                class_=lambda c: c and "wikitable" in c and "plainrowheaders" in c,
            )
            for table in tables:
                if not is_general_election_table(table):
                    continue

                h2 = find_preceding_heading(table, ("h2",))
                district = extract_district_number(
                    h2.get_text(strip=True) if h2 else "At-Large"
                )

                parsed: list[dict[str, str | float | bool | None]] = []
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
                    results.append((election, candidates))

            if not results:
                results.extend(_parse_rcv_tables(soup, state, year))

        return results


register_scraper("house", HouseScraper)
