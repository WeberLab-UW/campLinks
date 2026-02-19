"""Wikipedia scraper for US Senate elections.

Senate pages use the same ``wikitable plainrowheaders`` + ``vcard`` row
format as House races, so most parsing logic is reused from
``wiki_parsing``. The key difference is that Senate races are statewide
(no district) and use a different URL pattern.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from camplinks.http import BASE_URL
from camplinks.models import Candidate, Election
from camplinks.scrapers import register_scraper
from camplinks.scrapers.base import BaseScraper
from camplinks.wiki_parsing import (
    is_general_election_table,
    parse_candidate_row,
)

_STATE_LINK_RE_TEMPLATE = r"/wiki/{year}_United_States_Senate_election_in_"


class SenateScraper(BaseScraper):
    """Scraper for US Senate elections."""

    race_type = "US Senate"

    def build_index_url(self, year: int) -> str:
        """Build Wikipedia index URL for Senate elections.

        Args:
            year: Election year.

        Returns:
            Index page URL.
        """
        return f"{BASE_URL}/wiki/{year}_United_States_Senate_elections"

    def collect_state_urls(
        self, soup: BeautifulSoup, year: int
    ) -> list[tuple[str, str]]:
        """Extract state page URLs from the Senate elections index.

        Args:
            soup: Parsed index page.
            year: Election year.

        Returns:
            De-duplicated list of (state, url) tuples.
        """
        pattern = re.compile(rf"/wiki/{year}_United_States_Senate_election_in_")
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
        """Parse the general election results from a Senate state page.

        Senate races are statewide (district=None). Each state page
        has one general election table with the same format as House.

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

            if not parsed:
                continue

            candidates: list[Candidate] = []
            for c in parsed:
                name = str(c.get("name", ""))
                if not name:
                    continue
                candidates.append(
                    Candidate(
                        party=str(c.get("party", "")),
                        candidate_name=name,
                        wikipedia_url=str(c.get("wiki_url", "")),
                        vote_pct=float(vp)
                        if (vp := c.get("vote_pct")) is not None
                        and isinstance(vp, (int, float))
                        else None,
                        is_winner=bool(c.get("is_winner", False)),
                    )
                )

            if candidates:
                election = Election(
                    state=state,
                    race_type="US Senate",
                    year=year,
                    district=None,
                )
                return [(election, candidates)]

        return []


register_scraper("senate", SenateScraper)
