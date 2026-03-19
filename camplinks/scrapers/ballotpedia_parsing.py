"""Shared parsing utilities for Ballotpedia election pages.

Handles the standard Ballotpedia votebox HTML structure used across
mayoral, gubernatorial, and other election page types.
"""

from __future__ import annotations

import re

from bs4 import Tag

BALLOTPEDIA_BASE = "https://ballotpedia.org"
BALLOTPEDIA_DELAY_S: float = 1.5

_PARTY_RE = re.compile(r"\(([^)]+)\)\s*$")


def parse_candidate_cell(text: str) -> tuple[str, str]:
    """Extract candidate name and party from Ballotpedia cell text.

    Ballotpedia formats candidates as "John Smith (Democrat)" or
    "Jane Doe (Nonpartisan)" or "Bob Jones (D)".

    Args:
        text: Cell text containing name and optional party.

    Returns:
        Tuple of (name, party).
    """
    text = text.strip()
    match = _PARTY_RE.search(text)
    if match:
        party = match.group(1)
        name = text[: match.start()].strip()
        return name, party
    return text, ""


def detect_election_stage(votebox: Tag) -> str:
    """Determine election stage from the votebox race header.

    Checks the ``votebox-header-election-type`` h5 inside the votebox
    for keywords like "runoff" or "primary".

    Args:
        votebox: A ``.votebox`` or ``.rcvvotebox`` container element.

    Returns:
        ``"general"``, ``"runoff"``, or ``"primary"``.
    """
    header = votebox.find(["h5", "h3"], class_=lambda c: c and "votebox-header" in c)
    if header:
        text = header.get_text(strip=True).lower()
        if "runoff" in text:
            return "runoff"
        if "primary" in text:
            return "primary"
    return "general"


def parse_results_rows(
    container: Tag,
) -> list[dict[str, str | float | bool | None]]:
    """Parse candidate data from results_row elements inside a container.

    Works for both standard votebox and RCV votebox tables.

    Args:
        container: A tag containing ``<tr class="results_row">`` elements.

    Returns:
        List of parsed candidate dicts.
    """
    results: list[dict[str, str | float | bool | None]] = []
    rows = container.find_all("tr", class_="results_row")

    for row in rows:
        if not isinstance(row, Tag):
            continue

        classes = row.get("class") or []
        is_winner = "won" if "winner" in classes else "lost"

        # Find the text cell (candidate name + party)
        text_cell = row.find(
            "td", class_=lambda c: c and "votebox-results-cell--text" in c
        )
        if not text_cell:
            # Fallback: first td with an anchor tag
            for td in row.find_all("td"):
                if td.find("a"):
                    text_cell = td
                    break
        if not text_cell:
            continue

        # Extract candidate name and Ballotpedia URL
        link = text_cell.find("a")
        candidate_name = ""
        bp_url = ""
        if link and isinstance(link, Tag):
            candidate_name = link.get_text(strip=True)
            href = str(link.get("href", ""))
            if href.startswith("/"):
                bp_url = f"{BALLOTPEDIA_BASE}{href}"
            elif href.startswith("http"):
                bp_url = href

        # Extract party from the full cell text
        full_text = text_cell.get_text(strip=True)
        _, party = parse_candidate_cell(full_text)

        if not candidate_name:
            candidate_name, party = parse_candidate_cell(full_text)

        if not candidate_name:
            continue

        # Extract vote percentage from number cells
        vote_pct: float | None = None
        pct_span = row.find("span", class_="percentage_number")
        if pct_span:
            pct_text = pct_span.get_text(strip=True).replace("%", "").replace(",", "")
            try:
                vote_pct = float(pct_text)
            except ValueError:
                pass
        else:
            number_cells = row.find_all(
                "td",
                class_=lambda c: c and "votebox-results-cell--number" in c,
            )
            for cell in number_cells:
                cell_text = cell.get_text(strip=True).replace("%", "").replace(",", "")
                try:
                    val = float(cell_text)
                    if 0 <= val <= 100:
                        vote_pct = val
                        break
                except ValueError:
                    continue

        results.append(
            {
                "name": candidate_name,
                "party": party,
                "wiki_url": bp_url,
                "vote_pct": vote_pct,
                "is_winner": is_winner,
            }
        )

    return results


def parse_votebox(votebox: Tag) -> list[dict[str, str | float | bool | None]]:
    """Parse a standard Ballotpedia votebox container.

    Args:
        votebox: A ``<div class="votebox">`` element.

    Returns:
        List of parsed candidate dicts.
    """
    return parse_results_rows(votebox)


def parse_rcv_votebox(
    votebox: Tag,
) -> list[dict[str, str | float | bool | None]]:
    """Parse an RCV (ranked-choice voting) votebox container.

    RCV pages contain multiple round tables. This function finds the
    final round (the one containing a winner) and parses it.

    Args:
        votebox: A ``<div class="rcvvotebox">`` element.

    Returns:
        List of parsed candidate dicts from the final round.
    """
    tables = votebox.find_all("table")

    for table in tables:
        winner_row = table.find("tr", class_=lambda c: c and "winner" in c)
        if winner_row:
            return parse_results_rows(table)

    if tables:
        return parse_results_rows(tables[0])

    return []
