"""Shared Wikipedia HTML parsing utilities for camplinks.

Functions in this module work across all race types (House, Senate,
Governor, etc.) and handle common Wikipedia markup patterns.
"""

from __future__ import annotations

import re
from typing import Optional

from bs4 import Tag

from camplinks.http import BASE_URL

# ── Compiled patterns ──────────────────────────────────────────────────────

DISTRICT_NUM_RE = re.compile(r"(\d+)(?:st|nd|rd|th)")
AT_LARGE_RE = re.compile(r"at.large", re.IGNORECASE)
INCUMBENT_RE = re.compile(r"\s*\(incumbent\)")


def find_preceding_heading(
    element: Tag,
    heading_tags: tuple[str, ...],
) -> Optional[Tag]:
    """Walk backward through siblings to find the nearest heading tag.

    Wikipedia wraps headings in ``<div class="mw-heading mw-headingN">``
    containers, so we check both bare heading tags and heading tags
    nested inside mw-heading divs.

    Args:
        element: Starting element.
        heading_tags: Tuple of tag names to match (e.g. ``("h2",)``).

    Returns:
        The first matching heading Tag, or None.
    """
    prev = element.previous_sibling
    while prev is not None:
        if isinstance(prev, Tag):
            if prev.name in heading_tags:
                return prev
            if prev.name == "div" and "mw-heading" in (prev.get("class") or []):
                inner = prev.find(heading_tags)
                if inner and isinstance(inner, Tag):
                    return inner
        prev = prev.previous_sibling
    return None


def extract_district_number(heading_text: str) -> str:
    """Parse a district identifier from a heading string.

    Args:
        heading_text: Section heading text (e.g. "District 3[edit]").

    Returns:
        District string like "3" or "At-Large".
    """
    if AT_LARGE_RE.search(heading_text):
        return "At-Large"
    m = DISTRICT_NUM_RE.search(heading_text)
    if m:
        return m.group(1)
    m2 = re.search(r"[Dd]istrict\s+(\d+)", heading_text)
    if m2:
        return m2.group(1)
    lower = heading_text.strip().lower()
    if "general election" in lower or "results" in lower:
        return "At-Large"
    m3 = re.search(r"(\d+)", heading_text)
    return m3.group(1) if m3 else "At-Large"


def is_general_election_table(table: Tag) -> bool:
    """Heuristic: does this plainrowheaders table represent a general election?

    Checks the caption and the preceding h3/h4 heading.

    Args:
        table: A ``<table>`` element.

    Returns:
        True if this appears to be a general-election results table.
    """
    caption = table.find("caption")
    if caption:
        cap_text = caption.get_text(strip=True).lower()
        if "primary" in cap_text or "runoff" in cap_text:
            return False
        if "election" in cap_text:
            return True

    heading = find_preceding_heading(table, ("h3", "h4"))
    if heading:
        h_text = heading.get_text(strip=True).lower()
        if "general election" in h_text or "results" in h_text:
            return True
        if "primary" in h_text or "runoff" in h_text:
            return False

    return False


def parse_candidate_row(
    row: Tag,
) -> Optional[dict[str, str | float | bool | None]]:
    """Extract candidate info from a ``<tr class="vcard">`` row.

    Args:
        row: A table row with class ``vcard``.

    Returns:
        Dict with keys ``party``, ``name``, ``wiki_url``, ``vote_pct``,
        ``is_winner``; or None if the row cannot be parsed.
    """
    cells = row.find_all(["td", "th"])
    if len(cells) < 4:
        return None

    org_cell = None
    name_cell = None
    pct_idx = -1

    for i, cell in enumerate(cells):
        cls: list[str] = cell.get("class") or []  # type: ignore[assignment]
        if "org" in cls:
            org_cell = cell
            colspan = int(str(cell.get("colspan", "1")))
            if colspan >= 2:
                pct_idx = i + 2
            else:
                name_cell = cells[i + 1] if i + 1 < len(cells) else None
                pct_idx = i + 3
            break

    if org_cell is None:
        if len(cells) >= 5:
            org_cell = cells[1]
            name_cell = cells[2]
            pct_idx = 4
        else:
            return None

    party_text = org_cell.get_text(strip=True)
    candidate_name = name_cell.get_text(strip=True) if name_cell else ""
    candidate_name = INCUMBENT_RE.sub("", candidate_name).strip()

    wiki_url = ""
    if name_cell:
        link = name_cell.find("a")
        if link:
            href_val = str(link.get("href", ""))
            if href_val.startswith("/wiki/"):
                wiki_url = f"{BASE_URL}{href_val}"

    vote_pct: float | None = None
    if 0 <= pct_idx < len(cells):
        pct_text = cells[pct_idx].get_text(strip=True).replace(",", "").replace("%", "")
        try:
            vote_pct = float(pct_text)
        except ValueError:
            pass

    is_winner = False
    if name_cell and name_cell.find("b"):
        is_winner = True

    return {
        "party": party_text,
        "name": candidate_name,
        "wiki_url": wiki_url,
        "vote_pct": vote_pct,
        "is_winner": is_winner,
    }


def to_float(val: str | float | None) -> float | None:
    """Safely cast a mixed-type value to float.

    Args:
        val: A string, float, or None.

    Returns:
        The float value, or None.
    """
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
