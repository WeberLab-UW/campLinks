"""Look up candidates in the Archive of Political Emails (politicalemails.org).

For each candidate without an existing archive_lookups row, search the
archive by name, optionally enrich each hit with profile metadata, filter
to hits matching the candidate's state, and persist the results.

The lookup is idempotent: any candidate with an existing archive_lookups
row is skipped. To force a re-check, delete that row first.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from camplinks.db import (
    get_candidates_needing_archive_lookup,
    link_candidate_to_org,
    upsert_archive_lookup,
    upsert_archive_organization,
)
from camplinks.http import HEADERS

logger = logging.getLogger(__name__)

ARCHIVE_BASE = "https://politicalemails.org"
ARCHIVE_DELAY_S: float = 1.0
ARCHIVE_TIMEOUT_S: float = 20.0


@dataclass
class ArchiveMatch:
    """A single politicalemails.org organization match.

    Attributes:
        org_id: Numeric id parsed from the organization URL.
        name: Display name of the organization.
        archive_url: Canonical URL of the organization page.
        country: Two-letter country code from the search result flag.
        message_count: Number of archived messages, if shown.
        state: State/locality, populated only after profile enrichment.
        party: Party label, populated only after profile enrichment.
        office: Office held/sought, populated only after profile enrichment.
        website: Website URL, populated only after profile enrichment.
    """

    org_id: str
    name: str
    archive_url: str
    country: str | None = None
    message_count: int | None = None
    state: str | None = None
    party: str | None = None
    office: str | None = None
    website: str | None = None


class ArchiveClient:
    """Throttled HTTP client for politicalemails.org."""

    def __init__(
        self,
        delay_s: float = ARCHIVE_DELAY_S,
        timeout_s: float = ARCHIVE_TIMEOUT_S,
    ) -> None:
        """Initialize the client.

        Args:
            delay_s: Minimum seconds between requests.
            timeout_s: HTTP request timeout in seconds.
        """
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.delay_s = delay_s
        self.timeout_s = timeout_s
        self._last_call = 0.0

    def _throttle(self) -> None:
        """Sleep until at least delay_s has elapsed since the last call."""
        wait = self.delay_s - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _get(self, url: str) -> str:
        """Fetch *url* and return the response body as text.

        Args:
            url: Fully-qualified URL.

        Returns:
            Response body text.

        Raises:
            requests.RequestException: On HTTP or network failure.
        """
        self._throttle()
        resp = self.session.get(url, timeout=self.timeout_s)
        resp.raise_for_status()
        return resp.text

    def search(self, query: str) -> list[ArchiveMatch]:
        """Search the organizations index by free-text query.

        Args:
            query: Search term (typically the candidate name).

        Returns:
            List of ArchiveMatch objects from the search results.

        Raises:
            requests.RequestException: On HTTP or network failure.
        """
        url = f"{ARCHIVE_BASE}/organizations?{urlencode({'query': query})}"
        return parse_search_results(self._get(url))

    def profile(self, org_id: str) -> dict[str, str | None]:
        """Fetch and parse an organization's profile page.

        Args:
            org_id: politicalemails.org organization id.

        Returns:
            Dict with state, party, office, website keys (any may be None).

        Raises:
            requests.RequestException: On HTTP or network failure.
        """
        url = f"{ARCHIVE_BASE}/organizations/{org_id}"
        return parse_profile(self._get(url))


def parse_search_results(html: str) -> list[ArchiveMatch]:
    """Parse the politicalemails.org organizations search page.

    Args:
        html: HTML response body.

    Returns:
        List of ArchiveMatch objects, one per result tile.
    """
    soup = BeautifulSoup(html, "lxml")
    out: list[ArchiveMatch] = []
    for a in soup.select("a.resource-tease"):
        href = a.get("href", "")
        m = re.search(r"/organizations/(\d+)", str(href))
        if not m:
            continue
        org_id = m.group(1)

        name_el = a.select_one(".resource-tease__title-right")
        name = name_el.get_text(strip=True) if name_el else ""

        country: str | None = None
        flag = a.select_one(".flag-icon")
        if flag:
            classes = flag.get("class", []) or []
            for cls in classes:
                cm = re.match(r"flag-icon-([a-z]{2})$", cls)
                if cm:
                    country = cm.group(1)
                    break

        msg_count: int | None = None
        for meta in a.select(".resource-tease__meta-item"):
            strong = meta.find("strong")
            if strong and "message" in meta.get_text().lower():
                try:
                    msg_count = int(strong.get_text(strip=True).replace(",", ""))
                except ValueError:
                    msg_count = None
                break

        out.append(
            ArchiveMatch(
                org_id=org_id,
                name=name,
                archive_url=f"{ARCHIVE_BASE}/organizations/{org_id}",
                country=country,
                message_count=msg_count,
            )
        )
    return out


def parse_profile(html: str) -> dict[str, str | None]:
    """Parse the key/value list on a politicalemails.org organization page.

    Args:
        html: HTML response body.

    Returns:
        Dict with state, party, office, website keys (each str or None).
    """
    soup = BeautifulSoup(html, "lxml")
    out: dict[str, str | None] = {
        "state": None,
        "party": None,
        "office": None,
        "website": None,
    }
    for li in soup.select("ul.key-val-list li"):
        strong = li.find("strong")
        if not strong:
            continue
        key = strong.get_text(strip=True).rstrip(":").lower()
        link = li.find("a", href=lambda h: bool(h) and "/cdn-cgi/" not in h)
        if link:
            val: str | None = link.get_text(strip=True)
        else:
            text = li.get_text(" ", strip=True)
            val = text.replace(strong.get_text(strip=True), "", 1).strip(" :") or None
        if key == "state/locality":
            out["state"] = val
        elif key == "party":
            out["party"] = val
        elif key == "office held/sought":
            out["office"] = val
        elif key == "website":
            out["website"] = val
    return out


def normalize_state(state: str) -> str:
    """Reduce a candidate state field to a comparable state name.

    Handles the bp_municipal "City, State" format by taking the part
    after the last comma. Other formats pass through unchanged.

    Args:
        state: Raw state field from the elections table.

    Returns:
        Lowercased, stripped state name suitable for case-insensitive
        comparison.
    """
    if "," in state:
        state = state.rsplit(",", 1)[1]
    return state.strip().lower()


def filter_by_state(
    matches: list[ArchiveMatch], candidate_state: str
) -> list[ArchiveMatch]:
    """Keep only matches whose enriched state matches the candidate's.

    Matches with an unknown state (None) are dropped, since we have
    no way to verify they belong to the candidate's race. Matches
    where state was never enriched are also dropped.

    Args:
        matches: List of matches, ideally enriched with state info.
        candidate_state: Candidate state from the elections table.

    Returns:
        Filtered list of matches.
    """
    target = normalize_state(candidate_state)
    if not target:
        return matches
    return [m for m in matches if m.state and m.state.strip().lower() == target]


def lookup_candidate(
    client: ArchiveClient,
    name: str,
    state: str,
) -> tuple[str, list[ArchiveMatch]]:
    """Search the archive for *name* and filter to *state*.

    Args:
        client: Throttled archive client.
        name: Candidate name to search.
        state: Candidate state from the elections table.

    Returns:
        Tuple of (status, surviving matches). Status is one of
        "no_match", "single", "multiple", "error".
    """
    try:
        results = client.search(name)
    except requests.RequestException as exc:
        logger.error("archive search failed for %r: %s", name, exc)
        return "error", []

    if not results:
        return "no_match", []

    for m in results:
        try:
            profile = client.profile(m.org_id)
        except requests.RequestException as exc:
            logger.warning("archive profile fetch failed for %s: %s", m.org_id, exc)
            continue
        m.state = profile["state"]
        m.party = profile["party"]
        m.office = profile["office"]
        m.website = profile["website"]

    filtered = filter_by_state(results, state)
    if not filtered:
        return "no_match", []
    if len(filtered) == 1:
        return "single", filtered
    return "multiple", filtered


def lookup_archive_entries(
    conn: sqlite3.Connection,
    year: int | None = None,
    race_type: str | None = None,
    election_stage: str | None = "general",
    delay_s: float = ARCHIVE_DELAY_S,
) -> int:
    """Look up unprocessed candidates in the politicalemails.org archive.

    For each candidate not already in archive_lookups: search by name,
    enrich each hit's profile, filter to the candidate's state, and
    persist surviving matches. Always writes one archive_lookups row
    per candidate (including no_match / error outcomes) so the next run
    skips them.

    Args:
        conn: Open database connection.
        year: Optional filter by election year.
        race_type: Optional filter by race type.
        election_stage: Optional filter by election stage. Defaults to
            "general" to match enrich/search/validate convention.
        delay_s: Minimum seconds between HTTP requests.

    Returns:
        Number of candidates with at least one surviving match.
    """
    targets = get_candidates_needing_archive_lookup(
        conn,
        year=year,
        race_type=race_type,
        election_stage=election_stage,
    )
    if not targets:
        logger.info("No candidates need archive lookup.")
        return 0

    logger.info("Looking up %d candidates in politicalemails.org.", len(targets))
    client = ArchiveClient(delay_s=delay_s)

    matched_count = 0
    error_count = 0
    no_match_count = 0
    commit_every = 25

    for i, row in enumerate(
        tqdm(targets, desc="Archive lookup", unit="candidate"), start=1
    ):
        cid: int = row["candidate_id"]
        name: str = row["candidate_name"]
        state: str = row["state"]
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")

        status, matches = lookup_candidate(client, name, state)

        if status == "error":
            error_count += 1
        elif status == "no_match":
            no_match_count += 1
        else:
            matched_count += 1

        for m in matches:
            upsert_archive_organization(
                conn,
                org_id=m.org_id,
                name=m.name,
                archive_url=m.archive_url,
                country=m.country,
                state=m.state,
                party=m.party,
                office=m.office,
                website=m.website,
                message_count=m.message_count,
                fetched_at=now,
            )
            link_candidate_to_org(conn, cid, m.org_id)

        total_messages = (
            sum(m.message_count for m in matches if m.message_count is not None)
            if matches
            else None
        )
        upsert_archive_lookup(
            conn,
            candidate_id=cid,
            has_entry=bool(matches),
            match_count=len(matches),
            total_messages=total_messages,
            status=status,
            checked_at=now,
        )

        if i % commit_every == 0:
            conn.commit()

    conn.commit()

    logger.info(
        "Archive lookup complete: %d matched, %d no_match, %d errors.",
        matched_count,
        no_match_count,
        error_count,
    )
    return matched_count
