"""Unit tests for camplinks.archive module."""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest
import requests

from camplinks.archive import (
    ArchiveMatch,
    filter_by_state,
    lookup_archive_entries,
    lookup_candidate,
    normalize_state,
    parse_profile,
    parse_search_results,
)
from camplinks.db import (
    init_schema,
    upsert_candidate,
    upsert_election,
)
from camplinks.models import Candidate, Election


@pytest.fixture()
def db() -> sqlite3.Connection:
    """Create an in-memory database with full schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    init_schema(conn)
    return conn


def _seed_candidate(
    db: sqlite3.Connection,
    name: str = "Jane Doe",
    state: str = "Virginia",
    race_type: str = "US House",
    district: str = "5",
    year: int = 2024,
) -> int:
    """Insert an election + candidate. Return candidate_id."""
    e = Election(state=state, race_type=race_type, year=year, district=district)
    eid = upsert_election(db, e)
    c = Candidate(party="Democratic", candidate_name=name)
    cid = upsert_candidate(db, c, eid)
    db.commit()
    return cid


SEARCH_HTML_TWO_HITS = """
<html><body>
<a class="resource-tease" href="/organizations/123">
    <span class="flag-icon flag-icon-us"></span>
    <div class="resource-tease__title-right">Jane Doe for Congress</div>
    <div class="resource-tease__meta-item">
        <strong>1,234</strong> messages
    </div>
</a>
<a class="resource-tease" href="/organizations/456">
    <span class="flag-icon flag-icon-us"></span>
    <div class="resource-tease__title-right">Jane Doe (impostor)</div>
    <div class="resource-tease__meta-item">
        <strong>5</strong> messages
    </div>
</a>
</body></html>
"""

SEARCH_HTML_NO_HITS = "<html><body><p>No results.</p></body></html>"

PROFILE_HTML_VIRGINIA = """
<html><body>
<ul class="key-val-list">
    <li><strong>State/Locality:</strong> <a href="/x">Virginia</a></li>
    <li><strong>Party:</strong> Democratic</li>
    <li><strong>Office Held/Sought:</strong> US House</li>
    <li><strong>Website:</strong> <a href="https://janedoe.com">janedoe.com</a></li>
</ul>
</body></html>
"""

PROFILE_HTML_TEXAS = """
<html><body>
<ul class="key-val-list">
    <li><strong>State/Locality:</strong> Texas</li>
    <li><strong>Party:</strong> Independent</li>
</ul>
</body></html>
"""


class TestParseSearchResults:
    """parse_search_results extracts org id, name, country, message_count."""

    def test_extracts_two_hits(self) -> None:
        results = parse_search_results(SEARCH_HTML_TWO_HITS)
        assert len(results) == 2
        assert results[0].org_id == "123"
        assert results[0].name == "Jane Doe for Congress"
        assert results[0].country == "us"
        assert results[0].message_count == 1234
        assert results[0].archive_url == "https://politicalemails.org/organizations/123"
        assert results[1].org_id == "456"
        assert results[1].message_count == 5

    def test_no_hits_returns_empty(self) -> None:
        assert parse_search_results(SEARCH_HTML_NO_HITS) == []

    def test_skips_tile_without_org_id(self) -> None:
        html = """
        <a class="resource-tease" href="/users/789">
            <div class="resource-tease__title-right">Not an org</div>
        </a>
        """
        assert parse_search_results(html) == []


class TestParseProfile:
    """parse_profile extracts state/party/office/website from the key-val list."""

    def test_extracts_all_fields(self) -> None:
        profile = parse_profile(PROFILE_HTML_VIRGINIA)
        assert profile["state"] == "Virginia"
        assert profile["party"] == "Democratic"
        assert profile["office"] == "US House"
        assert profile["website"] == "janedoe.com"

    def test_extracts_partial_fields(self) -> None:
        profile = parse_profile(PROFILE_HTML_TEXAS)
        assert profile["state"] == "Texas"
        assert profile["party"] == "Independent"
        assert profile["office"] is None
        assert profile["website"] is None


class TestNormalizeState:
    """normalize_state strips whitespace, lowercases, splits on comma."""

    def test_plain_state(self) -> None:
        assert normalize_state("Virginia") == "virginia"

    def test_city_state_format(self) -> None:
        assert normalize_state("Houston, Texas") == "texas"

    def test_handles_extra_whitespace(self) -> None:
        assert normalize_state("  Texas  ") == "texas"

    def test_empty_string(self) -> None:
        assert normalize_state("") == ""


class TestFilterByState:
    """filter_by_state keeps only matches with matching enriched state."""

    def test_keeps_matching_state(self) -> None:
        matches = [
            ArchiveMatch(org_id="1", name="A", archive_url="x", state="Virginia"),
            ArchiveMatch(org_id="2", name="B", archive_url="x", state="Texas"),
        ]
        kept = filter_by_state(matches, "Virginia")
        assert len(kept) == 1
        assert kept[0].org_id == "1"

    def test_drops_unenriched_matches(self) -> None:
        matches = [
            ArchiveMatch(org_id="1", name="A", archive_url="x", state=None),
        ]
        assert filter_by_state(matches, "Virginia") == []

    def test_case_insensitive(self) -> None:
        matches = [
            ArchiveMatch(org_id="1", name="A", archive_url="x", state="VIRGINIA")
        ]
        assert len(filter_by_state(matches, "virginia")) == 1

    def test_handles_city_state_candidate(self) -> None:
        matches = [
            ArchiveMatch(org_id="1", name="A", archive_url="x", state="Texas"),
        ]
        kept = filter_by_state(matches, "Houston, Texas")
        assert len(kept) == 1


class TestLookupCandidate:
    """lookup_candidate orchestrates search + enrich + filter."""

    def test_no_search_results_returns_no_match(self) -> None:
        client = MagicMock()
        client.search.return_value = []
        status, matches = lookup_candidate(client, "Jane Doe", "Virginia")
        assert status == "no_match"
        assert matches == []
        client.profile.assert_not_called()

    def test_single_match_after_state_filter(self) -> None:
        client = MagicMock()
        client.search.return_value = [
            ArchiveMatch(org_id="1", name="A", archive_url="x", message_count=10),
            ArchiveMatch(org_id="2", name="B", archive_url="x", message_count=99),
        ]
        client.profile.side_effect = [
            {"state": "Virginia", "party": "D", "office": "US House", "website": None},
            {"state": "Texas", "party": "R", "office": "US House", "website": None},
        ]
        status, matches = lookup_candidate(client, "Jane Doe", "Virginia")
        assert status == "single"
        assert [m.org_id for m in matches] == ["1"]
        assert matches[0].state == "Virginia"

    def test_multiple_matches_when_two_orgs_share_state(self) -> None:
        client = MagicMock()
        client.search.return_value = [
            ArchiveMatch(org_id="1", name="A", archive_url="x"),
            ArchiveMatch(org_id="2", name="B", archive_url="x"),
        ]
        client.profile.side_effect = [
            {"state": "Virginia", "party": None, "office": None, "website": None},
            {"state": "Virginia", "party": None, "office": None, "website": None},
        ]
        status, matches = lookup_candidate(client, "Jane Doe", "Virginia")
        assert status == "multiple"
        assert len(matches) == 2

    def test_all_filtered_out_returns_no_match(self) -> None:
        client = MagicMock()
        client.search.return_value = [
            ArchiveMatch(org_id="1", name="A", archive_url="x"),
        ]
        client.profile.return_value = {
            "state": "California",
            "party": None,
            "office": None,
            "website": None,
        }
        status, matches = lookup_candidate(client, "Jane Doe", "Virginia")
        assert status == "no_match"
        assert matches == []

    def test_search_request_error_returns_error_status(self) -> None:
        client = MagicMock()
        client.search.side_effect = requests.ConnectionError("dns")
        status, matches = lookup_candidate(client, "Jane Doe", "Virginia")
        assert status == "error"
        assert matches == []

    def test_profile_failure_drops_that_match(self) -> None:
        client = MagicMock()
        client.search.return_value = [
            ArchiveMatch(org_id="1", name="A", archive_url="x"),
            ArchiveMatch(org_id="2", name="B", archive_url="x"),
        ]
        client.profile.side_effect = [
            requests.Timeout("slow"),
            {"state": "Virginia", "party": None, "office": None, "website": None},
        ]
        status, matches = lookup_candidate(client, "Jane Doe", "Virginia")
        assert status == "single"
        assert matches[0].org_id == "2"


class TestLookupArchiveEntries:
    """lookup_archive_entries persists results and is idempotent."""

    @patch("camplinks.archive.lookup_candidate")
    @patch("camplinks.archive.ArchiveClient")
    def test_writes_lookup_and_match_for_single_hit(
        self,
        mock_client_cls: MagicMock,
        mock_lookup: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        cid = _seed_candidate(db)
        mock_lookup.return_value = (
            "single",
            [
                ArchiveMatch(
                    org_id="42",
                    name="Jane Doe for Congress",
                    archive_url="https://politicalemails.org/organizations/42",
                    country="us",
                    message_count=1234,
                    state="Virginia",
                    party="Democratic",
                    office="US House",
                    website="https://janedoe.com",
                )
            ],
        )

        result = lookup_archive_entries(db)
        assert result == 1

        lookup_row = db.execute(
            "SELECT has_entry, match_count, total_messages, status "
            "FROM archive_lookups WHERE candidate_id = ?",
            (cid,),
        ).fetchone()
        assert lookup_row["has_entry"] == 1
        assert lookup_row["match_count"] == 1
        assert lookup_row["total_messages"] == 1234
        assert lookup_row["status"] == "single"

        org_row = db.execute(
            "SELECT name, state, message_count FROM archive_organizations "
            "WHERE org_id = '42'"
        ).fetchone()
        assert org_row["name"] == "Jane Doe for Congress"
        assert org_row["state"] == "Virginia"
        assert org_row["message_count"] == 1234

        match_row = db.execute(
            "SELECT COUNT(*) FROM candidate_archive_matches "
            "WHERE candidate_id = ? AND org_id = '42'",
            (cid,),
        ).fetchone()
        assert match_row[0] == 1

    @patch("camplinks.archive.lookup_candidate")
    @patch("camplinks.archive.ArchiveClient")
    def test_writes_no_match_row_with_zero_messages(
        self,
        mock_client_cls: MagicMock,
        mock_lookup: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        cid = _seed_candidate(db)
        mock_lookup.return_value = ("no_match", [])

        result = lookup_archive_entries(db)
        assert result == 0

        row = db.execute(
            "SELECT has_entry, match_count, total_messages, status "
            "FROM archive_lookups WHERE candidate_id = ?",
            (cid,),
        ).fetchone()
        assert row["has_entry"] == 0
        assert row["match_count"] == 0
        assert row["total_messages"] is None
        assert row["status"] == "no_match"

    @patch("camplinks.archive.lookup_candidate")
    @patch("camplinks.archive.ArchiveClient")
    def test_skips_already_looked_up_candidates(
        self,
        mock_client_cls: MagicMock,
        mock_lookup: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        _seed_candidate(db)
        mock_lookup.return_value = ("no_match", [])
        lookup_archive_entries(db)
        mock_lookup.reset_mock()

        result = lookup_archive_entries(db)
        assert result == 0
        mock_lookup.assert_not_called()

    @patch("camplinks.archive.lookup_candidate")
    @patch("camplinks.archive.ArchiveClient")
    def test_sums_message_count_across_multiple_matches(
        self,
        mock_client_cls: MagicMock,
        mock_lookup: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        cid = _seed_candidate(db)
        mock_lookup.return_value = (
            "multiple",
            [
                ArchiveMatch(
                    org_id="1",
                    name="Org 1",
                    archive_url="x",
                    state="Virginia",
                    message_count=100,
                ),
                ArchiveMatch(
                    org_id="2",
                    name="Org 2",
                    archive_url="x",
                    state="Virginia",
                    message_count=50,
                ),
            ],
        )

        lookup_archive_entries(db)

        row = db.execute(
            "SELECT match_count, total_messages, status "
            "FROM archive_lookups WHERE candidate_id = ?",
            (cid,),
        ).fetchone()
        assert row["match_count"] == 2
        assert row["total_messages"] == 150
        assert row["status"] == "multiple"

    @patch("camplinks.archive.lookup_candidate")
    @patch("camplinks.archive.ArchiveClient")
    def test_filters_by_year_and_race_type(
        self,
        mock_client_cls: MagicMock,
        mock_lookup: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        _seed_candidate(db, name="A", year=2024, race_type="US House")
        _seed_candidate(db, name="B", year=2026, race_type="US House")
        _seed_candidate(db, name="C", year=2024, race_type="US Senate", district="")
        mock_lookup.return_value = ("no_match", [])

        lookup_archive_entries(db, year=2024, race_type="US House")

        looked_up = {
            row[0]
            for row in db.execute(
                "SELECT candidate_name FROM candidates c "
                "JOIN archive_lookups a ON a.candidate_id = c.candidate_id"
            ).fetchall()
        }
        assert looked_up == {"A"}
