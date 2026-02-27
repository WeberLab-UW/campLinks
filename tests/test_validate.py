"""Unit tests for camplinks.validate module."""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest
import requests

from camplinks.db import (
    init_schema,
    upsert_candidate,
    upsert_contact_link,
    upsert_election,
)
from camplinks.models import Candidate, ContactLink, Election
from camplinks.validate import (
    check_url_accessible,
    query_wayback,
    validate_campaign_sites,
)


@pytest.fixture()
def db() -> sqlite3.Connection:
    """Create an in-memory database with schema and sample data."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    init_schema(conn)
    return conn


def _seed_candidate_with_site(
    db: sqlite3.Connection,
    name: str = "Alice",
    url: str = "https://alice.com",
    year: int = 2024,
) -> int:
    """Insert an election, candidate, and campaign_site link. Return candidate_id."""
    e = Election(state="Ohio", race_type="US House", year=year, district="5")
    eid = upsert_election(db, e)
    c = Candidate(party="Republican", candidate_name=name)
    cid = upsert_candidate(db, c, eid)
    upsert_contact_link(db, ContactLink(cid, "campaign_site", url, "wikipedia"))
    db.commit()
    return cid


class TestCheckUrlAccessible:
    """Tests for check_url_accessible()."""

    @patch("camplinks.validate.requests.head")
    def test_accessible_url_returns_true(self, mock_head: MagicMock) -> None:
        mock_head.return_value = MagicMock(status_code=200)
        assert check_url_accessible("https://example.com") is True

    @patch("camplinks.validate.requests.head")
    def test_404_returns_false(self, mock_head: MagicMock) -> None:
        mock_head.return_value = MagicMock(status_code=404)
        assert check_url_accessible("https://example.com") is False

    @patch("camplinks.validate.requests.head")
    def test_connection_error_returns_false(self, mock_head: MagicMock) -> None:
        mock_head.side_effect = requests.ConnectionError("DNS failed")
        assert check_url_accessible("https://example.com") is False

    @patch("camplinks.validate.requests.head")
    def test_timeout_returns_false(self, mock_head: MagicMock) -> None:
        mock_head.side_effect = requests.Timeout("timed out")
        assert check_url_accessible("https://example.com") is False

    @patch("camplinks.validate.requests.get")
    @patch("camplinks.validate.requests.head")
    def test_head_405_falls_back_to_get(
        self, mock_head: MagicMock, mock_get: MagicMock
    ) -> None:
        mock_head.return_value = MagicMock(status_code=405)
        mock_get.return_value = MagicMock(status_code=200)
        assert check_url_accessible("https://example.com") is True
        mock_get.assert_called_once()

    @patch("camplinks.validate.requests.head")
    def test_redirect_considered_accessible(self, mock_head: MagicMock) -> None:
        mock_head.return_value = MagicMock(status_code=200)
        assert check_url_accessible("https://example.com") is True
        mock_head.assert_called_once_with(
            "https://example.com",
            headers=pytest.importorskip("camplinks.http").HEADERS,
            timeout=10,
            allow_redirects=True,
        )


class TestQueryWayback:
    """Tests for query_wayback()."""

    @patch("camplinks.validate.time.sleep")
    @patch("camplinks.validate.requests.get")
    def test_returns_wayback_url_when_available(
        self, mock_get: MagicMock, mock_sleep: MagicMock
    ) -> None:
        import orjson

        mock_resp = MagicMock()
        mock_resp.content = orjson.dumps(
            {
                "archived_snapshots": {
                    "closest": {
                        "url": "https://web.archive.org/web/20240101/https://example.com",
                        "status": "200",
                        "available": True,
                        "timestamp": "20240101000000",
                    }
                }
            }
        )
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = query_wayback("https://example.com")
        assert result == "https://web.archive.org/web/20240101/https://example.com"

    @patch("camplinks.validate.time.sleep")
    @patch("camplinks.validate.requests.get")
    def test_returns_empty_when_no_snapshot(
        self, mock_get: MagicMock, mock_sleep: MagicMock
    ) -> None:
        import orjson

        mock_resp = MagicMock()
        mock_resp.content = orjson.dumps({"archived_snapshots": {}})
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        assert query_wayback("https://example.com") == ""

    @patch("camplinks.validate.time.sleep")
    @patch("camplinks.validate.requests.get")
    def test_returns_empty_on_request_error(
        self, mock_get: MagicMock, mock_sleep: MagicMock
    ) -> None:
        mock_get.side_effect = requests.ConnectionError("failed")
        assert query_wayback("https://example.com") == ""

    @patch("camplinks.validate.time.sleep")
    @patch("camplinks.validate.requests.get")
    def test_returns_empty_when_snapshot_unavailable(
        self, mock_get: MagicMock, mock_sleep: MagicMock
    ) -> None:
        import orjson

        mock_resp = MagicMock()
        mock_resp.content = orjson.dumps(
            {
                "archived_snapshots": {
                    "closest": {
                        "url": "https://web.archive.org/web/20240101/https://example.com",
                        "status": "200",
                        "available": False,
                    }
                }
            }
        )
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        assert query_wayback("https://example.com") == ""


class TestValidateCampaignSites:
    """Tests for validate_campaign_sites() orchestration."""

    @patch("camplinks.validate.check_url_accessible", return_value=True)
    @patch("camplinks.validate.save_cache")
    @patch("camplinks.validate.load_cache", return_value={})
    def test_skips_accessible_sites(
        self,
        mock_load: MagicMock,
        mock_save: MagicMock,
        mock_check: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        _seed_candidate_with_site(db)
        result = validate_campaign_sites(db)
        assert result == 0
        # No campaign_site_archived should exist
        row = db.execute(
            "SELECT COUNT(*) FROM contact_links WHERE link_type = 'campaign_site_archived'"
        ).fetchone()
        assert row[0] == 0

    @patch("camplinks.validate.query_wayback")
    @patch("camplinks.validate.check_url_accessible", return_value=False)
    @patch("camplinks.validate.save_cache")
    @patch("camplinks.validate.load_cache", return_value={})
    def test_writes_archived_url_for_inaccessible_site(
        self,
        mock_load: MagicMock,
        mock_save: MagicMock,
        mock_check: MagicMock,
        mock_wayback: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        mock_wayback.return_value = (
            "https://web.archive.org/web/20240101/https://alice.com"
        )
        cid = _seed_candidate_with_site(db)
        result = validate_campaign_sites(db)
        assert result == 1

        row = db.execute(
            "SELECT url, source FROM contact_links "
            "WHERE candidate_id = ? AND link_type = 'campaign_site_archived'",
            (cid,),
        ).fetchone()
        assert row["url"] == "https://web.archive.org/web/20240101/https://alice.com"
        assert row["source"] == "wayback"

    @patch("camplinks.validate.query_wayback", return_value="")
    @patch("camplinks.validate.check_url_accessible", return_value=False)
    @patch("camplinks.validate.save_cache")
    @patch("camplinks.validate.load_cache", return_value={})
    def test_handles_no_wayback_snapshot(
        self,
        mock_load: MagicMock,
        mock_save: MagicMock,
        mock_check: MagicMock,
        mock_wayback: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        _seed_candidate_with_site(db)
        result = validate_campaign_sites(db)
        assert result == 0
        row = db.execute(
            "SELECT COUNT(*) FROM contact_links WHERE link_type = 'campaign_site_archived'"
        ).fetchone()
        assert row[0] == 0

    @patch("camplinks.validate.check_url_accessible")
    @patch("camplinks.validate.save_cache")
    @patch("camplinks.validate.load_cache")
    def test_uses_cache_for_resumability(
        self,
        mock_load: MagicMock,
        mock_save: MagicMock,
        mock_check: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        _seed_candidate_with_site(db)
        mock_load.return_value = {
            "Republican|Ohio|5|Alice": {"status": "accessible"},
        }
        result = validate_campaign_sites(db)
        assert result == 0
        mock_check.assert_not_called()

    @patch("camplinks.validate.check_url_accessible")
    @patch("camplinks.validate.save_cache")
    @patch("camplinks.validate.load_cache", return_value={})
    def test_idempotent_skip_already_archived(
        self,
        mock_load: MagicMock,
        mock_save: MagicMock,
        mock_check: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        cid = _seed_candidate_with_site(db)
        upsert_contact_link(
            db,
            ContactLink(
                cid,
                "campaign_site_archived",
                "https://web.archive.org/web/old",
                "wayback",
            ),
        )
        db.commit()

        result = validate_campaign_sites(db)
        assert result == 0
        mock_check.assert_not_called()

    @patch("camplinks.validate.check_url_accessible", return_value=True)
    @patch("camplinks.validate.save_cache")
    @patch("camplinks.validate.load_cache", return_value={})
    def test_returns_zero_when_no_targets(
        self,
        mock_load: MagicMock,
        mock_save: MagicMock,
        mock_check: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        result = validate_campaign_sites(db)
        assert result == 0
        mock_check.assert_not_called()
