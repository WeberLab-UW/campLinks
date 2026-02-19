"""Unit tests for the convert_to_tidy migration script."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import polars as pl
import pytest

from convert_to_tidy import migrate


@pytest.fixture()
def sample_csv(tmp_path: Path) -> str:
    """Create a minimal wide-format CSV for testing."""
    csv_path = str(tmp_path / "test_races.csv")
    df = pl.DataFrame(
        {
            "State": ["Ohio", "Ohio"],
            "Race": ["US House of Representatives", "US House of Representatives"],
            "Year": [2024, 2024],
            "District": ["5", "6"],
            "Republican Candidate": ["Alice", "Carol"],
            "Republican Wiki URL": ["https://en.wikipedia.org/wiki/Alice", ""],
            "Republican Vote %": [55.0, 65.0],
            "Democrat Candidate": ["Bob", ""],
            "Democrat Wiki URL": ["", ""],
            "Democrat Vote %": [45.0, None],
            "Winner": ["Alice", "Carol"],
            "Republican Campaign Site": ["https://alice.com", "https://carol.com"],
            "Democrat Campaign Site": ["https://bob.com", ""],
            "Republican Campaign Facebook": ["https://fb.com/alice", ""],
            "Republican Campaign X": ["", ""],
            "Republican Campaign Instagram": ["", ""],
            "Republican Personal Website": ["", ""],
            "Republican Personal Facebook": ["", ""],
            "Republican Personal LinkedIn": ["", ""],
            "Democrat Campaign Facebook": ["", ""],
            "Democrat Campaign X": ["", ""],
            "Democrat Campaign Instagram": ["", ""],
            "Democrat Personal Website": ["", ""],
            "Democrat Personal Facebook": ["", ""],
            "Democrat Personal LinkedIn": ["", ""],
        }
    )
    df.write_csv(csv_path)
    return csv_path


class TestMigrate:
    """Tests for CSV-to-SQLite migration."""

    def test_election_count(self, sample_csv: str, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        migrate(sample_csv, db_path)

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM elections").fetchone()[0]
        assert count == 2
        conn.close()

    def test_candidate_count(self, sample_csv: str, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        migrate(sample_csv, db_path)

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
        # 2 Republicans + 1 Democrat (Bob); Carol's opponent is empty
        assert count == 3
        conn.close()

    def test_contact_links_created(self, sample_csv: str, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        migrate(sample_csv, db_path)

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM contact_links").fetchone()[0]
        # Alice: campaign_site + campaign_facebook = 2
        # Bob: campaign_site = 1
        # Carol: campaign_site = 1
        assert count == 4
        conn.close()

    def test_winner_flag(self, sample_csv: str, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        migrate(sample_csv, db_path)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        winners = conn.execute(
            "SELECT candidate_name FROM candidates WHERE is_winner = 1"
        ).fetchall()
        winner_names = {r["candidate_name"] for r in winners}
        assert winner_names == {"Alice", "Carol"}
        conn.close()

    def test_idempotent(self, sample_csv: str, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.db")
        migrate(sample_csv, db_path)
        migrate(sample_csv, db_path)

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM elections").fetchone()[0]
        assert count == 2
        conn.close()
