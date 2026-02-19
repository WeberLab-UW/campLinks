"""Unit tests for camplinks.db database layer."""

from __future__ import annotations

import sqlite3

import pytest

from camplinks.db import (
    get_candidates_missing_link,
    init_schema,
    upsert_candidate,
    upsert_contact_link,
    upsert_election,
)
from camplinks.models import Candidate, ContactLink, Election


@pytest.fixture()
def db() -> sqlite3.Connection:
    """Create an in-memory database with schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    init_schema(conn)
    return conn


class TestSchema:
    """Tests for database schema creation."""

    def test_tables_created(self, db: sqlite3.Connection) -> None:
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "elections" in tables
        assert "candidates" in tables
        assert "contact_links" in tables

    def test_foreign_keys_enabled(self, db: sqlite3.Connection) -> None:
        result = db.execute("PRAGMA foreign_keys").fetchone()
        assert result[0] == 1

    def test_idempotent_schema_init(self, db: sqlite3.Connection) -> None:
        init_schema(db)
        init_schema(db)
        count = db.execute("SELECT COUNT(*) FROM elections").fetchone()[0]
        assert count == 0


class TestUpsertElection:
    """Tests for election insertion."""

    def test_insert_returns_id(self, db: sqlite3.Connection) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid = upsert_election(db, e)
        assert eid >= 1
        assert e.election_id == eid

    def test_upsert_same_election_returns_same_id(self, db: sqlite3.Connection) -> None:
        e1 = Election(state="Ohio", race_type="US House", year=2024, district="5")
        e2 = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid1 = upsert_election(db, e1)
        eid2 = upsert_election(db, e2)
        assert eid1 == eid2

    def test_different_districts_get_different_ids(
        self, db: sqlite3.Connection
    ) -> None:
        e1 = Election(state="Ohio", race_type="US House", year=2024, district="5")
        e2 = Election(state="Ohio", race_type="US House", year=2024, district="6")
        eid1 = upsert_election(db, e1)
        eid2 = upsert_election(db, e2)
        assert eid1 != eid2

    def test_statewide_race_null_district(self, db: sqlite3.Connection) -> None:
        e = Election(state="Ohio", race_type="US Senate", year=2024, district=None)
        eid = upsert_election(db, e)
        assert eid >= 1


class TestUpsertCandidate:
    """Tests for candidate insertion."""

    def test_insert_candidate(self, db: sqlite3.Connection) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid = upsert_election(db, e)

        c = Candidate(
            party="Republican", candidate_name="Alice", vote_pct=55.0, is_winner=True
        )
        cid = upsert_candidate(db, c, eid)
        assert cid >= 1
        assert c.candidate_id == cid

    def test_upsert_preserves_wiki_url(self, db: sqlite3.Connection) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid = upsert_election(db, e)

        c1 = Candidate(
            party="Republican",
            candidate_name="Alice",
            wikipedia_url="https://en.wikipedia.org/wiki/Alice",
        )
        upsert_candidate(db, c1, eid)

        c2 = Candidate(party="Republican", candidate_name="Alice", wikipedia_url="")
        upsert_candidate(db, c2, eid)

        row = db.execute(
            "SELECT wikipedia_url FROM candidates WHERE candidate_name='Alice'"
        ).fetchone()
        assert row[0] == "https://en.wikipedia.org/wiki/Alice"

    def test_foreign_key_constraint(self, db: sqlite3.Connection) -> None:
        c = Candidate(party="Republican", candidate_name="Alice")
        with pytest.raises(sqlite3.IntegrityError):
            upsert_candidate(db, c, 9999)


class TestUpsertContactLink:
    """Tests for contact link insertion."""

    def test_insert_link(self, db: sqlite3.Connection) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid = upsert_election(db, e)
        c = Candidate(party="Republican", candidate_name="Alice")
        cid = upsert_candidate(db, c, eid)

        link = ContactLink(
            candidate_id=cid,
            link_type="campaign_site",
            url="https://alice.com",
            source="wikipedia",
        )
        upsert_contact_link(db, link)

        row = db.execute(
            "SELECT url FROM contact_links WHERE candidate_id=? AND link_type=?",
            (cid, "campaign_site"),
        ).fetchone()
        assert row[0] == "https://alice.com"

    def test_upsert_updates_url(self, db: sqlite3.Connection) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid = upsert_election(db, e)
        c = Candidate(party="Republican", candidate_name="Alice")
        cid = upsert_candidate(db, c, eid)

        link1 = ContactLink(cid, "campaign_site", "https://old.com", "wikipedia")
        upsert_contact_link(db, link1)

        link2 = ContactLink(cid, "campaign_site", "https://new.com", "ballotpedia")
        upsert_contact_link(db, link2)

        row = db.execute(
            "SELECT url, source FROM contact_links WHERE candidate_id=? AND link_type=?",
            (cid, "campaign_site"),
        ).fetchone()
        assert row[0] == "https://new.com"
        assert row[1] == "ballotpedia"


class TestGetCandidatesMissingLink:
    """Tests for finding candidates without specific contact links."""

    def test_finds_candidates_without_campaign_site(
        self, db: sqlite3.Connection
    ) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid = upsert_election(db, e)

        c1 = Candidate(party="Republican", candidate_name="Alice")
        cid1 = upsert_candidate(db, c1, eid)

        c2 = Candidate(party="Democratic", candidate_name="Bob")
        upsert_candidate(db, c2, eid)

        # Give Alice a campaign site
        upsert_contact_link(
            db, ContactLink(cid1, "campaign_site", "https://alice.com", "wikipedia")
        )
        db.commit()

        missing = get_candidates_missing_link(db, "campaign_site")
        assert len(missing) == 1
        assert missing[0]["candidate_name"] == "Bob"

    def test_filter_by_year(self, db: sqlite3.Connection) -> None:
        e1 = Election(state="Ohio", race_type="US House", year=2024, district="5")
        e2 = Election(state="Ohio", race_type="US House", year=2022, district="5")
        eid1 = upsert_election(db, e1)
        eid2 = upsert_election(db, e2)

        upsert_candidate(
            db, Candidate(party="Republican", candidate_name="Alice"), eid1
        )
        upsert_candidate(db, Candidate(party="Republican", candidate_name="Bob"), eid2)
        db.commit()

        missing_2024 = get_candidates_missing_link(db, "campaign_site", year=2024)
        assert len(missing_2024) == 1
        assert missing_2024[0]["candidate_name"] == "Alice"

    def test_skips_empty_names(self, db: sqlite3.Connection) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid = upsert_election(db, e)
        upsert_candidate(db, Candidate(party="Republican", candidate_name=""), eid)
        db.commit()

        missing = get_candidates_missing_link(db, "campaign_site")
        assert len(missing) == 0
