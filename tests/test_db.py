"""Unit tests for camplinks.db database layer."""

from __future__ import annotations

import sqlite3

import pytest

from camplinks.db import (
    get_candidates_missing_link,
    get_candidates_with_link,
    init_schema,
    migrate_schema,
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


class TestGetCandidatesWithLink:
    """Tests for finding candidates that have a specific contact link."""

    def test_finds_candidates_with_campaign_site(self, db: sqlite3.Connection) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid = upsert_election(db, e)
        c = Candidate(party="Republican", candidate_name="Alice")
        cid = upsert_candidate(db, c, eid)
        upsert_contact_link(
            db, ContactLink(cid, "campaign_site", "https://alice.com", "wikipedia")
        )
        db.commit()

        rows = get_candidates_with_link(db, "campaign_site")
        assert len(rows) == 1
        assert rows[0]["candidate_name"] == "Alice"
        assert rows[0]["campaign_site_url"] == "https://alice.com"

    def test_excludes_candidates_with_archived_link(
        self, db: sqlite3.Connection
    ) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid = upsert_election(db, e)
        c = Candidate(party="Republican", candidate_name="Alice")
        cid = upsert_candidate(db, c, eid)
        upsert_contact_link(
            db, ContactLink(cid, "campaign_site", "https://alice.com", "wikipedia")
        )
        upsert_contact_link(
            db,
            ContactLink(
                cid,
                "campaign_site_archived",
                "https://web.archive.org/web/alice.com",
                "wayback",
            ),
        )
        db.commit()

        rows = get_candidates_with_link(
            db, "campaign_site", exclude_link_type="campaign_site_archived"
        )
        assert len(rows) == 0

    def test_filter_by_year(self, db: sqlite3.Connection) -> None:
        e1 = Election(state="Ohio", race_type="US House", year=2024, district="5")
        e2 = Election(state="Ohio", race_type="US House", year=2022, district="5")
        eid1 = upsert_election(db, e1)
        eid2 = upsert_election(db, e2)

        c1 = Candidate(party="Republican", candidate_name="Alice")
        cid1 = upsert_candidate(db, c1, eid1)
        upsert_contact_link(
            db, ContactLink(cid1, "campaign_site", "https://alice.com", "wikipedia")
        )

        c2 = Candidate(party="Democratic", candidate_name="Bob")
        cid2 = upsert_candidate(db, c2, eid2)
        upsert_contact_link(
            db, ContactLink(cid2, "campaign_site", "https://bob.com", "wikipedia")
        )
        db.commit()

        rows = get_candidates_with_link(db, "campaign_site", year=2024)
        assert len(rows) == 1
        assert rows[0]["candidate_name"] == "Alice"

    def test_does_not_return_candidates_without_link(
        self, db: sqlite3.Connection
    ) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        eid = upsert_election(db, e)
        upsert_candidate(db, Candidate(party="Republican", candidate_name="Alice"), eid)
        db.commit()

        rows = get_candidates_with_link(db, "campaign_site")
        assert len(rows) == 0


class TestElectionStageUpsert:
    """Tests for election_stage field in upserts."""

    def test_default_stage_is_general(self, db: sqlite3.Connection) -> None:
        e = Election(state="Ohio", race_type="US House", year=2024, district="5")
        upsert_election(db, e)
        row = db.execute(
            "SELECT election_stage FROM elections WHERE election_id = ?",
            (e.election_id,),
        ).fetchone()
        assert row[0] == "general"

    def test_same_election_different_stages_get_different_ids(
        self, db: sqlite3.Connection
    ) -> None:
        e1 = Election(
            state="Ohio", race_type="US Senate", year=2024, election_stage="primary"
        )
        e2 = Election(
            state="Ohio", race_type="US Senate", year=2024, election_stage="general"
        )
        eid1 = upsert_election(db, e1)
        eid2 = upsert_election(db, e2)
        assert eid1 != eid2

    def test_upsert_same_stage_returns_same_id(self, db: sqlite3.Connection) -> None:
        e1 = Election(
            state="Ohio", race_type="US Senate", year=2024, election_stage="primary"
        )
        e2 = Election(
            state="Ohio", race_type="US Senate", year=2024, election_stage="primary"
        )
        eid1 = upsert_election(db, e1)
        eid2 = upsert_election(db, e2)
        assert eid1 == eid2

    def test_candidates_in_different_stages_are_independent(
        self, db: sqlite3.Connection
    ) -> None:
        e_primary = Election(
            state="Ohio", race_type="US Senate", year=2024, election_stage="primary"
        )
        e_general = Election(
            state="Ohio", race_type="US Senate", year=2024, election_stage="general"
        )
        eid_p = upsert_election(db, e_primary)
        eid_g = upsert_election(db, e_general)

        c1 = Candidate(party="Republican", candidate_name="Alice", vote_pct=60.0)
        c2 = Candidate(party="Republican", candidate_name="Alice", vote_pct=52.0)
        cid_p = upsert_candidate(db, c1, eid_p)
        cid_g = upsert_candidate(db, c2, eid_g)
        assert cid_p != cid_g


class TestMigrateSchema:
    """Tests for schema migration (adding election_stage column)."""

    def test_migrate_adds_election_stage_column(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        # Create OLD schema without election_stage
        conn.executescript("""\
            CREATE TABLE elections (
                election_id   INTEGER PRIMARY KEY,
                state         TEXT    NOT NULL,
                race_type     TEXT    NOT NULL,
                year          INTEGER NOT NULL,
                district      TEXT,
                wikipedia_url TEXT,
                UNIQUE(state, race_type, year, district)
            );
            CREATE TABLE candidates (
                candidate_id   INTEGER PRIMARY KEY,
                election_id    INTEGER NOT NULL REFERENCES elections(election_id),
                party          TEXT    NOT NULL,
                candidate_name TEXT    NOT NULL,
                wikipedia_url  TEXT,
                ballotpedia_url TEXT,
                vote_pct       REAL,
                is_winner      INTEGER NOT NULL DEFAULT 0,
                UNIQUE(election_id, candidate_name)
            );
        """)
        conn.execute(
            "INSERT INTO elections (state, race_type, year, district) "
            "VALUES ('Ohio', 'US House', 2024, '5')"
        )
        conn.execute(
            "INSERT INTO candidates (election_id, party, candidate_name) "
            "VALUES (1, 'Republican', 'Alice')"
        )
        conn.commit()

        migrate_schema(conn)

        cols = {
            row[1] for row in conn.execute("PRAGMA table_info(elections)").fetchall()
        }
        assert "election_stage" in cols

        row = conn.execute(
            "SELECT election_stage FROM elections WHERE election_id = 1"
        ).fetchone()
        assert row[0] == "general"

        # Verify FK still works
        cand = conn.execute(
            "SELECT candidate_name FROM candidates WHERE election_id = 1"
        ).fetchone()
        assert cand[0] == "Alice"

    def test_migrate_is_noop_on_new_schema(self, db: sqlite3.Connection) -> None:
        count_before = db.execute("SELECT COUNT(*) FROM elections").fetchone()[0]
        migrate_schema(db)
        count_after = db.execute("SELECT COUNT(*) FROM elections").fetchone()[0]
        assert count_before == count_after


class TestGetCandidatesMissingLinkElectionStage:
    """Tests for election_stage filter in get_candidates_missing_link."""

    def test_filter_by_election_stage(self, db: sqlite3.Connection) -> None:
        e1 = Election(
            state="Ohio", race_type="US Senate", year=2024, election_stage="primary"
        )
        e2 = Election(
            state="Ohio", race_type="US Senate", year=2024, election_stage="general"
        )
        eid1 = upsert_election(db, e1)
        eid2 = upsert_election(db, e2)
        upsert_candidate(
            db, Candidate(party="Republican", candidate_name="Alice"), eid1
        )
        upsert_candidate(db, Candidate(party="Republican", candidate_name="Bob"), eid2)
        db.commit()

        general_only = get_candidates_missing_link(
            db, "campaign_site", election_stage="general"
        )
        assert len(general_only) == 1
        assert general_only[0]["candidate_name"] == "Bob"

        primary_only = get_candidates_missing_link(
            db, "campaign_site", election_stage="primary"
        )
        assert len(primary_only) == 1
        assert primary_only[0]["candidate_name"] == "Alice"

        all_stages = get_candidates_missing_link(db, "campaign_site")
        assert len(all_stages) == 2
