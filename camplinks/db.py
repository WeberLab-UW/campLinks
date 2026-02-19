"""SQLite database layer for camplinks.

Handles schema creation, CRUD operations, and connection management.
All queries use parameterized statements to prevent SQL injection.
"""

from __future__ import annotations

import logging
import sqlite3

from camplinks.models import DB_FILENAME, Candidate, ContactLink, Election

logger = logging.getLogger(__name__)

SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS elections (
    election_id   INTEGER PRIMARY KEY,
    state         TEXT    NOT NULL,
    race_type     TEXT    NOT NULL,
    year          INTEGER NOT NULL,
    district      TEXT,
    wikipedia_url TEXT,
    UNIQUE(state, race_type, year, district)
);

CREATE TABLE IF NOT EXISTS candidates (
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

CREATE TABLE IF NOT EXISTS contact_links (
    contact_link_id INTEGER PRIMARY KEY,
    candidate_id    INTEGER NOT NULL REFERENCES candidates(candidate_id),
    link_type       TEXT    NOT NULL,
    url             TEXT    NOT NULL,
    source          TEXT    NOT NULL,
    UNIQUE(candidate_id, link_type)
);

CREATE INDEX IF NOT EXISTS idx_candidates_election
    ON candidates(election_id);
CREATE INDEX IF NOT EXISTS idx_contact_candidate
    ON contact_links(candidate_id);
CREATE INDEX IF NOT EXISTS idx_elections_lookup
    ON elections(year, race_type);
"""


def open_db(path: str = DB_FILENAME) -> sqlite3.Connection:
    """Open (or create) the SQLite database with recommended pragmas.

    Args:
        path: Filesystem path to the database file.

    Returns:
        An open sqlite3.Connection with WAL mode and foreign keys enabled.
    """
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -64000")  # 64 MB
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they do not exist.

    Args:
        conn: An open database connection.
    """
    conn.executescript(SCHEMA_SQL)
    conn.commit()


# ── Elections ──────────────────────────────────────────────────────────────


def upsert_election(conn: sqlite3.Connection, election: Election) -> int:
    """Insert an election or return the existing ID on conflict.

    Args:
        conn: Database connection.
        election: Election to insert.

    Returns:
        The election_id (new or existing).
    """
    cursor = conn.execute(
        """\
        INSERT INTO elections (state, race_type, year, district, wikipedia_url)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(state, race_type, year, district) DO UPDATE
            SET wikipedia_url = COALESCE(NULLIF(excluded.wikipedia_url, ''), wikipedia_url)
        RETURNING election_id
        """,
        (
            election.state,
            election.race_type,
            election.year,
            election.district,
            election.wikipedia_url,
        ),
    )
    row = cursor.fetchone()
    election_id: int = row[0]
    election.election_id = election_id
    return election_id


# ── Candidates ─────────────────────────────────────────────────────────────


def upsert_candidate(
    conn: sqlite3.Connection,
    candidate: Candidate,
    election_id: int,
) -> int:
    """Insert a candidate or return the existing ID on conflict.

    Args:
        conn: Database connection.
        candidate: Candidate to insert.
        election_id: Foreign key to the parent election.

    Returns:
        The candidate_id (new or existing).
    """
    cursor = conn.execute(
        """\
        INSERT INTO candidates
            (election_id, party, candidate_name, wikipedia_url,
             ballotpedia_url, vote_pct, is_winner)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(election_id, candidate_name) DO UPDATE SET
            wikipedia_url   = COALESCE(NULLIF(excluded.wikipedia_url, ''), wikipedia_url),
            ballotpedia_url = COALESCE(NULLIF(excluded.ballotpedia_url, ''), ballotpedia_url),
            vote_pct        = COALESCE(excluded.vote_pct, vote_pct),
            is_winner       = MAX(is_winner, excluded.is_winner)
        RETURNING candidate_id
        """,
        (
            election_id,
            candidate.party,
            candidate.candidate_name,
            candidate.wikipedia_url,
            candidate.ballotpedia_url,
            candidate.vote_pct,
            int(candidate.is_winner),
        ),
    )
    row = cursor.fetchone()
    candidate_id: int = row[0]
    candidate.candidate_id = candidate_id
    candidate.election_id = election_id
    return candidate_id


def get_candidates_missing_link(
    conn: sqlite3.Connection,
    link_type: str,
    year: int | None = None,
    race_type: str | None = None,
) -> list[sqlite3.Row]:
    """Find candidates that lack a specific contact link type.

    Args:
        conn: Database connection.
        link_type: The link_type value to check for (e.g. "campaign_site").
        year: Optional filter by election year.
        race_type: Optional filter by race type.

    Returns:
        List of Row objects with candidate and election fields.
    """
    query = """\
        SELECT c.candidate_id, c.candidate_name, c.party,
               c.wikipedia_url, c.ballotpedia_url,
               e.state, e.district, e.year, e.race_type
        FROM candidates c
        JOIN elections e ON c.election_id = e.election_id
        WHERE c.candidate_name != ''
          AND c.candidate_id NOT IN (
              SELECT cl.candidate_id FROM contact_links cl
              WHERE cl.link_type = ?
          )
    """
    params: list[str | int] = [link_type]

    if year is not None:
        query += " AND e.year = ?"
        params.append(year)
    if race_type is not None:
        query += " AND e.race_type = ?"
        params.append(race_type)

    return conn.execute(query, params).fetchall()


# ── Contact links ──────────────────────────────────────────────────────────


def upsert_contact_link(
    conn: sqlite3.Connection,
    link: ContactLink,
) -> None:
    """Insert a contact link, ignoring duplicates.

    Args:
        conn: Database connection.
        link: ContactLink to insert.
    """
    conn.execute(
        """\
        INSERT INTO contact_links (candidate_id, link_type, url, source)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(candidate_id, link_type) DO UPDATE
            SET url = excluded.url, source = excluded.source
        """,
        (link.candidate_id, link.link_type, link.url, link.source),
    )


def update_candidate_ballotpedia_url(
    conn: sqlite3.Connection,
    candidate_id: int,
    ballotpedia_url: str,
) -> None:
    """Set the Ballotpedia URL for a candidate.

    Args:
        conn: Database connection.
        candidate_id: The candidate to update.
        ballotpedia_url: The Ballotpedia page URL.
    """
    conn.execute(
        "UPDATE candidates SET ballotpedia_url = ? WHERE candidate_id = ?",
        (ballotpedia_url, candidate_id),
    )
