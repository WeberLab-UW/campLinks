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
    election_id    INTEGER PRIMARY KEY,
    state          TEXT    NOT NULL,
    race_type      TEXT    NOT NULL,
    year           INTEGER NOT NULL,
    district       TEXT,
    election_stage TEXT    NOT NULL DEFAULT 'general',
    wikipedia_url  TEXT,
    UNIQUE(state, race_type, year, district, election_stage)
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
CREATE INDEX IF NOT EXISTS idx_elections_stage
    ON elections(year, race_type, election_stage);
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


def migrate_schema(conn: sqlite3.Connection) -> None:
    """Migrate the elections table to include election_stage if needed.

    Detects whether the old schema (without election_stage) is in place
    and migrates by recreating the table with the new UNIQUE constraint.
    Existing rows get election_stage='general'.

    Args:
        conn: An open database connection.
    """
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "elections" not in tables:
        return

    cols = {row[1] for row in conn.execute("PRAGMA table_info(elections)").fetchall()}
    if "election_stage" in cols:
        return

    logger.info("Migrating elections table to add election_stage column...")

    # Merge duplicate statewide elections caused by SQLite treating NULLs
    # as distinct in the old UNIQUE(state, race_type, year, district).
    dupes = conn.execute(
        """\
        SELECT state, race_type, year, COALESCE(district, '') AS d
        FROM elections
        GROUP BY state, race_type, year, COALESCE(district, '')
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    for dupe in dupes:
        # Keep the election with the most candidates
        eids = conn.execute(
            """\
            SELECT e.election_id, COUNT(c.candidate_id) AS cnt
            FROM elections e
            LEFT JOIN candidates c ON c.election_id = e.election_id
            WHERE e.state = ? AND e.race_type = ? AND e.year = ?
              AND COALESCE(e.district, '') = ?
            GROUP BY e.election_id
            ORDER BY cnt DESC
            """,
            (dupe[0], dupe[1], dupe[2], dupe[3]),
        ).fetchall()
        if len(eids) <= 1:
            continue
        keep_id = eids[0][0]
        for eid_row in eids[1:]:
            drop_id = eid_row[0]
            conn.execute(
                "UPDATE candidates SET election_id = ? WHERE election_id = ?",
                (keep_id, drop_id),
            )
            conn.execute("DELETE FROM elections WHERE election_id = ?", (drop_id,))
        logger.info(
            "Merged %d duplicate elections for %s %s %d into eid=%d.",
            len(eids) - 1,
            dupe[0],
            dupe[1],
            dupe[2],
            keep_id,
        )
    conn.commit()

    conn.execute("PRAGMA foreign_keys = OFF")
    conn.executescript("""\
        CREATE TABLE elections_new (
            election_id    INTEGER PRIMARY KEY,
            state          TEXT    NOT NULL,
            race_type      TEXT    NOT NULL,
            year           INTEGER NOT NULL,
            district       TEXT,
            election_stage TEXT    NOT NULL DEFAULT 'general',
            wikipedia_url  TEXT,
            UNIQUE(state, race_type, year, district, election_stage)
        );

        INSERT INTO elections_new
            (election_id, state, race_type, year, district, election_stage, wikipedia_url)
        SELECT election_id, state, race_type, year, COALESCE(district, ''), 'general', wikipedia_url
        FROM elections;

        DROP TABLE elections;
        ALTER TABLE elections_new RENAME TO elections;

        CREATE INDEX IF NOT EXISTS idx_candidates_election
            ON candidates(election_id);
        CREATE INDEX IF NOT EXISTS idx_elections_lookup
            ON elections(year, race_type);
        CREATE INDEX IF NOT EXISTS idx_elections_stage
            ON elections(year, race_type, election_stage);
    """)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM elections").fetchone()[0]
    logger.info("Migration complete: %d elections set to 'general'.", count)


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
        INSERT INTO elections (state, race_type, year, district, election_stage, wikipedia_url)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(state, race_type, year, district, election_stage) DO UPDATE
            SET wikipedia_url = COALESCE(NULLIF(excluded.wikipedia_url, ''), wikipedia_url)
        RETURNING election_id
        """,
        (
            election.state,
            election.race_type,
            election.year,
            election.district or "",
            election.election_stage,
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
    election_stage: str | None = None,
) -> list[sqlite3.Row]:
    """Find candidates that lack a specific contact link type.

    Args:
        conn: Database connection.
        link_type: The link_type value to check for (e.g. "campaign_site").
        year: Optional filter by election year.
        race_type: Optional filter by race type.
        election_stage: Optional filter by election stage.

    Returns:
        List of Row objects with candidate and election fields.
    """
    query = """\
        SELECT c.candidate_id, c.candidate_name, c.party,
               c.wikipedia_url, c.ballotpedia_url,
               e.state, e.district, e.year, e.race_type, e.election_stage
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
    if election_stage is not None:
        query += " AND e.election_stage = ?"
        params.append(election_stage)

    return conn.execute(query, params).fetchall()


def get_candidates_with_link(
    conn: sqlite3.Connection,
    link_type: str,
    exclude_link_type: str | None = None,
    year: int | None = None,
    race_type: str | None = None,
    election_stage: str | None = None,
) -> list[sqlite3.Row]:
    """Find candidates that have a specific contact link type.

    Optionally excludes candidates that already have a second link type,
    enabling idempotent validation (skip already-validated candidates).

    Args:
        conn: Database connection.
        link_type: The link_type to require (e.g. "campaign_site").
        exclude_link_type: Optional link_type to exclude candidates who
            already have it (e.g. "campaign_site_archived").
        year: Optional filter by election year.
        race_type: Optional filter by race type.
        election_stage: Optional filter by election stage.

    Returns:
        List of Row objects with candidate, election, and link fields.
    """
    query = """\
        SELECT c.candidate_id, c.candidate_name, c.party,
               e.state, e.district, e.year, e.race_type, e.election_stage,
               cl.url AS campaign_site_url
        FROM candidates c
        JOIN elections e ON c.election_id = e.election_id
        JOIN contact_links cl ON cl.candidate_id = c.candidate_id
        WHERE c.candidate_name != ''
          AND cl.link_type = ?
    """
    params: list[str | int] = [link_type]

    if exclude_link_type is not None:
        query += """\
          AND c.candidate_id NOT IN (
              SELECT cl2.candidate_id FROM contact_links cl2
              WHERE cl2.link_type = ?
          )
        """
        params.append(exclude_link_type)

    if year is not None:
        query += " AND e.year = ?"
        params.append(year)
    if race_type is not None:
        query += " AND e.race_type = ?"
        params.append(race_type)
    if election_stage is not None:
        query += " AND e.election_stage = ?"
        params.append(election_stage)

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
