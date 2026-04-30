"""Add total_funding column to candidates table from state_leg_names_23_24.csv.

Matches on normalized candidate_name, state, and year.
"""

from __future__ import annotations

import csv
import logging
import sqlite3

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CSV_PATH = "state_leg_names_23_24.csv"
DB_PATH = "camplinks.db"

STATE_ABBREV: dict[str, str] = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}


def load_csv(path: str) -> dict[tuple[str, str, int], float]:
    """Load CSV into a lookup keyed by (normalized_name, full_state, year).

    Args:
        path: Path to state_leg_names_23_24.csv.

    Returns:
        Dict mapping (name, state, year) to Total_$ value.
    """
    lookup: dict[tuple[str, str, int], float] = {}
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("cand_name", "").strip().lower()
            abbrev = row.get("state", "").strip().upper()
            full_state = STATE_ABBREV.get(abbrev, "")
            try:
                year = int(row.get("year", "0").strip())
            except ValueError:
                continue
            raw_total = row.get("Total_$", "").strip().replace(",", "")
            try:
                total = float(raw_total)
            except ValueError:
                continue
            if name and full_state and year:
                key = (name, full_state, year)
                # Keep first occurrence if duplicates exist
                lookup.setdefault(key, total)
    logger.info("Loaded %d CSV entries", len(lookup))
    return lookup


def add_column(conn: sqlite3.Connection) -> None:
    """Add total_$ column to candidates table if not present.

    Args:
        conn: Open SQLite connection.
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(candidates)")}
    if "total_funding" not in cols:
        conn.execute("ALTER TABLE candidates ADD COLUMN total_funding REAL")
        conn.commit()
        logger.info("Added total_funding column to candidates")
    else:
        logger.info("total_funding column already exists")


def update_candidates(
    conn: sqlite3.Connection,
    lookup: dict[tuple[str, str, int], float],
) -> None:
    """Match candidates to CSV by name/state/year and write total_$.

    Args:
        conn: Open SQLite connection.
        lookup: CSV lookup keyed by (normalized_name, full_state, year).
    """
    rows = conn.execute(
        """
        SELECT c.candidate_id, c.candidate_name, e.state, e.year
        FROM candidates c
        JOIN elections e ON c.election_id = e.election_id
        WHERE e.race_type IN ('State House', 'State Senate')
          AND e.year IN (2023, 2024)
        """
    ).fetchall()

    updates: list[tuple[float, int]] = []
    matched = 0
    unmatched = 0

    for candidate_id, candidate_name, state, year in rows:
        key = (candidate_name.strip().lower(), state, year)
        total = lookup.get(key)
        if total is not None:
            updates.append((total, candidate_id))
            matched += 1
        else:
            unmatched += 1

    conn.executemany(
        "UPDATE candidates SET total_funding = ? WHERE candidate_id = ?",
        updates,
    )
    conn.commit()
    logger.info("Matched and updated %d candidates", matched)
    logger.info("No match found for %d candidates", unmatched)


def main() -> None:
    """Entry point."""
    lookup = load_csv(CSV_PATH)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        add_column(conn)
        update_candidates(conn, lookup)


if __name__ == "__main__":
    main()
