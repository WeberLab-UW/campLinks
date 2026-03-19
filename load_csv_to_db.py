"""Load candidates from candidate_names_23_to_25.csv into camplinks.db.

Inserts elections, candidates, and any contact links already present in the
CSV into the database. Uses upsert semantics so the script is safe to re-run.

Usage:
    python load_csv_to_db.py
    python load_csv_to_db.py --db camplinks.db --csv candidate_names_23_to_25.csv
"""

from __future__ import annotations

import argparse
import logging
import sqlite3

import pandas as pd
from tqdm import tqdm

from camplinks.db import (
    open_db,
    upsert_candidate,
    upsert_contact_link,
    upsert_election,
)
from camplinks.models import Candidate, ContactLink, Election

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

LINK_TYPES = [
    "campaign_site",
    "campaign_facebook",
    "campaign_x",
    "campaign_instagram",
    "personal_website",
    "personal_facebook",
    "personal_linkedin",
]

_STATEWIDE_RACE_TYPES: frozenset[str] = frozenset({
    "US Senate",
    "Governor",
    "Attorney General",
    "Mayor",
})

_STATE_ABBREV_TO_NAME: dict[str, str] = {
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


def _candidate_exists(conn: sqlite3.Connection, name: str, race_type: str, year: int) -> bool:
    """Return True if a candidate with this name, race_type, and year is already in the DB.

    Args:
        conn: Database connection.
        name: Candidate name (case-insensitive match).
        race_type: Race type string.
        year: Election year.

    Returns:
        True if a matching candidate exists.
    """
    row = conn.execute(
        """
        SELECT 1 FROM candidates c
        JOIN elections e ON c.election_id = e.election_id
        WHERE LOWER(c.candidate_name) = LOWER(?)
          AND e.race_type = ?
          AND e.year = ?
        LIMIT 1
        """,
        (name, race_type, year),
    ).fetchone()
    return row is not None


def _update_special_election(
    conn: sqlite3.Connection,
    state: str,
    race_type: str,
    year: int,
    district: str,
    special_election: bool,
) -> None:
    """Update the special_election flag on a matching election row.

    Args:
        conn: Database connection.
        state: Full state name.
        race_type: Race type string.
        year: Election year.
        district: District string (empty for statewide).
        special_election: Whether this is a special election.
    """
    conn.execute(
        """
        UPDATE elections
        SET special_election = MAX(special_election, ?)
        WHERE state = ? AND race_type = ? AND year = ? AND district = ?
        """,
        (int(special_election), state, race_type, year, district),
    )


def _parse_district(raw: object, race_type: str) -> str:
    """Return a normalized district string.

    Args:
        raw: Raw district value from the CSV cell.
        race_type: DB race_type for this election.

    Returns:
        Empty string for statewide races or missing values; otherwise the
        district as a string (numeric values are int-truncated).
    """
    if race_type in _STATEWIDE_RACE_TYPES:
        return ""
    if pd.isna(raw):
        return ""
    if isinstance(raw, str):
        return raw
    return str(int(float(raw)))


def _parse_vote_pct(raw: object) -> float | None:
    """Parse percentage_votes, returning None for non-numeric values.

    Args:
        raw: Raw cell value from the CSV.

    Returns:
        Float vote percentage, or None if the value is missing/unknown.
    """
    result = pd.to_numeric(raw, errors="coerce")
    return None if pd.isna(result) else float(result)


def load(db_path: str, csv_path: str) -> None:
    """Load all candidates from the CSV into the database.

    Args:
        db_path: Path to the SQLite database file.
        csv_path: Path to the candidates CSV file.
    """
    df = pd.read_csv(csv_path)
    logger.info("Loaded %d rows from %s", len(df), csv_path)
    print(df.columns)

    conn = open_db(db_path)
    total_already_in_db = 0
    inserted = 0
    try:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Importing candidates", unit="candidate"):
            name: str = str(row["cand_name"]).title() #change from full upper case to Title Case
            state_abbrev: str = str(row["state"]) 
            state: str = _STATE_ABBREV_TO_NAME.get(state_abbrev, state_abbrev) #in database the state is full name, not abbreviation. this changes it
            race_type: str = str(row["election_type"]) #get race_type
            year: int = int(row["year"]) #get year
            party: str = str(row["party"]).title() #get party of candidate - for some reason this csv just is reading Party with a capital P. May need to be changed for other csvs
            district: str = _parse_district(row["district"], race_type) #get district. if state wide, district = state
            #removed percentage vote when doing state house and state senate from follow the money bc they dont have that data
            vote_pct: float | None = _parse_vote_pct(row.get("percentage_votes")) #get percentage_votes
            outcome: str = str(row.get("race_outcome", "")).strip().lower()
            is_winner: str = "won" if outcome == "won" else "lost" if outcome == "lost" else "unknown"
            special_election: bool = str(row.get("special_election", "")).strip().upper() == "TRUE"

            if _candidate_exists(conn, name, race_type, year):
                print(name, race_type, year, 'is already in database')
                total_already_in_db += 1
                print(total_already_in_db)
                _update_special_election(conn, state, race_type, year, district, special_election)
                conn.commit()
                continue

            election_id = upsert_election(
                conn,
                Election(
                    state=state,
                    race_type=race_type,
                    year=year,
                    district=district,
                    election_stage="general",
                    special_election=special_election,
                ),
            )
            candidate_id = upsert_candidate(
                conn,
                Candidate(
                    party=party,
                    candidate_name=name,
                    vote_pct=vote_pct,
                    is_winner=is_winner,
                ),
                election_id,
            )

            for link_type in LINK_TYPES:
                url = row.get(link_type)
                if pd.notna(url) and str(url).strip():
                    upsert_contact_link(
                        conn,
                        ContactLink(
                            candidate_id=candidate_id,
                            link_type=link_type,
                            url=str(url).strip(),
                            source="csv_import",
                        ),
                    )

            inserted += 1
            if inserted % 100 == 0:
                conn.commit()

            conn.commit() #commit to database with every insert
        logger.info("Done. Inserted/updated %d candidates.", inserted)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="camplinks.db", help="Path to SQLite database")
    parser.add_argument(
        "--csv",
        default="candidate_names_23_to_25.csv",
        help="Path to candidates CSV",
    )
    args = parser.parse_args()
    load(args.db, args.csv)
