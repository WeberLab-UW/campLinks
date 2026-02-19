"""Migrate a legacy wide-format CSV into the normalized camplinks SQLite database.

Usage:
    python convert_to_tidy.py --csv house_races_2024.csv [--db camplinks.db]
"""

from __future__ import annotations

import argparse
import logging

import polars as pl

from camplinks.db import (
    init_schema,
    open_db,
    upsert_candidate,
    upsert_contact_link,
    upsert_election,
)
from camplinks.models import DB_FILENAME, Candidate, ContactLink, Election

logger = logging.getLogger(__name__)

# Maps CSV column suffixes to (link_type, source) pairs
_LINK_COLUMNS: dict[str, tuple[str, str]] = {
    "Campaign Site": ("campaign_site", "csv_import"),
    "Campaign Facebook": ("campaign_facebook", "csv_import"),
    "Campaign X": ("campaign_x", "csv_import"),
    "Campaign Instagram": ("campaign_instagram", "csv_import"),
    "Personal Website": ("personal_website", "csv_import"),
    "Personal Facebook": ("personal_facebook", "csv_import"),
    "Personal LinkedIn": ("personal_linkedin", "csv_import"),
}

_PARTIES = ("Republican", "Democrat")


def migrate(csv_path: str, db_path: str = DB_FILENAME) -> None:
    """Read a wide-format CSV and upsert rows into the normalized database.

    Args:
        csv_path: Path to the legacy wide-format CSV file.
        db_path: Path to the SQLite database (created if missing).
    """
    df = pl.read_csv(csv_path, schema_overrides={"District": pl.Utf8})
    conn = open_db(db_path)
    try:
        init_schema(conn)

        for row in df.iter_rows(named=True):
            election = Election(
                state=row["State"],
                race_type=row["Race"],
                year=int(row["Year"]),
                district=row.get("District"),
            )
            election_id = upsert_election(conn, election)

            winner_name: str = row.get("Winner", "") or ""

            for party in _PARTIES:
                name: str = row.get(f"{party} Candidate", "") or ""
                if not name:
                    continue

                candidate = Candidate(
                    party=party,
                    candidate_name=name,
                    wikipedia_url=row.get(f"{party} Wiki URL", "") or "",
                    vote_pct=row.get(f"{party} Vote %"),
                    is_winner=name == winner_name,
                )
                candidate_id = upsert_candidate(conn, candidate, election_id)

                for suffix, (link_type, source) in _LINK_COLUMNS.items():
                    url: str = row.get(f"{party} {suffix}", "") or ""
                    if url:
                        upsert_contact_link(
                            conn,
                            ContactLink(
                                candidate_id=candidate_id,
                                link_type=link_type,
                                url=url,
                                source=source,
                            ),
                        )

        conn.commit()
    finally:
        conn.close()
    logger.info("Migration complete: %s -> %s", csv_path, db_path)


def main() -> None:
    """CLI entry point for the migration script."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description="Migrate legacy CSV to SQLite")
    parser.add_argument("--csv", required=True, help="Path to the wide-format CSV")
    parser.add_argument("--db", default=DB_FILENAME, help="SQLite database path")
    args = parser.parse_args()
    migrate(args.csv, args.db)


if __name__ == "__main__":
    main()
