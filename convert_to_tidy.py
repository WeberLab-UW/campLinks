"""Migrate existing house_races_2024.csv into the camplinks SQLite database.

Reads the 25-column wide-format CSV and inserts normalized data into
the elections, candidates, and contact_links tables. Safe to re-run
(uses upsert semantics).

Usage::

    python convert_to_tidy.py
    python convert_to_tidy.py --csv house_races_2024.csv --db camplinks.db
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
from camplinks.models import (
    ContactLink,
    DB_FILENAME,
    Candidate,
    Election,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Maps old CSV column suffixes to link_type values
_CONTACT_SUFFIX_MAP: dict[str, str] = {
    "Campaign Site": "campaign_site",
    "Campaign Facebook": "campaign_facebook",
    "Campaign X": "campaign_x",
    "Campaign Instagram": "campaign_instagram",
    "Personal Website": "personal_website",
    "Personal Facebook": "personal_facebook",
    "Personal LinkedIn": "personal_linkedin",
}


def migrate(csv_path: str, db_path: str) -> None:
    """Read the wide CSV and populate the SQLite database.

    Args:
        csv_path: Path to house_races_2024.csv.
        db_path: Path to the SQLite database file.
    """
    df = pl.read_csv(csv_path, schema_overrides={"District": pl.Utf8})
    logger.info("Loaded %d rows from %s", len(df), csv_path)

    conn = open_db(db_path)
    init_schema(conn)

    elections_created = 0
    candidates_created = 0
    links_created = 0

    for idx in range(len(df)):
        row = df.row(idx, named=True)

        state = row.get("State", "")
        year = row.get("Year", 2024)
        district = row.get("District", "")

        # Insert election
        election = Election(
            state=state,
            race_type="US House",
            year=year,
            district=district,
        )
        eid = upsert_election(conn, election)
        elections_created += 1

        # Process each party
        for party in ("Republican", "Democrat"):
            name = row.get(f"{party} Candidate", "") or ""
            if not name.strip():
                continue

            wiki_url = row.get(f"{party} Wiki URL", "") or ""
            vote_pct_raw = row.get(f"{party} Vote %", None)
            vote_pct: float | None = None
            if vote_pct_raw is not None:
                try:
                    vote_pct = float(vote_pct_raw)
                except (ValueError, TypeError):
                    pass

            winner = row.get("Winner", "") or ""
            is_winner = name.strip() == winner.strip() and winner.strip() != ""

            candidate = Candidate(
                party=party if party != "Democrat" else "Democratic",
                candidate_name=name.strip(),
                wikipedia_url=wiki_url,
                vote_pct=vote_pct,
                is_winner=is_winner,
            )
            cid = upsert_candidate(conn, candidate, eid)
            candidates_created += 1

            # Insert contact links
            for suffix, link_type in _CONTACT_SUFFIX_MAP.items():
                col_name = f"{party} {suffix}"
                url = row.get(col_name, "") or ""
                if url.strip():
                    upsert_contact_link(
                        conn,
                        ContactLink(
                            candidate_id=cid,
                            link_type=link_type,
                            url=url.strip(),
                            source="wikipedia"
                            if link_type == "campaign_site"
                            else "ballotpedia",
                        ),
                    )
                    links_created += 1

    conn.commit()
    conn.close()

    logger.info(
        "Migration complete: %d elections, %d candidates, %d contact links.",
        elections_created,
        candidates_created,
        links_created,
    )


def main() -> None:
    """Parse CLI arguments and run the migration."""
    parser = argparse.ArgumentParser(
        description="Migrate house_races_2024.csv into camplinks SQLite database.",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="house_races_2024.csv",
        help="Path to the wide-format CSV (default: house_races_2024.csv)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=DB_FILENAME,
        help=f"SQLite database path (default: {DB_FILENAME})",
    )
    args = parser.parse_args()
    migrate(args.csv, args.db)


if __name__ == "__main__":
    main()
