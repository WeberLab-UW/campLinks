"""Compare candidate names in candidate_names_23_to_25.csv against camplinks.db.

Performs a case-insensitive match between the CSV `cand_name` column and the
`candidates.candidate_name` column in the database. Reports matched, unmatched,
and a summary count.

Usage:
    python check_candidate_names.py [--db camplinks.db] [--csv candidate_names_23_to_25.csv]
    python check_candidate_names.py --output results.csv
"""

from __future__ import annotations

import argparse
import logging
import sqlite3

import polars as pl
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

finished_csv = pd.read_csv("candidate_names_23_to_25.csv")
#obtained_names = [s.lower() for s in finished_csv['cand_name']]

LINK_TYPES = [
    "campaign_site",
    "campaign_facebook",
    "campaign_x",
    "campaign_instagram",
    "personal_website",
    "personal_facebook",
    "personal_linkedin",
]


def load_contact_links(db_path: str) -> dict[int, set[str]]:
    """Load all contact link types from the database, keyed by candidate_id.

    Args:
        db_path: Path to the SQLite database.

    Returns:
        Dict mapping candidate_id to a set of link_type strings present for
        that candidate.
    """
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute(
            "SELECT candidate_id, link_type FROM contact_links"
        ).fetchall()
    finally:
        con.close()

    links: dict[int, set[str]] = {}
    for candidate_id, link_type in rows:
        links.setdefault(candidate_id, set()).add(link_type)
    return links


def load_db_names(db_path: str) -> pl.DataFrame:
    """Load all candidate names from the database.

    Args:
        db_path: Path to the SQLite database.

    Returns:
        DataFrame with columns [candidate_id, candidate_name, candidate_name_lower,
        race_type, year, state, district, election_stage].
    """
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute(
            """
            SELECT
                c.candidate_id,
                c.candidate_name,
                LOWER(c.candidate_name) AS candidate_name_lower,
                e.race_type,
                e.year,
                e.state,
                e.district,
                e.election_stage
            FROM candidates c
            JOIN elections e ON c.election_id = e.election_id
            """
        ).fetchall()
        cols = [
            "candidate_id",
            "candidate_name",
            "candidate_name_lower",
            "race_type",
            "year",
            "state",
            "district",
            "election_stage",
        ]
        return pl.DataFrame(rows, schema=cols, orient="row")
    finally:
        con.close()

loaded_from_df = load_db_names('/Users/agueorg/Desktop/WeberLab/campLinks/camplinks.db')
contact_links = load_contact_links('/Users/agueorg/Desktop/WeberLab/campLinks/camplinks.db')

in_db = []
link_type_results: dict[str, list[bool]] = {lt: [] for lt in LINK_TYPES}

for i in range(0, len(finished_csv)):
    name = finished_csv['cand_name'][i].lower()
    year = finished_csv['year'][i]
    match = loaded_from_df.filter(pl.col("candidate_name_lower") == name)
    if match.is_empty():
        in_db.append('F')
        for lt in LINK_TYPES:
            link_type_results[lt].append(False)
    elif (match["year"] == year).any():
        in_db.append('T')
        matched_ids = (
            match.filter(pl.col("year") == year)["candidate_id"].to_list()
        )
        combined_links: set[str] = set().union(
            *(contact_links.get(cid, set()) for cid in matched_ids)
        )
        for lt in LINK_TYPES:
            link_type_results[lt].append(lt in combined_links)
    else:
        in_db.append('incorrect year')
        for lt in LINK_TYPES:
            link_type_results[lt].append(False)

finished_csv = finished_csv.assign(in_db=in_db)
for lt in LINK_TYPES:
    finished_csv[lt] = link_type_results[lt]

#finished_csv.to_csv('matched_data.csv')
