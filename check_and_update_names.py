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

import pandas as pd
import polars as pl
from tqdm import tqdm

from camplinks.db import (
    open_db,
    upsert_candidate,
    upsert_contact_link,
    upsert_election,
    update_candidate_ballotpedia_url,
)
from camplinks.models import BALLOTPEDIA_LABEL_MAP, Candidate, ContactLink, Election
from camplinks.search import find_candidate_info

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


def load_contact_links(db_path: str) -> dict[int, dict[str, str]]:
    """Load all contact links from the database, keyed by candidate_id.

    Args:
        db_path: Path to the SQLite database.

    Returns:
        Dict mapping candidate_id to a dict of {link_type: url} for that
        candidate.
    """
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute(
            "SELECT candidate_id, link_type, url FROM contact_links"
        ).fetchall()
    finally:
        con.close()

    links: dict[int, dict[str, str]] = {}
    for candidate_id, link_type, url in rows:
        links.setdefault(candidate_id, {})[link_type] = url
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
link_type_results: dict[str, list[str | None]] = {lt: [] for lt in LINK_TYPES}

for i in range(0, len(finished_csv)):
    name = finished_csv['cand_name'][i].lower()
    year = finished_csv['year'][i]
    match = loaded_from_df.filter(pl.col("candidate_name_lower") == name)
    if match.is_empty():
        in_db.append(False)
        for lt in LINK_TYPES:
            link_type_results[lt].append(None)
    elif (match["year"] == year).any():
        in_db.append(True)
        matched_ids = (
            match.filter(pl.col("year") == year)["candidate_id"].to_list()
        )
        combined_links: dict[str, str] = {}
        for cid in matched_ids:
            combined_links.update(contact_links.get(cid, {}))
        for lt in LINK_TYPES:
            link_type_results[lt].append(combined_links.get(lt))
    else:
        in_db.append(False)
        for lt in LINK_TYPES:
            link_type_results[lt].append(None)

finished_csv = finished_csv.assign(in_db=in_db)
for lt in LINK_TYPES:
    finished_csv[lt] = link_type_results[lt]

#names in CSV are different from database, need to be edited 
_ELECTION_TYPE_TO_KEYWORD: dict[str, str] = {
    "federal:house": "congress",
    "federal:senate": "senate",
    "state:governor": "governor",
    "state:attorney_general": "attorney general",
    "state:house": "state representative",
    "state:senate": "state senator",
    "municipal:mayor": "mayor",
}

_ELECTION_TYPE_TO_RACE_TYPE: dict[str, str] = {
    "federal:house": "US House",
    "federal:senate": "US Senate",
    "state:governor": "Governor",
    "state:attorney_general": "Attorney General",
    "state:house": "State House",
    "state:senate": "State Senate",
    "municipal:mayor": "Mayor",
}

_STATEWIDE_ELECTION_TYPES: frozenset[str] = frozenset({
    "federal:senate", "state:governor", "state:attorney_general", "municipal:mayor",
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

DB_PATH = "/Users/agueorg/Desktop/WeberLab/campLinks/camplinks.db"

not_in_db_mask = finished_csv["in_db"] == False  # noqa: E712
not_in_db_indices = finished_csv[not_in_db_mask].index #get indices of the names that are not in the db

conn = open_db(DB_PATH)
try:
    for idx in tqdm(not_in_db_indices, desc="Searching candidates not in DB", unit="candidate"): #cycle through the names we dont have
        row = finished_csv.loc[idx]
        name: str = str(row["cand_name"]).title() #make in more searchable format (e.g. Anna Gueorguieva; rather than all caps)
        print('looking for', name)
        state_abbrev: str = str(row["state"])
        state: str = _STATE_ABBREV_TO_NAME.get(state_abbrev, state_abbrev) #mapping CSV names onto DB names
        election_type: str = str(row.get("election_type", ""))  #mapping CSV names onto DB names
        keyword: str = _ELECTION_TYPE_TO_KEYWORD.get(election_type, "election")  #mapping CSV names onto DB names
        race_type: str = _ELECTION_TYPE_TO_RACE_TYPE.get(election_type, election_type)  #mapping CSV names onto DB names
        year: int = int(row["year"])
        party: str = str(row["party"]).title()
        vote_pct: float | None = pd.to_numeric(row.get("percentage_votes"), errors="coerce")
        if pd.isna(vote_pct):
            vote_pct = None
        is_winner: bool = str(row.get("race_outcome", "")).strip().lower() == "won"

        if election_type in _STATEWIDE_ELECTION_TYPES:
            district: str = ""
        else:
            raw_dist = row["district"]
            if pd.isna(raw_dist):
                district = ""
            elif isinstance(raw_dist, str): #this is because some districts are just the entire state so its already a string
                district = raw_dist
            else:
                district = str(int(float(raw_dist)))

        try:
            contacts = find_candidate_info(name, state, district, keyword)
        except Exception as exc:
            logger.error("Search failed for %s: %s", name, exc)
            contacts = {}

        bp_url: str = contacts.pop("_ballotpedia_url", "")
        source: str = "ballotpedia" if bp_url else "web_search"

        #update database when we find something
        election_id = upsert_election(
            conn,
            Election(
                state=state,
                race_type=race_type,
                year=year,
                district=district,
                election_stage="general",
            ),
        )
        candidate_id = upsert_candidate(
            conn,
            Candidate(
                party=party,
                candidate_name=name,
                ballotpedia_url=bp_url,
                vote_pct=vote_pct,
                is_winner=is_winner,
            ),
            election_id,
        )
        if bp_url:
            update_candidate_ballotpedia_url(conn, candidate_id, bp_url)

        for label, url in contacts.items():
            link_type = BALLOTPEDIA_LABEL_MAP.get(label)
            if link_type and link_type in LINK_TYPES:
                upsert_contact_link(
                    conn,
                    ContactLink(
                        candidate_id=candidate_id,
                        link_type=link_type,
                        url=url,
                        source=source,
                    ),
                )
                finished_csv.at[idx, link_type] = url

        conn.commit()
finally:
    conn.close()
 

finished_csv.to_csv("candidate_names_23_to_25.csv", index=False)
