"""Run the validate stage only for candidates in bad_wikipedia_urls.csv.

Checks whether newly found campaign site URLs are accessible, and stores
a Wayback Machine archive URL for any that are not.
"""

from __future__ import annotations

import csv
import logging
import sqlite3

from tqdm import tqdm

from camplinks.cache import load_cache, make_cache_key, save_cache
from camplinks.db import open_db, upsert_contact_link
from camplinks.models import ContactLink, DB_FILENAME
from camplinks.validate import (
    VALIDATE_CACHE_FILE,
    check_url_accessible,
    query_wayback,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CSV_PATH = "bad_wikipedia_urls.csv"
SAVE_INTERVAL = 25


def load_targets(
    conn: sqlite3.Connection,
    csv_path: str,
) -> list[sqlite3.Row]:
    """Load candidates from the audit CSV that now have a campaign_site link.

    Args:
        conn: Open database connection.
        csv_path: Path to bad_wikipedia_urls.csv.

    Returns:
        List of Row objects with candidate, election, and campaign_site_url fields.
    """
    with open(csv_path, newline="") as f:
        ids = [int(r["candidate_id"]) for r in csv.DictReader(f)]

    if not ids:
        return []

    ph = ",".join("?" * len(ids))
    return conn.execute(
        f"""
        SELECT c.candidate_id, c.candidate_name, c.party,
               e.state, e.district, e.year, e.race_type, e.election_stage,
               cl.url AS campaign_site_url
        FROM candidates c
        JOIN elections e ON c.election_id = e.election_id
        JOIN contact_links cl ON cl.candidate_id = c.candidate_id
        WHERE c.candidate_id IN ({ph})
          AND cl.link_type = 'campaign_site'
          AND c.candidate_id NOT IN (
              SELECT cl2.candidate_id FROM contact_links cl2
              WHERE cl2.link_type = 'campaign_site_archived'
          )
        """,
        ids,
    ).fetchall()


def run_validate(
    conn: sqlite3.Connection,
    targets: list[sqlite3.Row],
    cache_path: str = VALIDATE_CACHE_FILE,
) -> None:
    """Validate campaign site URLs and archive inaccessible ones.

    Args:
        conn: Open database connection.
        targets: Rows from load_targets.
        cache_path: Path for the validation cache file.
    """
    if not targets:
        logger.info("No campaign sites to validate.")
        return

    logger.info("Validating %d campaign site URLs...", len(targets))
    cache = load_cache(cache_path)

    accessible_count = 0
    archived_count = 0
    inaccessible_count = 0
    processed = 0

    for row in tqdm(targets, desc="Validating URLs", unit="candidate"):
        cid: int = row["candidate_id"]
        url: str = row["campaign_site_url"]
        key = make_cache_key(
            row["party"], row["state"], row["district"] or "", row["candidate_name"]
        )

        if key in cache:
            entry = cache[key]
        else:
            accessible = check_url_accessible(url)
            if accessible:
                entry = {"status": "accessible"}
            else:
                wayback_url = query_wayback(url, row["year"])
                logger.info("Inaccessible: %s — %s", row["candidate_name"], url)
                if wayback_url:
                    logger.info("  Archived: %s", wayback_url)
                else:
                    logger.info("  No archive found.")
                entry = {"status": "inaccessible", "wayback_url": wayback_url}

            cache[key] = entry
            processed += 1
            if processed % SAVE_INTERVAL == 0:
                save_cache(cache, cache_path)

        status = entry.get("status", "inaccessible")
        if status == "accessible":
            accessible_count += 1
        else:
            inaccessible_count += 1
            wayback_url = entry.get("wayback_url", "")
            if wayback_url:
                upsert_contact_link(
                    conn,
                    ContactLink(
                        candidate_id=cid,
                        link_type="campaign_site_archived",
                        url=wayback_url,
                        source="wayback",
                    ),
                )
                archived_count += 1

    conn.commit()
    save_cache(cache, cache_path)
    logger.info(
        "Validation complete: %d accessible, %d inaccessible (%d archived).",
        accessible_count,
        inaccessible_count,
        archived_count,
    )


def main() -> None:
    """Entry point."""
    with open_db(DB_FILENAME) as conn:
        targets = load_targets(conn, CSV_PATH)
        logger.info(
            "Found %d candidates with campaign_site links to validate.", len(targets)
        )
        run_validate(conn, targets)


if __name__ == "__main__":
    main()
