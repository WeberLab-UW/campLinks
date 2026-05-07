"""Export candidates with missing or invalid campaign_x URLs to a CSV.

Output columns: candidate_id, candidate_name, state, race_type, year, invalid_url
- invalid_url is populated for candidates with a bad URL, empty for those with none.
"""

from __future__ import annotations

import csv
import logging
from urllib.parse import urlparse

from camplinks.db import open_db
from camplinks.models import DB_FILENAME

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_PATH = "missing_x_links.csv"


def is_valid_profile(url: str) -> bool:
    """Return True if url is a valid X/Twitter profile URL."""
    try:
        p = urlparse(url)
        host = p.netloc.replace("www.", "")
        parts = [x for x in p.path.rstrip("/").split("/") if x]
        return host in ("x.com", "twitter.com") and len(parts) == 1
    except Exception:
        return False


def main() -> None:
    """Entry point."""
    with open_db(DB_FILENAME) as conn:
        invalid = conn.execute(
            """
            SELECT c.candidate_id, c.candidate_name, e.state, e.race_type, e.year, cl.url
            FROM candidates c
            JOIN elections e ON c.election_id = e.election_id
            JOIN contact_links cl ON cl.candidate_id = c.candidate_id
            WHERE cl.link_type = 'campaign_x' AND c.candidate_name != ''
            ORDER BY e.year, e.race_type, e.state
            """
        ).fetchall()
        invalid_rows = [
            (cid, name, state, rt, yr, url)
            for cid, name, state, rt, yr, url in invalid
            if not is_valid_profile(url)
        ]

        no_x = conn.execute(
            """
            SELECT c.candidate_id, c.candidate_name, e.state, e.race_type, e.year
            FROM candidates c
            JOIN elections e ON c.election_id = e.election_id
            WHERE c.candidate_name != ''
              AND c.candidate_id NOT IN (
                  SELECT candidate_id FROM contact_links WHERE link_type = 'campaign_x'
              )
            ORDER BY e.year, e.race_type, e.state
            """
        ).fetchall()

    fieldnames = ["candidate_id", "candidate_name", "state", "race_type", "year", "invalid_url"]
    rows = (
        [
            {"candidate_id": cid, "candidate_name": name, "state": state,
             "race_type": rt, "year": yr, "invalid_url": url}
            for cid, name, state, rt, yr, url in invalid_rows
        ]
        + [
            {"candidate_id": cid, "candidate_name": name, "state": state,
             "race_type": rt, "year": yr, "invalid_url": ""}
            for cid, name, state, rt, yr in no_x
        ]
    )

    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(
        "Wrote %d rows to %s (%d invalid URLs, %d missing).",
        len(rows), OUTPUT_PATH, len(invalid_rows), len(no_x),
    )


if __name__ == "__main__":
    main()
