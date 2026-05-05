"""Remove campaign_site_content and campaign_site links for no_candidate rows in validated.csv.

For every candidate in validated.csv with verdict='no_candidate':
  - Deletes all rows from campaign_site_content
  - Deletes their campaign_site row from contact_links
"""

from __future__ import annotations

import csv
import logging

from camplinks.db import open_db
from camplinks.models import DB_FILENAME

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CSV_PATH = "validated.csv"


def main() -> None:
    """Entry point."""
    with open(CSV_PATH, newline="") as f:
        ids = [
            int(r["candidate_id"])
            for r in csv.DictReader(f)
            if r.get("verdict") == "wayback_recovered"
        ]

    logger.info("Found %d no_candidate rows in validated.csv.", len(ids))

    with open_db(DB_FILENAME) as conn:
        ph = ",".join("?" * len(ids))

        content_deleted = conn.execute(
            f"DELETE FROM campaign_site_content WHERE candidate_id IN ({ph})",
            ids,
        ).rowcount
        logger.info("Deleted %d rows from campaign_site_content.", content_deleted)

        links_deleted = conn.execute(
            f"DELETE FROM contact_links WHERE candidate_id IN ({ph}) AND link_type = 'campaign_site'",
            ids,
        ).rowcount
        logger.info("Deleted %d campaign_site rows from contact_links.", links_deleted)

        conn.commit()
        logger.info("Done.")


if __name__ == "__main__":
    main()
