"""Upsert contact links for live_campaign and wayback_recovered candidates in validated.csv.

- live_campaign:      upserts found_url as campaign_site (source=web_search)
- wayback_recovered:  upserts wayback_url as campaign_site_archived (source=wayback)
"""

from __future__ import annotations

import csv
import logging

from camplinks.db import open_db, upsert_contact_link
from camplinks.models import ContactLink, DB_FILENAME

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CSV_PATH = "validated.csv"


def main() -> None:
    """Entry point."""
    with open(CSV_PATH, newline="") as f:
        rows = list(csv.DictReader(f))

    live = [r for r in rows if r["verdict"] == "live_campaign"]
    wayback = [r for r in rows if r["verdict"] == "wayback_recovered"]
    logger.info(
        "Processing %d live_campaign and %d wayback_recovered candidates.",
        len(live),
        len(wayback),
    )

    with open_db(DB_FILENAME) as conn:
        live_count = 0
        for row in live:
            if not row.get("found_url"):
                continue
            upsert_contact_link(
                conn,
                ContactLink(
                    candidate_id=int(row["candidate_id"]),
                    link_type="campaign_site",
                    url=row["found_url"],
                    source="web_search",
                ),
            )
            live_count += 1

        wb_count = 0
        for row in wayback:
            if not row.get("wayback_url"):
                continue
            upsert_contact_link(
                conn,
                ContactLink(
                    candidate_id=int(row["candidate_id"]),
                    link_type="campaign_site_archived",
                    url=row["wayback_url"],
                    source="wayback",
                ),
            )
            wb_count += 1

        conn.commit()

    logger.info("Upserted %d campaign_site links (live_campaign).", live_count)
    logger.info(
        "Upserted %d campaign_site_archived links (wayback_recovered).", wb_count
    )


if __name__ == "__main__":
    main()
