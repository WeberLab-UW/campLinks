"""Run Wayback Machine lookups for candidates with verdict='dead_url' in validated.csv.

Reads the 171 dead_url rows, calls query_wayback for each, writes results back
to validated.csv (updating wayback_url and verdict), and upserts archive links
into the database.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from tqdm import tqdm

from camplinks.db import open_db, upsert_contact_link
from camplinks.models import ContactLink, DB_FILENAME
from camplinks.validate import query_wayback

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CSV_PATH = Path("validated.csv")
SAVE_INTERVAL = 10


def main() -> None:
    """Entry point."""
    rows = list(csv.DictReader(CSV_PATH.open(newline="")))
    fieldnames = list(rows[0].keys())

    dead = [(i, r) for i, r in enumerate(rows) if r.get("verdict") == "dead_url"]
    logger.info("Found %d dead_url candidates to process.", len(dead))

    recovered = 0
    not_found = 0

    with open_db(DB_FILENAME) as conn:
        for batch_start in range(0, len(dead), SAVE_INTERVAL):
            batch = dead[batch_start : batch_start + SAVE_INTERVAL]

            for idx, row in tqdm(
                batch,
                desc=f"Wayback [{batch_start + 1}-{min(batch_start + SAVE_INTERVAL, len(dead))}/{len(dead)}]",
                unit="candidate",
            ):
                found_url: str = row["found_url"]
                year: int = int(row["year"])
                cid: int = int(row["candidate_id"])

                wayback_url = query_wayback(found_url, year)

                if wayback_url:
                    rows[idx]["wayback_url"] = wayback_url
                    rows[idx]["verdict"] = "wayback_recovered"
                    upsert_contact_link(
                        conn,
                        ContactLink(
                            candidate_id=cid,
                            link_type="campaign_site_archived",
                            url=wayback_url,
                            source="wayback",
                        ),
                    )
                    recovered += 1
                    logger.info(
                        "Recovered: %s (%s) -> %s",
                        row["candidate_name"],
                        found_url,
                        wayback_url,
                    )
                else:
                    rows[idx]["verdict"] = "no_wayback_found"
                    not_found += 1

            conn.commit()

            # Write progress back to CSV after each batch
            with CSV_PATH.open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

    logger.info(
        "Done: %d recovered via Wayback, %d no archive found.",
        recovered,
        not_found,
    )


if __name__ == "__main__":
    main()
