"""Remove campaign_x contact links that are not valid X/Twitter profile URLs.

A valid profile URL has exactly one path segment on x.com or twitter.com.
Everything else (homepage, tweet links, hashtags, wrong domains) is deleted.
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from camplinks.db import open_db
from camplinks.models import DB_FILENAME

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


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
        rows = conn.execute(
            "SELECT contact_link_id, url FROM contact_links WHERE link_type = 'campaign_x'"
        ).fetchall()

        invalid_ids = [
            link_id for link_id, url in rows if not is_valid_profile(url)
        ]

        logger.info(
            "Found %d campaign_x links; %d invalid, %d valid profiles.",
            len(rows),
            len(invalid_ids),
            len(rows) - len(invalid_ids),
        )

        ph = ",".join("?" * len(invalid_ids))
        deleted = conn.execute(
            f"DELETE FROM contact_links WHERE contact_link_id IN ({ph})",
            invalid_ids,
        ).rowcount
        conn.commit()

        logger.info("Deleted %d invalid campaign_x links.", deleted)


if __name__ == "__main__":
    main()
