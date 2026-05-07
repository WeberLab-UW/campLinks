"""Remove contact_links rows where link_type='campaign_x' and url='https://x.com/'."""

from __future__ import annotations

import logging

from camplinks.db import open_db
from camplinks.models import DB_FILENAME

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    """Entry point."""
    with open_db(DB_FILENAME) as conn:
        deleted = conn.execute(
            "DELETE FROM contact_links WHERE link_type = 'campaign_x' AND url = 'https://x.com/'"
        ).rowcount
        conn.commit()
    logger.info("Deleted %d rows.", deleted)


if __name__ == "__main__":
    main()
