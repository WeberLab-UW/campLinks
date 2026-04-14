"""Standalone runner for campaign site text scraping.

Delegates to camplinks.get_text_content.scrape_campaign_content.
Run directly to scrape the next batch of candidates.
"""

import sqlite3

from camplinks.get_text_content import scrape_campaign_content
from camplinks.models import DB_FILENAME

with sqlite3.connect(DB_FILENAME) as conn:
    conn.execute("PRAGMA foreign_keys = ON")
    scrape_campaign_content(conn)

print("Done.")
