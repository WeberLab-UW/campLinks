"""Compute and store token_length for campaign_site_content rows missing it.

Uses the NLTK word tokenizer to count tokens in cleaned_text.
Only processes rows where token_length IS NULL.
"""

from __future__ import annotations

import logging
import sqlite3

import nltk
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "camplinks.db"
BATCH_SIZE = 500


def ensure_nltk_data() -> None:
    """Download required NLTK tokenizer data if not already present."""
    try:
        nltk.data.find("tokenizers/punkt_tab")
    except LookupError:
        logger.info("Downloading NLTK punkt_tab tokenizer...")
        nltk.download("punkt_tab", quiet=True)


def main() -> None:
    """Entry point."""
    ensure_nltk_data()

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = WAL")

        rows = conn.execute(
            """
            SELECT content_id, cleaned_text
            FROM campaign_site_content
            WHERE token_length IS NULL
              AND cleaned_text IS NOT NULL
              AND cleaned_text != ''
            """
        ).fetchall()

        logger.info("Found %d rows missing token_length.", len(rows))

        updates: list[tuple[int, int]] = []
        for content_id, text in tqdm(rows, desc="Tokenizing", unit="row"):
            token_count = len(nltk.word_tokenize(text))
            updates.append((token_count, content_id))

            if len(updates) >= BATCH_SIZE:
                conn.executemany(
                    "UPDATE campaign_site_content SET token_length = ? WHERE content_id = ?",
                    updates,
                )
                conn.commit()
                updates = []

        if updates:
            conn.executemany(
                "UPDATE campaign_site_content SET token_length = ? WHERE content_id = ?",
                updates,
            )
            conn.commit()

    logger.info("Done. Updated %d rows.", len(rows))


if __name__ == "__main__":
    main()
