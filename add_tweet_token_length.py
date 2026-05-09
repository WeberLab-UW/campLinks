"""Backfill text_token_length for tweets rows missing it.

Uses the NLTK word tokenizer to count tokens in the text column.
Only processes rows where text_token_length IS NULL.
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

        existing = {row[1] for row in conn.execute("PRAGMA table_info(tweets)").fetchall()}
        if "text_token_length" not in existing:
            conn.execute("ALTER TABLE tweets ADD COLUMN text_token_length INTEGER")
            conn.commit()

        rows = conn.execute(
            """
            SELECT tweet_db_id, text
            FROM tweets
            WHERE text_token_length IS NULL
            """
        ).fetchall()

        logger.info("Found %d rows missing text_token_length.", len(rows))

        updates: list[tuple[int, int]] = []
        for tweet_db_id, text in tqdm(rows, desc="Tokenizing", unit="tweet"):
            token_count = len(nltk.word_tokenize(text)) if text else 0
            updates.append((token_count, tweet_db_id))

            if len(updates) >= BATCH_SIZE:
                conn.executemany(
                    "UPDATE tweets SET text_token_length = ? WHERE tweet_db_id = ?",
                    updates,
                )
                conn.commit()
                updates = []

        if updates:
            conn.executemany(
                "UPDATE tweets SET text_token_length = ? WHERE tweet_db_id = ?",
                updates,
            )
            conn.commit()

    logger.info("Done. Updated %d rows.", len(rows))


if __name__ == "__main__":
    main()
