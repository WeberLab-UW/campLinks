"""Run Pangram AI text detection on tweets rows missing text_AI_result.

Queries camplinks.db for tweets where text_AI_result IS NULL and text IS NOT NULL,
calls the Pangram API on the tweet text, and writes results back to the DB.
Saves progress every SAVE_INTERVAL rows (resumable).

Requires PANGRAM_API_KEY in environment or .env file.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from collections import Counter
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from pangram import Pangram
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = str(Path(__file__).parent.parent.parent / "camplinks.db")
SAVE_INTERVAL = 25
UNABLE_TO_RUN = "unable to run"
MAX_RETRIES = 3
RETRY_BACKOFF = [10, 60]


def check_ai(text: str, client: Pangram) -> tuple[str, float | None, str, float, float, int]:
    """Run Pangram prediction on tweet text with retry on failure.

    Args:
        text: Tweet text to classify.
        client: Authenticated Pangram client.

    Returns:
        Tuple of (text_AI_result, assistance_score, confidence, fraction_ai,
        fraction_human, num_ai_segments). All values are UNABLE_TO_RUN on failure.
    """
    result = None
    for attempt in range(MAX_RETRIES):
        try:
            result = client.predict(text)
            break
        except Exception as exc:
            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            logger.error(
                "Pangram API error (attempt %d/%d): %s — retrying in %ds",
                attempt + 1, MAX_RETRIES, exc, wait,
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(wait)

    if result is None:
        return (UNABLE_TO_RUN,) * 6  # type: ignore[return-value]

    ai_label: str = result["prediction_short"]
    fraction_ai: float = result["fraction_ai"]
    fraction_human: float = result["fraction_human"]
    num_ai_segments: int = result["num_ai_segments"]

    confidence_vals: list[str] = []
    assistance_vals: list[float] = []
    for window in result["windows"]:
        confidence_vals.append(window["confidence"])
        assistance_vals.append(window["ai_assistance_score"])

    confidence: str = Counter(confidence_vals).most_common(1)[0][0] if confidence_vals else UNABLE_TO_RUN
    assistance_score: float = float(np.mean(assistance_vals)) if assistance_vals else 0.0

    return (ai_label, assistance_score, confidence, fraction_ai, fraction_human, num_ai_segments)


def main() -> None:
    """Entry point."""
    load_dotenv()
    api_key = os.getenv("PANGRAM_API_KEY")
    if not api_key:
        raise RuntimeError("PANGRAM_API_KEY environment variable is not set.")

    pangram_client = Pangram(api_key=api_key)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = WAL")

        rows = conn.execute(
            """
            SELECT tweet_db_id, candidate_name, text
            FROM tweets
            WHERE (text_AI_result IS NULL OR text_AI_result = '')
              AND text IS NOT NULL
              AND text != ''
              AND token_length > 70
            """
        ).fetchall()

        logger.info("Found %d tweets to process.", len(rows))

        processed = unable = 0

        for tweet_db_id, candidate_name, text in tqdm(rows, desc="Running Pangram", unit="tweet"):
            (
                ai_label, assistance_score, confidence,
                fraction_ai, fraction_human, num_ai_segments,
            ) = check_ai(text, pangram_client)

            if ai_label == UNABLE_TO_RUN:
                unable += 1

            logger.info("[%s] tweet %s -> %s", candidate_name, tweet_db_id, ai_label)

            conn.execute(
                """
                UPDATE tweets
                SET text_AI_result         = ?,
                    assistance_score = ?,
                    confidence       = ?,
                    fraction_ai      = ?,
                    fraction_human   = ?,
                    num_ai_segments  = ?
                WHERE tweet_db_id = ?
                """,
                (
                    ai_label,
                    None if assistance_score == UNABLE_TO_RUN else assistance_score,
                    confidence,
                    None if fraction_ai == UNABLE_TO_RUN else fraction_ai,
                    None if fraction_human == UNABLE_TO_RUN else fraction_human,
                    None if num_ai_segments == UNABLE_TO_RUN else num_ai_segments,
                    tweet_db_id,
                ),
            )
            processed += 1

            if processed % SAVE_INTERVAL == 0:
                conn.commit()
                logger.info("Progress: %d/%d (last: %s -> %s)", processed, len(rows), candidate_name, ai_label)

        conn.commit()

    logger.info("Done. %d tweets processed, %d unable to run.", processed, unable)


if __name__ == "__main__":
    main()
