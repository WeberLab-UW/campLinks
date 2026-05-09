"""Run Pangram AI text detection on campaign_site_content rows missing AI_label.

Queries camplinks.db for rows where AI_label IS NULL, calls the Pangram API
on sample_60, and writes results back to the DB. Saves progress every
SAVE_INTERVAL rows (resumable — re-running skips already-labeled rows).

Requires PANGRAM_API_KEY in environment or .env file.
"""

from __future__ import annotations

import logging
import os
import sqlite3
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
RETRY_BACKOFF = [10, 30, 60]


def check_ai(text: str, client: Pangram) -> tuple[str, float | None, str, float, float, float, int]:
    """Run Pangram prediction on a text sample with retry on timeout.

    Args:
        text: The sample_60 text to classify.
        client: Authenticated Pangram client.

    Returns:
        Tuple of (AI_label, assistance_score, confidence, fraction_ai,
        fraction_ai_assisted, fraction_human, num_ai_segments).
        All values are UNABLE_TO_RUN strings on API failure.
    """
    import time

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
        return (UNABLE_TO_RUN,) * 7  # type: ignore[return-value]

    ai_label: str = result["prediction_short"]
    fraction_ai: float = result["fraction_ai"]
    fraction_ai_assisted: float = result["fraction_ai_assisted"]
    fraction_human: float = result["fraction_human"]
    num_ai_segments: int = result["num_ai_segments"]

    confidence_vals: list[str] = []
    assistance_vals: list[float] = []
    for window in result["windows"]:
        confidence_vals.append(window["confidence"])
        assistance_vals.append(window["ai_assistance_score"])

    confidence: str = Counter(confidence_vals).most_common(1)[0][0] if confidence_vals else UNABLE_TO_RUN
    assistance_score: float = float(np.mean(assistance_vals)) if assistance_vals else 0.0

    return (ai_label, assistance_score, confidence, fraction_ai, fraction_ai_assisted, fraction_human, num_ai_segments)


def main() -> None:
    """Entry point."""
    load_dotenv()
    api_key = os.getenv("PANGRAM_API_KEY")
    if not api_key:
        raise RuntimeError("PANGRAM_API_KEY environment variable is not set.")

    pangram_client = Pangram(api_key=api_key)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = WAL")

        #replace WHERE (AI_label IS NULL OR AI_label = '') with WHERE AI_label = 'unable to run'    
        #if you want to re-run some of the previous detections that didnt work, bc they mainly didnt work
        #due to API issue 
        rows = conn.execute(
            """
            SELECT content_id, candidate_name, sample_60
            FROM campaign_site_content
            WHERE (AI_label IS NULL OR AI_label = '')
              AND sample_60 IS NOT NULL
              AND sample_60 != ''
              AND sample_60 NOT LIKE 'ERROR:%'
            """
        ).fetchall()

        logger.info("Found %d rows to process.", len(rows))

        processed = 0
        unable = 0

        for content_id, candidate_name, sample_60 in tqdm(rows, desc="Running Pangram", unit="row"):
            (
                ai_label, assistance_score, confidence,
                fraction_ai, _fraction_ai_assisted,
                fraction_human, num_ai_segments,
            ) = check_ai(sample_60, pangram_client)

            if ai_label == UNABLE_TO_RUN:
                unable += 1

            logger.info(
                "[%s] %s\n  -> %s",
                candidate_name,
                sample_60[:150],
                ai_label,
            )

            conn.execute(
                """
                UPDATE campaign_site_content
                SET AI_label          = ?,
                    assistance_score  = ?,
                    confidence        = ?,
                    fraction_ai       = ?,
                    fraction_human    = ?,
                    num_ai_segments   = ?
                WHERE content_id = ?
                """,
                (
                    ai_label,
                    None if assistance_score == UNABLE_TO_RUN else assistance_score,
                    confidence,
                    None if fraction_ai == UNABLE_TO_RUN else fraction_ai,
                    None if fraction_human == UNABLE_TO_RUN else fraction_human,
                    None if num_ai_segments == UNABLE_TO_RUN else num_ai_segments,
                    content_id,
                ),
            )
            processed += 1

            if processed % SAVE_INTERVAL == 0:
                conn.commit()
                logger.info(
                    "Progress: %d/%d (last: %s -> %s)",
                    processed, len(rows), candidate_name, ai_label,
                )

        conn.commit()

    logger.info(
        "Done. %d rows processed, %d unable to run.",
        processed, unable,
    )


if __name__ == "__main__":
    main()
