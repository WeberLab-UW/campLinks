
"""Compute per-candidate AI magnitude score and write to candidates table.

This file impliments the magnitude calculation in metric-madmod.md acorss all modalities
and content types (for now, text and images across campaign sites and X posts).

Magnitude is on a scale from 0 to 1 and reflects AI assistance for that candidates campaign
It uses the following data:

    Text fraction (tweets) — length-weighted mean Pangram assistance_score:
        sum(token_length * assistance_score) / sum(token_length)

    Image fraction (tweets) — proportion of image-labeled tweets that are AI:
        count(image_AI_result='yes') / count(image_AI_result IN ('yes','no'))

    Text fraction (campaign site) — length-weighted mean Pangram assistance_score:
        sum(token_length * assistance_score) / sum(token_length)

    Image fraction (campaign site) — proportion of site images that are AI:
        count(image_AI_result='yes') / count(image_AI_result IN ('yes','no'))

    campaign_ai_magnitude = tweet_text_fraction + tweet_image_fraction
                          + site_text_fraction  + site_image_fraction

Each component defaults to 0 when a candidate has no eligible data for it.
Candidates with no eligible data across all modalities are written as
"no_relevant_data". Candidates with no tweets and no campaign site content
at all are left NULL.

Adds campaign_ai_magnitude column to candidates table if absent, then updates
all candidates that have data in at least one modality.

Usage:
    python code/compute_campaign_magnitude.py
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "camplinks.db"


def ensure_column(conn: sqlite3.Connection) -> None:
    """Add campaign_ai_magnitude column to candidates if it does not exist.

    Args:
        conn: Open SQLite connection.
    """
    existing = {r[1] for r in conn.execute("PRAGMA table_info(candidates)").fetchall()}
    if "campaign_ai_magnitude" not in existing:
        conn.execute("ALTER TABLE candidates ADD COLUMN campaign_ai_magnitude REAL")
        conn.commit()
        logger.info("Added campaign_ai_magnitude column to candidates.")


def text_fraction(
    tweet_rows: list[tuple[int, float]],
    site_rows: list[tuple[int, float]],
) -> float:
    """Compute length-weighted mean assistance score across tweets and campaign site.

    Args:
        tweet_rows: List of (token_length, assistance_score) tuples from tweets.
        site_rows: List of (token_length, assistance_score) tuples from campaign site.

    Returns:
        Weighted mean in [0, 1], or 0.0 if no eligible rows.
    """
    combined = tweet_rows + site_rows
    eligible = [
        (token_length, assistance_score)
        for token_length, assistance_score in combined
        if token_length and token_length > 0 and assistance_score is not None
    ]
    if not eligible:
        return 0.0
    numerator = sum(token_length * assistance_score for token_length, assistance_score in eligible)
    denominator = sum(token_length for token_length, _ in eligible)
    return numerator / denominator if denominator > 0 else 0.0


def image_fraction(
    tweet_image_results: list[str],
    site_image_results: list[str],
) -> float:
    """Compute fraction of AI-labeled images across tweets and campaign site.

    Args:
        tweet_image_results: List of image_AI_result values ('yes'/'no') from tweets.
        site_image_results: List of image_AI_result values ('yes'/'no') from campaign site.

    Returns:
        Fraction in [0, 1], or 0.0 if no eligible rows.
    """
    combined_image_results = tweet_image_results + site_image_results
    if not combined_image_results:
        return 0.0
    return sum(1 for result in combined_image_results if result == "yes") / len(combined_image_results)


def main() -> None:
    """Entry point."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode = WAL")

    ensure_column(conn)

    candidate_ids = [
        r[0] for r in conn.execute("SELECT candidate_id FROM candidates").fetchall()
    ]

    logger.info("Computing magnitude for %d candidates.", len(candidate_ids))

    updated = 0
    for cid in tqdm(sorted(candidate_ids), desc="Computing magnitude", unit="candidate"):

        # ── Tweets: text ─────────────────────────────────────────────────────
        tweet_text_rows = conn.execute(
            """
            SELECT token_length, assistance_score
            FROM tweets
            WHERE candidate_id = ?
              AND assistance_score IS NOT NULL
              AND token_length IS NOT NULL
            """,
            (cid,),
        ).fetchall()

        # ── Tweets: images ───────────────────────────────────────────────────
        tweet_image_results = [
            r[0] for r in conn.execute(
                """
                SELECT image_AI_result
                FROM tweets
                WHERE candidate_id = ?
                  AND image_AI_result IN ('yes', 'no')
                """,
                (cid,),
            ).fetchall()
        ]

        # ── Campaign site: text ───────────────────────────────────────────────
        site_text_rows = conn.execute(
            """
            SELECT token_length, assistance_score
            FROM campaign_site_content
            WHERE candidate_id = ?
              AND content_type = 'text'
              AND assistance_score IS NOT NULL
              AND token_length IS NOT NULL
            """,
            (cid,),
        ).fetchall()

        # ── Campaign site: images ─────────────────────────────────────────────
        site_image_results = [
            r[0] for r in conn.execute(
                """
                SELECT image_AI_result
                FROM campaign_site_content
                WHERE candidate_id = ?
                  AND content_type = 'image'
                  AND image_AI_result IN ('yes', 'no')
                """,
                (cid,),
            ).fetchall()
        ]

        has_any_data = any([
            tweet_text_rows, tweet_image_results,
            site_text_rows, site_image_results,
        ])

        magnitude: float | str
        if not has_any_data:
            magnitude = "no_relevant_data"
        else:
            magnitude = (
                text_fraction(tweet_text_rows, site_text_rows)
                + image_fraction(tweet_image_results, site_image_results)
            )

        conn.execute(
            "UPDATE candidates SET campaign_ai_magnitude = ? WHERE candidate_id = ?",
            (magnitude, cid),
        )
        updated += 1

    conn.commit()
    conn.close()
    logger.info("Done. Updated %d candidates.", updated)


if __name__ == "__main__":
    main()
