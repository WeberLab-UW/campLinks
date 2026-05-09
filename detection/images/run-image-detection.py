"""Run GPT-4o-mini image AI detection on tweets with images.

Adds image_AI_label column to the tweets table (if absent), then queries rows
where image_paths is non-empty and image_AI_label IS NULL. For each tweet,
sends each .jpg/.png image (base64-encoded) to GPT-4o-mini with the prompt:
  "Is this an AI-generated image? Answer in one word: yes or no"

If any image in the tweet is labeled "yes", the tweet is labeled "yes".
Rows with only video paths (.mp4) are skipped. Saves every SAVE_INTERVAL rows.

Requires OPENAI_API_KEY in environment or .env file.
"""

from __future__ import annotations

import base64
import logging
import os
import sqlite3
import time
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = str(Path(__file__).parent.parent.parent / "camplinks.db")
IMAGE_ROOT = Path(__file__).parent.parent.parent
SAVE_INTERVAL = 10
MODEL = "gpt-5.4-mini"
PROMPT = "Is this an AI-generated image? Answer in one word: yes or no"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
MAX_RETRIES = 3
RETRY_BACKOFF = [10, 30, 60]


def encode_image(path: Path) -> str:
    """Base64-encode an image file.

    Args:
        path: Absolute path to the image file.

    Returns:
        Base64-encoded string of the image bytes.
    """
    
    return base64.standard_b64encode(path.read_bytes()).decode("utf-8")


def classify_image(client: OpenAI, image_path: Path) -> str:
    """Ask GPT-4o-mini whether an image is AI-generated.

    Args:
        client: Authenticated OpenAI client.
        image_path: Path to the local image file.

    Returns:
        "yes", "no", or "error" if the API call fails after retries.
    """
    suffix = image_path.suffix.lower()
    mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                ".webp": "image/webp", ".gif": "image/gif"}
    mime = mime_map.get(suffix, "image/jpeg")
    b64 = encode_image(image_path)

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:{mime};base64,{b64}"},
                            },
                            {"type": "text", "text": PROMPT},
                        ],
                    }
                ],
                max_completion_tokens=5,
            )
            answer = response.choices[0].message.content.strip().lower()
            return "yes" if answer.startswith("yes") else "no"
        except Exception as exc:
            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            logger.error(
                "OpenAI API error (attempt %d/%d): %s — retrying in %ds",
                attempt + 1, MAX_RETRIES, exc, wait,
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(wait)

    return "error"


def classify_tweet_images(client: OpenAI, image_paths_str: str) -> str | None:
    """Classify all images in a tweet and return aggregate label.

    Args:
        client: Authenticated OpenAI client.
        image_paths_str: Pipe-separated local image paths from tweets.image_paths.

    Returns:
        "yes" if any image is AI-generated, "no" if all are human,
        "error" if API failures, or None if no valid images found.
    """
    paths = [p.strip() for p in image_paths_str.split(",") if p.strip()]
    image_files = [
        IMAGE_ROOT / p for p in paths
        if Path(p).suffix.lower() in IMAGE_EXTENSIONS
    ]

    if not image_files:
        return None

    results: list[str] = []
    for img_path in image_files:
        if not img_path.exists():
            logger.warning("Image not found, skipping: %s", img_path)
            continue
        label = classify_image(client, img_path)
        logger.info("  %s -> %s", img_path.name, label)
        results.append(label)

    if not results:
        return None
    if "yes" in results:
        return "yes"
    if "error" in results:
        return "error"
    return "no"


def main() -> None:
    """Entry point."""
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is not set.")

    client = OpenAI(api_key=api_key)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = WAL")

        conn.execute(
            "ALTER TABLE tweets ADD COLUMN image_AI_label TEXT"
        ) if not _column_exists(conn, "tweets", "image_AI_label") else None

        rows = conn.execute(
            """
            SELECT tweet_db_id, candidate_name, image_paths
            FROM tweets
            WHERE image_paths IS NOT NULL
              AND image_paths != ''
              AND image_AI_label IS NULL
            """
        ).fetchall()

        logger.info("Found %d tweets with images to classify.", len(rows))

        processed = 0
        skipped = 0

        for tweet_db_id, candidate_name, image_paths in tqdm(rows, desc="Classifying images", unit="tweet"):
            label = classify_tweet_images(client, image_paths)

            if label is None:
                skipped += 1
                logger.info("[%s] tweet %s — no valid images (videos only), skipping.", candidate_name, tweet_db_id)
                continue

            logger.info("[%s] tweet %s -> %s", candidate_name, tweet_db_id, label)

            conn.execute(
                "UPDATE tweets SET image_AI_label = ? WHERE tweet_db_id = ?",
                (label, tweet_db_id),
            )
            processed += 1

            if processed % SAVE_INTERVAL == 0:
                conn.commit()
                logger.info("Progress: %d/%d saved.", processed, len(rows))

        conn.commit()

    logger.info(
        "Done. %d tweets classified, %d skipped (video-only).",
        processed, skipped,
    )


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check if a column exists in a SQLite table.

    Args:
        conn: Active SQLite connection.
        table: Table name.
        column: Column name to check.

    Returns:
        True if the column exists, False otherwise.
    """
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()  # noqa: S608
    return any(row[1] == column for row in rows)


if __name__ == "__main__":
    main()
