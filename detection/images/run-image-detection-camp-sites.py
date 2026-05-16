"""Run GPT-5.4-mini image AI detection on campaign site images.

Queries campaign_site_content where content_type = 'image', image_path is set,
and image_AI_result IS NULL. For each row, loads the local image file,
sends it to GPT-5.4-mini, and upserts the result into image_AI_result.

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

MIME_MAP = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
}


def classify_image(client: OpenAI, image_path: Path) -> str:
    """Ask GPT-5.4-mini whether an image is AI-generated.

    Args:
        client: Authenticated OpenAI client.
        image_path: Path to the local image file.

    Returns:
        "yes", "no", "output error", or "error" if API call fails after retries.
    """
    suffix = image_path.suffix.lower()
    mime = MIME_MAP.get(suffix, "image/jpeg")
    b64 = base64.standard_b64encode(image_path.read_bytes()).decode("utf-8")

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
                max_completion_tokens=30,
            )
            answer = response.choices[0].message.content.strip().lower()
            if answer == "yes":
                return "yes"
            elif answer == "no":
                return "no"
            else:
                return "output error"
        except Exception as exc:
            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            logger.error(
                "OpenAI API error (attempt %d/%d): %s — retrying in %ds",
                attempt + 1, MAX_RETRIES, exc, wait,
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(wait)

    return "API error"


def main() -> None:
    """Entry point."""
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is not set.")

    client = OpenAI(api_key=api_key)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = WAL")

        rows = conn.execute(
            """
            SELECT content_id, candidate_name, image_path
            FROM campaign_site_content
            WHERE content_type = 'image'
              AND image_path IS NOT NULL
              AND image_path != ''
              AND image_AI_result IS NULL
            """
        ).fetchall()

        logger.info("Found %d campaign site images to classify.", len(rows))

        processed = skipped = 0

        for content_id, candidate_name, image_path_str in tqdm(rows, desc="Classifying images", unit="image"):
            abs_path = IMAGE_ROOT / image_path_str

            if not abs_path.exists():
                logger.warning("[%s] Image not found, skipping: %s", candidate_name, abs_path)
                skipped += 1
                continue

            if abs_path.suffix.lower() not in IMAGE_EXTENSIONS:
                skipped += 1
                continue

            label = classify_image(client, abs_path)
            logger.info("[%s] %s -> %s", candidate_name, abs_path.name, label)

            conn.execute(
                "UPDATE campaign_site_content SET image_AI_result = ? WHERE content_id = ?",
                (label, content_id),
            )
            processed += 1

            if processed % SAVE_INTERVAL == 0:
                conn.commit()
                logger.info("Progress: %d/%d saved.", processed, len(rows))

        conn.commit()

    logger.info(
        "Done. %d images classified, %d skipped (not found or invalid).",
        processed, skipped,
    )


if __name__ == "__main__":
    main()
