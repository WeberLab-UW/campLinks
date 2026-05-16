"""Run GPT-5.4-mini image AI detection on images in sample_images_from_chen.csv.

Downloads each image from image_url, classifies it via GPT-5.4-mini, and writes
the result to a new column 'gpt-5.4-results' in the CSV. Skips rows already
classified. Saves progress every SAVE_INTERVAL rows.

Requires OPENAI_API_KEY in environment or .env file.
"""

from __future__ import annotations

import base64
import csv
import logging
import os
import tempfile
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CSV_PATH = Path(__file__).parent / "sample_images_from_chen.csv"
RESULT_COLUMN = "gpt-5.4-results"
SAVE_INTERVAL = 10
MODEL = "gpt-5.4-mini"
PROMPT = "Is this an AI-generated image? Answer in one word: yes or no"
MAX_RETRIES = 3
RETRY_BACKOFF = [10, 30, 60]
DOWNLOAD_TIMEOUT_S = 15

#fake browser identity set tot help with downloading the images
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

MIME_MAP = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
}


def download_image(url: str) -> tuple[bytes, str]:
    """Download an image from a URL and return its bytes and MIME type.

    Args:
        url: Remote image URL.

    Returns:
        Tuple of (image bytes, mime type string).

    Raises:
        requests.RequestException: If the download fails.
    """
    resp = requests.get(url, headers=HEADERS, timeout=DOWNLOAD_TIMEOUT_S)
    resp.raise_for_status()
    suffix = Path(urlparse(url).path).suffix.lower()
    mime = MIME_MAP.get(suffix, "image/jpeg")
    return resp.content, mime


def classify_image_bytes(client: OpenAI, image_bytes: bytes, mime: str) -> str:
    """Ask GPT-5.4-mini whether an image is AI-generated.

    Args:
        client: Authenticated OpenAI client.
        image_bytes: Raw image bytes.
        mime: MIME type of the image.

    Returns:
        "yes", "no", or "error" if the API call fails after retries.
    """
    b64 = base64.standard_b64encode(image_bytes).decode("utf-8")

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
                max_completion_tokens=50,
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

    return "error"


def write_csv(rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    """Write rows back to CSV_PATH.

    Args:
        rows: All CSV rows.
        fieldnames: Column names in order.
    """
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    """Entry point."""
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is not set.")

    client = OpenAI(api_key=api_key)

    with open(CSV_PATH, newline="") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys()) if rows else []

    if RESULT_COLUMN not in fieldnames:
        fieldnames.append(RESULT_COLUMN)
        for r in rows:
            r[RESULT_COLUMN] = ""

    to_classify = [
        (i, r) for i, r in enumerate(rows)
        if not r.get(RESULT_COLUMN, "").strip()
    ]

    logger.info("%d images to classify (%d already done).", len(to_classify), len(rows) - len(to_classify))

    processed = errors = 0

    for idx, row in tqdm(to_classify, desc="Classifying images", unit="image"):
        image_url = row.get("image_url", "").strip()
        if not image_url:
            rows[idx][RESULT_COLUMN] = "error"
            errors += 1
            continue

        try:
            image_bytes, mime = download_image(image_url)
        except Exception as exc:
            logger.error("Failed to download %s: %s", image_url, exc)
            rows[idx][RESULT_COLUMN] = "unable to download image"
            errors += 1
            processed += 1
            if processed % SAVE_INTERVAL == 0:
                write_csv(rows, fieldnames)
            continue

        label = classify_image_bytes(client, image_bytes, mime)
        rows[idx][RESULT_COLUMN] = label
        logger.info("%s -> %s", image_url, label)

        processed += 1
        if processed % SAVE_INTERVAL == 0:
            write_csv(rows, fieldnames)
            logger.info("Saved progress: %d/%d classified.", processed, len(to_classify))

    write_csv(rows, fieldnames)
    logger.info(
        "Done. %d classified, %d errors.",
        processed - errors, errors,
    )


if __name__ == "__main__":
    main()
