"""Batch GPT-5.4-mini image AI detection on tweets using the OpenAI Batch API.

Splits tweets with images into batches of BATCH_SIZE, submits each as an
OpenAI batch job (50% cheaper than synchronous calls), polls for completion,
and writes results back to the tweets table as image_AI_result.

Each image in a tweet gets its own request (custom_id = "{tweet_db_id}_{img_idx}").
A tweet is labeled "yes" if any image is AI-generated, "no" if all are human.

Skips .mp4 and other video files. Skips tweets already labeled.
Saves a batch_state.json file to resume if interrupted.

Requires OPENAI_API_KEY in environment or .env file.
"""

from __future__ import annotations

import argparse
import base64
import logging
import os
import sqlite3
import tempfile
import time
from pathlib import Path

import orjson
from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent.parent / "camplinks.db"
IMAGE_ROOT = Path(__file__).parent.parent.parent
STATE_FILE = Path(__file__).parent / "batch_state.json"

BATCH_SIZE = 300          # max images per batch (keep JSONL well under 100MB)
POLL_INTERVAL_S = 60      # seconds between status checks
MODEL = "gpt-5.4-mini"
PROMPT = "Is this an AI-generated image? Answer in one word: yes or no"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
MIME_MAP = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".png": "image/png", ".webp": "image/webp", ".gif": "image/gif",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def encode_image(path: Path) -> str:
    """Base64-encode a local image file.

    Args:
        path: Absolute path to the image.

    Returns:
        Base64-encoded string.
    """
    return base64.standard_b64encode(path.read_bytes()).decode("utf-8")


def get_image_paths(image_paths_str: str) -> list[Path]:
    """Return valid local image paths from a comma-separated image_paths string.

    Args:
        image_paths_str: Comma-separated relative image paths from tweets table.

    Returns:
        List of existing Path objects for image files (no videos).
    """
    paths = []
    for p in image_paths_str.split(","):
        p = p.strip()
        if not p:
            continue
        if Path(p).suffix.lower() not in IMAGE_EXTENSIONS:
            continue
        full = IMAGE_ROOT / p
        if not full.exists():
            full = IMAGE_ROOT / p.replace("tweet_images/", "tweet_images_compliance_required/", 1)
        if full.exists():
            paths.append(full)
        else:
            logger.warning("Image not found, skipping: %s", full)
    return paths


def build_request(tweet_db_id: int, img_idx: int, img_path: Path) -> dict:
    """Build a single JSONL batch request object for one image.

    Args:
        tweet_db_id: DB primary key of the tweet.
        img_idx: Index of this image within the tweet.
        img_path: Local path to the image file.

    Returns:
        Dict formatted for OpenAI batch JSONL input.
    """
    suffix = img_path.suffix.lower()
    mime = MIME_MAP.get(suffix, "image/jpeg")
    b64 = encode_image(img_path)
    return {
        "custom_id": f"{tweet_db_id}_{img_idx}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
                        {"type": "text", "text": PROMPT},
                    ],
                }
            ],
            "max_completion_tokens": 10,
        },
    }


# ── State persistence ─────────────────────────────────────────────────────────

def load_state() -> dict:
    """Load batch state from disk for resumability.

    Returns:
        Dict with 'pending_batches' list of {batch_id, tweet_db_ids}.
    """
    if STATE_FILE.exists():
        return orjson.loads(STATE_FILE.read_bytes())
    return {"pending_batches": []}


def save_state(state: dict) -> None:
    """Persist batch state to disk.

    Args:
        state: State dict to write.
    """
    STATE_FILE.write_bytes(orjson.dumps(state))


# ── Batch submission ──────────────────────────────────────────────────────────

def submit_batch(client: OpenAI, tweet_chunk: list[tuple[int, str]]) -> str | None:
    """Build a JSONL file for a chunk of tweets and submit as an OpenAI batch.

    Args:
        client: Authenticated OpenAI client.
        tweet_chunk: List of (tweet_db_id, image_paths_str) tuples.

    Returns:
        OpenAI batch ID, or None if no valid images in the chunk.
    """
    lines = []
    for tweet_db_id, image_paths_str in tweet_chunk:
        img_paths = get_image_paths(image_paths_str)
        for idx, img_path in enumerate(img_paths):
            req = build_request(tweet_db_id, idx, img_path)
            lines.append(orjson.dumps(req))

    if not lines:
        return None

    jsonl_bytes = b"\n".join(lines)

    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as tmp:
        tmp.write(jsonl_bytes)
        tmp_path = tmp.name

    try:
        with open(tmp_path, "rb") as f:
            uploaded = client.files.create(file=f, purpose="batch")
        batch = client.batches.create(
            input_file_id=uploaded.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        logger.info("Submitted batch %s (%d requests, %d tweets).",
                    batch.id, len(lines), len(tweet_chunk))
        return batch.id
    finally:
        os.unlink(tmp_path)


# ── Result processing ─────────────────────────────────────────────────────────

def parse_results(result_content: str) -> dict[int, str]:
    """Parse batch output JSONL and aggregate per-tweet labels.

    A tweet is "yes" if any image is AI-generated. "no" if all are human.
    "output error" / "API error" if issues occurred.

    Args:
        result_content: Raw JSONL string from OpenAI output file.

    Returns:
        Dict mapping tweet_db_id -> label.
    """
    tweet_results: dict[int, list[str]] = {}

    for line in result_content.strip().splitlines():
        item = orjson.loads(line)
        custom_id = item["custom_id"]
        tweet_db_id = int(custom_id.split("_")[0])

        if item.get("error"):
            label = "API error"
        else:
            content = item["response"]["body"]["choices"][0]["message"]["content"]
            answer = content.strip().lower()
            if answer == "yes":
                label = "yes"
            elif answer == "no":
                label = "no"
            else:
                label = "output error"

        tweet_results.setdefault(tweet_db_id, []).append(label)

    aggregated: dict[int, str] = {}
    for tweet_db_id, labels in tweet_results.items():
        if "yes" in labels:
            aggregated[tweet_db_id] = "yes"
        elif "API error" in labels or "output error" in labels:
            aggregated[tweet_db_id] = "API error"
        else:
            aggregated[tweet_db_id] = "no"

    return aggregated


def collect_batch(client: OpenAI, batch_id: str) -> dict[int, str] | None:
    """Poll a batch until complete and return parsed results.

    Args:
        client: Authenticated OpenAI client.
        batch_id: OpenAI batch ID to poll.

    Returns:
        Dict of tweet_db_id -> label, or None if batch failed.
    """
    while True:
        batch = client.batches.retrieve(batch_id)
        logger.info("Batch %s status: %s (%s/%s completed)",
                    batch_id, batch.status,
                    batch.request_counts.completed,
                    batch.request_counts.total)

        if batch.status == "completed":
            content = client.files.content(batch.output_file_id).text
            return parse_results(content)
        elif batch.status in ("failed", "expired", "cancelled"):
            logger.error("Batch %s ended with status: %s", batch_id, batch.status)
            return None

        time.sleep(POLL_INTERVAL_S)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    """Entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true",
                        help="Submit only the first batch to verify the pipeline works.")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is not set.")

    client = OpenAI(api_key=api_key)
    state = load_state()

    with sqlite3.connect(str(DB_PATH), timeout=30) as conn:
        conn.execute("PRAGMA journal_mode = WAL")

        # Add column if missing
        existing = {r[1] for r in conn.execute("PRAGMA table_info(tweets)").fetchall()}
        if "image_AI_result" not in existing:
            conn.execute("ALTER TABLE tweets ADD COLUMN image_AI_result TEXT")
            conn.commit()

        rows = conn.execute(
            """
            SELECT tweet_db_id, image_paths
            FROM tweets
            WHERE image_paths IS NOT NULL
              AND image_paths != ''
              AND image_AI_result IS NULL
            """
        ).fetchall()

    # Filter to only tweets with at least one valid image (no videos)
    rows = [(tid, ip) for tid, ip in rows
            if any(Path(p.strip()).suffix.lower() in IMAGE_EXTENSIONS
                   for p in ip.split(",") if p.strip())]

    logger.info("%d tweets with images to classify.", len(rows))

    # ── Phase 1: Submit batches ───────────────────────────────────────────────
    if not state["pending_batches"]:
        # Split by image count, not tweet count
        chunks: list[list[tuple[int, str]]] = []
        current_chunk: list[tuple[int, str]] = []
        current_img_count = 0
        for tweet_db_id, image_paths_str in rows:
            img_count = sum(
                1 for p in image_paths_str.split(",")
                if p.strip() and Path(p.strip()).suffix.lower() in IMAGE_EXTENSIONS
            )
            if current_img_count + img_count > BATCH_SIZE and current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
                current_img_count = 0
            current_chunk.append((tweet_db_id, image_paths_str))
            current_img_count += img_count
        if current_chunk:
            chunks.append(current_chunk)

        if args.test:
            chunks = chunks[:1]
            logger.info("TEST MODE: submitting only the first batch (%d tweets).", len(chunks[0]))
        else:
            logger.info("Submitting %d batches of up to %d images each.", len(chunks), BATCH_SIZE)

        for chunk in tqdm(chunks, desc="Submitting batches", unit="batch"):
            tweet_ids = [t[0] for t in chunk]
            batch_id = submit_batch(client, chunk)
            if batch_id:
                state["pending_batches"].append({"batch_id": batch_id, "tweet_ids": tweet_ids})
                save_state(state)
            time.sleep(1)
    else:
        logger.info("Resuming — %d batches already submitted.", len(state["pending_batches"]))

    # ── Phase 2: Collect results ──────────────────────────────────────────────
    total_saved = 0
    remaining = []

    for entry in tqdm(state["pending_batches"], desc="Collecting batches", unit="batch"):
        results = collect_batch(client, entry["batch_id"])
        if results is None:
            remaining.append(entry)
            continue

        with sqlite3.connect(str(DB_PATH), timeout=30) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            for tweet_db_id, label in results.items():
                conn.execute(
                    "UPDATE tweets SET image_AI_result = ? WHERE tweet_db_id = ?",
                    (label, tweet_db_id),
                )
            conn.commit()
            total_saved += len(results)

    state["pending_batches"] = remaining
    save_state(state)

    if not remaining:
        STATE_FILE.unlink(missing_ok=True)
        logger.info("All batches complete. %d tweets labeled.", total_saved)
    else:
        logger.warning("%d batches failed — re-run to retry.", len(remaining))


if __name__ == "__main__":
    main()
