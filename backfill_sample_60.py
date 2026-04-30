"""Backfill the sample_60 column in the campaign_site_content table.

Reads all rows with cleaned_text but no sample_60, generates a 60%
sentence-chunk sample for each, and writes it back to the database.
"""

from __future__ import annotations

import re
import random
import sqlite3

from tqdm import tqdm

from camplinks.models import DB_FILENAME


def _sample_text(text: str, fraction: float = 0.60, max_attempts: int = 5) -> str:
    """Return a contiguous sentence chunk of the given fraction.

    Args:
        text: Input text to sample from.
        fraction: Fraction of sentences to include.
        max_attempts: Maximum retry attempts to avoid ERROR-containing chunks.

    Returns:
        Sampled text string.
    """
    if not isinstance(text, str) or text.startswith("ERROR:") or text == "":
        return text
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
    if not sentences:
        return text
    k = max(1, int(len(sentences) * fraction))
    max_start = max(0, len(sentences) - k)
    sample = ""
    for _ in range(max_attempts):
        start = random.randint(0, max_start)
        sample = " ".join(sentences[start : start + k])
        if "ERROR" not in sample:
            return sample
    return sample


def backfill(db_path: str = DB_FILENAME) -> None:
    """Add sample_60 column if missing and backfill all eligible rows.

    Args:
        db_path: Path to the SQLite database file.
    """
    conn = sqlite3.connect(db_path)

    try:
        conn.execute("ALTER TABLE campaign_site_content ADD COLUMN sample_60 TEXT")
        conn.commit()
        print("Added sample_60 column.")
    except sqlite3.OperationalError:
        print("sample_60 column already exists.")

    rows = conn.execute(
        """
        SELECT content_id, cleaned_text FROM campaign_site_content
        WHERE sample_60 IS NULL AND cleaned_text IS NOT NULL AND cleaned_text != ''
        """
    ).fetchall()

    print(f"Backfilling {len(rows)} rows...")

    for content_id, cleaned_text in tqdm(rows, desc="Backfilling sample_60", unit="row"):
        conn.execute(
            "UPDATE campaign_site_content SET sample_60 = ? WHERE content_id = ?",
            (_sample_text(cleaned_text), content_id),
        )

    conn.commit()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    backfill()
