"""Validate cleaning_round_1 X profiles by checking if accounts actually exist.

Makes HTTP requests to each profile URL and checks for:
  - 404: account does not exist
  - "account suspended" in response: suspended account
  - other errors: unreachable

Valid active accounts are written to cleaning_round_2.
Saves progress every SAVE_INTERVAL rows (resumable).
"""

from __future__ import annotations

import csv
import logging
import time
from pathlib import Path

import requests
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

INPUT_CSV = Path("missing_x_links.csv")
SAVE_INTERVAL = 50
REQUEST_DELAY_S = 1.5
TIMEOUT_S = 10

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SUSPENDED_PHRASES = [
    "account suspended",
    "this account has been suspended",
]


def check_profile(url: str) -> str:
    """Check if an X/Twitter profile URL is active.

    Args:
        url: X/Twitter profile URL to check.

    Returns:
        "valid", "suspended", "not_found", or "error".
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S, allow_redirects=True)
        if resp.status_code == 404:
            return "not_found"
        if resp.status_code == 200:
            body = resp.text.lower()
            if any(phrase in body for phrase in SUSPENDED_PHRASES):
                return "suspended"
            return "valid"
        return "error"
    except requests.RequestException:
        return "error"


def main() -> None:
    """Entry point."""
    with open(INPUT_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
        fieldnames = list(rows[0].keys())

    if "cleaning_round_2" not in fieldnames:
        fieldnames.append("cleaning_round_2")
        for r in rows:
            r["cleaning_round_2"] = ""

    to_check = [
        (i, r) for i, r in enumerate(rows)
        if r.get("cleaning_round_1", "").strip() and not r.get("cleaning_round_2", "").strip()
    ]

    logger.info("%d profiles to validate.", len(to_check))

    valid = suspended = not_found = errors = 0
    processed = 0

    for idx, row in tqdm(to_check, desc="Validating profiles", unit="profile"):
        url = row["cleaning_round_1"].strip()
        status = check_profile(url)

        if status == "valid":
            rows[idx]["cleaning_round_2"] = url
            valid += 1
        else:
            rows[idx]["cleaning_round_2"] = f"INVALID:{status}"
            if status == "suspended":
                suspended += 1
            elif status == "not_found":
                not_found += 1
            else:
                errors += 1

        logger.info("%s -> %s (%s)", row["candidate_name"], url, status)
        processed += 1
        time.sleep(REQUEST_DELAY_S)

        if processed % SAVE_INTERVAL == 0:
            _write_csv(rows, fieldnames)
            logger.info("Saved progress: %d checked.", processed)

    _write_csv(rows, fieldnames)
    logger.info(
        "Done. valid=%d, suspended=%d, not_found=%d, errors=%d",
        valid, suspended, not_found, errors,
    )


def _write_csv(rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    """Write rows back to INPUT_CSV.

    Args:
        rows: All CSV rows.
        fieldnames: Column names in order.
    """
    with open(INPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
