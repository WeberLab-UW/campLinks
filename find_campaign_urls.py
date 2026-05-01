"""Look up campaign sites from Ballotpedia for candidates in bad_aggregator_urls.csv.

For each candidate, fetches their existing ballotpedia_url from the database
and extracts the campaign website from the page. Results are written to a
new_campaign_url column in the CSV. No web search is performed and the
database is not modified. Rows that already have new_campaign_url set are
skipped (resumable).
"""

from __future__ import annotations

import csv
import logging
import sqlite3
import time

import requests
from tqdm import tqdm

from camplinks.db import open_db
from camplinks.models import DB_FILENAME
from camplinks.http import fetch_soup
from camplinks.scrapers.ballotpedia_parsing import BALLOTPEDIA_DELAY_S
from camplinks.search import extract_all_contact_links

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CSV_PATH = "bad_aggregator_urls.csv"
SAVE_INTERVAL = 25
MAX_RETRIES = 2
RETRY_WAIT_S = 10.0


def load_candidate_urls(
    conn: sqlite3.Connection,
    candidate_ids: list[int],
) -> dict[int, tuple[str, str]]:
    """Fetch wikipedia_url and ballotpedia_url for the given candidate IDs.

    Args:
        conn: Open database connection.
        candidate_ids: List of candidate IDs to look up.

    Returns:
        Dict mapping candidate_id to (wikipedia_url, ballotpedia_url).
    """
    ph = ",".join("?" * len(candidate_ids))
    rows = conn.execute(
        f"SELECT candidate_id, wikipedia_url, ballotpedia_url FROM candidates"
        f" WHERE candidate_id IN ({ph})",
        candidate_ids,
    ).fetchall()
    return {r[0]: (r[1] or "", r[2] or "") for r in rows}


def find_campaign_url(ballotpedia_url: str) -> str:
    """Extract campaign website from a Ballotpedia candidate page.

    Retries up to MAX_RETRIES times on HTTP errors (e.g. 429 rate limit).

    Args:
        ballotpedia_url: Candidate's Ballotpedia page URL, or empty string.

    Returns:
        Campaign site URL, or empty string if not found.
    """
    if not ballotpedia_url:
        return ""

    for attempt in range(MAX_RETRIES + 1):
        try:
            soup = fetch_soup(ballotpedia_url, delay_s=BALLOTPEDIA_DELAY_S)
            links = extract_all_contact_links(soup)
            return links.get("campaign website") or links.get("campaign_site") or ""
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status == 429 and attempt < MAX_RETRIES:
                wait = RETRY_WAIT_S * (attempt + 1)
                logger.warning(
                    "Rate limited on %s (attempt %d), waiting %.0fs",
                    ballotpedia_url, attempt + 1, wait,
                )
                time.sleep(wait)
            else:
                logger.error("HTTP %d fetching %s", status, ballotpedia_url)
                return ""
        except requests.RequestException as exc:
            logger.error("Request error fetching %s: %s", ballotpedia_url, exc)
            return ""

    return ""


def main() -> None:
    """Entry point."""
    with open(CSV_PATH, newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        logger.info("CSV is empty.")
        return

    fieldnames = list(rows[0].keys())
    if "new_campaign_url" not in fieldnames:
        fieldnames.append("new_campaign_url")
        for row in rows:
            row["new_campaign_url"] = ""

    candidate_ids = [int(r["candidate_id"]) for r in rows]

    with open_db(DB_FILENAME) as conn:
        url_map = load_candidate_urls(conn, candidate_ids)

    pending = [r for r in rows if not r.get("new_campaign_url", "").strip()]
    already_done = len(rows) - len(pending)
    if already_done:
        logger.info("Skipping %d rows already resolved.", already_done)

    has_bp = sum(1 for cid in [int(r["candidate_id"]) for r in pending] if url_map.get(cid, ("", ""))[1])
    logger.info(
        "%d candidates to process; %d have a Ballotpedia URL.",
        len(pending), has_bp,
    )

    found = sum(1 for r in rows if r.get("new_campaign_url", "").strip())
    processed_since_save = 0

    for row in tqdm(pending, desc="Looking up campaign URLs", unit="candidate"):
        cid = int(row["candidate_id"])
        _, bp_url = url_map.get(cid, ("", ""))
        new_url = find_campaign_url(bp_url)
        row["new_campaign_url"] = new_url
        if new_url:
            found += 1

        processed_since_save += 1
        if processed_since_save % SAVE_INTERVAL == 0:
            with open(CSV_PATH, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(
        "Done. Found campaign URLs for %d / %d candidates. CSV updated.",
        found, len(rows),
    )


if __name__ == "__main__":
    main()
