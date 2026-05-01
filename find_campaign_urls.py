"""Look up campaign sites from Wikipedia/Ballotpedia for candidates in bad_aggregator_urls.csv.

For each candidate, fetches their existing wikipedia_url or ballotpedia_url
from the database and extracts the campaign website from the page. Results
are written to a new_campaign_url column in the CSV. No web search is
performed and the database is not modified.
"""

from __future__ import annotations

import csv
import logging
import sqlite3
import time

import requests
from tqdm import tqdm

from camplinks.db import open_db
from camplinks.enrich import extract_campaign_website
from camplinks.http import fetch_soup
from camplinks.models import DB_FILENAME
from camplinks.search import extract_all_contact_links

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CSV_PATH = "bad_aggregator_urls.csv"
DELAY_S = 0.5


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


def find_campaign_url(wikipedia_url: str, ballotpedia_url: str) -> str:
    """Extract campaign website from Wikipedia or Ballotpedia page.

    Tries Wikipedia first, then Ballotpedia. Returns the first URL found.

    Args:
        wikipedia_url: Candidate's Wikipedia page URL, or empty string.
        ballotpedia_url: Candidate's Ballotpedia page URL, or empty string.

    Returns:
        Campaign site URL, or empty string if not found.
    """
    if wikipedia_url:
        try:
            soup = fetch_soup(wikipedia_url)
            url = extract_campaign_website(soup)
            if url:
                return url
            time.sleep(DELAY_S)
        except requests.RequestException as exc:
            logger.error("HTTP error fetching %s: %s", wikipedia_url, exc)

    if ballotpedia_url:
        try:
            soup = fetch_soup(ballotpedia_url)
            links = extract_all_contact_links(soup)
            url = links.get("Campaign website") or links.get("campaign_site") or ""
            if url:
                return url
            time.sleep(DELAY_S)
        except requests.RequestException as exc:
            logger.error("HTTP error fetching %s: %s", ballotpedia_url, exc)

    return ""


def main() -> None:
    """Entry point."""
    with open(CSV_PATH, newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        logger.info("CSV is empty.")
        return

    candidate_ids = [int(r["candidate_id"]) for r in rows]

    with open_db(DB_FILENAME) as conn:
        url_map = load_candidate_urls(conn, candidate_ids)

    has_wiki = sum(1 for v in url_map.values() if v[0])
    has_bp = sum(1 for v in url_map.values() if v[1])
    no_source = sum(1 for v in url_map.values() if not v[0] and not v[1])
    logger.info(
        "%d candidates: %d have Wikipedia URL, %d have Ballotpedia URL, %d have neither",
        len(rows), has_wiki, has_bp, no_source,
    )

    fieldnames = list(rows[0].keys())
    if "new_campaign_url" not in fieldnames:
        fieldnames.append("new_campaign_url")

    found = 0
    for row in tqdm(rows, desc="Looking up campaign URLs", unit="candidate"):
        cid = int(row["candidate_id"])
        wiki_url, bp_url = url_map.get(cid, ("", ""))
        new_url = find_campaign_url(wiki_url, bp_url)
        row["new_campaign_url"] = new_url
        if new_url:
            found += 1

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
