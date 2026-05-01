"""Re-run enrich and search stages for candidates in bad_wikipedia_urls.csv.

Reads the audit CSV produced when bad Wikipedia-sourced campaign site URLs
were cleared, then runs enrich (Wikipedia lookup → campaign site extraction)
followed by web search for any candidates that enrich cannot resolve.
"""

from __future__ import annotations

import csv
import logging
import sqlite3
import time

import requests
from tqdm import tqdm

from camplinks.cache import load_cache, make_cache_key, save_cache
from camplinks.db import (
    open_db,
    update_candidate_ballotpedia_url,
    update_candidate_wikipedia_url,
    upsert_contact_link,
)
from camplinks.enrich import extract_campaign_website, find_wikipedia_url
from camplinks.http import fetch_soup
from camplinks.models import ContactLink, DB_FILENAME
from camplinks.search import (
    BALLOTPEDIA_LABEL_MAP,
    CACHE_FILE,
    _race_keyword,
    find_candidate_info,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CSV_PATH = "bad_wikipedia_urls.csv"
ENRICH_DELAY_S = 0.5


def load_target_candidates(
    conn: sqlite3.Connection,
    csv_path: str,
) -> list[sqlite3.Row]:
    """Load candidate rows for IDs listed in the audit CSV.

    Args:
        conn: Open database connection.
        csv_path: Path to bad_wikipedia_urls.csv.

    Returns:
        List of Row objects with candidate and election fields.
    """
    with open(csv_path, newline="") as f:
        ids = [int(r["candidate_id"]) for r in csv.DictReader(f)]

    if not ids:
        return []

    ph = ",".join("?" * len(ids))
    return conn.execute(
        f"""
        SELECT c.candidate_id, c.candidate_name, c.party,
               c.wikipedia_url, c.ballotpedia_url,
               e.state, e.district, e.year, e.race_type, e.election_stage
        FROM candidates c
        JOIN elections e ON c.election_id = e.election_id
        WHERE c.candidate_id IN ({ph})
          AND c.candidate_id NOT IN (
              SELECT cl.candidate_id FROM contact_links cl
              WHERE cl.link_type = 'campaign_site'
          )
        """,
        ids,
    ).fetchall()


def run_enrich(
    conn: sqlite3.Connection,
    targets: list[sqlite3.Row],
) -> set[int]:
    """Run Wikipedia enrich for the given candidates.

    For candidates without a wikipedia_url, searches DDG to find one first.
    Then fetches their Wikipedia page and extracts the campaign site URL.

    Args:
        conn: Open database connection.
        targets: Candidate rows from load_target_candidates.

    Returns:
        Set of candidate_ids that now have a campaign_site link.
    """
    # Step 1: fill in missing wikipedia_urls via DDG search
    missing_wiki = [r for r in targets if not r["wikipedia_url"]]
    if missing_wiki:
        logger.info("Finding Wikipedia URLs for %d candidates...", len(missing_wiki))
        for row in tqdm(missing_wiki, desc="Wikipedia URL search", unit="candidate"):
            try:
                url = find_wikipedia_url(
                    row["candidate_name"], row["state"], row["race_type"]
                )
                if url:
                    update_candidate_wikipedia_url(conn, row["candidate_id"], url)
            except Exception as exc:
                logger.error(
                    "Wikipedia search failed for %s: %s", row["candidate_name"], exc
                )
        conn.commit()

    # Reload so wikipedia_url updates are visible
    ids = [r["candidate_id"] for r in targets]
    ph = ",".join("?" * len(ids))
    refreshed = conn.execute(
        f"SELECT candidate_id, wikipedia_url FROM candidates WHERE candidate_id IN ({ph})",
        ids,
    ).fetchall()
    wiki_map: dict[int, str] = {r["candidate_id"]: r["wikipedia_url"] for r in refreshed}

    # Step 2: group candidates by wikipedia_url to avoid redundant fetches
    url_to_ids: dict[str, list[int]] = {}
    for row in targets:
        wiki_url = wiki_map.get(row["candidate_id"], "")
        if wiki_url:
            url_to_ids.setdefault(wiki_url, []).append(row["candidate_id"])

    if not url_to_ids:
        logger.info("No candidates have a Wikipedia URL to enrich from.")
        return set()

    logger.info(
        "Enriching %d candidates from %d unique Wikipedia pages...",
        sum(len(v) for v in url_to_ids.values()),
        len(url_to_ids),
    )

    enriched: set[int] = set()
    for url, candidate_ids in tqdm(
        url_to_ids.items(), desc="Fetching campaign sites", unit="page"
    ):
        try:
            soup = fetch_soup(url)
            campaign_url = extract_campaign_website(soup)
            if campaign_url:
                for cid in candidate_ids:
                    upsert_contact_link(
                        conn,
                        ContactLink(
                            candidate_id=cid,
                            link_type="campaign_site",
                            url=campaign_url,
                            source="wikipedia",
                        ),
                    )
                    enriched.add(cid)
            time.sleep(ENRICH_DELAY_S)
        except requests.RequestException as exc:
            logger.error("HTTP error for %s: %s", url, exc)
        except Exception as exc:
            logger.error("Error parsing %s: %s", url, exc)

    conn.commit()
    logger.info("Enrich found campaign sites for %d candidates.", len(enriched))
    return enriched


def run_search(
    conn: sqlite3.Connection,
    targets: list[sqlite3.Row],
    enriched_ids: set[int],
    cache_path: str = CACHE_FILE,
) -> int:
    """Run web search for candidates that enrich could not resolve.

    Args:
        conn: Open database connection.
        targets: Candidate rows from load_target_candidates.
        enriched_ids: Candidate IDs already resolved by enrich (skipped).
        cache_path: Path for the search cache file.

    Returns:
        Number of candidates with new contact info found.
    """
    remaining = [r for r in targets if r["candidate_id"] not in enriched_ids]
    if not remaining:
        logger.info("All candidates resolved by enrich; skipping search.")
        return 0

    logger.info("Running search for %d remaining candidates...", len(remaining))
    cache = load_cache(cache_path)

    found_count = 0
    processed = 0

    for row in tqdm(remaining, desc="Searching candidates", unit="candidate"):
        cid = row["candidate_id"]
        cache_key = make_cache_key(
            row["party"], row["state"], row["district"] or "", row["candidate_name"]
        )

        if cache_key in cache:
            contacts = cache[cache_key]
        else:
            keyword = _race_keyword(row["race_type"])
            contacts = find_candidate_info(
                row["candidate_name"], row["state"], row["district"] or "", keyword
            )
            cache[cache_key] = contacts
            processed += 1
            if processed % 25 == 0:
                save_cache(cache, cache_path)

        bp_url = contacts.pop("_ballotpedia_url", "")
        if bp_url:
            update_candidate_ballotpedia_url(conn, cid, bp_url)

        any_found = False
        for label, url in contacts.items():
            link_type = BALLOTPEDIA_LABEL_MAP.get(label)
            if link_type and url:
                upsert_contact_link(
                    conn,
                    ContactLink(
                        candidate_id=cid,
                        link_type=link_type,
                        url=url,
                        source="ballotpedia" if bp_url else "web_search",
                    ),
                )
                any_found = True

        if any_found:
            found_count += 1

    conn.commit()
    save_cache(cache, cache_path)
    logger.info("Search found contact info for %d / %d candidates.", found_count, len(remaining))
    return found_count


def write_results_to_csv(conn: sqlite3.Connection, csv_path: str) -> None:
    """Add found_campaign_url column to the audit CSV from DB contact_links.

    Args:
        conn: Open database connection.
        csv_path: Path to bad_wikipedia_urls.csv.
    """
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return

    ids = [int(r["candidate_id"]) for r in rows]
    ph = ",".join("?" * len(ids))
    found: dict[int, str] = {
        row[0]: row[1]
        for row in conn.execute(
            f"""SELECT candidate_id, url FROM contact_links
                WHERE link_type = 'campaign_site'
                AND candidate_id IN ({ph})""",
            ids,
        )
    }

    fieldnames = list(rows[0].keys())
    if "found_campaign_url" not in fieldnames:
        fieldnames.append("found_campaign_url")

    for row in rows:
        row["found_campaign_url"] = found.get(int(row["candidate_id"]), "")

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    resolved = sum(1 for r in rows if r["found_campaign_url"])
    logger.info(
        "Updated %s: %d / %d candidates now have a found_campaign_url.",
        csv_path, resolved, len(rows),
    )


def main() -> None:
    """Entry point."""
    with open_db(DB_FILENAME) as conn:
        targets = load_target_candidates(conn, CSV_PATH)
        logger.info("Loaded %d candidates still missing campaign_site.", len(targets))

        if not targets:
            logger.info("Nothing to do — writing current DB results to CSV.")
            write_results_to_csv(conn, CSV_PATH)
            return

        enriched_ids = run_enrich(conn, targets)
        run_search(conn, targets, enriched_ids)
        write_results_to_csv(conn, CSV_PATH)


if __name__ == "__main__":
    main()
