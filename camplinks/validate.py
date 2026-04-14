"""Validate campaign site URLs and look up archived versions.

For each candidate with a campaign_site contact link, checks whether
the URL is still accessible. If not, queries the Wayback Machine
Availability API and stores the archived URL as a
campaign_site_archived contact link.
"""

from __future__ import annotations

import logging
import random
import sqlite3
import time
from urllib.parse import urlparse

import orjson
import requests
from tqdm import tqdm

from camplinks.cache import load_cache, make_cache_key, save_cache
from camplinks.db import get_candidates_with_link, upsert_contact_link
from camplinks.http import HEADERS
from camplinks.models import ContactLink

logger = logging.getLogger(__name__)

WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_DELAY_S: float = 0.5
VALIDATE_CACHE_FILE = "validate_cache.json"
VALIDATE_SAVE_INTERVAL = 25
HEAD_TIMEOUT_S: float = 10


def check_url_accessible(url: str) -> bool:
    """Check whether a URL is accessible via HTTP.

    Uses a HEAD request first for efficiency, falling back to GET if
    the server does not support HEAD (405). Considers any status < 400
    as accessible.

    Args:
        url: The URL to check.

    Returns:
        True if the URL responds with a 2xx or 3xx status code.
    """
    try:
        resp = requests.head(
            url,
            headers=HEADERS,
            timeout=HEAD_TIMEOUT_S,
            allow_redirects=True,
        )
        if resp.status_code == 405:
            resp = requests.get(
                url,
                headers=HEADERS,
                timeout=HEAD_TIMEOUT_S,
                allow_redirects=True,
            )
        return resp.status_code < 400
    except requests.RequestException:
        return False


def query_wayback(url: str, year: int) -> str:
    """Query the Wayback Machine CDX API for a working snapshot from the election year.

    Fetches all snapshots for the given URL captured during the election
    year, checks each for accessibility, and returns a random working one.

    Args:
        url: The original URL to look up in the Wayback Machine.
        year: The election year to search snapshots within.

    Returns:
        A working Wayback Machine snapshot URL, or empty string if none found.
    """
    parsed_url = urlparse(url)
    root_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"
    lookup_url = root_url if parsed_url.path not in ("", "/") else url

    params = {
        "url": lookup_url,
        "output": "json",
        "from": f"{year}0101",
        "to": f"{year}1231",
        "fl": "timestamp",
        "limit": "20",
    }
    rows = None
    for attempt in range(3):
        time.sleep(WAYBACK_DELAY_S * (attempt + 1))
        try:
            resp = requests.get(
                WAYBACK_CDX_URL,
                params=params,
                headers=HEADERS,
                timeout=60,
            )
            resp.raise_for_status()
            rows = orjson.loads(resp.content)
            break
        except requests.Timeout:
            logger.warning(
                "Wayback CDX timeout for %s (attempt %d/3)", url, attempt + 1
            )
        except requests.RequestException as exc:
            logger.error("Wayback CDX API error for %s: %s", url, exc)
            return ""
        except orjson.JSONDecodeError as exc:
            logger.error("Wayback CDX API parse error for %s: %s", url, exc)
            return ""
    if rows is None:
        logger.error("Wayback CDX API failed after 3 attempts for %s", url)
        return ""

    # rows[0] is the header ["timestamp"]; rest are data rows
    if not rows or len(rows) < 2:
        return ""

    timestamps = [row[0] for row in rows[1:]]
    snapshot_urls = [f"https://web.archive.org/web/{ts}/{url}" for ts in timestamps]

    return random.choice(snapshot_urls)


def validate_campaign_sites(
    conn: sqlite3.Connection,
    cache_path: str = VALIDATE_CACHE_FILE,
    year: int | None = None,
    race_type: str | None = None,
    election_stage: str | None = "general",
) -> int:
    """Validate campaign site URLs and archive inaccessible ones.

    For each candidate with a campaign_site link, checks if the URL is
    accessible. If not, queries the Wayback Machine for an archived
    snapshot and writes it as a campaign_site_archived contact link.

    Args:
        conn: Open database connection.
        cache_path: Path for the incremental validation cache file.
        year: Optional filter by election year.
        race_type: Optional filter by race type.
        election_stage: Optional filter by election stage. Defaults to
            "general" to avoid validating primary-only candidates.

    Returns:
        Number of archived URLs found and saved.
    """
    targets = get_candidates_with_link(
        conn,
        "campaign_site",
        exclude_link_type="campaign_site_archived",
        year=year,
        race_type=race_type,
        election_stage=election_stage,
    )
    if not targets:
        logger.info("No campaign sites to validate.")
        return 0

    logger.info("Found %d campaign sites to validate.", len(targets))
    cache = load_cache(cache_path)

    archived_count = 0
    accessible_count = 0
    inaccessible_count = 0
    processed = 0

    for row in tqdm(targets, desc="Validating campaign sites", unit="candidate"):
        cid: int = row["candidate_id"]
        url: str = row["campaign_site_url"]
        key = make_cache_key(
            row["party"],
            row["state"],
            row["district"] or "",
            row["candidate_name"],
        )

        if key in cache:
            entry = cache[key]
        else:
            accessible = check_url_accessible(url)
            if accessible:
                entry = {"status": "accessible"}
            else:
                wayback_url = query_wayback(url, row["year"])
                logger.info(
                    "Inaccessible: %s — %s", row["candidate_name"], url
                )
                if wayback_url:
                    logger.info("  Archived: %s", wayback_url)
                else:
                    logger.info("  No archive found.")
                entry = {
                    "status": "inaccessible",
                    "wayback_url": wayback_url,
                }
            cache[key] = entry
            processed += 1

            if processed % VALIDATE_SAVE_INTERVAL == 0:
                save_cache(cache, cache_path)

        if entry["status"] == "accessible":
            accessible_count += 1
        else:
            inaccessible_count += 1
            wayback = entry.get("wayback_url", "")
            if wayback:
                upsert_contact_link(
                    conn,
                    ContactLink(
                        candidate_id=cid,
                        link_type="campaign_site_archived",
                        url=wayback,
                        source="wayback",
                    ),
                )
                archived_count += 1

    conn.commit()
    save_cache(cache, cache_path)

    logger.info(
        "Validation complete: %d checked, %d accessible, "
        "%d inaccessible (%d archived), %d new lookups.",
        len(targets),
        accessible_count,
        inaccessible_count,
        archived_count,
        processed,
    )
    return archived_count
