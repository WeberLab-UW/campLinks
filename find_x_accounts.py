"""Search for X/Twitter profile URLs for candidates missing or having invalid ones.

Reads missing_x_links.csv and writes found_account back to that file.

Three strategies (in order):
  1. Tweet URL    -> extract handle from path, construct profile URL directly.
  2. Ballotpedia  -> DDG site:ballotpedia.org search, scrape "campaign x" from infobox.
  3. DDG fallback -> site:x.com search and broader web search filtered to x.com.

Saves progress every SAVE_INTERVAL candidates (resumable).
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from urllib.parse import urlparse

import orjson
import requests
from tqdm import tqdm

from camplinks.http import ddg_search, fetch_soup
from camplinks.search import (
    BALLOTPEDIA_DELAY_S,
    _race_keyword,
    extract_all_contact_links,
    find_ballotpedia_url,
)

_BALLOTPEDIA_X_LABEL = "campaign x"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

INPUT_CSV = Path("missing_x_links.csv")
CACHE_FILE = Path("x_account_search_cache.json")
SAVE_INTERVAL = 25


def is_valid_profile(url: str) -> bool:
    """Return True if url is a valid X/Twitter profile URL."""
    try:
        p = urlparse(url)
        host = p.netloc.replace("www.", "")
        parts = [x for x in p.path.rstrip("/").split("/") if x]
        return host in ("x.com", "twitter.com") and len(parts) == 1
    except Exception:
        return False


def extract_handle_from_tweet(url: str) -> str:
    """Extract profile URL from a tweet link, e.g. x.com/handle/status/123 -> x.com/handle."""
    try:
        p = urlparse(url)
        host = p.netloc.replace("www.", "")
        parts = [x for x in p.path.rstrip("/").split("/") if x]
        if host in ("x.com", "twitter.com") and len(parts) >= 2 and parts[1] == "status":
            return f"https://x.com/{parts[0]}"
    except Exception:
        pass
    return ""


def search_ballotpedia_x(name: str, state: str, race_type: str) -> str:
    """Look up a candidate's X profile via their Ballotpedia infobox.

    Args:
        name: Candidate name.
        state: State name.
        race_type: Race type string (e.g. "US House").

    Returns:
        Valid x.com profile URL from Ballotpedia, or empty string.
    """
    keyword = _race_keyword(race_type)
    bp_url = find_ballotpedia_url(name, state, keyword)
    if not bp_url:
        return ""
    try:
        soup = fetch_soup(bp_url, delay_s=BALLOTPEDIA_DELAY_S)
        links = extract_all_contact_links(soup)
        url = links.get(_BALLOTPEDIA_X_LABEL, "")
        if url and is_valid_profile(url):
            return url
    except (requests.RequestException, AttributeError, ValueError):
        pass
    return ""


def search_x_ddg(name: str, state: str, race_type: str, year: str) -> str:
    """Search DDG for a candidate's X profile URL.

    Args:
        name: Candidate name.
        state: State name.
        race_type: Race type string.
        year: Election year.

    Returns:
        Best matching x.com profile URL, or empty string if not found.
    """
    query = f'"{name}" {state} {year} {race_type} site:x.com OR site:twitter.com'
    for r in ddg_search(query, max_results=5):
        url = r.get("href", "")
        if is_valid_profile(url):
            return url

    # Broader fallback: search without site: operator, filter results manually
    query2 = f'"{name}" {state} {year} {race_type} twitter OR "x.com"'
    for r in ddg_search(query2, max_results=10):
        url = r.get("href", "")
        if is_valid_profile(url):
            return url

    return ""


def load_cache(path: Path) -> dict[str, str]:
    """Load search cache from disk."""
    if path.exists():
        return orjson.loads(path.read_bytes())
    return {}


def save_cache(cache: dict[str, str], path: Path) -> None:
    """Save search cache to disk."""
    path.write_bytes(orjson.dumps(cache))


def make_key(row: dict[str, str]) -> str:
    """Build a unique cache key for a candidate row."""
    return "|".join([row["candidate_id"], row["candidate_name"], row["state"], row["year"]])


def main() -> None:
    """Entry point."""
    rows = list(csv.DictReader(INPUT_CSV.open(newline="")))
    fieldnames = list(rows[0].keys())
    if "found_account" not in fieldnames:
        fieldnames.append("found_account")
        for r in rows:
            r["found_account"] = ""

    # Only process rows not yet resolved
    to_process = [
        (i, r) for i, r in enumerate(rows)
        if not r.get("found_account")
    ]
    logger.info(
        "%d total rows, %d still need processing.",
        len(rows), len(to_process),
    )

    cache = load_cache(CACHE_FILE)
    auto_fixed = 0
    found = 0
    not_found = 0
    processed = 0

    for idx, row in tqdm(to_process, desc="Finding X accounts", unit="candidate"):
        key = make_key(row)
        invalid_url = row.get("invalid_url", "")

        if key in cache:
            rows[idx]["found_account"] = cache[key]
            if cache[key]:
                found += 1
            else:
                not_found += 1
            continue

        # Strategy 1: Ballotpedia infobox (all candidates)
        profile = search_ballotpedia_x(
            row["candidate_name"], row["state"], row["race_type"]
        )

        # Strategy 2: tweet URL -> extract handle directly
        if not profile and invalid_url:
            profile = extract_handle_from_tweet(invalid_url)
            if profile:
                auto_fixed += 1

        # Strategy 3: DDG search fallback
        if not profile:
            profile = search_x_ddg(
                row["candidate_name"], row["state"], row["race_type"], row["year"]
            )
        rows[idx]["found_account"] = profile
        cache[key] = profile
        if profile:
            found += 1
        else:
            not_found += 1

        processed += 1
        if processed % SAVE_INTERVAL == 0:
            save_cache(cache, CACHE_FILE)
            _write_csv(rows, fieldnames)

    save_cache(cache, CACHE_FILE)
    _write_csv(rows, fieldnames)
    logger.info(
        "Done: %d auto-fixed from tweet URLs, %d found via search, %d not found.",
        auto_fixed, found, not_found,
    )


def _write_csv(rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    """Write rows back to INPUT_CSV."""
    with INPUT_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
