"""Find campaign websites and contact info via Ballotpedia and web search.

Two-tier strategy:
  1. Search DuckDuckGo for the candidate's Ballotpedia page, then scrape
     all contact/social links from the Ballotpedia infobox.
  2. For candidates still missing a campaign website, run a general web
     search and apply heuristics to identify likely campaign URLs.

Results are written to the contact_links table.
"""

from __future__ import annotations

import logging
import sqlite3
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

from camplinks.cache import (
    CACHE_FILE,
    SAVE_INTERVAL,
    load_cache,
    make_cache_key,
    save_cache,
)
from camplinks.db import (
    get_candidates_missing_link,
    update_candidate_ballotpedia_url,
    upsert_contact_link,
)
from camplinks.http import ddg_search, fetch_soup
from camplinks.models import BALLOTPEDIA_LABEL_MAP, ContactLink

logger = logging.getLogger(__name__)

BALLOTPEDIA_DELAY_S: float = 1.5

# Domains to skip in Tier-2 web search scoring
SKIP_DOMAINS: frozenset[str] = frozenset(
    {
        "ballotpedia.org",
        "wikipedia.org",
        "fec.gov",
        "opensecrets.org",
        "facebook.com",
        "twitter.com",
        "x.com",
        "youtube.com",
        "linkedin.com",
        "instagram.com",
        "reddit.com",
        "tiktok.com",
        "nytimes.com",
        "cnn.com",
        "foxnews.com",
        "washingtonpost.com",
        "politico.com",
        "nbcnews.com",
        "abcnews.go.com",
        "cbsnews.com",
        "apnews.com",
        "reuters.com",
        "thehill.com",
        "npr.org",
        "bbc.com",
        "usatoday.com",
    }
)


# ── Tier 1: Ballotpedia ───────────────────────────────────────────────────


def find_ballotpedia_url(
    name: str,
    state: str,
    race_type: str = "congress",
) -> str:
    """Search DDG for a candidate's Ballotpedia page.

    Args:
        name: Candidate full name.
        state: US state name.
        race_type: Race keyword for search (e.g. "congress", "senate").

    Returns:
        The Ballotpedia page URL, or empty string if not found.
    """
    query = f'site:ballotpedia.org "{name}" {state} {race_type} 2024'
    results = ddg_search(query, max_results=5)
    for r in results:
        href = r.get("href", "")
        if "ballotpedia.org/" in href and "/wiki/" not in href:
            return href
    return ""


def extract_all_contact_links(soup: BeautifulSoup) -> dict[str, str]:
    """Extract all contact/social links from a Ballotpedia candidate page.

    Parses the infobox "Contact" section for links like campaign website,
    campaign Facebook, personal LinkedIn, etc.

    Args:
        soup: Parsed Ballotpedia page.

    Returns:
        Dict mapping lowercased label (e.g. "campaign website") to URL.
    """
    links: dict[str, str] = {}
    infobox = soup.find("div", class_="infobox person")
    if not infobox or not isinstance(infobox, Tag):
        return links

    contact_header: Tag | None = None
    for div in infobox.find_all("div", class_="widget-row"):
        if not isinstance(div, Tag):
            continue
        if div.get_text(strip=True) == "Contact":
            contact_header = div
            break

    if contact_header is None:
        return links

    for sib in contact_header.find_next_siblings("div", class_="widget-row"):
        if not isinstance(sib, Tag):
            continue
        classes = sib.get("class") or []
        if "white" not in classes:
            break
        a_tag = sib.find("a")
        if a_tag and isinstance(a_tag, Tag) and a_tag.get("href"):
            label = a_tag.get_text(strip=True).lower()
            links[label] = str(a_tag["href"])

    return links


# ── Tier 2: Web search ────────────────────────────────────────────────────


def score_campaign_url(
    url: str,
    title: str,
    body: str,
    candidate_last_name: str,
    state: str,
) -> float:
    """Score a search result for likelihood of being a campaign website.

    Args:
        url: The result URL.
        title: The result title text.
        body: The result snippet text.
        candidate_last_name: Candidate's last name.
        state: State name.

    Returns:
        Score between 0.0 and 1.0.
    """
    score = 0.0
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    for skip in SKIP_DOMAINS:
        if skip in domain:
            return 0.0

    if ".gov" in domain:
        return 0.0

    last = candidate_last_name.lower().replace("'", "").replace("-", "")
    domain_clean = domain.replace("-", "").replace(".", "")

    if last in domain_clean:
        score += 0.4

    campaign_words = [
        "forcongress",
        "forsenate",
        "elect",
        "vote",
        "campaign",
        "committee",
    ]
    for word in campaign_words:
        if word in domain_clean:
            score += 0.2
            break

    if parsed.path in ("", "/", "/index.html"):
        score += 0.1

    combined = f"{title} {body}".lower()
    if "campaign" in combined or "congress" in combined or "senate" in combined:
        score += 0.1
    if "official" in combined:
        score += 0.05

    if state.lower() in combined:
        score += 0.05

    if domain.endswith(".com") or domain.endswith(".org"):
        score += 0.05

    return min(score, 1.0)


def search_campaign_site_web(
    name: str,
    state: str,
    district: str,
    race_type: str = "congress",
) -> str:
    """Search the open web for a candidate's campaign website.

    Tries multiple query variations and returns the highest-scoring
    result above a confidence threshold.

    Args:
        name: Candidate full name.
        state: State name.
        district: District identifier.
        race_type: Race keyword for search queries.

    Returns:
        Campaign website URL, or empty string.
    """
    last_name = name.split()[-1] if name.split() else name

    queries = [
        f'"{name}" {state} 2024 {race_type} campaign official website',
        f'"{name}" for {race_type} 2024 {state}',
    ]

    best_url = ""
    best_score = 0.0

    for query in queries:
        results = ddg_search(query, max_results=8)
        for r in results:
            href = r.get("href", "")
            title = r.get("title", "")
            body = r.get("body", "")
            s = score_campaign_url(href, title, body, last_name, state)
            if s > best_score:
                best_score = s
                best_url = href

        if best_score >= 0.5:
            break

    if best_score >= 0.3:
        return best_url
    return ""


# ── Orchestration ──────────────────────────────────────────────────────────


def _race_keyword(race_type: str) -> str:
    """Map a race_type to a search keyword.

    Args:
        race_type: Canonical race type (e.g. "US House").

    Returns:
        Search keyword string.
    """
    mapping = {
        "US House": "congress",
        "US Senate": "senate",
        "Governor": "governor",
    }
    return mapping.get(race_type, "election")


def find_candidate_info(
    name: str,
    state: str,
    district: str,
    race_type: str = "congress",
) -> dict[str, str]:
    """Find all available contact info for a single candidate.

    Runs Tier 1 (Ballotpedia) first, then Tier 2 (web search) if
    no campaign website was found.

    Args:
        name: Candidate full name.
        state: State name.
        district: District identifier.
        race_type: Race keyword for search.

    Returns:
        Dict mapping contact labels to URLs.
    """
    contacts: dict[str, str] = {}

    bp_url = find_ballotpedia_url(name, state, race_type)
    if bp_url:
        try:
            soup = fetch_soup(bp_url, delay_s=BALLOTPEDIA_DELAY_S)
            contacts = extract_all_contact_links(soup)
            contacts["_ballotpedia_url"] = bp_url
        except requests.RequestException as exc:
            logger.error("Ballotpedia fetch failed for %s: %s", name, exc)
        except (AttributeError, KeyError, ValueError, TypeError) as exc:
            logger.error("Ballotpedia parse error for %s: %s", name, exc)

    if "campaign website" not in contacts:
        campaign_url = search_campaign_site_web(name, state, district, race_type)
        if campaign_url:
            contacts["campaign website"] = campaign_url

    return contacts


def search_all_candidates(
    conn: sqlite3.Connection,
    cache_path: str = CACHE_FILE,
    year: int | None = None,
    race_type: str | None = None,
) -> int:
    """Find contact info for all candidates missing a campaign site.

    Args:
        conn: Open database connection.
        cache_path: Path for the incremental cache file.
        year: Optional filter by election year.
        race_type: Optional filter by race type.

    Returns:
        Number of candidates with new contact info found.
    """
    targets = get_candidates_missing_link(
        conn, "campaign_site", year=year, race_type=race_type
    )

    if not targets:
        logger.info("No candidates need contact search.")
        return 0

    logger.info("Found %d candidates needing search.", len(targets))

    cache = load_cache(cache_path)
    logger.info("Loaded cache with %d entries.", len(cache))

    processed = 0
    found_count = 0

    for row in tqdm(targets, desc="Searching candidate contacts", unit="candidate"):
        cid = row["candidate_id"]
        name = row["candidate_name"]
        state = row["state"]
        district = row["district"] or ""
        party = row["party"]
        rt = row["race_type"]

        cache_key = make_cache_key(party, state, district, name)

        if cache_key in cache:
            contacts = cache[cache_key]
        else:
            keyword = _race_keyword(rt)
            contacts = find_candidate_info(name, state, district, keyword)
            cache[cache_key] = contacts
            processed += 1

            if processed % SAVE_INTERVAL == 0:
                save_cache(cache, cache_path)

        # Write contact links to DB
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

    logger.info(
        "Found contact info for %d / %d candidates (%d new searches).",
        found_count,
        len(targets),
        processed,
    )
    return found_count
