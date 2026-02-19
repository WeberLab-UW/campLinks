"""Enrich candidates with campaign website URLs from Wikipedia.

For each candidate that has a Wikipedia page, fetches the page and
extracts the campaign website from the infobox (primary) or External
links section (fallback). Results are written to the contact_links table.
"""

from __future__ import annotations

import logging
import sqlite3

import requests
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

from camplinks.db import upsert_contact_link
from camplinks.http import fetch_soup
from camplinks.models import ContactLink

logger = logging.getLogger(__name__)


def extract_campaign_website(soup: BeautifulSoup) -> str:
    """Extract the campaign website URL from a politician's Wikipedia page.

    Checks the infobox first (most reliable), then falls back to the
    External links section.

    Args:
        soup: Parsed BeautifulSoup of the Wikipedia page.

    Returns:
        Campaign website URL string, or empty string if not found.
    """
    # --- Strategy 1: Infobox "Website" row ---
    infobox = soup.find("table", class_="infobox")
    if infobox and isinstance(infobox, Tag):
        for th in infobox.find_all("th", class_="infobox-label"):
            if th.get_text(strip=True).lower() == "website":
                td = th.find_next_sibling("td", class_="infobox-data")
                if td is None:
                    continue
                for a_tag in td.find_all("a", class_="external"):
                    if "campaign" in a_tag.get_text(strip=True).lower():
                        return str(a_tag["href"])
                all_links = td.find_all("a", class_="external")
                if len(all_links) == 1:
                    return str(all_links[0]["href"])
                for a_tag in all_links:
                    href = str(a_tag["href"])
                    if ".gov" not in href:
                        return href

    # --- Strategy 2: External links section ---
    heading: Tag | None = None
    h2 = soup.find("h2", id="External_links")
    if h2 and isinstance(h2, Tag):
        heading = h2
    else:
        span = soup.find("span", id="External_links")
        if span and span.parent and isinstance(span.parent, Tag):
            heading = span.parent

    if heading is not None:
        container: Tag = heading
        if (
            heading.parent
            and isinstance(heading.parent, Tag)
            and heading.parent.name == "div"
            and "mw-heading" in (heading.parent.get("class") or [])
        ):
            container = heading.parent

        for sib in container.find_next_siblings():
            if not isinstance(sib, Tag):
                continue
            if sib.name and sib.name.startswith("h"):
                break
            if sib.name == "div" and "mw-heading" in (sib.get("class") or []):
                break
            if sib.name == "ul":
                for li in sib.find_all("li", recursive=False):
                    text = li.get_text(strip=True).lower()
                    if "campaign" in text:
                        a_tag = li.find("a", class_="external")
                        if a_tag:
                            return str(a_tag["href"])

    return ""


def enrich_from_wikipedia(conn: sqlite3.Connection) -> int:
    """Fetch campaign websites for all candidates with Wikipedia URLs.

    Queries candidates that have a wikipedia_url but no campaign_site
    contact link, fetches their Wikipedia pages, and extracts campaign
    website URLs.

    Args:
        conn: Open database connection.

    Returns:
        Number of campaign sites found.
    """
    rows = conn.execute(
        """\
        SELECT c.candidate_id, c.wikipedia_url
        FROM candidates c
        WHERE c.wikipedia_url != ''
          AND c.candidate_id NOT IN (
              SELECT cl.candidate_id FROM contact_links cl
              WHERE cl.link_type = 'campaign_site'
          )
        """
    ).fetchall()

    if not rows:
        logger.info("No candidates need Wikipedia enrichment.")
        return 0

    # De-duplicate URLs to avoid redundant fetches
    url_to_ids: dict[str, list[int]] = {}
    for row in rows:
        url = row["wikipedia_url"]
        url_to_ids.setdefault(url, []).append(row["candidate_id"])

    logger.info(
        "Enriching %d candidates from %d unique Wikipedia pages...",
        len(rows),
        len(url_to_ids),
    )

    found = 0
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
                found += 1
        except requests.HTTPError as exc:
            logger.error("HTTP error for %s: %s", url, exc)
        except (AttributeError, KeyError, ValueError, TypeError) as exc:
            logger.error("Error parsing %s: %s", url, exc)

    conn.commit()
    logger.info(
        "Found campaign sites for %d / %d unique pages.", found, len(url_to_ids)
    )
    return found
