"""Download images from campaign site pages for AI-labeled candidates.

Queries campaign_site_content for candidates where text_AI_result = 'AI', fetches
their campaign site pages, downloads all images, saves them to disk under
campaign-site-images/{candidate_id}/, and inserts a row per image into
campaign_site_content with content_type = 'image'.

Skips image URLs already present in campaign_site_content. Saves progress
every SAVE_INTERVAL images.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "camplinks.db"
IMAGE_DIR = Path(__file__).parent / "campaign-site-images"
SAVE_INTERVAL = 20
REQUEST_DELAY_S = 1.0
TIMEOUT_S = 15
MIN_IMAGE_BYTES = 5_000

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

MIME_TO_EXT = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


def fetch_soup(url: str) -> BeautifulSoup | None:
    """Fetch a URL and return a BeautifulSoup object, or None on failure.

    Args:
        url: Page URL to fetch.

    Returns:
        Parsed BeautifulSoup or None if the request fails.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except Exception as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return None


def extract_image_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    """Extract absolute image URLs from a page.

    Args:
        soup: Parsed page.
        base_url: Base URL for resolving relative paths.

    Returns:
        Deduplicated list of absolute image URLs with valid extensions.
    """
    seen: set[str] = set()
    results: list[str] = []
    for img in soup.find_all("img", src=True):
        src = img["src"].strip()
        absolute = urljoin(base_url, src)
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            continue
        ext = Path(parsed.path).suffix.lower()
        if ext not in IMAGE_EXTENSIONS:
            continue
        if absolute not in seen:
            seen.add(absolute)
            results.append(absolute)
    return results


def download_image(url: str, dest_path: Path) -> bool:
    """Download an image to disk.

    Args:
        url: Remote image URL.
        dest_path: Local path to save the image.

    Returns:
        True if downloaded successfully, False otherwise.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S, stream=True)
        resp.raise_for_status()
        data = resp.content
        if len(data) < MIN_IMAGE_BYTES:
            return False
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(data)
        return True
    except Exception as exc:
        logger.warning("Failed to download %s: %s", url, exc)
        return False


def load_ai_candidates(conn: sqlite3.Connection) -> list[dict]:
    """Load candidates with at least one AI-labeled page in campaign_site_content.

    Args:
        conn: Open SQLite connection.

    Returns:
        List of dicts with candidate_id, candidate_name, x_url (campaign site URL),
        race_type, year, state, required_compliance.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT
            c.candidate_id,
            c.candidate_name,
            cl.url AS site_url,
            csc.race_type,
            csc.year,
            csc.state,
            csc.required_compliance
        FROM campaign_site_content csc
        JOIN candidates c ON csc.candidate_id = c.candidate_id
        JOIN contact_links cl ON cl.candidate_id = c.candidate_id
        WHERE csc.text_AI_result IN ('AI', 'Mixed')
          AND cl.link_type = 'campaign_site'
          AND cl.url IS NOT NULL
          AND cl.url != ''
        """
    ).fetchall()
    return [
        {
            "candidate_id": r[0],
            "candidate_name": r[1],
            "site_url": r[2],
            "race_type": r[3],
            "year": r[4],
            "state": r[5],
            "required_compliance": r[6],
        }
        for r in rows
    ]


def load_scraped_image_urls(conn: sqlite3.Connection) -> set[str]:
    """Return page_urls already stored as images in campaign_site_content.

    Args:
        conn: Open SQLite connection.

    Returns:
        Set of image URLs already inserted.
    """
    rows = conn.execute(
        "SELECT page_url FROM campaign_site_content WHERE content_type = 'image'"
    ).fetchall()
    return {r[0] for r in rows}


def insert_image_row(
    conn: sqlite3.Connection,
    candidate_id: int,
    candidate_name: str,
    image_url: str,
    image_path: str,
    page_type: str,
    race_type: str,
    year: int,
    state: str,
    required_compliance: str,
) -> None:
    """Insert an image row into campaign_site_content.

    Args:
        conn: Open SQLite connection.
        candidate_id: FK to candidates.
        candidate_name: Candidate display name.
        image_url: Remote URL of the image (used as page_url).
        image_path: Relative local path where the image is saved.
        page_type: Page type (home/policy/about/unknown).
        race_type: Race type from elections.
        year: Election year.
        state: State abbreviation or name.
        required_compliance: Compliance level.
    """
    conn.execute(
        """
        INSERT INTO campaign_site_content
            (candidate_id, candidate_name, page_url, page_type, link_type,
             image_path, race_type, year, state, required_compliance, content_type)
        VALUES (?, ?, ?, ?, 'campaign_site', ?, ?, ?, ?, ?, 'image')
        ON CONFLICT(candidate_id, page_url) DO NOTHING
        """,
        (
            candidate_id, candidate_name, image_url, page_type,
            image_path, race_type, year, state, required_compliance,
        ),
    )


def classify_page_type(url: str) -> str:
    """Guess page type from URL path keywords.

    Args:
        url: Page URL.

    Returns:
        'policy', 'about', or 'home'.
    """
    path = urlparse(url).path.lower()
    policy_kw = {"issue", "issues", "policy", "policies", "platform", "agenda",
                 "priorities", "positions", "plans", "vision", "values"}
    about_kw = {"about", "meet", "bio"}
    parts = set(path.strip("/").split("/"))
    if parts & policy_kw:
        return "policy"
    if parts & about_kw:
        return "about"
    return "home"


def main() -> None:
    """Entry point."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode = WAL")

    candidates = load_ai_candidates(conn)
    already_scraped = load_scraped_image_urls(conn)

    logger.info("Found %d AI-labeled candidates to scrape images for.", len(candidates))

    total_saved = 0
    total_skipped = 0

    for cand in tqdm(candidates, desc="Candidates", unit="candidate"):
        cid = cand["candidate_id"]
        name = cand["candidate_name"]
        site_url = cand["site_url"]

        soup = fetch_soup(site_url)
        if soup is None:
            continue
        time.sleep(REQUEST_DELAY_S)

        image_urls = extract_image_urls(soup, site_url)
        page_type = classify_page_type(site_url)

        cand_dir = IMAGE_DIR / str(cid)

        for img_url in image_urls:
            if img_url in already_scraped:
                total_skipped += 1
                continue

            ext = Path(urlparse(img_url).path).suffix.lower() or ".jpg"
            filename = f"{abs(hash(img_url))}{ext}"
            dest_path = cand_dir / filename
            rel_path = str(dest_path.relative_to(Path(__file__).parent))

            success = download_image(img_url, dest_path)
            if not success:
                continue

            insert_image_row(
                conn,
                candidate_id=cid,
                candidate_name=name,
                image_url=img_url,
                image_path=rel_path,
                page_type=page_type,
                race_type=cand["race_type"],
                year=cand["year"],
                state=cand["state"],
                required_compliance=cand["required_compliance"],
            )
            already_scraped.add(img_url)
            total_saved += 1

            if total_saved % SAVE_INTERVAL == 0:
                conn.commit()
                logger.info("Saved %d images so far.", total_saved)

    conn.commit()
    conn.close()
    logger.info(
        "Done. %d images saved, %d skipped (already in DB).",
        total_saved, total_skipped,
    )


if __name__ == "__main__":
    main()
