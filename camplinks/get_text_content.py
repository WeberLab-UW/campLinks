"""Scrape visible text from candidate campaign websites.

For each candidate with a campaign_site URL, fetches the home page plus
any policy and about subpages, cleans the text, and stores a random
40% sample in the ``content`` table of the database.
"""

from __future__ import annotations

import logging
import random
import re
import sqlite3
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; research-scraper/1.0)"}
FETCH_TIMEOUT_S: int = 15
CANDIDATE_DELAY_S: float = 1.0
PAGE_DELAY_S: float = 1.0

INVISIBLE_TAGS: frozenset[str] = frozenset(
    {"script", "style", "noscript", "head", "meta", "link", "template"}
)
POLICY_KEYWORDS: frozenset[str] = frozenset(
    {
        "issue", "issues", "policy", "policies", "platform", "agenda",
        "priorities", "priority", "positions", "position", "plans", "plan",
        "vision", "values", "focus", "reform"
    }
)
ABOUT_KEYWORDS: frozenset[str] = frozenset({"about", "meet"})


def init_content_table(conn: sqlite3.Connection) -> None:
    """Create the content table if it does not exist.

    Args:
        conn: Open SQLite connection.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS content (
            content_id       INTEGER PRIMARY KEY,
            candidate_id     INTEGER NOT NULL REFERENCES candidates(candidate_id),
            candidate_name   TEXT    NOT NULL,
            page_url         TEXT    NOT NULL,
            page_type        TEXT    NOT NULL,
            link_type        TEXT,
            race_type        TEXT,
            year             INTEGER,
            unprocessed_text TEXT,
            cleaned_text     TEXT,
            sampled_text     TEXT,
            UNIQUE(candidate_id, page_url)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_content_candidate ON content(candidate_id)"
    )
    # Migrations: add columns if they do not yet exist
    for col in ("link_type TEXT", "race_type TEXT", "year INTEGER"):
        try:
            conn.execute(f"ALTER TABLE content ADD COLUMN {col}")
        except Exception:
            pass  # column already exists
    conn.commit()


def _insert_content(
    conn: sqlite3.Connection,
    candidate_id: int,
    candidate_name: str,
    page_url: str,
    page_type: str,
    link_type: str,
    race_type: str,
    year: int,
    unprocessed_text: str,
    cleaned_text: str,
    sampled_text: str,
) -> None:
    """Insert a scraped page into the content table (skip on conflict, aka there candidate already exists for the given URL and race).

    Args:
        conn: Open SQLite connection.
        candidate_id: FK to candidates table.
        candidate_name: Display name of the candidate.
        page_url: URL of the scraped page.
        page_type: One of ``"home"``, ``"policy"``, or ``"about"``.
        link_type: One of ``"campaign_site"`` or ``"campaign_site_archived"``.
        race_type: Race type from the elections table (e.g. ``"Governor"``).
        year: Election year from the elections table.
        unprocessed_text: Raw visible text from the page.
        cleaned_text: Text after character cleaning.
        sampled_text: Random sentence-chunk sample of cleaned_text.
    """
    conn.execute(
        """
        INSERT INTO content
            (candidate_id, candidate_name, page_url, page_type, link_type,
             race_type, year, unprocessed_text, cleaned_text, sampled_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id, page_url) DO NOTHING
        """,
        (
            candidate_id, candidate_name, page_url, page_type, link_type,
            race_type, year, unprocessed_text, cleaned_text, sampled_text,
        ),
    )
    conn.commit()


def _load_scraped_ids(
    conn: sqlite3.Connection, skip_empty: bool = True
) -> set[int]:
    """Return candidate_ids that already have rows in the content table.

    Args:
        conn: Open SQLite connection.
        skip_empty: If True, exclude candidates whose sampled_text is NULL or
            empty so they are eligible to be re-scraped.

    Returns:
        Set of already-scraped candidate_id integers.
    """
    if skip_empty:
        rows = conn.execute(
            """
            SELECT DISTINCT candidate_id FROM content
            WHERE sampled_text IS NOT NULL AND sampled_text != ''
            """
        ).fetchall()
    else:
        rows = conn.execute("SELECT DISTINCT candidate_id FROM content").fetchall()
    return {row[0] for row in rows}


def _fetch_soup(url: str) -> BeautifulSoup | None:
    """Fetch a URL and return a BeautifulSoup object, or None on failure.

    Args:
        url: URL to fetch.

    Returns:
        Parsed BeautifulSoup or None if the request failed.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT_S)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return None


def _extract_visible_text(soup: BeautifulSoup) -> str:
    """Strip invisible tags and return visible text.

    Args:
        soup: Parsed BeautifulSoup object.

    Returns:
        Cleaned visible text string.
    """
    for tag in soup(INVISIBLE_TAGS):
        tag.decompose()
    raw = soup.get_text(separator=" ")
    lines = [line.strip() for line in raw.splitlines()]
    return " ".join(line for line in lines if line)


def _internal_links(
    soup: BeautifulSoup, base_url: str, keywords: frozenset[str]
) -> list[str]:
    """Find internal links whose path contains any of the given keywords.

    Args:
        soup: Parsed page.
        base_url: Base URL for resolving relative links.
        keywords: Path segment keywords to match.

    Returns:
        Deduplicated list of absolute URLs.
    """
    base_domain = urlparse(base_url).netloc
    seen: set[str] = set()
    links: list[str] = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        try:
            absolute = urljoin(base_url, href)
            parsed = urlparse(absolute)
        except ValueError:
            continue
        if parsed.scheme not in ("http", "https"):
            continue
        if parsed.netloc != base_domain:
            continue
        if absolute in seen:
            continue
        path_parts = set(parsed.path.lower().strip("/").split("/"))
        if path_parts & keywords:
            seen.add(absolute)
            links.append(absolute)
    return links


_ONCLICK_RE = re.compile(
    r"""(?:window\.)?location(?:\.href)?\s*=\s*['"]([^'"]+)['"]"""
)
_BUTTON_URL_ATTRS: tuple[str, ...] = ("data-href", "data-url", "data-target", "formaction")

#some pages may link out from buttons, not just the header. This also looks for them
def _button_links(
    soup: BeautifulSoup, base_url: str, keywords: frozenset[str]
) -> list[str]:
    """Find internal URLs reachable via buttons whose label matches keywords.

    Extracts URLs from ``onclick`` JS, ``data-href``/``data-url``/
    ``data-target``/``formaction`` attributes on ``<button>`` elements.
    Only returns URLs whose netloc matches the base domain.

    Args:
        soup: Parsed page.
        base_url: Base URL for resolving relative links and domain check.
        keywords: Button text keywords to match (case-insensitive).

    Returns:
        Deduplicated list of absolute URLs on the same domain.
    """
    base_domain = urlparse(base_url).netloc
    seen: set[str] = set()
    links: list[str] = []

    for btn in soup.find_all("button"):
        label = btn.get_text(separator=" ").lower().strip()
        label_words = set(re.split(r"\W+", label))
        if not (label_words & keywords):
            continue

        candidates: list[str] = []

        for attr in _BUTTON_URL_ATTRS:
            val = btn.get(attr, "").strip()
            if val:
                candidates.append(val)

        onclick = btn.get("onclick", "")
        if onclick:
            for match in _ONCLICK_RE.finditer(onclick):
                candidates.append(match.group(1))

        for href in candidates:
            absolute = urljoin(base_url, href)
            parsed = urlparse(absolute)
            if parsed.scheme not in ("http", "https"):
                continue
            if parsed.netloc != base_domain:
                continue
            if absolute not in seen:
                seen.add(absolute)
                links.append(absolute)

    return links


def _clean_text(text: str) -> str:
    """Remove characters outside letters, numbers, whitespace, and punctuation.

    Args:
        text: Raw text string.

    Returns:
        Cleaned text string.
    """
    if not isinstance(text, str):
        return text
    return re.sub(r"[^a-zA-Z0-9\s!@#$%&*()\:.,?'\"-]", "", text)


def _sample_text(text: str, fraction: float = 0.4, max_attempts: int = 5) -> str:
    """Return a contiguous ~40% sentence chunk starting at a random position.

    Retries up to max_attempts times to avoid chunks containing ``"ERROR"``.

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


def _scrape_candidate_pages(campaign_url: str) -> list[dict[str, str]]:
    """Scrape the home page and policy/about subpages for one candidate.

    Args:
        campaign_url: Root campaign site URL.

    Returns:
        List of dicts with keys ``page_url``, ``page_type``, ``visible_text``.
    """
    if not isinstance(campaign_url, str) or not campaign_url.strip():
        return []

    home_soup = _fetch_soup(campaign_url)
    if home_soup is None:
        return [
            {
                "page_url": campaign_url,
                "page_type": "home",
                "visible_text": "ERROR: could not fetch page",
            }
        ]

    pages: list[dict[str, str]] = [
        {
            "page_url": campaign_url,
            "page_type": "home",
            "visible_text": _extract_visible_text(home_soup),
        }
    ]

    policy_links = dict.fromkeys(
        _internal_links(home_soup, campaign_url, POLICY_KEYWORDS)
        + _button_links(home_soup, campaign_url, POLICY_KEYWORDS)
    )
    for link in policy_links:
        logger.info("  -> policy subpage: %s", link)
        sub = _fetch_soup(link)
        if sub:
            pages.append(
                {
                    "page_url": link,
                    "page_type": "policy",
                    "visible_text": _extract_visible_text(sub),
                }
            )
        time.sleep(PAGE_DELAY_S)

    about_links = dict.fromkeys(
        _internal_links(home_soup, campaign_url, ABOUT_KEYWORDS)
        + _button_links(home_soup, campaign_url, ABOUT_KEYWORDS)
    )
    for link in about_links:
        logger.info("  -> about subpage: %s", link)
        sub = _fetch_soup(link)
        if sub:
            pages.append(
                {
                    "page_url": link,
                    "page_type": "about",
                    "visible_text": _extract_visible_text(sub),
                }
            )
        time.sleep(PAGE_DELAY_S)

    return pages


def scrape_campaign_content(
    conn: sqlite3.Connection,
    year: int | None = None,
    race_type: str | None = None,
    election_stage: str | None = "general",
) -> int:
    """Scrape campaign site text for candidates missing content rows.

    Queries candidates with a ``campaign_site`` or ``campaign_site_archived``
    link, skips those already in the ``content`` table, and scrapes all
    remaining candidates.

    Args:
        conn: Open SQLite connection.
        year: Optional filter by election year.
        race_type: Optional filter by race type.
        election_stage: Optional filter by election stage.

    Returns:
        Number of candidates successfully scraped.
    """
    init_content_table(conn)

    query = """\
        SELECT c.candidate_id, c.candidate_name, cl.url, cl.link_type, e.race_type, e.year
        FROM candidates c
        JOIN elections e ON c.election_id = e.election_id
        JOIN contact_links cl ON cl.candidate_id = c.candidate_id
        WHERE cl.link_type IN ('campaign_site', 'campaign_site_archived')
          AND cl.url IS NOT NULL
          AND cl.url != ''
    """
    params: list[str | int] = []
    if election_stage is not None:
        query += " AND e.election_stage = ?"
        params.append(election_stage)
    if year is not None:
        query += " AND e.year = ?"
        params.append(year)
    if race_type is not None:
        query += " AND e.race_type = ?"
        params.append(race_type)

    rows = conn.execute(query, params).fetchall()
    already_scraped = _load_scraped_ids(conn)
    remaining = [r for r in rows if r[0] not in already_scraped]

    logger.info(
        "Skipping %d already-scraped candidates. %d remaining.",
        len(rows) - len(remaining),
        len(remaining),
    )

    scraped = 0
    for row in tqdm(remaining, desc="Scraping campaign content", unit="candidate"):
        candidate_id, candidate_name, url, link_type, row_race_type, row_year = (
            row[0], row[1], row[2], row[3], row[4], row[5]
        )
        logger.info("Scraping: %s", url)
        for page in _scrape_candidate_pages(url):
            ct = _clean_text(page["visible_text"])
            _insert_content(
                conn,
                candidate_id=candidate_id,
                candidate_name=candidate_name,
                page_url=page["page_url"],
                page_type=page["page_type"],
                link_type=link_type,
                race_type=row_race_type,
                year=row_year,
                unprocessed_text=page["visible_text"],
                cleaned_text=ct,
                sampled_text=_sample_text(ct),
            )
        scraped += 1
        time.sleep(CANDIDATE_DELAY_S)

    logger.info("Scraped content for %d candidates.", scraped)
    return scraped
