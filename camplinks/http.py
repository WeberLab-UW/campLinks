"""Shared HTTP and search utilities for camplinks.

Consolidates fetch_soup (used by all scrapers) and ddg_search (used by
the search module) into one place.
"""

from __future__ import annotations

import logging
import time

import requests
from bs4 import BeautifulSoup
from ddgs import DDGS
from ddgs.exceptions import DDGSException, RatelimitException

logger = logging.getLogger(__name__)

HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "WikiResearchBot/1.0 (educational research)"
    ),
}

BASE_URL = "https://en.wikipedia.org"
DEFAULT_DELAY_S: float = 0.5
DDG_DELAY_S: float = 3.0


def fetch_soup(url: str, delay_s: float = DEFAULT_DELAY_S) -> BeautifulSoup:
    """Fetch *url* and return a parsed BeautifulSoup tree.

    Args:
        url: Fully-qualified URL to fetch.
        delay_s: Polite crawl delay in seconds.

    Returns:
        Parsed BeautifulSoup document.

    Raises:
        requests.HTTPError: If the HTTP response is not OK.
    """
    time.sleep(delay_s)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


def ddg_search(
    query: str,
    max_results: int = 5,
    max_retries: int = 3,
) -> list[dict[str, str]]:
    """Run a DuckDuckGo text search with backoff on rate limits.

    Args:
        query: The search query string.
        max_results: Maximum results to return.
        max_retries: How many times to retry on rate limit.

    Returns:
        List of result dicts with 'title', 'href', 'body' keys.
    """
    backoff = 30.0
    for attempt in range(max_retries + 1):
        try:
            time.sleep(DDG_DELAY_S)
            with DDGS() as ddgs:
                return list(ddgs.text(query, max_results=max_results))
        except (RatelimitException, DDGSException) as exc:
            is_rate_limit = isinstance(exc, RatelimitException) or ("429" in str(exc))
            if is_rate_limit and attempt < max_retries:
                wait = backoff * (2**attempt)
                logger.info(
                    "DDG rate limited, waiting %.0fs (attempt %d/%d)",
                    wait,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "DDG search failed after %d attempts: %s",
                    attempt + 1,
                    exc,
                )
                return []
    return []
