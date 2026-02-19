"""JSON-based incremental search cache for camplinks.

Persists search results to disk so interrupted runs can resume without
re-querying already-found candidates.
"""

from __future__ import annotations

import pathlib

import orjson

CACHE_FILE = "campaign_search_cache.json"
SAVE_INTERVAL = 25


def make_cache_key(
    party: str,
    state: str,
    district: str,
    name: str,
) -> str:
    """Build a deterministic cache key for a candidate.

    Args:
        party: Party name.
        state: State name.
        district: District identifier.
        name: Candidate full name.

    Returns:
        A pipe-delimited string key.
    """
    return f"{party}|{state}|{district}|{name}"


def load_cache(path: str = CACHE_FILE) -> dict[str, dict[str, str]]:
    """Load the incremental search cache from disk.

    Args:
        path: Path to the JSON cache file.

    Returns:
        Mapping of cache key to contact-links dict.
    """
    p = pathlib.Path(path)
    if p.exists():
        data = orjson.loads(p.read_bytes())
        if isinstance(data, dict):
            return data
    return {}


def save_cache(
    cache: dict[str, dict[str, str]],
    path: str = CACHE_FILE,
) -> None:
    """Persist the search cache to disk.

    Args:
        cache: The cache dict to save.
        path: Path to write the JSON file.
    """
    pathlib.Path(path).write_bytes(orjson.dumps(cache, option=orjson.OPT_INDENT_2))
