"""Race-specific Wikipedia scrapers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from camplinks.scrapers.base import BaseScraper

SCRAPER_REGISTRY: dict[str, type[BaseScraper]] = {}


def register_scraper(name: str, cls: type[BaseScraper]) -> None:
    """Register a scraper class under a short name.

    Args:
        name: Lookup key (e.g. "house", "senate").
        cls: The scraper class.
    """
    SCRAPER_REGISTRY[name] = cls


def get_scraper(name: str) -> type[BaseScraper]:
    """Retrieve a registered scraper class by name.

    Args:
        name: Lookup key.

    Returns:
        The scraper class.

    Raises:
        KeyError: If the name is not registered.
    """
    return SCRAPER_REGISTRY[name]
