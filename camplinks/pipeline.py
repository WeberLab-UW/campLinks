"""Pipeline orchestrator — chains scrape, enrich, and search stages.

Each stage is idempotent (upsert semantics) so it is safe to re-run
any stage without duplicating data.
"""

from __future__ import annotations

import logging
import sqlite3

from camplinks.db import init_schema, open_db
from camplinks.enrich import enrich_from_wikipedia
from camplinks.models import DB_FILENAME
from camplinks.scrapers import get_scraper
from camplinks.search import search_all_candidates

logger = logging.getLogger(__name__)


def run_pipeline(
    year: int,
    race: str,
    stage: str | None = None,
    db_path: str = DB_FILENAME,
) -> None:
    """Run the camplinks pipeline for a given race and year.

    Args:
        year: Election year (e.g. 2024).
        race: Race key (e.g. "house", "senate") or "all" for every
            registered scraper.
        stage: Optional stage filter — "scrape", "enrich", or "search".
            If None, all stages run in order.
        db_path: Path to the SQLite database file.
    """
    conn = open_db(db_path)
    init_schema(conn)

    try:
        _run(conn, year, race, stage)
    finally:
        conn.close()


def _run(
    conn: sqlite3.Connection,
    year: int,
    race: str,
    stage: str | None,
) -> None:
    """Internal pipeline execution.

    Args:
        conn: Open database connection.
        year: Election year.
        race: Race key or "all".
        stage: Stage filter or None for all.
    """
    from camplinks.scrapers import SCRAPER_REGISTRY

    # Determine which scrapers to run
    if race == "all":
        scraper_names = list(SCRAPER_REGISTRY.keys())
    else:
        scraper_names = [race]

    run_scrape = stage in (None, "scrape")
    run_enrich = stage in (None, "enrich")
    run_search = stage in (None, "search")

    # Stage 1: Scrape
    if run_scrape:
        for name in scraper_names:
            scraper_cls = get_scraper(name)
            scraper = scraper_cls()
            scraper.scrape_all(year, conn)

    # Stage 2: Enrich (race-agnostic — enriches all candidates with wiki URLs)
    if run_enrich:
        enrich_from_wikipedia(conn)

    # Stage 3: Search (race-agnostic — searches for all missing contacts)
    if run_search:
        race_type = None
        if race != "all":
            scraper_cls = get_scraper(race)
            race_type = scraper_cls.race_type
        search_all_candidates(conn, year=year, race_type=race_type)

    # Summary
    _print_summary(conn, year)


def _print_summary(conn: sqlite3.Connection, year: int) -> None:
    """Print a summary of the database contents for a given year.

    Args:
        conn: Open database connection.
        year: Election year to summarize.
    """
    elections = conn.execute(
        "SELECT COUNT(*) FROM elections WHERE year = ?", (year,)
    ).fetchone()[0]

    candidates = conn.execute(
        """\
        SELECT COUNT(*) FROM candidates c
        JOIN elections e ON c.election_id = e.election_id
        WHERE e.year = ?
        """,
        (year,),
    ).fetchone()[0]

    contacts = conn.execute(
        """\
        SELECT COUNT(*) FROM contact_links cl
        JOIN candidates c ON cl.candidate_id = c.candidate_id
        JOIN elections e ON c.election_id = e.election_id
        WHERE e.year = ?
        """,
        (year,),
    ).fetchone()[0]

    logger.info(
        "Database summary for %d: %d elections, %d candidates, %d contact links.",
        year,
        elections,
        candidates,
        contacts,
    )
