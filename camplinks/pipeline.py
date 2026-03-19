"""Pipeline orchestrator — chains scrape, enrich, search, and validate stages.

Each stage is idempotent (upsert semantics) so it is safe to re-run
any stage without duplicating data.
"""

from __future__ import annotations

import logging
import sqlite3

from camplinks.db import init_schema, migrate_schema, open_db
from camplinks.enrich import enrich_from_wikipedia, enrich_wikipedia_urls
from camplinks.models import DB_FILENAME
from camplinks.scrapers import get_scraper
from camplinks.search import search_all_candidates
from camplinks.validate import validate_campaign_sites

logger = logging.getLogger(__name__)


def run_pipeline(
    year: int,
    race: str,
    stage: str | None = None,
    db_path: str = DB_FILENAME,
    election_stage: str | None = None,
) -> None:
    """Run the camplinks pipeline for a given race and year.

    Args:
        year: Election year (e.g. 2024).
        race: Race key (e.g. "house", "senate") or "all" for every
            registered scraper.
        stage: Optional stage filter — "scrape", "enrich", "search",
            or "validate". If None, all stages run in order.
        db_path: Path to the SQLite database file.
        election_stage: Optional election stage filter for
            enrich/search/validate. Defaults to "general" for those
            stages if not specified.
    """
    conn = open_db(db_path)
    migrate_schema(conn)
    init_schema(conn)

    try:
        _run(conn, year, race, stage, election_stage)
    finally:
        conn.close()


def _run(
    conn: sqlite3.Connection,
    year: int,
    race: str,
    stage: str | None,
    election_stage: str | None,
) -> None:
    """Internal pipeline execution.

    Args:
        conn: Open database connection.
        year: Election year.
        race: Race key or "all".
        stage: Stage filter or None for all.
        election_stage: Election stage filter for downstream stages.
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
    run_validate = stage in (None, "validate")

    # Stage 1: Scrape
    if run_scrape:
        for name in scraper_names:
            scraper_cls = get_scraper(name)
            scraper = scraper_cls()
            scraper.scrape_all(year, conn)

    # For downstream stages, default to "general" unless explicitly overridden
    downstream_stage = election_stage if election_stage is not None else "general"

    # Stage 2: Enrich (race-agnostic — enriches all candidates with wiki URLs)
    if run_enrich:
        race_type_filter = None
        if race != "all":
            scraper_cls = get_scraper(race)
            race_type_filter = scraper_cls.race_type
        enrich_wikipedia_urls(
            conn, year=year, race_type=race_type_filter, election_stage=downstream_stage
        )
        enrich_from_wikipedia(conn, election_stage=downstream_stage)

    # Stage 3: Search (race-agnostic — searches for all missing contacts)
    if run_search:
        race_type = None
        if race != "all":
            scraper_cls = get_scraper(race)
            race_type = scraper_cls.race_type
        search_all_candidates(
            conn, year=year, race_type=race_type, election_stage=downstream_stage
        )

    # Stage 4: Validate (race-agnostic — validates all campaign_site links)
    if run_validate:
        race_type = None
        if race != "all":
            scraper_cls = get_scraper(race)
            race_type = scraper_cls.race_type
        validate_campaign_sites(
            conn, year=year, race_type=race_type, election_stage=downstream_stage
        )

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

    stage_rows = conn.execute(
        """\
        SELECT election_stage, COUNT(*) FROM elections
        WHERE year = ? GROUP BY election_stage ORDER BY election_stage
        """,
        (year,),
    ).fetchall()
    for row in stage_rows:
        logger.info("  %s: %d elections", row[0], row[1])
