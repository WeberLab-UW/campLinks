"""CLI entry point for camplinks.

Usage::

    python -m camplinks --year 2024 --race house
    python -m camplinks --year 2024 --race senate --stage scrape
    python -m camplinks --year 2024 --race all
"""

from __future__ import annotations

import argparse
import logging

from camplinks.models import DB_FILENAME

# Import scrapers to trigger registration via register_scraper()
import camplinks.scrapers.attorney_general  # noqa: F401
import camplinks.scrapers.governor  # noqa: F401
import camplinks.scrapers.house  # noqa: F401
import camplinks.scrapers.judicial  # noqa: F401
import camplinks.scrapers.municipal  # noqa: F401
import camplinks.scrapers.senate  # noqa: F401
import camplinks.scrapers.special_house  # noqa: F401
import camplinks.scrapers.state_leg_special  # noqa: F401
import camplinks.scrapers.state_legislative  # noqa: F401


def main() -> None:
    """Parse CLI arguments and run the pipeline."""
    parser = argparse.ArgumentParser(
        prog="camplinks",
        description="Scrape and enrich US political election data.",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Election year (e.g. 2024)",
    )
    parser.add_argument(
        "--race",
        type=str,
        required=True,
        help='Race type: "house", "senate", or "all"',
    )
    parser.add_argument(
        "--stage",
        type=str,
        default=None,
        choices=["scrape", "enrich", "search"],
        help="Run only this pipeline stage (default: all stages)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=DB_FILENAME,
        help=f"SQLite database path (default: {DB_FILENAME})",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    from camplinks.pipeline import run_pipeline

    run_pipeline(
        year=args.year,
        race=args.race,
        stage=args.stage,
        db_path=args.db,
    )


if __name__ == "__main__":
    main()
