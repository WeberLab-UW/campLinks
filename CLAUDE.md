# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**camplinks** is a generalizable pipeline for scraping US political election data from Wikipedia and enriching it with candidate contact information from Ballotpedia and web search. Data is stored in a normalized SQLite database.

Supported race types: US House, US Senate (extensible to Governor, AG, etc. by adding scraper classes).

## Package Structure

```
camplinks/
  __main__.py        # CLI entry point (argparse)
  pipeline.py        # Orchestrator: scrape -> enrich -> search
  db.py              # SQLite schema, CRUD helpers (upsert semantics)
  models.py          # Dataclasses: Election, Candidate, ContactLink
  http.py            # Shared fetch_soup + ddg_search
  cache.py           # JSON cache for incremental search
  wiki_parsing.py    # Shared Wikipedia HTML parsing utilities
  enrich.py          # Wikipedia campaign site extraction
  search.py          # Ballotpedia + DDG web search
  scrapers/
    base.py          # BaseScraper ABC
    house.py         # US House scraper (standard, California, RCV formats)
    senate.py        # US Senate scraper
convert_to_tidy.py   # One-time migration: legacy CSV -> SQLite
```

## Commands

```bash
# Install
uv sync

# Run pipeline
python -m camplinks --year 2024 --race house                    # full pipeline
python -m camplinks --year 2024 --race senate --stage scrape    # scrape only
python -m camplinks --year 2024 --race all                      # all race types

# Migrate legacy CSV
python convert_to_tidy.py --csv house_races_2024.csv

# Tests
pytest tests/                                           # all tests
pytest tests/test_db.py::TestUpsertElection             # single class
pytest tests/test_db.py::TestUpsertElection::test_insert_returns_id  # single test

# Type checking & linting
mypy camplinks/
ruff format .
ruff check .
```

## Database Schema

Three normalized tables in `camplinks.db`:
- **elections** — one row per contest (state + race_type + year + district)
- **candidates** — one row per candidate per election (party, name, vote_pct, is_winner)
- **contact_links** — one row per link per candidate (link_type, url, source)

All writes use upsert (ON CONFLICT) so stages are idempotent and safe to re-run.

## Architecture Notes

### Adding a New Race Type
1. Create `camplinks/scrapers/{race}.py` with a class extending `BaseScraper`
2. Implement `build_index_url()`, `collect_state_urls()`, `parse_state_page()`
3. Call `register_scraper("name", MyScraperClass)` at module level
4. Import the module in `camplinks/__main__.py`

### Wikipedia HTML Parsing
- Headings wrapped in `<div class="mw-heading mw-headingN">` — sibling traversal must check inside wrappers.
- Three House table formats: **standard** (`wikitable plainrowheaders`), **California** (combined primary+general), **RCV** (Alaska `wikitable sortable`).
- Senate uses the same `wikitable plainrowheaders` + `vcard` row format as House.
- Shared parsing in `wiki_parsing.py`: `find_preceding_heading`, `parse_candidate_row`, `is_general_election_table`.

### Search Strategy (camplinks/search.py)
- **Tier 1 (Ballotpedia):** DDG `site:ballotpedia.org` search, then parses `<div class="infobox person">` Contact section.
- **Tier 2 (Web search fallback):** Multiple DDG query variations with heuristic URL scoring (0.0-1.0). Early-stops at confidence >= 0.5.
- Cache key: `party|state|district|name`. Auto-saves every 25 candidates.

### Rate Limiting
- Wikipedia: 0.5s delay. Ballotpedia: 1.5s delay. DuckDuckGo: 3.0s with exponential backoff (30s, 60s, 120s).

## Tech Stack Constraints
- **Polars only** (never pandas), **orjson** for JSON, **SQLite** for database.
- **ddgs v9+** — import as `from ddgs import DDGS`.
- **uv** for packages, **ruff** for formatting/linting.
- All functions require type hints and docstrings per `~/Desktop/CLAUDE.md`.
