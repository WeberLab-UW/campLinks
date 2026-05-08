# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**camplinks** is a generalizable pipeline for scraping US political election data from Wikipedia and enriching it with candidate contact information from Ballotpedia and web search. Data is stored in a normalized SQLite database.

Supported race types: US House, US Senate, Governor, Attorney General, State House, State Senate, State Supreme Court, Mayor.

## Scraper Registry Keys

| `--race` key | race_type in DB | Notes |
|---|---|---|
| `house` | US House | Standard, California, RCV formats |
| `senate` | US Senate | Statewide |
| `governor` | Governor | Statewide |
| `attorney_general` | Attorney General | Falls back to gubernatorial index if AG index 404s |
| `special_house` | US House | Same DB race_type as regular House |
| `state_leg` | State House / State Senate | Chamber determined per-page |
| `state_leg_special` | State House / State Senate | Filters specials from state leg index |
| `municipal` | Mayor | Uses Wikipedia category page as index |
| `bp_municipal` | Mayor | Ballotpedia, top-100 cities by population |
| `bp_governor` | Governor | Ballotpedia, all 50 states (fallback if index fails) |
| `judicial` | State Supreme Court | Contested + retention + Pattern A tables |

## Commands

```bash
# Install
uv sync

# Run pipeline
python -m camplinks --year 2024 --race house                    # full pipeline
python -m camplinks --year 2024 --race senate --stage scrape    # scrape only
python -m camplinks --year 2025 --race governor --stage scrape  # gubernatorial
python -m camplinks --year 2025 --race municipal --stage scrape # mayoral (Wikipedia)
python -m camplinks --year 2023 --race bp_municipal --stage scrape # mayoral (Ballotpedia, top-100 cities)
python -m camplinks --year 2026 --race bp_governor --stage scrape # gubernatorial (Ballotpedia, all states)
python -m camplinks --year 2024 --race all                      # all race types
python -m camplinks --year 2024 --race house --db custom.db     # custom DB path
python -m camplinks --year 2026 --race senate --stage scrape    # scrape 2026 (captures primaries)
python -m camplinks --year 2024 --race house --election-stage primary --stage search  # search primary candidates
python -m camplinks --year 2024 --race house --stage archive   # politicalemails.org lookup (opt-in)

# Migrate legacy CSV
python convert_to_tidy.py --csv house_races_2024.csv --db camplinks.db

# Tests
uv run pytest tests/                                              # all tests
uv run pytest tests/test_db.py::TestUpsertElection                # single class
uv run pytest tests/test_db.py::TestUpsertElection::test_insert_returns_id  # single test

# Type checking & linting
uv run mypy camplinks/
uv run ruff format .
uv run ruff check .
```

Console script `camplinks` is also installed via `pyproject.toml` (`[project.scripts]`), equivalent to `python -m camplinks`.

## Database Schema

`camplinks.db` is gitignored (~350MB) and distributed via GitHub Releases, not tracked in source. After cloning, fetch the latest release artifact or rebuild from scratch via the scrape stage. The `.gitignore` rule `*.db` covers it; do not re-add the file to tracking.

Three normalized tables in `camplinks.db`:
- **elections** -- one row per contest, UNIQUE(state, race_type, year, district, election_stage)
- **candidates** -- one row per candidate per election, UNIQUE(election_id, candidate_name)
- **contact_links** -- one row per link per candidate, UNIQUE(candidate_id, link_type)

Plus four tables for the politicalemails.org archive lookup (populated by `--stage archive`):
- **archive_organizations** -- cached org metadata, PK `org_id` (string from politicalemails.org URL).
- **archive_lookups** -- one row per candidate that has been checked, PK `candidate_id`. Columns: `has_entry`, `match_count`, `total_messages`, `status` (`no_match`/`single`/`multiple`/`error`), `checked_at`. The presence of a row means "already checked, do not re-query."
- **candidate_archive_matches** -- junction table, composite PK `(candidate_id, org_id)`.
- **archive_messages** -- reserved for phase-2 email body storage (one row per email, FK to org).

`election_stage` values: `general`, `primary`, `runoff`

`link_type` values: `campaign_site`, `campaign_site_archived`, `campaign_facebook`, `campaign_x`, `campaign_instagram`, `personal_website`, `personal_facebook`, `personal_linkedin`

`source` values: `wikipedia`, `ballotpedia`, `web_search`, `wayback`, `csv_import`

All writes use upsert (ON CONFLICT) so stages are idempotent and safe to re-run.

### Upsert Merge Semantics

Upserts are not simple overwrites -- they merge intelligently:
- **elections:** Updates `wikipedia_url` only if the new value is non-empty.
- **candidates:** Preserves non-empty `wikipedia_url`/`ballotpedia_url`, takes max `is_winner` flag.
- **contact_links:** Overwrites URL and source on conflict (one URL per link_type per candidate).

## Architecture Notes

### Pipeline Stages

Four default stages run in order: **scrape -> enrich -> search -> validate**. Each is independently runnable via `--stage` and idempotent. A fifth stage (**archive**) is opt-in only — it must be invoked explicitly via `--stage archive`. Orchestration lives in `camplinks/pipeline.py:run_pipeline()`, which the CLI dispatches to.

1. **Scrape:** Fetches Wikipedia index page for the race/year, follows state links, parses candidate tables (general, primary, and runoff). Commits after each state.
2. **Enrich:** For candidates with `wikipedia_url` but no `campaign_site` link, fetches their Wikipedia page and extracts the campaign website from the infobox or External links section.
3. **Search:** For candidates still missing `campaign_site`, runs two-tier search. Uses `campaign_search_cache.json` for resumability (auto-saves every 25 candidates).
4. **Validate:** Checks campaign site URLs for accessibility. For dead links, queries the Wayback Machine and stores the archived URL as a `campaign_site_archived` contact link.
5. **Archive (opt-in):** For candidates without an `archive_lookups` row, searches politicalemails.org by name, enriches each hit with profile metadata, filters to hits matching the candidate's state, and persists results. Idempotent via the `archive_lookups` row (delete to re-check). Excluded from the default pipeline because it makes 1 search + N profile fetches per candidate at 1.0s rate limit — hours over the full DB.

Enrich, search, validate, and archive default to `election_stage="general"` to avoid wasting rate-limited queries on primary-only candidates. Override with `--election-stage`.

### Scraper Registry Pattern

Scrapers register themselves at import time via `register_scraper("name", MyClass)` in `camplinks/scrapers/__init__.py`. The CLI imports scraper modules in `__main__.py` to trigger registration, then looks up scraper classes from `SCRAPER_REGISTRY` by the `--race` argument.

### Adding a New Race Type
1. Create `camplinks/scrapers/{race}.py` with a class extending `BaseScraper`
2. Implement `build_index_url()`, `collect_state_urls()`, `parse_state_page()`
3. Call `register_scraper("name", MyScraperClass)` at module level
4. Import the module in `camplinks/__main__.py`

Enrichment and search stages work automatically for any race type (they query the candidates table, not race-specific logic).

### Wikipedia HTML Parsing Gotchas
- Headings are wrapped in `<div class="mw-heading mw-headingN">` -- `find_preceding_heading()` in `wiki_parsing.py` checks both bare heading tags AND headings nested inside these wrappers.
- Three House table formats: **standard** (`wikitable plainrowheaders`), **California** (combined primary+general with `<th>` section headers), **RCV** (Alaska `wikitable sortable` with `<span class="vcard">` instead of full row vcards). The House scraper falls back from standard to RCV if no results found.
- `parse_candidate_row()` identifies cells by CSS class (`class="org"` for party) rather than column index, since colspan variations shift column positions.
- **Two table patterns across scrapers:** Pattern B (standard `wikitable plainrowheaders` + `vcard` rows) used by most scrapers; Pattern A (basic `wikitable` with `<th scope="row">`) used by some judicial and municipal pages. `parse_basic_wikitable_row()` handles Pattern A.
- **Retention elections** (judicial): Yes/No vote format instead of candidate-vs-candidate. `_parse_retention_table()` in `judicial.py` handles this.
- **Municipal scraper index:** Uses Wikipedia category page (`Category:{year}_United_States_mayoral_elections`) with `<div class="mw-category">` structure instead of a standard article.
- **Heading search order:** For district extraction, search h2 headings first, then h3 -- searching both simultaneously can match h3 "General election" instead of h2 "District N".
- **Table classification:** `classify_election_table()` returns `"general"`, `"primary"`, `"runoff"`, or `None` by checking caption text then preceding h3/h4 headings. `extract_primary_party()` walks backward to find the h2 heading (e.g. "Republican primary") to set candidate party for primary tables.

### Ballotpedia Municipal Scraper (`bp_municipal`)
- Overrides `scrape_all()` because the city list comes from a static top-100 page, not a year-specific index.
- Three HTML formats: standard `.votebox`, `.rcvvotebox` (RCV cities like SF, NYC), and runoff voteboxes (Houston, Nashville).
- Election stage detected from `<h5 class="votebox-header-election-type">` text within each votebox.
- Cities with no election that year return 404 -- caught and logged at INFO level, not ERROR.
- State field uses `"City, State"` format (e.g., "Houston, Texas") to prevent collisions (Portland OR vs ME).
- Hardcoded fallback city list (`_FALLBACK_CITIES`) if the top-100 page is unavailable.

### Ballotpedia Governor Scraper (`bp_governor`)
- Overrides `scrape_all()` for Ballotpedia-specific delay and 404 handling (not every state has a governor race every cycle).
- Parses index page (`Gubernatorial_elections,_{year}`) to discover which states have races; falls back to all 50 states if index fails.
- Skips lieutenant governor pages (URLs containing "lieutenant") to avoid data contamination.
- Shares votebox parsing code with `bp_municipal` via `ballotpedia_parsing.py`.
- URL pattern: `{State}_gubernatorial_election,_{year}` (comma format, not parentheses).

### Ballotpedia Shared Parsing (`ballotpedia_parsing.py`)
- Extracted from `ballotpedia_municipal.py` to avoid duplication across Ballotpedia scrapers.
- Contains: `parse_votebox()`, `parse_rcv_votebox()`, `detect_election_stage()`, `parse_candidate_cell()`, `parse_results_rows()`.
- Constants: `BALLOTPEDIA_BASE`, `BALLOTPEDIA_DELAY_S`.

### Search Strategy (camplinks/search.py)
- **Tier 1 (Ballotpedia):** DDG `site:ballotpedia.org` search, then parses `<div class="infobox person">` Contact section. Extracts all link types (campaign site, Facebook, X, Instagram, etc.).
- **Tier 2 (Web search fallback):** Multiple DDG query variations with heuristic URL scoring (0.0-1.0). Early-stops at confidence >= 0.5, threshold >= 0.3.
- **Ballotpedia infobox parsing:** "Contact" section identified by a `widget-row` with text "Contact"; subsequent white `widget-row`s are entries until a non-white `widget-row` is found.
- Cache key: `party|state|district|name`.

### Rate Limiting
- Wikipedia: 0.5s delay. Ballotpedia: 1.5s delay. DuckDuckGo: 3.0s with exponential backoff (30s, 60s, 120s). politicalemails.org: 1.0s delay (no retries).
- DDG can throw `RatelimitException` or `DDGSException` with "429"; handled with retry logic in `http.py`.

### Archive Lookup (`camplinks/archive.py`)
- Disambiguation: search returns hits without state, so each hit's profile page is fetched to read `state/locality`. Only hits whose state matches the candidate's `elections.state` (case-insensitive, with bp_municipal `City, State` normalization) are kept.
- "How many" semantics: `archive_lookups.total_messages` is the **sum of message counts** across all surviving matches (not the number of orgs — `match_count` holds that).
- Status values: `no_match` (zero hits or all filtered out), `single` (one survivor), `multiple` (>1 survivor), `error` (search HTTP error). Profile-fetch errors drop that one match but do not error the whole lookup.
- The standalone `archive_lookup.py` (CSV in/out) is unrelated to the pipeline; keep it as-is for ad-hoc CSV workflows.

### Database Gotchas
- `elections.district` is `''` (empty string) for statewide races (Senate, Governor, AG, Mayor). `upsert_election()` normalizes `None` to `''` so the UNIQUE constraint works correctly (SQLite treats NULLs as distinct, which would break upsert).
- `get_candidates_missing_link()` filters `WHERE candidate_name != ''` to skip placeholder rows.
- `open_db()` sets `PRAGMA foreign_keys = ON`, `journal_mode = WAL`, `synchronous = NORMAL`, `cache_size = -64000`.

## Coverage Report

[COVERAGE.md](COVERAGE.md) tracks current database stats (elections, candidates, contact links by year/race/source). **Update it after any pipeline run** by querying the database and rewriting the file with fresh numbers. Include the current date in the "Last updated" line.

## Tech Stack Constraints
- **Python 3.11+**, **uv** for packages, **ruff** for formatting/linting, **mypy** for type checking.
- **Polars only** (never pandas), **orjson** for JSON, **SQLite** (raw `sqlite3`, no ORM).
- **BeautifulSoup** with **lxml** parser for HTML parsing.
- **ddgs v9+** -- import as `from ddgs import DDGS`; exceptions from `ddgs.exceptions`.
- All functions require type hints and docstrings (Args/Returns/Raises format).
- Use `logger.error` for error reporting, never bare `print`.
