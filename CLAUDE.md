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
| `judicial` | State Supreme Court | Contested + retention + Pattern A tables |

## Commands

```bash
# Install
uv sync

# Run pipeline
python -m camplinks --year 2024 --race house                    # full pipeline
python -m camplinks --year 2024 --race senate --stage scrape    # scrape only
python -m camplinks --year 2025 --race governor --stage scrape  # gubernatorial
python -m camplinks --year 2025 --race municipal --stage scrape # mayoral
python -m camplinks --year 2024 --race all                      # all race types
python -m camplinks --year 2024 --race house --db custom.db     # custom DB path

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
- **elections** -- one row per contest, UNIQUE(state, race_type, year, district)
- **candidates** -- one row per candidate per election, UNIQUE(election_id, candidate_name)
- **contact_links** -- one row per link per candidate, UNIQUE(candidate_id, link_type)

`link_type` values: `campaign_site`, `campaign_facebook`, `campaign_x`, `campaign_instagram`, `personal_website`, `personal_facebook`, `personal_linkedin`

`source` values: `wikipedia`, `ballotpedia`, `web_search`

All writes use upsert (ON CONFLICT) so stages are idempotent and safe to re-run.

### Upsert Merge Semantics

Upserts are not simple overwrites -- they merge intelligently:
- **elections:** Updates `wikipedia_url` only if the new value is non-empty.
- **candidates:** Preserves non-empty `wikipedia_url`/`ballotpedia_url`, takes max `is_winner` flag.
- **contact_links:** Overwrites URL and source on conflict (one URL per link_type per candidate).

## Architecture Notes

### Pipeline Stages

Three stages run in order: **scrape -> enrich -> search**. Each is independently runnable via `--stage` and idempotent.

1. **Scrape:** Fetches Wikipedia index page for the race/year, follows state links, parses candidate tables. Commits after each state.
2. **Enrich:** For candidates with `wikipedia_url` but no `campaign_site` link, fetches their Wikipedia page and extracts the campaign website from the infobox or External links section.
3. **Search:** For candidates still missing `campaign_site`, runs two-tier search. Uses `campaign_search_cache.json` for resumability (auto-saves every 25 candidates).

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

### Search Strategy (camplinks/search.py)
- **Tier 1 (Ballotpedia):** DDG `site:ballotpedia.org` search, then parses `<div class="infobox person">` Contact section. Extracts all link types (campaign site, Facebook, X, Instagram, etc.).
- **Tier 2 (Web search fallback):** Multiple DDG query variations with heuristic URL scoring (0.0-1.0). Early-stops at confidence >= 0.5, threshold >= 0.3.
- **Ballotpedia infobox parsing:** "Contact" section identified by a `widget-row` with text "Contact"; subsequent white `widget-row`s are entries until a non-white `widget-row` is found.
- Cache key: `party|state|district|name`.

### Rate Limiting
- Wikipedia: 0.5s delay. Ballotpedia: 1.5s delay. DuckDuckGo: 3.0s with exponential backoff (30s, 60s, 120s).
- DDG can throw `RatelimitException` or `DDGSException` with "429"; handled with retry logic in `http.py`.

### Database Gotchas
- `elections.district` is NULL for statewide races (Senate, Governor, AG, Mayor). SQLite treats multiple NULLs as distinct in UNIQUE constraints, so this works correctly.
- `get_candidates_missing_link()` filters `WHERE candidate_name != ''` to skip placeholder rows.
- `open_db()` sets `PRAGMA foreign_keys = ON`, `journal_mode = WAL`, `synchronous = NORMAL`, `cache_size = -64000`.

## Tech Stack Constraints
- **Polars only** (never pandas), **orjson** for JSON, **SQLite** for database.
- **ddgs v9+** -- import as `from ddgs import DDGS`.
- **uv** for packages, **ruff** for formatting/linting.
- All functions require type hints and docstrings per `~/Desktop/CLAUDE.md`.
