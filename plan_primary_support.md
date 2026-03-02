# Plan: Add Primary/Runoff Election Stage Support

## Context

Currently, all scrapers filter for general election tables only (`is_general_election_table()` returns False for primaries/runoffs). The database has no concept of election stage -- every record is implicitly a general election. This means:

- For **future elections** (e.g. 2026), Wikipedia pages have primary results tables but no general election table yet, so the scraper finds **nothing**
- For **past elections** (e.g. 2024), primary results exist on Wikipedia but are discarded

This plan adds an `election_stage` field ("general", "primary", "runoff") to the Election model, enabling the pipeline to scrape and store all stages as separate election records. The same candidate can appear in both a primary and a general election.

**Scope:** Only structured results tables (wikitable plainrowheaders + vcard rows) -- no prose/candidate-list parsing.

## Changes (~15 files modified)

### 1. `camplinks/models.py` -- add election_stage field

- Add `ELECTION_STAGES = ("general", "primary", "runoff")` constant
- Add `election_stage: str = "general"` to `Election` dataclass (before `election_id`)
- Default of `"general"` keeps all existing construction sites backward-compatible

### 2. `camplinks/db.py` -- schema migration + upsert changes

**Schema:** Add `election_stage TEXT NOT NULL DEFAULT 'general'` column to elections table. Change UNIQUE constraint to `UNIQUE(state, race_type, year, district, election_stage)`.

**Migration function** `migrate_schema(conn)`:
- Check if `election_stage` column already exists via `PRAGMA table_info(elections)` -- skip if so
- If elections table doesn't exist yet (fresh DB) -- skip (init_schema handles it)
- Otherwise: create `elections_new` with new schema, copy data with `election_stage='general'`, drop old table, rename new table, recreate indexes
- Must disable foreign keys during migration (`PRAGMA foreign_keys = OFF`)
- All 667 existing elections get `election_stage='general'`

**Upsert changes:**
- `upsert_election()`: Add `election_stage` to INSERT columns and ON CONFLICT target
- `get_candidates_missing_link()`: Add optional `election_stage` filter param, include `e.election_stage` in SELECT
- `get_candidates_with_link()`: Same -- add optional `election_stage` filter param

Add new index: `CREATE INDEX idx_elections_stage ON elections(year, race_type, election_stage)`

### 3. `camplinks/wiki_parsing.py` -- classify tables by stage

**New function** `classify_election_table(table) -> str | None`:
- Returns `"general"`, `"primary"`, `"runoff"`, or `None`
- Same heuristic as current `is_general_election_table()` but returns the stage instead of a boolean
- Checks caption first (primary/runoff/election keywords), then h3/h4 heading

**Keep** `is_general_election_table()` as a thin wrapper: `return classify_election_table(table) == "general"`

**New function** `extract_primary_party(table) -> str`:
- Walks backward to find h2 heading (e.g. "Republican primary", "Democratic primary")
- Extracts party name via regex
- Used to set/override candidate party for primary tables where vcard rows may be ambiguous

### 4. Scraper changes (8 files, same pattern)

All scrapers that currently do:
```python
if not is_general_election_table(table):
    continue
```
Change to:
```python
stage = classify_election_table(table)
if stage is None:
    continue
```

And construct `Election(..., election_stage=stage)`.

**Early-return scrapers** (senate.py, governor.py, attorney_general.py, special_house.py, municipal.py) currently `return [(election, candidates)]` on the first match. These must change to **accumulate all matching tables** into a results list so both primary and general are captured.

For primary tables, call `extract_primary_party(table)` and set each candidate's party if not already set from the vcard row.

**Files:** `house.py`, `senate.py`, `governor.py`, `attorney_general.py`, `judicial.py`, `state_legislative.py`, `state_leg_special.py`, `special_house.py`

**Exceptions:**
- **California parser** (`house.py:_parse_california_table`): Keep general-only for now. California's combined primary+general table format requires separate handling.
- **RCV parser** (`house.py:_parse_rcv_tables`): Default to `election_stage="general"`.
- **Municipal scraper**: Uses its own `_is_results_table()` heuristic, not `is_general_election_table()`. Leave defaulting to `election_stage="general"` -- municipal primary data is rarely structured on Wikipedia.

### 5. Downstream stages -- default to general-only

Primary candidates are less useful for contact enrichment (losers' sites die faster, wastes rate-limited queries). Enrich/search/validate default to general-only with opt-in for primary.

**`camplinks/enrich.py`**: Add `election_stage: str | None = "general"` parameter to `enrich_from_wikipedia()`, filter the SQL query.

**`camplinks/search.py`**: Add `election_stage: str | None = "general"` parameter to `search_all_candidates()`, pass through to `get_candidates_missing_link()`.

**`camplinks/validate.py`**: Add `election_stage: str | None = "general"` parameter to `validate_campaign_sites()`, pass through to `get_candidates_with_link()`.

### 6. `camplinks/pipeline.py` -- orchestrator updates

- Call `migrate_schema(conn)` before `init_schema(conn)`
- Pass `election_stage="general"` to enrich/search/validate stages
- Update `_print_summary()` to break down by election_stage

### 7. `camplinks/__main__.py` -- CLI update

- Add `--election-stage` argument with choices `["general", "primary", "runoff"]`, default `None`
- For scrape: `None` means scrape all stages found on the page
- For enrich/search/validate: defaults to general-only

### 8. Tests

**`tests/test_db.py`:**
- New `TestElectionStageUpsert` class: same election with different stages get different IDs, same stage is idempotent, default is "general"
- New `TestMigrateSchema` class: create old schema, insert data, run migration, verify column exists and all rows are "general", verify foreign keys intact
- Add `election_stage` filter tests for `get_candidates_missing_link` and `get_candidates_with_link`

**`tests/test_wiki_parsing.py`:**
- New `TestClassifyElectionTable` class: general/primary/runoff captions and headings, returns None for unclassifiable
- New `TestExtractPrimaryParty` class: Republican/Democratic headings, no-match returns ""

**Scraper test files** (test_senate.py, test_governor.py, etc.):
- Add test HTML with primary table, verify `election_stage="primary"` on returned Election
- Add test with both primary + general tables, verify both returned

### 9. Documentation

- Update `README.md` ER diagram annotations to show `election_stage`
- Update `CLAUDE.md` database schema section
- Update `USAGE.md` schema reference diagram

## Implementation Order

1. `camplinks/models.py` (foundation)
2. `camplinks/db.py` (schema + migration + upserts)
3. `camplinks/wiki_parsing.py` (classify + extract_primary_party)
4. `camplinks/scrapers/*.py` (all 8 scrapers)
5. `camplinks/enrich.py`, `camplinks/search.py`, `camplinks/validate.py` (downstream stages)
6. `camplinks/pipeline.py` (orchestrator)
7. `camplinks/__main__.py` (CLI)
8. Tests
9. Documentation

## Verification

```bash
# Run full test suite
pytest tests/ -v

# Type check and lint
mypy camplinks/
ruff check .
ruff format .

# Test migration on live database (backup first)
cp camplinks.db camplinks.db.bak
python -m camplinks --year 2026 --race senate --stage scrape

# Verify primary data was captured
sqlite3 camplinks.db "SELECT election_stage, COUNT(*) FROM elections GROUP BY election_stage;"
sqlite3 camplinks.db "SELECT e.election_stage, COUNT(*) FROM candidates c JOIN elections e ON c.election_id=e.election_id WHERE e.year=2026 GROUP BY e.election_stage;"
```

## Edge Cases

- **NULL district + UNIQUE:** SQLite treats NULLs as distinct in UNIQUE constraints, so (OH, US Senate, 2026, NULL, "primary") and (OH, US Senate, 2026, NULL, "general") correctly coexist
- **Same candidate in primary + general:** Gets separate candidate_id entries under different election_id records. Contact links are per-candidate, so they're independent
- **Cache key collision:** Search cache key is `party|state|district|name`. Same candidate in primary+general shares cache key -- this is desirable (don't search twice)
- **Primary tables without results:** Only `<table>` elements are classified, so prose-only primary sections are naturally skipped
