# Contributing to camplinks

Thanks for your interest in contributing! This guide covers how to set up the project and submit changes.

## Setup

```bash
# Clone and install
git clone https://github.com/nicweber/campLinks.git
cd campLinks
uv sync
```

This installs all runtime and dev dependencies into a `.venv`.

## Development workflow

1. Create a branch for your change.
2. Make your edits.
3. Run the checks below before opening a PR.

### Running checks

```bash
# Tests
uv run pytest tests/

# Type checking
uv run mypy camplinks/

# Linting and formatting
uv run ruff check .
uv run ruff format .
```

All four must pass cleanly.

### Running a single test

```bash
uv run pytest tests/test_db.py::TestUpsertElection::test_insert_returns_id
```

## Code conventions

- **Polars only** (never pandas), **orjson** for JSON.
- All functions need type hints and docstrings (Args / Returns / Raises).
- Use `T | None` instead of `Optional[T]`.
- Catch specific exceptions, never bare `except:` or broad `except Exception`.
- Use `logger.error()` for errors, not `print()`.
- Line length: 88 characters (ruff default).

See [CLAUDE.md](CLAUDE.md) for the full set of conventions enforced on this project.

## Adding a new race type

1. Create `camplinks/scrapers/{race}.py` extending `BaseScraper`.
2. Implement `build_index_url()`, `collect_state_urls()`, `parse_state_page()`.
3. Call `register_scraper("name", MyScraperClass)` at module level.
4. Import the new module in `camplinks/__main__.py`.

See [USAGE.md](USAGE.md) for a full walkthrough with a Governor scraper example.

## Submitting a pull request

- Keep PRs focused on a single change.
- Include tests for new functionality.
- Ensure all checks pass before requesting review.
