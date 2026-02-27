# Agent Guidelines for Python Code Quality

These rules MUST be followed by all AI coding agents and contributors.

## Core Principles

All code MUST be fully optimized: maximizing algorithmic big-O efficiency, using parallelization/vectorization where appropriate, maximizing code reuse (DRY), and no extra code beyond what is necessary.

## Preferred Tools

- Use `uv` for Python package management and to create a `.venv` if it is not present.
- Ensure `ipykernel` and `ipywidgets` is installed in `.venv` for Jupyter Notebook compatibility. This should not be in package requirements.
- Use `tqdm` to track long-running loops within Jupyter Notebooks. The `description` of the progress bar should be contextually sensitive.
- Use `orjson` for JSON loading/dumping.
- Use `logger.error` instead of `print` for error reporting.
- **ALWAYS** use `polars` instead of `pandas` for data frame manipulation.
- If a `polars` dataframe will be printed, **NEVER** simultaneously print the number of entries nor the schema as it is redundant.
- **NEVER** ingest more than 10 rows of a data frame at a time in conversation context.
- In Jupyter Notebooks, DataFrame objects within conditional blocks should be explicitly `print()` as they will not be printed automatically.

## Code Style

- **NEVER** use emoji or unicode that emulates emoji (e.g. checkmarks, crosses).
- Limit line length to 88 characters (ruff formatter standard).
- **MUST** use type hints for all function signatures. **NEVER** use `Any` unless absolutely necessary.
- **MUST** include docstrings for all public functions/classes with Args/Returns/Raises format.

## Testing

- **MUST** mock external dependencies (APIs, databases, file systems).
- **NEVER** run tests you generate without first saving them as their own discrete file.
- **NEVER** delete files created as a part of testing.
- Ensure the folder used for test outputs is present in `.gitignore`.
- Follow the Arrange-Act-Assert pattern.

## Database

- **MUST** use parameterized queries; **NEVER** use string concatenation for query building.
- **MUST** define foreign key constraints. Index foreign key columns and frequent WHERE/JOIN/ORDER BY columns.
- Batch INSERT/UPDATE operations instead of individual row operations.
- Avoid `SELECT *`; explicitly list required columns.

## Before Committing

- [ ] All tests pass (`pytest tests/`)
- [ ] Type checking passes (`mypy camplinks/`)
- [ ] Code formatter and linter pass (`ruff format .` and `ruff check .`)
- [ ] All functions have docstrings and type hints
- [ ] No commented-out code or debug statements
