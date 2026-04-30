"""Add AI detection columns to campaign_site_content table and populate from CSV."""

from __future__ import annotations

import csv
import logging
import sqlite3

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CSV_PATH = "detection/campaign_site_text_detection_final.csv"
DB_PATH = "camplinks.db"

NEW_COLUMNS: list[tuple[str, str]] = [
    ("AI_label", "TEXT"),
    ("assistance_score", "REAL"),
    ("confidence", "TEXT"),
    ("fraction_ai", "REAL"),
    ("fraction_human", "REAL"),
    ("num_ai_segments", "INTEGER"),
    ("token_length", "INTEGER"),
]


def add_columns(conn: sqlite3.Connection) -> None:
    """Add new detection columns to campaign_site_content table if not already present.

    Args:
        conn: Open SQLite connection.
    """
    cur = conn.execute("PRAGMA table_info(campaign_site_content)")
    existing = {row[1] for row in cur.fetchall()}
    for col_name, col_type in NEW_COLUMNS:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE campaign_site_content ADD COLUMN {col_name} {col_type}")
            logger.info("Added column %s %s", col_name, col_type)
        else:
            logger.info("Column %s already exists, skipping", col_name)
    conn.commit()


def load_csv(path: str) -> dict[int, dict[str, str]]:
    """Load detection CSV keyed by content_id.

    Args:
        path: Path to the CSV file.

    Returns:
        Dict mapping content_id to a dict of detection column values plus
        candidate_name for verification.
    """
    keep = {col for col, _ in NEW_COLUMNS} | {"candidate_name"}
    records: dict[int, dict[str, str]] = {}
    with open(path, encoding="latin-1", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_id = (row.get("content_id") or "").strip()
            if not raw_id.isdigit():
                continue
            content_id = int(raw_id)
            records[content_id] = {k: (row.get(k) or "").strip() for k in keep}
    logger.info("Loaded %d rows from CSV", len(records))
    return records


def _coerce(value: str, col_type: str) -> float | int | str | None:
    """Coerce a string value to the target SQLite column type.

    Args:
        value: Raw string from CSV.
        col_type: One of TEXT, REAL, INTEGER.

    Returns:
        Typed Python value, or None if value is empty or unparseable.
    """
    if value == "":
        return None
    if col_type == "REAL":
        try:
            return float(value)
        except ValueError:
            return None
    if col_type == "INTEGER":
        try:
            return int(value)
        except ValueError:
            return None
    return value


def update_rows(conn: sqlite3.Connection, records: dict[int, dict[str, str]]) -> None:
    """Update campaign_site_content rows with detection data matched by content_id.

    Candidate_name is verified and mismatches are logged, but the update
    proceeds on content_id alone (which is the primary key) to handle
    encoding and whitespace inconsistencies between the CSV and DB.
    Rows with no matching content_id are left unchanged.

    Args:
        conn: Open SQLite connection.
        records: Detection data keyed by content_id.
    """
    col_names = [col for col, _ in NEW_COLUMNS]
    col_types = {col: typ for col, typ in NEW_COLUMNS}

    set_clause = ", ".join(f"{c} = ?" for c in col_names)
    sql = f"UPDATE campaign_site_content SET {set_clause} WHERE content_id = ?"

    updated = 0
    name_mismatch = 0
    skipped_missing = 0

    cur = conn.cursor()

    # Pre-fetch all content_id -> candidate_name for name verification
    db_names: dict[int, str] = {
        row[0]: row[1]
        for row in conn.execute("SELECT content_id, candidate_name FROM campaign_site_content")
    }

    for content_id, data in records.items():
        if content_id not in db_names:
            skipped_missing += 1
            continue

        db_name = db_names[content_id].strip()
        csv_name = data["candidate_name"]
        if db_name != csv_name:
            name_mismatch += 1
            logger.info(
                "content_id=%d name mismatch (updating anyway): CSV=%r DB=%r",
                content_id,
                csv_name,
                db_name,
            )

        values: list[float | int | str | None] = [
            _coerce(data.get(col, ""), col_types[col]) for col in col_names
        ]
        values.append(content_id)
        cur.execute(sql, values)
        updated += 1

    conn.commit()
    logger.info("Updated %d rows", updated)
    if name_mismatch:
        logger.info(
            "%d rows had candidate_name encoding/whitespace differences (still updated)",
            name_mismatch,
        )
    if skipped_missing:
        logger.info(
            "%d CSV rows had no matching content_id in DB", skipped_missing
        )


def main() -> None:
    """Entry point."""
    records = load_csv(CSV_PATH)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        add_columns(conn)
        update_rows(conn, records)


if __name__ == "__main__":
    main()
