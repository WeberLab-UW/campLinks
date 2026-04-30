"""Add required_compliance column to the candidates and campaign_site_content tables.

For each candidate, checks if their election state has a law in
state_laws.csv and if their election year is >= the law's year_enacted.
If both are true, sets required_compliance to the law's type
(e.g. "Disclosure" or "Prohibition"). Otherwise sets it to NULL.

The campaign_site_content table is then updated by joining on candidate_id.
"""

from __future__ import annotations

import csv
import sqlite3

from tqdm import tqdm

from camplinks.models import DB_FILENAME

STATE_LAWS_CSV = "state_laws.csv"


def load_state_laws(csv_path: str) -> dict[str, tuple[int, str]]:
    """Load state laws from CSV into a dict keyed by state name.

    If a state appears multiple times, the earliest year_enacted is used.

    Args:
        csv_path: Path to state_laws.csv.

    Returns:
        Dict mapping lowercase state name to (year_enacted, type).
    """
    laws: dict[str, tuple[int, str]] = {}
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            state = row["state"].strip().lower()
            year = int(row["year_enacted"].strip())
            law_type = row["type"].strip()
            if state not in laws or year < laws[state][0]:
                laws[state] = (year, law_type)
    return laws


def add_compliance_column(db_path: str = DB_FILENAME) -> None:
    """Add and populate required_compliance on the candidates and campaign_site_content tables.

    Args:
        db_path: Path to the SQLite database file.
    """
    laws = load_state_laws(STATE_LAWS_CSV)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # ── candidates table ──────────────────────────────────────────────────
    try:
        conn.execute("ALTER TABLE candidates ADD COLUMN required_compliance TEXT")
        conn.commit()
        print("Added required_compliance column to candidates.")
    except sqlite3.OperationalError:
        print("required_compliance column already exists in candidates.")

    rows = conn.execute(
        """
        SELECT c.candidate_id, e.state, e.year
        FROM candidates c
        JOIN elections e ON c.election_id = e.election_id
        WHERE c.candidate_name != ''
        """
    ).fetchall()

    print(f"Processing {len(rows)} candidates...")

    updates: list[tuple[str | None, int]] = []
    for row in tqdm(rows, desc="Setting compliance", unit="candidate"):
        state_key = row["state"].strip().lower()
        election_year = row["year"]

        if state_key in laws:
            law_year, law_type = laws[state_key]
            compliance = law_type if election_year >= law_year else None
        else:
            compliance = None

        updates.append((compliance, row["candidate_id"]))

    conn.executemany(
        "UPDATE candidates SET required_compliance = ? WHERE candidate_id = ?",
        updates,
    )
    conn.commit()

    # this is the number of candidates that require some form of compliance
    set_count = sum(1 for v, _ in updates if v is not None)
    print(f"Done. {set_count} candidates marked with required_compliance.")

    # ── campaign_site_content table ───────────────────────────────────────
    for col in ("required_compliance TEXT", "state TEXT"):
        try:
            conn.execute(f"ALTER TABLE campaign_site_content ADD COLUMN {col}")
            conn.commit()
            print(f"Added {col.split()[0]} column to campaign_site_content.")
        except sqlite3.OperationalError:
            print(f"{col.split()[0]} column already exists in campaign_site_content.")

    conn.execute(
        """
        UPDATE campaign_site_content
        SET required_compliance = (
            SELECT c.required_compliance
            FROM candidates c
            WHERE c.candidate_id = campaign_site_content.candidate_id
        ),
        state = (
            SELECT e.state
            FROM candidates c
            JOIN elections e ON c.election_id = e.election_id
            WHERE c.candidate_id = campaign_site_content.candidate_id
        )
        """
    )
    conn.commit()
    conn.close()
    print("campaign_site_content table required_compliance and state updated.")


if __name__ == "__main__":
    add_compliance_column()
