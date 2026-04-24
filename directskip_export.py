"""
directskip_export.py — Generate a DirectSkip upload CSV from the database.

Usage:
    python main.py --directskip-export [--dry-run]

Output:
    exports/directskip_YYYY-MM-DD.csv

Inclusion criteria:
  - Has owner_last (skip traced via BatchData) OR owner name from scrape
  - Has a property street address
  - directskip_date IS NULL (not yet run through DirectSkip)
  - Not cancelled
  - sale_date >= today

Columns match the DirectSkip upload template exactly:
  First Name, Last Name,
  Property Address, Property City, Property State, Property Zip,
  Mailing Address, Mailing City, Mailing State, Mailing Zip,
  Custom Field 1 (Sale Date), Custom Field 2 (County), Custom Field 3 (Case #)
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from db import _conn

EXPORTS_DIR = Path(__file__).parent / "exports"

HEADERS = [
    "First Name", "Last Name",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Custom Field 1", "Custom Field 2", "Custom Field 3",
]


def generate(dry_run: bool = False) -> Path | None:
    """
    Build the DirectSkip upload CSV and write it to exports/.
    Returns the output path, or None if no qualifying rows / dry run.
    """
    today = date.today().isoformat()

    with _conn() as con:
        rows = con.execute("""
            SELECT
                owner_first,
                owner_last,
                owner_secondary_first,
                owner_secondary_last,
                street,
                city,
                state,
                zip,
                mailing_address,
                sale_date,
                county,
                case_number
            FROM listings
            WHERE (directskip_date IS NULL OR directskip_date = '')
              AND (cancelled IS NULL OR LOWER(cancelled) != 'yes')
              AND sale_date >= ?
              AND street IS NOT NULL AND street != ''
              AND (
                (owner_last IS NOT NULL AND owner_last != '')
                OR (owner_first IS NOT NULL AND owner_first != '')
              )
            ORDER BY sale_date ASC, id
        """, (today,)).fetchall()

    if not rows:
        print("  [DS Export] No qualifying listings — nothing to export.")
        return None

    csv_rows = []
    for r in rows:
        first  = (r["owner_first"]  or "").strip()
        last   = (r["owner_last"]   or "").strip()

        prop_street = (r["street"] or "").strip()
        prop_city   = (r["city"]   or "").strip()
        prop_state  = (r["state"]  or "").strip()
        prop_zip    = (r["zip"]    or "").strip()

        # Mailing address — fall back to property address if blank
        mailing_raw = (r["mailing_address"] or "").strip()
        if mailing_raw:
            m_street, m_city, m_state, m_zip = _split_mailing(mailing_raw)
        else:
            m_street, m_city, m_state, m_zip = prop_street, prop_city, prop_state, prop_zip

        csv_rows.append([
            first, last,
            prop_street, prop_city, prop_state, prop_zip,
            m_street, m_city, m_state, m_zip,
            r["sale_date"] or "",   # Custom Field 1 — Sale Date
            r["county"]    or "",   # Custom Field 2 — County
            r["case_number"] or "", # Custom Field 3 — Case #
        ])

        # If there's a secondary owner, add them as a separate row too
        sec_first = (r["owner_secondary_first"] or "").strip()
        sec_last  = (r["owner_secondary_last"]  or "").strip()
        if sec_last or sec_first:
            csv_rows.append([
                sec_first, sec_last,
                prop_street, prop_city, prop_state, prop_zip,
                m_street, m_city, m_state, m_zip,
                r["sale_date"] or "",
                r["county"]    or "",
                r["case_number"] or "",
            ])

    if dry_run:
        print(f"  [DS Export] [DRY RUN] Would export {len(csv_rows)} row(s).")
        for row in csv_rows[:5]:
            print(f"    {row[0]} {row[1]} | {row[2]}, {row[4]} | Sale: {row[10]}")
        if len(csv_rows) > 5:
            print(f"    ... and {len(csv_rows) - 5} more")
        return None

    EXPORTS_DIR.mkdir(exist_ok=True)
    out_path = EXPORTS_DIR / f"directskip_{today}.csv"

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerows(csv_rows)

    print(f"  [DS Export] Exported {len(csv_rows)} row(s) → {out_path}")
    return out_path


def _split_mailing(raw: str) -> tuple[str, str, str, str]:
    """
    Parse 'street, city, state zip' → (street, city, state, zip).
    Falls back gracefully if the format doesn't match.
    """
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) >= 3:
        street = parts[0]
        city   = parts[1]
        state_zip = parts[2].strip().split()
        state  = state_zip[0] if state_zip else ""
        zip_   = state_zip[1] if len(state_zip) > 1 else ""
        return street, city, state, zip_
    elif len(parts) == 2:
        return parts[0], parts[1], "", ""
    else:
        return raw, "", "", ""
