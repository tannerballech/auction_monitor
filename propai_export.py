"""
propai_export.py — Generate a Prop.ai upload CSV.

Usage:
    python main.py --propai [--dry-run]

Output:
    exports/propai_YYYY-MM-DD.csv

Same inclusion criteria as the PhoneBurner export:
  - equity_signal IN (🏆, ✅)
  - sale_date between today+5 and today+30
  - directskip_date populated
  - not cancelled

One row per phone number — a person with 5 phones produces 5 rows.
Blank phones are skipped.

Columns: First, Last, Full Name, Address, Phone, City, State, Zip
"""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

from db import _conn

EXPORTS_DIR = Path(__file__).parent / "exports"

HEADERS = ["First", "Last", "Full Name", "Address", "Phone", "City", "State", "Zip"]


def generate(dry_run: bool = False) -> Path | None:
    """
    Build the Prop.ai CSV and write it to exports/.
    Returns the output path, or None if no qualifying rows.
    """
    today    = date.today()
    date_min = (today + timedelta(days=5)).isoformat()
    date_max = (today + timedelta(days=30)).isoformat()

    with _conn() as con:
        persons = con.execute("""
            SELECT
                p.first_name,
                p.last_name,
                p.phone1, p.phone2, p.phone3,
                p.phone4, p.phone5, p.phone6, p.phone7,
                l.street,
                l.city,
                l.state,
                l.zip
            FROM directskip_persons p
            JOIN listings l ON l.id = p.listing_id
            WHERE l.equity_signal IN ('🏆', '✅')
              AND (l.directskip_date IS NOT NULL AND l.directskip_date != '')
              AND (l.cancelled IS NULL OR LOWER(l.cancelled) != 'yes')
              AND l.sale_date BETWEEN ? AND ?
            ORDER BY l.sale_date ASC, p.listing_id, p.person_number
        """, (date_min, date_max)).fetchall()

    csv_rows = []
    for p in persons:
        first     = (p["first_name"] or "").strip()
        last      = (p["last_name"]  or "").strip()
        full_name = f"{first} {last}".strip()
        address   = p["street"] or ""
        city      = p["city"]   or ""
        state     = p["state"]  or ""
        zip_      = p["zip"]    or ""

        phones = [
            p["phone1"], p["phone2"], p["phone3"],
            p["phone4"], p["phone5"], p["phone6"], p["phone7"],
        ]

        for phone in phones:
            if not (phone or "").strip():
                continue
            csv_rows.append([first, last, full_name, address, phone.strip(),
                             city, state, zip_])

    if not csv_rows:
        print("  No qualifying rows — nothing to export.")
        return None

    if dry_run:
        print(f"  [DRY RUN] Would export {len(csv_rows)} row(s).")
        for row in csv_rows[:5]:
            print(f"    {row[2]} | {row[3]}, {row[6]} | {row[4]}")
        if len(csv_rows) > 5:
            print(f"    ... and {len(csv_rows) - 5} more")
        return None

    EXPORTS_DIR.mkdir(exist_ok=True)
    out_path = EXPORTS_DIR / f"propai_{today.isoformat()}.csv"

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerows(csv_rows)

    print(f"  Exported {len(csv_rows)} row(s) → {out_path}")
    return out_path
