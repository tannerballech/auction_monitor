"""
phoneburner_export.py — Generate a PhoneBurner upload CSV.

Usage:
    python main.py --phoneburner [--dry-run]

Output:
    exports/phoneburner_YYYY-MM-DD.csv

Inclusion criteria:
  - equity_signal IN (🏆, ✅)
  - sale_date between today+5 and today+30
  - directskip_date populated (has been through DirectSkip)
  - not cancelled

One row per DirectSkip person (up to 3 per listing).
Relatives are excluded from this export.

Columns (match PhoneBurner import template exactly):
  First Name, Last Name, Phone, Phone 2, Phone 3, Phone 4, Phone 5,
  Property Address, Relation, Property City, Property State, Property Zip,
  Primary Owner, Auction Date
"""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

from db import _conn

EXPORTS_DIR = Path(__file__).parent / "exports"

HEADERS = [
    "First Name", "Last Name",
    "Phone", "Phone 2", "Phone 3", "Phone 4", "Phone 5",
    "Property Address", "Relation",
    "Property City", "Property State", "Property Zip",
    "Primary Owner", "Auction Date",
]


def _format_date(d: str) -> str:
    """Convert YYYY-MM-DD to MM/DD/YYYY for PhoneBurner."""
    try:
        return date.fromisoformat(d).strftime("%m/%d/%Y")
    except (ValueError, TypeError):
        return d or ""


def generate(dry_run: bool = False) -> Path | None:
    """
    Build the PhoneBurner CSV and write it to exports/.
    Returns the output path, or None if no qualifying rows.
    """
    today      = date.today()
    date_min   = (today + timedelta(days=5)).isoformat()
    date_max   = (today + timedelta(days=30)).isoformat()

    with _conn() as con:
        rows = con.execute("""
            SELECT
                p.person_number,
                p.first_name,
                p.last_name,
                p.phone1, p.phone2, p.phone3, p.phone4, p.phone5,
                l.street,
                l.city,
                l.state,
                l.zip,
                l.sale_date,
                p1.first_name  AS primary_first,
                p1.last_name   AS primary_last
            FROM directskip_persons p
            JOIN listings l
              ON l.id = p.listing_id
            LEFT JOIN directskip_persons p1
              ON p1.listing_id = p.listing_id AND p1.person_number = 1
            WHERE l.equity_signal IN ('🏆', '✅')
              AND (l.directskip_date IS NOT NULL AND l.directskip_date != '')
              AND (l.cancelled IS NULL OR LOWER(l.cancelled) != 'yes')
              AND l.sale_date BETWEEN ? AND ?
            ORDER BY l.sale_date ASC, p.listing_id, p.person_number
        """, (date_min, date_max)).fetchall()

    if not rows:
        print("  No qualifying rows — nothing to export.")
        return None

    csv_rows = []
    for r in rows:
        primary_owner = f"{r['primary_first'] or ''} {r['primary_last'] or ''}".strip()
        csv_rows.append([
            r["first_name"] or "",
            r["last_name"]  or "",
            r["phone1"] or "",
            r["phone2"] or "",
            r["phone3"] or "",
            r["phone4"] or "",
            r["phone5"] or "",
            r["street"] or "",
            "Owner",
            r["city"]  or "",
            r["state"] or "",
            r["zip"]   or "",
            primary_owner,
            _format_date(r["sale_date"]),
        ])

    if dry_run:
        print(f"  [DRY RUN] Would export {len(csv_rows)} row(s).")
        for row in csv_rows[:5]:
            print(f"    {row[0]} {row[1]} | {row[7]}, {row[9]} | Sale: {row[13]} | Phones: {[p for p in row[2:7] if p]}")
        if len(csv_rows) > 5:
            print(f"    ... and {len(csv_rows) - 5} more")
        return None

    EXPORTS_DIR.mkdir(exist_ok=True)
    out_path = EXPORTS_DIR / f"phoneburner_{today.isoformat()}.csv"

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerows(csv_rows)

    print(f"  Exported {len(csv_rows)} row(s) → {out_path}")
    return out_path
