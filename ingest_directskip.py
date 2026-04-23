"""
ingest_directskip.py — Ingest a DirectSkip results CSV into the database.

Usage:
    python main.py --ingest-directskip path/to/results.csv [--dry-run]

Matches each CSV row back to a listing using the three fields we embedded
at export time:
    Input Custom Field 3  → county   (e.g. "Knox")
    Input Property Address → street_number extracted from leading digits
    Input Custom Field 2  → sale_date (YYYY-MM-DD)

Writes to:
    directskip_persons    — Person 1 / 2 / 3 contact data (INSERT OR REPLACE)
    directskip_relatives  — Relatives for each person   (INSERT OR REPLACE)
    listings.directskip_date — stamped with today's date

Re-running the same file is safe — INSERT OR REPLACE overwrites existing rows.
"""

from __future__ import annotations

import csv
import re
import traceback
from datetime import date
from pathlib import Path

import db
from db import _conn, _extract_street_number

# ── Helpers ───────────────────────────────────────────────────────────────────

def _v(row: dict, key: str) -> str:
    """Safe value getter — returns stripped string, never None."""
    return (row.get(key) or "").strip()


def _find_listing_id(county: str, property_address: str, sale_date: str) -> int | None:
    """
    Look up the listing id using the dedup key (county, street_number, sale_date).
    county is stored lowercase; street_number is leading digits of street address.
    """
    county_lower  = county.strip().lower()
    street_number = _extract_street_number(property_address.strip())
    if not county_lower or not street_number or not sale_date:
        return None
    with _conn() as con:
        row = con.execute(
            "SELECT id FROM listings WHERE county=? AND street_number=? AND sale_date=?",
            (county_lower, street_number, sale_date),
        ).fetchone()
    return row["id"] if row else None


def _phone_pairs(row: dict, prefix: str, count: int) -> list[tuple[str, str]]:
    """
    Extract (phone, phone_type) pairs from a row.
    prefix examples: "" (person 1), "Person2 ", "Person2 Relative1 "
    count: number of phone slots (7 for persons, 5 for relatives)
    Returns only non-blank phones.
    """
    pairs = []
    for i in range(1, count + 1):
        phone = _v(row, f"{prefix}Phone{i}")
        ptype = _v(row, f"{prefix}Phone{i} Type")
        if phone:
            pairs.append((phone, ptype))
    return pairs


def _upsert_person(
    listing_id: int,
    person_number: int,
    result_code: str,
    first_name: str,
    last_name: str,
    age: str,
    deceased: str,
    phones: list[tuple[str, str]],
    email1: str,
    email2: str,
    mailing_street: str,
    mailing_city: str,
    mailing_state: str,
    mailing_zip: str,
) -> None:
    """INSERT OR REPLACE a directskip_persons row."""
    # Pad phones to 7 slots
    phones = (phones + [("", "")] * 7)[:7]

    with _conn() as con:
        con.execute("""
            INSERT OR REPLACE INTO directskip_persons (
                listing_id, person_number,
                result_code, first_name, last_name, age, deceased,
                phone1, phone1_type, phone2, phone2_type, phone3, phone3_type,
                phone4, phone4_type, phone5, phone5_type, phone6, phone6_type,
                phone7, phone7_type,
                email1, email2,
                mailing_street, mailing_city, mailing_state, mailing_zip
            ) VALUES (
                ?,?,  ?,?,?,?,?,
                ?,?,?,?,?,?,  ?,?,?,?,?,?,  ?,?,
                ?,?,
                ?,?,?,?
            )
        """, (
            listing_id, person_number,
            result_code, first_name, last_name, age, deceased,
            phones[0][0], phones[0][1], phones[1][0], phones[1][1],
            phones[2][0], phones[2][1], phones[3][0], phones[3][1],
            phones[4][0], phones[4][1], phones[5][0], phones[5][1],
            phones[6][0], phones[6][1],
            email1, email2,
            mailing_street, mailing_city, mailing_state, mailing_zip,
        ))


def _upsert_relative(
    listing_id: int,
    person_number: int,
    relative_number: int,
    name: str,
    age: str,
    phones: list[tuple[str, str]],
) -> None:
    """INSERT OR REPLACE a directskip_relatives row."""
    if not name:
        return
    phones = (phones + [("", "")] * 5)[:5]
    with _conn() as con:
        con.execute("""
            INSERT OR REPLACE INTO directskip_relatives (
                listing_id, person_number, relative_number,
                name, age,
                phone1, phone1_type, phone2, phone2_type, phone3, phone3_type,
                phone4, phone4_type, phone5, phone5_type
            ) VALUES (?,?,?, ?,?, ?,?,?,?,?,?, ?,?,?,?)
        """, (
            listing_id, person_number, relative_number,
            name, age,
            phones[0][0], phones[0][1], phones[1][0], phones[1][1],
            phones[2][0], phones[2][1], phones[3][0], phones[3][1],
            phones[4][0], phones[4][1],
        ))


# ── Main ingestion ────────────────────────────────────────────────────────────

def ingest(csv_path: str | Path, dry_run: bool = False) -> dict:
    """
    Parse a DirectSkip results CSV and write all data to the DB.

    Returns a summary dict:
        matched      — rows that found a listing in the DB
        unmatched    — rows with no DB match (logged as warnings)
        persons      — total person records written
        relatives    — total relative records written
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"DirectSkip CSV not found: {csv_path}")

    db.init_db()
    today = date.today().isoformat()

    counts = {"matched": 0, "unmatched": 0, "persons": 0, "relatives": 0}
    unmatched_rows: list[str] = []

    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    print(f"\n[DIRECTSKIP] Ingesting {len(rows)} rows from {csv_path.name}...")

    for row in rows:
        county          = _v(row, "Input Custom Field 3")
        property_addr   = _v(row, "Input Property Address")
        sale_date       = _v(row, "Input Custom Field 2")

        listing_id = _find_listing_id(county, property_addr, sale_date)

        if listing_id is None:
            label = f"{_v(row,'Input Last Name')}, {_v(row,'Input First Name')} — {property_addr}, {county} {sale_date}"
            print(f"  [NO MATCH] {label}")
            unmatched_rows.append(label)
            counts["unmatched"] += 1
            continue

        counts["matched"] += 1

        if dry_run:
            print(f"  [DRY RUN] id={listing_id}: {_v(row,'Input First Name')} {_v(row,'Input Last Name')} — {property_addr}")
            continue

        # ── Person 1 ──────────────────────────────────────────────────────────
        _upsert_person(
            listing_id      = listing_id,
            person_number   = 1,
            result_code     = _v(row, "ResultCode"),
            first_name      = _v(row, "Matched First Name"),
            last_name       = _v(row, "Matched Last Name"),
            age             = _v(row, "Age"),
            deceased        = _v(row, "Deceased"),
            phones          = _phone_pairs(row, "", 7),
            email1          = _v(row, "Email1"),
            email2          = _v(row, "Email2"),
            mailing_street  = _v(row, "Confirmed Mailing Address"),
            mailing_city    = _v(row, "Confirmed Mailing City"),
            mailing_state   = _v(row, "Confirmed Mailing State"),
            mailing_zip     = _v(row, "Confirmed Mailing Zip"),
        )
        counts["persons"] += 1

        for rel_n in range(1, 6):
            _upsert_relative(
                listing_id      = listing_id,
                person_number   = 1,
                relative_number = rel_n,
                name            = _v(row, f"Relative{rel_n} Name"),
                age             = _v(row, f"Relative{rel_n} Age"),
                phones          = _phone_pairs(row, f"Relative{rel_n} ", 5),
            )
            if _v(row, f"Relative{rel_n} Name"):
                counts["relatives"] += 1

        # ── Person 2 ──────────────────────────────────────────────────────────
        if _v(row, "Person2 First Name") or _v(row, "Person2 Last Name"):
            _upsert_person(
                listing_id      = listing_id,
                person_number   = 2,
                result_code     = "",
                first_name      = _v(row, "Person2 First Name"),
                last_name       = _v(row, "Person2 Last Name"),
                age             = _v(row, "Person2 Age"),
                deceased        = _v(row, "Person2 Deceased"),
                phones          = _phone_pairs(row, "Person2 ", 7),
                email1          = _v(row, "Person2 Email1"),
                email2          = _v(row, "Person2 Email2"),
                mailing_street  = _v(row, "Person2 Confirmed Mailing Address"),
                mailing_city    = _v(row, "Person2 Confirmed Mailing City"),
                mailing_state   = _v(row, "Person2 Confirmed Mailing State"),
                mailing_zip     = _v(row, "Person2 Confirmed Mailing Zip"),
            )
            counts["persons"] += 1

            for rel_n in range(1, 6):
                _upsert_relative(
                    listing_id      = listing_id,
                    person_number   = 2,
                    relative_number = rel_n,
                    name            = _v(row, f"Person2 Relative{rel_n} Name"),
                    age             = _v(row, f"Person2 Relative{rel_n} Age"),
                    phones          = _phone_pairs(row, f"Person2 Relative{rel_n} ", 5),
                )
                if _v(row, f"Person2 Relative{rel_n} Name"):
                    counts["relatives"] += 1

        # ── Person 3 ──────────────────────────────────────────────────────────
        if _v(row, "Person3 First Name") or _v(row, "Person3 Last Name"):
            _upsert_person(
                listing_id      = listing_id,
                person_number   = 3,
                result_code     = "",
                first_name      = _v(row, "Person3 First Name"),
                last_name       = _v(row, "Person3 Last Name"),
                age             = _v(row, "Person3 Age"),
                deceased        = _v(row, "Person3 Deceased"),
                phones          = _phone_pairs(row, "Person3 ", 7),
                email1          = _v(row, "Person3 Email1"),
                email2          = _v(row, "Person3 Email2"),
                mailing_street  = _v(row, "Person3 Confirmed Mailing Address"),
                mailing_city    = _v(row, "Person3 Confirmed Mailing City"),
                mailing_state   = _v(row, "Person3 Confirmed Mailing State"),
                mailing_zip     = _v(row, "Person3 Confirmed Mailing Zip"),
            )
            counts["persons"] += 1

            for rel_n in range(1, 6):
                _upsert_relative(
                    listing_id      = listing_id,
                    person_number   = 3,
                    relative_number = rel_n,
                    name            = _v(row, f"Person3 Relative{rel_n} Name"),
                    age             = _v(row, f"Person3 Relative{rel_n} Age"),
                    phones          = _phone_pairs(row, f"Person3 Relative{rel_n} ", 5),
                )
                if _v(row, f"Person3 Relative{rel_n} Name"):
                    counts["relatives"] += 1

        # ── Stamp directskip_date on the listing ──────────────────────────────
        with _conn() as con:
            con.execute(
                "UPDATE listings SET directskip_date=? WHERE id=?",
                (today, listing_id),
            )

    print(f"\n  Matched:   {counts['matched']}")
    print(f"  Unmatched: {counts['unmatched']}")
    if not dry_run:
        print(f"  Persons written:   {counts['persons']}")
        print(f"  Relatives written: {counts['relatives']}")

    if unmatched_rows:
        print(f"\n  Unmatched rows (check manually):")
        for label in unmatched_rows:
            print(f"    {label}")

    return counts
