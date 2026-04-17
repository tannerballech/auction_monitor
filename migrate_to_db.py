"""
migrate_to_db.py — One-time migration from Google Sheets → SQLite.

Reads all three tabs (Auctions, Needs Review, Heir Leads) and inserts every
row into the local database.  Safe to re-run: existing rows (same dedup key)
are skipped without error.

Usage:
    python migrate_to_db.py [--dry-run]
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

import db
from sheets_writer import (
    _get_all_rows_full,
    _get_all_rows,
    TAB_MAIN,
    TAB_REVIEW,
    TAB_HEIR_LEADS,
    COL_COUNTY, COL_STATE, COL_SALE_DATE, COL_CASE_NUM, COL_PLAINTIFF,
    COL_DEFENDANT, COL_STREET, COL_CITY, COL_ZIP, COL_APPRAISED,
    COL_JUDGMENT, COL_ATTORNEY, COL_EMV, COL_EQUITY_DOL, COL_SIGNAL,
    COL_CANCELLED, COL_SOURCE_URL, COL_DATE_ADDED, COL_NOTES,
    COL_OWNER_PRIMARY, COL_OWNER_SECONDARY, COL_PHONES, COL_EMAILS,
    COL_MAILING_ADDR, COL_DECEASED, COL_ST_DATE,
    COL_OBIT_FOUND, COL_OBIT_SUMMARY, COL_HEIRS, COL_DEF_MATCH, COL_HR_DATE,
    HL_COL_STREET, HL_COL_CITY, HL_COL_COUNTY, HL_COL_STATE,
    HL_COL_SALE_DATE, HL_COL_SIGNAL, HL_COL_OWNER, HL_COL_HEIR_NAME,
    HL_COL_REL, HL_COL_DEF_MATCH, HL_COL_PHONES, HL_COL_EMAILS,
    HL_COL_MAILING, HL_COL_ST_DATE, HL_COL_STATUS,
    HEADER_MAIN, HEADER_REVIEW, HEIR_LEADS_HEADER,
)


def _cell(row: list, idx: int) -> str:
    return row[idx].strip() if len(row) > idx and row[idx] else ""


def _is_header(row: list, expected_header: list) -> bool:
    if not row:
        return False
    return row[0].strip() == expected_header[0].strip()


# ---------------------------------------------------------------------------
# Auctions tab
# ---------------------------------------------------------------------------

def migrate_auctions(dry_run: bool) -> tuple[int, int]:
    print("\n── Auctions tab ──────────────────────────────────────────────")
    rows = _get_all_rows_full(TAB_MAIN)
    if not rows:
        print("  (empty)")
        return 0, 0

    inserted = skipped = 0
    for i, row in enumerate(rows):
        if i == 0 or _is_header(row, HEADER_MAIN):
            continue

        listing = {
            "County":                   _cell(row, COL_COUNTY),
            "State":                    _cell(row, COL_STATE),
            "Sale Date":                _cell(row, COL_SALE_DATE),
            "Case Number":              _cell(row, COL_CASE_NUM),
            "Plaintiff":                _cell(row, COL_PLAINTIFF),
            "Defendant(s)":             _cell(row, COL_DEFENDANT),
            "Street":                   _cell(row, COL_STREET),
            "City":                     _cell(row, COL_CITY),
            "Zip":                      _cell(row, COL_ZIP),
            "Appraised Value":          _cell(row, COL_APPRAISED),
            "Judgment / Loan Amount":   _cell(row, COL_JUDGMENT),
            "Attorney / Firm":          _cell(row, COL_ATTORNEY),
            "Cancelled":                _cell(row, COL_CANCELLED),
            "Source URL":               _cell(row, COL_SOURCE_URL),
            "Date Added":               _cell(row, COL_DATE_ADDED),
            "Notes":                    _cell(row, COL_NOTES),
        }

        # Phase 2 valuation fields
        emv     = _cell(row, COL_EMV)
        equity  = _cell(row, COL_EQUITY_DOL)
        signal  = _cell(row, COL_SIGNAL)

        # Phase 3 skip trace fields — skip the bad April 14 run entirely
        st_date = _cell(row, COL_ST_DATE)
        if st_date == "2026-04-14":
            owner_primary = owner_secondary = phones = emails = ""
            mailing_addr = deceased = st_date = ""
        else:
            owner_primary   = _cell(row, COL_OWNER_PRIMARY)
            owner_secondary = _cell(row, COL_OWNER_SECONDARY)
            phones          = _cell(row, COL_PHONES)
            emails          = _cell(row, COL_EMAILS)
            mailing_addr    = _cell(row, COL_MAILING_ADDR)
            deceased        = _cell(row, COL_DECEASED)

        # Phase 4 heir research fields
        obit_found      = _cell(row, COL_OBIT_FOUND)
        obit_summary    = _cell(row, COL_OBIT_SUMMARY)
        heirs           = _cell(row, COL_HEIRS)
        def_match       = _cell(row, COL_DEF_MATCH)
        hr_date         = _cell(row, COL_HR_DATE)

        if not listing["County"] and not listing["Street"]:
            continue  # truly blank row

        if dry_run:
            print(f"  [DRY RUN] would insert: {listing['County']} | {listing['Street']} | {listing['Sale Date']}")
            inserted += 1
            continue

        row_id, was_inserted = db.insert_listing(listing)

        if was_inserted and row_id:
            # Back-fill downstream phases in one UPDATE
            import sqlite3
            import db as _db
            with _db._conn() as con:
                con.execute("""
                    UPDATE listings SET
                        est_market_value=?, est_equity=?, equity_signal=?,
                        owner_primary=?, owner_secondary=?, owner_phones=?,
                        owner_emails=?, mailing_address=?, deceased=?, skiptrace_date=?,
                        obit_found=?, obit_summary=?, heirs=?, defendant_match=?,
                        heir_research_date=?
                    WHERE id=?
                """, (
                    emv, equity, signal,
                    owner_primary, owner_secondary, phones,
                    emails, mailing_addr, deceased, st_date,
                    obit_found, obit_summary, heirs, def_match, hr_date,
                    row_id,
                ))
            print(f"  [OK]      id={row_id:>5}  {listing['County']:>12} | {listing['Street'][:35]:<35} | {listing['Sale Date']}")
            inserted += 1
        else:
            skipped += 1

    print(f"  Inserted: {inserted}   Skipped (duplicate): {skipped}")
    return inserted, skipped


# ---------------------------------------------------------------------------
# Needs Review tab
# ---------------------------------------------------------------------------

def migrate_needs_review(dry_run: bool) -> int:
    print("\n── Needs Review tab ──────────────────────────────────────────")
    rows = _get_all_rows(TAB_REVIEW)
    if not rows:
        print("  (empty)")
        return 0

    COL_REASON = 19  # T on Needs Review tab

    inserted = 0
    for i, row in enumerate(rows):
        if i == 0 or _is_header(row, HEADER_REVIEW):
            continue
        if not any(row):
            continue

        listing = {
            "County":                   _cell(row, COL_COUNTY),
            "State":                    _cell(row, COL_STATE),
            "Sale Date":                _cell(row, COL_SALE_DATE),
            "Case Number":              _cell(row, COL_CASE_NUM),
            "Plaintiff":                _cell(row, COL_PLAINTIFF),
            "Defendant(s)":             _cell(row, COL_DEFENDANT),
            "Street":                   _cell(row, COL_STREET),
            "City":                     _cell(row, COL_CITY),
            "Zip":                      _cell(row, COL_ZIP),
            "Appraised Value":          _cell(row, COL_APPRAISED),
            "Judgment / Loan Amount":   _cell(row, COL_JUDGMENT),
            "Attorney / Firm":          _cell(row, COL_ATTORNEY),
            "Cancelled":                _cell(row, COL_CANCELLED),
            "Source URL":               _cell(row, COL_SOURCE_URL),
            "Date Added":               _cell(row, COL_DATE_ADDED),
            "Notes":                    _cell(row, COL_NOTES),
        }
        reason = _cell(row, COL_REASON)

        if dry_run:
            print(f"  [DRY RUN] would insert: {listing['Street'] or '(no street)'} — {reason[:60]}")
            inserted += 1
            continue

        db.insert_needs_review(listing, reason)
        print(f"  [OK] {listing['Street'] or '(no street)':<40} reason: {reason[:50]}")
        inserted += 1

    print(f"  Inserted: {inserted}")
    return inserted


# ---------------------------------------------------------------------------
# Heir Leads tab
# ---------------------------------------------------------------------------

def migrate_heir_leads(dry_run: bool) -> int:
    print("\n── Heir Leads tab ────────────────────────────────────────────")
    from sheets_writer import _get_service, SPREADSHEET_ID
    try:
        result = (
            _get_service().spreadsheets().values()
            .get(spreadsheetId=SPREADSHEET_ID, range="'Heir Leads'!A:O")
            .execute()
        )
        rows = result.get("values", [])
    except Exception as e:
        print(f"  Could not read Heir Leads tab: {e}")
        return 0

    if not rows:
        print("  (empty)")
        return 0

    inserted = 0
    for i, row in enumerate(rows):
        if i == 0 or _is_header(row, HEIR_LEADS_HEADER):
            continue
        if not any(row):
            continue

        lead = {
            "Property Street":  _cell(row, HL_COL_STREET),
            "Property City":    _cell(row, HL_COL_CITY),
            "County":           _cell(row, HL_COL_COUNTY),
            "State":            _cell(row, HL_COL_STATE),
            "Sale Date":        _cell(row, HL_COL_SALE_DATE),
            "Equity Signal":    _cell(row, HL_COL_SIGNAL),
            "Deceased Owner":   _cell(row, HL_COL_OWNER),
            "Heir Name":        _cell(row, HL_COL_HEIR_NAME),
            "Relationship":     _cell(row, HL_COL_REL),
            "Defendant Match":  _cell(row, HL_COL_DEF_MATCH),
            "Phone(s)":         _cell(row, HL_COL_PHONES),
            "Email(s)":         _cell(row, HL_COL_EMAILS),
            "Mailing Address":  _cell(row, HL_COL_MAILING),
            "Skip Traced Date": _cell(row, HL_COL_ST_DATE),
            "Status":           _cell(row, HL_COL_STATUS),
        }

        if dry_run:
            print(f"  [DRY RUN] would insert: {lead['Heir Name'] or '(no name)'} — {lead['Property Street']}")
            inserted += 1
            continue

        db.insert_heir_lead(None, lead)
        print(f"  [OK] {lead['Heir Name'] or '(no name)':<30} {lead['Property Street']}")
        inserted += 1

    print(f"  Inserted: {inserted}")
    return inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Google Sheets data to SQLite")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be inserted without writing")
    args = parser.parse_args()

    print(f"Database: {db.DB_PATH}")
    if not args.dry_run:
        db.init_db()
        print("Schema initialised.")
    else:
        print("DRY RUN — no data will be written.\n")

    a_ins, a_skip = migrate_auctions(args.dry_run)
    nr_ins        = migrate_needs_review(args.dry_run)
    hl_ins        = migrate_heir_leads(args.dry_run)

    print(f"""
╔══════════════════════════════╗
║  Migration summary           ║
╠══════════════════════════════╣
║  Auctions inserted : {a_ins:>6}  ║
║  Auctions skipped  : {a_skip:>6}  ║
║  Needs Review rows : {nr_ins:>6}  ║
║  Heir Lead rows    : {hl_ins:>6}  ║
╚══════════════════════════════╝
""")


if __name__ == "__main__":
    main()
