"""
sheets_sync.py — Dump SQLite → Google Sheets after each pipeline batch.

Called at the end of run_scrape(), run_valuate(), run_skiptrace(),
run_heirresearch(), and run_heir_skiptrace() in main.py (skip dry-run).

Overwrites three tabs completely on each call:
  Auctions     ← db.listings    (all rows, newest sale date first)
  Heir Leads   ← db.heir_leads  (all rows)
  Needs Review ← db.needs_review (all unreviewed rows)

Auth reuses the same OAuth token/service as sheets_writer.py.
"""

from __future__ import annotations

import logging
from typing import Any

from db import _conn, _row_to_dict
from sheets_writer import _get_service
from config import SPREADSHEET_ID

logger = logging.getLogger(__name__)

# ── Tab names ─────────────────────────────────────────────────────────────────

TAB_AUCTIONS    = "Auctions"
TAB_HEIR_LEADS  = "Heir Leads"
TAB_NEEDS_REVIEW = "Needs Review"

# ── Column definitions ────────────────────────────────────────────────────────
#
# Each entry is (sheet_header, db_column_name).
# Rows are built by pulling db_column_name from each DB row dict.
#
# county is stored lowercase in DB; we title-case it on output.

AUCTIONS_COLS: list[tuple[str, str]] = [
    ("ID",                      "id"),
    ("County",                  "county"),
    ("State",                   "state"),
    ("Sale Date",               "sale_date"),
    ("Case Number",             "case_number"),
    ("Plaintiff",               "plaintiff"),
    ("Defendant(s)",            "defendants"),
    ("Street",                  "street"),
    ("City",                    "city"),
    ("Zip",                     "zip"),
    ("Appraised Value",         "appraised_value"),
    ("Judgment / Loan Amount",  "judgment"),
    ("Attorney / Firm",         "attorney"),
    ("Cancelled",               "cancelled"),
    ("Estimated Market Value",  "est_market_value"),
    ("Estimated Equity",        "est_equity"),
    ("Equity Signal",           "equity_signal"),
    ("Notes",                   "notes"),
    ("Owner First Name",                "owner_first"),
    ("Owner Last Name",                 "owner_last"),
    ("Owner First Name (Secondary)",    "owner_secondary_first"),
    ("Owner Last Name (Secondary)",     "owner_secondary_last"),
    ("Owner Phone(s)",                  "owner_phones"),
    ("Owner Email(s)",          "owner_emails"),
    ("Mailing Address",         "mailing_address"),
    ("Deceased",                "deceased"),
    ("Skip Trace Date",         "skiptrace_date"),
    ("Obit Found",              "obit_found"),
    ("Obit Summary",            "obit_summary"),
    ("Heirs",                   "heirs"),
    ("Defendant Match",         "defendant_match"),
    ("Heir Research Date",      "heir_research_date"),
    ("Source URL",              "source_url"),
    ("Date Added",              "date_added"),
]

HEIR_LEADS_COLS: list[tuple[str, str]] = [
    ("ID",              "id"),
    ("Listing ID",      "listing_id"),
    ("Property Street", "property_street"),
    ("Property City",   "property_city"),
    ("County",         "county"),
    ("State",           "state"),
    ("Sale Date",       "sale_date"),
    ("Equity Signal",   "equity_signal"),
    ("Deceased Owner",  "deceased_owner"),
    ("Heir Name",       "heir_name"),
    ("Relationship",    "relationship"),
    ("Defendant Match", "defendant_match"),
    ("Phone(s)",        "phones"),
    ("Email(s)",        "emails"),
    ("Mailing Address", "mailing_address"),
    ("Skip Traced Date","skip_traced_date"),
    ("Status",          "status"),
]

NEEDS_REVIEW_COLS: list[tuple[str, str]] = [
    ("ID",                      "id"),
    ("County",                  "county"),
    ("State",                   "state"),
    ("Sale Date",               "sale_date"),
    ("Case Number",             "case_number"),
    ("Plaintiff",               "plaintiff"),
    ("Defendant(s)",            "defendants"),
    ("Street",                  "street"),
    ("City",                    "city"),
    ("Zip",                     "zip"),
    ("Appraised Value",         "appraised_value"),
    ("Judgment / Loan Amount",  "judgment"),
    ("Attorney / Firm",         "attorney"),
    ("Cancelled",               "cancelled"),
    ("Source URL",              "source_url"),
    ("Date Added",              "date_added"),
    ("Notes",                   "notes"),
    ("Reason",                  "reason"),
    ("Reviewed",                "reviewed"),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _col_letter(n: int) -> str:
    """Convert 1-based column index to A-Z / AA-ZZ letter string."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _rows_to_values(
    db_rows: list[dict],
    col_defs: list[tuple[str, str]],
) -> list[list[Any]]:
    """Build a 2-D list (header row + data rows) from DB row dicts."""
    headers = [h for h, _ in col_defs]
    data: list[list[Any]] = [headers]
    for row in db_rows:
        cells: list[Any] = []
        for _, db_col in col_defs:
            val = row.get(db_col, "") or ""
            # Title-case county for readability (stored lowercase in DB)
            if db_col == "county" and isinstance(val, str):
                val = val.title()
            cells.append(val)
        data.append(cells)
    return data


def _ensure_tab(svc, title: str, existing_titles: set[str]) -> None:
    """Create a sheet tab if it doesn't already exist."""
    if title in existing_titles:
        return
    logger.info(f"  [SYNC] Creating tab '{title}'...")
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
    ).execute()


def _clear_and_write(svc, tab: str, values: list[list[Any]]) -> None:
    """Clear a tab then write values starting at A1. Chunks by 500 rows."""
    if not values:
        return

    n_cols    = len(values[0])
    last_col  = _col_letter(n_cols)
    full_range = f"{tab}!A:{last_col}"

    # Clear
    svc.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=full_range,
    ).execute()

    # Write in chunks of 500 rows to avoid Windows socket issues on large payloads
    chunk_size = 500
    for start in range(0, len(values), chunk_size):
        chunk      = values[start : start + chunk_size]
        start_row  = start + 1          # 1-based
        write_range = f"{tab}!A{start_row}:{last_col}{start_row + len(chunk) - 1}"
        svc.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=write_range,
            valueInputOption="USER_ENTERED",
            body={"values": chunk},
        ).execute()


# ── DB reads ──────────────────────────────────────────────────────────────────

def _read_all_listings() -> list[dict]:
    """
    Return listings that meet equity criteria, sorted by signal priority:
      🏆 first → ✅ → ❓ / blank (not yet valuated) last.
    Rows with ⚠️ or ❌ are excluded — below the threshold we act on.
    Within each signal group, upcoming sale dates come first.
    """
    with _conn() as con:
        rows = con.execute("""
            SELECT * FROM listings
            WHERE equity_signal IN ('🏆', '✅', '❓')
               OR equity_signal IS NULL
               OR equity_signal = ''
            ORDER BY
              CASE equity_signal
                WHEN '🏆' THEN 1
                WHEN '✅' THEN 2
                ELSE 3
              END,
              sale_date ASC,
              id
        """).fetchall()
    return [_row_to_dict(r) for r in rows]


def _read_all_heir_leads() -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM heir_leads ORDER BY id"
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def _read_all_needs_review() -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM needs_review ORDER BY id"
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


# ── Public entry point ────────────────────────────────────────────────────────

def sync_to_sheets() -> None:
    """
    Dump all three SQLite tables to Google Sheets, overwriting each tab.
    Safe to call after any pipeline batch; silently logs errors so a
    Sheets failure never aborts the pipeline.
    """
    try:
        svc = _get_service()

        # Discover existing tabs so we can create any that are missing
        meta            = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        existing_titles = {s["properties"]["title"] for s in meta.get("sheets", [])}

        for title in (TAB_AUCTIONS, TAB_HEIR_LEADS, TAB_NEEDS_REVIEW):
            _ensure_tab(svc, title, existing_titles)

        # ── Auctions ──────────────────────────────────────────────────────────
        listings = _read_all_listings()
        values   = _rows_to_values(listings, AUCTIONS_COLS)
        _clear_and_write(svc, TAB_AUCTIONS, values)
        logger.info(f"  [SYNC] Auctions: {len(listings)} row(s) written.")

        # ── Heir Leads ────────────────────────────────────────────────────────
        leads  = _read_all_heir_leads()
        values = _rows_to_values(leads, HEIR_LEADS_COLS)
        _clear_and_write(svc, TAB_HEIR_LEADS, values)
        logger.info(f"  [SYNC] Heir Leads: {len(leads)} row(s) written.")

        # ── Needs Review ──────────────────────────────────────────────────────
        review = _read_all_needs_review()
        values = _rows_to_values(review, NEEDS_REVIEW_COLS)
        _clear_and_write(svc, TAB_NEEDS_REVIEW, values)
        logger.info(f"  [SYNC] Needs Review: {len(review)} row(s) written.")

    except Exception as e:
        logger.error(f"  [SYNC] Sheets sync failed: {e}")
        import traceback
        traceback.print_exc()
