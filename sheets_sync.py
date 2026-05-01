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
from datetime import date, timedelta
from typing import Any

from db import _conn, _row_to_dict, MIN_DAYS_OUT
from sheets_writer import _get_service
from config import SPREADSHEET_ID

logger = logging.getLogger(__name__)

# ── Tab names ─────────────────────────────────────────────────────────────────

TAB_AUCTIONS     = "Auctions"
TAB_HEIR_LEADS   = "Heir Leads"
TAB_NEEDS_REVIEW = "Needs Review"
TAB_DIRECTSKIP   = "DirectSkip"
TAB_DS_PERSONS   = "DS Persons"
TAB_DS_RELATIVES = "DS Relatives"

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


def _ensure_grid_rows(svc, sheet_id: int, needed_rows: int) -> None:
    """Expand a sheet's rowCount to at least needed_rows."""
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [{
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"rowCount": needed_rows},
                    },
                    "fields": "gridProperties.rowCount",
                }
            }]
        },
    ).execute()


def _clear_and_write(
    svc,
    tab: str,
    values: list[list[Any]],
    sheet_id: int | None = None,
) -> None:
    """Clear a tab then write values starting at A1. Chunks by 500 rows.

    If sheet_id is provided the sheet's rowCount is expanded as needed so
    writes never crash against Google Sheets' default 1 000-row grid limit.
    """
    if not values:
        return

    # Expand the grid if we have more rows than the current limit
    if sheet_id is not None:
        needed = max(1000, len(values) + 50)
        _ensure_grid_rows(svc, sheet_id, needed)

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
    Return listings that meet equity criteria and are at least MIN_DAYS_OUT
    days in the future, sorted by signal priority:
      🏆 first → ✅ → ❓ / blank (not yet valuated) last.
    Rows with ⚠️ or ❌ are excluded — below the threshold we act on.
    Within each signal group, upcoming sale dates come first.
    """
    cutoff = str(date.today() + timedelta(days=MIN_DAYS_OUT))
    with _conn() as con:
        rows = con.execute("""
            SELECT * FROM listings
            WHERE (equity_signal IN ('🏆', '✅', '❓')
               OR equity_signal IS NULL
               OR equity_signal = '')
              AND sale_date >= ?
            ORDER BY
              CASE equity_signal
                WHEN '🏆' THEN 1
                WHEN '✅' THEN 2
                ELSE 3
              END,
              sale_date ASC,
              id
        """, (cutoff,)).fetchall()
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


def _split_mailing_address(addr: str) -> tuple[str, str, str, str]:
    """
    Parse a mailing address string (from skiptrace._extract_mailing_address)
    into (street, city, state, zip).

    Expected format: "123 Main St, Louisville, KY 40202"
    Handles partial / missing components gracefully.
    """
    if not (addr or "").strip():
        return "", "", "", ""
    parts = [p.strip() for p in addr.split(",")]
    street = parts[0] if parts else ""
    city   = parts[1] if len(parts) > 1 else ""
    state = zip_ = ""
    if len(parts) > 2:
        state_zip = parts[2].strip().split()
        state = state_zip[0] if state_zip else ""
        zip_  = state_zip[1] if len(state_zip) > 1 else ""
    return street, city, state, zip_


# DirectSkip column headers match the upload template exactly.
DIRECTSKIP_HEADERS = [
    "First Name", "Last Name",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Custom Field 1", "Custom Field 2", "Custom Field 3",
]


def _build_directskip_rows() -> list[list]:
    """
    Build DirectSkip upload rows from qualifying listings.

    Inclusion criteria:
      - equity_signal IN (🏆, ✅, ❓) or blank (not yet valuated)
      - sale_date >= today (upcoming only)
      - not cancelled
      - owner_last is populated (must have been skip-traced)
      - street is populated

    Mailing address: use BatchData mailing_address if present;
    fall back to property address (street/city/state/zip).

    Custom fields: Equity Signal | Sale Date | County
    """
    from datetime import date as _date
    today = _date.today().isoformat()

    with _conn() as con:
        rows = con.execute("""
            SELECT * FROM listings
            WHERE (owner_last IS NOT NULL AND owner_last != '')
              AND (street     IS NOT NULL AND street     != '')
              AND (cancelled  IS NULL OR LOWER(cancelled) != 'yes')
              AND sale_date >= ?
              AND (
                    equity_signal IN ('🏆', '✅', '❓')
                    OR equity_signal IS NULL
                    OR equity_signal = ''
              )
              AND (directskip_date IS NULL OR directskip_date = '')
            ORDER BY
              CASE equity_signal
                WHEN '🏆' THEN 1
                WHEN '✅' THEN 2
                ELSE 3
              END,
              sale_date ASC,
              id
        """, (today,)).fetchall()

    result = [DIRECTSKIP_HEADERS]
    for r in rows:
        mailing_raw = (r["mailing_address"] or "").strip()
        if mailing_raw:
            m_street, m_city, m_state, m_zip = _split_mailing_address(mailing_raw)
        else:
            # Fall back to property address
            m_street = r["street"] or ""
            m_city   = r["city"]   or ""
            m_state  = r["state"]  or ""
            m_zip    = r["zip"]    or ""

        result.append([
            r["owner_first"] or "",
            r["owner_last"]  or "",
            r["street"]      or "",
            r["city"]        or "",
            r["state"]       or "",
            r["zip"]         or "",
            m_street,
            m_city,
            m_state,
            m_zip,
            r["equity_signal"] or "",   # Custom Field 1
            r["sale_date"]     or "",   # Custom Field 2
            (r["county"] or "").title(), # Custom Field 3
        ])

    return result


# ── DirectSkip Persons / Relatives tabs ──────────────────────────────────────

DS_PERSONS_HEADERS = [
    "Listing ID", "County", "State", "Sale Date", "Property Street",
    "Person #", "Result Code",
    "First Name", "Last Name", "Age", "Deceased",
    "Phone 1", "Phone 1 Type", "Phone 2", "Phone 2 Type",
    "Phone 3", "Phone 3 Type", "Phone 4", "Phone 4 Type",
    "Phone 5", "Phone 5 Type", "Phone 6", "Phone 6 Type",
    "Phone 7", "Phone 7 Type",
    "Email 1", "Email 2",
    "Mailing Street", "Mailing City", "Mailing State", "Mailing Zip",
]

DS_RELATIVES_HEADERS = [
    "Listing ID", "County", "State", "Sale Date", "Property Street",
    "Person #", "Relative #",
    "Name", "Age",
    "Phone 1", "Phone 1 Type", "Phone 2", "Phone 2 Type",
    "Phone 3", "Phone 3 Type", "Phone 4", "Phone 4 Type",
    "Phone 5", "Phone 5 Type",
    "Called", "Call Date",
]


def _build_ds_persons_rows() -> list[list]:
    """Join directskip_persons with listings for context columns."""
    cutoff = str(date.today() + timedelta(days=MIN_DAYS_OUT))
    with _conn() as con:
        rows = con.execute("""
            SELECT
                p.listing_id, l.county, l.state, l.sale_date, l.street,
                p.person_number, p.result_code,
                p.first_name, p.last_name, p.age, p.deceased,
                p.phone1, p.phone1_type, p.phone2, p.phone2_type,
                p.phone3, p.phone3_type, p.phone4, p.phone4_type,
                p.phone5, p.phone5_type, p.phone6, p.phone6_type,
                p.phone7, p.phone7_type,
                p.email1, p.email2,
                p.mailing_street, p.mailing_city, p.mailing_state, p.mailing_zip
            FROM directskip_persons p
            JOIN listings l ON l.id = p.listing_id
            WHERE l.equity_signal IN ('🏆', '✅')
              AND l.sale_date >= ?
            ORDER BY p.listing_id, p.person_number
        """, (cutoff,)).fetchall()

    result = [DS_PERSONS_HEADERS]
    for r in rows:
        result.append([
            r[0],                       # listing_id
            (r[1] or "").title(),       # county
            r[2] or "",                 # state
            r[3] or "",                 # sale_date
            r[4] or "",                 # street
            r[5],                       # person_number
            r[6] or "",                 # result_code
            r[7] or "",  r[8] or "",   # first, last
            r[9] or "",  r[10] or "",  # age, deceased
            r[11] or "", r[12] or "",  # phone1, type
            r[13] or "", r[14] or "",  # phone2, type
            r[15] or "", r[16] or "",  # phone3, type
            r[17] or "", r[18] or "",  # phone4, type
            r[19] or "", r[20] or "",  # phone5, type
            r[21] or "", r[22] or "",  # phone6, type
            r[23] or "", r[24] or "",  # phone7, type
            r[25] or "", r[26] or "",  # email1, email2
            r[27] or "", r[28] or "", r[29] or "", r[30] or "",  # mailing
        ])
    return result


def _build_ds_relatives_rows() -> list[list]:
    """Join directskip_relatives with listings for context columns."""
    cutoff = str(date.today() + timedelta(days=MIN_DAYS_OUT))
    with _conn() as con:
        rows = con.execute("""
            SELECT
                r.listing_id, l.county, l.state, l.sale_date, l.street,
                r.person_number, r.relative_number,
                r.name, r.age,
                r.phone1, r.phone1_type, r.phone2, r.phone2_type,
                r.phone3, r.phone3_type, r.phone4, r.phone4_type,
                r.phone5, r.phone5_type,
                r.called, r.call_date
            FROM directskip_relatives r
            JOIN listings l ON l.id = r.listing_id
            WHERE r.name IS NOT NULL AND r.name != ''
              AND l.equity_signal IN ('🏆', '✅')
              AND l.sale_date >= ?
            ORDER BY r.listing_id, r.person_number, r.relative_number
        """, (cutoff,)).fetchall()

    result = [DS_RELATIVES_HEADERS]
    for r in rows:
        result.append([
            r[0],                       # listing_id
            (r[1] or "").title(),       # county
            r[2] or "",                 # state
            r[3] or "",                 # sale_date
            r[4] or "",                 # street
            r[5],                       # person_number
            r[6],                       # relative_number
            r[7] or "",  r[8] or "",   # name, age
            r[9] or "",  r[10] or "",  # phone1, type
            r[11] or "", r[12] or "",  # phone2, type
            r[13] or "", r[14] or "",  # phone3, type
            r[15] or "", r[16] or "",  # phone4, type
            r[17] or "", r[18] or "",  # phone5, type
            r[19] or 0,  r[20] or "",  # called, call_date
        ])
    return result


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

        for title in (TAB_AUCTIONS, TAB_HEIR_LEADS, TAB_NEEDS_REVIEW,
                      TAB_DIRECTSKIP, TAB_DS_PERSONS, TAB_DS_RELATIVES):
            _ensure_tab(svc, title, existing_titles)

        # Re-fetch metadata after any new tabs were created so sheet IDs are current
        meta    = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        tab_ids = {
            s["properties"]["title"]: s["properties"]["sheetId"]
            for s in meta.get("sheets", [])
        }

        # ── Auctions ──────────────────────────────────────────────────────────
        listings = _read_all_listings()
        values   = _rows_to_values(listings, AUCTIONS_COLS)
        _clear_and_write(svc, TAB_AUCTIONS, values, tab_ids.get(TAB_AUCTIONS))
        logger.info(f"  [SYNC] Auctions: {len(listings)} row(s) written.")

        # ── Heir Leads ────────────────────────────────────────────────────────
        leads  = _read_all_heir_leads()
        values = _rows_to_values(leads, HEIR_LEADS_COLS)
        _clear_and_write(svc, TAB_HEIR_LEADS, values, tab_ids.get(TAB_HEIR_LEADS))
        logger.info(f"  [SYNC] Heir Leads: {len(leads)} row(s) written.")

        # ── Needs Review ──────────────────────────────────────────────────────
        review = _read_all_needs_review()
        values = _rows_to_values(review, NEEDS_REVIEW_COLS)
        _clear_and_write(svc, TAB_NEEDS_REVIEW, values, tab_ids.get(TAB_NEEDS_REVIEW))
        logger.info(f"  [SYNC] Needs Review: {len(review)} row(s) written.")

        # ── DirectSkip (upload queue) ─────────────────────────────────────────
        ds_rows = _build_directskip_rows()
        _clear_and_write(svc, TAB_DIRECTSKIP, ds_rows, tab_ids.get(TAB_DIRECTSKIP))
        logger.info(f"  [SYNC] DirectSkip: {len(ds_rows) - 1} row(s) written.")

        # ── DS Persons ────────────────────────────────────────────────────────
        dsp_rows = _build_ds_persons_rows()
        _clear_and_write(svc, TAB_DS_PERSONS, dsp_rows, tab_ids.get(TAB_DS_PERSONS))
        logger.info(f"  [SYNC] DS Persons: {len(dsp_rows) - 1} row(s) written.")

        # ── DS Relatives ──────────────────────────────────────────────────────
        dsr_rows = _build_ds_relatives_rows()
        _clear_and_write(svc, TAB_DS_RELATIVES, dsr_rows, tab_ids.get(TAB_DS_RELATIVES))
        logger.info(f"  [SYNC] DS Relatives: {len(dsr_rows) - 1} row(s) written.")

    except Exception as e:
        logger.error(f"  [SYNC] Sheets sync failed: {e}")
        import traceback
        traceback.print_exc()
