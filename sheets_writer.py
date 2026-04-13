"""
sheets_writer.py — Eagle Creek Auction Monitor
Handles all Google Sheets read/write operations.

Column layout — Auctions tab (post address-split refactor + Phase 3 + Phase 4):
  A  County                   col 0
  B  State                    col 1
  C  Sale Date                col 2
  D  Case Number              col 3
  E  Plaintiff                col 4
  F  Defendant(s)             col 5
  G  Street                   col 6
  H  City                     col 7
  I  Zip                      col 8
  J  Appraised Value          col 9
  K  Judgment / Loan Amount   col 10
  L  Attorney / Firm          col 11
  M  Estimated Market Value   col 12  — filled by --valuate
  N  Estimated Equity         col 13  — filled by --valuate
  O  Equity Signal            col 14  — filled by --valuate
  P  Cancelled                col 15
  Q  Source URL               col 16
  R  Date Added               col 17
  S  Notes                    col 18
  T  Owner Name (Primary)     col 19  — filled by --skiptrace
  U  Owner Name (Secondary)   col 20  — filled by --skiptrace
  V  Owner Phone(s)           col 21  — filled by --skiptrace
  W  Owner Email(s)           col 22  — filled by --skiptrace
  X  Mailing Address          col 23  — filled by --skiptrace
  Y  Deceased                 col 24  — filled by --skiptrace
  Z  Skip Trace Date          col 25  — filled by --skiptrace
  AA Obit Found               col 26  — filled by --heirresearch
  AB Obit Summary             col 27  — filled by --heirresearch
  AC Heirs                    col 28  — filled by --heirresearch
  AD Defendant Match          col 29  — filled by --heirresearch
  AE Heir Research Date       col 30  — filled by --heirresearch

  NOTE: Needs Review tab has "Reason" at col T (idx 19). The two tabs have
  independent schemas; Needs Review never receives skip trace or heir research
  columns.

Deduplication key (Auctions): county + street_number + sale_date + defendant_last_name
Admission gates (applied in order):
  1. "Street" must contain a parseable street number → else Needs Review
  2. "Sale Date" must be present and parseable as YYYY-MM-DD → else Needs Review
  3. Sale date must be >= MIN_DAYS_OUT calendar days from today → else silent skip
  4. Dedup key must not already exist in sheet or current batch → else silent skip

Heir Leads tab column layout:
  A  Property Street    col 0
  B  Property City      col 1
  C  County             col 2
  D  State              col 3
  E  Sale Date          col 4
  F  Equity Signal      col 5
  G  Deceased Owner     col 6
  H  Heir Name          col 7
  I  Relationship       col 8
  J  Defendant Match    col 9
  K  Phone(s)           col 10  — filled by --heirskiptrace
  L  Email(s)           col 11  — filled by --heirskiptrace
  M  Mailing Address    col 12  — filled by --heirskiptrace
  N  Skip Traced Date   col 13  — filled by --heirskiptrace
  O  Status             col 14
"""

from __future__ import annotations

import os
import re
from datetime import date, timedelta, datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

import sys
sys.path.insert(0, os.path.dirname(__file__))
from config import SPREADSHEET_ID


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]
TOKEN_FILE = "sheets_token.json"

TAB_MAIN       = "Auctions"
TAB_REVIEW     = "Needs Review"
TAB_HEIR_LEADS = "Heir Leads"

MIN_DAYS_OUT = 3

HEADER_MAIN = [
    "County",                   # A
    "State",                    # B
    "Sale Date",                # C
    "Case Number",              # D
    "Plaintiff",                # E
    "Defendant(s)",             # F
    "Street",                   # G
    "City",                     # H
    "Zip",                      # I
    "Appraised Value",          # J
    "Judgment / Loan Amount",   # K
    "Attorney / Firm",          # L
    "Estimated Market Value",   # M
    "Estimated Equity",         # N
    "Equity Signal",            # O
    "Cancelled",                # P
    "Source URL",               # Q
    "Date Added",               # R
    "Notes",                    # S
]

HEADER_REVIEW = HEADER_MAIN + ["Reason"]  # T on Needs Review tab only

HEIR_LEADS_HEADER = [
    "Property Street",   # A
    "Property City",     # B
    "County",            # C
    "State",             # D
    "Sale Date",         # E
    "Equity Signal",     # F
    "Deceased Owner",    # G
    "Heir Name",         # H
    "Relationship",      # I
    "Defendant Match",   # J
    "Phone(s)",          # K  — filled by --heirskiptrace
    "Email(s)",          # L  — filled by --heirskiptrace
    "Mailing Address",   # M  — filled by --heirskiptrace
    "Skip Traced Date",  # N  — filled by --heirskiptrace
    "Status",            # O
]

FILLABLE_COLUMNS = {
    "Appraised Value":        "J",
    "Judgment / Loan Amount": "K",
    "Attorney / Firm":        "L",
}

SKIPTRACE_COLUMNS = {
    "Owner Name (Primary)":   "T",
    "Owner Name (Secondary)": "U",
    "Owner Phone(s)":         "V",
    "Owner Email(s)":         "W",
    "Mailing Address":        "X",
    "Deceased":               "Y",
    "Skip Trace Date":        "Z",
}

HEIR_RESEARCH_COLUMNS = {
    "Obit Found":         "AA",
    "Obit Summary":       "AB",
    "Heirs":              "AC",
    "Defendant Match":    "AD",
    "Heir Research Date": "AE",
}

# Heir Leads tab — Phase 4b columns
HEIR_SKIPTRACE_COLUMNS = {
    "Phone(s)":         "K",
    "Email(s)":         "L",
    "Mailing Address":  "M",
    "Skip Traced Date": "N",
}

_SKIPTRACE_QUALIFYING_SIGNALS = {"🏆", "✅", "❓"}

# ---------------------------------------------------------------------------
# 0-based column indices — Auctions tab
# ---------------------------------------------------------------------------
COL_COUNTY      = 0   # A
COL_STATE       = 1   # B
COL_SALE_DATE   = 2   # C
COL_CASE_NUM    = 3   # D
COL_PLAINTIFF   = 4   # E
COL_DEFENDANT   = 5   # F
COL_STREET      = 6   # G
COL_CITY        = 7   # H
COL_ZIP         = 8   # I
COL_APPRAISED   = 9   # J
COL_JUDGMENT    = 10  # K
COL_ATTORNEY    = 11  # L
COL_EMV         = 12  # M
COL_EQUITY_DOL  = 13  # N
COL_SIGNAL      = 14  # O
COL_CANCELLED   = 15  # P
COL_SOURCE_URL  = 16  # Q
COL_DATE_ADDED  = 17  # R
COL_NOTES       = 18  # S
# Phase 3 skip trace
COL_OWNER_PRIMARY   = 19  # T
COL_OWNER_SECONDARY = 20  # U
COL_PHONES          = 21  # V
COL_EMAILS          = 22  # W
COL_MAILING_ADDR    = 23  # X
COL_DECEASED        = 24  # Y
COL_ST_DATE         = 25  # Z
# Phase 4 heir research
COL_OBIT_FOUND   = 26  # AA
COL_OBIT_SUMMARY = 27  # AB
COL_HEIRS        = 28  # AC
COL_DEF_MATCH    = 29  # AD
COL_HR_DATE      = 30  # AE
# Needs Review only
COL_REASON      = 19  # T

# 0-based column indices — Heir Leads tab
HL_COL_STREET      = 0   # A  Property Street
HL_COL_CITY        = 1   # B  Property City
HL_COL_COUNTY      = 2   # C  County
HL_COL_STATE       = 3   # D  State
HL_COL_SALE_DATE   = 4   # E  Sale Date
HL_COL_SIGNAL      = 5   # F  Equity Signal
HL_COL_OWNER       = 6   # G  Deceased Owner
HL_COL_HEIR_NAME   = 7   # H  Heir Name
HL_COL_REL         = 8   # I  Relationship
HL_COL_DEF_MATCH   = 9   # J  Defendant Match
HL_COL_PHONES      = 10  # K  Phone(s)        — Phase 4b
HL_COL_EMAILS      = 11  # L  Email(s)         — Phase 4b
HL_COL_MAILING     = 12  # M  Mailing Address  — Phase 4b
HL_COL_ST_DATE     = 13  # N  Skip Traced Date — Phase 4b
HL_COL_STATUS      = 14  # O  Status


# ---------------------------------------------------------------------------
# Auth — lazy singleton
# ---------------------------------------------------------------------------

_service = None

def _get_service():
    global _service
    if _service is not None:
        return _service

    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            from google_auth_oauthlib.flow import InstalledAppFlow
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    _service = build("sheets", "v4", credentials=creds)
    return _service


# ---------------------------------------------------------------------------
# Dedup key helpers
# ---------------------------------------------------------------------------

_DEFENDANT_PREFIXES = re.compile(
    r"^\s*(?:estate\s+of|unknown\s+heirs?\s+of|heirs?\s+of|"
    r"successors?\s+of|representatives?\s+of)\s+",
    re.IGNORECASE,
)

def _extract_defendant_last_name(defendant: str) -> str:
    if not defendant:
        return ""
    first = defendant.split(",")[0].strip()
    first = _DEFENDANT_PREFIXES.sub("", first).strip()
    if "," in first:
        last_name = first.split(",")[0].strip()
    else:
        parts = first.split()
        last_name = parts[-1] if parts else ""
    return last_name.lower()


def _extract_street_number(street: str) -> str:
    if not street:
        return ""
    m = re.match(r"^(\d+)", street.strip())
    return m.group(1) if m else ""


def _make_dedup_key(listing: dict) -> tuple:
    return (
        listing.get("County", "").strip().lower(),
        _extract_street_number(listing.get("Street", "").strip()),
        listing.get("Sale Date", "").strip(),
        _extract_defendant_last_name(listing.get("Defendant(s)", "").strip()),
    )


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _parse_sale_date(sale_date_str: str) -> date | None:
    if not sale_date_str or not sale_date_str.strip():
        return None
    try:
        return date.fromisoformat(sale_date_str.strip())
    except ValueError:
        return None


def _is_at_least_n_days_out(sale_date_str: str, n: int = MIN_DAYS_OUT) -> bool:
    parsed = _parse_sale_date(sale_date_str)
    return parsed is not None and parsed >= date.today() + timedelta(days=n)


# ---------------------------------------------------------------------------
# Low-level sheet I/O
# ---------------------------------------------------------------------------

def _get_all_rows(tab: str) -> list[list]:
    """Read A:S (19 cols) — sufficient for scrape/dedup/valuation."""
    result = (
        _get_service().spreadsheets().values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"{tab}!A:S")
        .execute()
    )
    return result.get("values", [])


def _get_all_rows_extended(tab: str) -> list[list]:
    """Read A:Z (26 cols) — for skip trace reads."""
    result = (
        _get_service().spreadsheets().values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"{tab}!A:Z")
        .execute()
    )
    return result.get("values", [])


def _get_all_rows_full(tab: str) -> list[list]:
    """Read A:AE (31 cols) — for heir research reads."""
    result = (
        _get_service().spreadsheets().values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"{tab}!A:AE")
        .execute()
    )
    return result.get("values", [])


def _append_rows(tab: str, rows: list[list]) -> None:
    _get_service().spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!A:A",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def _ensure_header(tab: str, header: list[str]) -> None:
    if not _get_all_rows(tab):
        _get_service().spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{tab}'!A1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()
        print(f"  [SHEETS] Header written to '{tab}' tab.")


def _tab_exists(tab_name: str) -> bool:
    meta = _get_service().spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID
    ).execute()
    return any(
        s["properties"]["title"] == tab_name
        for s in meta.get("sheets", [])
    )


# ---------------------------------------------------------------------------
# Existing-key index
# ---------------------------------------------------------------------------

def _build_existing_keys(rows: list[list]) -> set[tuple]:
    keys: set[tuple] = set()
    for row in rows[1:]:
        county    = row[COL_COUNTY].strip()    if len(row) > COL_COUNTY    else ""
        street    = row[COL_STREET].strip()    if len(row) > COL_STREET    else ""
        sale_date = row[COL_SALE_DATE].strip() if len(row) > COL_SALE_DATE else ""
        defendant = row[COL_DEFENDANT].strip() if len(row) > COL_DEFENDANT else ""
        street_num = _extract_street_number(street)
        last_name  = _extract_defendant_last_name(defendant)
        if street_num and sale_date:
            keys.add((county.lower(), street_num, sale_date, last_name))
    return keys


# ---------------------------------------------------------------------------
# Listing → row converters
# ---------------------------------------------------------------------------

def _listing_to_row(listing: dict) -> list:
    return [
        listing.get("County", ""),
        listing.get("State", ""),
        listing.get("Sale Date", ""),
        listing.get("Case Number", ""),
        listing.get("Plaintiff", ""),
        listing.get("Defendant(s)", ""),
        listing.get("Street", ""),
        listing.get("City", ""),
        listing.get("Zip", ""),
        listing.get("Appraised Value", ""),
        listing.get("Judgment / Loan Amount", ""),
        listing.get("Attorney / Firm", ""),
        "",  # M EMV
        "",  # N Equity $
        "",  # O Signal
        listing.get("Cancelled", ""),
        listing.get("Source URL", ""),
        date.today().isoformat(),
        listing.get("Notes", ""),
    ]


def _listing_to_review_row(listing: dict, reason: str) -> list:
    return _listing_to_row(listing) + [reason]


# ---------------------------------------------------------------------------
# Public API — Scrape pipeline
# ---------------------------------------------------------------------------

def write_new_listings(listings: list[dict], dry_run: bool = False) -> dict:
    _ensure_header(TAB_MAIN,   HEADER_MAIN)
    _ensure_header(TAB_REVIEW, HEADER_REVIEW)

    existing_keys = _build_existing_keys(_get_all_rows(TAB_MAIN))
    # Secondary 3-tuple index (county, street_num, sale_date) — no defendant.
    # Used to catch cross-source dupes where TNLedger has a defendant name
    # but a trustee scraper doesn't (both would otherwise pass the 4-tuple check).
    existing_keys_3: set[tuple] = {(c, s, d) for (c, s, d, _) in existing_keys}
    seen_this_batch: set[tuple] = set()
    seen_this_batch_3: set[tuple] = set()
    to_add:    list[list] = []
    to_review: list[list] = []
    counts = {"added": 0, "needs_review": 0, "skipped_too_soon": 0, "skipped_duplicate": 0}

    for listing in listings:
        street    = listing.get("Street", "").strip()
        sale_date = listing.get("Sale Date", "").strip()

        if not _extract_street_number(street):
            reason = f"No parseable street number in Street: '{street}'"
            print(f"  [NEEDS REVIEW - no street#] {street or '(blank)'}")
            to_review.append(_listing_to_review_row(listing, reason))
            counts["needs_review"] += 1
            continue

        if not _parse_sale_date(sale_date):
            reason = f"Missing or unparseable sale date: '{sale_date}'"
            print(f"  [NEEDS REVIEW - no date]    {street} — '{sale_date}'")
            to_review.append(_listing_to_review_row(listing, reason))
            counts["needs_review"] += 1
            continue

        if not _is_at_least_n_days_out(sale_date):
            print(f"  [SKIP - too soon]           {street} — {sale_date}")
            counts["skipped_too_soon"] += 1
            continue

        key = _make_dedup_key(listing)
        last_name = key[3]  # (county, street_num, sale_date, last_name)

        is_dup = key in existing_keys or key in seen_this_batch
        if not is_dup and last_name == "":
            # No defendant (trustee scraper) — secondary check ignoring defendant
            # catches properties that TNLedger already wrote with a defendant name.
            key_3 = key[:3]
            is_dup = key_3 in existing_keys_3 or key_3 in seen_this_batch_3

        if is_dup:
            print(f"  [SKIP - duplicate]          {street} — {sale_date}")
            counts["skipped_duplicate"] += 1
            continue

        seen_this_batch.add(key)
        if last_name == "":
            seen_this_batch_3.add(key[:3])
        to_add.append(_listing_to_row(listing))
        counts["added"] += 1
        print(f"  [ADD]                       {street} — {sale_date}")

    if not dry_run:
        if to_add:
            _append_rows(TAB_MAIN, to_add)
        if to_review:
            _append_rows(TAB_REVIEW, to_review)

    prefix = "[DRY RUN] " if dry_run else ""
    print(
        f"\n  {prefix}Added {counts['added']} | "
        f"Needs Review {counts['needs_review']} | "
        f"Too soon {counts['skipped_too_soon']} | "
        f"Duplicates {counts['skipped_duplicate']}"
    )
    return {**counts, "dry_run": dry_run}


def get_existing_case_numbers(county: str) -> dict[str, tuple[int, bool]]:
    rows   = _get_all_rows(TAB_MAIN)
    result: dict[str, tuple[int, bool]] = {}
    for i, row in enumerate(rows[1:], start=2):
        row_county = row[COL_COUNTY].strip() if len(row) > COL_COUNTY else ""
        if row_county.lower() != county.lower():
            continue
        case_num = row[COL_CASE_NUM].strip() if len(row) > COL_CASE_NUM else ""
        if case_num:
            already_cancelled = (
                row[COL_CANCELLED].strip().lower() == "yes"
                if len(row) > COL_CANCELLED else False
            )
            result[case_num] = (i, already_cancelled)
    return result

def get_existing_rows_by_street(county: str) -> dict[str, tuple[int, bool]]:
    """
    Returns {street_number: (row_index, already_cancelled)} for all rows
    in the Auctions tab matching the given county.
    Used for cancellation matching when no case number is available (e.g. Campbell KY).
    Street number is the leading digits of col G (Street), same logic as dedup key.
    """
    rows = _get_all_rows(TAB_MAIN)
    out: dict[str, tuple[int, bool]] = {}
    for i, row in enumerate(rows[1:], start=2):
        row_county = row[COL_COUNTY].strip() if len(row) > COL_COUNTY else ""
        if row_county.lower() != county.lower():
            continue
        street = row[COL_STREET].strip() if len(row) > COL_STREET else ""
        m = re.match(r"^(\d+)", street)
        if not m:
            continue
        street_num = m.group(1)
        already_cancelled = (
            row[COL_CANCELLED].strip().lower() == "yes"
            if len(row) > COL_CANCELLED else False
        )
        if street_num not in out:
            out[street_num] = (i, already_cancelled)
    return out

def update_cancellations(cancellation_rows: dict[int, str], dry_run: bool = False) -> int:
    if not cancellation_rows:
        return 0
    if dry_run:
        for row in cancellation_rows:
            print(f"  [DRY RUN] Row {row}: Cancelled → Yes")
        return len(cancellation_rows)

    data = [
        {"range": f"{TAB_MAIN}!P{row}", "values": [["Yes"]]}
        for row in cancellation_rows
    ]
    _get_service().spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()
    for row in cancellation_rows:
        print(f"  [CANCEL] Row {row} → Cancelled = Yes")
    return len(cancellation_rows)


def update_blank_fields(scraped_listings: list[dict], dry_run: bool = False) -> int:
    if not scraped_listings:
        return 0

    svc    = _get_service()
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{TAB_MAIN}!A2:S"
    ).execute()
    rows = result.get("values", [])
    if not rows:
        return 0

    _COL_IDX = {"J": 9, "K": 10, "L": 11}
    existing_index = {}
    for i, row in enumerate(rows):
        row_padded = row + [""] * (19 - len(row))
        county    = row_padded[COL_COUNTY].strip().lower()
        sale_date = row_padded[COL_SALE_DATE].strip()
        street    = row_padded[COL_STREET].strip()
        defendant = row_padded[COL_DEFENDANT].strip()
        street_num = _extract_street_number(street)
        def_last   = _extract_defendant_last_name(defendant)
        if not county or not street_num:
            continue
        key = (county, street_num, sale_date, def_last)
        existing_index[key] = {
            "row_number": i + 2,
            **{col: row_padded[idx] for col, idx in _COL_IDX.items()},
        }

    updates = []
    for listing in scraped_listings:
        key = (
            listing.get("County", "").strip().lower(),
            _extract_street_number(listing.get("Street", "").strip()),
            listing.get("Sale Date", "").strip(),
            _extract_defendant_last_name(listing.get("Defendant(s)", "").strip()),
        )
        if key not in existing_index:
            continue
        existing_row = existing_index[key]
        row_num      = existing_row["row_number"]
        for field, col_letter in FILLABLE_COLUMNS.items():
            current_val = existing_row.get(col_letter, "").strip()
            new_val     = str(listing.get(field, "")).strip()
            if not current_val and new_val:
                updates.append({
                    "range":  f"{TAB_MAIN}!{col_letter}{row_num}",
                    "values": [[new_val]],
                })

    if not updates:
        return 0
    if dry_run:
        print(f"  [DryRun] Would back-fill {len(updates)} blank field(s).")
        return len(updates)

    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": updates}
    ).execute()
    return len(updates)


# ---------------------------------------------------------------------------
# Public API — Valuation pipeline
# ---------------------------------------------------------------------------

def get_listings_needing_valuation(county_filter: list[str] | None = None) -> list[dict]:
    rows      = _get_all_rows(TAB_MAIN)
    today_str = date.today().isoformat()
    results: list[dict] = []

    for i, row in enumerate(rows[1:], start=2):
        if len(row) < 7:
            continue
        county    = row[COL_COUNTY].strip()    if len(row) > COL_COUNTY    else ""
        sale_date = row[COL_SALE_DATE].strip() if len(row) > COL_SALE_DATE else ""
        cancelled = row[COL_CANCELLED].strip() if len(row) > COL_CANCELLED else ""
        emv       = row[COL_EMV].strip()       if len(row) > COL_EMV       else ""

        if county_filter and county.lower() not in [c.lower() for c in county_filter]:
            continue
        state = row[1].strip() if len(row) > 1 else ""  # col B = State
        if state.upper() == "TN":
            import config as _cfg
            tn_whitelist = getattr(_cfg, "TN_VALUATE_COUNTIES", None)
            if tn_whitelist is not None:
                if county.lower() not in [c.lower() for c in tn_whitelist]:
                    continue
        if cancelled.lower() == "yes":
            continue
        if not sale_date or sale_date < today_str:
            continue
        if emv:
            continue

        results.append({
            "_row_index":             i,
            "County":                 county,
            "State":                  row[COL_STATE].strip()    if len(row) > COL_STATE    else "",
            "Sale Date":              sale_date,
            "Case Number":            row[COL_CASE_NUM].strip() if len(row) > COL_CASE_NUM else "",
            "Street":                 row[COL_STREET].strip()   if len(row) > COL_STREET   else "",
            "City":                   row[COL_CITY].strip()     if len(row) > COL_CITY     else "",
            "Zip":                    row[COL_ZIP].strip()      if len(row) > COL_ZIP      else "",
            "Judgment / Loan Amount": row[COL_JUDGMENT].strip() if len(row) > COL_JUDGMENT else "",
        })
    return results


def update_valuations(valuations: list[dict], dry_run: bool = False) -> int:
    updated = 0
    data    = []

    for v in valuations:
        row    = v["_row_index"]
        emv    = v.get("Estimated Market Value", "")
        equity = v.get("Estimated Equity", "")
        signal = v.get("Equity Signal", "")
        notes  = v.get("Notes", "")

        if dry_run:
            print(f"  [DRY RUN] Row {row}: EMV={emv}  Equity={equity}  Signal={signal}")
            updated += 1
            continue

        data.append({"range": f"{TAB_MAIN}!M{row}:O{row}", "values": [[emv, equity, signal]]})
        data.append({"range": f"{TAB_MAIN}!S{row}",         "values": [[notes]]})
        print(f"  [VALUATE] Row {row} — {signal} {emv}")
        updated += 1

    if not dry_run and data:
        _get_service().spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()
    return updated


# ---------------------------------------------------------------------------
# Public API — Skip trace pipeline (Phase 3)
# ---------------------------------------------------------------------------

def ensure_skiptrace_header() -> None:
    svc    = _get_service()
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{TAB_MAIN}!T1:Z1"
    ).execute()
    existing = result.get("values", [[]])[0] if result.get("values") else []
    expected = list(SKIPTRACE_COLUMNS.keys())
    if existing == expected:
        return
    svc.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{TAB_MAIN}!T1:Z1",
        valueInputOption="RAW",
        body={"values": [expected]},
    ).execute()
    print("  ✓ Skip trace column headers written to row 1 (T–Z).")


def get_listings_needing_skiptrace() -> list[dict]:
    today_str = date.today().isoformat()
    rows      = _get_all_rows_extended(TAB_MAIN)
    qualifying: list[dict] = []

    for i, row in enumerate(rows[1:], start=2):
        padded = row + [""] * (26 - len(row))
        equity_signal = padded[COL_SIGNAL].strip()
        cancelled     = padded[COL_CANCELLED].strip().lower()
        sale_date     = padded[COL_SALE_DATE].strip()
        st_date       = padded[COL_ST_DATE].strip()

        if equity_signal not in _SKIPTRACE_QUALIFYING_SIGNALS:
            continue
        if cancelled == "yes":
            continue
        if not sale_date or sale_date < today_str:
            continue
        if st_date:
            continue

        qualifying.append({
            "_row_index":    i,
            "County":        padded[COL_COUNTY].strip(),
            "State":         padded[COL_STATE].strip(),
            "Sale Date":     sale_date,
            "Case Number":   padded[COL_CASE_NUM].strip(),
            "Street":        padded[COL_STREET].strip(),
            "City":          padded[COL_CITY].strip(),
            "Zip":           padded[COL_ZIP].strip(),
            "Equity Signal": equity_signal,
        })
    return qualifying


def update_skiptraces(results: list[dict], dry_run: bool = False) -> int:
    if dry_run:
        count = sum(1 for r in results if not r.get("_skipped"))
        print(f"  [DRY RUN] Would update {count} row(s) with skip trace data.")
        return count

    data = []
    for result in results:
        if result.get("_skipped"):
            continue
        row_idx = result["_row_index"]
        for field, col_letter in SKIPTRACE_COLUMNS.items():
            data.append({
                "range":  f"{TAB_MAIN}!{col_letter}{row_idx}",
                "values": [[str(result.get(field, ""))]],
            })

    if not data:
        return 0

    _get_service().spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()
    return sum(1 for r in results if not r.get("_skipped"))


# ---------------------------------------------------------------------------
# Public API — Heir research pipeline (Phase 4)
# ---------------------------------------------------------------------------

def ensure_heir_research_headers() -> None:
    """
    Write heir research headers (AA–AE) to Auctions row 1 and create the
    Heir Leads tab if it doesn't exist. Safe to call on every run.
    """
    svc = _get_service()

    # Auctions AA:AE
    result   = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{TAB_MAIN}!AA1:AE1"
    ).execute()
    existing = result.get("values", [[]])[0] if result.get("values") else []
    expected = list(HEIR_RESEARCH_COLUMNS.keys())

    if existing != expected:
        svc.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_MAIN}!AA1:AE1",
            valueInputOption="RAW",
            body={"values": [expected]},
        ).execute()
        print("  ✓ Heir research column headers written to row 1 (AA–AE).")

    # Heir Leads tab
    if not _tab_exists(TAB_HEIR_LEADS):
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TAB_HEIR_LEADS}}}]},
        ).execute()
        svc.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{TAB_HEIR_LEADS}'!A1",
            valueInputOption="RAW",
            body={"values": [HEIR_LEADS_HEADER]},
        ).execute()
        print(f"  ✓ '{TAB_HEIR_LEADS}' tab created with header.")


def get_listings_needing_heir_research() -> list[dict]:
    """
    Return Auctions rows where:
      - Deceased (col Y) = "Yes"
      - Heir Research Date (col AE) is blank
      - Sale Date >= today (no point researching past auctions)
    """
    today_str = date.today().isoformat()
    rows      = _get_all_rows_full(TAB_MAIN)  # A:AE
    qualifying: list[dict] = []

    for i, row in enumerate(rows[1:], start=2):
        padded = row + [""] * (31 - len(row))

        deceased  = padded[COL_DECEASED].strip()
        hr_date   = padded[COL_HR_DATE].strip()
        sale_date = padded[COL_SALE_DATE].strip()

        if deceased.lower() != "yes":
            continue
        if hr_date:
            continue
        if not sale_date or sale_date < today_str:
            continue

        qualifying.append({
            "_row_index":           i,
            "County":               padded[COL_COUNTY].strip(),
            "State":                padded[COL_STATE].strip(),
            "Sale Date":            sale_date,
            "Case Number":          padded[COL_CASE_NUM].strip(),
            "Street":               padded[COL_STREET].strip(),
            "City":                 padded[COL_CITY].strip(),
            "Zip":                  padded[COL_ZIP].strip(),
            "Equity Signal":        padded[COL_SIGNAL].strip(),
            "Defendant(s)":         padded[COL_DEFENDANT].strip(),
            "Owner Name (Primary)": padded[COL_OWNER_PRIMARY].strip(),
        })
    return qualifying


def update_heir_research(results: list[dict], dry_run: bool = False) -> int:
    """
    Write heir research data to Auctions tab columns AA–AE.
    Writes all non-skipped results (including no-hit) so they won't be re-run.
    """
    if dry_run:
        count = sum(1 for r in results if not r.get("_skipped"))
        print(f"  [DRY RUN] Would update {count} row(s) with heir research data.")
        return count

    data = []
    for result in results:
        if result.get("_skipped"):
            continue
        row_idx = result["_row_index"]
        for field, col_letter in HEIR_RESEARCH_COLUMNS.items():
            data.append({
                "range":  f"{TAB_MAIN}!{col_letter}{row_idx}",
                "values": [[str(result.get(field, ""))]],
            })

    if not data:
        return 0

    _get_service().spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()
    return sum(1 for r in results if not r.get("_skipped"))


def write_heir_leads(results: list[dict], dry_run: bool = False) -> int:
    """
    Write one row per heir to the Heir Leads tab for results where
    obit_found = "Yes" and at least one heir was extracted.

    Deduplicates by (street_number, sale_date, heir_name_lower).
    Phone/email/mailing columns left blank — filled by Phase 4b
    heir skip trace (--heirskiptrace).

    Returns count of new rows written.
    """
    # Build existing key set for dedup
    existing_keys: set[tuple] = set()
    if _tab_exists(TAB_HEIR_LEADS):
        result = _get_service().spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{TAB_HEIR_LEADS}'!A:O",
        ).execute()
        for row in result.get("values", [])[1:]:
            if len(row) >= 8:
                street_num = _extract_street_number(row[HL_COL_STREET])
                sale_date  = row[HL_COL_SALE_DATE] if len(row) > HL_COL_SALE_DATE else ""
                heir_name  = row[HL_COL_HEIR_NAME].strip().lower() if len(row) > HL_COL_HEIR_NAME else ""
                if street_num and heir_name:
                    existing_keys.add((street_num, sale_date, heir_name))

    to_add: list[list] = []

    for result in results:
        if result.get("_skipped"):
            continue
        if result.get("Obit Found") != "Yes":
            continue
        heirs = result.get("_heirs_list", [])
        if not heirs:
            continue

        street     = result.get("Street", "")
        city       = result.get("City", "")
        county     = result.get("County", "")
        state      = result.get("State", "")
        sale_date  = result.get("Sale Date", "")
        signal     = result.get("Equity Signal", "")
        owner_name = result.get("Owner Name (Primary)", "")
        def_match  = result.get("Defendant Match", "No")
        street_num = _extract_street_number(street)

        for heir in heirs:
            heir_name = heir.get("name", "").strip()
            heir_rel  = heir.get("relationship", "").strip()
            if not heir_name:
                continue

            dedup_key = (street_num, sale_date, heir_name.lower())
            if dedup_key in existing_keys:
                continue

            # Per-heir defendant match flag
            heir_match = "Yes" if heir_name.lower() in def_match.lower() else "No"

            to_add.append([
                street,      # A Property Street
                city,        # B Property City
                county,      # C County
                state,       # D State
                sale_date,   # E Sale Date
                signal,      # F Equity Signal
                owner_name,  # G Deceased Owner
                heir_name,   # H Heir Name
                heir_rel,    # I Relationship
                heir_match,  # J Defendant Match
                "",          # K Phone(s)        — Phase 4b
                "",          # L Email(s)         — Phase 4b
                "",          # M Mailing Address  — Phase 4b
                "",          # N Skip Traced Date — Phase 4b
                "New",       # O Status
            ])
            existing_keys.add(dedup_key)

    if not to_add:
        return 0

    if dry_run:
        print(f"  [DRY RUN] Would add {len(to_add)} row(s) to '{TAB_HEIR_LEADS}'.")
        return len(to_add)

    _append_rows(TAB_HEIR_LEADS, to_add)
    return len(to_add)


# ---------------------------------------------------------------------------
# Public API — Heir skip trace pipeline (Phase 4b)
# ---------------------------------------------------------------------------

def get_heirs_needing_skiptrace() -> list[dict]:
    """
    Read the Heir Leads tab and return every row where Skip Traced Date
    (col N) is blank.

    Skips the header row. Rows with a blank Heir Name (col H) are still
    returned — run_heir_skiptrace() writes today's date to those rows as
    a no-op so they don't recur.

    Returns a list of dicts, each with:
        row_index  (int)  1-based sheet row number
        heir_name  (str)  col H — full name of the heir (may be blank)
        street     (str)  col A — property street address
        city       (str)  col B — property city
        state      (str)  col D — state abbreviation
    """
    svc = _get_service()
    result = (
        svc.spreadsheets()
        .values()
        .get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{TAB_HEIR_LEADS}'!A:O",
        )
        .execute()
    )

    rows = result.get("values", [])
    if not rows:
        return []

    heirs = []
    for i, row in enumerate(rows):
        if i == 0:
            continue  # skip header row

        # Pad to 14 columns so index 13 (col N) is always safe
        row = row + [""] * (14 - len(row))

        skip_traced_date = row[HL_COL_ST_DATE].strip()  # col N
        if skip_traced_date:
            continue  # already processed

        heirs.append(
            {
                "row_index": i + 1,                          # 1-based for Sheets API
                "heir_name": row[HL_COL_HEIR_NAME].strip(),  # col H
                "street":    row[HL_COL_STREET].strip(),     # col A
                "city":      row[HL_COL_CITY].strip(),       # col B
                "state":     row[HL_COL_STATE].strip(),      # col D
            }
        )

    return heirs


def update_heir_skiptraces(results: list[dict], dry_run: bool = False) -> int:
    """
    Write Tracerfy skip trace results back to Heir Leads tab cols K–N.

    Uses a single batchUpdate call for all rows to stay well within the
    Google Sheets 300 requests/minute quota.

    Each result dict must have:
        row_index  (int)  1-based sheet row
        phones     (str)  formatted phone string (may be "")
        emails     (str)  formatted email string (may be "")
        mailing    (str)  formatted mailing address (may be "")
        date       (str)  YYYY-MM-DD — written even on no-hit to prevent re-run

    Returns number of rows updated (or that would be updated on dry run).
    """
    if not results:
        return 0

    if dry_run:
        for r in results:
            phones_preview  = r["phones"][:35]  if r["phones"]  else "(none)"
            mailing_preview = r["mailing"][:40] if r["mailing"] else "(none)"
            print(
                f"  [dry-run] Row {r['row_index']:>4}: "
                f"phones={phones_preview!r}  mailing={mailing_preview!r}"
            )
        return len(results)

    svc = _get_service()
    data = []
    for r in results:
        row_n = r["row_index"]
        data.append(
            {
                "range":  f"'{TAB_HEIR_LEADS}'!K{row_n}:N{row_n}",
                "values": [[r["phones"], r["emails"], r["mailing"], r["date"]]],
            }
        )

    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()

    return len(results)

def dedup_heir_phones(dry_run: bool = False) -> int:
    """
    After heir skip trace, ensure each phone number appears in only one
    Heir Leads row per property address.

    Groups rows by (street, city). Within each group, iterates in sheet row
    order. For each skip-traced row with phones in col K:

      - Splits the phone string by ", " into individual numbers
      - Separates them into already-seen (dup) and not-yet-seen (unique)
      - If unique numbers remain: updates col K to unique-only
      - If no unique numbers remain: clears col K and sets Status (col O)
        to "Dup Phone" so the Prop.ai push filter skips this row

    Rows with blank phones, blank Skip Traced Date (col N), or already
    marked "Dup Phone" are skipped entirely.

    Returns count of rows modified.
    """
    from collections import defaultdict

    svc = _get_service()
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{TAB_HEIR_LEADS}'!A:O",
    ).execute()

    rows = result.get("values", [])
    if len(rows) <= 1:
        return 0

    # ── Build property groups ─────────────────────────────────────────────────
    # Key: (street.lower(), city.lower()) — groups all heirs for a property.
    # We preserve sheet row order within each group so the first heir's phones
    # are always treated as the canonical set for that number.
    property_groups: dict[tuple, list[dict]] = defaultdict(list)

    for i, row in enumerate(rows):
        if i == 0:
            continue  # header
        row = row + [""] * (15 - len(row))

        st_date = row[HL_COL_ST_DATE].strip()   # col N
        status  = row[HL_COL_STATUS].strip()     # col O

        if not st_date:
            continue  # skip trace hasn't run yet for this row
        if status == "Dup Phone":
            continue  # already handled in a prior run

        group_key = (row[HL_COL_STREET].strip().lower(), row[HL_COL_CITY].strip().lower())
        property_groups[group_key].append({
            "row_index": i + 1,                        # 1-based
            "phones":    row[HL_COL_PHONES].strip(),   # col K
        })

    # ── Deduplicate within each property ─────────────────────────────────────
    updates: list[dict] = []
    total_modified = 0

    for group_rows in property_groups.values():
        seen_phones: set[str] = set()

        for row_info in group_rows:
            row_n      = row_info["row_index"]
            phones_str = row_info["phones"]

            if not phones_str:
                continue  # no phones to process

            phone_list = [p.strip() for p in phones_str.split(",") if p.strip()]
            unique     = [p for p in phone_list if p not in seen_phones]
            dups       = [p for p in phone_list if p in seen_phones]

            if not dups:
                # All new — add to seen set and move on, no write needed
                seen_phones.update(phone_list)
                continue

            # At least one duplicate found
            seen_phones.update(unique)

            if unique:
                # Partial dup — keep only the unique phones
                new_phones_str = ", ".join(unique)
                if dry_run:
                    print(
                        f"  [dry-run] Row {row_n}: removed dup phone(s) {dups} "
                        f"→ col K updated to {new_phones_str!r}"
                    )
                else:
                    updates.append({
                        "range":  f"'{TAB_HEIR_LEADS}'!K{row_n}",
                        "values": [[new_phones_str]],
                    })
                total_modified += 1

            else:
                # All phones already seen in an earlier row — clear and mark
                if dry_run:
                    print(
                        f"  [dry-run] Row {row_n}: all phones already covered "
                        f"→ col K cleared, Status → 'Dup Phone'"
                    )
                else:
                    updates.append({
                        "range":  f"'{TAB_HEIR_LEADS}'!K{row_n}",
                        "values": [[""]],
                    })
                    updates.append({
                        "range":  f"'{TAB_HEIR_LEADS}'!O{row_n}",
                        "values": [["Dup Phone"]],
                    })
                total_modified += 1

    # ── Write all updates in one batch ────────────────────────────────────────
    if updates and not dry_run:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": updates},
        ).execute()

    return total_modified


# ---------------------------------------------------------------------------
# TN active rows — for dedup and for check mode
# ---------------------------------------------------------------------------

def get_tn_existing_set() -> set[tuple]:
    """
    Return a set of (county_lower, street_number, sale_date) tuples for all
    active (not Cancelled) TN rows in the Auctions tab.

    Used by scrape_rubin_lublin() to prevent duplicating rows already written
    by TNLedger (which uses a different case-number namespace).
    """
    svc = _get_service()
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Auctions!A:S",
    ).execute()
    rows = result.get("values", [])
    if not rows:
        return set()

    addr_set: set[tuple] = set()
    for i, row in enumerate(rows[1:], start=2):  # skip header, row_index is 1-based
        # Pad to at least 19 columns
        row = row + [""] * (19 - len(row))
        state = row[1].strip()
        sale_date = row[2].strip()
        county = row[0].strip()
        street = row[6].strip()
        cancelled = row[15].strip().lower()

        if state.upper() != "TN":
            continue
        if cancelled == "yes":
            continue

        street_num = re.match(r"^(\d+)", street)
        if street_num and sale_date:
            addr_set.add((county.lower(), street_num.group(1), sale_date))

    return addr_set


def get_tn_listings_for_check() -> list[dict]:
    """
    Return all active (non-cancelled, Sale Date ≥ today) TN rows from the
    Auctions tab.  Each dict includes row_index and all fields needed for
    trustee site cross-checking.
    """
    svc = _get_service()
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Auctions!A:S",
    ).execute()
    rows = result.get("values", [])
    if not rows:
        return []

    today = date.today()
    listings = []
    for i, row in enumerate(rows[1:], start=2):
        row = row + [""] * (19 - len(row))
        state = row[1].strip()
        sale_date = row[2].strip()
        cancelled = row[15].strip().lower()

        if state.upper() != "TN":
            continue
        if cancelled == "yes":
            continue
        try:
            if datetime.strptime(sale_date, "%Y-%m-%d").date() < today:
                continue
        except ValueError:
            continue

        listings.append({
            "row_index": i,
            "County": row[0].strip(),
            "State": row[1].strip(),
            "Sale Date": row[2].strip(),
            "Case Number": row[3].strip(),
            "Plaintiff": row[4].strip(),
            "Defendant(s)": row[5].strip(),
            "Street": row[6].strip(),
            "City": row[7].strip(),
            "Zip": row[8].strip(),
            "Attorney / Firm": row[11].strip(),
            "Notes": row[18].strip(),
        })

    return listings


# ---------------------------------------------------------------------------
# Write postponements
# ---------------------------------------------------------------------------

def update_tn_postponements(updates: list[dict], dry_run: bool = False) -> int:
    """
    Write postponement updates to the Auctions sheet.

    Each update dict: {row_index, old_date, new_date}
      - Updates col C (Sale Date) to new_date
      - Appends a postponement note to col S (Notes)

    Returns number of rows updated.
    """
    if not updates:
        return 0

    today_str = date.today().strftime("%Y-%m-%d")

    # We need the current Notes content to append rather than overwrite.
    # Fetch current notes for the affected rows in one batch.
    svc = _get_service()
    row_indices = [u["row_index"] for u in updates]

    # Build a mapping of row_index → current notes string
    current_notes: dict[int, str] = {}
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Auctions!S:S",
    ).execute()
    notes_col = result.get("values", [])
    for idx in row_indices:
        # idx is 1-based sheet row; notes_col[0] is header (row 1)
        notes_row = idx - 1  # convert to 0-based list index
        if notes_row < len(notes_col):
            current_notes[idx] = (notes_col[notes_row] or [""])[0]
        else:
            current_notes[idx] = ""

    if dry_run:
        for u in updates:
            print(
                f"  [DRY RUN] Row {u['row_index']}: "
                f"Sale Date {u['old_date']} → {u['new_date']} (postponed)"
            )
        return len(updates)

    batch_data = []
    for u in updates:
        idx = u["row_index"]
        old_date = u["old_date"]
        new_date = u["new_date"]
        note = f"Postponed from {old_date} to {new_date} — checked {today_str}"
        existing = current_notes.get(idx, "")
        new_notes = f"{existing} | {note}".lstrip(" |") if existing else note

        batch_data.append({
            "range": f"Auctions!C{idx}",
            "values": [[new_date]],
        })
        batch_data.append({
            "range": f"Auctions!S{idx}",
            "values": [[new_notes]],
        })

    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": batch_data},
    ).execute()

    return len(updates)


# ---------------------------------------------------------------------------
# Flag rows for manual check
# ---------------------------------------------------------------------------

def flag_tn_for_manual_check(flags: list[dict], dry_run: bool = False) -> int:
    """
    Append a warning note to col S (Notes) for rows that need manual checking.

    Each flag dict: {row_index, reason}

    Returns number of rows flagged.
    """
    if not flags:
        return 0

    today_str = date.today().strftime("%Y-%m-%d")
    svc = _get_service()

    # Fetch current notes for affected rows
    row_indices = [f["row_index"] for f in flags]
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Auctions!S:S",
    ).execute()
    notes_col = result.get("values", [])

    current_notes: dict[int, str] = {}
    for idx in row_indices:
        notes_row = idx - 1
        if notes_row < len(notes_col):
            current_notes[idx] = (notes_col[notes_row] or [""])[0]
        else:
            current_notes[idx] = ""

    if dry_run:
        for f in flags:
            print(
                f"  [DRY RUN] Row {f['row_index']}: "
                f"⚠️ Manual check — {f['reason']}"
            )
        return len(flags)

    batch_data = []
    for f in flags:
        idx = f["row_index"]
        note = f"⚠️ Manual check ({today_str}): {f['reason']}"
        existing = current_notes.get(idx, "")
        new_notes = f"{existing} | {note}".lstrip(" |") if existing else note

        batch_data.append({
            "range": f"Auctions!S{idx}",
            "values": [[new_notes]],
        })

    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": batch_data},
    ).execute()

    return len(flags)


# ---------------------------------------------------------------------------
# Trustee Registry tab
# ---------------------------------------------------------------------------

_TRUSTEE_REGISTRY_TAB = "Trustee Registry"
_TRUSTEE_REGISTRY_HEADERS = [
    "Canonical Name", "Known Aliases", "Site URL",
    "Scraper Status", "Notes", "Date Added",
]


def ensure_trustee_registry_tab() -> None:
    """
    Create the 'Trustee Registry' sheet tab if it doesn't exist,
    and write headers on first creation.
    """
    svc = _get_service()
    meta = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    existing_titles = {s["properties"]["title"] for s in meta.get("sheets", [])}

    if _TRUSTEE_REGISTRY_TAB not in existing_titles:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": _TRUSTEE_REGISTRY_TAB}}}]},
        ).execute()
        # Write headers
        svc.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{_TRUSTEE_REGISTRY_TAB}!A1",
            valueInputOption="RAW",
            body={"values": [_TRUSTEE_REGISTRY_HEADERS]},
        ).execute()


def write_unknown_trustees(
        unknown_dict: dict[str, str],
        dry_run: bool = False,
) -> int:
    """
    Append rows to the Trustee Registry tab for firms not yet in the registry.
    unknown_dict: {firm_name: source_url}

    Deduplicates against existing tab rows (by canonical name, case-insensitive).
    Returns number of new rows written.
    """
    if not unknown_dict:
        return 0

    svc = _get_service()
    ensure_trustee_registry_tab()

    # Read existing names to dedup
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{_TRUSTEE_REGISTRY_TAB}!A:A",
    ).execute()
    existing_names = {
        r[0].lower() for r in result.get("values", []) if r
    }

    today_str = date.today().strftime("%Y-%m-%d")
    new_rows = []
    for name, source_url in unknown_dict.items():
        if name.lower() in existing_names:
            continue
        new_rows.append([
            name,  # Canonical Name (raw — manual cleanup expected)
            "",  # Known Aliases
            source_url,  # Site URL (source of discovery)
            "Needs Research",
            "",
            today_str,
        ])

    if not new_rows:
        return 0

    if dry_run:
        for row in new_rows:
            print(f"  [DRY RUN] Trustee Registry: would add '{row[0]}'")
        return len(new_rows)

    svc.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{_TRUSTEE_REGISTRY_TAB}!A:F",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": new_rows},
    ).execute()

    return len(new_rows)