#!/usr/bin/env python3
"""
migrate_address_columns.py

One-time migration: splits the "Property Address" column (col G) in the
Auctions tab into three separate columns:
    G → Street
    H → City  (new)
    I → Zip   (new)

Everything from col H onward shifts two columns right.

Usage:
    python migrate_address_columns.py --dry-run   # inspect results first
    python migrate_address_columns.py             # apply changes

Always make a copy of your spreadsheet before running without --dry-run.
"""

import argparse
import json
import os
import sys
import time

import anthropic
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ── Config ─────────────────────────────────────────────────────────────────────

SPREADSHEET_ID: str = ""   # loaded from config.py at runtime
ANTHROPIC_API_KEY: str = ""  # loaded from config.py at runtime
AUCTIONS_TAB   = "Auctions"
BATCH_SIZE     = 20        # addresses per Claude call
MODEL          = "claude-haiku-4-5-20251001"
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets"]

# Column indices, 0-based, in the CURRENT (pre-migration) layout
COL_PROPERTY_ADDRESS = 6   # G

# ── Google Sheets auth ──────────────────────────────────────────────────────────

def _get_sheets_service():
    creds = None
    token_path = "sheets_token.json"
    creds_path = "credentials.json"

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as fh:
            fh.write(creds.to_json())
    return build("sheets", "v4", credentials=creds)


def _get_sheet_id(service, tab_name: str) -> int:
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    for sheet in meta["sheets"]:
        if sheet["properties"]["title"] == tab_name:
            return sheet["properties"]["sheetId"]
    raise ValueError(f"Tab '{tab_name}' not found in spreadsheet")


# ── Claude address parser ───────────────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a US property address parser. Given a JSON array of address strings, return a JSON array of the same length where each element has exactly these three keys:

  "street" — house number + street name only (no city, state, or zip)
  "city"   — city or municipality name only
  "zip"    — 5-digit zip code as a string, or "" if genuinely absent

Rules:
- Return ONLY the raw JSON array. No markdown, no fences, no explanation.
- The output array must be the same length as the input array.
- Never guess a value that isn't present in the input. Use "" for missing fields.
- State abbreviations (KY, TN, OH, IN) and full state names (Tennessee, Kentucky, etc.) are NOT returned — omit them from all output fields.
- If the zip code appears embedded in the street portion before a comma (Jefferson KY format like "123 MAIN ST 40202, Louisville, KY"), extract it correctly.
- If there is no comma separating street from city, infer the boundary from the street suffix (Dr, St, Ave, Rd, Ct, Ln, Way, Blvd, Pike, Cir, Pl, Court, Drive, Street, Avenue, Road, Lane, Boulevard, Circle, Place, etc.).
- If the zip appears BEFORE the state ("CINCINNATI, 45239, OH" format), still extract it correctly as zip.
- Strip leading/trailing whitespace from all fields. Strip non-breaking spaces (\\xa0) from street.

Examples (cover all known formats in this dataset):

Standard KY/TN format — Street, City, ST Zip:
Input:  ["77 McMillan Drive, Independence, KY 41051",
         "7775 Cedar Wood Circle, Florence, KY  41042",
         "204 McCall Rd, Maryville, TN 37804"]
Output: [{"street":"77 McMillan Drive","city":"Independence","zip":"41051"},
         {"street":"7775 Cedar Wood Circle","city":"Florence","zip":"41042"},
         {"street":"204 McCall Rd","city":"Maryville","zip":"37804"}]

Full state name spelled out:
Input:  ["755 Bazel Road, Harriman, Tennessee 37748",
         "1352 Cook Road, Crossville, Tennessee 38555"]
Output: [{"street":"755 Bazel Road","city":"Harriman","zip":"37748"},
         {"street":"1352 Cook Road","city":"Crossville","zip":"38555"}]

Hamilton OH format — Street, City, Zip, State (zip BEFORE state):
Input:  ["1940 SUNDALE AVE, CINCINNATI, 45239, OH",
         "412 BRADLEY AVE, CINCINNATI , 45215",
         "843 FINDLAY ST, CINCINNATI, 45214, OH"]
Output: [{"street":"1940 SUNDALE AVE","city":"CINCINNATI","zip":"45239"},
         {"street":"412 BRADLEY AVE","city":"CINCINNATI","zip":"45215"},
         {"street":"843 FINDLAY ST","city":"CINCINNATI","zip":"45214"}]

Jefferson KY format — zip embedded before city (Street+Zip, City, State):
Input:  ["123 MAIN ST 40202, Louisville, KY",
         "456 OAK AVE 40203, Louisville, KY"]
Output: [{"street":"123 MAIN ST","city":"Louisville","zip":"40202"},
         {"street":"456 OAK AVE","city":"Louisville","zip":"40203"}]

Fayette KY — no zip in source:
Input:  ["520 BROOK FARM COURT, Lexington, KY"]
Output: [{"street":"520 BROOK FARM COURT","city":"Lexington","zip":""}]

Jessamine KY — no comma between street and city:
Input:  ["438 MAIN ST NICHOLASVILLE KY 40356",
         "109 Willow Court Wilmore KY 40390"]
Output: [{"street":"438 MAIN ST","city":"NICHOLASVILLE","zip":"40356"},
         {"street":"109 Willow Court","city":"Wilmore","zip":"40390"}]

Franklin KY — street only, city from geocoder (already appended):
Input:  ["109 Willowcrest Drive, Frankfort"]
Output: [{"street":"109 Willowcrest Drive","city":"Frankfort","zip":""}]

Clark IN — street and city, no zip, no state:
Input:  ["150 PIKE ST, Sellersburg, IN",
         "225 W. UTICA ST., SELLERSBURG, IN"]
Output: [{"street":"150 PIKE ST","city":"Sellersburg","zip":""},
         {"street":"225 W. UTICA ST.","city":"SELLERSBURG","zip":""}]

Floyd IN — Street, City, State, Zip (separate cells, now combined):
Input:  ["3456 WASHINGTON AVE, NEW ALBANY, IN 47150"]
Output: [{"street":"3456 WASHINGTON AVE","city":"NEW ALBANY","zip":"47150"}]
"""

def _parse_batch(addresses: list[str]) -> list[dict]:
    """Send one batch to Claude Haiku; return list of {street, city, zip} dicts."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    response = client.messages.create(
        model=MODEL,
        max_tokens=2000,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": json.dumps(addresses)}],
    )
    raw = response.content[0].text.strip()
    # Strip any accidental markdown fences
    raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    return json.loads(raw)


def _parse_all_addresses(addresses: list[str]) -> list[dict]:
    """Parse all addresses in BATCH_SIZE chunks; return aligned list of dicts."""
    results = []
    n_batches = (len(addresses) + BATCH_SIZE - 1) // BATCH_SIZE

    for b in range(n_batches):
        batch = addresses[b * BATCH_SIZE : (b + 1) * BATCH_SIZE]
        print(f"  Batch {b+1}/{n_batches} ({len(batch)} addresses) ... ", end="", flush=True)
        try:
            parsed = _parse_batch(batch)
            # Safety: if Claude returned fewer items than sent, pad with fallbacks
            while len(parsed) < len(batch):
                idx = len(parsed)
                parsed.append({"street": batch[idx], "city": "", "zip": ""})
            results.extend(parsed[:len(batch)])   # trim if somehow returned too many
            print("OK")
        except Exception as exc:
            print(f"ERROR: {exc}")
            # Pad with fallbacks so row alignment stays intact
            for addr in batch:
                results.append({"street": addr, "city": "", "zip": ""})

        if b < n_batches - 1:
            time.sleep(0.5)   # brief pause between batches

    return results


# ── Sheet mutation helpers ──────────────────────────────────────────────────────

def _read_all_rows(service) -> list[list]:
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{AUCTIONS_TAB}!A:T",   # wide enough for post-migration layout too
    ).execute()
    return result.get("values", [])


def _insert_two_columns_after_G(service, sheet_id: int):
    """
    Insert 2 blank columns at 0-based positions 7 and 8 (current H, I).
    This pushes all existing columns from H onward two places to the right.
    Column G (Property Address) stays in place; we'll overwrite it with Street.
    """
    body = {"requests": [{
        "insertDimension": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "startIndex": 7,   # insert before 0-based index 7 → new cols land at H(7) and I(8)
                "endIndex": 9,
            },
            "inheritFromBefore": True,
        }
    }]}
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body=body
    ).execute()


def _write_street_city_zip(
    service,
    updates: list[tuple[int, str, str, str]],
):
    """
    updates: list of (row_1based, street, city, zip)
    Writes Street → col G, City → col H, Zip → col I for each row.
    Uses a single batchUpdate call.
    """
    if not updates:
        return
    data = [
        {
            "range": f"{AUCTIONS_TAB}!G{row}:I{row}",
            "values": [[street, city, zip_code]],
        }
        for (row, street, city, zip_code) in updates
    ]
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


def _update_header(service):
    """Rewrite row 1 with the full post-migration header."""
    new_header = [
        "County", "State", "Sale Date", "Case Number", "Plaintiff", "Defendant(s)",
        "Street", "City", "Zip",
        "Appraised Value", "Judgment / Loan Amount", "Attorney / Firm",
        "Estimated Market Value", "Estimated Equity", "Equity Signal",
        "Cancelled", "Source URL", "Date Added", "Notes",
    ]
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{AUCTIONS_TAB}!A1",
        valueInputOption="RAW",
        body={"values": [new_header]},
    ).execute()


# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    global SPREADSHEET_ID, ANTHROPIC_API_KEY

    ap = argparse.ArgumentParser(
        description="Migrate Property Address → Street / City / Zip"
    )
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse and print results without writing to the sheet")
    args = ap.parse_args()

    # Load settings from config.py (must be run from project root)
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import config
    SPREADSHEET_ID    = config.SPREADSHEET_ID
    ANTHROPIC_API_KEY = config.ANTHROPIC_API_KEY

    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"\n{prefix}Eagle Creek — Address Column Migration")
    print("=" * 60)

    service      = _get_sheets_service()
    sheet_id     = _get_sheet_id(service, AUCTIONS_TAB)

    # ── Step 1: Read current data ──────────────────────────────────────────────
    print("\n[1/4] Reading Auctions tab ...")
    rows = _read_all_rows(service)
    if not rows:
        print("  No data found. Exiting.")
        return

    header    = rows[0]
    data_rows = rows[1:]
    print(f"  Header: {header[:10]} ...")
    print(f"  {len(data_rows)} data rows.")

    # Sanity-check: make sure col G still says "Property Address"
    g_header = header[COL_PROPERTY_ADDRESS] if len(header) > COL_PROPERTY_ADDRESS else ""
    if g_header == "Street":
        print("\n  ⚠️  Column G already says 'Street' — migration may have already run.")
        confirm = input("  Continue anyway? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("  Aborted.")
            return
    elif g_header != "Property Address":
        print(f"\n  ⚠️  Column G header is '{g_header}', expected 'Property Address'.")
        confirm = input("  Continue anyway? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("  Aborted.")
            return

    # Collect raw addresses aligned to 1-based row numbers
    raw_addresses: list[str] = []
    row_numbers:   list[int] = []
    for i, row in enumerate(data_rows, start=2):   # row 1 = header
        addr = row[COL_PROPERTY_ADDRESS].strip() if len(row) > COL_PROPERTY_ADDRESS else ""
        raw_addresses.append(addr)
        row_numbers.append(i)

    # ── Step 2: Parse with Claude ──────────────────────────────────────────────
    print(f"\n[2/4] Parsing {len(raw_addresses)} addresses with Claude Haiku ...")
    parsed_list = _parse_all_addresses(raw_addresses)

    # Build the update tuples and audit for blanks
    updates: list[tuple[int, str, str, str]] = []
    blank_zip_rows:  list[tuple[int, str]] = []
    blank_city_rows: list[tuple[int, str]] = []

    for row_num, raw_addr, parsed in zip(row_numbers, raw_addresses, parsed_list):
        street   = parsed.get("street", raw_addr).strip()
        city     = parsed.get("city",   "").strip()
        zip_code = parsed.get("zip",    "").strip()
        updates.append((row_num, street, city, zip_code))
        if not zip_code:
            blank_zip_rows.append((row_num, raw_addr))
        if not city:
            blank_city_rows.append((row_num, raw_addr))

    # ── Step 3: Report ─────────────────────────────────────────────────────────
    print(f"\n[3/4] Parse results:")
    print(f"  Total rows:       {len(updates)}")
    print(f"  Blank zip:        {len(blank_zip_rows)}")
    print(f"  Blank city:       {len(blank_city_rows)}")

    if blank_zip_rows:
        print(f"\n  Rows with blank zip (showing up to 20):")
        for rn, addr in blank_zip_rows[:20]:
            print(f"    Row {rn:4d}: {addr!r}")
        if len(blank_zip_rows) > 20:
            print(f"    ... and {len(blank_zip_rows) - 20} more")

    if blank_city_rows:
        print(f"\n  Rows with blank city (showing up to 10):")
        for rn, addr in blank_city_rows[:10]:
            print(f"    Row {rn:4d}: {addr!r}")

    print(f"\n  Sample of parsed results (first 15 rows):")
    print(f"  {'Row':>4}  {'Street':<40}  {'City':<20}  {'Zip':<6}  (original)")
    print(f"  {'-'*4}  {'-'*40}  {'-'*20}  {'-'*6}  {'-'*35}")
    for row_num, street, city, zip_code in updates[:15]:
        original = raw_addresses[row_numbers.index(row_num)]
        print(f"  {row_num:>4}  {street:<40}  {city:<20}  {zip_code:<6}  {original!r}")
    if len(updates) > 15:
        print(f"  ... and {len(updates) - 15} more rows")

    # ── Step 4: Write (or dry-run exit) ────────────────────────────────────────
    if args.dry_run:
        print("\n[4/4] Dry run — no changes written.")
        print("      Inspect the results above, then re-run without --dry-run to apply.")
        return

    print(f"\n[4/4] About to write to '{AUCTIONS_TAB}' tab.")
    print(f"  Operations:")
    print(f"    • Insert 2 blank columns after G (shifts H→J, I→K, etc.)")
    print(f"    • Write Street / City / Zip into G / H / I for {len(updates)} rows")
    print(f"    • Rewrite the header row")
    print(f"\n  ⚠️  Make a copy of your spreadsheet before proceeding.")
    print(f"      This is NOT easily reversible.")
    confirm = input("\n  Type 'apply' to proceed, anything else to abort: ").strip().lower()
    if confirm != "apply":
        print("  Aborted.")
        return

    print("\n  Inserting 2 columns after G ...", end=" ", flush=True)
    _insert_two_columns_after_G(service, sheet_id)
    print("done.")

    time.sleep(1.0)   # let the structural change settle before value writes

    print(f"  Writing Street / City / Zip for {len(updates)} rows ...", end=" ", flush=True)
    _write_street_city_zip(service, updates)
    print("done.")

    print("  Updating header row ...", end=" ", flush=True)
    _update_header(service)
    print("done.")

    print(f"\n✅ Migration complete.")
    if blank_zip_rows:
        print(f"   ⚠️  {len(blank_zip_rows)} rows have blank Zip (col I).")
        print(f"      Expected for: Fayette KY, Franklin KY, Clark IN, Madison KY,")
        print(f"      and any row where the source never included a zip.")
        print(f"      The refactored scrapers will back-fill zip via Nominatim on next run.")
    if blank_city_rows:
        print(f"   ⚠️  {len(blank_city_rows)} rows have blank City (col H) — review manually.")


if __name__ == "__main__":
    main()