"""
backfill_internetpostings_attorney.py
One-time (re-runnable) script to back-fill the Attorney / Firm column (col L)
for Auctions rows that were written from internetpostings.com with a blank firm
because PDF extraction failed when no session cookies were passed.

Safe to re-run — only touches rows where:
  State = TN
  Source URL = https://www.internetpostings.com
  Attorney / Firm is blank
  Not Cancelled
"""

import base64
import logging
import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from scrapers.tn_trustees.internetpostings import (
    _tos_flow,
    _extract_pdf_text,
    _extract_trustee_name,
    _street_number,
    _addresses_match,
    _parse_date,
    SOURCE,
)
from config import SPREADSHEET_ID

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]
TOKEN_FILE = "sheets_token.json"
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
]


def _sheets_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
    return build("sheets", "v4", credentials=creds)


# ---------------------------------------------------------------------------
# Read rows needing back-fill
# ---------------------------------------------------------------------------

COL_STATE     = 1
COL_ATTORNEY  = 11
COL_CANCELLED = 15
COL_SOURCE    = 16
COL_STREET    = 6
COL_CITY      = 7
COL_COUNTY    = 0


def get_rows_needing_backfill(svc) -> list[dict]:
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Auctions!A:Q",
    ).execute()
    rows = result.get("values", [])

    candidates = []
    for i, row in enumerate(rows[1:], start=2):
        padded = row + [""] * (17 - len(row))
        if padded[COL_STATE].strip().upper() != "TN":
            continue
        if padded[COL_ATTORNEY].strip():
            continue
        if padded[COL_CANCELLED].strip().lower() == "yes":
            continue
        if SOURCE not in padded[COL_SOURCE].strip():
            continue
        candidates.append({
            "row_index": i,
            "Street":    padded[COL_STREET].strip(),
            "City":      padded[COL_CITY].strip(),
            "County":    padded[COL_COUNTY].strip(),
        })
    return candidates


# ---------------------------------------------------------------------------
# Parse table — extracts pdf_url from onclick (site uses window.open, not href)
# ---------------------------------------------------------------------------

def _parse_table_local(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    all_rows = table.find_all("tr")
    if not all_rows:
        return []

    headers = [c.get_text(strip=True).lower()
               for c in all_rows[0].find_all(["th", "td"])]

    def _col(kw, default):
        for i, h in enumerate(headers):
            if kw in h:
                return i
        return default

    addr_idx   = _col("address",  1)
    city_idx   = _col("city",     2)
    county_idx = _col("county",   3)
    state_idx  = _col("state",    4)
    zip_idx    = _col("zip",      5)
    orig_idx   = _col("original", 6)
    new_idx    = _col("new",      7)

    rows = []
    for tr in all_rows[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) < max(addr_idx, state_idx, orig_idx) + 1:
            continue

        def cell(idx):
            return cells[idx].get_text(strip=True) if idx < len(cells) else ""

        if cell(state_idx).upper() != "TN":
            continue

        orig_date = _parse_date(cell(orig_idx))
        if not orig_date:
            continue

        # Extract PDF URL from onclick: window.open('Document.ashx?p=...&d=...', ...)
        pdf_url = ""
        link = cells[0].find("a") if cells else None
        if link:
            onclick = link.get("onclick", "")
            m = re.search(r"window\.open\('([^']+)'", onclick)
            if m:
                rel = m.group(1)
                pdf_url = (rel if rel.startswith("http")
                           else SOURCE.rstrip("/") + "/" + rel.lstrip("/"))

        rows.append({
            "pdf_url":            pdf_url,
            "Street":             cell(addr_idx),
            "City":               cell(city_idx),
            "County":             cell(county_idx),
            "Original Sale Date": orig_date,
            "New Sale Date":      _parse_date(cell(new_idx)),
        })

    logger.info("Parsed %d TN row(s) from table", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Main back-fill logic
# ---------------------------------------------------------------------------

def run_backfill(dry_run: bool = False):
    svc = _sheets_service()
    candidates = get_rows_needing_backfill(svc)

    if not candidates:
        print("No rows need back-filling — all internetpostings TN rows have Attorney / Firm set.")
        return

    print(f"Found {len(candidates)} row(s) with blank Attorney / Firm from internetpostings.com")

    sheet_index: dict[tuple, dict] = {}
    for row in candidates:
        key = (row["County"].lower(), _street_number(row["Street"]))
        if key[1]:
            sheet_index[key] = row

    updates: dict[int, str] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=_LAUNCH_ARGS)
        context = browser.new_context(
            user_agent=_USER_AGENT,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        try:
            if not _tos_flow(page):
                print("ERROR: ToS navigation failed. Aborting.")
                return

            html = page.content()
            site_rows = _parse_table_local(html)
            print(f"Site has {len(site_rows)} TN rows to match against.")

            for site in site_rows:
                key = (site["County"].lower(), _street_number(site["Street"]))
                if key not in sheet_index:
                    continue

                sheet_row = sheet_index[key]

                if not _addresses_match(
                    site["Street"], site["City"],
                    sheet_row["Street"], sheet_row["City"],
                ):
                    continue

                pdf_url = site.get("pdf_url", "")
                if not pdf_url:
                    print(f"  Row {sheet_row['row_index']}: {sheet_row['Street']} → (no PDF URL found)")
                    continue

                # Fetch PDF using browser's own JS fetch — carries full ASP.NET session
                try:
                    pdf_b64 = page.evaluate("""
                        async (url) => {
                            const resp = await fetch(url);
                            const buffer = await resp.arrayBuffer();
                            const bytes = new Uint8Array(buffer);
                            let binary = '';
                            bytes.forEach(b => binary += String.fromCharCode(b));
                            return btoa(binary);
                        }
                    """, pdf_url)
                    pdf_bytes = base64.b64decode(pdf_b64) if pdf_b64 else b""
                except Exception as e:
                    logger.warning("PDF JS fetch failed (%s): %s", pdf_url, e)
                    pdf_bytes = b""

                notice_text = _extract_pdf_text(pdf_bytes)
                trustee     = _extract_trustee_name(notice_text)

                row_index = sheet_row["row_index"]
                if trustee:
                    updates[row_index] = trustee
                    print(f"  Row {row_index}: {sheet_row['Street']} → {trustee}")
                else:
                    start = pdf_bytes[:8] if pdf_bytes else b"(empty)"
                    print(f"  Row {row_index}: {sheet_row['Street']} → "
                          f"(not found; {len(pdf_bytes)} bytes, starts {start!r})")

        finally:
            browser.close()

    if not updates:
        print("No trustee names extracted — nothing to write.")
        return

    if dry_run:
        print(f"\n[DRY RUN] Would update {len(updates)} row(s).")
        return

    batch_data = [
        {"range": f"Auctions!L{row_idx}", "values": [[name]]}
        for row_idx, name in updates.items()
    ]
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": batch_data},
    ).execute()
    print(f"\nWrote Attorney / Firm to {len(updates)} row(s).")

    remaining = len(candidates) - len(updates)
    if remaining:
        print(f"{remaining} row(s) could not be matched or extracted.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run_backfill(dry_run=args.dry_run)