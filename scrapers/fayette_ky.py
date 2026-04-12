"""
scrapers/fayette_ky.py — Fayette County KY
Source: https://faycom.info/upcoming/

Site migrated from old showcase.php PHP app to WordPress/Elementor.
Each listing is a <div class="e-loop-item type-sale ..."> block.
Cancelled sales have 'sale-status-cancelled' in their CSS classes.

City is always Lexington (Fayette County is coextensive with Lexington).
Zip is not in the source — geocode_address() looks it up via Nominatim.
Appraisal amounts arrive blank when listings are first posted and are
filled ~2 weeks before the sale date. The general update_blank_fields()
in sheets_writer handles back-filling those on subsequent runs.

Returns (new_listings, cancellation_updates) — same signature as jefferson_ky.py.
cancellation_updates: {row_index: "Yes"} for rows newly marked cancelled.
"""

from __future__ import annotations
import re
import requests
from bs4 import BeautifulSoup

from .base import empty_listing, normalize_date, clean_money, geocode_address
from config import DEFAULT_HEADERS

COUNTY = "Fayette"
STATE = "KY"
CITY  = "Lexington"
URL   = "https://faycom.info/upcoming/"

HEADERS = {
    **DEFAULT_HEADERS,
    "Referer": "https://faycom.info/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def scrape(existing: dict | None = None) -> tuple[list[dict], dict]:
    """
    Args:
        existing: {case_number: (row_index, already_cancelled)} from
                  sheets_writer.get_existing_case_numbers("Fayette")

    Returns:
        (new_listings, cancellation_updates)
        new_listings: active (non-cancelled) listings not yet in the sheet
        cancellation_updates: {row_index: "Yes"} for rows newly cancelled
    """
    existing = existing or {}

    try:
        resp = requests.get(URL, headers=HEADERS, timeout=15)
    except Exception as e:
        print(f"  [Fayette] Fetch error: {e}")
        return [], {}

    if resp.status_code != 200:
        print(f"  [Fayette] HTTP {resp.status_code} — automated access blocked.")
        print(f"  [Fayette] WARNING: Manual check required at: {URL}")
        return [], {}

    soup = BeautifulSoup(resp.text, "html.parser")

    item_divs = [
        div for div in soup.find_all("div", class_="e-loop-item")
        if "type-sale" in div.get("class", [])
    ]

    if not item_divs:
        print(f"  [Fayette] No listing blocks found — page structure may have changed.")
        return [], {}

    new_listings = []
    cancellation_updates = {}

    for div in item_divs:
        listing = _parse_listing(div)
        if not listing:
            continue

        classes = div.get("class", [])
        is_cancelled = "sale-status-cancelled" in classes
        case_num = listing.get("Case Number", "").strip()

        if is_cancelled:
            if case_num and case_num in existing:
                row_idx, already_cancelled = existing[case_num]
                if not already_cancelled:
                    cancellation_updates[row_idx] = "Yes"
            continue

        new_listings.append(listing)

    print(f"  [Fayette] {len(new_listings)} active listing(s), "
          f"{len(cancellation_updates)} new cancellation(s).")
    return new_listings, cancellation_updates


def _parse_listing(div) -> dict | None:
    """Parse a single e-loop-item div into a listing dict. Returns None if unparseable."""
    text = div.get_text("\n", strip=True)

    listing = empty_listing(COUNTY, STATE)
    listing["Source URL"] = URL

    # ── Sale date + street address ───────────────────────────────────────────
    # Title line format: "March 30, 2026 • 520 BROOK FARM COURT"
    m = re.search(r'([A-Za-z]+ \d+,\s*\d{4})\s*[•·]\s*(.+)', text)
    if m:
        listing["Sale Date"] = normalize_date(m.group(1).strip())
        raw_street = m.group(2).strip()
        # Strip any trailing "**SALE CANCELLED**" text
        raw_street = re.sub(
            r'\s*\*+SALE CANCELLED\*+.*', '', raw_street, flags=re.IGNORECASE
        ).strip()
        street = raw_street.title()

        # City is always Lexington; geocode for zip
        _, zip_code = geocode_address(street, CITY, STATE)

        listing["Street"] = street
        listing["City"]   = CITY
        listing["Zip"]    = zip_code

    # ── Case number ──────────────────────────────────────────────────────────
    m = re.search(r'Action No\.\s*([\w-]+)', text)
    if m:
        listing["Case Number"] = m.group(1).strip()

    if not listing.get("Case Number"):
        return None

    # ── Plaintiff / Defendant ────────────────────────────────────────────────
    m = re.search(r'([^\n•]+?)\s+vs\.\s+([^\n–\-]+?)\s+[–\-]\s+Action', text, re.IGNORECASE)
    if m:
        listing["Plaintiff"]    = m.group(1).strip()
        listing["Defendant(s)"] = m.group(2).strip()

    # ── Judgment / principal amount ──────────────────────────────────────────
    m = re.search(r'principal amount of \$([\d,]+\.?\d*)', text, re.IGNORECASE)
    if m:
        listing["Judgment / Loan Amount"] = clean_money("$" + m.group(1))

    # ── Plaintiff's attorney ─────────────────────────────────────────────────
    m = re.search(r"PLAINTIFF'S ATTORNEY:\s*(.+?)(?:\n|APPRAISAL|$)", text,
                  re.IGNORECASE | re.DOTALL)
    if m:
        listing["Attorney / Firm"] = m.group(1).strip().split("\n")[0].strip()

    # ── Appraisal amount — blank on initial posting, back-filled later ───────
    m = re.search(r'APPRAISAL AMOUNT:\s*\$?([\d,]+\.?\d*)', text, re.IGNORECASE)
    if m and m.group(1):
        listing["Appraised Value"] = clean_money("$" + m.group(1))

    return listing