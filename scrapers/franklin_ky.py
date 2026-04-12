"""
scrapers/franklin_ky.py
Franklin County KY — Master Commissioner sales
Source: http://www.franklincomc.com/upcoming-sales.html

Static GoDaddy Website Builder page — server-rendered HTML, no Playwright needed.
Returns (new_listings, cancellation_updates) like Jefferson and Fayette.

Cancellation detection: the word "CANCELLED" appears in the sale block text
when a listing is cancelled. Cancelled listings already in the sheet are
flagged via cancellation_updates. Brand-new cancelled listings are skipped.

City/zip lookup: geocode_address() from base.py resolves city and zip via
Nominatim (two-pass, 1 req/sec). Franklin County includes Frankfort (county
seat), Peaks Mill, Bridgeport, etc. Falls back to "Frankfort" if unresolvable.

Appraisal starts as TBD — update_blank_fields() back-fills it on later runs.

Page structure notes:
  - The page may contain "special" sales conducted by a different commissioner
    that appear BEFORE the standard "SALE 1:", "SALE 2:" blocks. These are
    parsed from blocks[0] of the SALE splitter rather than being discarded.
  - Multi-parcel commercial sales use "Parcel N: address" format. The parser
    extracts the first numbered street address from these blocks.
  - Listings with no extractable street number (e.g. "Capital Center Drive")
    are skipped and logged as known commercial properties.
"""

import re
import time
import logging
import requests
from bs4 import BeautifulSoup

from .base import empty_listing, clean_money, normalize_date, geocode_address

logger = logging.getLogger(__name__)

COUNTY        = "Franklin"
STATE         = "KY"
URL           = "http://www.franklincomc.com/upcoming-sales.html"
FALLBACK_CITY = "Frankfort"   # used when geocoder returns no match

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ----------------------------------------------------------------------------
# Public entry point
# ----------------------------------------------------------------------------

def scrape_franklin_ky(existing=None, dry_run=False):
    """
    Fetch and parse Franklin County KY MC sale listings.

    Args:
        existing (dict): {case_number: (row_index, already_cancelled)} from
                         sheets_writer.get_existing_case_numbers("Franklin")
        dry_run  (bool): If True, skip all network/geocoder calls and return stubs.

    Returns:
        (new_listings, cancellation_updates)
        new_listings         — list of active listing dicts not already in the sheet
        cancellation_updates — {row_index: "Yes"} for sheet rows newly marked cancelled
    """
    if existing is None:
        existing = {}

    if dry_run:
        logger.info("[franklin_ky] dry-run — skipping fetch")
        return [], {}

    try:
        resp = requests.get(URL, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as exc:
        logger.error(f"[franklin_ky] Fetch failed: {exc}")
        return [], {}

    soup = BeautifulSoup(resp.text, "html.parser")
    page_text = soup.get_text(separator="\n")

    listings = _parse_page(page_text)

    # ── Cancellation tracking ─────────────────────────────────────────────────
    cancellation_updates = {}
    for lst in listings:
        case_num = lst.get("Case Number")
        if not case_num:
            continue
        if lst.get("Cancelled") == "Yes" and case_num in existing:
            row_idx, already_cancelled = existing[case_num]
            if not already_cancelled:
                cancellation_updates[row_idx] = "Yes"
                logger.info(f"[franklin_ky] Marking cancelled: {case_num} (row {row_idx})")

    # ── New listings: active only, not already in the sheet ──────────────────
    new_listings = [
        lst for lst in listings
        if lst.get("Case Number")
        and lst["Case Number"] not in existing
        and lst.get("Cancelled") != "Yes"
    ]

    # ── Geocode city + zip for new listings via shared Nominatim utility ──────
    # _street_only is a temporary field set by _parse_sale_block() — pop it
    # here so it never reaches sheets_writer.
    for lst in new_listings:
        street = lst.pop("_street_only", None)
        if street:
            city, zip_code = geocode_address(street, "", STATE)
            lst["City"] = city or FALLBACK_CITY
            lst["Zip"]  = zip_code

    logger.info(
        f"[franklin_ky] {len(listings)} on page, "
        f"{len(new_listings)} new, "
        f"{len(cancellation_updates)} cancellation(s)"
    )
    return new_listings, cancellation_updates


# ----------------------------------------------------------------------------
# Parsing helpers
# ----------------------------------------------------------------------------

_DATE_RE = re.compile(
    r"(?:MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY),\s+"
    r"(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST"
    r"|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)"
    r"\s+\d{1,2},\s+\d{4}",
    re.IGNORECASE,
)

_SALE_SPLIT_RE = re.compile(r"\n\s*SALE\s+\d+\s*:", re.IGNORECASE)


def _parse_page(text):
    listings = []
    date_matches = list(_DATE_RE.finditer(text))

    if not date_matches:
        logger.warning("[franklin_ky] No sale-date headers found on page")
        return listings

    for i, dm in enumerate(date_matches):
        sale_date_raw = dm.group(0)
        sale_date = normalize_date(sale_date_raw)
        if not sale_date:
            logger.warning(f"[franklin_ky] Could not parse date: {sale_date_raw!r}")

        section_start = dm.end()
        section_end   = date_matches[i + 1].start() if i + 1 < len(date_matches) else len(text)
        section       = text[section_start:section_end]

        blocks = _SALE_SPLIT_RE.split(section)

        # blocks[0] is content before the first "SALE N:" marker.
        # This may contain a special sale run by a different commissioner
        # (e.g. "Special Master Commissioner James Liebman"). Parse it too —
        # _parse_sale_block() returns None if no Civil Action number is found,
        # so pure intro text is safely ignored.
        for block in blocks:
            lst = _parse_sale_block(block.strip(), sale_date)
            if lst:
                listings.append(lst)

    return listings


def _parse_sale_block(block, sale_date):
    """
    Parse a single sale block into a listing dict.

    Address extraction uses two passes:
      1. Standard: line starting with a street number
         e.g. "348 Green Fields Lane - Parcel No. ..."
      2. Parcel format: "Parcel N: street_number address"
         e.g. "Parcel 1:  102 Athletic Drive - Parcel No. ..."
         Used for multi-parcel commercial sales — first numbered address wins.

    Street is set into listing["Street"]. City and Zip are filled by the
    caller via geocode_address(). The temporary "_street_only" field carries
    the street string up to the caller without polluting the final dict.
    """
    lst = empty_listing(COUNTY, STATE)
    lst["Sale Date"]  = sale_date or ""
    lst["Source URL"] = URL

    # ── Cancellation ─────────────────────────────────────────────────────────
    if re.search(r"\bCANCELLED?\b", block, re.IGNORECASE):
        lst["Cancelled"] = "Yes"

    # ── Case number ──────────────────────────────────────────────────────────
    case_m = re.search(r"Civil Action No\.\s+([\w\-]+)", block, re.IGNORECASE)
    if case_m:
        lst["Case Number"] = case_m.group(1).strip()

    # ── Plaintiff + Defendant ────────────────────────────────────────────────
    vs_m = re.search(
        r"Civil Action No\.\s+[\w\-]+\s*[-\u2013]\s*"
        r"(.+?)\s+v\s+"
        r"(.+?)"
        r"(?:,?\s*et al\.?)?"
        r",?\s*to raise",
        block, re.IGNORECASE | re.DOTALL,
    )
    if vs_m:
        lst["Plaintiff"]    = vs_m.group(1).strip()
        defendant_raw       = vs_m.group(2).strip()
        defendant_raw = re.sub(r",?\s*et al\.?\s*$", "", defendant_raw, flags=re.IGNORECASE).strip()
        lst["Defendant(s)"] = defendant_raw

    # ── Judgment / Loan Amount ───────────────────────────────────────────────
    judgment_m = re.search(
        r"to raise the sum of\s+(\$[0-9,]+(?:\.\d{2})?)",
        block, re.IGNORECASE,
    )
    if judgment_m:
        lst["Judgment / Loan Amount"] = clean_money(judgment_m.group(1))

    # ── Street address — pass 1: standard (line starts with street number) ───
    addr_m = re.search(
        r"^\s*(\d+\s+[A-Za-z0-9 .#'\-]+?)(?:\s*[-\u2013]\s*Parcel No\..*)?$",
        block, re.MULTILINE,
    )

    # ── Street address — pass 2: "Parcel N: address" format ─────────────────
    # Handles multi-parcel commercial sales where addresses follow a label.
    # Takes the first numbered street address found.
    if not addr_m:
        addr_m = re.search(
            r"Parcel\s+\d+\s*:\s+(\d+\s+[A-Za-z0-9 .#'\-]+?)(?:\s*[-\u2013]\s*Parcel No\..*)?$",
            block, re.MULTILINE,
        )

    if addr_m:
        street = addr_m.group(1).strip()
        lst["Street"]       = street
        lst["_street_only"] = street   # temp — popped by scrape_franklin_ky()

    # ── Attorney / Firm ──────────────────────────────────────────────────────
    atty_m = re.search(r"Attorney for Plaintiff:\s*(.+)", block, re.IGNORECASE)
    if atty_m:
        lst["Attorney / Firm"] = atty_m.group(1).strip()

    # ── Appraised Value (TBD initially; back-filled by update_blank_fields) ──
    appraisal_m = re.search(r"Appraisal:\s*(.+)", block, re.IGNORECASE)
    if appraisal_m:
        appraisal_raw = appraisal_m.group(1).strip()
        if appraisal_raw.upper() not in ("TBD", ""):
            lst["Appraised Value"] = clean_money(appraisal_raw)

    # ── Guard: need at minimum a case number ─────────────────────────────────
    if not lst.get("Case Number"):
        return None

    if lst.get("Cancelled") != "Yes" and not lst.get("Street"):
        logger.warning(
            f"[franklin_ky] No street address found for active listing "
            f"(case {lst.get('Case Number', 'unknown')}) — likely commercial property "
            f"with non-standard address. Skipping."
        )
        return None

    return lst