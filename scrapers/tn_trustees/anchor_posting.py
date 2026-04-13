"""
scrapers/tn_trustees/anchor_posting.py
McMichael Taylor Gray LLC — TN Foreclosure Sale Scraper
https://anchorposting.com

MTG's statutory internet posting site under T.C.A. § 35-5-104(a).
All listings on this site are McMichael Taylor Gray LLC.

--- Site architecture (confirmed from live data) ---

  GET https://anchorposting.com/tn-foreclosure-search/

  Plain WordPress page. Table is fully server-rendered — no JS, no auth,
  no disclaimer gate needed.

  Table columns:
    Reference No. | County | Sale Date | Sale Time | Address | Postponed Sale Date

  Reference No.: e.g. "25-004144" or "TN2025-00444"
    Links to PDF: https://anchorposting.com/wp-tables/25-004144 NOS.pdf
    (URL has a literal space before "NOS" — must be %-encoded when fetching)
    Stored as Case Number in the sheet.

  County: e.g. "Shelby" — pre-parsed column, no lookup needed.

  Sale Date: "04/16/26" (MM/DD/YY, 2-digit year)

  Address: "1368 Lehr Road, Memphis, TN 38116"
    Format: "STREET, CITY, STATE ZIP"
    Title case, comma-separated.

  Postponed Sale Date: "4/15/2026" (M/D/YYYY) or blank
    Populated = this row is the ORIGINAL listing, now superseded by postponement.
    The new active listing appears as a separate row with a later Sale Date and
    blank Postponed Sale Date.

--- Duplicate row handling ---

When a sale is postponed, the site retains the original row (with Postponed
Sale Date populated) and adds a new row for the rescheduled date (blank
Postponed Sale Date). Both rows share the same Reference No. and address.

Discovery: skip rows where Postponed Sale Date is populated — those are
historical. Only import rows with blank Postponed Sale Date and future date.

Check mode: rows with Postponed Sale Date populated signal that a previously
scheduled sale was moved. Match by Reference No. (primary) or address (fallback).

--- Two modes ---

  scrape_anchor_posting(existing_addr_set, dry_run)
      Discovery: fetch table, skip rows with Postponed Sale Date, return new
      listings not already in the sheet.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: fetch table, cross-reference sheet rows by Reference No. or address.
      Row with populated Postponed Sale Date where dates differ → postponement.
      Absent within CHECK_WINDOW_DAYS → manual-check flag.
      Returns (postponements, flags).
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

from scrapers.base import empty_listing

logger = logging.getLogger(__name__)

TRUSTEE           = "McMichael Taylor Gray LLC"
STATE             = "TN"
SOURCE_URL        = "https://anchorposting.com/tn-foreclosure-search/"
CHECK_WINDOW_DAYS = 14

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------

def _fetch_html() -> Optional[str]:
    """Fetch the listings page. Returns HTML or None on failure."""
    try:
        resp = requests.get(SOURCE_URL, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        logger.info("[anchor_posting] Fetched page (%.1f KB)", len(resp.content) / 1024)
        return resp.text
    except requests.RequestException as e:
        logger.error("[anchor_posting] Fetch failed: %s", e)
        return None


def _parse_date(raw: str) -> str:
    """
    Parse to ISO YYYY-MM-DD.
    Handles: "04/16/26" (MM/DD/YY), "4/15/2026" (M/D/YYYY).
    Returns "" on failure.
    """
    raw = raw.strip()
    if not raw:
        return ""
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    logger.debug("[anchor_posting] Unparseable date: %r", raw)
    return ""


def _parse_address(raw: str) -> tuple[str, str, str]:
    """
    Parse "1368 Lehr Road, Memphis, TN 38116" → (street, city, zip).
    Format: "STREET, CITY, STATE ZIP"
    """
    raw = raw.strip()
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) >= 3:
        street   = parts[0]
        city     = parts[1].strip()
        # "TN 38116" or "TN  38116"
        state_zip = parts[2].strip()
        m = re.search(r"(\d{5})", state_zip)
        zip_code = m.group(1) if m else ""
        return street, city, zip_code
    elif len(parts) == 2:
        street = parts[0]
        # "Memphis TN 38116"
        m = re.match(r"^(.+?)\s+TN\s+(\d{5})", parts[1], re.I)
        if m:
            return street, m.group(1).strip(), m.group(2)
        return street, parts[1], ""
    return raw, "", ""


def _parse_table(html: str) -> list[dict]:
    """
    Parse all rows from the listing table.

    Returns list of dicts:
      ref_num, county, street, city, zip, sale_date,
      postponed_date ("")  ← "" means active listing, populated = superseded
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the table — look for one with a "County" and "Sale Date" header
    table = None
    for t in soup.find_all("table"):
        text = t.get_text().lower()
        if "county" in text and "sale date" in text:
            table = t
            break

    if not table:
        # Fallback: first sizable table
        tables = soup.find_all("table")
        table = max(tables, key=lambda t: len(t.find_all("tr"))) if tables else None

    if not table:
        logger.error("[anchor_posting] No table found in HTML")
        return []

    rows_el = table.find_all("tr")
    if not rows_el:
        return []

    # Parse header
    header_cells = rows_el[0].find_all(["th", "td"])
    headers = [c.get_text(strip=True).lower() for c in header_cells]
    logger.debug("[anchor_posting] Headers: %s", headers)

    def _col(keywords: list[str], default: int) -> int:
        for i, h in enumerate(headers):
            if any(kw in h for kw in keywords):
                return i
        return default

    ref_idx      = _col(["reference", "ref"],            0)
    county_idx   = _col(["county"],                       1)
    date_idx     = _col(["sale date"],                    2)
    addr_idx     = _col(["address"],                      4)
    postponed_idx = _col(["postponed", "new"],            5)

    rows = []
    for tr in rows_el[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) < 4:
            continue

        def cell(idx: int) -> str:
            if idx >= len(cells):
                return ""
            # For the reference cell, extract text from the link if present
            a = cells[idx].find("a")
            return (a or cells[idx]).get_text(strip=True)

        ref_num       = cell(ref_idx).strip()
        county        = cell(county_idx).strip().title()
        sale_date_raw = cell(date_idx).strip()
        addr_raw      = cell(addr_idx).strip()
        postponed_raw = cell(postponed_idx).strip()

        if not addr_raw or not sale_date_raw:
            continue

        sale_date      = _parse_date(sale_date_raw)
        postponed_date = _parse_date(postponed_raw)

        if not sale_date:
            logger.debug("[anchor_posting] Skipping — bad date: %r", sale_date_raw)
            continue

        street, city, zip_code = _parse_address(addr_raw)
        if not street:
            continue

        rows.append({
            "ref_num":        ref_num,
            "county":         county,
            "street":         street,
            "city":           city,
            "zip":            zip_code,
            "sale_date":      sale_date,
            "postponed_date": postponed_date,  # "" = active, populated = superseded
        })

    logger.info("[anchor_posting] Parsed %d row(s) from table", len(rows))
    return rows


def _get_listings() -> list[dict]:
    html = _fetch_html()
    return _parse_table(html) if html else []


# ---------------------------------------------------------------------------
# Address utilities
# ---------------------------------------------------------------------------

def _street_number(street: str) -> str:
    m = re.match(r"^(\d+)", street.strip())
    return m.group(1) if m else ""


def _street_first_word(street: str) -> str:
    name = re.sub(r"^\d+\s*", "", street.strip().lower())
    name = re.sub(r"[^\w\s]", "", name)
    words = name.split()
    return words[0] if words else ""


def _addresses_match(a_st: str, a_city: str, b_st: str, b_city: str) -> bool:
    num_a, num_b = _street_number(a_st), _street_number(b_st)
    if not num_a or not num_b or num_a != num_b:
        return False
    if _street_first_word(a_st) != _street_first_word(b_st):
        return False
    ca, cb = a_city.lower().strip(), b_city.lower().strip()
    if ca and cb and ca != cb:
        return False
    return True


# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def scrape_anchor_posting(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.

    Skips rows where postponed_date is populated — those are superseded
    original listings. Only imports active rows (blank postponed_date)
    with a future sale date.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        for all active TN rows — prevents cross-source duplicates.

    Returns (new_listings, {}).
    """
    site_rows = _get_listings()
    if not site_rows:
        return [], {}

    today_str = date.today().isoformat()
    new_listings: list[dict] = []
    stats = {"past": 0, "superseded": 0, "dup": 0}

    for row in site_rows:
        # Skip superseded (original) rows — they have postponed_date populated
        if row["postponed_date"]:
            stats["superseded"] += 1
            continue

        sale_date = row["sale_date"]
        if not sale_date or sale_date < today_str:
            stats["past"] += 1
            continue

        street_num   = _street_number(row["street"])
        county_lower = row["county"].lower()

        if (county_lower, street_num, sale_date) in existing_addr_set:
            stats["dup"] += 1
            continue

        listing = empty_listing(county=row["county"], state="TN")
        listing.update({
            "Sale Date":              sale_date,
            "Case Number":            row["ref_num"],
            "Plaintiff":              "",
            "Defendant(s)":           "",
            "Street":                 row["street"],
            "City":                   row["city"],
            "Zip":                    row["zip"],
            "Appraised Value":        "",
            "Judgment / Loan Amount": "",
            "Attorney / Firm":        TRUSTEE,
            "Cancelled":              "",
            "Source URL":             SOURCE_URL,
            "Notes":                  "",
        })

        new_listings.append(listing)
        if not dry_run:
            existing_addr_set.add((county_lower, street_num, sale_date))

    logger.info(
        "[anchor_posting] Discovery — new=%d superseded=%d past=%d dup=%d",
        len(new_listings), stats["superseded"], stats["past"], stats["dup"],
    )
    return new_listings, {}


# ---------------------------------------------------------------------------
# Check mode
# ---------------------------------------------------------------------------

def check_existing(
    sheet_rows: list[dict],
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Check mode. Detect postponements and absences for existing MTG sheet rows.

    Postponement detection:
      The site retains the original row with Postponed Sale Date populated.
      We match by Reference No. (Case Number in sheet), then by address.
      If the site row's sale_date matches our sheet_date AND postponed_date
      is populated → postponement to postponed_date.

    Absent within CHECK_WINDOW_DAYS → manual-check flag.

    Returns (postponements, flags).
    """
    if not sheet_rows:
        return [], []

    site_rows = _get_listings()
    if not site_rows:
        logger.warning("[anchor_posting] No rows returned — skipping check")
        return [], []

    today     = date.today()
    today_str = today.isoformat()
    threshold = (today + timedelta(days=CHECK_WINDOW_DAYS)).isoformat()

    # Build fast lookups
    # By reference number → list of site rows (could be original + rescheduled)
    site_by_ref: dict[str, list[dict]] = {}
    for r in site_rows:
        key = r["ref_num"].upper()
        site_by_ref.setdefault(key, []).append(r)

    postponements: list[dict] = []
    flags: list[dict] = []

    for sheet_row in sheet_rows:
        sheet_street = sheet_row.get("Street", "")
        sheet_city   = sheet_row.get("City", "")
        sheet_date   = sheet_row.get("Sale Date", "")
        sheet_case   = (sheet_row.get("Case Number") or "").upper()
        row_index    = sheet_row.get("row_index")

        if not sheet_street or not sheet_date:
            continue
        if sheet_date < today_str:
            continue

        # Find matching site rows
        matched_site_rows = (
            site_by_ref.get(sheet_case, [])
            or [r for r in site_rows
                if _addresses_match(r["street"], r["city"], sheet_street, sheet_city)]
        )

        if not matched_site_rows:
            # Not found on site at all
            if sheet_date <= threshold:
                days_out = (date.fromisoformat(sheet_date) - today).days
                flags.append({
                    "row_index": row_index,
                    "note": (
                        f"⚠️ Manual check — Not found on Anchor Posting site "
                        f"({days_out} day(s) until scheduled sale on {sheet_date})"
                    ),
                })
            continue

        # Look for a site row where sale_date == sheet_date AND
        # postponed_date is populated → that's our postponement signal
        postponed_row = next(
            (r for r in matched_site_rows
             if r["sale_date"] == sheet_date and r["postponed_date"]),
            None,
        )

        if postponed_row:
            new_date = postponed_row["postponed_date"]
            logger.info(
                "[anchor_posting] Postponement: row %s  %s → %s  (%s)",
                row_index, sheet_date, new_date, sheet_street,
            )
            postponements.append({
                "row_index": row_index,
                "old_date":  sheet_date,
                "new_date":  new_date,
                "note": f"Postponed: {sheet_date} → {new_date} (Anchor Posting)",
            })
            continue

        # Row found on site with no postponement — all good, no action needed

    logger.info(
        "[anchor_posting] Check — %d postponement(s), %d flag(s) from %d row(s)",
        len(postponements), len(flags), len(sheet_rows),
    )
    return postponements, flags