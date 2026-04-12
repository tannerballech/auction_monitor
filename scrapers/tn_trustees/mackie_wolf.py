"""
scrapers/tn_trustees/mackie_wolf.py
Mackie Wolf Zientz & Mann, P.C. — TN Foreclosure Sale Scraper
https://mwzmlaw.com/tn-investors/

MWZM publishes a TN sale report PDF at a date-constructable URL:
  https://mwzmlaw.com/wp-content/uploads/YYYY/MM/TN-Sale-Report-as-of-MM.DD.YYYY.pdf

The report is not always published daily.  Discovery tries today's date and
falls back up to MAX_LOOKBACK_DAYS if the current date's PDF doesn't exist yet.

PDF columns:
  Sale Date | File | Address | Full County | Sale Trustee Designation | City TN Zip

"Sale Trustee Designation" values seen: AUCTION, MWZM, HUBZU, HUDMARSH.
All rows are included regardless of designation.
No judgment/loan amount is published.

PDF parsing uses pdfplumber (not raw pdfminer text extraction).  pdfminer's
extract_text() reads this PDF's layout non-linearly — partial column blocks
split across page breaks — making regex row-matching unreliable.  pdfplumber's
table extractor uses column-boundary detection to return properly aligned rows.

Requires: pip install pdfplumber

Two modes:

  scrape_mackie_wolf(existing_addr_set, dry_run)
      Discovery: fetch PDF, parse, return new listings not already in sheet.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: fetch same PDF, cross-reference Mackie Wolf sheet rows.
      Same property, different date → postponement.
      Absent within CHECK_WINDOW_DAYS → manual-check flag.
      Returns (postponements, flags).
"""

from __future__ import annotations
import logging
import re
from datetime import date, datetime, timedelta
from io import BytesIO

import requests

from scrapers.base import empty_listing

logger = logging.getLogger(__name__)

TRUSTEE           = "Mackie Wolf Zientz & Mann, P.C."
STATE             = "TN"
SOURCE_BASE       = "https://mwzmlaw.com/wp-content/uploads"
MAX_LOOKBACK_DAYS = 7    # days back to search for the latest published PDF
CHECK_WINDOW_DAYS = 14   # flag sheet rows whose sale is ≤ this many days away and absent

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
}

# Validates the file number format seen in MWZM reports
_FILE_RE = re.compile(r"^\d{2}-\d{5,7}-\d{3}$")

# Parses "Lewisburg TN 37091" → (city, zip)
_CITY_ZIP_RE = re.compile(r"^(.+?)\s+TN\s+(\d{5})$")


# ---------------------------------------------------------------------------
# URL construction + PDF fetch
# ---------------------------------------------------------------------------

def _pdf_url_for_date(d: date) -> str:
    """Build the MWZM PDF URL for a given date."""
    return (
        f"{SOURCE_BASE}/{d.year}/{d.month:02d}/"
        f"TN-Sale-Report-as-of-{d.month:02d}.{d.day:02d}.{d.year}.pdf"
    )


def _fetch_pdf() -> tuple[bytes, str] | tuple[None, None]:
    """
    Try today's PDF, working back MAX_LOOKBACK_DAYS days until one is found.
    Returns (pdf_bytes, source_url), or (None, None) if nothing is available.
    """
    today = date.today()
    for days_back in range(MAX_LOOKBACK_DAYS + 1):
        target = today - timedelta(days=days_back)
        url = _pdf_url_for_date(target)
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=20)
            if resp.status_code == 200 and resp.content:
                logger.info("[mackie_wolf] PDF found for %s: %s", target, url)
                return resp.content, url
            elif resp.status_code == 404:
                logger.debug("[mackie_wolf] No PDF for %s (404)", target)
            else:
                logger.warning(
                    "[mackie_wolf] Unexpected status %s for %s", resp.status_code, url
                )
        except requests.RequestException as e:
            logger.warning("[mackie_wolf] Request error for %s: %s", url, e)

    logger.error("[mackie_wolf] No PDF found in last %d days", MAX_LOOKBACK_DAYS)
    return None, None


# ---------------------------------------------------------------------------
# PDF parsing — pdfplumber table extraction
# ---------------------------------------------------------------------------

def _parse_sale_date(raw: str) -> str:
    """'4/14/2026' → '2026-04-14'. Returns '' on failure."""
    try:
        return datetime.strptime(raw.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _parse_pdf(pdf_bytes: bytes) -> list[dict]:
    """
    Parse the MWZM TN sale report PDF using pdfplumber's table extractor.

    pdfminer's extract_text() is unreliable for this PDF layout — it reads
    partial column blocks non-linearly and splits data rows across page breaks.
    pdfplumber's extract_table() uses column-boundary detection to return
    properly aligned rows regardless of internal text order.

    Strategy:
      1. Extract all table rows across all pages.  Try default settings first;
         fall back to text-strategy for borderless tables.
      2. Find the header row to determine column indices dynamically.
      3. Validate each data row by requiring a parseable date + file number.
      4. Parse "City TN Zip" from the last column.
    """
    try:
        import pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber is required for Mackie Wolf PDF parsing. "
            "Run: pip install pdfplumber"
        )

    all_rows: list[list] = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                all_rows.extend(table)
            else:
                # Borderless table fallback — use whitespace as column separator
                table = page.extract_table(
                    table_settings={
                        "vertical_strategy":   "text",
                        "horizontal_strategy": "text",
                    }
                )
                if table:
                    all_rows.extend(table)

    if not all_rows:
        logger.warning("[mackie_wolf] pdfplumber found no table rows in PDF")
        return []

    # ── Identify header row and column indices ────────────────────────────
    date_idx   = 0
    file_idx   = 1
    addr_idx   = 2
    county_idx = 3
    city_idx   = -1   # last column; refined from header scan

    data_start = 0
    for i, row in enumerate(all_rows):
        row_text = " ".join(str(c or "").lower() for c in row)
        if "sale date" in row_text and "file" in row_text:
            for j, cell in enumerate(row):
                c = str(cell or "").lower().strip()
                if "sale date" in c:
                    date_idx = j
                elif c == "file":
                    file_idx = j
                elif "address" in c:
                    addr_idx = j
                elif "county" in c:
                    county_idx = j
            # City/zip is the rightmost populated header column
            city_idx   = max(j for j, cell in enumerate(row) if str(cell or "").strip())
            data_start = i + 1
            break

    # ── Parse data rows ───────────────────────────────────────────────────
    results: list[dict] = []

    for row in all_rows[data_start:]:
        if not row:
            continue

        def cell(idx: int) -> str:
            return str(row[idx] or "").strip() if idx < len(row) else ""

        sale_date_raw = cell(date_idx)
        file_num = cell(file_idx)
        addr_raw = cell(addr_idx)  # "Street\nCity TN Zip"
        county = cell(county_idx)

        # Require a valid date and file number — filters blank/orphaned rows
        if not re.match(r"\d{1,2}/\d{1,2}/\d{4}", sale_date_raw):
            continue
        if not _FILE_RE.match(file_num):
            continue

        # Split address cell on newline → street + city/zip
        if "\n" in addr_raw:
            street, city_state_zip = addr_raw.split("\n", 1)
            street = street.strip()
            city_state_zip = city_state_zip.strip()
        else:
            street = addr_raw
            city_state_zip = ""

        if not street or not county:
            logger.debug("[mackie_wolf] Missing street or county — skipping: %r", row)
            continue

        sale_date = _parse_sale_date(sale_date_raw)
        if not sale_date:
            logger.warning("[mackie_wolf] Unparseable date %r — skipping", sale_date_raw)
            continue

        # Parse "Lewisburg TN 37091" → city, zip
        city = zip_code = ""
        if city_state_zip:
            m = _CITY_ZIP_RE.match(city_state_zip)
            if m:
                city = m.group(1).strip()
                zip_code = m.group(2).strip()
            else:
                logger.debug(
                    "[mackie_wolf] Could not parse city/zip from %r", city_state_zip
                )

        results.append({
            "Sale Date": sale_date,
            "Case Number": file_num,
            "Street": street,
            "City": city,
            "County": county,
            "State": STATE,
            "Zip": zip_code,
        })

    logger.info("[mackie_wolf] Parsed %d row(s) from PDF", len(results))
    return results


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


def _addresses_match(site_street: str, site_city: str,
                     sheet_street: str, sheet_city: str) -> bool:
    num_a = _street_number(site_street)
    num_b = _street_number(sheet_street)
    if not num_a or not num_b or num_a != num_b:
        return False
    if _street_first_word(site_street) != _street_first_word(sheet_street):
        return False
    city_a = site_city.lower().strip()
    city_b = sheet_city.lower().strip()
    if city_a and city_b and city_a != city_b:
        return False
    return True


# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def scrape_mackie_wolf(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.  Fetch the latest MWZM TN sale report PDF and return
    listings not already in the sheet.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        covering all active TN rows — prevents duplicating TNLedger rows.

    Returns (new_listings, {}).  cancellation_updates is always empty —
    MWZM doesn't publish cancellations; listings simply drop off the report.
    """
    pdf_bytes, source_url = _fetch_pdf()
    if not pdf_bytes:
        logger.error("[mackie_wolf] No PDF available — skipping discovery.")
        return [], {}

    site_rows = _parse_pdf(pdf_bytes)

    today = date.today()
    new_listings: list[dict] = []

    for site in site_rows:
        orig_date = site["Sale Date"]
        county    = site["County"]
        street    = site["Street"]

        # Gate 3 — skip if fewer than 3 days out
        try:
            days_out = (datetime.strptime(orig_date, "%Y-%m-%d").date() - today).days
        except ValueError:
            continue
        if days_out < 3:
            continue

        # Address-based dedup against all existing TN rows
        addr_key = (county.lower(), _street_number(street), orig_date)
        if addr_key in existing_addr_set:
            continue

        listing = empty_listing(county, STATE)
        listing["Case Number"]            = site["Case Number"]
        listing["Attorney / Firm"]        = TRUSTEE
        listing["Sale Date"]              = orig_date
        listing["Street"]                 = street
        listing["City"]                   = site["City"]
        listing["Zip"]                    = site["Zip"]
        listing["Judgment / Loan Amount"] = ""
        listing["Source URL"]             = source_url

        new_listings.append(listing)

    logger.info("[mackie_wolf] %d new listing(s) after dedup", len(new_listings))
    return new_listings, {}


# ---------------------------------------------------------------------------
# Check mode
# ---------------------------------------------------------------------------

def check_existing(
    sheet_rows: list[dict],
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Check mode.  Fetch the latest MWZM TN sale report PDF and cross-reference
    Mackie Wolf sheet rows.

    Postponement: listing present on site with a different sale date.
    Flag:         listing absent AND sale is within CHECK_WINDOW_DAYS.

    Returns (postponements, flags).
    """
    pdf_bytes, _ = _fetch_pdf()
    if not pdf_bytes:
        logger.error("[mackie_wolf] No PDF available — skipping check.")
        return [], []

    site_rows = _parse_pdf(pdf_bytes)

    # Index by (county_lower, street_number) → site row
    site_index: dict[tuple, dict] = {}
    for s in site_rows:
        key = (s["County"].lower(), _street_number(s["Street"]))
        if key and key not in site_index:
            site_index[key] = s

    today = date.today()
    postponements: list[dict] = []
    flags: list[dict] = []

    for row in sheet_rows:
        sale_date_str = row.get("Sale Date", "")
        county        = row.get("County", "")
        sheet_street  = row.get("Street", "")
        sheet_city    = row.get("City", "")
        row_index     = row["row_index"]

        street_num = _street_number(sheet_street)
        if not street_num:
            continue

        site_hit = site_index.get((county.lower(), street_num))

        if site_hit is None:
            # Not found — flag if sale is approaching
            try:
                days_out = (
                    datetime.strptime(sale_date_str, "%Y-%m-%d").date() - today
                ).days
            except ValueError:
                continue
            if 0 <= days_out <= CHECK_WINDOW_DAYS:
                flags.append({
                    "row_index": row_index,
                    "reason": (
                        f"Not found on MWZM site "
                        f"({days_out} day(s) until scheduled sale on {sale_date_str})"
                    ),
                })
            continue

        # Verify same property
        if not _addresses_match(
            site_hit["Street"], site_hit["City"],
            sheet_street, sheet_city,
        ):
            continue

        # Postponement: site date differs from sheet date
        new_date = site_hit["Sale Date"]
        if new_date != sale_date_str:
            postponements.append({
                "row_index": row_index,
                "old_date":  sale_date_str,
                "new_date":  new_date,
            })

    return postponements, flags