"""
Rubin Lublin TN, PLLC — Foreclosure Sale Scraper
https://rlselaw.com/property-listing/tennessee-property-listings/

Two modes:

  scrape_rubin_lublin(existing_addr_set, dry_run)
      Discovery: fetches the TN listing table, returns new listings not already
      in the Auctions sheet (checked by address+county+date, not case number,
      since Rubin Lublin file #s live in a different namespace than TNLedger FK IDs).
      Returns (new_listings, cancellation_updates).  cancellation_updates is always
      empty — Rubin Lublin doesn't publish cancellations; cancelled listings simply
      disappear from the table.

  check_existing(sheet_rows, dry_run)
      Check mode: compares existing Auctions sheet rows (passed in from main.py)
      against the current live listing table to detect:
        - Postponements  (listing still present but sale date changed)
        - Missing-close  (listing absent AND sale date within CHECK_WINDOW_DAYS)
      Returns (postponements, flags).
        postponements: list of {row_index, old_date, new_date}
        flags:         list of {row_index, reason}
"""

import logging
import re
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

from scrapers.base import empty_listing, clean_money

# Standard browser headers — same pattern used across all scrapers
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

logger = logging.getLogger(__name__)

SOURCE         = "https://rlselaw.com/property-listing/tennessee-property-listings/"
STATE          = "TN"
TRUSTEE        = "Rubin Lublin TN, PLLC"
CHECK_WINDOW_DAYS = 14   # flag sheet rows whose sale is ≤ this many days away and absent from site


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------

def _parse_sale_date(raw: str) -> str:
    """
    '04/14/2026 (9am - 7pm)' → '2026-04-14'
    Returns '' if unparseable.
    """
    m = re.match(r"(\d{1,2}/\d{1,2}/\d{4})", raw.strip())
    if not m:
        return ""
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _fetch_listings() -> list[dict]:
    """
    Fetch the TN listing table from rlselaw.com and return a list of raw dicts.
    Keys: Sale Date (YYYY-MM-DD), Case Number (file#), Street, City, Zip,
          County, Judgment / Loan Amount (dollar string or '').
    Returns [] if the table can't be found.
    """
    try:
        resp = requests.get(SOURCE, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("[rubin_lublin] Fetch failed: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        logger.warning("[rubin_lublin] No table found on listing page")
        return []

    rows = table.find_all("tr")
    results = []
    for row in rows[1:]:  # skip header row
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        if len(cells) < 6:
            continue

        sale_date_raw = cells[0]
        file_no       = cells[1].strip()
        street        = cells[2].strip()
        city          = cells[3].strip()
        zip_code      = cells[4].strip()
        county        = cells[5].strip()
        bid_raw       = cells[6].strip() if len(cells) > 6 else ""

        sale_date = _parse_sale_date(sale_date_raw)
        if not sale_date or not street or not county:
            continue

        # Only capture bid if it looks like an actual dollar amount
        loan_amount = ""
        if bid_raw.startswith("$"):
            try:
                loan_amount = clean_money(bid_raw)
            except Exception:
                loan_amount = bid_raw

        results.append({
            "Sale Date":               sale_date,
            "Case Number":             file_no,
            "Street":                  street,
            "City":                    city,
            "Zip":                     zip_code,
            "County":                  county,
            "Judgment / Loan Amount":  loan_amount,
        })

    logger.info("[rubin_lublin] Fetched %d listing(s) from site", len(results))
    return results


# ---------------------------------------------------------------------------
# Address utilities
# ---------------------------------------------------------------------------

def _street_number(street: str) -> str:
    """Extract leading digits from a street address."""
    m = re.match(r"^(\d+)", street.strip())
    return m.group(1) if m else ""


def _street_first_word(street: str) -> str:
    """
    Return the first word of the street name (i.e., after the street number),
    lowercased and stripped of punctuation.  Used for fuzzy matching.
    """
    # Remove leading number
    name = re.sub(r"^\d+\s*", "", street.strip().lower())
    # Strip punctuation
    name = re.sub(r"[^\w\s]", "", name)
    words = name.split()
    return words[0] if words else ""


def _addresses_match(site_street: str, site_city: str,
                     sheet_street: str, sheet_city: str) -> bool:
    """
    Return True if two address pairs refer to the same property.
    Match criteria:
      - Street numbers are identical
      - First word of the street name matches
      - Cities match (if both non-empty)
    """
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

def scrape_rubin_lublin(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.  Fetch the current Rubin Lublin TN listing table and
    return listings not already in the sheet.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        covering ALL active TN rows in the sheet — used to prevent duplicating
        entries that TNLedger already wrote (with different case number format).

    Returns (new_listings, cancellation_updates).
    cancellation_updates is always {} — Rubin Lublin has no cancellation signal.
    """
    site_listings = _fetch_listings()
    if not site_listings:
        return [], {}

    today = date.today()
    new_listings = []

    for site in site_listings:
        sale_date = site["Sale Date"]
        county    = site["County"]
        street    = site["Street"]

        # Gate 3 equivalent: skip if too close
        try:
            days_out = (datetime.strptime(sale_date, "%Y-%m-%d").date() - today).days
        except ValueError:
            continue
        if days_out < 3:
            continue

        # Address-based dedup against existing TN rows
        addr_key = (county.lower(), _street_number(street), sale_date)
        if addr_key in existing_addr_set:
            continue

        listing = empty_listing(county, STATE)
        listing["Case Number"]             = site["Case Number"]
        listing["Attorney / Firm"]         = TRUSTEE
        listing["Sale Date"]               = sale_date
        listing["Street"]                  = street
        listing["City"]                    = site["City"]
        listing["Zip"]                     = site["Zip"]
        listing["Judgment / Loan Amount"]  = site["Judgment / Loan Amount"]
        listing["Source URL"]              = SOURCE

        new_listings.append(listing)

    logger.info("[rubin_lublin] %d new listing(s) after dedup", len(new_listings))
    return new_listings, {}


# ---------------------------------------------------------------------------
# Check mode
# ---------------------------------------------------------------------------

def check_existing(
    sheet_rows: list[dict],
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Check mode.  Compare existing Auctions sheet rows against the current
    Rubin Lublin listing page to detect postponements and near-sale absences.

    sheet_rows: list of dicts from sheets_writer.get_tn_listings_for_check(),
        each containing: row_index, County, Street, City, Sale Date, Notes.

    Returns:
      postponements: list of {row_index, old_date, new_date}
      flags:         list of {row_index, reason}
    """
    site_listings = _fetch_listings()

    # Index site listings for O(1) lookup:
    # (county_lower, street_number) → site dict
    # Note: multiple listings could share a street number in different counties,
    # so we include county in the key.  Same address on different sale dates
    # would be rare but handled — we take the first hit and compare dates.
    site_index: dict[tuple, dict] = {}
    for s in site_listings:
        key = (s["County"].lower(), _street_number(s["Street"]))
        if key and key not in site_index:
            site_index[key] = s

    today = date.today()
    postponements: list[dict] = []
    flags: list[dict]         = []

    for row in sheet_rows:
        sale_date_str = row.get("Sale Date", "")
        row_index     = row["row_index"]
        county        = row.get("County", "")
        sheet_street  = row.get("Street", "")
        sheet_city    = row.get("City", "")

        try:
            sale_date = datetime.strptime(sale_date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        street_num = _street_number(sheet_street)
        if not street_num:
            continue

        site_hit = site_index.get((county.lower(), street_num))

        if site_hit is None:
            # Not found on site — flag if sale is within the check window
            days_until = (sale_date - today).days
            if 0 <= days_until <= CHECK_WINDOW_DAYS:
                flags.append({
                    "row_index": row_index,
                    "reason": (
                        f"Not found on Rubin Lublin site "
                        f"({days_until} day(s) until scheduled sale on {sale_date_str})"
                    ),
                })
            continue

        # Found — verify it's actually the same property (city cross-check)
        if not _addresses_match(
            site_hit["Street"], site_hit["City"],
            sheet_street, sheet_city,
        ):
            # Street number collision across different properties — treat as not found
            days_until = (sale_date - today).days
            if 0 <= days_until <= CHECK_WINDOW_DAYS:
                flags.append({
                    "row_index": row_index,
                    "reason": (
                        f"Street number {street_num} found on site but city mismatch "
                        f"(site: {site_hit['City']}, sheet: {sheet_city})"
                    ),
                })
            continue

        # Same property — check for date change (postponement)
        site_date = site_hit["Sale Date"]
        if site_date and site_date != sale_date_str:
            postponements.append({
                "row_index": row_index,
                "old_date":  sale_date_str,
                "new_date":  site_date,
            })

    return postponements, flags