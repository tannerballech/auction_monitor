"""
scrapers/tn_trustees/mickel_law.py
Mickel Law Firm, P.A. — TN Foreclosure Sale Scraper
https://trustee-foreclosuresalesonline.com/

Third-party internet posting platform referenced in Mickel Law Firm notices:
"This sale can be viewed at www.trustee-foreclosuresalesonline.com"

ATTRIBUTION ASSUMPTION: All TN rows on this site are attributed to Mickel Law
Firm, P.A. This assumption holds as long as Mickel is the only firm posting
here. If other firms are discovered using this platform, add a firm identifier
column mapping or switch to check-mode-only. The AUCTIONEER column contains
the auction company (Auction.com, ServiceLink, etc.) not the trustee firm —
it cannot be used to distinguish firms.

Page structure:
  Single page, fully server-rendered. All TN rows returned in one request.
  No JS or pagination needed — client-side JS handles the "50 rows + paginate"
  display, but all rows are in the HTML.

Table columns (TN section):
  DATE | TIME | PRIOR SALE DATE | ADDRESS | CITY | COUNTY | STATE | ZIP |
  LOCATION | AUCTIONEER

Postponement logic:
  PRIOR SALE DATE is populated when a sale was previously postponed.
  For discovery, we take the current DATE as the active sale date regardless.
  For check mode, a sheet row's date differing from the site row's DATE = postponement.

Two modes:

  scrape_mickel(existing_addr_set, dry_run)
      Discovery: fetch TN table, return new listings not already in sheet.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: same fetch, cross-reference Mickel sheet rows.
      Same property, different date → postponement.
      Absent within CHECK_WINDOW_DAYS → manual-check flag.
      Returns (postponements, flags).
"""

from __future__ import annotations
import logging
import re
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

from scrapers.base import empty_listing

logger = logging.getLogger(__name__)

TRUSTEE           = "Mickel Law Firm, P.A."
STATE             = "TN"
SOURCE_URL        = "https://trustee-foreclosuresalesonline.com/"
CHECK_WINDOW_DAYS = 14

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------

def _parse_sale_date(raw: str) -> str:
    """'11/20/2025' → '2025-11-20'. Returns '' on failure."""
    raw = raw.strip()
    if not raw:
        return ""
    try:
        return datetime.strptime(raw, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _fetch_tn_listings() -> list[dict]:
    """
    Fetch the page and parse the TN table section.
    All TN rows are present in the server-rendered HTML regardless of the
    client-side 50-row display limit.

    Returns list of dicts:
      Sale Date, Street, City, County, State, Zip, Prior Sale Date (str or '')
    """
    try:
        resp = requests.get(SOURCE_URL, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("[mickel_law] Fetch failed: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the Tennessee heading then grab the next table after it
    tn_heading = soup.find(
        lambda tag: tag.name in ["h1", "h2", "h3", "h4"]
        and "Tennessee" in tag.get_text()
    )
    if not tn_heading:
        logger.warning("[mickel_law] Could not find Tennessee section heading")
        return []

    tn_table = tn_heading.find_next("table")
    if not tn_table:
        logger.warning("[mickel_law] No table found after Tennessee heading")
        return []

    all_rows = tn_table.find_all("tr")
    if not all_rows:
        return []

    # Parse header row to find column indices robustly
    header_cells = all_rows[0].find_all(["th", "td"])
    headers = [c.get_text(strip=True).lower() for c in header_cells]

    def _col(keyword: str, default: int) -> int:
        for i, h in enumerate(headers):
            if keyword in h:
                return i
        return default

    date_idx       = _col("date",     0)
    prior_date_idx = _col("prior",    2)
    addr_idx       = _col("address",  3)
    city_idx       = _col("city",     4)
    county_idx     = _col("county",   5)
    state_idx      = _col("state",    6)
    zip_idx        = _col("zip",      7)

    rows = []
    for tr in all_rows[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) < max(date_idx, addr_idx, county_idx) + 1:
            continue

        def cell(idx: int) -> str:
            return cells[idx].get_text(strip=True) if idx < len(cells) else ""

        state = cell(state_idx).upper()
        if state != "TN":
            continue

        sale_date = _parse_sale_date(cell(date_idx))
        if not sale_date:
            logger.debug("[mickel_law] Unparseable date %r — skipping", cell(date_idx))
            continue

        street  = cell(addr_idx).strip()
        city    = cell(city_idx).strip()
        county  = cell(county_idx).strip()
        zip_code = cell(zip_idx).strip()

        if not street or not county:
            logger.debug("[mickel_law] Missing street or county — skipping: %r", cells)
            continue

        prior_date = _parse_sale_date(cell(prior_date_idx))

        rows.append({
            "Sale Date":       sale_date,
            "Street":          street,
            "City":            city,
            "County":          county,
            "State":           STATE,
            "Zip":             zip_code,
            "Prior Sale Date": prior_date,
        })

    logger.info("[mickel_law] Fetched %d TN row(s)", len(rows))
    return rows


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

def scrape_mickel(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.  Fetch TN listings and return new ones not in the sheet.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        covering all active TN rows — prevents duplicating TNLedger rows.

    Returns (new_listings, {}).
    """
    site_rows = _fetch_tn_listings()
    today = date.today()
    new_listings: list[dict] = []

    for site in site_rows:
        orig_date = site["Sale Date"]
        county    = site["County"]
        street    = site["Street"]

        # Gate 3 — skip if fewer than 3 days out
        try:
            days_out = (
                datetime.strptime(orig_date, "%Y-%m-%d").date() - today
            ).days
        except ValueError:
            continue
        if days_out < 3:
            continue

        # Address-based dedup against all existing TN rows
        addr_key = (county.lower(), _street_number(street), orig_date)
        if addr_key in existing_addr_set:
            continue

        listing = empty_listing(county, STATE)
        listing["Case Number"]            = ""
        listing["Attorney / Firm"]        = TRUSTEE
        listing["Sale Date"]              = orig_date
        listing["Street"]                 = street
        listing["City"]                   = site["City"]
        listing["Zip"]                    = site["Zip"]
        listing["Judgment / Loan Amount"] = ""
        listing["Source URL"]             = SOURCE_URL

        new_listings.append(listing)

    logger.info("[mickel_law] %d new listing(s) after dedup", len(new_listings))
    return new_listings, {}


# ---------------------------------------------------------------------------
# Check mode
# ---------------------------------------------------------------------------

def check_existing(
    sheet_rows: list[dict],
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Check mode.  Fetch TN listings and cross-reference Mickel sheet rows.

    Postponement: sheet row address found on site with a different sale date.
      The PRIOR SALE DATE column confirms the old date was postponed.
    Flag: sheet row absent AND sale within CHECK_WINDOW_DAYS.

    Returns (postponements, flags).
    """
    site_rows = _fetch_tn_listings()

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
                        f"Not found on trustee-foreclosuresalesonline.com "
                        f"({days_out} day(s) until scheduled sale on {sale_date_str})"
                    ),
                })
            continue

        if not _addresses_match(
            site_hit["Street"], site_hit["City"],
            sheet_street, sheet_city,
        ):
            continue

        new_date = site_hit["Sale Date"]
        if new_date != sale_date_str:
            postponements.append({
                "row_index": row_index,
                "old_date":  sale_date_str,
                "new_date":  new_date,
            })

    return postponements, flags