"""
scrapers/tn_trustees/capital_city_postings.py
Padgett Law Group — TN Foreclosure Sale Scraper
https://capitalcitypostings.com/tennessee-postings

Capital City Posting is a third-party internet posting company used by
Padgett Law Group (PLG) and potentially other TN trustees for statutory
internet posting of foreclosure sales.

Table columns:
  NOS (link) | County | Sale Date | Property Address | Postponed | Client

Key logic:
  - Filter Client == "PLG" for Padgett rows (other firms also use this platform)
  - Rows with blank "Postponed" = active upcoming sale
  - Rows marked "Postponed" (or "Posponed" — site typo) = prior posting,
    superseded by a new row with a later date and blank Postponed
  - Sale Date is already ISO format (YYYY-MM-DD)
  - Address format: "2200 South Parkway East, Memphis, TN 38114"

Note: Other clients (e.g. "Allen, Nelson & Bowers") also post here.
If additional trustee firms are identified using this platform, add their
Client column values to _TARGET_CLIENTS and registry entries accordingly.

Two modes:

  scrape_padgett(existing_addr_set, dry_run)
      Discovery: fetch table, filter PLG + active rows, return new listings.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: same fetch, cross-reference Padgett sheet rows.
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

TRUSTEE           = "Padgett Law Group"
STATE             = "TN"
SOURCE_URL        = "https://capitalcitypostings.com/tennessee-postings"
CHECK_WINDOW_DAYS = 14

# Client column values to capture — add others if additional firms are found
_TARGET_CLIENTS = {"PLG"}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Matches "Postponed" and the site's typo "Posponed"
_POSTPONED_RE = re.compile(r"pos[t]?poned", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------

def _parse_address(raw: str) -> tuple[str, str, str]:
    """
    '2200 South Parkway East, Memphis, TN 38114' → (street, city, zip).
    Splits on comma: [street, city, state_zip].
    State is always TN — skip it.
    """
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) >= 3:
        street = parts[0]
        city   = parts[1]
        # parts[2] is "TN 38114" — extract zip
        m = re.search(r"(\d{5})", parts[2])
        zip_code = m.group(1) if m else ""
        return street, city, zip_code
    if len(parts) == 2:
        street = parts[0]
        # "Memphis TN 38114"
        m = re.search(r"^(.+?)\s+TN\s+(\d{5})", parts[1])
        if m:
            return street, m.group(1).strip(), m.group(2)
        return street, parts[1], ""
    return raw.strip(), "", ""


def _fetch_listings() -> list[dict]:
    """
    Fetch the Capital City Postings TN table and return all parsed rows.
    Includes both active and postponed rows — caller filters as needed.

    Returns list of dicts:
      Sale Date (YYYY-MM-DD), County, Street, City, Zip, State,
      Postponed (bool), Client (str)
    """
    try:
        resp = requests.get(SOURCE_URL, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("[capital_city_postings] Fetch failed: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if not table:
        logger.warning("[capital_city_postings] No table found on page")
        return []

    # Identify column indices from header row
    headers_row = table.find("tr")
    if not headers_row:
        return []
    headers = [th.get_text(strip=True).lower() for th in headers_row.find_all(["th", "td"])]

    def _col(keyword: str, default: int) -> int:
        for i, h in enumerate(headers):
            if keyword in h:
                return i
        return default

    county_idx    = _col("county",    1)
    date_idx      = _col("sale date", 2)
    addr_idx      = _col("address",   3)
    postponed_idx = _col("postponed", 4)
    client_idx    = _col("client",    5)

    rows = []
    for tr in table.find_all("tr")[1:]:   # skip header
        cells = tr.find_all(["td", "th"])
        if len(cells) < max(county_idx, date_idx, addr_idx) + 1:
            continue

        def cell(idx: int) -> str:
            return cells[idx].get_text(strip=True) if idx < len(cells) else ""

        county_raw    = cell(county_idx)
        sale_date_raw = cell(date_idx)
        addr_raw      = cell(addr_idx)
        postponed_raw = cell(postponed_idx)
        client_raw    = cell(client_idx).strip()

        # County field often includes "County" suffix — strip it
        county = re.sub(r"\s+County\s*$", "", county_raw, flags=re.IGNORECASE).strip()

        # Sale date is already ISO — validate format
        if not re.match(r"\d{4}-\d{2}-\d{2}", sale_date_raw):
            logger.debug(
                "[capital_city_postings] Unexpected date format %r — skipping",
                sale_date_raw,
            )
            continue

        street, city, zip_code = _parse_address(addr_raw)
        if not street:
            logger.debug(
                "[capital_city_postings] Unparseable address %r — skipping", addr_raw
            )
            continue

        is_postponed = bool(_POSTPONED_RE.search(postponed_raw))

        rows.append({
            "Sale Date":  sale_date_raw,
            "County":     county,
            "Street":     street,
            "City":       city,
            "Zip":        zip_code,
            "State":      STATE,
            "Postponed":  is_postponed,
            "Client":     client_raw,
        })

    logger.info("[capital_city_postings] Fetched %d total row(s)", len(rows))
    return rows


def _active_plg_rows(all_rows: list[dict]) -> list[dict]:
    """
    Filter to PLG rows with blank Postponed column and a future sale date.
    These represent currently active upcoming sales.
    """
    today_str = date.today().isoformat()
    return [
        r for r in all_rows
        if r["Client"] in _TARGET_CLIENTS
        and not r["Postponed"]
        and r["Sale Date"] >= today_str
    ]


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

def scrape_padgett(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.  Fetch Capital City Postings TN table, filter to active
    PLG rows, return listings not already in the sheet.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        covering all active TN rows — prevents duplicating TNLedger rows.

    Returns (new_listings, {}).
    """
    all_rows  = _fetch_listings()
    site_rows = _active_plg_rows(all_rows)
    logger.info("[capital_city_postings] %d active PLG row(s)", len(site_rows))

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

    logger.info(
        "[capital_city_postings] %d new listing(s) after dedup", len(new_listings)
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
    Check mode.  Fetch Capital City Postings TN table, cross-reference
    Padgett sheet rows.

    Active site row = Client is PLG + Postponed is blank.
    Postponement: sheet row address found on site with a different date.
    Flag: sheet row absent from site AND sale within CHECK_WINDOW_DAYS.

    Returns (postponements, flags).
    """
    all_rows  = _fetch_listings()
    site_rows = _active_plg_rows(all_rows)

    # Index active rows by (county_lower, street_number) → site row
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
                        f"Not found on Capital City Postings "
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