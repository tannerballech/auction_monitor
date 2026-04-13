"""
scrapers/tn_trustees/clear_recon.py
Clear Recon LLC — TN Foreclosure Sale Scraper
https://clearrecon-tn.com

Clear Recon is a non-judicial foreclosure trustee operating statewide in TN.
They maintain a dedicated Tennessee listing site gated behind a disclaimer page.

--- Site architecture ---

  GET https://clearrecon-tn.com/tennessee-disclaimer/
      Disclaimer page. "Agree" link points to /tennessee-listings/.
      The listings page checks for a session cookie or Referer — if you
      navigate directly you get bounced back to the disclaimer.
      Strategy: use requests.Session to GET the disclaimer first (sets any
      server cookies), then GET listings with Referer header.
      Fallback: Playwright (headless=False) if requests is blocked.

  https://clearrecon-tn.com/tennessee-listings/
      WordPress page with a JavaScript-enhanced DataTables table.
      All rows are present in the initial HTML (client-side pagination) —
      no server-side pagination needed.

--- Table columns (confirmed from live data) ---

  TS Number | Address | Sale Date | Current Bid

  TS Number:   e.g. "1006-1749A"   (Clear Recon file number — use as case number)
  Address:     e.g. "55 Benzing Rd, Antioch TN, 37013"
               Format: "STREET, CITY STATE, ZIP"
  Sale Date:   e.g. "05/08/2026"  (MM/DD/YYYY — full 4-digit year)
  Current Bid: e.g. "$315,000.00" or "$0.00" or "View on Auction.com"
               "$0.00" = opening bid not yet set (treat as blank, not zero)
               "View on Auction.com" = sale hosted on auction.com (still valid)

--- Postponement ---

No explicit new-date column on this site. Absent-within-window logic applies:
if a sheet row's sale is within CHECK_WINDOW_DAYS and the property is gone
from the live listing page, flag for manual check (may be postponed or sold).

--- Two modes ---

  scrape_clear_recon(existing_addr_set, dry_run)
      Discovery: fetch listings, return new rows not already in sheet.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: fetch live listings, cross-reference sheet rows by address.
      Absent within CHECK_WINDOW_DAYS → manual-check flag.
      Returns ([], flags).  No postponement signal available.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

from scrapers.base import empty_listing

logger = logging.getLogger(__name__)

TRUSTEE           = "Clear Recon LLC"
STATE             = "TN"
SOURCE_URL        = "https://clearrecon-tn.com/"
DISCLAIMER_URL    = "https://clearrecon-tn.com/tennessee-disclaimer/"
LISTINGS_URL      = "https://clearrecon-tn.com/tennessee-listings/"
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
    """
    Fetch the listings page HTML using a requests.Session.
    Strategy: GET disclaimer first (sets session cookies), then GET
    listings with Referer header pointing to the disclaimer.
    Returns HTML string, or None on failure.
    """
    session = requests.Session()
    session.headers.update(_HEADERS)

    # Step 1: GET disclaimer page — establishes session cookies
    try:
        resp = session.get(DISCLAIMER_URL, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("[clear_recon] Disclaimer fetch failed: %s", e)
        return None

    # Step 2: GET listings with Referer set to disclaimer URL
    try:
        resp = session.get(
            LISTINGS_URL,
            timeout=20,
            headers={**_HEADERS, "Referer": DISCLAIMER_URL},
            allow_redirects=True,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("[clear_recon] Listings fetch failed: %s", e)
        return None

    # Check if we were bounced back to the disclaimer
    if "tennessee-disclaimer" in resp.url or "tennessee-disclaimer" in resp.text[:500]:
        logger.warning("[clear_recon] Redirected back to disclaimer — session cookie not accepted")
        return None

    logger.info("[clear_recon] Listings page fetched (%.1f KB)", len(resp.content) / 1024)
    return resp.text


def _fetch_html_playwright() -> Optional[str]:
    """
    Playwright fallback: navigate to disclaimer, click Agree, return listings HTML.
    Only called if requests approach fails.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("[clear_recon] Playwright not installed — cannot use fallback")
        return None

    html = None
    launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-sandbox",
    ]
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, args=launch_args)
        page = browser.new_page()
        page.set_extra_http_headers({"User-Agent": _HEADERS["User-Agent"]})
        try:
            page.goto(DISCLAIMER_URL, wait_until="domcontentloaded", timeout=30_000)
            # Click the "Agree" link — it's a plain <a> tag
            page.click("a[href*='tennessee-listings']", timeout=10_000)
            page.wait_for_url("**/tennessee-listings/**", timeout=15_000)
            page.wait_for_load_state("networkidle", timeout=15_000)
            html = page.content()
            logger.info("[clear_recon] Playwright: listings page loaded")
        except Exception as e:
            logger.error("[clear_recon] Playwright error: %s", e)
        finally:
            browser.close()

    return html


def _parse_address(raw: str) -> tuple[str, str, str]:
    """
    Parse "55 Benzing Rd, Antioch TN, 37013" → (street, city, zip).

    Format confirmed from live data: "STREET, CITY STATE, ZIP"
    The city and state are space-separated in the middle segment.
    State is always TN — we discard it.
    """
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) >= 3:
        street   = parts[0]
        # "Antioch TN" → city = "Antioch"
        city_state = parts[1].strip()
        m = re.match(r"^(.+?)\s+[A-Z]{2}$", city_state)
        city = m.group(1).strip() if m else city_state
        # Zip is the last segment (may have extra spaces)
        zip_code = re.sub(r"\D", "", parts[-1])[:5]
        return street, city, zip_code
    if len(parts) == 2:
        street = parts[0]
        # "Antioch TN 37013"
        m = re.match(r"^(.+?)\s+TN\s+(\d{5})", parts[1])
        if m:
            return street, m.group(1).strip(), m.group(2)
        return street, parts[1], ""
    return raw.strip(), "", ""


def _parse_date(raw: str) -> str:
    """'05/08/2026' → '2026-05-08'. Returns '' on failure."""
    raw = raw.strip()
    if not raw:
        return ""
    try:
        return datetime.strptime(raw, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        pass
    try:
        return datetime.strptime(raw, "%m/%d/%y").strftime("%Y-%m-%d")
    except ValueError:
        pass
    logger.debug("[clear_recon] Unparseable date: %r", raw)
    return ""


def _parse_bid(raw: str) -> str:
    """
    '$315,000.00' → '$315,000'
    '$0.00'        → ''   (opening bid not yet set)
    'View on Auction.com' → ''
    """
    raw = raw.strip()
    if not raw or raw.lower().startswith("view"):
        return ""
    m = re.match(r"\$\s*([\d,]+)(?:\.\d+)?", raw)
    if not m:
        return ""
    amount_str = m.group(1).replace(",", "")
    try:
        amount = int(amount_str)
    except ValueError:
        return ""
    return "" if amount == 0 else f"${amount:,}"


def _parse_table(html: str) -> list[dict]:
    """
    Parse the DataTables listing table from the page HTML.
    All rows are present in the initial HTML (client-side pagination).

    Returns list of dicts with keys:
      ts_number, street, city, zip, sale_date, bid
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the main data table — DataTables usually wraps in a <table> with
    # an id or class containing 'datatable', 'listing', or similar.
    # Fall back to the first table with 4+ columns if no id match.
    table = (
        soup.find("table", id=re.compile(r"datatable|listing|sale|auction", re.I))
        or soup.find("table", class_=re.compile(r"datatable|listing|sale|auction", re.I))
    )
    if not table:
        # Last resort: largest table on the page
        tables = soup.find_all("table")
        if not tables:
            logger.error("[clear_recon] No table found in HTML")
            return []
        table = max(tables, key=lambda t: len(t.find_all("tr")))

    # Identify column indices from header
    header_row = table.find("tr")
    if not header_row:
        logger.error("[clear_recon] No header row in table")
        return []

    headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]
    logger.debug("[clear_recon] Table headers: %s", headers)

    def _col(keywords: list[str], default: int) -> int:
        for i, h in enumerate(headers):
            if any(kw in h for kw in keywords):
                return i
        return default

    ts_idx   = _col(["ts", "number", "file"], 0)
    addr_idx = _col(["address", "addr"],      1)
    date_idx = _col(["date", "sale"],         2)
    bid_idx  = _col(["bid", "amount"],        3)

    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) < 3:
            continue

        def cell(idx: int) -> str:
            return cells[idx].get_text(separator=" ", strip=True) if idx < len(cells) else ""

        ts_number = cell(ts_idx)
        addr_raw  = cell(addr_idx)
        date_raw  = cell(date_idx)
        bid_raw   = cell(bid_idx)

        if not addr_raw or not date_raw:
            continue

        street, city, zip_code = _parse_address(addr_raw)
        sale_date = _parse_date(date_raw)
        bid       = _parse_bid(bid_raw)

        if not street or not sale_date:
            logger.debug("[clear_recon] Skipping unparseable row: %s / %s", addr_raw, date_raw)
            continue

        rows.append({
            "ts_number": ts_number,
            "street":    street,
            "city":      city,
            "zip":       zip_code,
            "sale_date": sale_date,
            "bid":       bid,
        })

    logger.info("[clear_recon] Parsed %d row(s) from table", len(rows))
    return rows


def _get_listings() -> list[dict]:
    """
    Fetch and parse all listings. Tries requests first, Playwright fallback.
    Returns list of parsed row dicts, or [] on failure.
    """
    html = _fetch_html()
    if not html:
        logger.warning("[clear_recon] requests approach failed — trying Playwright")
        html = _fetch_html_playwright()
    if not html:
        logger.error("[clear_recon] Both fetch strategies failed")
        return []
    return _parse_table(html)


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

def scrape_clear_recon(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        for all active TN rows — prevents cross-source duplicates.

    Returns (new_listings, {}).
    """
    site_rows = _get_listings()
    if not site_rows:
        return [], {}

    today_str = date.today().isoformat()
    new_listings: list[dict] = []
    stats = {"past": 0, "dup": 0}

    for row in site_rows:
        sale_date = row["sale_date"]

        if not sale_date or sale_date < today_str:
            stats["past"] += 1
            continue

        # We don't have a county in the listing data — derive from city
        # using the county lookup in base (Nominatim geocoding).
        # For now set county to "" and let sheets_writer gate on street number.
        # The street number + sale date cross-source dedup is state-wide for TN.
        street_num = _street_number(row["street"])

        # Check for duplicate using city as a proxy for county in the 3-tuple
        # (county_lower is "" for Clear Recon rows — match on street_num + sale_date only)
        is_dup = any(
            sn == street_num and sd == sale_date
            for (_, sn, sd) in existing_addr_set
        )
        if is_dup:
            stats["dup"] += 1
            continue

        # Attempt to extract county from city/zip via lookup
        county = _county_from_city(row["city"], row["zip"])

        listing = empty_listing(county=county, state="TN")
        listing.update({
            "Sale Date":              sale_date,
            "Case Number":            row["ts_number"],
            "Plaintiff":              "",
            "Defendant(s)":           "",
            "Street":                 row["street"],
            "City":                   row["city"],
            "Zip":                    row["zip"],
            "Appraised Value":        "",
            "Judgment / Loan Amount": row["bid"],
            "Attorney / Firm":        TRUSTEE,
            "Cancelled":              "",
            "Source URL":             LISTINGS_URL,
            "Notes":                  "",
        })

        new_listings.append(listing)
        if not dry_run:
            existing_addr_set.add(("", street_num, sale_date))

    logger.info(
        "[clear_recon] Discovery — new=%d past=%d dup=%d",
        len(new_listings), stats["past"], stats["dup"],
    )
    return new_listings, {}


def _county_from_city(city: str, zip_code: str) -> str:
    """
    County resolution from city name for TN.
    Built from all 60 cities confirmed in the live Clear Recon listing set.
    Falls back to "" → sheets_writer routes to Needs Review tab.
    """
    CITY_TO_COUNTY: dict[str, str] = {
        # Davidson
        "antioch":          "Davidson",
        "nashville":        "Davidson",
        "madison":          "Davidson",
        "old hickory":      "Davidson",
        "hermitage":        "Davidson",
        "joelton":          "Davidson",
        # Williamson
        "brentwood":        "Williamson",
        "franklin":         "Williamson",
        "spring hill":      "Williamson",
        "nolensville":      "Williamson",
        # Rutherford
        "murfreesboro":     "Rutherford",
        "smyrna":           "Rutherford",
        "la vergne":        "Rutherford",
        # Montgomery
        "clarksville":      "Montgomery",
        # Knox
        "knoxville":        "Knox",
        "powell":           "Knox",
        # Blount
        "maryville":        "Blount",
        "alcoa":            "Blount",
        "townsend":         "Blount",
        # Hamilton
        "chattanooga":      "Hamilton",
        "hixson":           "Hamilton",
        "east ridge":       "Hamilton",
        "signal mountain":  "Hamilton",
        # Shelby
        "memphis":          "Shelby",
        "bartlett":         "Shelby",
        "collierville":     "Shelby",
        "germantown":       "Shelby",
        "cordova":          "Shelby",
        "arlington":        "Shelby",
        "lakeland":         "Shelby",
        "millington":       "Shelby",
        # Madison
        "jackson":          "Madison",
        # Sumner
        "hendersonville":   "Sumner",
        "gallatin":         "Sumner",
        "portland":         "Sumner",
        "goodlettsville":   "Sumner",
        "white house":      "Sumner",
        # Maury
        "columbia":         "Maury",
        "spring hill":      "Maury",   # straddles Maury/Williamson; Maury zip wins most
        # Putnam
        "cookeville":       "Putnam",
        # White
        "sparta":           "White",
        # Cheatham
        "ashland city":     "Cheatham",
        "kingston springs": "Cheatham",
        # Dickson
        "charlotte":        "Dickson",
        "dickson":          "Dickson",
        "burns":            "Dickson",
        "white bluff":      "Dickson",
        # Stewart
        "dover":            "Stewart",
        # Marion
        "whitwell":         "Marion",
        # Roane
        "rockwood":         "Roane",
        "kingston":         "Roane",
        # Hawkins
        "mount carmel":     "Hawkins",
        "rogersville":      "Hawkins",
        # Monroe
        "madisonville":     "Monroe",
        # Hamblen
        "morristown":       "Hamblen",
        # Washington
        "johnson city":     "Washington",
        # Sullivan
        "kingsport":        "Sullivan",
        # Macon
        "lafayette":        "Macon",
        "red boiling spgs": "Macon",
        "westmoreland":     "Macon",
        # Trousdale
        "hartsville":       "Trousdale",
        # Fentress
        "jamestown":        "Fentress",
        # Jefferson
        "jefferson city":   "Jefferson",
        "jefferson cty":    "Jefferson",   # abbreviated form seen in Clear Recon data
        "dandridge":        "Jefferson",
        # Hickman
        "lyles":            "Hickman",
        "centerville":      "Hickman",
        # Claiborne
        "new tazewell":     "Claiborne",
        "tazewell":         "Claiborne",
        # Hardeman
        "whiteville":       "Hardeman",
        "bolivar":          "Hardeman",
        # Morgan
        "lancing":          "Morgan",
        "wartburg":         "Morgan",
        # Tipton
        "munford":          "Tipton",
        "mason":            "Tipton",
        "atoka":            "Tipton",
        "covington":        "Tipton",
        # Grainger
        "rutledge":         "Grainger",
        # DeKalb
        "smithville":       "DeKalb",
        # Chester
        "henderson":        "Chester",
        # Coffee
        "hillsboro":        "Coffee",
        "manchester":       "Coffee",
        "tullahoma":        "Coffee",
        # Anderson
        "clinton":          "Anderson",
        "oak ridge":        "Anderson",
        "norris":           "Anderson",
        # Bradley
        "cleveland":        "Bradley",
        # Wilson
        "lebanon":          "Wilson",
        "mt juliet":        "Wilson",
        "mount juliet":     "Wilson",
        # Robertson
        "springfield":      "Robertson",
        # Sevier
        "sevierville":      "Sevier",
        "pigeon forge":     "Sevier",
        "gatlinburg":       "Sevier",
        # Greene
        "greeneville":      "Greene",
        # Lawrence
        "lawrenceburg":     "Lawrence",
    }
    return CITY_TO_COUNTY.get(city.lower().strip(), "")


# ---------------------------------------------------------------------------
# Check mode
# ---------------------------------------------------------------------------

def check_existing(
    sheet_rows: list[dict],
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Check mode. Detect absent listings (potential postponements/cancellations).

    sheet_rows: Clear Recon rows from get_tn_listings_for_check().

    Absent within CHECK_WINDOW_DAYS → manual-check flag.
    No explicit postponement signal (no new-date column on this site).

    Returns ([], flags).
    """
    if not sheet_rows:
        return [], []

    site_rows = _get_listings()
    if not site_rows:
        logger.warning("[clear_recon] No rows returned — skipping check")
        return [], []

    today      = date.today()
    today_str  = today.isoformat()
    threshold  = (today + timedelta(days=CHECK_WINDOW_DAYS)).isoformat()
    flags: list[dict] = []

    for sheet_row in sheet_rows:
        sheet_street = sheet_row.get("Street", "")
        sheet_city   = sheet_row.get("City", "")
        sheet_date   = sheet_row.get("Sale Date", "")
        row_index    = sheet_row.get("row_index")

        if not sheet_street or not sheet_date:
            continue
        if sheet_date < today_str or sheet_date > threshold:
            continue

        # Look for a matching row on the live site
        found = any(
            _addresses_match(site_row["street"], site_row["city"],
                             sheet_street, sheet_city)
            for site_row in site_rows
        )

        if not found:
            days_out = (date.fromisoformat(sheet_date) - today).days
            logger.info(
                "[clear_recon] Not found on site: %s (%d day(s) out, row %s)",
                sheet_street, days_out, row_index,
            )
            flags.append({
                "row_index": row_index,
                "note": (
                    f"⚠️ Manual check — Not found on Clear Recon site "
                    f"({days_out} day(s) until scheduled sale on {sheet_date})"
                ),
            })

    logger.info(
        "[clear_recon] Check — %d manual-check flag(s) from %d sheet row(s)",
        len(flags), len(sheet_rows),
    )
    return [], flags