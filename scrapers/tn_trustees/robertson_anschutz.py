"""
scrapers/tn_trustees/robertson_anschutz.py
Robertson, Anschutz, Schneid, Crane & Partners, PLLC — TN Foreclosure Sale Scraper
https://www.rascranesalesinfo.com/

Flow:
  1. Navigate to /TN-Disclaimer
  2. Click "I Agree with Disclaimer" label + "View Sales" button (ASP.NET postback)
  3. Land on /TN-Sales — paginated table, 15 rows/page, up to 5 pages
  4. Advance pages via the Telerik RadSlider "Increase" button

Table columns (fixed order, no visible header row):
  File Number | Sale Scheduled Date | County | Property Address | Bid Amount | Sale Crier

Address format: "7524 WOODSTREAM DR, NASHVILLE, TN, 37221"
Bid Amount:     "$377226.41" | "$0.00" | "N/A" — store when > $0

Two modes:

  scrape_robertson_anschutz(existing_addr_set, dry_run)
      Discovery: navigate disclaimer, scrape all pages, return new listings.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: same scrape, cross-reference RASC sheet rows.
      Same property, different date → postponement.
      Absent within CHECK_WINDOW_DAYS → manual-check flag.
      Returns (postponements, flags).
"""

from __future__ import annotations
import logging
import re
from datetime import date, datetime

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from scrapers.base import empty_listing

logger = logging.getLogger(__name__)

TRUSTEE           = "Robertson, Anschutz, Schneid, Crane & Partners, PLLC"
STATE             = "TN"
DISCLAIMER_URL    = "https://www.rascranesalesinfo.com/TN-Disclaimer"
SALES_URL         = "https://www.rascranesalesinfo.com/TN-Sales"
SOURCE_URL        = SALES_URL
TABLE_ID          = "ctl00_MainContent_radActivateSales_ctl00"
CHECK_WINDOW_DAYS = 14

_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
]
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Disclaimer flow
# ---------------------------------------------------------------------------

def _accept_disclaimer(page) -> bool:
    """
    Navigate to /TN-Disclaimer, check the agreement, click View Sales.
    Waits for navigation to /TN-Sales.
    Returns True on success, False on failure.
    """
    try:
        page.goto(DISCLAIMER_URL, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(3_000)

        # Click the label (covers the checkbox — must target label not input)
        page.click("label[for='cbDisclaimerAgreement']")
        page.wait_for_timeout(1_500)

        # Click "View Sales" anchor (fires __doPostBack)
        page.click("#MainContent_btnOk")

        # Wait for navigation to TN-Sales
        page.wait_for_url("**/TN-Sales**", timeout=20_000)
        page.wait_for_timeout(3_000)

        return True

    except Exception as e:
        logger.error("[robertson_anschutz] Disclaimer flow failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Table parsing
# ---------------------------------------------------------------------------

def _parse_sale_date(raw: str) -> str:
    """'5/28/2026' → '2026-05-28'. Returns '' on failure."""
    try:
        return datetime.strptime(raw.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _parse_address(raw: str) -> tuple[str, str, str]:
    """
    '7524 WOODSTREAM DR, NASHVILLE, TN, 37221' → (street, city, zip).
    Handles occasional extra spaces around commas.
    Returns ('', '', '') on failure.
    """
    parts = [p.strip() for p in raw.split(",")]
    # Expected: [street, city, state_abbr, zip]
    # State is always TN — skip it
    if len(parts) >= 4:
        return parts[0], parts[1], parts[3]
    if len(parts) == 3:
        # Missing state: [street, city, zip]
        return parts[0], parts[1], parts[2]
    return raw.strip(), "", ""


def _parse_bid(raw: str) -> str:
    """
    '$377226.41' → '$377,226.41' normalized string.
    '$0.00' or 'N/A' → '' (not meaningful).
    """
    raw = raw.strip()
    if not raw or raw == "N/A":
        return ""
    # Strip dollar sign and parse
    try:
        amount = float(raw.replace("$", "").replace(",", ""))
        if amount <= 0:
            return ""
        return f"${amount:,.2f}"
    except ValueError:
        return ""


def _parse_table_html(html: str) -> list[dict]:
    """
    Parse all data rows from the RASC TN sales table.
    Returns list of dicts with: Sale Date, Case Number, Street, City, Zip,
    County, State, Judgment / Loan Amount.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id=TABLE_ID)
    if not table:
        logger.warning("[robertson_anschutz] Could not find main table in HTML")
        return []

    rows = []
    for tr in table.find_all("tr", class_=["rgRow", "rgAltRow"]):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 4:
            continue

        file_num   = cells[0]
        date_raw   = cells[1]
        county     = cells[2]
        addr_raw   = cells[3]
        bid_raw    = cells[4] if len(cells) > 4 else ""

        sale_date = _parse_sale_date(date_raw)
        if not sale_date:
            logger.debug("[robertson_anschutz] Unparseable date %r — skipping", date_raw)
            continue

        street, city, zip_code = _parse_address(addr_raw)
        if not street:
            logger.debug("[robertson_anschutz] Unparseable address %r — skipping", addr_raw)
            continue

        rows.append({
            "Sale Date":              sale_date,
            "Case Number":            file_num,
            "Street":                 street,
            "City":                   city,
            "Zip":                    zip_code,
            "County":                 county,
            "State":                  STATE,
            "Judgment / Loan Amount": _parse_bid(bid_raw),
        })

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
# Core Playwright session — scrape all pages
# ---------------------------------------------------------------------------

def _scrape_all_pages(page) -> list[dict]:
    """
    Parse all paginated pages of the TN-Sales table.
    Advances pages by clicking the RadSlider "Increase" button.
    Returns combined list of row dicts across all pages.
    """
    all_rows: list[dict] = []
    page_num = 1

    while True:
        # Wait for the table to be present before reading content
        try:
            page.wait_for_selector(f"#{TABLE_ID}", timeout=15_000)
        except Exception:
            logger.warning("[robertson_anschutz] Table not found on page %d", page_num)
            break

        html = page.content()
        rows = _parse_table_html(html)
        logger.info(
            "[robertson_anschutz] Page %d: %d row(s)", page_num, len(rows)
        )
        all_rows.extend(rows)

        # Check current/total page from label
        label = page.query_selector(".rgSliderLabel")
        if label:
            label_text = label.inner_text()
            m = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", label_text)
            if m:
                current = int(m.group(1))
                total   = int(m.group(2))
                if current >= total:
                    break

        # Advance to next page
        increase_btn = page.query_selector(".rslIncrease")
        if not increase_btn:
            break

        increase_btn.click()

        # Wait for table to update — row count or page label changes
        page.wait_for_timeout(3_000)
        page_num += 1

        if page_num > 20:
            logger.warning("[robertson_anschutz] Hit page cap — stopping pagination")
            break

    logger.info("[robertson_anschutz] Total rows across all pages: %d", len(all_rows))
    return all_rows
# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def scrape_robertson_anschutz(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.  Navigate disclaimer, scrape all TN-Sales pages, return
    listings not already in the sheet.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        covering all active TN rows — prevents duplicating TNLedger rows.

    Returns (new_listings, {}).
    """
    def _session(page):
        if not _accept_disclaimer(page):
            return []

        site_rows = _scrape_all_pages(page)
        today = date.today()
        new_listings = []

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
            listing["Case Number"]            = site["Case Number"]
            listing["Attorney / Firm"]        = TRUSTEE
            listing["Sale Date"]              = orig_date
            listing["Street"]                 = street
            listing["City"]                   = site["City"]
            listing["Zip"]                    = site["Zip"]
            listing["Judgment / Loan Amount"] = site["Judgment / Loan Amount"]
            listing["Source URL"]             = SOURCE_URL

            new_listings.append(listing)

        logger.info(
            "[robertson_anschutz] %d new listing(s) after dedup", len(new_listings)
        )
        return new_listings

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=_LAUNCH_ARGS)
        context = browser.new_context(
            user_agent=_USER_AGENT,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()
        try:
            result = _session(page)
        finally:
            browser.close()

    return (result or []), {}


# ---------------------------------------------------------------------------
# Check mode
# ---------------------------------------------------------------------------

def check_existing(
    sheet_rows: list[dict],
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Check mode.  Navigate disclaimer, scrape all TN-Sales pages, cross-reference
    Robertson Anschutz sheet rows.

    Postponement: listing present on site with a different sale date.
    Flag:         listing absent AND sale is within CHECK_WINDOW_DAYS.

    Returns (postponements, flags).
    """
    def _session(page):
        if not _accept_disclaimer(page):
            return [], []

        site_rows = _scrape_all_pages(page)

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
                            f"Not found on RASC site "
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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=_LAUNCH_ARGS)
        context = browser.new_context(
            user_agent=_USER_AGENT,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()
        try:
            result = _session(page)
        finally:
            browser.close()

    return result if result else ([], [])