"""
internetpostings.com — Foundation Legal Group, LLP
https://www.internetpostings.com/

Platform operated by Attorney's Title Group LLC.  Every TN listing on this site
is Foundation Legal Group, LLP (or predecessor Wilson & Associates, P.L.L.C.).
Trustee name is hardcoded — no PDF fetch needed.

Two modes:

  scrape_internetpostings(existing_addr_set, dry_run)
      Discovery: navigate ToS, parse TN listing table, return new listings
      not already in the sheet.  Trustee hardcoded as Foundation Legal Group, LLP.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check mode: same ToS + parse, cross-reference ALL active TN sheet rows
      against the site.  Detects postponements via populated "New Sale Date" column.
      No "absent = flag" logic — we don't have full firm coverage.
      Returns (postponements, []).
"""

from __future__ import annotations
import logging
import re
from datetime import date, datetime

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from scrapers.base import empty_listing

logger = logging.getLogger(__name__)

SOURCE  = "https://www.internetpostings.com/"
STATE   = "TN"
TRUSTEE = "Foundation Legal Group, LLP"

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
# ToS navigation
# ---------------------------------------------------------------------------

def _tos_flow(page) -> bool:
    """
    Navigate to internetpostings.com and complete the ToS flow:
      1. Scroll every overflowing element to its bottom (catches the ToS div)
      2. Also scroll the window to the bottom
      3. Click the checkbox
      4. Click "View Property Listings"

    Returns True on success, False if any step fails.
    """
    try:
        page.goto(SOURCE, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(2_000)

        # Step 1 — scroll all overflow containers (catches ToS div regardless of selector)
        page.evaluate("""
            Array.from(document.querySelectorAll('*')).forEach(el => {
                const s = window.getComputedStyle(el);
                const ov = s.overflow + s.overflowY;
                if ((ov.includes('scroll') || ov.includes('auto'))
                        && el.scrollHeight > el.clientHeight + 10) {
                    el.scrollTop = el.scrollHeight;
                }
            });
        """)
        # Step 2 — also scroll the window itself
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_500)

        # Step 3 — click the checkbox
        checkbox = page.locator("input[type='checkbox']").first
        checkbox.wait_for(state="visible", timeout=10_000)
        checkbox.click()
        page.wait_for_timeout(1_500)

        # Step 4 — click "View Property Listings" (text match, case-insensitive)
        btn = page.get_by_text(re.compile(r"view property listings", re.I)).first
        btn.wait_for(state="visible", timeout=10_000)
        btn.click()
        page.wait_for_load_state("domcontentloaded", timeout=20_000)
        page.wait_for_timeout(2_000)

        return True

    except Exception as e:
        logger.error("[internetpostings] ToS navigation failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Table parsing
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> str:
    """'4/13/2026 10:00 AM' or '04/16/2026' → '2026-04-16'. Returns '' on failure."""
    raw = raw.strip()
    if not raw or raw == "--":
        return ""
    raw = raw.split()[0]   # strip time component
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _parse_table(html: str) -> list[dict]:
    """
    Parse the listing table from the page HTML.
    Returns a list of row dicts for TN rows only, each containing:
      Street, City, County, State, Zip,
      Original Sale Date (YYYY-MM-DD), New Sale Date (YYYY-MM-DD or '').

    The site uses onclick="window.open('Document.ashx?...')" — there is no href.
    PDF URL extraction removed entirely; trustee is hardcoded.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        logger.warning("[internetpostings] No listing table found in page HTML")
        return []

    all_rows = table.find_all("tr")
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

        def cell(idx: int) -> str:
            return cells[idx].get_text(strip=True) if idx < len(cells) else ""

        state = cell(state_idx).upper()
        if state != "TN":
            continue

        orig_date = _parse_date(cell(orig_idx))
        new_date  = _parse_date(cell(new_idx))

        if not orig_date:
            continue

        rows.append({
            "Street":             cell(addr_idx),
            "City":               cell(city_idx),
            "County":             cell(county_idx),
            "State":              state,
            "Zip":                cell(zip_idx),
            "Original Sale Date": orig_date,
            "New Sale Date":      new_date,
        })

    logger.info("[internetpostings] Parsed %d TN row(s) from table", len(rows))
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
# Core Playwright session — shared by both modes
# ---------------------------------------------------------------------------

def _run_playwright_session(callback):
    """
    Launch Playwright, navigate through the ToS, then call callback(page, context).
    Returns whatever callback returns, or None on failure.
    """
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
                return None
            return callback(page, context)
        finally:
            browser.close()


# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def scrape_internetpostings(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.  Navigate ToS, parse TN listing table.
    Trustee hardcoded as Foundation Legal Group, LLP — no PDF fetch needed.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        covering all active TN rows — used to avoid duplicating TNLedger rows.

    Returns (new_listings, {}).
    """

    def _session(page, _context):
        html = page.content()
        site_rows = _parse_table(html)

        today = date.today()
        new_listings = []

        for site in site_rows:
            orig_date = site["Original Sale Date"]
            county    = site["County"]
            street    = site["Street"]

            # Gate 3 equivalent — skip if fewer than 3 days out
            try:
                days_out = (datetime.strptime(orig_date, "%Y-%m-%d").date() - today).days
            except ValueError:
                continue
            if days_out < 3:
                continue

            # Address-based dedup against existing TN rows
            addr_key = (county.lower(), _street_number(street), orig_date)
            if addr_key in existing_addr_set:
                continue

            listing = empty_listing(county or STATE, STATE)
            listing["Case Number"]            = ""
            listing["Attorney / Firm"]        = TRUSTEE
            listing["Sale Date"]              = orig_date
            listing["Street"]                 = street
            listing["City"]                   = site["City"]
            listing["Zip"]                    = site["Zip"]
            listing["Judgment / Loan Amount"] = ""
            listing["Source URL"]             = SOURCE

            new_listings.append(listing)

        logger.info("[internetpostings] %d new listing(s) after dedup", len(new_listings))
        return new_listings

    result = _run_playwright_session(_session)
    return (result or []), {}


# ---------------------------------------------------------------------------
# Check mode
# ---------------------------------------------------------------------------

def check_existing(
    sheet_rows: list[dict],
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Check mode.  Navigate ToS, parse TN listing table, cross-reference ALL
    active TN sheet rows against the site.

    Non-empty "New Sale Date" on the site = explicit postponement signal.
    No "not found = flag" logic — we don't have full firm coverage.

    Returns (postponements, []).
    """

    def _session(page, _context):
        html = page.content()
        site_rows = _parse_table(html)

        # Index site rows by (county_lower, street_number) → site row.
        # Multiple rows could share a street number in different counties,
        # so the key includes county.
        site_index: dict[tuple, dict] = {}
        for s in site_rows:
            key = (s["County"].lower(), _street_number(s["Street"]))
            if key and key not in site_index:
                site_index[key] = s

        postponements: list[dict] = []

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
                continue  # not found — no signal (we don't have full firm coverage)

            # Verify same property (city + street name check)
            if not _addresses_match(
                site_hit["Street"], site_hit["City"],
                sheet_street, sheet_city,
            ):
                continue

            # Explicit postponement: New Sale Date is populated and different
            new_date = site_hit.get("New Sale Date", "")
            if new_date and new_date != sale_date_str:
                postponements.append({
                    "row_index": row_index,
                    "old_date":  sale_date_str,
                    "new_date":  new_date,
                })

        return postponements

    result = _run_playwright_session(_session)
    return (result or []), []