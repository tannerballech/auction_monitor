"""
Floyd County IN — Sheriff Sales
Source: https://www.fcsdin.com/sheriffsales/
Format: JS-rendered WordPress table. Requires Playwright (headless=False).

Table schema: DATE | Address | City | State | Zip Code | Status
- Month-name rows (JANUARY, FEBRUARY...) span the table — skipped.
- "*NO SALES*" rows — skipped.
- Date rows: cells[1] = "February 12, 2026", remaining cells blank.
- Property rows: cells[1]=address, cells[2]=city, cells[3]=state,
  cells[4]=zip — assigned directly to Street/City/Zip.

No case numbers available on this source.
No cancellation data — listings disappear when removed; Status col is blank.
Dedup relies entirely on sheets_writer Gate 4 (street_number + sale_date + "").
"""

import re
import logging
from datetime import datetime

from playwright.sync_api import sync_playwright

from .base import empty_listing

COUNTY = "Floyd"
STATE = "IN"
URL = "https://www.fcsdin.com/sheriffsales/"
SOURCE_URL = URL

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
]

_MONTH_NAMES = {
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
}


def _parse_full_date(text):
    """Parse 'February 12, 2026' → '2026-02-12', or None."""
    text = text.strip()
    for fmt in ("%B %d, %Y", "%B %d %Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def _is_month_header(cells):
    if len(cells) < 2:
        return False
    first = re.sub(r'[*\s]', '', cells[1]).lower()
    return first in _MONTH_NAMES


def _is_no_sales(cells):
    return any("no sales" in c.lower() for c in cells)


def _cells_are_header(cells):
    joined = " ".join(cells).lower()
    return "address" in joined and "zip" in joined


def _extract_property(cells, current_date):
    """
    cells[0] always blank (DATE colspan).
    cells[1]=street, cells[2]=city, cells[3]=state, cells[4]=zip
    Returns (street, city, zip_code) or None if not a property row.
    """
    if not current_date or len(cells) < 5:
        return None
    street = cells[1].strip()
    if not re.match(r'^\d+', street):
        return None
    city     = cells[2].strip() if len(cells) > 2 else ""
    zip_code = cells[4].strip() if len(cells) > 4 else ""
    return street, city, zip_code


def scrape_floyd_in(existing=None, dry_run=False):
    """
    Scrape Floyd County IN sheriff sales.
    Returns (new_listings, cancellation_updates).
    cancellation_updates is always empty — not tracked by this source.
    On --dry-run, skips Playwright entirely and returns ([], {}).
    """
    existing = existing or {}

    if dry_run:
        logger.info("[Floyd IN] dry_run — skipping Playwright fetch")
        return [], {}

    listings = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=_LAUNCH_ARGS)
        context = browser.new_context(
            user_agent=_USER_AGENT,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        try:
            page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        except Exception:
            logger.warning("[Floyd IN] goto timed out on domcontentloaded — proceeding anyway")

        try:
            page.wait_for_function(
                "() => !document.body.innerText.includes('Loading')",
                timeout=20000,
            )
        except Exception:
            logger.warning("[Floyd IN] Timed out waiting for table; proceeding anyway")

        page.wait_for_timeout(2500)

        rows = page.query_selector_all("table tr")
        logger.info(f"[Floyd IN] Found {len(rows)} table row(s)")

        current_sale_date = None

        for row in rows:
            cells = [
                el.inner_text().strip()
                for el in row.query_selector_all("td, th")
            ]
            cells = [re.sub(r'\s+', ' ', c).strip() for c in cells]

            if not cells or all(not c for c in cells):
                continue
            if _cells_are_header(cells):
                continue
            if _is_month_header(cells):
                continue
            if _is_no_sales(cells):
                continue

            # Date row: cells[1] looks like "February 12, 2026"
            parsed_date = _parse_full_date(cells[1]) if len(cells) > 1 else None
            if parsed_date:
                current_sale_date = parsed_date
                continue

            # Property row — cells already split into street / city / zip
            result = _extract_property(cells, current_sale_date)
            if result is None:
                continue

            street, city, zip_code = result

            cancelled = any(
                "cancel" in c.lower() or "withdrawn" in c.lower()
                for c in cells
            )

            listing = empty_listing(COUNTY, STATE)
            listing["Sale Date"]  = current_sale_date
            listing["Street"]     = street
            listing["City"]       = city
            listing["Zip"]        = zip_code
            listing["Source URL"] = SOURCE_URL
            if cancelled:
                listing["Cancelled"] = "Yes"

            listings.append(listing)

        browser.close()

    logger.info(f"[Floyd IN] Found {len(listings)} listing(s)")
    return listings, {}