"""
Clark County IN — Sheriff Sales
Source: https://clarkcosheriff.com/sheriff-sales
Format: Plain text, date headers (MAR/17), then address lines.
No case numbers, plaintiff/defendant, or judgment amounts available.
Dedup relies entirely on sheets_writer Gate 4 (street_number + sale_date + "").

Cancellation format: the site appends ***C A N C E L L E D*** (or similar)
to the city name after the comma, e.g.:
  "124  W CHESTNUT ST., JEFFERSONVILLE***C A N C E L L E D***"
The scraper strips this marker, flags the listing as Cancelled = "Yes",
and still writes it so sheets_writer can update any existing row.

City is present in source ("518 EASTSIDE AVE., SELLERSBURG").
Zip is not available from this source — Nominatim geocode_address() is used
to look it up after parsing. State is always IN.
"""

import re
import logging
from datetime import date

import requests
from bs4 import BeautifulSoup

from .base import empty_listing, geocode_address

COUNTY = "Clark"
STATE = "IN"
URL = "https://clarkcosheriff.com/sheriff-sales"
SOURCE_URL = URL

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# Matches ***C A N C E L L E D***, ***CANCELLED***, or similar variants
_CANCELLED_RE = re.compile(r'\*+\s*C[\sA-Z]*E\s*L\s*L\s*E\s*D\s*\*+', re.IGNORECASE)


def _parse_clark_date(text, year):
    """Parse 'MAR/17' → '2026-03-17', or None on failure."""
    m = re.match(r'^([A-Z]{3})/(\d{1,2})$', text.strip().upper())
    if not m:
        return None
    month = _MONTH_MAP.get(m.group(1))
    if not month:
        return None
    try:
        return date(year, month, int(m.group(2))).strftime("%Y-%m-%d")
    except ValueError:
        return None


def scrape_clark_in(existing=None, dry_run=False):
    """
    Scrape Clark County IN sheriff sales.
    Returns (new_listings, cancellation_updates).

    Cancelled listings are included in new_listings with Cancelled="Yes"
    so that sheets_writer.update_cancellations() can mark existing rows.
    The cancellation_updates dict is always empty — Clark has no case numbers
    to match against, so cancellation tracking uses the standard Gate 4
    dedup path rather than the case-number matching path.
    """
    existing = existing or {}

    resp = requests.get(URL, headers=_HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    page_text = soup.get_text(separator="\n").replace("\xa0", " ")

    year = date.today().year
    year_m = re.search(r'SHERIFF SALES\s+(\d{4})', page_text, re.IGNORECASE)
    if year_m:
        year = int(year_m.group(1))

    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]

    listings = []
    current_sale_date = None
    in_sales_section = False

    for line in lines:
        if re.search(r'SHERIFF SALES\s+\d{4}', line, re.IGNORECASE):
            in_sales_section = True
            continue

        if not in_sales_section:
            continue

        if line.startswith("Copyright") or "Website Designed" in line:
            break

        # Date header: MAR/17, APR/2, etc.
        if re.match(r'^[A-Z]{3}/\d{1,2}$', line.upper()):
            current_sale_date = _parse_clark_date(line, year)
            continue

        # Address line: starts with a house number
        if current_sale_date and re.match(r'^\d+\s+\S', line):
            line = line.replace("\xa0", " ").strip()

            # ── Cancellation detection ────────────────────────────────────────
            # Site appends ***C A N C E L L E D*** to the city portion.
            # Detect it anywhere in the line, strip it, then parse normally.
            cancelled = bool(_CANCELLED_RE.search(line))
            clean_line = _CANCELLED_RE.sub("", line).strip().rstrip(",").strip()

            # ── Address split ─────────────────────────────────────────────────
            parts  = clean_line.rsplit(",", 1)
            street = parts[0].strip()
            city   = parts[1].strip() if len(parts) > 1 else ""

            # Normalize multiple spaces in street (source artifact)
            street = re.sub(r' {2,}', ' ', street)

            # Geocode for zip — city is known, state is always IN
            _, zip_code = geocode_address(street, city, STATE)

            listing = empty_listing(COUNTY, STATE)
            listing["Sale Date"]  = current_sale_date
            listing["Street"]     = street
            listing["City"]       = city
            listing["Zip"]        = zip_code
            listing["Cancelled"]  = "Yes" if cancelled else ""
            listing["Source URL"] = SOURCE_URL
            listings.append(listing)

    logger.info(f"[Clark IN] Found {len(listings)} listing(s)")
    # Match cancelled listings against existing sheet rows by street number
    cancellation_updates: dict[int, str] = {}
    active_listings = []

    for lst in listings:
        cancelled = lst.get("Cancelled") == "Yes"
        street = lst.get("Street", "")
        m = re.match(r"^(\d+)", street.strip())
        street_num = m.group(1) if m else ""

        if cancelled:
            if street_num and street_num in existing:
                row_idx, already_cancelled = existing[street_num]
                if not already_cancelled:
                    cancellation_updates[row_idx] = "Yes"
            continue  # never write cancelled listings as new rows

        active_listings.append(lst)

    logger.info(f"[Clark IN] Found {len(active_listings)} active listing(s), "
                f"{len(cancellation_updates)} cancellation(s)")
    return active_listings, cancellation_updates