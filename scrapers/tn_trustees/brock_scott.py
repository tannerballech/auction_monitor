"""
scrapers/tn_trustees/brock_scott.py
Brock & Scott, PLLC — TN Foreclosure Sale Scraper
https://www.brockandscott.com/foreclosure-sales/?_sft_foreclosure_state=tn

Pure server-rendered WordPress site — no JS, no disclaimer flow.
Paginated at 10 listings/page via &sf_paged=N parameter.

Listing structure (article.foreclosure_search elements):
  .forecol pairs: County | Sale Date | State | Court SP# | Case# | Address |
                  Opening Bid Amount | Book Page

Address format: "3875 Faxon Avenue   Memphis, Tennessee 38122"
  — full state name, extra whitespace, no comma before city.
  Parsed by splitting on last two whitespace-delimited tokens (zip, state_name),
  then extracting city as the last word(s) before state.

Two modes:

  scrape_brock_scott(existing_addr_set, dry_run)
      Discovery: fetch all pages, return new listings not already in sheet.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: same fetch, cross-reference Brock & Scott sheet rows.
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

TRUSTEE           = "Brock & Scott, PLLC"
STATE             = "TN"
BASE_URL          = "https://www.brockandscott.com/foreclosure-sales/"
TN_PARAMS         = {"_sft_foreclosure_state": "tn"}
SOURCE_URL        = BASE_URL + "?_sft_foreclosure_state=tn"
CHECK_WINDOW_DAYS = 14
MAX_PAGES         = 20   # safety cap

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Maps full US state names to 2-letter abbreviations (TN only needed but
# include neighbours in case of data entry variation)
_STATE_ABBR = {
    "tennessee": "TN", "kentucky": "KY", "georgia": "GA",
    "north carolina": "NC", "south carolina": "SC", "virginia": "VA",
    "alabama": "AL", "mississippi": "MS", "arkansas": "AR",
}


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------

def _parse_sale_date(raw: str) -> str:
    """
    '04/16/2026 - 11:00:00 AM' → '2026-04-16'.
    Returns '' on failure.
    """
    m = re.match(r"(\d{1,2}/\d{1,2}/\d{4})", raw.strip())
    if not m:
        return ""
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _parse_address(raw: str) -> tuple[str, str, str]:
    """
    Parse '3875 Faxon Avenue   Memphis, Tennessee 38122'
    → (street, city, zip).

    Strategy:
      1. Normalise whitespace
      2. Extract zip (trailing 5-digit token)
      3. Extract state name (token before zip — may be two words like "North Carolina")
      4. Everything before state = street + city; split on last comma or last
         word boundary before the city starts
    """
    raw = " ".join(raw.split())   # collapse all whitespace

    # Extract zip
    m_zip = re.search(r"\b(\d{5})(?:-\d{4})?\s*$", raw)
    if not m_zip:
        return raw, "", ""
    zip_code = m_zip.group(1)
    before_zip = raw[:m_zip.start()].strip()

    # Extract state name — check two-word states first, then single word
    state_name = ""
    for candidate in sorted(_STATE_ABBR.keys(), key=len, reverse=True):
        if before_zip.lower().endswith(candidate):
            state_name = candidate
            before_zip = before_zip[:-len(candidate)].strip().rstrip(",").strip()
            break

    # before_zip is now "3875 Faxon Avenue   Memphis" or "3875 Faxon Ave, Memphis"
    # Split on last comma if present, else last whitespace run before a capitalised word
    if "," in before_zip:
        parts = before_zip.rsplit(",", 1)
        street = parts[0].strip()
        city   = parts[1].strip()
    else:
        # No comma — city is the last whitespace-separated token(s)
        # Heuristic: find where the street name ends and city begins.
        # Street ends after the first "St|Ave|Rd|Dr|Ln|Blvd|Way|Ct|Pl|Cir|Pike|Hwy|..."
        # then the city follows.
        m_city = re.search(
            r"(?i)\b(street|avenue|road|drive|lane|boulevard|way|court|"
            r"place|circle|pike|highway|trail|run|loop|ridge|hollow|"
            r"grove|pointe|cove|trace|crossing|pass|path|row)\b\s*(.+)$",
            before_zip,
        )
        if m_city:
            street = before_zip[:m_city.end(1)].strip()
            city   = m_city.group(2).strip()
        else:
            # Last resort — split on last space
            parts = before_zip.rsplit(None, 1)
            street = parts[0].strip() if len(parts) > 1 else before_zip
            city   = parts[1].strip() if len(parts) > 1 else ""

    return street, city, zip_code


def _parse_fields(article) -> dict:
    """
    Extract key→value pairs from .forecol divs inside one article element.
    Returns a flat dict of label → value (both stripped).
    """
    fields: dict[str, str] = {}
    for col in article.select(".forecol"):
        ps = col.find_all("p")
        if len(ps) >= 2:
            label = ps[0].get_text(strip=True).rstrip(":").strip()
            value = ps[1].get_text(strip=True)
            if label:
                fields[label] = value
    return fields


def _fetch_page(page_num: int) -> list[dict]:
    """
    Fetch one page of TN listings and return parsed row dicts.
    Returns [] on HTTP error or if no articles found.
    """
    params = dict(TN_PARAMS)
    if page_num > 1:
        params["sf_paged"] = str(page_num)

    try:
        resp = requests.get(BASE_URL, headers=_HEADERS, params=params, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("[brock_scott] Fetch error (page %d): %s", page_num, e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    articles = soup.find_all("article", class_=re.compile(r"foreclosure_search"))

    rows = []
    for article in articles:
        fields = _parse_fields(article)

        county     = fields.get("County", "").strip()
        state_raw  = fields.get("State", "").strip().upper()
        date_raw   = fields.get("Sale Date", "").strip()
        case_num   = fields.get("Case #", "").strip()
        addr_raw   = fields.get("Address", "").strip()
        bid_raw    = fields.get("Opening Bid Amount", "").strip()

        # Only process TN listings (filter is in URL but belt-and-suspenders)
        if state_raw != "TN":
            continue

        sale_date = _parse_sale_date(date_raw)
        if not sale_date:
            logger.debug("[brock_scott] Unparseable date %r — skipping", date_raw)
            continue

        street, city, zip_code = _parse_address(addr_raw)
        if not street:
            logger.debug("[brock_scott] Unparseable address %r — skipping", addr_raw)
            continue

        # Bid amount — store if non-zero
        bid = ""
        try:
            amount = float(bid_raw.replace(",", ""))
            if amount > 0:
                bid = f"${amount:,.2f}"
        except (ValueError, AttributeError):
            pass

        rows.append({
            "Sale Date":              sale_date,
            "Case Number":            case_num,
            "Street":                 street,
            "City":                   city,
            "Zip":                    zip_code,
            "County":                 county,
            "State":                  STATE,
            "Judgment / Loan Amount": bid,
        })

    return rows


def _has_next_page(page_num: int) -> bool:
    """
    Re-fetch the page to check whether a "Next" pagination link exists.
    Cheaper to check via the already-fetched soup — but we don't cache it,
    so we infer: if _fetch_page returned 10 rows (full page), try next.
    This avoids an extra HTTP call; the loop stops when a page returns < 10.
    """
    return True   # caller uses row-count logic instead


def _fetch_all() -> list[dict]:
    """Fetch all paginated pages and return combined list of row dicts."""
    all_rows: list[dict] = []

    for page_num in range(1, MAX_PAGES + 1):
        rows = _fetch_page(page_num)
        logger.info("[brock_scott] Page %d: %d row(s)", page_num, len(rows))
        all_rows.extend(rows)

        if len(rows) < 10:
            # Partial page = last page
            break

    logger.info("[brock_scott] Total rows: %d", len(all_rows))
    return all_rows


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

def scrape_brock_scott(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.  Fetch all TN listings from brockandscott.com and return
    listings not already in the sheet.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        covering all active TN rows — prevents duplicating TNLedger rows.

    Returns (new_listings, {}).
    """
    site_rows = _fetch_all()
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
        listing["Case Number"]            = site["Case Number"]
        listing["Attorney / Firm"]        = TRUSTEE
        listing["Sale Date"]              = orig_date
        listing["Street"]                 = street
        listing["City"]                   = site["City"]
        listing["Zip"]                    = site["Zip"]
        listing["Judgment / Loan Amount"] = site["Judgment / Loan Amount"]
        listing["Source URL"]             = SOURCE_URL

        new_listings.append(listing)

    logger.info("[brock_scott] %d new listing(s) after dedup", len(new_listings))
    return new_listings, {}


# ---------------------------------------------------------------------------
# Check mode
# ---------------------------------------------------------------------------

def check_existing(
    sheet_rows: list[dict],
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Check mode.  Fetch all TN listings and cross-reference Brock & Scott
    sheet rows.

    Postponement: listing present on site with a different sale date.
    Flag:         listing absent AND sale is within CHECK_WINDOW_DAYS.

    Returns (postponements, flags).
    """
    site_rows = _fetch_all()

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
                        f"Not found on Brock & Scott site "
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