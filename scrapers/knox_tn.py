"""
Knox County TN — TNLedger Knoxville Edition
https://tnledger.com/Knoxville/Notices.aspx?noticesDate=M/D/YYYY

Non-judicial trustee sales published weekly on Fridays.
Main listing table: server-rendered HTML, no JS rendering required.
Detail pages: full notice text accessible without login.

Design notes:
  - No court case numbers on non-judicial sales. TNLedger's FK ID (e.g. "FK502122")
    is used as Case Number for dedup purposes — it's stable per notice.
  - No cancellation tracking. Postponed/cancelled trustee sales simply stop appearing
    in the table. Gate 3 (sale date < MIN_DAYS_OUT) handles aged-out rows naturally.
  - Knox County ≠ just Knoxville. Addresses in the table are street-only (no city).
    Claude extracts Street/City/State/Zip from the detail page notice text. If that
    fails, Street is set from the table address; City/Zip left blank.
  - Detail pages are fetched in parallel (ThreadPoolExecutor).
  - On --dry-run, detail fetches are skipped entirely; stubs are returned from
    table data only so the caller can log what would have been written.
  - claude_parse_listings() returns state as 2-letter abbreviation. A full state
    name (e.g. "Tennessee") is normalised to "TN" inside claude_parse_listings().
"""

import re
import time
import logging
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from scrapers.base import empty_listing, claude_parse_listings, normalize_date, clean_money

logger = logging.getLogger(__name__)

COUNTY = "Knox"
STATE = "TN"

_ZIP_TO_COUNTY = {
    # Knox
    "37865": "Knox",   # Seymour — looks Sevier but is Knox
    "37721": "Knox",   # Corryton
    "37806": "Knox",   # Mascot
    "37849": "Knox",   # Powell
    "37934": "Knox",   # Farragut
    # Sevier
    "37862": "Sevier", "37863": "Sevier", "37876": "Sevier",
    "37738": "Sevier",
    # Blount
    "37801": "Blount", "37803": "Blount", "37804": "Blount",
    "37701": "Blount", "37737": "Blount", "37886": "Blount",
    # Roane
    "37748": "Roane", "37854": "Roane", "37763": "Roane", "37840": "Roane",
    # Cumberland
    "38555": "Cumberland", "38558": "Cumberland",
    "38571": "Cumberland", "38572": "Cumberland",
    # Hamblen
    "37813": "Hamblen", "37814": "Hamblen",
    "37877": "Hamblen", "37891": "Hamblen",
    # Anderson
    "37830": "Anderson", "37705": "Anderson",
    # Jefferson
    "37760": "Jefferson", "37725": "Jefferson",
    # Cocke
    "37722": "Cocke", "37821": "Cocke", "37843": "Cocke",
    # Loudon
    "37742": "Loudon", "37774": "Loudon", "37771": "Loudon",
}

_CITY_TO_COUNTY = {
    "knoxville": "Knox", "powell": "Knox", "corryton": "Knox",
    "mascot": "Knox", "farragut": "Knox", "heiskell": "Knox",
    "halls": "Knox", "karns": "Knox", "seymour": "Knox",
    "strawberry plains": "Knox", "hardin valley": "Knox",
    "sevierville": "Sevier", "pigeon forge": "Sevier",
    "gatlinburg": "Sevier", "pittman center": "Sevier",
    "maryville": "Blount", "alcoa": "Blount", "friendsville": "Blount",
    "walland": "Blount", "townsend": "Blount",
    "crossville": "Cumberland", "crab orchard": "Cumberland",
    "morristown": "Hamblen", "talbott": "Hamblen",
    "whitesburg": "Hamblen", "russellville": "Hamblen",
    "oak ridge": "Anderson", "andersonville": "Anderson",
    "clinton": "Anderson", "norris": "Anderson", "rocky top": "Anderson",
    "harriman": "Roane", "rockwood": "Roane", "kingston": "Roane",
    "jefferson city": "Jefferson", "dandridge": "Jefferson",
    "white pine": "Jefferson", "new market": "Jefferson",
    "newport": "Cocke", "parrottsville": "Cocke",
    "del rio": "Cocke", "cosby": "Cocke",
    "greenback": "Loudon", "lenoir city": "Loudon",
    "loudon": "Loudon", "philadelphia": "Loudon",
}


def _resolve_county(city: str, zip_code: str) -> str:
    """
    Determine the correct TN county from city name and/or zip code.
    Zip lookup takes priority over city name — more reliable for edge cases.
    Logs a warning for unrecognized cities so the dicts can be extended.
    """
    if zip_code:
        county = _ZIP_TO_COUNTY.get(zip_code.strip())
        if county:
            return county

    if city:
        county = _CITY_TO_COUNTY.get(city.strip().lower())
        if county:
            return county
        logger.warning(
            "Knox TN: unrecognized city %r (zip %r) — defaulting to Knox. "
            "Add to _CITY_TO_COUNTY or _ZIP_TO_COUNTY if it appears regularly.",
            city, zip_code,
        )

    return "Knox"


_LISTING_URL = "https://tnledger.com/Knoxville/Notices.aspx?noticesDate={date}"
_DETAIL_URL  = "https://tnledger.com/Knoxville/Search/Details/ViewNotice.aspx?id={id}&date={date}"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://tnledger.com/",
}

WEEKS_BACK  = 4
MAX_WORKERS = 4


# ── Date helpers ──────────────────────────────────────────────────────────────

def _last_n_fridays(n: int) -> list:
    today = date.today()
    days_since_friday = (today.weekday() - 4) % 7
    most_recent = today - timedelta(days=days_since_friday)
    return [
        "{}/{}/{}".format(
            (most_recent - timedelta(weeks=i)).month,
            (most_recent - timedelta(weeks=i)).day,
            (most_recent - timedelta(weeks=i)).year,
        )
        for i in range(n)
    ]


# ── Listing page (table scrape) ───────────────────────────────────────────────

def _fetch_listing_page(date_str):
    url = _LISTING_URL.format(date=date_str.replace("/", "%2f"))
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("Knox TN: listing page fetch failed for %s: %s", date_str, e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "ctl00_ContentPane_ForeclosureGridView"})
    if not table:
        logger.warning("Knox TN: foreclosure table not found on %s page", date_str)
        return []

    rows = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        a_tag = tds[0].find("a")
        if not a_tag:
            continue
        m = re.search(r"OpenChildFT2\('([^']+)','([^']+)'\)", a_tag.get("href", ""))
        if not m:
            continue

        fk_id     = m.group(1)
        page_date = m.group(2)

        rows.append({
            "id":               fk_id,
            "page_date":        page_date,
            "borrower":         tds[1].get_text(strip=True),
            "address_raw":      tds[2].get_text(strip=True),
            "auction_date_raw": tds[3].get_text(strip=True),
        })

    return rows


# ── Detail page ───────────────────────────────────────────────────────────────

def _extract_notice_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for attrs in (
        {"id": "ctl00_content"},
        {"class": "content-wide"},
        {"id": "main"},
        {"id": "ContentPane"},
    ):
        div = soup.find("div", attrs)
        if div:
            return div.get_text(separator="\n", strip=True)
    if soup.body:
        return soup.body.get_text(separator="\n", strip=True)
    return soup.get_text(separator="\n", strip=True)


def _fetch_detail(row):
    """
    Fetch one detail page and return a populated listing dict, or None on failure.

    claude_parse_listings() returns Street/City/State/Zip as separate fields.
    County is resolved from City + Zip via _resolve_county().
    Fallback when Claude parse fails: Street from table address_raw, City/Zip blank.
    """
    url = _DETAIL_URL.format(id=row["id"], date=row["page_date"])
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("Knox TN: detail fetch failed for %s: %s", row["id"], e)
        return None

    notice_text = _extract_notice_text(resp.text)
    if not notice_text:
        logger.warning("Knox TN: no text extracted from detail page %s", row["id"])
        return None

    # One Claude call per notice — prevents truncation from combining notices
    parsed_list = claude_parse_listings(notice_text, county=COUNTY, state=STATE, source_url=url)
    parsed = parsed_list[0] if parsed_list else {}

    # Address components — prefer Claude's parse (includes city/zip) over table street-only.
    street   = parsed.get("Street", "").strip()
    city     = parsed.get("City", "").strip()
    zip_code = parsed.get("Zip", "").strip()

    if not street:
        # Claude failed to extract address — fall back to table street
        street = row["address_raw"]
        city   = ""
        zip_code = ""
        logger.warning(
            "Knox TN: Claude returned no address for %s — using table fallback: %r",
            row["id"], street,
        )

    resolved_county = _resolve_county(city, zip_code)

    listing = empty_listing(resolved_county, STATE)
    listing["Case Number"]            = row["id"]
    listing["Defendant(s)"]           = row["borrower"]
    listing["Street"]                 = street
    listing["City"]                   = city
    listing["Zip"]                    = zip_code
    listing["Sale Date"]              = normalize_date(row["auction_date_raw"])
    listing["Source URL"]             = url
    listing["Plaintiff"]              = parsed.get("Plaintiff", "")
    listing["Attorney / Firm"]        = parsed.get("Attorney / Firm", "")
    listing["Judgment / Loan Amount"] = clean_money(parsed.get("Judgment / Loan Amount", ""))
    listing["Appraised Value"]        = clean_money(parsed.get("Appraised Value", ""))
    listing["Notes"]                  = parsed.get("Notes", "")

    return listing


# ── Main entry point ──────────────────────────────────────────────────────────

def scrape_knox_tn(existing=None, dry_run=False):
    """
    Scrape Knox County TN (+ surrounding East TN counties) from TNLedger.

    Args:
        existing:  set of FK IDs already in the Auctions sheet.
        dry_run:   If True, skip detail fetches and return stubs from table data.

    Returns:
        List of listing dicts (no cancellation_updates — Knox TN is non-judicial).
    """
    if existing is None:
        existing = set()

    # 1. Collect unique rows from the last N Friday pages
    seen_ids = set()
    all_rows = []

    for date_str in _last_n_fridays(WEEKS_BACK):
        page_rows = _fetch_listing_page(date_str)
        new_on_page = 0
        for row in page_rows:
            if row["id"] not in seen_ids:
                seen_ids.add(row["id"])
                all_rows.append(row)
                new_on_page += 1
        logger.info(
            "Knox TN: %s → %d rows on page, %d new after cross-page dedup",
            date_str, len(page_rows), new_on_page,
        )
        time.sleep(0.5)

    logger.info("Knox TN: %d unique notices across %d Friday pages", len(all_rows), WEEKS_BACK)

    # 2. Filter out already-known notices
    new_rows = [r for r in all_rows if r["id"] not in existing]
    if len(all_rows) - len(new_rows):
        logger.info("Knox TN: skipping %d notices already in sheet", len(all_rows) - len(new_rows))

    if not new_rows:
        logger.info("Knox TN: no new notices to process")
        return []

    # 3. Dry run: return stubs from table data only
    if dry_run:
        logger.info("Knox TN: dry run — %d stubs (detail fetches skipped)", len(new_rows))
        stubs = []
        for row in new_rows:
            listing = empty_listing(COUNTY, STATE)
            listing["Case Number"]  = row["id"]
            listing["Defendant(s)"] = row["borrower"]
            listing["Street"]       = row["address_raw"]   # street-only from table
            listing["City"]         = ""
            listing["Zip"]          = ""
            listing["Sale Date"]    = normalize_date(row["auction_date_raw"])
            listing["Source URL"]   = _DETAIL_URL.format(id=row["id"], date=row["page_date"])
            listing["Notes"]        = "[dry run — detail not fetched]"
            stubs.append(listing)
        return stubs

    # 4. Fetch detail pages in parallel
    logger.info(
        "Knox TN: fetching detail pages for %d new notices (max_workers=%d)...",
        len(new_rows), MAX_WORKERS,
    )
    listings = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_detail, row): row for row in new_rows}
        for future in as_completed(futures):
            result = future.result()
            if result:
                listings.append(result)

    logger.info("Knox TN: parsed %d/%d listings successfully", len(listings), len(new_rows))
    return listings