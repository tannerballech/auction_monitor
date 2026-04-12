"""
scrapers/hamilton_oh.py
Hamilton County OH — RealAuction / sheriffsaleauction.ohio.gov

Source: https://hamilton.sheriffsaleauction.ohio.gov
Tech:   ColdFusion SSR app. Requires Playwright because plain requests gets
        session-locked (returns "Account Locked Out"). A real browser session
        loads cleanly without credentials.

Navigation pattern:
  - AUCTIONDATE=0  →  exposes the "Current" link (next upcoming sale) plus
                       "Next Auction" links.
  - Follows Next Auction links forward from Current until no more links exist.

HTML structure per listing:
  div.AUCTION_ITEM
    div.AUCTION_STATS           ← auction status (Cancelled etc.) lives here
      div.ASTAT_MSGA → label   (e.g. "Auction Status")
      div.ASTAT_MSGB → value   (e.g. "Cancelled")
    div.AUCTION_DETAILS
      table.ad_tab              ← case #, address, appraised value etc.

Cancellation is detected from AUCTION_STATS, NOT from the ad_tab table's
"Case Status" field (which remains "ACTIVE" even for cancelled auctions).

Data per listing (public, no login):
  - Case #          →  Case Number  (parenthetical stripped: "A2001551 (11216)" → "A2001551")
  - Property Address →  Street / City / Zip
  - Appraised Value  →  Appraised Value
  - Auction Status   →  Cancelled flag ("Cancelled" in ASTAT_MSGB)
  - Sale date        →  from page header, shared by all listings on that page

Not available without login: Plaintiff, Defendant, Attorney, Judgment Amount.
Opening Bid (= 2/3 of appraised by Ohio law) is NOT the judgment — left blank.
"""

import re
from datetime import date

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from scrapers.base import empty_listing, normalize_date, clean_money, split_standard_address

COUNTY    = "Hamilton"
STATE     = "OH"
BASE_URL  = "https://hamilton.sheriffsaleauction.ohio.gov"
START_URL = f"{BASE_URL}/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=0"

_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
]
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

_DATE_RE = re.compile(
    r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+'
    r'(\w+ \d{1,2}, \d{4})',
    re.IGNORECASE,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _full_url(href):
    if href.startswith("http"):
        return href
    return BASE_URL + ("" if href.startswith("/") else "/") + href


def _get_html(page, url, wait_ms=2000):
    page.goto(url, wait_until="networkidle")
    page.wait_for_timeout(wait_ms)
    return page.content()


def _parse_sale_date(soup):
    text = soup.get_text(separator=" ")
    m = _DATE_RE.search(text)
    return normalize_date(m.group(1)) if m else None


def _parse_auction_item(item_div, sale_date):
    """
    Parse one AUCTION_ITEM div into a listing dict + cancelled flag.

    Reads cancellation from div.AUCTION_STATS (ASTAT_MSGB value),
    and case/address/appraised data from the ad_tab table inside
    div.AUCTION_DETAILS.
    """
    # ── Auction status (cancelled check) ─────────────────────────────────────
    cancelled = False
    stats_div = item_div.find("div", class_="AUCTION_STATS")
    if stats_div:
        label_div = stats_div.find("div", class_="ASTAT_MSGA")
        value_div = stats_div.find("div", class_="ASTAT_MSGB")
        if label_div and value_div:
            label = label_div.get_text(strip=True).upper()
            value = value_div.get_text(strip=True).upper()
            if "AUCTION STATUS" in label and "CANCEL" in value:
                cancelled = True

    # ── ad_tab table fields ───────────────────────────────────────────────────
    table = item_div.find("table", class_="ad_tab")
    fields = {}
    last_key = None

    if table:
        for tr in table.find_all("tr"):
            th = tr.find("th", class_="AD_LBL")
            td = tr.find("td", class_="AD_DTA")
            if not td:
                continue
            label = th.get_text(strip=True).rstrip(":") if th else ""
            value = td.get_text(separator=" ", strip=True)
            if label:
                fields[label] = value
                last_key = label
            elif last_key and value:
                fields[last_key] = fields[last_key] + ", " + value

    # Case number — strip parenthetical: "A2404311 (11595)" → "A2404311"
    case_raw = fields.get("Case #", "")
    case_no  = re.sub(r'\s*\(\d+\)\s*$', '', case_raw).strip()

    # Address — clean stray spaces around commas then split
    raw_address = re.sub(r'\s+,\s+', ', ', fields.get("Property Address", "")).strip()
    street, city, zip_code = split_standard_address(raw_address)

    appraisal = clean_money(fields.get("Appraised Value", ""))

    listing = empty_listing(COUNTY, STATE)
    listing["Sale Date"]       = sale_date
    listing["Case Number"]     = case_no
    listing["Street"]          = street
    listing["City"]            = city
    listing["Zip"]             = zip_code
    listing["Appraised Value"] = appraisal
    listing["Cancelled"]       = "Yes" if cancelled else ""
    listing["Source URL"]      = BASE_URL

    return listing, cancelled


def _next_url(soup):
    for a in soup.find_all("a", href=True):
        if "AUCTIONDATE" in a["href"].upper() and "Next" in a.get_text():
            return _full_url(a["href"])
    return None


def _current_url(soup):
    for a in soup.find_all("a", href=True):
        if "AUCTIONDATE" in a["href"].upper() and a.get_text(strip=True) == "Current":
            return _full_url(a["href"])
    return None


def _scrape_page(soup, sale_date, existing, new_listings, cancellation_updates):
    """Process all AUCTION_ITEM divs on one page. Mutates lists/dicts in place."""
    auction_items = soup.find_all("div", class_="AUCTION_ITEM")

    for item_div in auction_items:
        try:
            listing, cancelled = _parse_auction_item(item_div, sale_date)
        except Exception as e:
            print(f"Hamilton OH: error parsing listing — {e}")
            continue

        case_no = listing["Case Number"]
        if not case_no:
            continue

        if cancelled:
            if case_no in existing:
                row_idx, already_cancelled = existing[case_no]
                if not already_cancelled:
                    cancellation_updates[row_idx] = "Yes"
                    print(f"Hamilton OH: cancellation matched — {case_no} (row {row_idx})")
                else:
                    print(f"Hamilton OH: {case_no} already cancelled in sheet.")
            else:
                print(f"Hamilton OH: cancelled listing {case_no} not in sheet (may have been added after cancel).")
            continue

        if case_no in existing:
            continue

        new_listings.append(listing)


# ── Public API ────────────────────────────────────────────────────────────────

def scrape_hamilton_oh(existing=None):
    """
    Scrape Hamilton County OH sheriff sale listings.

    Navigates from the Current (next upcoming) auction page forward through
    all future pages via Next Auction links.

    Args:
        existing: {case_number: (row_index, already_cancelled)} from
                  sheets_writer.get_existing_case_numbers("Hamilton")

    Returns:
        (new_listings, cancellation_updates)
    """
    existing = existing or {}
    new_listings         = []
    cancellation_updates = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=_LAUNCH_ARGS)
        context = browser.new_context(
            user_agent=_USER_AGENT,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        html = _get_html(page, START_URL)
        soup = BeautifulSoup(html, "html.parser")
        start = _current_url(soup)

        if not start:
            print("Hamilton OH: could not find Current auction link — aborting.")
            browser.close()
            return [], {}

        url     = start
        visited = set()
        today   = date.today()

        while url and url not in visited:
            visited.add(url)
            html = _get_html(page, url)
            soup = BeautifulSoup(html, "html.parser")

            sale_date = _parse_sale_date(soup)
            if not sale_date:
                print(f"Hamilton OH: could not parse sale date at {url} — skipping.")
                url = _next_url(soup)
                continue

            try:
                if date.fromisoformat(sale_date) < today:
                    print(f"Hamilton OH: reached past date ({sale_date}) — stopping.")
                    break
            except ValueError:
                url = _next_url(soup)
                continue

            print(f"Hamilton OH: scraping sale_date={sale_date}")
            _scrape_page(soup, sale_date, existing, new_listings, cancellation_updates)

            url = _next_url(soup)

        browser.close()

    return new_listings, cancellation_updates