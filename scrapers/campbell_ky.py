"""
scrapers/campbell_ky.py — Campbell County KY
Source: campbellcountyky.gov Master Commissioner properties database.

The county publishes its own structured listing at:
  https://campbellcountyky.gov/egov/apps/properties/listing.egov
    (browse page — lists all current MC sale properties)
  https://campbellcountyky.gov/egov/apps/properties/listing.egov?view=detail&id=N
    (detail page per listing — full sale info)

Strategy:
  1. Fetch the browse page
  2. Find all detail-page links (pattern: ?view=detail&id=N)
  3. Fetch each detail page
  4. Feed each page's text to Claude for structured extraction
  5. Fall back with a clear warning if the site 403s

Cancellation tracking:
  Campbell's site never shows the original sale date for cancelled listings,
  so we can't use the standard dedup key to find them in the sheet.
  Instead we match by street number against existing rows passed in via
  the `existing` dict ({street_number: (row_index, already_cancelled)}).

Return signature: (new_listings, cancellation_updates)
  — same pattern as Jefferson KY, Fayette KY, Franklin KY, Jessamine KY.

If the site returns 403 consistently, the fallback is kypublicnotice.com
(the old strategy). That fallback is preserved below and can be re-enabled
by setting USE_COUNTY_SITE = False in this file.
"""

from __future__ import annotations

import re
import time
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from .base import empty_listing, normalize_date, claude_parse_listings
from config import DEFAULT_HEADERS

COUNTY = "Campbell"
STATE  = "KY"

# Primary source — Campbell County MC properties database
COUNTY_SITE_BASE    = "https://campbellcountyky.gov"
COUNTY_BROWSE_URL   = (
    "https://campbellcountyky.gov/egov/apps/properties/listing.egov"
    "?eGov_searchDepartment=&eGov_searchTopic=&eGov_searchCategory="
)
COUNTY_DETAIL_URL   = (
    "https://campbellcountyky.gov/egov/apps/properties/listing.egov"
    "?view=detail&id={id}"
)

# Fallback source — kypublicnotice.com (original strategy, sparse data)
KYPN_SEARCH_URL     = "https://kypublicnotice.com/index.php/main/search"
KYPN_BASE_URL       = "https://kypublicnotice.com"

# Set to False to skip the county site and go straight to kypublicnotice.com
USE_COUNTY_SITE = True


def scrape(existing: dict | None = None) -> tuple[list[dict], dict[int, str]]:
    """
    Returns (new_listings, cancellation_updates).

    existing: {street_number: (row_index, already_cancelled)} from
              sheets_writer.get_existing_rows_by_street("Campbell").
              Used to match cancelled listings by address since Campbell
              never publishes the original sale date for cancelled entries.
    """
    if existing is None:
        existing = {}

    if USE_COUNTY_SITE:
        listings, cancellations = _scrape_county_site(existing)
        if listings or cancellations:
            return listings, cancellations
        print(f"  [Campbell KY] County site returned no results — trying kypublicnotice.com fallback.")

    raw = _scrape_kypublicnotice()
    return raw, {}


# ---------------------------------------------------------------------------
# Primary: campbellcountyky.gov
# ---------------------------------------------------------------------------

def _scrape_county_site(existing: dict) -> tuple[list[dict], dict[int, str]]:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    # ── Step 1: Fetch browse page ─────────────────────────────────────────
    try:
        resp = session.get(COUNTY_BROWSE_URL, timeout=15)
        if resp.status_code == 403:
            print(
                f"  [Campbell KY] County site returned 403. "
                f"Falling back to kypublicnotice.com."
            )
            return [], {}
        resp.raise_for_status()
    except Exception as e:
        print(f"  [Campbell KY] Failed to fetch county browse page: {e}")
        return [], {}

    browse_soup = BeautifulSoup(resp.text, "html.parser")

    # ── Step 2: Find all detail page links ───────────────────────────────
    detail_ids: list[str] = []
    for a in browse_soup.find_all("a", href=True):
        href = a["href"]
        match = re.search(r"[?&]id=(\d+)", href)
        if match and "detail" in href:
            detail_ids.append(match.group(1))

    seen: set[str] = set()
    unique_ids = [i for i in detail_ids if not (i in seen or seen.add(i))]

    if not unique_ids:
        print(f"  [Campbell KY] No detail links found — parsing browse page directly.")
        browse_text = browse_soup.get_text("\n", strip=True)
        if len(browse_text) > 200:
            listings = claude_parse_listings(browse_text, COUNTY, STATE, COUNTY_BROWSE_URL)
            print(f"  [Campbell KY] Found {len(listings)} listings (from browse page).")
            return listings, {}
        return [], {}

    print(f"  [Campbell KY] Found {len(unique_ids)} listing(s) on browse page. Fetching details...")

    # ── Step 3: Fetch and parse each detail page individually ─────────────
    # One Claude call per listing — avoids combined text truncation when
    # many listings are present.
    all_listings: list[dict] = []
    cancellation_updates: dict[int, str] = {}

    for listing_id in unique_ids:
        url = COUNTY_DETAIL_URL.format(id=listing_id)
        try:
            time.sleep(1)
            detail_resp = session.get(url, timeout=12)
            if detail_resp.status_code != 200:
                print(f"  [Campbell KY] Detail page {listing_id} returned {detail_resp.status_code} — skipping.")
                continue
            detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
            text = detail_soup.get_text("\n", strip=True)
            if len(text) < 100:
                continue

            parsed = claude_parse_listings(text, COUNTY, STATE, url)
            if not parsed:
                print(f"  [Campbell KY] No listing extracted from detail page {listing_id}.")
                continue

            for listing in parsed:
                cancelled = str(listing.get("Cancelled", "")).strip().lower() in (
                    "yes", "cancelled", "canceled"
                )
                street = listing.get("Street", "")
                m = re.match(r"^(\d+)", street.strip())
                street_num = m.group(1) if m else ""

                if cancelled:
                    # Campbell never gives us the original sale date for cancelled
                    # listings, so we match by street number against existing rows.
                    if street_num and street_num in existing:
                        row_index, already_cancelled = existing[street_num]
                        if not already_cancelled:
                            cancellation_updates[row_index] = "Yes"
                            print(f"  [Campbell KY] Cancellation matched by address: {street}")
                        # Don't add to new listings regardless
                    else:
                        print(
                            f"  [Campbell KY] Cancelled listing has no prior row to update "
                            f"(address not in sheet): {street or '(no street parsed)'}"
                        )
                    continue  # never write cancelled listings as new rows

                all_listings.append(listing)

        except Exception as e:
            print(f"  [Campbell KY] Error fetching detail {listing_id}: {e}")
            continue

    print(
        f"  [Campbell KY] Found {len(all_listings)} active listing(s), "
        f"{len(cancellation_updates)} cancellation(s)."
    )
    return all_listings, cancellation_updates


# ---------------------------------------------------------------------------
# Fallback: kypublicnotice.com (original strategy — sparse but functional)
# ---------------------------------------------------------------------------

def _scrape_kypublicnotice() -> list[dict]:
    end_date   = datetime.today()
    start_date = end_date - timedelta(days=45)

    payload = {
        "q":      "commissioner sale",
        "county": "Campbell",
        "city":   "",
        "pub":    "",
        "sd":     start_date.strftime("%m/%d/%Y"),
        "ed":     end_date.strftime("%m/%d/%Y"),
        "submit": "Search",
    }

    headers = {
        **DEFAULT_HEADERS,
        "Referer":      KYPN_SEARCH_URL,
        "Origin":       KYPN_BASE_URL,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    try:
        session = requests.Session()
        session.headers.update(headers)
        session.get(KYPN_SEARCH_URL, timeout=10)
        time.sleep(1)
        resp = session.post(KYPN_SEARCH_URL, data=payload, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [Campbell KY / KYPN fallback] Search POST failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    all_text_blocks: list[str] = []

    for elem in soup.find_all(
        string=re.compile(r"COMMISSIONER.?S SALE|CAMPBELL CIRCUIT COURT", re.IGNORECASE)
    ):
        parent = elem.find_parent()
        if not parent:
            continue

        block = parent.get_text("\n", strip=True)
        if len(block) > 30:
            all_text_blocks.append(block)

        link = parent.find("a", href=True)
        if not link:
            link_parent = parent.find_parent()
            if link_parent:
                link = link_parent.find("a", href=True)

        if link:
            href = link["href"]
            if not href.startswith("http"):
                href = KYPN_BASE_URL + href
            if not href.lower().endswith(".pdf"):
                full_html = _safe_fetch(session, href)
                if full_html:
                    full_soup = BeautifulSoup(full_html, "html.parser")
                    full_text = full_soup.get_text("\n", strip=True)
                    if len(full_text) > 100:
                        all_text_blocks.append(full_text)

    if not all_text_blocks:
        print(
            f"  [Campbell KY] No commissioner sale notices found on kypublicnotice.com. "
            f"Check manually if expected."
        )
        return []

    seen: set[str] = set()
    unique_blocks = []
    for block in all_text_blocks:
        key = block[:100].strip()
        if key not in seen:
            seen.add(key)
            unique_blocks.append(block)

    combined = "\n\n---\n\n".join(unique_blocks)
    listings = claude_parse_listings(combined, COUNTY, STATE, KYPN_SEARCH_URL)
    print(f"  [Campbell KY / KYPN fallback] Found {len(listings)} listings.")
    return listings


def _safe_fetch(session: requests.Session, url: str) -> str | None:
    try:
        time.sleep(1)
        resp = session.get(url, timeout=12)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None