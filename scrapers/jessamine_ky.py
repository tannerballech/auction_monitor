"""
scrapers/jessamine_ky.py — Jessamine County KY Master Commissioner Sales
Source: https://jessaminemc.com/upcomingsales.php
Format: PHP site, sale-date links reveal listings via JS; no URL change on click.
Listings delimited by <u>ONE</u> / <u>TWO</u> etc. (ordinal words, underlined).
Requires Playwright (headless=False — Cloudflare fingerprint check on headless).
Return signature: (new_listings, cancellation_updates)
  new_listings        — list of listing dicts (active, not yet in sheet)
  cancellation_updates — {row_index: "Yes"} for rows newly cancelled

Address format: "438 MAIN ST NICHOLASVILLE KY 40356" — no comma between
street and city. Parsed by _parse_jessamine_address() using street-suffix
boundary detection.
"""

import re
import time
import logging
from datetime import date

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from scrapers.base import empty_listing, clean_money, split_standard_address

logger = logging.getLogger(__name__)

COUNTY = "Jessamine"
STATE  = "KY"
URL    = "https://jessaminemc.com/upcomingsales.php"

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
]

_ORDINALS = [
    "ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT",
    "NINE", "TEN", "ELEVEN", "TWELVE", "THIRTEEN", "FOURTEEN", "FIFTEEN",
    "SIXTEEN", "SEVENTEEN", "EIGHTEEN", "NINETEEN", "TWENTY",
]

_DELIM_RE = re.compile(
    r"(?:^|\n)\s*(?:__)?(" + "|".join(_ORDINALS) + r")(?:__)?\s*\n",
    re.IGNORECASE,
)

# Street suffixes used to locate the street/city boundary in no-comma addresses.
# Ordered longest-first so "Street" matches before "St" etc. doesn't matter
# because finditer gives us all matches and we take the last one.
_STREET_SUFFIX_RE = re.compile(
    r'\b(Boulevard|Terrace|Circle|Avenue|Street|Drive|Court|Place|'
    r'Trail|Lane|Road|Pike|Blvd|Ter|Cir|Ave|Trl|Hwy|Ct|Dr|St|Rd|Ln|'
    r'Way|Pl)\.?\b',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Address parser — no-comma format
# ---------------------------------------------------------------------------

def _parse_jessamine_address(raw: str) -> tuple[str, str, str]:
    """
    Split a Jessamine address string into (street, city, zip).

    Jessamine addresses have no comma between street and city:
      "438 MAIN ST NICHOLASVILLE KY 40356"  → ("438 MAIN ST", "NICHOLASVILLE", "40356")
      "109 Willow Court Wilmore KY 40390"   → ("109 Willow Court", "Wilmore", "40390")
      "438 MAIN ST NICHOLASVILLE KY"        → ("438 MAIN ST", "NICHOLASVILLE", "")

    Falls back to split_standard_address() if the address contains a comma,
    which handles any listings that happen to use the standard format.
    """
    if not raw:
        return "", "", ""

    raw = raw.strip()

    # If commas present, use the standard parser
    if "," in raw:
        return split_standard_address(raw)

    # Step 1: extract trailing 5-digit zip
    zip_match = re.search(r'\b(\d{5})\s*$', raw)
    zip_code = zip_match.group(1) if zip_match else ""

    # Step 2: strip zip and state abbreviation from the right
    stripped = re.sub(r'\s+[A-Z]{2}\s+\d{5}\s*$', '', raw).strip()
    if stripped == raw:
        # No "ST XXXXX" pattern — try stripping just a state abbreviation
        stripped = re.sub(r'\s+[A-Z]{2}\s*$', '', raw).strip()

    # stripped is now e.g. "438 MAIN ST NICHOLASVILLE"

    # Step 3: find the last street suffix — everything up to and including it
    # is the street; everything after is the city
    matches = list(_STREET_SUFFIX_RE.finditer(stripped))
    if matches:
        last = matches[-1]
        street = stripped[:last.end()].strip()
        city   = stripped[last.end():].strip()
    else:
        # No known suffix — put everything in street, city unknown
        logger.warning(f"[Jessamine] No street suffix found in: {raw!r}")
        street = stripped
        city   = ""

    return street, city, zip_code


# ---------------------------------------------------------------------------
# HTML fetch via Playwright
# ---------------------------------------------------------------------------

def _fetch_rendered_text(dry_run: bool = False) -> tuple[str, str]:
    """
    Opens the page, clicks the first future sale-date link,
    waits for listings to render, returns (page_text, sale_date_iso).
    sale_date_iso is already in YYYY-MM-DD format (taken from link text).
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=_LAUNCH_ARGS)
        context = browser.new_context(
            user_agent=_USER_AGENT,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        logger.info("Navigating to %s", URL)
        page.goto(URL, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(2)

        # Press Escape in case a popup appeared (safe no-op if not)
        page.keyboard.press("Escape")
        time.sleep(0.5)

        # Find the first future sale-date link (YYYY-MM-DD format)
        links = page.locator("a").all()
        today = date.today()
        future_link     = None
        future_date_str = None

        for link in links:
            try:
                text = link.inner_text().strip()
            except Exception:
                continue
            m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", text)
            if not m:
                continue
            try:
                link_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                continue
            if link_date >= today:
                future_link     = link
                future_date_str = text   # already YYYY-MM-DD
                logger.info("Found future sale date link: %s", text)
                break

        if future_link is None:
            logger.warning("No future sale-date link found on %s", URL)
            browser.close()
            return "", ""

        if dry_run:
            logger.info("[dry-run] Skipping click and content fetch.")
            browser.close()
            return "", future_date_str

        future_link.click()
        try:
            page.wait_for_selector("text=Plaintiff", timeout=8_000)
        except PlaywrightTimeout:
            logger.warning("Timed out waiting for 'Plaintiff' — trying anyway")
        time.sleep(1)

        body_text = page.locator("body").inner_text()
        body_html = page.locator("body").inner_html()
        browser.close()

        return _normalize_body(body_text, body_html), future_date_str


def _normalize_body(text: str, html: str) -> str:
    """Normalize ordinal delimiters to __ONE__ form for consistent splitting."""
    lines = text.split("\n")
    normalized = []
    for line in lines:
        stripped = line.strip()
        if stripped.upper() in _ORDINALS:
            normalized.append(f"__{stripped.upper()}__")
        else:
            normalized.append(line)
    return "\n".join(normalized)


# ---------------------------------------------------------------------------
# Listing parser
# ---------------------------------------------------------------------------

def _split_into_blocks(body_text: str) -> list[str]:
    """Split the body text into per-listing blocks on __ONE__, __TWO__, etc."""
    parts = _DELIM_RE.split(body_text)
    blocks = []
    i = 1
    while i < len(parts) - 1:
        blocks.append(parts[i + 1])
        i += 2
    return blocks


def _field(pattern: str, text: str, flags: int = re.IGNORECASE) -> str:
    """Return first capture group from pattern, or empty string."""
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else ""


def _parse_block(block: str, sale_date_override: str) -> dict | None:
    """
    Parse one listing block into a listing dict.
    sale_date_override is already YYYY-MM-DD (from the link we clicked).
    """
    listing = empty_listing(COUNTY, STATE)

    is_cancelled = bool(re.search(r"\bCANCELLED?\b", block, re.IGNORECASE))

    listing["Sale Date"]   = sale_date_override
    listing["Source URL"]  = URL
    listing["Case Number"] = _field(r"Case\s+Number[:\s]+([0-9]{2}-CI-[0-9]+)", block)
    listing["Plaintiff"]   = _field(r"Plaintiff[:\s]*\n([^\n]+)", block)
    listing["Defendant(s)"] = _field(r"Defendants?[:\s]*\n([^\n]+)", block)

    # Address — no-comma format; split into Street/City/Zip
    raw_address = _field(r"Property\s+Address[:\s]*\n([^\n]+)", block)
    if raw_address:
        street, city, zip_code = _parse_jessamine_address(raw_address)
        listing["Street"] = street
        listing["City"]   = city
        listing["Zip"]    = zip_code

    raw_judgment = _field(r"Judgment\s+Amount[:\s]*\n([^\n]+)", block)
    if raw_judgment:
        listing["Judgment / Loan Amount"] = clean_money(raw_judgment)

    raw_appraisal = _field(r"Appraisal[:\s]*\n([^\n]+)", block)
    if raw_appraisal and "tbd" not in raw_appraisal.lower():
        listing["Appraised Value"] = clean_money(raw_appraisal)

    listing["Attorney / Firm"] = _field(
        r"Attorney\s+for\s+Plaintiff[:\s]*([^\n]+)", block
    )

    if is_cancelled:
        listing["Cancelled"] = "Yes"

    # Need at least a street number or case number to be useful
    if not listing["Street"] and not listing["Case Number"]:
        logger.debug("Block too sparse, skipping:\n%s", block[:200])
        return None

    return listing


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def scrape_jessamine_ky(existing: dict | None = None, dry_run: bool = False):
    """
    Scrape Jessamine County KY Master Commissioner sales.
    Returns (new_listings, cancellation_updates).
    """
    if existing is None:
        existing = {}

    body_text, sale_date_str = _fetch_rendered_text(dry_run=dry_run)

    if not body_text:
        if dry_run:
            logger.info("[dry-run] No content fetched — returning empty.")
        else:
            logger.warning("Empty body returned from Jessamine fetch.")
        return [], {}

    blocks = _split_into_blocks(body_text)
    logger.info("Found %d listing block(s) for sale date %s", len(blocks), sale_date_str)

    new_listings: list[dict] = []
    cancellation_updates: dict[int, str] = {}

    for block in blocks:
        listing = _parse_block(block, sale_date_str)
        if listing is None:
            continue

        case_num     = listing.get("Case Number", "")
        is_cancelled = listing.get("Cancelled") == "Yes"

        if case_num in existing:
            row_idx, already_cancelled = existing[case_num]
            if is_cancelled and not already_cancelled:
                cancellation_updates[row_idx] = "Yes"
                logger.info("Cancellation: case %s (row %d)", case_num, row_idx)
            continue

        if is_cancelled:
            logger.debug("New cancelled listing — skipping write: %s", case_num)
            continue

        new_listings.append(listing)

    return new_listings, cancellation_updates