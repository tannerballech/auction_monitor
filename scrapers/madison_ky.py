"""
scrapers/madison_ky.py
Madison County KY — Master Commissioner's Sale
Source: https://www.madisonkymastercommissioner.com/

Tech: Wix site. Listings are posted as PDFs linked behind a JS modal.
      Playwright clicks "Current Properties" in the nav, waits for the
      [role='dialog'] to open, extracts the PDF href, then fetches and
      parses the PDF with requests. No login required.

PDF format (confirmed against live March 27 2026 notice):
  - Sale date in header: "FRIDAY, MARCH 27, 2026"
  - Table: Address | Parcel ID | Case Number | Amount To Be Raised
  - Address column: "712 AMANDA COURT, RICHMOND" (street + city, no zip/state)
  - Cancelled listings prefixed with * in the address field
  - No plaintiff, defendant, or attorney published — sparse by design.
  - Dedup keys on (county, street_number, sale_date, "") since defendant
    is always blank for this county.
  - Zip looked up via geocode_address() (Nominatim).
"""

import re
import requests
from datetime import date

from playwright.sync_api import sync_playwright

from scrapers.base import empty_listing, normalize_date, clean_money, \
    split_standard_address, geocode_address

COUNTY     = "Madison"
STATE      = "KY"
SOURCE_URL = "https://www.madisonkymastercommissioner.com/"

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

_CASE_RE   = re.compile(r'\b(\d{2,4}-CI-\d+)\b')
_PARCEL_RE = re.compile(r'\b[A-Z]?\d{3,4}[A-Z]?-[\dA-Z]+-[\dA-Z]+(?:-[A-Z])?\b')
_AMOUNT_RE = re.compile(r'\$\s*([\d,]+\.?\d*)')
_DATE_RE   = re.compile(
    r'(?:MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY),?\s+'
    r'([A-Z]+ \d{1,2}, \d{4})',
    re.IGNORECASE,
)


# ── PDF discovery via Playwright ──────────────────────────────────────────────

def _get_pdf_url():
    """
    Open the Wix site, click 'Current Properties', wait for the dialog,
    and return the PDF href. Returns None if anything fails.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=_LAUNCH_ARGS)
        ctx = browser.new_context(
            user_agent=_USER_AGENT,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = ctx.new_page()

        try:
            page.goto(SOURCE_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(4000)

            page.keyboard.press("Escape")
            page.wait_for_timeout(500)

            page.get_by_text("Current Properties", exact=True).first.click()
            page.wait_for_selector(
                "[role='dialog'] a[href*='_files/ugd']", timeout=10000
            )
            page.wait_for_timeout(1000)

            el  = page.query_selector("[role='dialog'] a[href*='_files/ugd']")
            url = el.get_attribute("href") if el else None
            return url

        except Exception as e:
            print(f"Madison KY: error discovering PDF URL — {e}")
            return None

        finally:
            browser.close()


# ── PDF fetch ─────────────────────────────────────────────────────────────────

def _fetch_pdf_text(pdf_url):
    """Fetch a PDF and return its extracted text via pdfminer."""
    import io
    try:
        from pdfminer.high_level import extract_text
    except ImportError:
        raise ImportError(
            "pdfminer.six is required for Madison KY PDF parsing. "
            "Run: pip install pdfminer.six"
        )

    headers = {"User-Agent": _USER_AGENT}
    resp    = requests.get(pdf_url, headers=headers, timeout=30)
    resp.raise_for_status()
    return extract_text(io.BytesIO(resp.content))


# ── PDF parse ─────────────────────────────────────────────────────────────────

def _parse_pdf(pdf_text, existing):
    """
    Parse the text content of a Madison County MC sale PDF.
    Returns (new_listings, cancellation_updates).

    pdfminer extracts this PDF's 4-column table column-by-column, not
    row-by-row. Text order: all addresses → all parcel IDs → all case
    numbers → all amounts. We extract each column as a list and zip
    them positionally.

    Address format from PDF: "712 AMANDA COURT, RICHMOND"
    (street + city separated by comma, no state, no zip).
    split_standard_address() handles this; geocode_address() fills zip.
    """
    new_listings         = []
    cancellation_updates = {}

    # ── Sale date ─────────────────────────────────────────────────────────────
    date_m = _DATE_RE.search(pdf_text)
    if not date_m:
        print("Madison KY: could not find sale date in PDF.")
        return [], {}

    sale_date = normalize_date(date_m.group(1))
    if not sale_date:
        print(f"Madison KY: could not normalize sale date '{date_m.group(1)}'.")
        return [], {}

    try:
        if date.fromisoformat(sale_date) < date.today():
            print(f"Madison KY: PDF sale date {sale_date} is in the past — skipping.")
            return [], {}
    except ValueError:
        return [], {}

    # ── Isolate the table region ──────────────────────────────────────────────
    table_start = re.search(r'PROPERTY ADDRESS\s+PARCEL ID', pdf_text, re.IGNORECASE)
    table_end   = re.search(r'BROOKS\s+STUMBO', pdf_text, re.IGNORECASE)
    if not table_start or not table_end:
        print("Madison KY: could not isolate table region in PDF.")
        return [], {}

    region = pdf_text[table_start.end() : table_end.start()]
    region = re.sub(r'CIVIL ACTION NO\.?\s+AMOUNT TO\s+BE RAISED', '', region,
                    flags=re.IGNORECASE)

    # ── Classify tokens ───────────────────────────────────────────────────────
    tokens = [t.strip() for t in region.split('\n') if t.strip()]

    addresses    = []
    case_numbers = []
    amounts      = []

    for token in tokens:
        if _CASE_RE.fullmatch(token):
            case_numbers.append(token)
        elif re.match(r'\$[\s\d,\.]+$', token):
            amounts.append(re.sub(r'\s+', '', token))
        elif _PARCEL_RE.search(token) and not re.search(r'[a-z]', token):
            pass   # parcel ID — skip
        else:
            addresses.append(token)

    # ── Validate column counts ────────────────────────────────────────────────
    n = len(case_numbers)
    if not n:
        print("Madison KY: no case numbers found in PDF.")
        return [], {}
    if len(addresses) != n:
        print(f"Madison KY: address count ({len(addresses)}) != "
              f"case number count ({n}) — check PDF layout.")
    if len(amounts) != n:
        print(f"Madison KY: amount count ({len(amounts)}) != "
              f"case number count ({n}) — some judgments may be blank.")

    # ── Zip columns and build listings ────────────────────────────────────────
    for i, case_no in enumerate(case_numbers):
        address_raw  = addresses[i]    if i < len(addresses) else ""
        judgment_raw = amounts[i]      if i < len(amounts)   else ""

        cancelled   = address_raw.startswith('*')
        address_raw = address_raw.lstrip('* ').strip()

        # Split "712 AMANDA COURT, RICHMOND" → street, city, zip
        # split_standard_address handles "Street, City" with no state/zip.
        # geocode_address fills in the zip (city is already known from PDF).
        street, city, zip_code = split_standard_address(address_raw)
        if street and not zip_code:
            _, zip_code = geocode_address(street, city, STATE)

        # judgment_raw already has internal whitespace stripped (e.g. "$115,061.57")
        # clean_money handles the $ prefix and commas directly
        judgment = clean_money(judgment_raw) if judgment_raw else ""

        if cancelled:
            if case_no in existing:
                row_idx, already_cancelled = existing[case_no]
                if not already_cancelled:
                    cancellation_updates[row_idx] = "Yes"
            continue

        if case_no in existing:
            continue

        listing = empty_listing(COUNTY, STATE)
        listing["Sale Date"]              = sale_date
        listing["Case Number"]            = case_no
        listing["Street"]                 = street
        listing["City"]                   = city
        listing["Zip"]                    = zip_code
        listing["Judgment / Loan Amount"] = judgment
        listing["Source URL"]             = SOURCE_URL
        new_listings.append(listing)

    return new_listings, cancellation_updates


# ── Public API ────────────────────────────────────────────────────────────────

def scrape_madison_ky(existing=None, dry_run=False):
    """
    Scrape Madison County KY master commissioner sale listings.

    Args:
        existing: {case_number: (row_index, already_cancelled)} from
                  sheets_writer.get_existing_case_numbers("Madison")
        dry_run:  If True, skip Playwright and PDF fetch (returns empty).

    Returns:
        (new_listings, cancellation_updates)
    """
    existing = existing or {}

    if dry_run:
        print("Madison KY: dry run — skipping Playwright PDF fetch.")
        return [], {}

    print("Madison KY: opening browser to find current PDF...")
    pdf_url = _get_pdf_url()
    if not pdf_url:
        print("Madison KY: could not locate PDF URL — aborting.")
        return [], {}
    print(f"Madison KY: found PDF → {pdf_url}")

    try:
        pdf_text = _fetch_pdf_text(pdf_url)
    except Exception as e:
        print(f"Madison KY: error fetching PDF — {e}")
        return [], {}

    return _parse_pdf(pdf_text, existing)