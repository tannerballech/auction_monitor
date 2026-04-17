"""
scrapers/tn_trustees/foreclosure_postings.py
Internet Posting Company (IPC) — foreclosure-postings.com
Covers: Vylla Solutions Tennessee LLC + The Law Offices of Arnold M. Weiss, PLLC

Platform: Microsoft Power Apps portal. The listing table is rendered inside a
Power Apps iframe and not directly accessible via requests. Playwright navigates
the real browser session, clicks the Download button, intercepts the .xlsx file
before it writes to disk, and parses it with openpyxl.

Excel columns (confirmed from live download 2026-04-13):
  Posting ID | Post Type | Current Sale Date | County | Address | City |
  Law Firm | TS Number | Sale Status

Key fields:
  "Law Firm"          — firm name, allows filtering without PDF fetching
  "Current Sale Date" — active sale date (updated on postponement)
  "Sale Status"       — "Active", "Postponed", "Cancelled" (values inferred;
                        confirm on first run and update _ACTIVE_STATUSES /
                        _POSTPONED_STATUSES / _CANCELLED_STATUSES below)
  "TS Number"         — trustee file number; stored as Case Number in sheet
  "County"            — county name (no "County" suffix)
  "Address"           — street address only
  "City"              — city name

--- Playwright notes ---

The Power Apps portal requires a full browser session to load. headless=False
is used to avoid bot-detection issues. The portal appears to be publicly
accessible (no login required) but requires JS execution.

Power Apps portals can be slow to initialize. The scraper waits for the
download button to become visible with a generous timeout. If the button
selector changes, update _DOWNLOAD_BTN_SELECTOR below.

--- Two modes ---

  scrape_foreclosure_postings(existing_addr_set, dry_run)
      Discovery: download Excel, filter to _TARGET_FIRMS and active/future rows,
      return new listings not already in sheet.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: download Excel, match sheet rows by TS Number or address.
      Sale Status = "Postponed" + Current Sale Date ≠ sheet date → postponement.
      Absent within CHECK_WINDOW_DAYS → manual-check flag.
      Returns (postponements, flags).
"""

from __future__ import annotations

import io
import logging
import re
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import openpyxl
import requests

from scrapers.base import empty_listing
from scrapers.tn_trustees.registry import TRUSTEE_REGISTRY

logger = logging.getLogger(__name__)

SOURCE_URL        = "https://www.foreclosure-postings.com/Tennessee/"
STATE             = "TN"
CHECK_WINDOW_DAYS = 14

# ── TODO: Confirm these after first run ──────────────────────────────────────
# Run with dry_run=True and check what values appear in the Sale Status column.
# Update these sets to match the actual values used by the site.
_ACTIVE_STATUSES    = {"active", ""}          # blank may also mean active
_POSTPONED_STATUSES = {"postponed"}
_CANCELLED_STATUSES = {"cancelled", "cancel"}
# ─────────────────────────────────────────────────────────────────────────────

# Registry keys for firms on this platform.
# The Law Firm column value from the Excel is matched against these patterns.
_TARGET_FIRMS: dict[str, str] = {
    # registry_key → canonical name (for display)
    "vylla":        "Vylla Solutions Tennessee LLC",
    "arnold_weiss": "The Law Offices of Arnold M. Weiss, PLLC",
}

# Regex patterns to match Law Firm column values → registry key
_FIRM_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"vylla",                    re.I), "vylla"),
    (re.compile(r"weiss|arnold.*weiss",       re.I), "arnold_weiss"),
]

# ── TODO: Confirm the download button selector after first Playwright run ────
# Open the page in Chrome DevTools, find the download button element, and
# copy its selector here. Common Power Apps button selectors:
#   "button:has-text('Download')"
#   "button[aria-label='Download']"
#   "[data-control-name='download_button']"
_DOWNLOAD_BTN_SELECTOR = "button:has-text('Download')"

_PLAYWRIGHT_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
]


# ---------------------------------------------------------------------------
# Excel download via Playwright
# ---------------------------------------------------------------------------

def _download_excel_playwright() -> Optional[bytes]:
    """
    Navigate to the IPC Tennessee page in Playwright, click Download,
    intercept the file, and return the raw Excel bytes.
    Returns None on any error.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("[foreclosure_postings] Playwright not installed")
        return None

    excel_bytes: Optional[bytes] = None

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, args=_PLAYWRIGHT_LAUNCH_ARGS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            logger.info("[foreclosure_postings] Navigating to %s", SOURCE_URL)
            page.goto(SOURCE_URL, timeout=60_000)

            # Power Apps can be very slow to initialize — wait for networkidle
            # then also wait for the download button to appear
            page.wait_for_load_state("networkidle", timeout=60_000)

            # Extra wait for Power Apps JS framework to finish rendering
            time.sleep(3)

            # Wait for download button to be visible
            try:
                page.wait_for_selector(
                    _DOWNLOAD_BTN_SELECTOR,
                    state="visible",
                    timeout=30_000,
                )
                logger.info("[foreclosure_postings] Download button found")
            except Exception as e:
                logger.error(
                    "[foreclosure_postings] Download button not found with selector %r: %s",
                    _DOWNLOAD_BTN_SELECTOR, e,
                )
                # Log all buttons visible for debugging
                buttons = page.query_selector_all("button")
                logger.debug(
                    "[foreclosure_postings] Visible buttons: %s",
                    [b.inner_text() for b in buttons[:10]],
                )
                return None

            # Intercept the download
            with page.expect_download(timeout=30_000) as download_info:
                page.click(_DOWNLOAD_BTN_SELECTOR)

            download = download_info.value
            logger.info(
                "[foreclosure_postings] Download started: %s", download.suggested_filename
            )

            # Save to temp file and read bytes
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp_path = Path(tmp.name)

            download.save_as(str(tmp_path))
            excel_bytes = tmp_path.read_bytes()
            tmp_path.unlink(missing_ok=True)

            logger.info(
                "[foreclosure_postings] Excel downloaded (%.1f KB)",
                len(excel_bytes) / 1024,
            )

        except Exception as e:
            logger.error("[foreclosure_postings] Playwright error: %s", e)

        finally:
            context.close()
            browser.close()

    return excel_bytes


# ---------------------------------------------------------------------------
# Excel parsing
# ---------------------------------------------------------------------------

def _resolve_firm(law_firm_raw: str) -> Optional[str]:
    """
    Match a raw Law Firm cell value to a registry key.
    Returns registry key or None if not in _TARGET_FIRMS.
    """
    for pattern, key in _FIRM_PATTERNS:
        if pattern.search(law_firm_raw):
            return key
    return None


def _parse_date(val) -> str:
    """
    Parse an Excel cell value (datetime, date, or string) to ISO YYYY-MM-DD.
    Returns '' on failure.
    """
    if val is None:
        return ""
    if isinstance(val, (datetime, date)):
        return val.strftime("%Y-%m-%d") if isinstance(val, datetime) else val.isoformat()
    raw = str(val).strip()
    if not raw:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    logger.debug("[foreclosure_postings] Unparseable date: %r", raw)
    return ""


def _parse_excel(excel_bytes: bytes) -> list[dict]:
    """
    Parse the downloaded Excel using pandas (more tolerant of style quirks
    than raw openpyxl, which rejects certain Fill types in Power Apps exports).
    """
    import pandas as pd

    try:
        df = pd.read_excel(io.BytesIO(excel_bytes), engine="calamine", dtype=str)
    except Exception as e:
        logger.error("[foreclosure_postings] pandas failed to read Excel: %s", e)
        return []

    # Normalise column names to lowercase for robust matching
    df.columns = [str(c).lower().strip() for c in df.columns]
    logger.debug("[foreclosure_postings] Excel columns: %s", list(df.columns))

    seen_status_values: set[str] = set()
    rows = []

    for _, series in df.iterrows():
        def cell(*candidates: str) -> str:
            for key in candidates:
                val = series.get(key)
                if val is not None and str(val).strip() not in ("", "nan", "NaT"):
                    return str(val).strip()
            return ""

        law_firm_raw = cell("law firm", "law_firm", "firm")
        registry_key = _resolve_firm(law_firm_raw)

        sale_date_raw = cell("current sale date", "sale date", "saledate", "current_sale_date")
        sale_date = _parse_date(sale_date_raw)

        status_raw = cell("sale status", "sale_status", "status").lower()
        seen_status_values.add(status_raw)

        rows.append({
            "posting_id":   cell("posting id", "posting_id"),
            "post_type":    cell("post type", "post_type"),
            "sale_date":    sale_date,
            "county":       cell("county").strip().title(),
            "street":       cell("address"),
            "city":         cell("city"),
            "law_firm_raw": law_firm_raw,
            "registry_key": registry_key,
            "ts_number":    cell("ts number", "ts_number", "ts #"),
            "sale_status":  status_raw,
        })

    if seen_status_values:
        logger.info(
            "[foreclosure_postings] Observed Sale Status values: %s",
            seen_status_values,
        )

    logger.info("[foreclosure_postings] Parsed %d row(s) from Excel", len(rows))
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


def _addresses_match(a_st: str, a_city: str, b_st: str, b_city: str) -> bool:
    num_a, num_b = _street_number(a_st), _street_number(b_st)
    if not num_a or not num_b or num_a != num_b:
        return False
    if _street_first_word(a_st) != _street_first_word(b_st):
        return False
    ca, cb = a_city.lower().strip(), b_city.lower().strip()
    if ca and cb and ca != cb:
        return False
    return True


# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def scrape_foreclosure_postings(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode. Downloads Excel, filters to target firms and future
    active listings, returns rows not already in the sheet.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples.
    Returns (new_listings, {}).
    """
    excel_bytes = _download_excel_playwright()
    if not excel_bytes:
        logger.warning("[foreclosure_postings] No Excel data — skipping discovery")
        return [], {}

    all_rows = _parse_excel(excel_bytes)
    today_str = date.today().isoformat()
    new_listings: list[dict] = []
    stats = {"other_firm": 0, "cancelled": 0, "past": 0, "dup": 0}

    for row in all_rows:
        # Only target firms
        if row["registry_key"] not in _TARGET_FIRMS:
            stats["other_firm"] += 1
            continue

        # Skip cancelled
        if row["sale_status"] in _CANCELLED_STATUSES:
            stats["cancelled"] += 1
            continue

        # Skip past dates
        sale_date = row["sale_date"]
        if not sale_date or sale_date < today_str:
            stats["past"] += 1
            continue

        # Cross-source dedup
        street_num   = _street_number(row["street"])
        county_lower = row["county"].lower()
        if (county_lower, street_num, sale_date) in existing_addr_set:
            stats["dup"] += 1
            continue

        registry_key = row["registry_key"]
        trustee_name = TRUSTEE_REGISTRY.get(registry_key, {}).get(
            "canonical_name", row["law_firm_raw"]
        )

        listing = empty_listing(county=row["county"], state="TN")
        listing.update({
            "Sale Date":              sale_date,
            "Case Number":            row["ts_number"],
            "Plaintiff":              "",
            "Defendant(s)":           "",
            "Street":                 row["street"],
            "City":                   row["city"],
            "Zip":                    "",   # not in Excel columns
            "Appraised Value":        "",
            "Judgment / Loan Amount": "",
            "Attorney / Firm":        trustee_name,
            "Cancelled":              "",
            "Source URL":             SOURCE_URL,
            "Notes":                  (
                "Postponed" if row["sale_status"] in _POSTPONED_STATUSES else ""
            ),
        })

        new_listings.append(listing)
        if not dry_run:
            existing_addr_set.add((county_lower, street_num, sale_date))

    logger.info(
        "[foreclosure_postings] Discovery — new=%d other_firm=%d "
        "cancelled=%d past=%d dup=%d",
        len(new_listings), stats["other_firm"], stats["cancelled"],
        stats["past"], stats["dup"],
    )
    return new_listings, {}


# ---------------------------------------------------------------------------
# Check mode
# ---------------------------------------------------------------------------

def check_existing(
    sheet_rows: list[dict],
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Check mode. Detect postponements and cancellations for existing sheet rows.

    Matching: TS Number (Case Number in sheet) first, then address fuzzy match.

    Postponement: Sale Status = "Postponed" AND Current Sale Date ≠ sheet date.
      The Current Sale Date in the Excel is taken as the new rescheduled date.

    Absent within CHECK_WINDOW_DAYS → manual-check flag.

    Returns (postponements, flags).
    """
    if not sheet_rows:
        return [], []

    excel_bytes = _download_excel_playwright()
    if not excel_bytes:
        logger.warning("[foreclosure_postings] No Excel data — skipping check")
        return [], []

    all_rows = _parse_excel(excel_bytes)

    # Build lookup by TS number
    site_by_ts: dict[str, dict] = {}
    for r in all_rows:
        if r["ts_number"]:
            site_by_ts[r["ts_number"].upper()] = r

    today      = date.today()
    today_str  = today.isoformat()
    threshold  = (today + timedelta(days=CHECK_WINDOW_DAYS)).isoformat()

    postponements: list[dict] = []
    flags: list[dict] = []

    for sheet_row in sheet_rows:
        sheet_street = sheet_row.get("Street", "")
        sheet_city   = sheet_row.get("City", "")
        sheet_date   = sheet_row.get("Sale Date", "")
        sheet_ts     = (sheet_row.get("Case Number") or "").upper()
        row_index    = sheet_row.get("row_index")

        if not sheet_street or not sheet_date or sheet_date < today_str:
            continue

        # Match by TS number first, fallback to address
        site_row = site_by_ts.get(sheet_ts)
        if not site_row:
            site_row = next(
                (r for r in all_rows
                 if _addresses_match(r["street"], r["city"], sheet_street, sheet_city)),
                None,
            )

        if site_row is None:
            # Not found on site
            if sheet_date <= threshold:
                days_out = (date.fromisoformat(sheet_date) - today).days
                flags.append({
                    "row_index": row_index,
                    "note": (
                        f"⚠️ Manual check — Not found on IPC site "
                        f"({days_out} day(s) until scheduled sale on {sheet_date})"
                    ),
                })
            continue

        # Check for postponement
        if site_row["sale_status"] in _POSTPONED_STATUSES:
            new_date = site_row["sale_date"]  # Current Sale Date = rescheduled date
            if new_date and new_date != sheet_date:
                logger.info(
                    "[foreclosure_postings] Postponement: row %s  %s → %s  (%s)",
                    row_index, sheet_date, new_date, sheet_street,
                )
                postponements.append({
                    "row_index": row_index,
                    "old_date":  sheet_date,
                    "new_date":  new_date,
                    "note": (
                        f"Postponed: {sheet_date} → {new_date} "
                        f"(IPC / foreclosure-postings.com)"
                    ),
                })

    logger.info(
        "[foreclosure_postings] Check — %d postponement(s), %d flag(s) from %d row(s)",
        len(postponements), len(flags), len(sheet_rows),
    )
    return postponements, flags