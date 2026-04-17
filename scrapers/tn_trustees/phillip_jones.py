"""
scrapers/tn_trustees/phillip_jones.py
The Law Office of J. Phillip Jones — TN Foreclosure Sale Scraper
https://www.phillipjoneslaw.com/foreclosure-auctions.cfm

J. Phillip Jones and/or Jessica D. Binkley act as substitute trustee across
all TN counties. Their listing page is a plain ColdFusion-rendered HTML table
with no JavaScript dependency. The disclaimer is bypassed by the ?accept=yes
query parameter already present in the known URL.

--- Site architecture (confirmed from live data) ---

  GET https://www.phillipjoneslaw.com/foreclosure-auctions.cfm?accept=yes

  Fully server-rendered HTML. No pagination — all results on one page (~30-40 rows).

  Table columns:
    Case # | Address | County | Sale Date | Sale Time | Status

  Case #:    e.g. "F26-0079"  (use as Case Number in sheet)
  Address:   e.g. "4344 CRANBURY PARK CV. MEMPHIS, TN 38141"
             ALL CAPS, period after some street types, city+state+zip at end.
             Format: "STREET. CITY, STATE ZIP" or "STREET, CITY, STATE ZIP"
  County:    e.g. "Shelby" — pre-parsed, no lookup needed.
  Sale Date: e.g. "03/18/2026"  (MM/DD/YYYY)
  Sale Time: e.g. "10:00 AM"
  Status:    Freeform text, encodes postponements and cancellations:
             "POSTPONED TO 4-28-26 @ 1:00 P.M. SEE WWW.WILLIAMSAUCTION.COM..."
             "SALE CANCELLED SEE WWW.WILLIAMSAUCTION.COM..."
             "Opening Bid $59,863.51"
             "BIDDING INFORMATION WILL NOT BE AVAILABLE PRIOR TO SALE"
             "SEE WWW.WILLIAMSAUCTION.COM FOR DETAILS"

--- Postponement detection ---

Status field is scanned for "POSTPONED TO" followed by a date. If the
postponed date differs from the current Sale Date, it is a postponement.
The new date is extracted from the status text.

--- Cancellation detection ---

"SALE CANCELLED" in the Status field = cancelled. These rows are still
returned in discovery mode so the cancellation can be written to the sheet
if the row already exists there.

--- Two modes ---

  scrape_phillip_jones(existing_addr_set, dry_run)
      Discovery: fetch table, skip cancelled rows, return new listings.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: cross-reference sheet rows by case number (preferred) or address.
      Postponement signal in Status field → postponement.
      "SALE CANCELLED" in Status → cancellation flag.
      Absent within CHECK_WINDOW_DAYS → manual-check flag.
      Returns (postponements, flags).
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

from scrapers.base import empty_listing

logger = logging.getLogger(__name__)

TRUSTEE           = "J. Phillip Jones / Jessica D. Binkley"
STATE             = "TN"
SOURCE_URL        = "https://www.phillipjoneslaw.com/foreclosure-auctions.cfm?accept=yes"
CHECK_WINDOW_DAYS = 14

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Matches "POSTPONED TO 4-28-26" or "POSTPONED TO MAY 13, 2026" etc.
_POSTPONED_RE = re.compile(
    r"POSTPONED\s+TO\s+"
    r"(?:"
    r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})"          # M-D-YY or M/D/YYYY
    r"|"
    r"([A-Z]+\.?\s+\d{1,2},?\s+\d{4})"           # MAY 13, 2026
    r")",
    re.IGNORECASE,
)

_CANCELLED_RE = re.compile(r"SALE\s+CANCELLED|CANCELLED\s+SALE", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------

def _fetch_html() -> Optional[str]:
    """Fetch the listing page. Returns HTML string or None on failure."""
    try:
        resp = requests.get(SOURCE_URL, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        logger.info("[phillip_jones] Fetched page (%.1f KB)", len(resp.content) / 1024)
        return resp.text
    except requests.RequestException as e:
        logger.error("[phillip_jones] Fetch failed: %s", e)
        return None


def _parse_date_str(raw: str) -> str:
    """
    Parse several date formats to ISO YYYY-MM-DD.

    Handles:
      "03/18/2026"     MM/DD/YYYY
      "4-28-26"        M-D-YY  (from postponement text)
      "5-13-26"        M-D-YY
      "MAY 13, 2026"   Month D, YYYY
      "MAY 13 2026"    Month D YYYY
    Returns '' on failure.
    """
    raw = raw.strip().rstrip(".")
    if not raw:
        return ""
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    for fmt in ("%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    logger.debug("[phillip_jones] Unparseable date: %r", raw)
    return ""


def _parse_postponed_date(status: str) -> str:
    """
    Extract the new sale date from a "POSTPONED TO ..." status string.
    Returns ISO date string, or '' if not found/parseable.
    """
    m = _POSTPONED_RE.search(status)
    if not m:
        return ""
    raw = (m.group(1) or m.group(2) or "").strip()
    return _parse_date_str(raw)


def _parse_address(raw: str) -> tuple[str, str, str]:
    """
    Parse "4344 CRANBURY PARK CV. MEMPHIS, TN 38141" → (street, city, zip).

    The address is ALL CAPS. Format varies:
      "STREET. CITY, TN ZIP"          (period before city)
      "STREET, CITY, TN ZIP"          (comma before city)
      "STREET, CITY, TN ZIP"          (multi-word city e.g. WHITE BLUFF)

    Strategy:
      1. Find ", TN " or ". TN " followed by digits — extracts zip.
      2. Everything before that is "STREET[,/.] CITY".
      3. Split street from city at the last ", " or ". " before the state.
    """
    raw = raw.strip()
    raw = raw.replace("\n",", ")

    # Step 1: find state marker + zip
    m = re.search(r"[,.]?\s+TN\s+(\d{4,5})", raw, re.I)
    if not m:
        # No state marker — return as-is, city and zip unknown
        return raw.title(), "", ""

    zip_code     = m.group(1)          # may be 4 or 5 digits; store as-is
    before_state = raw[:m.start()].strip()   # e.g. "4344 CRANBURY PARK CV. MEMPHIS"

    # Step 2: split street from city at last ", " or ". "
    split = before_state.rfind(", ")
    if split < 0:
        split = before_state.rfind(". ")

    if split >= 0:
        street = before_state[:split].strip().title()
        city   = before_state[split + 2:].strip().title()
    else:
        street = before_state.title()
        city   = ""

    return street, city, zip_code


def _parse_table(html: str) -> list[dict]:
    """
    Parse the main foreclosure table. Returns list of row dicts:
      case_num, street, city, county, zip, sale_date, status,
      postponed_date ('' if not postponed), cancelled (bool)
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the results table — look for one with a "Case #" or "County" header
    tables = soup.find_all("table")
    table = None
    for t in tables:
        header_text = t.get_text().lower()
        if "case" in header_text and "county" in header_text and "sale date" in header_text:
            table = t
            break

    if not table:
        # Fallback: largest table
        if tables:
            table = max(tables, key=lambda t: len(t.find_all("tr")))
        else:
            logger.error("[phillip_jones] No table found")
            return []

    rows_el = table.find_all("tr")
    if not rows_el:
        return []

    # Parse header row for column indices
    header = rows_el[0]
    headers = [th.get_text(strip=True).lower() for th in header.find_all(["th", "td"])]
    logger.debug("[phillip_jones] Headers: %s", headers)

    def _col(keywords: list[str], default: int) -> int:
        for i, h in enumerate(headers):
            if any(kw in h for kw in keywords):
                return i
        return default

    case_idx   = _col(["case"],    0)
    addr_idx   = _col(["address"], 1)
    county_idx = _col(["county"],  2)
    date_idx   = _col(["sale date", "date"], 3)
    status_idx = _col(["status"],  5)

    rows = []
    for tr in rows_el[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) < 4:
            continue

        def cell(idx: int) -> str:
            return cells[idx].get_text(separator=" ", strip=True) if idx < len(cells) else ""

        case_num   = cell(case_idx).strip()
        addr_raw   = cell(addr_idx).strip()
        county     = cell(county_idx).strip().title()
        date_raw   = cell(date_idx).strip()
        status     = cell(status_idx).strip()

        if not addr_raw or not date_raw:
            continue

        street, city, zip_code = _parse_address(addr_raw)
        sale_date = _parse_date_str(date_raw)

        if not sale_date:
            logger.debug("[phillip_jones] Skipping — unparseable date: %r", date_raw)
            continue

        postponed_date = _parse_postponed_date(status)
        cancelled      = bool(_CANCELLED_RE.search(status))

        rows.append({
            "case_num":      case_num,
            "street":        street,
            "city":          city,
            "county":        county,
            "zip":           zip_code,
            "sale_date":     sale_date,
            "status":        status,
            "postponed_date": postponed_date,  # '' if not postponed
            "cancelled":     cancelled,
        })

    logger.info("[phillip_jones] Parsed %d row(s) from table", len(rows))
    return rows


def _get_listings() -> list[dict]:
    html = _fetch_html()
    if not html:
        return []
    return _parse_table(html)


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

def scrape_phillip_jones(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        for all active TN rows.

    Returns (new_listings, {}).
    Cancelled rows are skipped in discovery (not yet in sheet = nothing to cancel).
    """
    site_rows = _get_listings()
    if not site_rows:
        return [], {}

    today_str = date.today().isoformat()
    new_listings: list[dict] = []
    stats = {"past": 0, "cancelled": 0, "dup": 0}

    for row in site_rows:
        # Use postponed date if available (that's the active sale date)
        sale_date = row["postponed_date"] if row["postponed_date"] else row["sale_date"]

        if row["cancelled"]:
            stats["cancelled"] += 1
            continue

        if not sale_date or sale_date < today_str:
            stats["past"] += 1
            continue

        street_num   = _street_number(row["street"])
        county_lower = row["county"].lower()

        if (county_lower, street_num, sale_date) in existing_addr_set:
            stats["dup"] += 1
            continue

        listing = empty_listing(county=row["county"], state="TN")
        listing.update({
            "Sale Date":              sale_date,
            "Case Number":            row["case_num"],
            "Plaintiff":              "",
            "Defendant(s)":           "",
            "Street":                 row["street"],
            "City":                   row["city"],
            "Zip":                    row["zip"],
            "Appraised Value":        "",
            "Judgment / Loan Amount": "",
            "Attorney / Firm":        TRUSTEE,
            "Cancelled":              "",
            "Source URL":             SOURCE_URL,
            "Notes": (
                f"Postponed from {row['sale_date']}"
                if row["postponed_date"] else ""
            ),
        })

        new_listings.append(listing)
        if not dry_run:
            existing_addr_set.add((county_lower, street_num, sale_date))

    logger.info(
        "[phillip_jones] Discovery — new=%d past=%d cancelled=%d dup=%d",
        len(new_listings), stats["past"], stats["cancelled"], stats["dup"],
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

    Matching priority:
      1. Case number (most reliable — field is "Case Number" in sheet row dict)
      2. Address fuzzy match (fallback if case number blank)

    Postponement: Status contains "POSTPONED TO <date>" and new date ≠ sheet date.
    Cancellation: Status contains "SALE CANCELLED" → appended as a flag note
                  (actual cancellation write is handled by update_cancellations,
                   not here — we just flag it for manual confirmation).
    Absent within CHECK_WINDOW_DAYS → manual-check flag.

    Returns (postponements, flags).
    """
    if not sheet_rows:
        return [], []

    site_rows = _get_listings()
    if not site_rows:
        logger.warning("[phillip_jones] No rows returned — skipping check")
        return [], []

    # Build case-number lookup for fast matching
    site_by_case: dict[str, dict] = {}
    for r in site_rows:
        if r["case_num"]:
            site_by_case[r["case_num"].upper()] = r

    today      = date.today()
    today_str  = today.isoformat()
    threshold  = (today + timedelta(days=CHECK_WINDOW_DAYS)).isoformat()

    postponements: list[dict] = []
    flags: list[dict] = []

    for sheet_row in sheet_rows:
        sheet_street   = sheet_row.get("Street", "")
        sheet_city     = sheet_row.get("City", "")
        sheet_date     = sheet_row.get("Sale Date", "")
        sheet_case     = (sheet_row.get("Case Number") or "").upper()
        row_index      = sheet_row.get("row_index")

        if not sheet_street or not sheet_date:
            continue
        if sheet_date < today_str:
            continue

        # Attempt match by case number first, then by address
        site_row = site_by_case.get(sheet_case)
        if not site_row:
            site_row = next(
                (r for r in site_rows
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
                        f"⚠️ Manual check — Not found on Phillip Jones site "
                        f"({days_out} day(s) until scheduled sale on {sheet_date})"
                    ),
                })
            continue

        # Found — check for postponement
        new_date = site_row["postponed_date"]
        if new_date and new_date != sheet_date:
            logger.info(
                "[phillip_jones] Postponement: row %s  %s → %s  (%s)",
                row_index, sheet_date, new_date, sheet_street,
            )
            postponements.append({
                "row_index": row_index,
                "old_date":  sheet_date,
                "new_date":  new_date,
                "note": f"Postponed: {sheet_date} → {new_date} (Phillip Jones site)",
            })
            continue

        # Check for cancellation
        if site_row["cancelled"]:
            flags.append({
                "row_index": row_index,
                "note": (
                    f"⚠️ Manual check — Marked CANCELLED on Phillip Jones site "
                    f"(sale was {sheet_date})"
                ),
            })

    logger.info(
        "[phillip_jones] Check — %d postponement(s), %d flag(s) from %d sheet row(s)",
        len(postponements), len(flags), len(sheet_rows),
    )
    return postponements, flags