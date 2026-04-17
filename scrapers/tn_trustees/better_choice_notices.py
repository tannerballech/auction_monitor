"""
scrapers/tn_trustees/better_choice_notices.py
BetterChoiceNotices.com — TN Foreclosure Sale Scraper
https://betterchoicenotices.com

Multi-firm statutory internet posting platform. Used by many TN law firms/
trustees. Currently targeted firms:
  - LOGS Legal Group, LLP  (registry: "llg_trustee")

Other firms on the platform (already covered by other scrapers or too small):
  Rubin Lublin LLC        → rubin_lublin.py     (covered)
  RAS                     → robertson_anschutz  (covered)
  Brock & Scott PLLC      → brock_scott.py      (covered)
  Aldridge Pite LLC       → Clear Recon rows    (duplicates via dedup)
  Wilson Worley            → 3 rows, no scraper
  Peaseley & Derryberry    → 2 rows, no scraper
  Solomon Baggett LLC      → 2 rows, no scraper
  Hodges Doughty & Carson  → 2 rows, no scraper
  Stubbs Law Group         → 1 row,  no scraper
  Middle TN Law Group      → 1 row,  no scraper
  SoBro Law Group          → 1 row,  no scraper
  Ingle Law Firm           → 1 row,  no scraper

To add a firm: append its exact customer_name string to _TARGET_FIRMS.

--- API (confirmed via DevTools + probe, 2026-04-13) ---

  GET https://api.betterchoicenotices.com/api/notices/
  Params:
    stateId=44          Tennessee's state ID
    page=N              1-indexed
    page_size=100       max observed without error; default UI is 25
    searchFromDate=YYYY-MM-DD   } filter notices whose posting window
    searchToDate=YYYY-MM-DD     } includes this date range; both=today
                                  returns all currently-active notices

  Each row dict:
    "id"                    BCN internal ID (int)
    "county_name"           "Madison" — proper case, no "County" suffix
    "state_code"            "TN"
    "property_address"      EITHER "123 Main St, Nashville, TN 37201"
                            OR     "123 MAIN ST"  (street only)
    "sale_date"             "2026-05-05"  — ISO YYYY-MM-DD
    "postponed_sale_date"   "2026-07-02" or null
    "is_postponement"       1 if this row itself IS a postponement notice, else 0
    "cancelled"             null or a truthy value
    "customer_name"         firm that submitted — use for filtering
    "law_firm_case_number"  trustee/law firm's file number → Case Number
    "row_count"             total rows matching query (present on every row)

--- Postponement logic ---

`postponed_sale_date` non-null = this sale has been moved to that date.
`sale_date` on the row is still the ORIGINAL scheduled date.
The rescheduled date is `postponed_sale_date`.

Discovery: import rows where cancelled is falsy. Use postponed_sale_date
as the active sale date if set, otherwise use sale_date.

Check mode: same row, postponed_sale_date differs from sheet's Sale Date
→ postponement detected.

--- Address handling ---

Two formats observed:
  Full:        "31 Flaxen Cove Jackson, TN 38305"
  Street-only: "3833 ROLLINGWOOD DR"

Parser tries comma-split for full addresses. Street-only rows get county
from county_name but city/zip left blank (valuation still possible by
county+street geocode in BatchData).

--- Two modes ---

  scrape_better_choice_notices(existing_addr_set, dry_run)
      Discovery: fetch all TN pages, filter to _TARGET_FIRMS, return new
      listings not already in sheet.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: fetch all TN pages, match sheet rows by case number or address.
      postponed_sale_date non-null and differs from sheet date → postponement.
      Absent within CHECK_WINDOW_DAYS → manual-check flag.
      Returns (postponements, flags).
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional

import requests

from scrapers.base import empty_listing
from scrapers.tn_trustees.registry import TRUSTEE_REGISTRY

logger = logging.getLogger(__name__)

API_BASE          = "https://api.betterchoicenotices.com"
LISTINGS_ENDPOINT = API_BASE + "/api/notices/"
SOURCE_URL        = "https://betterchoicenotices.com/"
STATE_ID          = 44          # Tennessee
PAGE_SIZE         = 100
CHECK_WINDOW_DAYS = 14

# customer_name values → registry key for firms we want to scrape.
# Add entries here as new firms are confirmed on this platform.
_TARGET_FIRMS: dict[str, str] = {
    "LOGS Legal Group, LLP": "llg_trustee",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": SOURCE_URL,
}


# ---------------------------------------------------------------------------
# API fetch — paginated
# ---------------------------------------------------------------------------

def _fetch_all_rows() -> list[dict]:
    """
    Fetch all TN notices from the BCN API, paginating until exhausted.
    Uses today's date as both searchFromDate and searchToDate, which returns
    all notices whose active posting window includes today (i.e., all currently
    scheduled sales).
    Returns [] on any error.
    """
    today = date.today().isoformat()
    all_rows: list[dict] = []
    page = 1

    while True:
        params = {
            "stateId":        STATE_ID,
            "page":           page,
            "page_size":      PAGE_SIZE,
            "searchFromDate": today,
            "searchToDate":   today,
        }
        try:
            resp = requests.get(
                LISTINGS_ENDPOINT,
                params=params,
                headers=_HEADERS,
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error("[better_choice_notices] Fetch error (page %d): %s", page, e)
            break
        except ValueError as e:
            logger.error("[better_choice_notices] JSON error (page %d): %s", page, e)
            break

        if not isinstance(data, list) or not data:
            break

        all_rows.extend(data)

        # row_count tells us the total — stop when we have everything
        total = data[0].get("row_count", 0)
        logger.info(
            "[better_choice_notices] Page %d: %d rows (total %d/%d)",
            page, len(data), len(all_rows), total,
        )

        if len(all_rows) >= total or len(data) < PAGE_SIZE:
            break

        page += 1

    # Log all unique customer names seen — useful for discovering new firms
    seen_customers = sorted({r.get("customer_name", "") for r in all_rows})
    logger.info("[better_choice_notices] Customer names seen: %s", seen_customers)

    return all_rows


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------

def _parse_address(raw: str) -> tuple[str, str, str]:
    """
    Parse property_address into (street, city, zip).

    Full format:  "31 Flaxen Cove Jackson, TN 38305"
    Street-only:  "3833 ROLLINGWOOD DR"  (city/zip left blank)

    Strategy: look for ", TN " pattern to identify full addresses.
    """
    raw = raw.strip()
    if not raw:
        return "", "", ""

    # Full address: ends with ", TN XXXXX" or has ", CITY, TN XXXXX"
    m = re.search(r",\s*TN\s+(\d{5})\s*$", raw, re.I)
    if m:
        zip_code = m.group(1)
        before   = raw[:m.start()].strip()
        # before = "31 Flaxen Cove Jackson" or "31 Flaxen Cove, Jackson"
        # Split on last comma to get street vs city
        if "," in before:
            parts  = before.rsplit(",", 1)
            street = parts[0].strip().title()
            city   = parts[1].strip().title()
        else:
            # No comma: try to split on last space-group before city token
            # Heuristic: last word before TN is city, rest is street
            # This handles "31 Flaxen Cove Jackson" → street="31 Flaxen Cove", city="Jackson"
            words  = before.rsplit(None, 1)
            street = words[0].strip().title() if len(words) > 1 else before.title()
            city   = words[1].strip().title() if len(words) > 1 else ""
        return street, city, zip_code

    # Street-only — return as-is (title-cased), no city/zip
    return raw.title(), "", ""


def _resolve_registry_key(customer_name: str) -> Optional[str]:
    """Map a customer_name string to a registry key. Returns None if not targeted."""
    return _TARGET_FIRMS.get(customer_name)


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

def scrape_better_choice_notices(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        for all active TN rows — prevents cross-source duplicates.

    Returns (new_listings, {}).
    """
    raw_rows = _fetch_all_rows()
    if not raw_rows:
        return [], {}

    today_str = date.today().isoformat()
    new_listings: list[dict] = []
    stats = {"other_firm": 0, "cancelled": 0, "past": 0, "dup": 0}

    for row in raw_rows:
        customer = row.get("customer_name", "")
        registry_key = _resolve_registry_key(customer)

        if registry_key is None:
            stats["other_firm"] += 1
            continue

        # Skip cancelled
        if row.get("cancelled"):
            stats["cancelled"] += 1
            continue

        # Active sale date: use postponed date if set, else original
        orig_date      = row.get("sale_date", "")
        postponed_date = row.get("postponed_sale_date") or ""
        sale_date      = postponed_date if postponed_date else orig_date

        if not sale_date or sale_date < today_str:
            stats["past"] += 1
            continue

        county = row.get("county_name", "").strip()
        street, city, zip_code = _parse_address(row.get("property_address", ""))

        if not street:
            logger.debug(
                "[better_choice_notices] Skipping — no street: %r",
                row.get("property_address"),
            )
            continue

        # Cross-source dedup
        street_num   = _street_number(street)
        county_lower = county.lower()
        if (county_lower, street_num, sale_date) in existing_addr_set:
            stats["dup"] += 1
            continue

        trustee_name = TRUSTEE_REGISTRY.get(registry_key, {}).get(
            "canonical_name", customer
        )

        listing = empty_listing(county=county, state="TN")
        listing.update({
            "Sale Date":              sale_date,
            "Case Number":            row.get("law_firm_case_number", ""),
            "Plaintiff":              "",
            "Defendant(s)":           "",
            "Street":                 street,
            "City":                   city,
            "Zip":                    zip_code,
            "Appraised Value":        "",
            "Judgment / Loan Amount": "",
            "Attorney / Firm":        trustee_name,
            "Cancelled":              "",
            "Source URL":             SOURCE_URL,
            "Notes": (
                f"Postponed from {orig_date}" if postponed_date else ""
            ),
        })

        new_listings.append(listing)
        if not dry_run:
            existing_addr_set.add((county_lower, street_num, sale_date))

    logger.info(
        "[better_choice_notices] Discovery — new=%d other_firm=%d "
        "cancelled=%d past=%d dup=%d",
        len(new_listings), stats["other_firm"],
        stats["cancelled"], stats["past"], stats["dup"],
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
    Check mode. Detect postponements and absences for LLG sheet rows.

    Matching: law_firm_case_number (Case Number in sheet) first,
    then address fuzzy match.

    Postponement: postponed_sale_date is set and differs from sheet date.
    Absent within CHECK_WINDOW_DAYS → manual-check flag.

    Returns (postponements, flags).
    """
    if not sheet_rows:
        return [], []

    raw_rows = _fetch_all_rows()
    if not raw_rows:
        logger.warning("[better_choice_notices] No rows returned — skipping check")
        return [], []

    today      = date.today()
    today_str  = today.isoformat()
    threshold  = (today + timedelta(days=CHECK_WINDOW_DAYS)).isoformat()

    # Build fast lookup by case number
    site_by_case: dict[str, dict] = {}
    for r in raw_rows:
        cn = (r.get("law_firm_case_number") or "").upper()
        if cn:
            site_by_case[cn] = r

    postponements: list[dict] = []
    flags: list[dict] = []

    for sheet_row in sheet_rows:
        sheet_street = sheet_row.get("Street", "")
        sheet_city   = sheet_row.get("City", "")
        sheet_date   = sheet_row.get("Sale Date", "")
        sheet_case   = (sheet_row.get("Case Number") or "").upper()
        row_index    = sheet_row.get("row_index")

        if not sheet_street or not sheet_date or sheet_date < today_str:
            continue

        # Match by case number first, then address
        site_row = site_by_case.get(sheet_case)
        if not site_row:
            site_street_parsed = None
            for r in raw_rows:
                st, city, _ = _parse_address(r.get("property_address", ""))
                if _addresses_match(st, city, sheet_street, sheet_city):
                    site_row = r
                    break

        if site_row is None:
            if sheet_date <= threshold:
                days_out = (date.fromisoformat(sheet_date) - today).days
                flags.append({
                    "row_index": row_index,
                    "note": (
                        f"⚠️ Manual check — Not found on BetterChoiceNotices "
                        f"({days_out} day(s) until scheduled sale on {sheet_date})"
                    ),
                })
            continue

        # Check for postponement
        postponed_date = site_row.get("postponed_sale_date") or ""
        if postponed_date and postponed_date != sheet_date:
            logger.info(
                "[better_choice_notices] Postponement: row %s  %s → %s  (%s)",
                row_index, sheet_date, postponed_date, sheet_street,
            )
            postponements.append({
                "row_index": row_index,
                "old_date":  sheet_date,
                "new_date":  postponed_date,
                "note": (
                    f"Postponed: {sheet_date} → {postponed_date} "
                    f"(BetterChoiceNotices)"
                ),
            })

    logger.info(
        "[better_choice_notices] Check — %d postponement(s), %d flag(s) from %d row(s)",
        len(postponements), len(flags), len(sheet_rows),
    )
    return postponements, flags