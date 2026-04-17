import csv
import io
import re
import time
from datetime import date, timedelta

import requests
import anthropic

from scrapers.base import empty_listing, normalize_date, clean_money
from storage import MIN_DAYS_OUT
from config import ANTHROPIC_API_KEY, DEFAULT_HEADERS

CSV_URL = "https://www.jeffcomm.org/docs/webPush.csv"
HANDBILL_BASE = "https://www.jeffcomm.org/docs/handbill/{}.doc"
COUNTY = "Jefferson"
STATE = "KY"

# Jefferson CSV exp4 format: "123 MAIN ST 40202" — zip appended to street, no delimiter
_ZIP_SUFFIX_RE = re.compile(r'^(.+?)\s+(\d{5})\s*$')


def _parse_address(address_raw: str) -> tuple[str, str, str]:
    """
    Split Jefferson KY's exp4 address field into (street, city, zip).

    exp4 contains only the street + zip (no city):
      "123 MAIN ST 40202"  →  ("123 MAIN ST", "Louisville", "40202")
      "456 OAK AVE"        →  ("456 OAK AVE", "Louisville", "")   ← no zip, rare

    City is always Louisville. State is always KY (stored in col B, not here).
    """
    if not address_raw:
        return "", "Louisville", ""

    m = _ZIP_SUFFIX_RE.match(address_raw.strip())
    if m:
        return m.group(1).strip(), "Louisville", m.group(2)

    # No trailing zip found — use the whole string as the street
    return address_raw.strip(), "Louisville", ""


def _parse_case_style(case_style: str):
    """Split 'PLAINTIFF vs. DEFENDANT' into two parts."""
    parts = re.split(r'\s+vs\.?\s+', case_style, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) == 2:
        plaintiff = parts[0].strip()
        defendant = parts[1].strip()
    else:
        plaintiff = case_style.strip()
        defendant = ""

    if "HOLLY M. JOHNSON" in plaintiff or "FINANCE AND ADMINISTRATION CABINET" in plaintiff:
        plaintiff = "Jefferson County Attorney (Code Enforcement Lien)"

    return plaintiff, defendant


def _extract_doc_text(content: bytes) -> str:
    """Pull readable strings from a binary .doc file."""
    chunks = re.findall(b'[\x20-\x7e]{5,}', content)
    return ' '.join(c.decode('ascii', errors='ignore') for c in chunks)


def _fetch_judgment_from_handbill(case_number: str) -> str:
    """
    Fetch the handbill .doc for this case and extract judgment amount.
    Returns a normalized money string, or empty string if unavailable.
    """
    url = HANDBILL_BASE.format(case_number.upper())
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=10)
        if resp.status_code != 200:
            return ""

        raw_text = _extract_doc_text(resp.content)
        if not raw_text:
            return ""

        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": (
                    "This is raw text extracted from a foreclosure sale handbill document.\n"
                    "Find the judgment amount (also called: amount owed, total judgment, "
                    "amount of judgment, judgment for).\n"
                    "Return ONLY the dollar amount as a number, e.g. '123456.78'.\n"
                    "If you cannot find a clear judgment amount, return an empty string.\n\n"
                    f"Text:\n{raw_text[:3000]}"
                )
            }]
        )
        raw = response.content[0].text.strip()
        if raw and not raw.startswith("$"):
            raw = "$" + raw
        return clean_money(raw)

    except Exception as e:
        print(f"  [WARN] Handbill fetch failed for {case_number}: {e}")
        return ""


def scrape(existing: dict[str, int] | None = None) -> tuple[list[dict], dict[int, str]]:
    """
    Returns:
      - new_listings: listings not yet in the sheet (handbills fetched)
      - cancellation_updates: {row_index: "Yes"} for known listings now withdrawn
    """
    new_listings: list[dict] = []
    cancellation_updates: dict[int, str] = {}

    existing = existing or {}

    try:
        resp = requests.get(CSV_URL, headers=DEFAULT_HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[Jefferson KY] Fetch error: {e}")
        return new_listings, cancellation_updates

    reader = csv.DictReader(io.StringIO(resp.text))

    for row in reader:
        case_number   = (row.get("CASE") or "").strip()
        case_style    = (row.get("expr5") or "").strip()
        attorney      = (row.get("ATTORNEY") or "").strip()
        sale_date_raw = (row.get("Expr1") or "").strip()
        address_raw   = (row.get("exp4") or "").strip()
        withdrawn     = (row.get("Expr3") or "").strip().upper()

        if not case_number:
            continue

        # ── Already in sheet — skip handbill, just check cancellation ────
        if case_number in existing:
            row_index, already_cancelled = existing[case_number]
            if withdrawn == "WITHDRAWN" and not already_cancelled:
                cancellation_updates[row_index] = "Yes"
                print(f"  [Jefferson KY] Case {case_number} now WITHDRAWN — flagging row {row_index}")
            continue

        # ── New listing — build it and fetch handbill ────────────────────
        plaintiff, defendant = _parse_case_style(case_style)
        street, city, zip_code = _parse_address(address_raw)
        sale_date = normalize_date(sale_date_raw)

        too_soon = True
        try:
            parsed = date.fromisoformat(sale_date)
            too_soon = parsed < date.today() + timedelta(days=MIN_DAYS_OUT)
        except ValueError:
            pass

        judgment = ""
        if withdrawn != "WITHDRAWN" and not too_soon:
            judgment = _fetch_judgment_from_handbill(case_number)
            time.sleep(1)

        listing = empty_listing(COUNTY, STATE, CSV_URL)
        listing["Case Number"]            = case_number
        listing["Sale Date"]              = sale_date
        listing["Plaintiff"]              = plaintiff
        listing["Defendant(s)"]           = defendant
        listing["Street"]                 = street
        listing["City"]                   = city
        listing["Zip"]                    = zip_code
        listing["Attorney / Firm"]        = attorney
        listing["Judgment / Loan Amount"] = judgment
        listing["Cancelled"]              = "Yes" if withdrawn == "WITHDRAWN" else ""

        new_listings.append(listing)

    print(
        f"[Jefferson KY] {len(new_listings)} new listing(s), "
        f"{len(cancellation_updates)} cancellation(s) to update"
    )
    return new_listings, cancellation_updates