"""
skiptrace.py — Phase 3: BatchData skip trace integration.

Reads listings from the Auctions sheet that:
  - Have equity signal ✅ or 🏆
  - Are upcoming (Sale Date >= today)
  - Are not marked Cancelled
  - Have not already been skip traced (Skip Trace Date column is blank)

Writes results back to the sheet in columns T–Z:
  T: Owner Name (Primary)
  U: Owner Name (Secondary)
  V: Owner Phone(s)      — comma-separated, mobile numbers first, DNC excluded
  W: Owner Email(s)      — comma-separated
  X: Mailing Address     — "street, city, state zip"
  Y: Deceased            — "Yes" / "No" / "" (unknown / no match)
  Z: Skip Trace Date     — YYYY-MM-DD

Confirmed BatchData response shape (from live test 2026-04-02):
  raw["results"]["persons"]            — list of person records
  person["name"]["full"]               — full name string
  person["name"]["first/last/middle"]  — name components
  person["phoneNumbers"]               — list: {number, type, dnc, reachable, score, ...}
  person["emails"]                     — list (may be empty)
  person["mailingAddress"]             — {street, city, state, zip, ...}
  person["death"]["deceased"]          — bool
  person["meta"]["matched"]            — bool (False = no match for this address)

Usage (called from main.py --skiptrace):
    from skiptrace import run_skiptraces
    updated = run_skiptraces(listings, dry_run=False)
"""

import logging
import time
from datetime import date

import requests

from config import BATCHDATA_API_KEY

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

SKIP_TRACE_URL  = "https://api.batchdata.com/api/v1/property/skip-trace"
CALL_DELAY_SECS = 2      # pause between API calls
QUALIFYING_SIGNALS = {"🏆", "✅"}


# ── Public entry point ────────────────────────────────────────────────────────

def run_skiptraces(listings: list[dict], dry_run: bool = False) -> list[dict]:
    """
    Skip trace each listing in `listings`.

    `listings` is the list returned by sheets_writer.get_listings_needing_skiptrace().
    Each dict has at minimum: Street, City, State, Zip, _row_index.

    Returns a list of result dicts (one per listing), each containing the
    original listing dict merged with skip trace fields, plus:
        "_row_index"   — sheet row (1-indexed, passed through from input)
        "_skipped"     — True if the API call was skipped (dry run or hard error)
        "_error"       — error message string if the call failed, else None

    sheets_writer.update_skiptraces() consumes this list.
    """
    results = []

    for i, listing in enumerate(listings):
        street  = listing.get("Street", "").strip()
        city    = listing.get("City", "").strip()
        state   = listing.get("State", "").strip()
        zip_    = listing.get("Zip", "").strip()
        address_display = f"{street}, {city}, {state} {zip_}".strip(", ")

        if dry_run:
            logger.info(f"  [DRY RUN] Would skip trace: {address_display}")
            results.append({**listing, "_skipped": True, "_error": None})
            continue

        if not street:
            logger.warning(f"  Skipping row {listing.get('_row_index')}: no street address.")
            results.append({**listing, "_skipped": True, "_error": "No street address"})
            continue

        logger.info(f"  [{i+1}/{len(listings)}] Skip tracing: {address_display}")

        try:
            raw    = _call_skip_trace(street, city, state, zip_)
            parsed = _parse_response(raw, listing)
            results.append(parsed)
            owner = parsed.get("Owner Name (Primary)") or "(no match)"
            logger.info(f"    → {owner}")
        except Exception as e:
            logger.error(f"    ERROR: {e}")
            results.append({**listing, "_skipped": True, "_error": str(e)})

        if i < len(listings) - 1:
            time.sleep(CALL_DELAY_SECS)

    return results


# ── API call ──────────────────────────────────────────────────────────────────

def _call_skip_trace(street: str, city: str, state: str, zip_code: str) -> dict:
    """POST to BatchData skip trace endpoint. Returns parsed JSON."""
    payload = {
        "requests": [
            {
                "propertyAddress": {
                    "street": street,
                    "city":   city,
                    "state":  state,
                    "zip":    zip_code,
                }
            }
        ]
    }
    headers = {
        "Authorization": f"Bearer {BATCHDATA_API_KEY}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }
    resp = requests.post(SKIP_TRACE_URL, json=payload, headers=headers, timeout=30)

    if resp.status_code == 402:
        raise RuntimeError(
            "BatchData returned 402 Payment Required — skip trace may not be "
            "enabled on your current plan."
        )
    if resp.status_code == 429:
        raise RuntimeError("BatchData rate limit hit (429). Slow down call frequency.")
    if resp.status_code != 200:
        raise RuntimeError(f"BatchData HTTP {resp.status_code}: {resp.text[:300]}")

    return resp.json()


# ── Response parsing ──────────────────────────────────────────────────────────

def _parse_response(raw: dict, listing: dict) -> dict:
    """
    Extract owner data from a BatchData skip trace response.

    Confirmed shape:
        raw["results"]["persons"]  — list of person records
        raw["results"]["meta"]     — request-level metadata

    Multiple persons in the list = multiple owners. We treat persons[0] as
    primary and persons[1] (if present) as secondary.
    """
    result = {**listing, "_skipped": False, "_error": None}
    today  = date.today().isoformat()

    persons = raw.get("results", {}).get("persons", [])

    if not persons:
        logger.info("    No persons found for this address.")
        result.update(_empty_skiptrace_fields(today))
        return result

    # Filter to matched persons only (meta.matched may be absent — default True)
    matched = [p for p in persons if p.get("meta", {}).get("matched", True)]
    if not matched:
        logger.info("    Address found but no owner match returned.")
        result.update(_empty_skiptrace_fields(today))
        return result

    primary   = matched[0]
    secondary = matched[1] if len(matched) > 1 else None

    # ── Names ─────────────────────────────────────────────────────────────────
    result["Owner Name (Primary)"]   = _extract_name(primary)
    result["Owner Name (Secondary)"] = _extract_name(secondary) if secondary else ""

    # ── Phones — collect across all matched persons, mobile first, DNC excluded
    all_phones = []
    for person in matched:
        all_phones.extend(_extract_phones(person))

    seen_phones: set[str] = set()
    deduped_phones = []
    for p in all_phones:
        if p not in seen_phones:
            seen_phones.add(p)
            deduped_phones.append(p)
    result["Owner Phone(s)"] = ", ".join(deduped_phones)

    # ── Emails — collect across all matched persons ───────────────────────────
    all_emails = []
    for person in matched:
        all_emails.extend(_extract_emails(person))

    seen_emails: set[str] = set()
    deduped_emails = []
    for e in all_emails:
        if e not in seen_emails:
            seen_emails.add(e)
            deduped_emails.append(e)
    result["Owner Email(s)"] = ", ".join(deduped_emails)

    # ── Mailing address — from primary owner ─────────────────────────────────
    result["Mailing Address"] = _extract_mailing_address(primary)

    # ── Deceased — flag Yes if ANY matched owner is deceased ─────────────────
    deceased = any(_extract_deceased(p) for p in matched)
    result["Deceased"] = "Yes" if deceased else "No"

    result["Skip Trace Date"] = today

    return result


# ── Field extraction helpers ──────────────────────────────────────────────────

def _extract_name(person: dict) -> str:
    """
    Extract full name from a person record.
    BatchData: person["name"]["full"] / ["first"] / ["last"] / ["middle"]
    """
    if not person:
        return ""
    name = person.get("name", {})
    if isinstance(name, str):
        return name.strip().title()
    full = name.get("full", "").strip()
    if full:
        return full.title()
    parts = [name.get("first", ""), name.get("last", "")]
    return " ".join(p for p in parts if p).strip().title()


def _extract_phones(person: dict) -> list[str]:
    """
    Extract phone numbers from a person record.
    BatchData: person["phoneNumbers"][]{number, type, dnc, reachable, score}

    Filters out DNC=True and reachable=False numbers.
    Sorts mobile/cell before landline.
    """
    raw_phones = person.get("phoneNumbers", [])
    if not raw_phones:
        return []

    valid = []
    for p in raw_phones:
        if not isinstance(p, dict):
            continue
        if p.get("dnc", False):
            continue
        if p.get("reachable") is False:
            continue
        number = p.get("number", "").strip()
        if not number:
            continue
        valid.append(p)

    def sort_key(p):
        t = (p.get("type") or "").lower()
        if "mobile" in t or "cell" in t:
            return 0
        if "land" in t:
            return 2
        return 1

    valid.sort(key=sort_key)
    return [_format_phone(p["number"]) for p in valid]


def _format_phone(raw: str) -> str:
    """Format a 10-digit number as (XXX) XXX-XXXX. Returns raw if unparseable."""
    digits = "".join(c for c in raw if c.isdigit())
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return raw


def _extract_emails(person: dict) -> list[str]:
    """
    Extract email addresses from a person record.
    BatchData: person["emails"] — list of strings or dicts.
    """
    raw = person.get("emails", [])
    if not raw:
        return []
    result = []
    for e in raw:
        if isinstance(e, dict):
            addr = e.get("email") or e.get("address") or e.get("value") or ""
        else:
            addr = str(e)
        addr = addr.strip().lower()
        if addr and "@" in addr:
            result.append(addr)
    return result


def _extract_mailing_address(person: dict) -> str:
    """
    Extract mailing address from a person record.
    BatchData: person["mailingAddress"]{street, city, state, zip}
    """
    mailing = person.get("mailingAddress", {})
    if not mailing or not isinstance(mailing, dict):
        return ""
    street = mailing.get("street", "").strip()
    city   = mailing.get("city", "").strip()
    state  = mailing.get("state", "").strip()
    zip_   = mailing.get("zip", "").strip()
    parts  = [p for p in [street, city] if p]
    state_zip = " ".join(p for p in [state, zip_] if p)
    if state_zip:
        parts.append(state_zip)
    return ", ".join(parts)


def _extract_deceased(person: dict) -> bool:
    """
    Check deceased flag.
    BatchData: person["death"]["deceased"] — bool
    """
    death = person.get("death", {})
    if not death or not isinstance(death, dict):
        return False
    val = death.get("deceased", False)
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "yes", "1")
    return False


def _empty_skiptrace_fields(today: str) -> dict:
    """Return blank skip trace fields for a no-match result."""
    return {
        "Owner Name (Primary)":   "",
        "Owner Name (Secondary)": "",
        "Owner Phone(s)":         "",
        "Owner Email(s)":         "",
        "Mailing Address":        "",
        "Deceased":               "",
        "Skip Trace Date":        today,
    }