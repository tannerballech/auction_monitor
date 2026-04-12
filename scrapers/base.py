"""
scrapers/base.py — Eagle Creek Auction Monitor
Base class for all scrapers. Defines the standard output dict shape and
provides shared helpers (HTTP fetch, Claude parse, date normalization,
address geocoding).
"""

from __future__ import annotations

import re
import time
from datetime import datetime, date
from typing import Optional
import requests
import anthropic
import logging

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import DEFAULT_HEADERS, ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

# ── Standard listing dict ─────────────────────────────────────────────────────
def empty_listing(county: str, state: str, source_url: str = "") -> dict:
    """Returns a blank listing dict with all expected keys."""
    return {
        "County": county,
        "State": state,
        "Sale Date": "",
        "Case Number": "",
        "Plaintiff": "",
        "Defendant(s)": "",
        "Street": "",
        "City": "",
        "Zip": "",
        "Appraised Value": "",
        "Judgment / Loan Amount": "",
        "Attorney / Firm": "",
        "Estimated Market Value": "",
        "Cancelled": "",
        "Source URL": source_url,
        "Date Added": datetime.today().strftime("%Y-%m-%d"),
        "Notes": "",
    }


# ── Address helpers ───────────────────────────────────────────────────────────

def split_standard_address(full_address: str) -> tuple[str, str, str]:
    """
    Split a standard US address in the form "Street, City, ST  Zip"
    into (street, city, zip).

    Handles:
      "77 McMillan Drive, Independence, KY 41051"  → ("77 McMillan Drive", "Independence", "41051")
      "7775 Cedar Wood Circle, Florence, KY  41042" → ("7775 Cedar Wood Circle", "Florence", "41042")
      "412 BRADLEY AVE, CINCINNATI , 45215"         → ("412 BRADLEY AVE", "CINCINNATI", "45215")
      "1940 SUNDALE AVE, CINCINNATI, 45239, OH"     → ("1940 SUNDALE AVE", "CINCINNATI", "45239")

    Returns ("", "", "") if the address doesn't match expected patterns.
    """
    if not full_address:
        return "", "", ""

    parts = [p.strip() for p in full_address.split(",")]

    if len(parts) < 2:
        return full_address.strip(), "", ""

    street = parts[0].strip()

    # Find the zip — could be trailing digits in any part after the first
    zip_code = ""
    city = ""

    for part in parts[1:]:
        part = part.strip()
        # Extract 5-digit zip if present in this chunk
        zip_match = re.search(r"\b(\d{5})\b", part)
        if zip_match and not zip_code:
            zip_code = zip_match.group(1)
            # Strip state abbrev and zip from this chunk to get city
            candidate_city = re.sub(r"\b[A-Z]{2}\b", "", part)   # strip state abbrev
            candidate_city = re.sub(r"\b\d{5}\b", "", candidate_city).strip()
            if candidate_city:
                city = candidate_city
        elif not zip_code and not re.match(r"^[A-Z]{2}$", part):
            # No zip yet and not a bare state abbrev — this is the city chunk
            if not city:
                city = part

    # If we still have no city, use parts[1] stripped of state/zip
    if not city and len(parts) > 1:
        city = re.sub(r"\b[A-Z]{2}\b", "", parts[1])
        city = re.sub(r"\b\d{5}\b", "", city).strip()

    return street, city, zip_code


# ── Nominatim geocoder (shared by Fayette, Franklin, Clark, Madison) ──────────

_geocode_cache: dict[str, tuple[str, str]] = {}   # key → (city, zip)

def geocode_address(street: str, city: str = "", state: str = "") -> tuple[str, str]:
    """
    Look up (city, zip) for a street address using Nominatim (OpenStreetMap).

    Pass city if known — the lookup will use it to constrain results.
    If city is blank, the function tries to discover it from OSM.

    Returns (city, zip) — either value may be "" if not found.
    Respects Nominatim's 1 req/sec policy via time.sleep().
    Uses an in-run cache so the same street is never looked up twice.
    """
    if not street:
        return city, ""

    cache_key = f"{street.lower()}|{city.lower()}|{state.lower()}"
    if cache_key in _geocode_cache:
        return _geocode_cache[cache_key]

    headers = {
        "User-Agent": "EagleCreekAuctionMonitor/1.0 (real-estate research tool)",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def _extract_from_result(result: dict) -> tuple[str, str]:
        addr = result.get("address", {})
        found_city = (
            addr.get("city")
            or addr.get("town")
            or addr.get("village")
            or addr.get("hamlet")
            or ""
        )
        found_zip = addr.get("postcode", "")
        # Postcode may be "12345-6789" — keep only the 5-digit base
        if found_zip:
            found_zip = found_zip[:5]
        return found_city, found_zip

    # ── Pass 1: structured query ──────────────────────────────────────────
    params: dict = {
        "format": "json",
        "addressdetails": 1,
        "limit": 1,
        "street": street,
        "state": state or "KY",
    }
    if city:
        params["city"] = city

    time.sleep(1.0)
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params=params,
            headers=headers,
            timeout=10,
        )
        results = resp.json()
        if results:
            found_city, found_zip = _extract_from_result(results[0])
            # Use discovered city only if we didn't already have one
            out_city = city or found_city
            out_zip  = found_zip
            if out_zip:
                logger.info(f"  [geocode] Structured hit: {street!r} → city={out_city!r} zip={out_zip!r}")
                _geocode_cache[cache_key] = (out_city, out_zip)
                return out_city, out_zip
    except Exception as e:
        logger.warning(f"  [geocode] Structured query failed for {street!r}: {e}")

    # ── Pass 2: free-form query ───────────────────────────────────────────
    q_parts = [street]
    if city:
        q_parts.append(city)
    if state:
        q_parts.append(state)
    q_parts.append("US")
    q = ", ".join(q_parts)

    time.sleep(1.0)
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"format": "json", "addressdetails": 1, "limit": 1, "q": q},
            headers=headers,
            timeout=10,
        )
        results = resp.json()
        if results:
            found_city, found_zip = _extract_from_result(results[0])
            out_city = city or found_city
            out_zip  = found_zip
            logger.info(f"  [geocode] Free-form hit: {street!r} → city={out_city!r} zip={out_zip!r}")
            _geocode_cache[cache_key] = (out_city, out_zip)
            return out_city, out_zip
    except Exception as e:
        logger.warning(f"  [geocode] Free-form query failed for {street!r}: {e}")

    # ── Fallback: return whatever city we already knew, empty zip ─────────
    logger.warning(f"  [geocode] No result for {street!r} — using city={city!r}, zip=''")
    _geocode_cache[cache_key] = (city, "")
    return city, ""


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def fetch_html(url: str, timeout: int = 15, retries: int = 2) -> Optional[str]:
    """
    Fetch a URL and return raw HTML text, or None on failure.
    Respects robots by adding a 1s delay and identifying with a real UA.
    """
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
            resp.raise_for_status()
            time.sleep(1)  # Be polite
            return resp.text
        except requests.exceptions.HTTPError as e:
            if resp.status_code in (403, 404):
                print(f"  [WARN] {url} returned {resp.status_code} — skipping.")
                return None
            if attempt < retries:
                time.sleep(3)
                continue
            print(f"  [ERROR] Failed to fetch {url}: {e}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(3)
                continue
            print(f"  [ERROR] Failed to fetch {url}: {e}")
            return None


# ── Claude parse helper ───────────────────────────────────────────────────────
def claude_parse_listings(raw_text: str, county: str, state: str, source_url: str) -> list[dict]:
    """
    Send raw text (email body or scraped HTML) to Claude and ask it to extract
    structured auction listings. Returns a list of listing dicts.
    Used for unstructured sources: Scott/Rowan emails, Campbell KY, Knox TN.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    system_prompt = """You are a real estate data extraction assistant.
Extract all auction/foreclosure sale listings from the provided text.
For each listing, return a JSON array of objects with EXACTLY these keys:
- sale_date (string, format YYYY-MM-DD if possible, else as written)
- case_number (string, the court case number, empty string if not present)
- plaintiff (string, the lender or party bringing the action)
- defendants (string, the property owner(s) or named parties)
- street (string, house number + street name ONLY — no city, state, or zip)
- city (string, city or municipality name ONLY)
- state (string, 2-letter abbreviation, e.g. "KY", "TN", "IN", "OH")
- zip (string, 5-digit zip code, or empty string if not present)
- appraised_value (string, dollar amount if present, else empty string)
- judgment_amount (string, dollar amount of judgment/loan if present, else empty string)
- attorney (string, attorney name or firm if present, else empty string)
- cancelled (string, "Yes" if sale is cancelled, else empty string)
- notes (string, any other relevant info)

Return ONLY the JSON array, no other text. If no listings found, return [].
Always return state as a 2-letter abbreviation — never spell out the full state name.
"""

    user_prompt = f"""County: {county}, {state}
Source: {source_url}

TEXT TO PARSE:
{raw_text[:12000]}
"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw_json = response.content[0].text.strip()

        import json
        # Strip markdown code fences if present
        raw_json = re.sub(r"^```json\s*|^```\s*|```$", "", raw_json, flags=re.MULTILINE).strip()
        parsed = json.loads(raw_json)

        # Normalise full state names → abbreviations as a safety net
        _STATE_ABBREVS = {
            "kentucky": "KY", "tennessee": "TN", "ohio": "OH",
            "indiana": "IN", "north carolina": "NC",
        }

        listings = []
        for item in parsed:
            listing = empty_listing(county, state, source_url)
            listing["Sale Date"]              = item.get("sale_date", "")
            listing["Case Number"]            = item.get("case_number", "")
            listing["Plaintiff"]              = item.get("plaintiff", "")
            listing["Defendant(s)"]           = item.get("defendants", "")
            listing["Street"]                 = item.get("street", "")
            listing["City"]                   = item.get("city", "")
            raw_state                         = item.get("state", state)
            listing["State"]                  = _STATE_ABBREVS.get(raw_state.lower(), raw_state) or state
            listing["Zip"]                    = item.get("zip", "")
            listing["Appraised Value"]        = item.get("appraised_value", "")
            listing["Judgment / Loan Amount"] = item.get("judgment_amount", "")
            listing["Attorney / Firm"]        = item.get("attorney", "")
            listing["Cancelled"]              = item.get("cancelled", "")
            listing["Notes"]                  = item.get("notes", "")
            listings.append(listing)

        return listings

    except Exception as e:
        print(f"  [ERROR] Claude parse failed for {county}: {e}")
        return []


# ── Date helpers ──────────────────────────────────────────────────────────────
MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

def normalize_date(raw: str) -> str:
    """
    Try to convert various date formats to YYYY-MM-DD.
    Falls back to the original string if parsing fails.
    """
    if not raw:
        return ""
    raw = raw.strip()

    raw = re.sub(r"^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*", "", raw, flags=re.IGNORECASE)

    # Try common formats
    for fmt in ("%B %d, %Y", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%B %dst, %Y", "%B %dnd, %Y", "%B %drd, %Y", "%B %dth, %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Try "March 17th, 2026" style with ordinal stripping
    cleaned = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", raw)
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    return raw  # Return as-is if we can't parse


# ── Money helpers ─────────────────────────────────────────────────────────────
def clean_money(raw: str) -> str:
    """Normalize money strings — strip trailing junk after + or ,"""
    if not raw:
        return ""
    # Keep just the dollar amount before any "+" or "plus" text
    match = re.match(r"(\$[\d,]+\.?\d*)", raw.strip())
    if match:
        return match.group(1)
    return raw.strip()