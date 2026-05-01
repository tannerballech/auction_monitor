"""
valuation.py — Market valuation via BatchData API

Per-call cost:
  - Always:           basic ($0.01) + valuation ($0.06)          = $0.07
  - Missing judgment: + mortgage-liens ($0.10)                    = $0.17

Judgment amount logic:
  - If we have a scraped judgment AND BatchData open lien balance:
      use max(judgment, open_lien_balance)
  - If only one source: use what we have
  - If neither: leave equity blank, signal = ❓

Equity signal buckets (from config.py → EQUITY_THRESHOLDS):
  🏆  40%+   ✅  25–39%   ⚠️  10–24%   ❌  below 10%   ❓  no data

Address: listings carry Street / City / State / Zip as separate fields
(refactored April 2026). No address parsing needed here.

Key contract with storage.update_valuations():
  run_valuations() returns dicts that include ALL of these title-case keys:
    "Estimated Market Value"  — formatted dollar string  e.g. "$209,799"
    "Estimated Equity"        — formatted dollar string  e.g. "$186,906"
    "Equity Signal"           — emoji                    e.g. "🏆"
    "Notes"                   — full notes string (confidence already embedded)
    "id"                      — int, DB primary key from get_listings_needing_valuation()

  It also carries snake_case keys for display in run_valuate():
    "emv", "debt", "equity_signal"
"""

import re
import time
import logging
import traceback

import requests

from config import BATCHDATA_API_KEY, EQUITY_THRESHOLDS
from db import get_city_alias, upsert_city_alias

logger = logging.getLogger(__name__)

BATCHDATA_ENDPOINT        = "https://api.batchdata.com/api/v1/property/lookup/all-attributes"
BATCHDATA_VERIFY_ENDPOINT = "https://api.batchdata.com/api/v1/address/verify"
RATE_LIMIT_DELAY          = 1.0
REQUEST_TIMEOUT           = 30


# ── BatchData call ────────────────────────────────────────────────────────────

def _batchdata_lookup(address_dict: dict, include_mortgage: bool) -> dict | None:
    """
    Hit the BatchData property lookup endpoint.
    Returns the raw property dict from the response, or None on failure.
    address_dict must have keys: street, city, state, zip (zip may be "").
    """
    datasets = ["basic", "valuation"]
    if include_mortgage:
        datasets.append("mortgage-liens")

    payload = {
        "requests": [
            {
                "address": address_dict,
                "options": {"datasets": datasets},
            }
        ]
    }

    headers = {
        "Authorization": f"Bearer {BATCHDATA_API_KEY}",
        "Content-Type":  "application/json",
    }

    try:
        resp = requests.post(
            BATCHDATA_ENDPOINT,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as e:
        logger.error(f"[valuation] BatchData request failed: {e}")
        return None

    if resp.status_code != 200:
        logger.error(f"[valuation] BatchData HTTP {resp.status_code}: {resp.text[:300]}")
        return None

    if not resp.text.strip():
        logger.error("[valuation] BatchData returned empty response body")
        return None

    try:
        data = resp.json()
    except Exception as e:
        logger.error(f"[valuation] BatchData JSON parse failed: {e}")
        return None

    try:
        props = data["results"]["properties"]
    except (KeyError, TypeError):
        logger.error(f"[valuation] Unexpected BatchData response structure: {data}")
        return None

    if not props:
        logger.warning("[valuation] BatchData returned no matching properties")
        return None

    return props[0]


# ── Address normalisation ─────────────────────────────────────────────────────

def _normalize_address(addr: dict) -> dict | None:
    """
    Return a corrected address dict if the city can be normalised to its
    USPS-canonical form (e.g. "Wilder" → "Newport"). Returns None on failure
    or if nothing changed.

    Two-tier strategy:
      1. Check the local city_aliases table first. Repeat aliases (e.g. the
         Campbell County KY suburbs all → Newport) cost zero API calls.
      2. On a cache miss, call BatchData's address-verify endpoint. If it
         returns a different canonical city, store the mapping so the next
         hit is free.

    Only called as a fallback when the primary property lookup returns
    nothing, so the verify round-trip is paid only when it would actually help.
    """
    orig_city  = (addr.get("city")  or "").strip()
    orig_state = (addr.get("state") or "").strip()
    orig_zip   = (addr.get("zip")   or "").strip()

    cached = get_city_alias(orig_city, orig_state, orig_zip)
    if cached and cached.lower() != orig_city.lower():
        logger.info(
            f"[valuation] Address normalised from cache: city "
            f"{orig_city!r} → {cached!r} (zip={orig_zip!r})"
        )
        return {
            "street": addr["street"],
            "city":   cached,
            "state":  orig_state,
            "zip":    orig_zip,
        }

    headers = {
        "Authorization": f"Bearer {BATCHDATA_API_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {"requests": [{"address": addr}]}

    try:
        resp = requests.post(
            BATCHDATA_VERIFY_ENDPOINT,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as e:
        logger.warning(f"[valuation] Address verify request failed: {e}")
        return None

    if resp.status_code != 200:
        logger.warning(f"[valuation] Address verify HTTP {resp.status_code}")
        return None

    try:
        verified = resp.json()["results"]["addresses"][0]
    except (KeyError, IndexError, TypeError):
        return None

    if not verified.get("meta", {}).get("verified"):
        return None

    norm_city = (verified.get("city") or "").strip()

    # Only return a new dict if the city actually changed — no point retrying
    # with the same values we already tried.
    if norm_city and norm_city.lower() != orig_city.lower():
        logger.info(
            f"[valuation] Address normalised via BatchData: city "
            f"{orig_city!r} → {norm_city!r}"
        )
        upsert_city_alias(orig_city, orig_state, norm_city, orig_zip)
        return {
            "street": verified.get("street") or addr["street"],
            "city":   norm_city,
            "state":  verified.get("state") or addr["state"],
            "zip":    verified.get("zip") or addr.get("zip", ""),
        }

    return None


# ── Street pre-processing ────────────────────────────────────────────────────

_AKA_RE       = re.compile(r"^(\d+)(?:\s+a/k/a\s+\d+)+\s+(.*)", re.IGNORECASE)
_AMPERSAND_RE = re.compile(r"\s*&\s+\d+.*")
_RANGE_RE     = re.compile(r"^(\d+)-\d+\b")
_UNIT_RE      = re.compile(r"\s+#\S+(?:\s+\S+)?$")


def _clean_street(street: str) -> str:
    """
    Normalise messy court-record street strings for BatchData lookup.
    The raw value is never modified in the DB — this is lookup-only cleanup.

      "926 a/k/a 930 a/k/a 932 Dixie Highway"           → "926 Dixie Highway"
      "8117 Saint Anthony's Church Rd & 8119 Saint ..."  → "8117 Saint Anthony's Church Rd"
      "237-247 South Fifth Street"                       → "237 South Fifth Street"
      "6105 Titantic Way #L 116"                         → "6105 Titantic Way"
    """
    s = street.strip()
    s = _AKA_RE.sub(r"\1 \2", s)
    s = _AMPERSAND_RE.sub("", s)
    s = _RANGE_RE.sub(lambda m: m.group(1), s)
    s = _UNIT_RE.sub("", s)
    return s.strip()


# ── Equity signal ─────────────────────────────────────────────────────────────

def compute_equity_signal(emv: float, debt: float | None) -> str:
    """Returns one of: 🏆 ✅ ⚠️ ❌ ❓"""
    if debt is None or emv <= 0:
        return "❓"
    equity_pct = (emv - debt) / emv
    if equity_pct >= EQUITY_THRESHOLDS["home_run"]:
        return "🏆"
    elif equity_pct >= EQUITY_THRESHOLDS["decent"]:
        return "✅"
    elif equity_pct >= EQUITY_THRESHOLDS["tight"]:
        return "⚠️"
    else:
        return "❌"


# ── Main valuation function ───────────────────────────────────────────────────

def valuate_listing(listing: dict) -> dict | None:
    """
    Run valuation for a single listing dict.
    Reads address from listing["Street"], ["City"], ["State"], ["Zip"].

    Returns a result dict with snake_case keys:
        emv, emv_low, emv_high, debt, debt_source, equity_signal, notes

    Returns None if BatchData lookup fails entirely.
    """
    street = listing.get("Street", "").strip()
    city   = listing.get("City", "").strip()
    state  = listing.get("State", "").strip()
    zip_   = listing.get("Zip", "").strip()

    if not street:
        logger.warning(
            f"[valuation] Skipping — no street for "
            f"{listing.get('County', '?')} id={listing.get('id', '?')}"
        )
        return None

    clean = _clean_street(street)
    if clean != street:
        logger.info(f"[valuation] Street cleaned: {street!r} → {clean!r}")

    addr_dict = {
        "street": clean,
        "city":   city,
        "state":  state or "KY",
        "zip":    zip_,
    }

    raw_judgment     = listing.get("Judgment / Loan Amount", "")
    scraped_judgment = None
    if raw_judgment:
        try:
            scraped_judgment = float(
                str(raw_judgment).replace("$", "").replace(",", "").strip()
            )
        except (ValueError, TypeError):
            scraped_judgment = None

    include_mortgage = (scraped_judgment is None)

    prop = _batchdata_lookup(addr_dict, include_mortgage)

    # If the initial lookup failed, try once more with a normalised address.
    # BatchData uses USPS-canonical city names (e.g. "Newport" instead of
    # "Wilder" or "Southgate") that may differ from what the county records use.
    if prop is None:
        norm = _normalize_address(addr_dict)
        if norm:
            logger.info(f"[valuation] Retrying lookup with normalised address: {norm}")
            prop = _batchdata_lookup(norm, include_mortgage)

    if prop is None:
        return None

    val        = prop.get("valuation") or {}
    emv        = val.get("estimatedValue")
    emv_low    = val.get("priceRangeMin")
    emv_high   = val.get("priceRangeMax")
    confidence = val.get("confidenceScore")   # int 0–100

    if not emv:
        logger.warning(f"[valuation] No EMV returned for {street!r}, {city}, {state}")
        return None

    batchdata_lien_balance = None
    if include_mortgage:
        open_lien  = prop.get("openLien") or {}
        lien_count = open_lien.get("totalOpenLienCount", 0)
        if lien_count and lien_count > 0:
            batchdata_lien_balance = open_lien.get("totalOpenLienBalance")

    if scraped_judgment is not None and batchdata_lien_balance is not None:
        debt        = max(scraped_judgment, batchdata_lien_balance)
        debt_source = "max(scraped,batchdata)"
    elif scraped_judgment is not None:
        debt        = scraped_judgment
        debt_source = "scraped"
    elif batchdata_lien_balance is not None:
        debt        = batchdata_lien_balance
        debt_source = "batchdata"
    else:
        debt        = None
        debt_source = "none"

    signal = compute_equity_signal(emv, debt)

    # ── Build notes string (includes confidence label) ────────────────────────
    notes_parts = []
    if confidence is not None:
        if confidence >= 80:
            conf_label = "high"
        elif confidence >= 60:
            conf_label = "medium"
        else:
            conf_label = "low"
        notes_parts.append(f"Confidence: {conf_label} ({confidence})")

    if emv_low and emv_high:
        notes_parts.append(f"Range: ${emv_low:,.0f}–${emv_high:,.0f}")

    if debt_source == "batchdata":
        notes_parts.append("Debt: est. lien balance (BatchData)")
    elif debt_source == "max(scraped,batchdata)":
        notes_parts.append("Debt: max of scraped judgment and BatchData lien balance")
    elif debt_source == "none":
        notes_parts.append("No debt data available")

    return {
        "emv":          emv,
        "emv_low":      emv_low,
        "emv_high":     emv_high,
        "debt":         debt,
        "debt_source":  debt_source,
        "equity_signal": signal,
        "notes":        " | ".join(notes_parts),
    }


# ── Batch runner (called from main.py --valuate) ──────────────────────────────

def run_valuations(listings: list[dict], dry_run: bool = False) -> list[dict]:
    """
    Run valuations for a list of listings from get_listings_needing_valuation().

    Returns a list of merged dicts that satisfy two consumers:

    1. run_valuate() in main.py — uses snake_case keys for display:
           l.get("emv"), l.get("debt"), l.get("equity_signal")

    2. sheets_writer.update_valuations() — uses title-case keys for writing:
           v.get("Estimated Market Value")   → formatted "$xxx,xxx"
           v.get("Estimated Equity")         → formatted "$xxx,xxx" or ""
           v.get("Equity Signal")            → emoji
           v.get("Notes")                   → full notes string

    update_valuations() also checks v.get("confidence") to prepend a label
    to Notes. We do NOT include raw "confidence" here since the label is
    already embedded in the notes string by valuate_listing(). Omitting it
    prevents double-prefixing.
    """
    results = []
    total   = len(listings)

    for i, listing in enumerate(listings, 1):
        street  = listing.get("Street", "(no address)")
        city    = listing.get("City", "")
        county  = listing.get("County", "")
        display = f"{street}, {city}" if city else street

        print(f"  [{i}/{total}] {county} — {display}")

        if dry_run:
            print(f"    [dry-run] skipping BatchData call")
            continue

        try:
            result = valuate_listing(listing)
        except Exception:
            print(f"    ERROR during valuation:")
            traceback.print_exc()
            result = None

        if result is None:
            print(f"    No result returned — skipping")
        else:
            emv    = result["emv"]
            debt   = result.get("debt")
            signal = result.get("equity_signal", "❓")
            print(
                f"    EMV: ${emv:,.0f}  |  "
                f"Debt: {'${:,.0f}'.format(debt) if debt else 'unknown'}  |  "
                f"Signal: {signal}"
            )

            # ── Format values for sheets_writer.update_valuations() ──────────
            equity_dollars = (
                f"${emv - debt:,.0f}" if debt is not None else ""
            )

            results.append({
                # Pass-through listing fields (id, County, State, etc.)
                **listing,
                # Snake-case for display in run_valuate()
                "emv":           emv,
                "debt":          debt,
                "equity_signal": signal,
                "debt_source":   result.get("debt_source"),
                # Title-case for sheets_writer.update_valuations()
                # Note: do NOT include "confidence" key — notes string already
                # has the label embedded; adding confidence would cause sheets_writer
                # to double-prefix it.
                "Estimated Market Value": f"${emv:,.0f}",
                "Estimated Equity":       equity_dollars,
                "Equity Signal":          signal,
                "Notes":                  result.get("notes", ""),
            })

        if i < total:
            time.sleep(RATE_LIMIT_DELAY)

    return results