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

Key contract with sheets_writer.update_valuations():
  run_valuations() returns dicts that include ALL of these title-case keys:
    "Estimated Market Value"  — formatted dollar string  e.g. "$209,799"
    "Estimated Equity"        — formatted dollar string  e.g. "$186,906"
    "Equity Signal"           — emoji                    e.g. "🏆"
    "Notes"                   — full notes string (confidence already embedded)
    "_row_index"              — int, from get_listings_needing_valuation()

  It also carries snake_case keys for display in run_valuate():
    "emv", "debt", "equity_signal"
"""

import time
import logging
import traceback

import requests

from config import BATCHDATA_API_KEY, EQUITY_THRESHOLDS

logger = logging.getLogger(__name__)

BATCHDATA_ENDPOINT = "https://api.batchdata.com/api/v1/property/lookup/all-attributes"
RATE_LIMIT_DELAY   = 1.0
REQUEST_TIMEOUT    = 30


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
            f"{listing.get('County', '?')} row {listing.get('_row_index', '?')}"
        )
        return None

    addr_dict = {
        "street": street,
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
                # Pass-through listing fields (_row_index, County, State, etc.)
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