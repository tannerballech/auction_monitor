"""
scrapers/tn_trustees/nw_posting_services.py
Nationwide Posting Services — TN Foreclosure Sale Scraper
https://www.nwpostingservices.com

Third-party internet posting platform used by multiple TN trustees.
Currently confirmed:
  - Marinosci Law Group, P.C.          (registry key: "marinosci")
  - ALAW / James E. Albertelli, P.A.   (registry key: "albertelli_alaw")

Other firms may also post here — any unrecognized trustee found in a PDF
is logged for follow-up.

--- Site architecture (confirmed via DevTools) ---

POST https://www.nwpostingservices.com/Public/getAuctions
Content-Type: application/x-www-form-urlencoded
Body: SaleDateFrom=&SaleDateTo=&State=TN&County=&ZipCode=

Response: JSON array.  Each row:
  {
    "View":             "<a href='https://customnod.s3.amazonaws.com/2100032674.pdf' ...>View</a>",
    "Street":           "128 Horseshoe Dr",
    "City":             "Johnson City",
    "County":           "Carter",
    "State":            "TN",
    "Zip":              "37601",
    "OriginalSaledate": "11/18/25",           ← MM/DD/YY (2-digit year)
    "NewSaledate":      "<span class=\"postponed\">05/12/26</span>"  ← or "" if not postponed
  }

--- Postponement logic ---

"NewSaledate" being non-empty (after stripping HTML) is an explicit site-level
postponement signal.  No "absent = manual-check" logic — this is a multi-firm
platform so absence is not evidence of cancellation.

--- Trustee identification ---

The "View" field is an HTML anchor tag embedding the PDF URL.  Each PDF is
hosted on S3 (customnod.s3.amazonaws.com) and requires no authentication.
There is no firm column in the table — the PDF is the sole source of the
trustee firm name.

Per row:
  1. Extract PDF URL from "View" field HTML
  2. Fetch PDF, extract text with pdfplumber
  3. Regex fast-path against known firm name patterns
  4. Claude Haiku fallback if no pattern matches
  5. lookup_trustee() to get registry key
  6. Skip rows where registry key not in _TARGET_FIRMS

Unrecognized firms (lookup returns None) are logged as unknown.

--- Two modes ---

  scrape_nw_posting_services(existing_addr_set, dry_run)
      Discovery: fetch all TN rows, identify trustee via PDF, filter to
      _TARGET_FIRMS, return new listings not already in the sheet.
      Returns (new_listings, {}).

  check_existing(sheet_rows, dry_run)
      Check: fetch live rows, match sheet rows by address, detect postponements
      from non-empty NewSaledate.
      Returns (postponements, []).
"""

from __future__ import annotations

import html
import io
import logging
import re
import time
from datetime import date, datetime
from typing import Optional

import anthropic
import pdfplumber
import requests

from scrapers.base import empty_listing
from scrapers.tn_trustees.registry import lookup_trustee, TRUSTEE_REGISTRY
import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL     = "https://www.nwpostingservices.com"
SOURCE_URL   = BASE_URL + "/"
LISTINGS_URL = BASE_URL + "/Public/getAuctions"

CHECK_WINDOW_DAYS = 14

# Registry keys for firms confirmed to use this platform.
# Rows attributed to other firms are skipped (but logged).
_TARGET_FIRMS: set[str] = {"marinosci", "albertelli_alaw"}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Referer": SOURCE_URL,
}

# Extracts href URL from the HTML "View" field:
# <a href='https://customnod.s3.amazonaws.com/2100032674.pdf' target='_blank'>View</a>
_PDF_HREF_RE = re.compile(r"""href=['"](https://customnod\.s3\.amazonaws\.com/[^'"]+\.pdf)['"]""", re.I)

# Strips any HTML tags from NewSaledate field:
# "<span class=\"postponed\">05/12/26</span>"  →  "05/12/26"
_HTML_TAG_RE = re.compile(r"<[^>]+>")

# Regex fast-path for trustee identification from PDF text.
# More specific patterns first.
_TRUSTEE_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("marinosci",          re.compile(r"marinosci",                               re.I)),
    ("albertelli_alaw",    re.compile(r"albertelli|(?<!\w)alaw(?!\w)",            re.I)),
    ("mackie_wolf",        re.compile(r"mackie\s*wolf|mwzm",                      re.I)),
    ("rubin_lublin",       re.compile(r"rubin\s*lublin",                          re.I)),
    ("brock_scott",        re.compile(r"brock\s*(?:and|&)\s*scott",               re.I)),
    ("mcmichael_taylor",   re.compile(r"mcmichael\s*taylor",                      re.I)),
    ("clear_recon",        re.compile(r"clear\s*recon",                           re.I)),
    ("foundation_legal",   re.compile(
        r"foundation\s*legal|wilson\s*(?:&|and)\s*associates",                    re.I)),
    ("robertson_anschutz", re.compile(r"robertson.*anschutz|ras\s*crane",         re.I)),
    ("padgett_law",        re.compile(r"padgett",                                 re.I)),
    ("mickel_law",         re.compile(r"mickel",                                  re.I)),
    ("llg_trustee",        re.compile(r"logs\s*legal|llg\s*trustee",              re.I)),
]


# ---------------------------------------------------------------------------
# Fetch all rows
# ---------------------------------------------------------------------------

def _fetch_all_rows() -> list[dict]:
    """
    POST to /Public/getAuctions with State=TN and no other filters.
    Returns raw list of row dicts, or [] on any error.
    """
    payload = {
        "SaleDateFrom": "",
        "SaleDateTo":   "",
        "State":        "TN",
        "County":       "",
        "ZipCode":      "",
    }
    try:
        resp = requests.post(
            LISTINGS_URL,
            data=payload,
            headers=_HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("[nw_posting_services] POST failed: %s", e)
        return []

    try:
        data = resp.json()
    except ValueError as e:
        logger.error("[nw_posting_services] JSON parse error: %s", e)
        logger.debug("[nw_posting_services] Response: %s", resp.text[:500])
        return []

    if not isinstance(data, list):
        logger.error(
            "[nw_posting_services] Expected list, got %s. Keys: %s",
            type(data), list(data.keys()) if isinstance(data, dict) else "N/A",
        )
        return []

    logger.info("[nw_posting_services] Fetched %d raw row(s)", len(data))
    return data


# ---------------------------------------------------------------------------
# Row parsing helpers
# ---------------------------------------------------------------------------

def _extract_pdf_url(view_html: str) -> str:
    """
    Extract the S3 PDF URL from the raw HTML in the "View" field.

    Input:  "<a href='https://customnod.s3.amazonaws.com/2100032674.pdf' target='_blank'>View</a>"
    Output: "https://customnod.s3.amazonaws.com/2100032674.pdf"

    Returns "" if no URL found.
    """
    if not view_html:
        return ""
    # Unescape HTML entities first (&apos; etc.)
    unescaped = html.unescape(view_html)
    m = _PDF_HREF_RE.search(unescaped)
    return m.group(1) if m else ""


def _strip_html(raw: str) -> str:
    """Remove all HTML tags and unescape entities. Returns plain text."""
    if not raw:
        return ""
    return html.unescape(_HTML_TAG_RE.sub("", raw)).strip()


def _parse_date(raw: str) -> str:
    """
    Parse a date string to ISO YYYY-MM-DD.

    Handles:
      "MM/DD/YY"   → "%m/%d/%y"  (site's actual format, e.g. "11/18/25")
      "MM/DD/YYYY" → "%m/%d/%Y"  (defensive; in case site changes)
      "YYYY-MM-DD" → already ISO

    Two-digit years: Python's %y treats 00–68 as 2000–2068, 69–99 as 1969–1999.
    All foreclosure dates will be in 2025–2027 range, so this is safe.
    Returns "" on failure.
    """
    stripped = _strip_html(raw)
    if not stripped:
        return ""
    parts = stripped.split()
    if not parts:
        return ""
    raw = parts[0]   # strip any trailing time component
    for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    logger.debug("[nw_posting_services] Unparseable date: %r", raw)
    return ""


def _parse_row(raw: dict) -> Optional[dict]:
    """
    Normalise a raw API row into our intermediate dict.
    Returns None if required fields (Street, OriginalSaledate) are missing.
    """
    street = (raw.get("Street") or "").strip()
    city   = (raw.get("City")   or "").strip()
    county = (raw.get("County") or "").strip()
    state  = (raw.get("State")  or "TN").strip()
    zip_   = (raw.get("Zip")    or "").strip()

    orig_raw = raw.get("OriginalSaledate") or ""
    new_raw  = raw.get("NewSaledate")      or ""   # may be HTML-wrapped span

    if not street or not orig_raw:
        logger.debug("[nw_posting_services] Skipping incomplete row: %s", raw)
        return None

    orig_date = _parse_date(orig_raw)
    new_date  = _parse_date(new_raw)           # "" if not postponed or unparseable

    # Strip "County" suffix from county name if present
    county = re.sub(r"\s+County\s*$", "", county, flags=re.I).strip()

    pdf_url = _extract_pdf_url(raw.get("View") or "")
    if not pdf_url:
        logger.warning("[nw_posting_services] No PDF URL in View field for %s %s", street, county)

    return {
        "street":    street,
        "city":      city,
        "county":    county,
        "state":     state,
        "zip":       zip_,
        "orig_date": orig_date,
        "new_date":  new_date,
        "pdf_url":   pdf_url,
    }


# ---------------------------------------------------------------------------
# PDF fetching + trustee identification
# ---------------------------------------------------------------------------

def _fetch_pdf_text(pdf_url: str) -> str:
    """
    Download a PDF from S3 and extract all text using pdfplumber.
    Returns "" on any error.
    """
    if not pdf_url:
        return ""
    try:
        resp = requests.get(
            pdf_url,
            headers={"User-Agent": _HEADERS["User-Agent"]},
            timeout=25,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("[nw_posting_services] PDF fetch failed (%s): %s", pdf_url, e)
        return ""

    try:
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        logger.warning("[nw_posting_services] pdfplumber error (%s): %s", pdf_url, e)
        return ""


def _identify_trustee_regex(pdf_text: str) -> Optional[str]:
    """Fast-path: check PDF text against known firm patterns. Returns key or None."""
    for key, pattern in _TRUSTEE_PATTERNS:
        if pattern.search(pdf_text):
            return key
    return None


def _identify_trustee_claude(pdf_text: str) -> Optional[str]:
    """
    Fallback: ask Claude Haiku to extract the trustee firm name, then look it up.
    Uses the first 3000 chars (header area always has the firm name).
    Returns registry key or None.
    """
    if not pdf_text.strip():
        return None

    prompt = (
        "This text is from a Tennessee Notice of Trustee's Sale PDF.\n"
        "Identify the name of the law firm or trustee conducting this sale.\n"
        "Look for: 'Substitute Trustee:', 'Trustee:', or the firm name in the header.\n"
        "Reply with ONLY the firm name exactly as written in the document.\n"
        "If you cannot determine it, reply with: UNKNOWN\n\n"
        f"PDF TEXT:\n{pdf_text[:3000]}"
    )

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    for attempt, wait in enumerate([0, 10, 20]):
        if wait:
            time.sleep(wait)
        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=80,
                messages=[{"role": "user", "content": prompt}],
            )
            raw_name = msg.content[0].text.strip()
            if raw_name.upper() == "UNKNOWN":
                return None
            key, _ = lookup_trustee(raw_name)
            logger.debug("[nw_posting_services] Claude → %r → key=%r", raw_name, key)
            return key
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < 2:
                logger.warning("[nw_posting_services] Claude 529, waiting %ds...", wait or 10)
                continue
            logger.error("[nw_posting_services] Claude API error: %s", e)
            return None
        except Exception as e:
            logger.error("[nw_posting_services] Claude call error: %s", e)
            return None
    return None


def _identify_trustee(pdf_url: str, pdf_cache: dict) -> Optional[str]:
    """
    Determine the trustee registry key for a PDF URL.
    Results are cached so each PDF is fetched at most once per run.
    Returns registry key or None.
    """
    if pdf_url in pdf_cache:
        cached = pdf_cache[pdf_url]
        return None if cached == "UNKNOWN" else cached

    text = _fetch_pdf_text(pdf_url)
    if not text:
        pdf_cache[pdf_url] = "UNKNOWN"
        return None

    key = _identify_trustee_regex(text)
    if key:
        logger.debug("[nw_posting_services] Regex → %r (%s)", key, pdf_url)
        pdf_cache[pdf_url] = key
        return key

    key = _identify_trustee_claude(text)
    pdf_cache[pdf_url] = key if key else "UNKNOWN"
    return key


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


def _addresses_match(site_st: str, site_city: str,
                     sheet_st: str, sheet_city: str) -> bool:
    """Fuzzy match: street number + first street word + city (all optional city check)."""
    num_a = _street_number(site_st)
    num_b = _street_number(sheet_st)
    if not num_a or not num_b or num_a != num_b:
        return False
    if _street_first_word(site_st) != _street_first_word(sheet_st):
        return False
    ca, cb = site_city.lower().strip(), sheet_city.lower().strip()
    if ca and cb and ca != cb:
        return False
    return True


# ---------------------------------------------------------------------------
# Discovery mode
# ---------------------------------------------------------------------------

def scrape_nw_posting_services(
    existing_addr_set: set[tuple],
    dry_run: bool = False,
) -> tuple[list[dict], dict]:
    """
    Discovery mode.

    existing_addr_set: set of (county_lower, street_number, sale_date) tuples
        for all active TN rows — prevents cross-source duplicates.

    Returns (new_listings, {}).
    No cancellation signals are available from this platform.
    """
    raw_rows = _fetch_all_rows()
    if not raw_rows:
        return [], {}

    today_str = date.today().isoformat()
    pdf_cache: dict = {}
    new_listings: list[dict] = []
    stats = {"past": 0, "dup": 0, "other_firm": 0, "unknown": 0}

    for raw in raw_rows:
        row = _parse_row(raw)
        if not row:
            continue

        # Active sale date: prefer new_date (rescheduled) over orig_date
        sale_date = row["new_date"] if row["new_date"] else row["orig_date"]
        if not sale_date or sale_date < today_str:
            stats["past"] += 1
            continue

        # Cross-source dedup
        street_num   = _street_number(row["street"])
        county_lower = row["county"].lower()
        if (county_lower, street_num, sale_date) in existing_addr_set:
            stats["dup"] += 1
            continue

        # Identify trustee via PDF
        registry_key = _identify_trustee(row["pdf_url"], pdf_cache)

        if registry_key is None:
            logger.warning(
                "[nw_posting_services] Unidentified trustee — %s, %s County (pdf: %s)",
                row["street"], row["county"], row["pdf_url"],
            )
            stats["unknown"] += 1
            continue

        if registry_key not in _TARGET_FIRMS:
            logger.debug(
                "[nw_posting_services] Firm %r not in _TARGET_FIRMS — skipping", registry_key
            )
            stats["other_firm"] += 1
            continue

        trustee_name = TRUSTEE_REGISTRY.get(registry_key, {}).get(
            "canonical_name", registry_key
        )

        listing = empty_listing(county=row["county"], state="TN")
        listing.update({
            "Sale Date":              sale_date,
            "Case Number":            "",
            "Plaintiff":              "",
            "Defendant(s)":           "",
            "Street":                 row["street"],
            "City":                   row["city"],
            "Zip":                    row["zip"],
            "Appraised Value":        "",
            "Judgment / Loan Amount": "",
            "Attorney / Firm":        trustee_name,
            "Cancelled":              "",
            "Source URL":             SOURCE_URL,
            "Notes": (
                f"Postponed from {row['orig_date']}" if row["new_date"] else ""
            ),
        })

        new_listings.append(listing)
        if not dry_run:
            existing_addr_set.add((county_lower, street_num, sale_date))

    logger.info(
        "[nw_posting_services] Discovery — new=%d past=%d dup=%d "
        "other_firm=%d unknown=%d",
        len(new_listings), stats["past"], stats["dup"],
        stats["other_firm"], stats["unknown"],
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
    Check mode. Detect postponements for rows attributed to firms on this platform
    (Marinosci + ALAW combined; caller passes them together).

    sheet_rows: list of row dicts from get_tn_listings_for_check() for these firms.

    Postponement: same address found on site with NewSaledate (after HTML-strip)
    non-empty and different from the sheet's current Sale Date.

    Returns (postponements, []).
    No manual-check flags — absence on this multi-firm platform is not
    evidence of cancellation (same constraint as internetpostings.py).
    """
    if not sheet_rows:
        return [], []

    raw_rows = _fetch_all_rows()
    if not raw_rows:
        logger.warning("[nw_posting_services] No rows returned — skipping check")
        return [], []

    today_str = date.today().isoformat()

    # Parse site rows; keep only those with an explicit new/rescheduled date
    site_postponed: list[dict] = []
    for raw in raw_rows:
        row = _parse_row(raw)
        if row and row["new_date"] and row["new_date"] >= today_str:
            site_postponed.append(row)

    if not site_postponed:
        logger.info("[nw_posting_services] No postponed rows found on site")
        return [], []

    postponements: list[dict] = []

    for sheet_row in sheet_rows:
        sheet_street = sheet_row.get("Street", "")
        sheet_city   = sheet_row.get("City", "")
        sheet_date   = sheet_row.get("Sale Date", "")
        row_index    = sheet_row.get("row_index")

        if not sheet_street or not sheet_date or sheet_date < today_str:
            continue

        for site_row in site_postponed:
            if not _addresses_match(
                site_row["street"], site_row["city"],
                sheet_street, sheet_city,
            ):
                continue

            new_date = site_row["new_date"]
            if new_date != sheet_date:
                logger.info(
                    "[nw_posting_services] Postponement: row %s  %s → %s  (%s)",
                    row_index, sheet_date, new_date, sheet_street,
                )
                postponements.append({
                    "row_index": row_index,
                    "old_date":  sheet_date,
                    "new_date":  new_date,
                    "note": (
                        f"Postponed: {sheet_date} → {new_date} "
                        f"(NW Posting Services)"
                    ),
                })
            break  # matched this sheet row — move to next

    logger.info(
        "[nw_posting_services] Check — %d postponement(s) from %d sheet row(s)",
        len(postponements), len(sheet_rows),
    )
    return postponements, []