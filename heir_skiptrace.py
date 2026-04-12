"""
Phase 4b — Heir Skip Trace via Tracerfy
========================================
Uses Tracerfy's Instant Trace Lookup with find_owner=False to search for a
specific named person (heir) at an associated address (the deceased owner's
property address used as a location anchor).

Endpoint docs: https://www.tracerfy.com/skip-tracing-api-documentation/
Pricing:       5 credits per HIT, 0 credits on miss.
Rate limit:    500 RPM on the instant trace endpoint.

Public entry point
------------------
    result = skip_trace_heir(heir_name, street, city, state)

Returns a dict with keys:
    hit     (bool) — whether Tracerfy matched the person
    phones  (str)  — comma-separated non-DNC numbers, mobile-first
    emails  (str)  — comma-separated email addresses
    mailing (str)  — "Street, City, State Zip" or ""
"""

import logging
import requests

from config import TRACERFY_API_KEY, TRACERFY_INSTANT_TRACE_URL

logger = logging.getLogger(__name__)

# Generational suffixes to strip from heir names before splitting first/last
_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "esq", "esq."}


# ── Name Parsing ──────────────────────────────────────────────────────────────

def _parse_heir_name(full_name: str) -> tuple[str, str]:
    """
    Split a full heir name string into (first_name, last_name).

    Handles multi-word first names and strips trailing generational suffixes
    before splitting.

    Examples:
        "John Smith"        → ("John", "Smith")
        "Mary Jane Smith"   → ("Mary Jane", "Smith")
        "Robert Jones Jr."  → ("Robert", "Jones")
        "Alice"             → ("Alice", "")
        ""                  → ("", "")
    """
    parts = full_name.strip().split()

    # Strip trailing generational suffixes (e.g. "Jr.", "III")
    while parts and parts[-1].lower() in _SUFFIXES:
        parts.pop()

    if not parts:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")

    last = parts[-1]
    first = " ".join(parts[:-1])
    return (first, last)


# ── Tracerfy API Call ─────────────────────────────────────────────────────────

def _call_tracerfy(
    first_name: str,
    last_name: str,
    street: str,
    city: str,
    state: str,
) -> dict:
    """
    POST to Tracerfy Instant Trace Lookup with find_owner=False.

    find_owner=False tells Tracerfy to search for the specific named person
    at this address (used as a location anchor / last-known-associated address),
    rather than finding whoever currently owns the property.

    Returns the parsed JSON response dict.
    Raises RuntimeError on error HTTP status codes.
    """
    payload = {
        "address":    street,
        "city":       city,
        "state":      state,
        # zip omitted — optional per Tracerfy docs; Heir Leads tab has no zip column
        "find_owner": False,
        "first_name": first_name,
        "last_name":  last_name,
    }
    headers = {
        "Authorization": f"Bearer {TRACERFY_API_KEY}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    resp = requests.post(
        TRACERFY_INSTANT_TRACE_URL,
        json=payload,
        headers=headers,
        timeout=30,
    )

    if resp.status_code == 401:
        raise RuntimeError(
            "Tracerfy 401 Unauthorized — check TRACERFY_API_KEY in config.py."
        )
    if resp.status_code == 402:
        raise RuntimeError(
            "Tracerfy 402 Payment Required — check your Tracerfy credit balance."
        )
    if resp.status_code == 429:
        raise RuntimeError(
            "Tracerfy rate limit hit (429). Slow down call frequency."
        )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Tracerfy HTTP {resp.status_code}: {resp.text[:300]}"
        )

    return resp.json()


# ── Response Parsing ──────────────────────────────────────────────────────────

def _parse_phones(phones: list) -> str:
    """
    Return a comma-separated string of non-DNC phone numbers.
    Sorted by rank ascending (rank 1 = best/most likely mobile).
    """
    valid = [p for p in phones if not p.get("dnc", False)]
    valid.sort(key=lambda p: p.get("rank", 99))
    return ", ".join(p["number"] for p in valid if p.get("number"))


def _parse_emails(emails: list) -> str:
    """Return a comma-separated string of email addresses, sorted by rank."""
    sorted_emails = sorted(emails, key=lambda e: e.get("rank", 99))
    return ", ".join(e["email"] for e in sorted_emails if e.get("email"))


def _parse_mailing(mailing: dict) -> str:
    """
    Format a Tracerfy mailing_address dict into a single readable string.

    Output format: "123 Main St, Austin, TX 78701"
    """
    if not mailing:
        return ""
    street  = mailing.get("street", "").strip()
    city    = mailing.get("city",   "").strip()
    state   = mailing.get("state",  "").strip()
    zip_    = mailing.get("zip",    "").strip()

    city_state_zip = " ".join(filter(None, [city, state, zip_]))
    return ", ".join(filter(None, [street, city_state_zip]))


# ── Public Entry Point ────────────────────────────────────────────────────────

def skip_trace_heir(
    heir_name: str,
    street: str,
    city: str,
    state: str,
) -> dict:
    """
    Run a Tracerfy Instant Trace Lookup for a single heir.

    Passes heir's full name + the deceased owner's property address as a
    location anchor. Tracerfy searches for this specific named person
    associated with that address (prior residency, mail recipient, family
    member in public records, etc.).

    Args:
        heir_name:  Full name from Heir Leads col H (e.g. "Mary Jane Smith")
        street:     Property street from Heir Leads col A
        city:       Property city from Heir Leads col B
        state:      State from Heir Leads col D

    Returns dict:
        hit     (bool)  whether Tracerfy returned a match
        phones  (str)   comma-separated non-DNC numbers, or ""
        emails  (str)   comma-separated emails, or ""
        mailing (str)   formatted mailing address, or ""
    """
    first_name, last_name = _parse_heir_name(heir_name)

    if not first_name or not last_name:
        logger.warning(
            "Could not parse first/last name from %r — skipping Tracerfy call",
            heir_name,
        )
        return {"hit": False, "phones": "", "emails": "", "mailing": ""}

    logger.info(
        "Tracerfy lookup: %s %s @ %s, %s %s",
        first_name, last_name, street, city, state,
    )

    raw = _call_tracerfy(first_name, last_name, street, city, state)

    if not raw.get("hit"):
        logger.info("  → No match (hit=False, 0 credits deducted)")
        return {"hit": False, "phones": "", "emails": "", "mailing": ""}

    persons = raw.get("persons", [])
    if not persons:
        logger.info("  → hit=True but persons list empty — treating as no-hit")
        return {"hit": False, "phones": "", "emails": "", "mailing": ""}

    # Tracerfy already matched on name, so persons[0] is the right person
    person  = persons[0]
    phones  = _parse_phones(person.get("phones", []))
    emails  = _parse_emails(person.get("emails", []))
    mailing = _parse_mailing(person.get("mailing_address", {}))

    logger.info(
        "  → HIT: phones=%r  emails=%r  mailing=%r",
        phones, emails, mailing,
    )
    return {"hit": True, "phones": phones, "emails": emails, "mailing": mailing}