"""
heir_research.py — Phase 4: Deceased owner obituary research and heir extraction.

Triggered by: listings in the Auctions sheet where Deceased (col Y) = "Yes"
              and Heir Research Date (col AE) is blank.

For each qualifying listing:
  1. Calls Claude (with web_search) to find an obituary for the owner
  2. Extracts heirs / survivors with relationships
  3. Cross-references heir names against the foreclosure defendant (col F)
  4. Writes structured results to Auctions tab cols AA–AE
  5. Writes one row per heir to the "Heir Leads" tab

Heir Leads tab is the Prop.ai-ready staging area for a separate calling
campaign using heir/relative scripts. Contact info (phones/emails) is
populated by a follow-on heir skip trace step (Phase 4b — not yet built).

Usage (from main.py --heirresearch):
    from heir_research import run_heir_research
    results = run_heir_research(listings, dry_run=False)

Output contract per result dict:
    id                  int   — DB primary key, passed through from input
    _skipped            bool  — True if dry run or unrecoverable error
    _error              str|None
    _heirs_list         list  — [{"name": str, "relationship": str}, ...]
    _defendant_match    bool  — True if any heir matched a defendant name
    Obit Found          str   — "Yes" / "No" / ""
    Obit Summary        str   — narrative paragraph
    Heirs               str   — "Name (relationship); Name (relationship)"
    Defendant Match     str   — "Yes — Full Name (relationship)" or "No"
    Heir Research Date  str   — YYYY-MM-DD
"""

from __future__ import annotations

import json
import logging
import re
import time
import traceback
from datetime import date

import anthropic

from config import ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Model used for obituary research — update here if you switch versions.
# Opus gives better nuanced reading of obit text vs Sonnet.
CLAUDE_MODEL    = "claude-opus-4-5"
MAX_TOKENS      = 2000
CALL_DELAY_SECS = 3        # seconds between Claude calls
MAX_RETRIES     = 2
RETRY_DELAYS    = [10, 20] # seconds before retry 1, retry 2


# ── Anthropic client — lazy singleton ─────────────────────────────────────────

_client: anthropic.Anthropic | None = None

def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    return _client


# ── Public entry point ────────────────────────────────────────────────────────

def run_heir_research(listings: list[dict], dry_run: bool = False) -> list[dict]:
    """
    Research obituaries for all listings in `listings`.

    `listings` is the list returned by
    storage.get_listings_needing_heir_research(). Each dict has at
    minimum: id, Street, City, County, State, Defendant(s),
    Owner Name (Primary).

    Returns a list of result dicts consumed by:
      - sheets_writer.update_heir_research()   → writes Auctions AA:AE
      - sheets_writer.write_heir_leads()       → writes Heir Leads tab rows
    """
    results = []
    today   = date.today().isoformat()
    total   = len(listings)

    for i, listing in enumerate(listings):
        owner_name   = listing.get("Owner Name (Primary)", "").strip()
        street       = listing.get("Street", "").strip()
        city         = listing.get("City", "").strip()
        county       = listing.get("County", "").strip()
        state        = listing.get("State", "").strip()
        defendant_str = listing.get("Defendant(s)", "").strip()

        display = f"{owner_name or '(no name)'} — {street}, {city}"
        logger.info(f"  [{i+1}/{total}] {display}")

        if dry_run:
            logger.info("    [DRY RUN] Skipping Claude call.")
            results.append({**listing, "_skipped": True, "_error": None,
                            "_heirs_list": [], "_defendant_match": False})
            continue

        if not owner_name:
            logger.warning("    No owner name — marking for manual check.")
            results.append(_no_name_result(listing, today))
            continue

        try:
            prompt   = _build_prompt(owner_name, street, city, county, state, defendant_str)
            raw_text = _call_claude(prompt)

            if not raw_text:
                raise RuntimeError("Empty response from Claude.")

            parsed           = _parse_claude_response(raw_text)
            heirs            = parsed.get("heirs", [])
            defendant_match  = _cross_reference(heirs, defendant_str)
            heirs_str        = _format_heirs(heirs)

            obit_found = parsed.get("obit_found", False)
            result = {
                **listing,
                "_skipped":          False,
                "_error":            None,
                "_heirs_list":       heirs,
                "_defendant_match":  "Yes" in defendant_match,
                "Obit Found":        "Yes" if obit_found else "No",
                "Obit Summary":      parsed.get("summary", ""),
                "Heirs":             heirs_str,
                "Defendant Match":   defendant_match,
                "Heir Research Date": today,
            }
            results.append(result)

            status = "Found" if obit_found else "Not found"
            logger.info(
                f"    Obit: {status} | Heirs: {len(heirs)} | "
                f"Defendant match: {defendant_match}"
            )

        except Exception as e:
            logger.error(f"    ERROR: {e}")
            traceback.print_exc()
            results.append(_error_result(listing, today, str(e)))

        if i < total - 1:
            time.sleep(CALL_DELAY_SECS)

    return results


# ── Claude API call ───────────────────────────────────────────────────────────

def _call_claude(prompt: str) -> str | None:
    """
    Call Claude with web_search enabled. Retries on 529 overload.
    Returns the full text of Claude's response, or None on failure.
    """
    client = _get_client()

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=MAX_TOKENS,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role": "user", "content": prompt}],
            )

            # Collect all text blocks from the response
            text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    text += block.text

            return text.strip() or None

        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < MAX_RETRIES:
                delay = RETRY_DELAYS[attempt]
                logger.warning(
                    f"    Claude overloaded (529) — retrying in {delay}s "
                    f"(attempt {attempt + 1}/{MAX_RETRIES})..."
                )
                time.sleep(delay)
                continue
            raise

    return None


# ── Prompt construction ───────────────────────────────────────────────────────

def _build_prompt(
    owner_name: str,
    street: str,
    city: str,
    county: str,
    state: str,
    defendant_str: str,
) -> str:
    return f"""You are researching a real estate foreclosure case. The property owner is deceased.

Property: {street}, {city}, {county} County, {state}
Deceased Owner: {owner_name}
Defendant(s) listed in foreclosure: {defendant_str or "(not available)"}

Search for an obituary for this person. Try:
  - "{owner_name}" obituary {city} {state}
  - "{owner_name}" obituary {county} county {state}

If you find an obituary that plausibly matches this person (consistent location and time period):
  1. Note the publication and approximate date
  2. Extract ALL listed survivors with their relationships (wife, husband, son, daughter, \
brother, sister, stepson, stepdaughter, etc.)
  3. Note if any survivor name matches a defendant listed in the foreclosure

Return ONLY a JSON object. No markdown formatting, no text before or after:
{{
  "obit_found": true,
  "obit_source": "Publication Name, Month YYYY",
  "heirs": [
    {{"name": "Full Name", "relationship": "son"}},
    {{"name": "Full Name", "relationship": "wife"}}
  ],
  "confidence": "high",
  "summary": "Owner [Name] deceased. Obit found ([source]). Survivors: [comma-separated list with relationships]. [Note if any match a foreclosure defendant.]"
}}

If NO matching obituary is found:
{{
  "obit_found": false,
  "obit_source": "",
  "heirs": [],
  "confidence": "none",
  "summary": "No obituary found — manual check recommended."
}}

Return only the JSON object."""


# ── Response parsing ──────────────────────────────────────────────────────────

def _parse_claude_response(text: str) -> dict:
    """
    Parse Claude's JSON response. Handles markdown fences and stray text.
    Falls back to a safe no-hit dict on any parse failure.
    """
    # Strip markdown code fences
    text = re.sub(r"```(?:json)?\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()

    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find first {...} block
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    logger.warning("    Could not parse Claude response as JSON — defaulting to no-hit.")
    return {
        "obit_found":  False,
        "obit_source": "",
        "heirs":       [],
        "confidence":  "none",
        "summary":     "Error parsing research results — manual check recommended.",
    }


# ── Defendant cross-reference ─────────────────────────────────────────────────

def _cross_reference(heirs: list[dict], defendant_str: str) -> str:
    """
    Compare heir last names against all defendant last names.

    Returns "Yes — Name (relationship), Name (relationship)" if any match,
    or "No" if none match.

    Matching is by last name only (case-insensitive). This gives a high
    degree of certainty when combined with the same property address.
    """
    if not heirs or not defendant_str:
        return "No"

    # Strip "et al.", "et al", "Deceased", etc.
    clean = re.sub(
        r"\bet\.?\s*al\.?\b|\bdeceased\b|\bunknown\s+heirs?\b",
        "", defendant_str, flags=re.IGNORECASE
    )

    # Split on common delimiters
    raw_defendants = re.split(r"[,;&]|\band\b", clean, flags=re.IGNORECASE)

    def_last_names: set[str] = set()
    for d in raw_defendants:
        d = d.strip()
        if not d:
            continue
        # Handle "Last, First" vs "First Last"
        if "," in d:
            last = d.split(",")[0].strip()
        else:
            parts = d.split()
            last = parts[-1] if parts else ""
        if last:
            def_last_names.add(last.lower())

    matches = []
    for heir in heirs:
        name = heir.get("name", "").strip()
        if not name:
            continue
        parts     = name.split()
        heir_last = parts[-1].lower() if parts else ""
        if heir_last and heir_last in def_last_names:
            rel = heir.get("relationship", "heir")
            matches.append(f"{name} ({rel})")

    if matches:
        return "Yes — " + ", ".join(matches)
    return "No"


# ── Formatting helpers ────────────────────────────────────────────────────────

def _format_heirs(heirs: list[dict]) -> str:
    """
    Format heirs list as a semicolon-separated string for the sheet cell.
    Example: "John Smith (son); Patricia Hickey (wife)"
    """
    parts = []
    for h in heirs:
        name = h.get("name", "").strip()
        rel  = h.get("relationship", "").strip()
        if name:
            parts.append(f"{name} ({rel})" if rel else name)
    return "; ".join(parts)


# ── Default result constructors ───────────────────────────────────────────────

def _no_name_result(listing: dict, today: str) -> dict:
    return {
        **listing,
        "_skipped":          False,
        "_error":            "No owner name",
        "_heirs_list":       [],
        "_defendant_match":  False,
        "Obit Found":        "",
        "Obit Summary":      "No owner name available — manual check recommended.",
        "Heirs":             "",
        "Defendant Match":   "No",
        "Heir Research Date": today,
    }


def _error_result(listing: dict, today: str, error: str) -> dict:
    return {
        **listing,
        "_skipped":          False,
        "_error":            error,
        "_heirs_list":       [],
        "_defendant_match":  False,
        "Obit Found":        "",
        "Obit Summary":      f"Research error — manual check recommended.",
        "Heirs":             "",
        "Defendant Match":   "No",
        "Heir Research Date": today,
    }