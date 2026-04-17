"""
Trustee Registry for TN non-judicial foreclosure sales.

TRUSTEE_REGISTRY maps a canonical key → metadata dict with:
  canonical_name  - display name as it should appear in logs/sheets
  aliases         - list of all known name variants found in notice text
  site_url        - URL of the trustee's public sale listing page (or None)
  scraper         - scraper module name if one is built (or None)
  status          - "active" | "pending" | "no_scraper" | "no_site" | "needs_research"

lookup_trustee(raw_name) → (key, entry) or (None, None)
"""

import re

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

TRUSTEE_REGISTRY: dict[str, dict] = {
    "rubin_lublin": {
        "canonical_name": "Rubin Lublin TN, PLLC",
        "aliases": [
            "Rubin Lublin TN, PLLC",
            "Rubin Lublin TN PLLC",
            "Rubin Lublin TN",
            "Rubin Lublin",
        ],
        "site_url": "https://rlselaw.com/property-listing/tennessee-property-listings/",
        "scraper":  "rubin_lublin",
        "status":   "active",
    },
    "foundation_legal": {
        "canonical_name": "Foundation Legal Group, LLP",
        "aliases": [
            "Foundation Legal Group, LLP",
            "Foundation Legal Group LLP",
            "Foundation Legal Group",
            "Wilson & Associates, P.L.L.C.",
            "Wilson & Associates, PLLC",
            "Wilson & Associates",
            "Wilson & Associates P.L.L.C.",
        ],
        "site_url": "https://www.internetpostings.com",
        "scraper":  "internetpostings",
        "status":   "active",
    },
    "mackie_wolf": {
        "canonical_name": "Mackie Wolf Zientz & Mann, P.C.",
        "aliases": [
            "Mackie Wolf Zientz & Mann, P.C.",
            "Mackie Wolf Zientz & Mann, P.C",
            "Mackie Wolf Zientz & Mann PC",
            "Mackie Wolf Zientz and Mann, PC",
            "Mackie Wolf Zientz and Mann, P.C.",
            "Mackie Wolf Zientz and Mann PC",
            "MWZM",
        ],
        "site_url": "https://mwzmlaw.com/tn-investors/",
        "scraper": "mackie_wolf",
        "status": "active",
    },
    "llg_trustee": {
        "canonical_name": "LLG Trustee TN LLC",
        "aliases": [
            "LLG Trustee TN LLC",
            "LLG Trustee TN, LLC",
            "LLG Trustee Tennessee LLC",
            "LOGS Legal Group, LLP",  # ← customer_name on BCN
            "LOGS Legal Group LLP",
        ],
        "site_url": "https://betterchoicenotices.com/",
        "scraper": "better_choice_notices",
        "status": "active",  # was pending
    },
    "arnold_weiss": {
        "canonical_name": "The Law Offices of Arnold M. Weiss, PLLC",
        "aliases": [
            "The Law Offices of Arnold M. Weiss, PLLC",
            "Law Offices of Arnold M. Weiss, PLLC",
            "Arnold M. Weiss, PLLC",
            "Arnold M. Weiss",
            "Arnold M. Weiss, Esq.",
            "ARNOLD M. WEISS, ESQ.",
        ],
        # Posts on two platforms — foreclosure-postings.com is the Excel-download
        # one being built. BetterChoiceNotices.com is a separate scraper (pending).
        "site_url": "https://www.foreclosure-postings.com/Tennessee/",
        "scraper":  "foreclosure_postings",  # scraper in progress
        "status":   "pending",
    },
    "robertson_anschutz": {
        "canonical_name": "Robertson, Anschutz, Schneid, Crane & Partners, PLLC",
        "aliases": [
            "Robertson, Anschutz, Schneid, Crane & Partners, PLLC",
            "Robertson, Anschutz, Schneid, Crane & Partners, LLC",
            "Robertson, Anschutz, Schneid, Crane and Partners, PLLC",
            "Robertson Anschutz Schneid Crane & Partners PLLC",
            "Robertson Anschutz Schneid Crane and Partners PLLC",
            "RAS Crane",
            "RASC",
        ],
        "site_url": "https://www.rascranesalesinfo.com/",
        "scraper":  "robertson_anschutz",
        "status":   "active",
    },
    "brock_scott": {
        "canonical_name": "Brock & Scott, PLLC",
        "aliases": [
            "Brock & Scott, PLLC",
            "Brock & Scott PLLC",
            "Brock and Scott, PLLC",
            "Brock and Scott PLLC",
            "Brock & Scott",
        ],
        "site_url": "https://www.brockandscott.com/foreclosure-sales/?_sft_foreclosure_state=tn",
        "scraper":  "brock_scott",
        "status":   "active",
    },
    "albertelli_alaw": {
        "canonical_name": "James E. Albertelli, P.A. d/b/a ALAW",
        "aliases": [
            "James E. Albertelli, P.A. d/b/a ALAW",
            "James E. Albertelli, PA d/b/a ALAW",
            "James E. Albertelli, P.A. dba ALAW",
            "James E. Albertelli, P.A. d/b/a/ ALAW",
            "James E. Albertelli PA dba ALAW",
            "Albertelli Law",
            "ALAW",
        ],
        "site_url": "https://www.nwpostingservices.com/",
        "scraper": "nw_posting_services",
        "status": "active",
    },
    "marinosci": {
        "canonical_name": "Marinosci Law Group, P.C.",
        "aliases": [
            "Marinosci Law Group, P.C.",
            "Marinosci Law Group PC",
            "Marinosci Law Group P.C.",
            "Marinosci Law Group",
        ],
        "site_url": "https://www.nwpostingservices.com/",
        "scraper": "nw_posting_services",
        "status": "active",
    },
    "mcmichael_taylor": {
        "canonical_name": "McMichael Taylor Gray LLC",
        "aliases": [
            "McMichael Taylor Gray LLC",
            "McMichael Taylor Gray, LLC",
            "McMichael Taylor Gray",
            "MTG Law",
        ],
        "site_url": "https://anchorposting.com/tn-foreclosure-search/",
        "scraper": "anchor_posting",
        "status": "active",  # was no_scraper
    },
    "mickel_law": {
        "canonical_name": "Mickel Law Firm, P.A.",
        "aliases": [
            "Mickel Law Firm, P.A.",
            "Mickel Law Firm P.A.",
            "Mickel Law Firm, PA",
            "Mickel Law Firm PA",
            "Mickel Law Firm P A",
            "Mickel Law Firm",
        ],
        "site_url": "https://trustee-foreclosuresalesonline.com/",
        "scraper": "mickel_law",
        "status": "active",
    },
    "padgett_law": {
        "canonical_name": "Padgett Law Group",
        "aliases": [
            "Padgett Law Group",
            "Timothy D Padgett",
            "Timothy D. Padgett",
            "Timothy D Padgett / Padgett Law Group",
            "Timothy D Padgett / Padgett Law Group",
            "Timothy D Padgett, Padgett Law Group",
            "Timothy D. Padgett / Padgett Law Group",
        ],
        "site_url": "https://capitalcitypostings.com/tennessee-postings",
        "scraper":  "capital_city_postings",
        "status":   "active",
    },
    "allen_nelson_bowers": {
        "canonical_name": "Allen, Nelson & Bowers",
        "aliases": [
            "Allen, Nelson & Bowers",
            "Allen Nelson & Bowers",
            "Allen Nelson and Bowers",
        ],
        "site_url": "https://capitalcitypostings.com/tennessee-postings",
        "scraper":  "capital_city_postings",
        "status":   "needs_research",  # shares platform but client code unknown
    },
    "clear_recon": {
        "canonical_name": "Clear Recon LLC",
        "aliases": [
            "Clear Recon LLC",
            "Clear Recon, LLC",
            "Clear Recon",
        ],
        "site_url": "https://clearrecon-tn.com/tennessee-listings/",
        "scraper": "clear_recon",
        "status": "active",
    },
    "jones_binkley": {
        "canonical_name": "J. Phillip Jones / Jessica D. Binkley",
        "aliases": [
            "J. PHILLIP JONES AND/OR JESSICA D. BINKLEY",
            "J. PHILLIP JONES/JESSICA D. BINKLEY",
            "J. Phillip Jones / Jessica D. Binkley",
            "J. Phillip Jones and/or Jessica D. Binkley",
            "J Phillip Jones and or Jessica D Binkley",
            "J. Phillip Jones",
            "Phillip Jones",
        ],
        "site_url": "https://www.phillipjoneslaw.com/foreclosure-auctions.cfm?accept=yes",
        "scraper": "phillip_jones",
        "status": "active",
    },
    "mcwaters": {
        "canonical_name": "Thomas W. McWaters, Esq.",
        "aliases": [
            "Thomas W. McWaters, Esq.",
            "Thomas W. McWaters",
            "Thomas W McWaters Esq",
            "Thomas W McWaters",
            "Wood + Lamping LLP",
            "Wood and Lamping LLP",
            "Wood & Lamping LLP",
            "Wood & Lamping",
        ],
        "site_url": None,
        "scraper":  None,
        "status":   "no_site",
    },
    "meyer_burnett": {
        "canonical_name": "Meyer & Burnett PLLC",
        "aliases": [
            "Meyer & Burnett PLLC",
            "Meyer and Burnett PLLC",
            "Meyer & Burnett, PLLC",
            "Meyer and Burnett, PLLC",
            "Meyer & Burnett",
        ],
        "site_url": "https://www.nwpostingservices.com/",
        "scraper": "nw_posting_services",
        "status": "active",  # ← was needs_research
    },
    "vylla": {
        "canonical_name": "Vylla Solutions Tennessee LLC",
        "aliases": [
            "Vylla Solutions Tennessee LLC",
            "Vylla Solutions Tennessee, LLC",
            "Vylla Solutions",
            "Vylla",
        ],
        "site_url": "https://www.foreclosure-postings.com/Tennessee/",
        "scraper": "foreclosure_postings",  # scraper in progress
        "status": "pending",  # was no_scraper
    },
}


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def _normalize(name: str) -> str:
    """
    Reduce a trustee name to a canonical comparable form.
    Strips punctuation, legal suffixes, d/b/a clauses, and and/or constructs.
    """
    s = name.lower()
    # Standardize separators
    s = s.replace("&", "and").replace("+", "and")
    s = s.replace("/", " ").replace("\\", " ")
    # Remove d/b/a and everything that follows
    s = re.sub(r"\bd[- ]?/?b[- ]?/?a\b.*", "", s)
    # Collapse "and/or" → "and"
    s = s.replace("and/or", "and")
    # Remove punctuation (commas, periods, hyphens, apostrophes)
    s = re.sub(r"[,.\-']", " ", s)
    # Strip legal suffixes (longest first to avoid partial matches)
    _SUFFIXES = [
        "pllc", "llc", "llp", "pllp", "lllp",
        "pllc", "pla", "pa", "pc", "lp", "esq",
    ]
    for sfx in _SUFFIXES:
        s = re.sub(rf"\b{re.escape(sfx)}\b", "", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


# Build lookup index: normalized alias → registry key
_ALIAS_INDEX: dict[str, str] = {}
for _key, _entry in TRUSTEE_REGISTRY.items():
    for _alias in _entry["aliases"]:
        _norm = _normalize(_alias)
        _ALIAS_INDEX[_norm] = _key


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def lookup_trustee(raw_name: str) -> tuple[str | None, dict | None]:
    """
    Given a raw trustee/firm name from a notice or sheet row, return
    (registry_key, registry_entry).  Returns (None, None) if not found.

    Matching order:
    1. Exact normalized match
    2. Substring match (longer alias contains shorter raw_name or vice versa)
    """
    norm = _normalize(raw_name)
    if not norm:
        return None, None

    # Exact
    if norm in _ALIAS_INDEX:
        k = _ALIAS_INDEX[norm]
        return k, TRUSTEE_REGISTRY[k]

    # Substring fallback
    for alias_norm, k in _ALIAS_INDEX.items():
        if alias_norm and (alias_norm in norm or norm in alias_norm):
            return k, TRUSTEE_REGISTRY[k]

    return None, None