"""
config.py — Eagle Creek Auction Monitor
Central configuration for all sources, credentials paths, and sheet settings.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Google Sheets ─────────────────────────────────────────────────────────────
# Create a Google Sheet and paste its ID here (the long string in the URL)
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = "Auctions"  # Name of the tab within the spreadsheet

# ── Google / Gmail API credentials ────────────────────────────────────────────
# Path to credentials.json downloaded from Google Cloud Console
GOOGLE_CREDENTIALS_PATH = "credentials.json"
# Token will be auto-created on first run
GMAIL_TOKEN_PATH = "gmail_token.json"
SHEETS_TOKEN_PATH = "sheets_token.json"

# ── Anthropic API ─────────────────────────────────────────────────────────────
# Used for parsing unstructured email content and messy legal notice text
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

BATCHDATA_API_KEY =os.environ["BATCHDATA_API_KEY"]

TRACERFY_API_KEY = os.environ["TRACERFY_API_KEY"]

TRACERFY_INSTANT_TRACE_URL = os.environ.get(
    "TRACERFY_INSTANT_TRACE_URL",
    "https://app.fastappend.com/v1/api/instant-trace/"
)
# ── Email Sources ─────────────────────────────────────────────────────────────
EMAIL_SOURCES = [
    {
        "county": "Scott",
        "state": "KY",
        "sender": "kgross@carrowaylaw.com",
        "sender_name": "Kathy Gross",
    },
    {
        "county": "Rowan",
        "state": "KY",
        "sender": "budsalyer4@gmail.com",
        "sender_name": "Bud Salyer",
    },
]

TNPUBLICNOTICE_AUTH_COOKIE = os.environ.get("TNPUBLICNOTICE_AUTH_COOKIE","")
# How many days back to search Gmail for sale emails
GMAIL_LOOKBACK_DAYS = 30

# ── Web Sources ───────────────────────────────────────────────────────────────
WEB_SOURCES = [
    {
        "county": "Kenton",
        "state": "KY",
        "scraper": "kenton",
        "base_url": "https://tmcsales.info",
        # Pages follow pattern /{month}-{year}.html e.g. march-2026.html
    },
    {
        "county": "Boone",
        "state": "KY",
        "scraper": "boone",
        "url": "https://apps6.boonecountyky.org/BCMCSalesApp",
    },
    {
        "county": "Jessamine",
        "state": "KY",
        "scraper": "jessamine",
        "url": "https://jessaminemc.com/showcase.php?action=list&range=future",
    },
    {
        "county": "Fayette",
        "state": "KY",
        "scraper": "fayette",
        "url": "https://faycom.info/showcase.php?action=list&range=future",
        # Note: This site 403s on direct fetch — may need session cookies.
        # Falls back to Claude-assisted parse if blocked.
    },
    {
        "county": "Jefferson",
        "state": "KY",
        "scraper": "jefferson_ky",
        "url": "https://www.jeffcomm.org/upcoming-sales.php",
    },
    {
        "county": "Campbell",
        "state": "KY",
        "scraper": "campbell_ky",
        # Campbell posts via kypublicnotice.com (LINK Kenton Reader)
        "url": "https://www.kypublicnotice.com/",
        "search_terms": ["CAMPBELL COUNTY COMMISSIONER'S SALE", "CAMPBELL CIRCUIT COURT"],
    },
    {
        "county": "Hamilton",
        "state": "OH",
        "scraper": "hamilton_oh",
        "url": "https://hamilton.sheriffsaleauction.ohio.gov/",
    },
    {
        "county": "Jefferson",
        "state": "IN",
        "scraper": "jefferson_in",
        "url": "https://legacy.sri-taxsale.com/Foreclosure/PropertyListing.aspx?county=JEFFERSON",
    },
    {
        "county": "Knox",
        "state": "TN",
        "scraper": "knox_tn",
        # Tennessee is non-judicial — trustee sales posted on tnpublicnotice.com
        "url": "https://tnpublicnotice.com/",
        "search_terms": ["Knox County", "Trustee Sale", "Foreclosure"],
    },
]

# ── Sheet Column Headers ───────────────────────────────────────────────────────
# These map directly to columns in the Google Sheet, in order.
SHEET_COLUMNS = [
    "County",           # A
    "State",            # B
    "Sale Date",        # C
    "Case Number",      # D
    "Plaintiff",        # E
    "Defendant(s)",     # F
    "Property Address", # G
    "Appraised Value",  # H
    "Judgment / Loan Amount",  # I
    "Attorney / Firm",  # J
    "Estimated Market Value",  # K — populated by --valuate
    "Estimated Equity",        # L — calculated: EMV minus Judgment
    "Equity Signal",           # M — 🏆 / ✅ / ⚠️ / ❌ / ❓
    "Cancelled",        # N
    "Source URL",       # O
    "Date Added",       # P
    "Notes",            # Q
]

# ── Deduplication Key ─────────────────────────────────────────────────────────
# Two fields that together uniquely identify a listing
DEDUP_FIELDS = ["County", "Case Number"]

# ── Equity Signal Thresholds ──────────────────────────────────────────────────
# Equity % = (EMV - Judgment) / EMV
# Signals are assigned based on these thresholds.
EQUITY_THRESHOLDS = {
    "home_run":  0.40,   # 40%+ equity  → 🏆 Home Run
    "decent":    0.25,   # 25–39%       → ✅ Decent
    "tight":     0.10,   # 10–24%       → ⚠️ Tight
    # below 10% or negative → ❌ Underwater
    # no judgment amount    → ❓ Unclear
}

# ── Request Headers ───────────────────────────────────────────────────────────
# Polite browser-like headers to avoid immediate blocks
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

TN_VALUATE_COUNTIES: list[str] | None = [
"Knox",
"Loudon",
"Anderson",
"Blount",
"Sevier"
"Hamilton",
"Bradley",
"McMinn",
"Marion",
"Sequatchie",
"Rutherford",
"Williamson",
"Davidson",
"Sumner",
"Wilson"
]