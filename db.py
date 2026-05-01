"""
db.py — Eagle Creek Auction Monitor
SQLite persistence layer. Replaces Google Sheets as the primary data store.

Tables:
  listings     — main auction pipeline (phases 1–4)
  heir_leads   — individual heir contacts (phase 4b)
  needs_review — listings that failed admission gates

Dedup key for listings: (county, street_number, sale_date)
  county       → stored/compared lowercase
  street_number → leading digits extracted from street at insert time
  sale_date    → ISO format YYYY-MM-DD
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent / "auction_monitor.db"

MIN_DAYS_OUT = 3
_SKIPTRACE_QUALIFYING_SIGNALS = {"🏆", "✅", "❓"}


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

@contextmanager
def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS listings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,

    -- core scrape fields
    county              TEXT NOT NULL,
    state               TEXT,
    sale_date           TEXT,           -- YYYY-MM-DD
    case_number         TEXT,
    plaintiff           TEXT,
    defendants          TEXT,
    street              TEXT,
    street_number       TEXT,           -- leading digits, used for dedup
    city                TEXT,
    zip                 TEXT,
    appraised_value     TEXT,
    judgment            TEXT,
    attorney            TEXT,

    -- valuation (phase 2)
    est_market_value    TEXT,
    est_equity          TEXT,
    equity_signal       TEXT,

    -- admin
    cancelled           TEXT,
    source_url          TEXT,
    date_added          TEXT,           -- YYYY-MM-DD
    notes               TEXT,

    -- skip trace (phase 3)
    owner_primary           TEXT,
    owner_secondary         TEXT,
    owner_first             TEXT,       -- first (+ middle) name, primary owner
    owner_last              TEXT,       -- last name, primary owner
    owner_secondary_first   TEXT,       -- first (+ middle) name, secondary owner
    owner_secondary_last    TEXT,       -- last name, secondary owner
    owner_phones        TEXT,
    owner_emails        TEXT,
    mailing_address     TEXT,
    deceased            TEXT,
    skiptrace_date      TEXT,           -- YYYY-MM-DD

    -- directskip (phase 3b — future)
    directskip_date     TEXT,           -- YYYY-MM-DD, populated when results ingested

    -- outreach tracking
    propai_pushed_at    TEXT,           -- YYYY-MM-DD, set when pushed to Prop.ai campaign

    -- heir research (phase 4)
    obit_found          TEXT,
    obit_summary        TEXT,
    heirs               TEXT,
    defendant_match     TEXT,
    heir_research_date  TEXT,           -- YYYY-MM-DD

    UNIQUE (county, street_number, sale_date)
);

CREATE TABLE IF NOT EXISTS heir_leads (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id          INTEGER REFERENCES listings(id),

    property_street     TEXT,
    property_city       TEXT,
    county              TEXT,
    state               TEXT,
    sale_date           TEXT,
    equity_signal       TEXT,
    deceased_owner      TEXT,
    heir_name           TEXT,
    relationship        TEXT,
    defendant_match     TEXT,

    -- heir skip trace (phase 4b)
    phones              TEXT,
    emails              TEXT,
    mailing_address     TEXT,
    skip_traced_date    TEXT,
    status              TEXT
);

CREATE TABLE IF NOT EXISTS directskip_persons (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id      INTEGER REFERENCES listings(id),
    person_number   INTEGER NOT NULL,   -- 1, 2, or 3

    result_code     TEXT,               -- AB1 / AB2 / CI / etc. (person 1 only)
    first_name      TEXT,
    last_name       TEXT,
    age             TEXT,
    deceased        TEXT,               -- Y or N

    phone1 TEXT, phone1_type TEXT,
    phone2 TEXT, phone2_type TEXT,
    phone3 TEXT, phone3_type TEXT,
    phone4 TEXT, phone4_type TEXT,
    phone5 TEXT, phone5_type TEXT,
    phone6 TEXT, phone6_type TEXT,
    phone7 TEXT, phone7_type TEXT,

    email1          TEXT,
    email2          TEXT,

    mailing_street  TEXT,
    mailing_city    TEXT,
    mailing_state   TEXT,
    mailing_zip     TEXT,

    UNIQUE (listing_id, person_number)
);

CREATE TABLE IF NOT EXISTS directskip_relatives (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id      INTEGER REFERENCES listings(id),
    person_number   INTEGER NOT NULL,   -- which owner (1/2/3) this relative belongs to
    relative_number INTEGER NOT NULL,   -- 1–5

    name            TEXT,
    age             TEXT,

    phone1 TEXT, phone1_type TEXT,
    phone2 TEXT, phone2_type TEXT,
    phone3 TEXT, phone3_type TEXT,
    phone4 TEXT, phone4_type TEXT,
    phone5 TEXT, phone5_type TEXT,

    -- for future proximity-based calling campaign
    called          INTEGER DEFAULT 0,
    call_date       TEXT,

    UNIQUE (listing_id, person_number, relative_number)
);

CREATE TABLE IF NOT EXISTS needs_review (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    county      TEXT,
    state       TEXT,
    sale_date   TEXT,
    case_number TEXT,
    plaintiff   TEXT,
    defendants  TEXT,
    street      TEXT,
    city        TEXT,
    zip         TEXT,
    appraised_value TEXT,
    judgment    TEXT,
    attorney    TEXT,
    cancelled   TEXT,
    source_url  TEXT,
    date_added  TEXT,
    notes       TEXT,
    reason      TEXT,           -- why it was routed here
    reviewed    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS propai_pushes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id    INTEGER REFERENCES listings(id),
    campaign_id   TEXT NOT NULL,
    campaign_name TEXT,
    pushed_at     TEXT NOT NULL,    -- YYYY-MM-DD
    UNIQUE (listing_id, campaign_id)
);

CREATE TABLE IF NOT EXISTS propai_results (
    lead_id            TEXT PRIMARY KEY,
    campaign_id        TEXT NOT NULL,
    listing_id         INTEGER REFERENCES listings(id),
    phone_number       TEXT,
    prospect_name      TEXT,
    lead_status        TEXT,   -- New / Called - No Answer / Called - Human Answered / Callback Requested
    call_status        TEXT,   -- completed / failed
    answered_by        TEXT,   -- human / voicemail / unknown
    callback_requested INTEGER DEFAULT 0,
    total_calls        INTEGER DEFAULT 0,
    last_call_dt       TEXT,
    call_summary       TEXT,
    analysis           TEXT,
    transcript         TEXT,
    synced_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS phoneburner_contacts (
    contact_user_id TEXT PRIMARY KEY,
    listing_id      INTEGER REFERENCES listings(id),
    phone           TEXT,       -- primary phone used at push time
    pushed_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS phoneburner_results (
    call_id         TEXT PRIMARY KEY,
    dialsession_id  TEXT NOT NULL,
    listing_id      INTEGER REFERENCES listings(id),
    contact_user_id TEXT,
    phone           TEXT,
    disposition     TEXT,
    connected       INTEGER DEFAULT 0,
    voicemail       INTEGER DEFAULT 0,
    voicemail_sent  TEXT,
    note            TEXT,
    start_when      TEXT,
    end_when        TEXT,
    synced_at       TEXT NOT NULL
);
"""


def init_db() -> None:
    with _conn() as con:
        con.executescript(_DDL)
    _migrate_owner_name_cols()
    _migrate_propai_col()
    _migrate_phoneburner_cols()


def _migrate_owner_name_cols() -> None:
    """
    Add owner first/last name columns if they don't exist yet (schema migration),
    then backfill all rows that have owner_primary populated but owner_first empty.
    Safe to call repeatedly — no-ops once columns exist and backfill is done.
    """
    new_cols = [
        "owner_first",
        "owner_last",
        "owner_secondary_first",
        "owner_secondary_last",
        "directskip_date",
    ]
    with _conn() as con:
        existing = {row[1] for row in con.execute("PRAGMA table_info(listings)").fetchall()}
        for col in new_cols:
            if col not in existing:
                con.execute(f"ALTER TABLE listings ADD COLUMN {col} TEXT DEFAULT ''")

        # Backfill rows that have a primary name but empty first/last
        rows = con.execute(
            "SELECT id, owner_primary, owner_secondary FROM listings "
            "WHERE (owner_primary IS NOT NULL AND owner_primary != '') "
            "  AND (owner_first IS NULL OR owner_first = '')"
        ).fetchall()
        for row in rows:
            first, last         = _split_name(row["owner_primary"]  or "")
            sec_first, sec_last = _split_name(row["owner_secondary"] or "")
            con.execute(
                "UPDATE listings SET owner_first=?, owner_last=?, "
                "owner_secondary_first=?, owner_secondary_last=? WHERE id=?",
                (first, last, sec_first, sec_last, row["id"]),
            )


def _migrate_propai_col() -> None:
    """Add propai_pushed_at column to listings if it doesn't exist yet."""
    with _conn() as con:
        existing = {row[1] for row in con.execute("PRAGMA table_info(listings)").fetchall()}
        if "propai_pushed_at" not in existing:
            con.execute("ALTER TABLE listings ADD COLUMN propai_pushed_at TEXT DEFAULT ''")
        # propai_pushes and propai_results are created by _DDL via executescript (IF NOT EXISTS)


def _migrate_phoneburner_cols() -> None:
    """Add PhoneBurner-related columns to existing tables."""
    with _conn() as con:
        listings_cols = {row[1] for row in con.execute("PRAGMA table_info(listings)").fetchall()}
        if "follow_up_status" not in listings_cols:
            con.execute(
                "ALTER TABLE listings ADD COLUMN follow_up_status TEXT DEFAULT 'active'"
            )

        persons_cols = {
            row[1] for row in con.execute("PRAGMA table_info(directskip_persons)").fetchall()
        }
        if "phone_status" not in persons_cols:
            con.execute(
                "ALTER TABLE directskip_persons ADD COLUMN phone_status TEXT DEFAULT 'unknown'"
            )
        # phoneburner_contacts and phoneburner_results are created by _DDL (IF NOT EXISTS)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_street_number(street: str) -> str:
    if not street:
        return ""
    m = re.match(r"^(\d+)", street.strip())
    return m.group(1) if m else ""


def _split_name(full_name: str) -> tuple[str, str]:
    """
    Split a full name string into (first, last).
    'John Smith'        → ('John', 'Smith')
    'Mary Jane Watson'  → ('Mary Jane', 'Watson')
    'Madonna'           → ('Madonna', '')
    Returns ('', '') for blank input.
    """
    parts = (full_name or "").strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


def _parse_sale_date(s: str) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s.strip())
    except ValueError:
        return None


def _row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row)


# ---------------------------------------------------------------------------
# Phase 1 — Ingest new listings
# ---------------------------------------------------------------------------

def insert_listing(listing: dict) -> tuple[int | None, bool]:
    """Insert one listing dict.  Returns (id, inserted) where inserted=False means duplicate."""
    county = listing.get("County", "").strip().lower()
    street = listing.get("Street", "").strip()
    street_number = _extract_street_number(street)
    sale_date = listing.get("Sale Date", "").strip()

    sql = """
        INSERT OR IGNORE INTO listings (
            county, state, sale_date, case_number, plaintiff, defendants,
            street, street_number, city, zip,
            appraised_value, judgment, attorney,
            est_market_value, est_equity, equity_signal,
            cancelled, source_url, date_added, notes
        ) VALUES (
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            '', '', '',
            ?, ?, ?, ?
        )
    """
    params = (
        county,
        listing.get("State", ""),
        sale_date,
        listing.get("Case Number", ""),
        listing.get("Plaintiff", ""),
        listing.get("Defendant(s)", ""),
        street,
        street_number,
        listing.get("City", ""),
        listing.get("Zip", ""),
        listing.get("Appraised Value", ""),
        listing.get("Judgment / Loan Amount", ""),
        listing.get("Attorney / Firm", ""),
        listing.get("Cancelled", ""),
        listing.get("Source URL", ""),
        listing.get("Date Added", str(date.today())),
        listing.get("Notes", ""),
    )

    with _conn() as con:
        cur = con.execute(sql, params)
        if cur.lastrowid and cur.rowcount:
            return cur.lastrowid, True
        # Fetch existing id for the duplicate
        existing = con.execute(
            "SELECT id FROM listings WHERE county=? AND street_number=? AND sale_date=?",
            (county, street_number, sale_date),
        ).fetchone()
        return (existing["id"] if existing else None), False


def insert_needs_review(listing: dict, reason: str) -> None:
    sql = """
        INSERT INTO needs_review (
            county, state, sale_date, case_number, plaintiff, defendants,
            street, city, zip, appraised_value, judgment, attorney,
            cancelled, source_url, date_added, notes, reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        listing.get("County", "").strip().lower(),
        listing.get("State", ""),
        listing.get("Sale Date", ""),
        listing.get("Case Number", ""),
        listing.get("Plaintiff", ""),
        listing.get("Defendant(s)", ""),
        listing.get("Street", ""),
        listing.get("City", ""),
        listing.get("Zip", ""),
        listing.get("Appraised Value", ""),
        listing.get("Judgment / Loan Amount", ""),
        listing.get("Attorney / Firm", ""),
        listing.get("Cancelled", ""),
        listing.get("Source URL", ""),
        listing.get("Date Added", str(date.today())),
        listing.get("Notes", ""),
        reason,
    )
    with _conn() as con:
        con.execute(sql, params)


# ---------------------------------------------------------------------------
# Phase 2 — Valuation
# ---------------------------------------------------------------------------

def get_listings_needing_valuation() -> list[dict]:
    sql = """
        SELECT * FROM listings
        WHERE (est_market_value IS NULL OR est_market_value = '')
          AND (cancelled IS NULL OR LOWER(cancelled) != 'yes')
          AND sale_date >= ?
        ORDER BY sale_date
    """
    cutoff = str(date.today() + timedelta(days=MIN_DAYS_OUT))
    with _conn() as con:
        rows = con.execute(sql, (cutoff,)).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_valuation(listing_id: int, emv: str, equity: str, signal: str) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE listings SET est_market_value=?, est_equity=?, equity_signal=? WHERE id=?",
            (emv, equity, signal, listing_id),
        )


def update_cancelled(listing_id: int, value: str) -> None:
    with _conn() as con:
        con.execute("UPDATE listings SET cancelled=? WHERE id=?", (value, listing_id))


# ---------------------------------------------------------------------------
# Phase 3 — Skip trace
# ---------------------------------------------------------------------------

def get_listings_needing_skiptrace() -> list[dict]:
    placeholders = ",".join("?" * len(_SKIPTRACE_QUALIFYING_SIGNALS))
    sql = f"""
        SELECT * FROM listings
        WHERE equity_signal IN ({placeholders})
          AND (cancelled IS NULL OR LOWER(cancelled) != 'yes')
          AND sale_date >= ?
          AND (skiptrace_date IS NULL OR skiptrace_date = '')
        ORDER BY sale_date
    """
    cutoff = str(date.today() + timedelta(days=MIN_DAYS_OUT))
    params = (*_SKIPTRACE_QUALIFYING_SIGNALS, cutoff)
    with _conn() as con:
        rows = con.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_skiptrace(listing_id: int, result: dict) -> None:
    owner_primary   = result.get("Owner Name (Primary)", "")
    owner_secondary = result.get("Owner Name (Secondary)", "")
    first, last         = _split_name(owner_primary)
    sec_first, sec_last = _split_name(owner_secondary)

    sql = """
        UPDATE listings SET
            owner_primary           = ?,
            owner_secondary         = ?,
            owner_first             = ?,
            owner_last              = ?,
            owner_secondary_first   = ?,
            owner_secondary_last    = ?,
            owner_phones            = ?,
            owner_emails            = ?,
            mailing_address         = ?,
            deceased                = ?,
            skiptrace_date          = ?
        WHERE id = ?
    """
    params = (
        owner_primary,
        owner_secondary,
        first,
        last,
        sec_first,
        sec_last,
        result.get("Owner Phone(s)", ""),
        result.get("Owner Email(s)", ""),
        result.get("Mailing Address", ""),
        result.get("Deceased", ""),
        result.get("Skip Trace Date", str(date.today())),
        listing_id,
    )
    with _conn() as con:
        con.execute(sql, params)


# ---------------------------------------------------------------------------
# Phase 4 — Heir research
# ---------------------------------------------------------------------------

def get_listings_needing_heir_research() -> list[dict]:
    sql = """
        SELECT * FROM listings
        WHERE LOWER(deceased) = 'yes'
          AND (heir_research_date IS NULL OR heir_research_date = '')
          AND (cancelled IS NULL OR LOWER(cancelled) != 'yes')
          AND sale_date >= ?
        ORDER BY sale_date
    """
    cutoff = str(date.today() + timedelta(days=MIN_DAYS_OUT))
    with _conn() as con:
        rows = con.execute(sql, (cutoff,)).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_heir_research(listing_id: int, result: dict) -> None:
    sql = """
        UPDATE listings SET
            obit_found          = ?,
            obit_summary        = ?,
            heirs               = ?,
            defendant_match     = ?,
            heir_research_date  = ?
        WHERE id = ?
    """
    params = (
        result.get("Obit Found", ""),
        result.get("Obit Summary", ""),
        result.get("Heirs", ""),
        result.get("Defendant Match", ""),
        result.get("Heir Research Date", str(date.today())),
        listing_id,
    )
    with _conn() as con:
        con.execute(sql, params)


def insert_heir_lead(listing_id: int | None, lead: dict) -> int:
    sql = """
        INSERT INTO heir_leads (
            listing_id, property_street, property_city, county, state,
            sale_date, equity_signal, deceased_owner, heir_name, relationship,
            defendant_match, phones, emails, mailing_address, skip_traced_date, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        listing_id,
        lead.get("Property Street", ""),
        lead.get("Property City", ""),
        lead.get("County", ""),
        lead.get("State", ""),
        lead.get("Sale Date", ""),
        lead.get("Equity Signal", ""),
        lead.get("Deceased Owner", ""),
        lead.get("Heir Name", ""),
        lead.get("Relationship", ""),
        lead.get("Defendant Match", ""),
        lead.get("Phone(s)", ""),
        lead.get("Email(s)", ""),
        lead.get("Mailing Address", ""),
        lead.get("Skip Traced Date", ""),
        lead.get("Status", ""),
    )
    with _conn() as con:
        cur = con.execute(sql, params)
        return cur.lastrowid


def get_heir_leads_needing_skiptrace() -> list[dict]:
    sql = """
        SELECT * FROM heir_leads
        WHERE (skip_traced_date IS NULL OR skip_traced_date = '')
        ORDER BY id
    """
    with _conn() as con:
        rows = con.execute(sql).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_heir_lead_skiptrace(lead_id: int, result: dict) -> None:
    sql = """
        UPDATE heir_leads SET
            phones           = ?,
            emails           = ?,
            mailing_address  = ?,
            skip_traced_date = ?
        WHERE id = ?
    """
    params = (
        result.get("Phone(s)", ""),
        result.get("Email(s)", ""),
        result.get("Mailing Address", ""),
        result.get("Skip Traced Date", str(date.today())),
        lead_id,
    )
    with _conn() as con:
        con.execute(sql, params)


# ---------------------------------------------------------------------------
# General queries
# ---------------------------------------------------------------------------

def get_listing_by_id(listing_id: int) -> dict | None:
    with _conn() as con:
        row = con.execute("SELECT * FROM listings WHERE id=?", (listing_id,)).fetchone()
    return _row_to_dict(row) if row else None


def get_all_listings(include_past: bool = False) -> list[dict]:
    sql = "SELECT * FROM listings"
    params: tuple = ()
    if not include_past:
        sql += " WHERE sale_date >= ?"
        params = (str(date.today()),)
    sql += " ORDER BY sale_date, county"
    with _conn() as con:
        rows = con.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_all_needs_review(unreviewed_only: bool = True) -> list[dict]:
    sql = "SELECT * FROM needs_review"
    params: tuple = ()
    if unreviewed_only:
        sql += " WHERE reviewed = 0"
    sql += " ORDER BY id"
    with _conn() as con:
        rows = con.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]
