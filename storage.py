"""
storage.py — Eagle Creek Auction Monitor
Persistence layer: all reads and writes go through SQLite (db.py).
Replaces sheets_writer.py — public API is intentionally compatible so that
main.py and pipeline modules need minimal changes.

Key differences from sheets_writer.py:
  - No Google Sheets dependency
  - _row_index replaced by id (DB primary key) throughout
  - Row-alignment bugs are impossible: updates always target a stable id
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, datetime, timedelta

import db
from db import _conn, _extract_street_number
from scrapers.base import normalize_county

# ---------------------------------------------------------------------------
# Constants (re-exported so importers don't need to change)
# ---------------------------------------------------------------------------

MIN_DAYS_OUT = 3

_SKIPTRACE_QUALIFYING_SIGNALS = {"🏆", "✅", "❓"}

FILLABLE_COLUMNS = ["appraised_value", "judgment", "attorney"]
_FILLABLE_LISTING_KEYS = {
    "appraised_value":  "Appraised Value",
    "judgment":         "Judgment / Loan Amount",
    "attorney":         "Attorney / Firm",
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_DEFENDANT_PREFIXES = re.compile(
    r"^\s*(?:estate\s+of|unknown\s+heirs?\s+of|heirs?\s+of|"
    r"successors?\s+of|representatives?\s+of)\s+",
    re.IGNORECASE,
)


def _extract_defendant_last_name(defendant: str) -> str:
    if not defendant:
        return ""
    first = defendant.split(",")[0].strip()
    first = _DEFENDANT_PREFIXES.sub("", first).strip()
    if "," in first:
        last_name = first.split(",")[0].strip()
    else:
        parts = first.split()
        last_name = parts[-1] if parts else ""
    return last_name.lower()


def _parse_sale_date(s: str) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s.strip())
    except ValueError:
        return None


def _is_at_least_n_days_out(sale_date_str: str, n: int = MIN_DAYS_OUT) -> bool:
    parsed = _parse_sale_date(sale_date_str)
    return parsed is not None and parsed >= date.today() + timedelta(days=n)


# ---------------------------------------------------------------------------
# No-op stubs (replaced Google Sheets header setup)
# ---------------------------------------------------------------------------

def ensure_skiptrace_header() -> None:
    pass


def ensure_heir_research_headers() -> None:
    pass


def ensure_trustee_registry_tab() -> None:
    pass


# ---------------------------------------------------------------------------
# Phase 1 — Scrape pipeline
# ---------------------------------------------------------------------------

def write_new_listings(listings: list[dict], dry_run: bool = False) -> dict:
    db.init_db()

    # Build in-memory dedup set from DB (county, street_number, sale_date)
    with _conn() as con:
        rows = con.execute(
            "SELECT county, street_number, sale_date FROM listings"
        ).fetchall()
    existing_keys: set[tuple] = {(r["county"], r["street_number"], r["sale_date"]) for r in rows}
    seen_this_batch: set[tuple] = set()

    counts = {"added": 0, "needs_review": 0, "skipped_too_soon": 0, "skipped_duplicate": 0}

    for listing in listings:
        listing["County"] = normalize_county(listing.get("County", "").strip())

        street    = listing.get("Street", "").strip()
        sale_date = listing.get("Sale Date", "").strip()

        if not _extract_street_number(street):
            reason = f"No parseable street number in Street: '{street}'"
            print(f"  [NEEDS REVIEW - no street#] {street or '(blank)'}")
            if not dry_run:
                db.insert_needs_review(listing, reason)
            counts["needs_review"] += 1
            continue

        if not _parse_sale_date(sale_date):
            reason = f"Missing or unparseable sale date: '{sale_date}'"
            print(f"  [NEEDS REVIEW - no date]    {street} — '{sale_date}'")
            if not dry_run:
                db.insert_needs_review(listing, reason)
            counts["needs_review"] += 1
            continue

        if not _is_at_least_n_days_out(sale_date):
            print(f"  [SKIP - too soon]           {street} — {sale_date}")
            counts["skipped_too_soon"] += 1
            continue

        county     = listing["County"].strip().lower()
        street_num = _extract_street_number(street)
        dedup_key  = (county, street_num, sale_date)

        if dedup_key in existing_keys or dedup_key in seen_this_batch:
            print(f"  [SKIP - duplicate]          {street} — {sale_date}")
            counts["skipped_duplicate"] += 1
            continue

        seen_this_batch.add(dedup_key)

        if not dry_run:
            db.insert_listing(listing)

        print(f"  [ADD]                       {street} — {sale_date}")
        counts["added"] += 1

    prefix = "[DRY RUN] " if dry_run else ""
    print(
        f"\n  {prefix}Added {counts['added']} | "
        f"Needs Review {counts['needs_review']} | "
        f"Too soon {counts['skipped_too_soon']} | "
        f"Duplicates {counts['skipped_duplicate']}"
    )
    return {**counts, "dry_run": dry_run}


def get_existing_case_numbers(county: str) -> dict[str, tuple[int, bool]]:
    """Returns {case_number: (listing_id, already_cancelled)}"""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, case_number, cancelled FROM listings WHERE county=? AND case_number != ''",
            (county.lower(),),
        ).fetchall()
    return {
        r["case_number"]: (r["id"], r["cancelled"].lower() == "yes" if r["cancelled"] else False)
        for r in rows
        if r["case_number"]
    }


def get_existing_rows_by_street(county: str) -> dict[str, tuple[int, bool]]:
    """Returns {street_number: (listing_id, already_cancelled)}"""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, street_number, cancelled FROM listings WHERE county=? AND street_number != ''",
            (county.lower(),),
        ).fetchall()
    out: dict[str, tuple[int, bool]] = {}
    for r in rows:
        sn = r["street_number"]
        if sn and sn not in out:
            out[sn] = (r["id"], r["cancelled"].lower() == "yes" if r["cancelled"] else False)
    return out


def update_cancellations(cancellation_ids: dict[int, str], dry_run: bool = False) -> int:
    """cancellation_ids: {listing_id: 'Yes'}"""
    if not cancellation_ids:
        return 0
    for listing_id in cancellation_ids:
        if dry_run:
            print(f"  [DRY RUN] id={listing_id}: Cancelled → Yes")
        else:
            db.update_cancelled(listing_id, "Yes")
            print(f"  [CANCEL] id={listing_id} → Cancelled = Yes")
    return len(cancellation_ids)


def update_blank_fields(scraped_listings: list[dict], dry_run: bool = False) -> int:
    """Back-fill Appraised Value, Judgment, Attorney when blank in DB."""
    if not scraped_listings:
        return 0

    updated = 0
    for listing in scraped_listings:
        county     = listing.get("County", "").strip().lower()
        street_num = _extract_street_number(listing.get("Street", "").strip())
        sale_date  = listing.get("Sale Date", "").strip()
        if not county or not street_num:
            continue

        with _conn() as con:
            row = con.execute(
                "SELECT id, appraised_value, judgment, attorney FROM listings "
                "WHERE county=? AND street_number=? AND sale_date=?",
                (county, street_num, sale_date),
            ).fetchone()
            if not row:
                continue

            fields_to_set = {}
            for col, listing_key in _FILLABLE_LISTING_KEYS.items():
                current = (row[col] or "").strip()
                new_val = str(listing.get(listing_key, "")).strip()
                if not current and new_val:
                    fields_to_set[col] = new_val

            if not fields_to_set:
                continue

            if dry_run:
                print(f"  [DryRun] Would back-fill {list(fields_to_set.keys())} for id={row['id']}")
                updated += len(fields_to_set)
                continue

            set_clause = ", ".join(f"{k}=?" for k in fields_to_set)
            con.execute(
                f"UPDATE listings SET {set_clause} WHERE id=?",
                (*fields_to_set.values(), row["id"]),
            )
            updated += len(fields_to_set)

    return updated


# ---------------------------------------------------------------------------
# Phase 2 — Valuation pipeline
# ---------------------------------------------------------------------------

def get_listings_needing_valuation(county_filter: list[str] | None = None) -> list[dict]:
    import config as _cfg
    tn_whitelist = {c.lower() for c in (_cfg.TN_VALUATE_COUNTIES or [])}

    listings = db.get_listings_needing_valuation()

    if county_filter:
        cf_lower = {c.lower() for c in county_filter}
        listings = [l for l in listings if l["county"].lower() in cf_lower]

    # TN county whitelist
    filtered = []
    for l in listings:
        if l.get("state", "").upper() == "TN" and tn_whitelist:
            if l["county"].lower() not in tn_whitelist:
                continue
        filtered.append(l)

    # Normalize dict keys to match what valuation.py expects
    return [_db_to_listing(l) for l in filtered]


def update_valuations(valuations: list[dict], dry_run: bool = False) -> int:
    updated = 0
    for v in valuations:
        listing_id = v["id"]
        emv    = v.get("Estimated Market Value", "")
        equity = v.get("Estimated Equity", "")
        signal = v.get("Equity Signal", "")
        notes  = v.get("Notes", "")

        if dry_run:
            print(f"  [DRY RUN] id={listing_id}: EMV={emv}  Equity={equity}  Signal={signal}")
            updated += 1
            continue

        db.update_valuation(listing_id, emv, equity, signal)
        if notes:
            with _conn() as con:
                con.execute("UPDATE listings SET notes=? WHERE id=?", (notes, listing_id))
        print(f"  [VALUATE] id={listing_id} — {signal} {emv}")
        updated += 1

    return updated


# ---------------------------------------------------------------------------
# Phase 3 — Skip trace pipeline
# ---------------------------------------------------------------------------

def get_listings_needing_skiptrace() -> list[dict]:
    return [_db_to_listing(l) for l in db.get_listings_needing_skiptrace()]


def update_skiptraces(results: list[dict], dry_run: bool = False) -> int:
    if dry_run:
        count = sum(1 for r in results if not r.get("_skipped"))
        print(f"  [DRY RUN] Would update {count} row(s) with skip trace data.")
        return count

    written = 0
    for result in results:
        if result.get("_skipped"):
            continue
        db.update_skiptrace(result["id"], result)
        written += 1

    return written


# ---------------------------------------------------------------------------
# Phase 4 — Heir research pipeline
# ---------------------------------------------------------------------------

def get_listings_needing_heir_research() -> list[dict]:
    return [_db_to_listing(l) for l in db.get_listings_needing_heir_research()]


def update_heir_research(results: list[dict], dry_run: bool = False) -> int:
    if dry_run:
        count = sum(1 for r in results if not r.get("_skipped"))
        print(f"  [DRY RUN] Would update {count} row(s) with heir research data.")
        return count

    written = 0
    for result in results:
        if result.get("_skipped"):
            continue
        db.update_heir_research(result["id"], result)
        written += 1

    return written


def write_heir_leads(results: list[dict], dry_run: bool = False) -> int:
    # Build existing dedup set from DB
    with _conn() as con:
        existing_rows = con.execute(
            "SELECT property_street, sale_date, heir_name FROM heir_leads"
        ).fetchall()
    existing_keys: set[tuple] = {
        (_extract_street_number(r["property_street"]), r["sale_date"], r["heir_name"].lower())
        for r in existing_rows
        if r["heir_name"]
    }

    added = 0
    for result in results:
        if result.get("_skipped"):
            continue
        if result.get("Obit Found") != "Yes":
            continue
        heirs = result.get("_heirs_list", [])
        if not heirs:
            continue

        street     = result.get("Street", "")
        city       = result.get("City", "")
        county     = result.get("County", "")
        state      = result.get("State", "")
        sale_date  = result.get("Sale Date", "")
        signal     = result.get("Equity Signal", "")
        owner_name = result.get("Owner Name (Primary)", "")
        def_match  = result.get("Defendant Match", "No")
        listing_id = result.get("id")
        street_num = _extract_street_number(street)

        for heir in heirs:
            heir_name = heir.get("name", "").strip()
            heir_rel  = heir.get("relationship", "").strip()
            if not heir_name:
                continue

            dedup_key = (street_num, sale_date, heir_name.lower())
            if dedup_key in existing_keys:
                continue

            heir_match = "Yes" if heir_name.lower() in def_match.lower() else "No"
            lead = {
                "Property Street": street,
                "Property City":   city,
                "County":          county,
                "State":           state,
                "Sale Date":       sale_date,
                "Equity Signal":   signal,
                "Deceased Owner":  owner_name,
                "Heir Name":       heir_name,
                "Relationship":    heir_rel,
                "Defendant Match": heir_match,
                "Status":          "New",
            }

            if dry_run:
                print(f"  [DRY RUN] Would add heir lead: {heir_name} @ {street}")
                added += 1
                existing_keys.add(dedup_key)
                continue

            db.insert_heir_lead(listing_id, lead)
            added += 1
            existing_keys.add(dedup_key)

    if dry_run:
        print(f"  [DRY RUN] Would add {added} row(s) to heir_leads.")
    return added


# ---------------------------------------------------------------------------
# Phase 4b — Heir skip trace pipeline
# ---------------------------------------------------------------------------

def get_heirs_needing_skiptrace() -> list[dict]:
    leads = db.get_heir_leads_needing_skiptrace()
    return [
        {
            "row_index": l["id"],  # used as stable ID by run_heir_skiptrace
            "heir_name": l["heir_name"],
            "street":    l["property_street"],
            "city":      l["property_city"],
            "state":     l["state"],
        }
        for l in leads
    ]


def update_heir_skiptraces(results: list[dict], dry_run: bool = False) -> int:
    if not results:
        return 0

    if dry_run:
        for r in results:
            phones_preview  = r["phones"][:35]  if r["phones"]  else "(none)"
            mailing_preview = r["mailing"][:40] if r["mailing"] else "(none)"
            print(
                f"  [dry-run] id={r['row_index']:>4}: "
                f"phones={phones_preview!r}  mailing={mailing_preview!r}"
            )
        return len(results)

    for r in results:
        lead_result = {
            "Phone(s)":         r["phones"],
            "Email(s)":         r["emails"],
            "Mailing Address":  r["mailing"],
            "Skip Traced Date": r["date"],
        }
        db.update_heir_lead_skiptrace(r["row_index"], lead_result)

    return len(results)


def dedup_heir_phones(dry_run: bool = False) -> int:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, property_street, property_city, phones, status "
            "FROM heir_leads "
            "WHERE skip_traced_date != '' AND skip_traced_date IS NOT NULL "
            "  AND (status IS NULL OR status != 'Dup Phone') "
            "ORDER BY id"
        ).fetchall()

    if not rows:
        return 0

    property_groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        key = (r["property_street"].lower(), r["property_city"].lower())
        property_groups[key].append({"id": r["id"], "phones": r["phones"] or ""})

    total_modified = 0
    updates: list[tuple] = []  # (sql, params)

    for group in property_groups.values():
        seen_phones: set[str] = set()

        for row_info in group:
            phones_str = row_info["phones"]
            if not phones_str:
                continue

            phone_list = [p.strip() for p in phones_str.split(",") if p.strip()]
            unique = [p for p in phone_list if p not in seen_phones]
            dups   = [p for p in phone_list if p in seen_phones]

            if not dups:
                seen_phones.update(phone_list)
                continue

            seen_phones.update(unique)

            if unique:
                new_str = ", ".join(unique)
                if dry_run:
                    print(f"  [dry-run] id={row_info['id']}: removed dup phone(s) {dups} → {new_str!r}")
                else:
                    updates.append(("UPDATE heir_leads SET phones=? WHERE id=?", (new_str, row_info["id"])))
                total_modified += 1
            else:
                if dry_run:
                    print(f"  [dry-run] id={row_info['id']}: all phones already covered → cleared, Status=Dup Phone")
                else:
                    updates.append(("UPDATE heir_leads SET phones='', status='Dup Phone' WHERE id=?", (row_info["id"],)))
                total_modified += 1

    if updates and not dry_run:
        with _conn() as con:
            for sql, params in updates:
                con.execute(sql, params)

    return total_modified


# ---------------------------------------------------------------------------
# TN active rows — dedup and check mode
# ---------------------------------------------------------------------------

def get_tn_existing_set() -> set[tuple]:
    """Returns {(county_lower, street_number, sale_date)} for all active TN listings."""
    with _conn() as con:
        rows = con.execute(
            "SELECT county, street_number, sale_date FROM listings "
            "WHERE UPPER(state)='TN' AND (cancelled IS NULL OR LOWER(cancelled)!='yes') "
            "  AND street_number != '' AND sale_date != ''"
        ).fetchall()
    return {(r["county"], r["street_number"], r["sale_date"]) for r in rows}


def get_tn_listings_for_check() -> list[dict]:
    """Return active (non-cancelled, sale_date >= today) TN listings for --tncheck."""
    today_str = date.today().isoformat()
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM listings "
            "WHERE UPPER(state)='TN' "
            "  AND (cancelled IS NULL OR LOWER(cancelled)!='yes') "
            "  AND sale_date >= ? "
            "ORDER BY sale_date",
            (today_str,),
        ).fetchall()
    return [
        {
            "row_index":      r["id"],   # stable ID — used by update_tn_postponements
            "id":             r["id"],
            "County":         r["county"],
            "State":          r["state"],
            "Sale Date":      r["sale_date"],
            "Case Number":    r["case_number"] or "",
            "Plaintiff":      r["plaintiff"] or "",
            "Defendant(s)":   r["defendants"] or "",
            "Street":         r["street"] or "",
            "City":           r["city"] or "",
            "Zip":            r["zip"] or "",
            "Attorney / Firm": r["attorney"] or "",
            "Notes":          r["notes"] or "",
        }
        for r in rows
    ]


def update_tn_postponements(updates: list[dict], dry_run: bool = False) -> int:
    """Each update: {row_index (=listing id), old_date, new_date}"""
    if not updates:
        return 0

    today_str = date.today().strftime("%Y-%m-%d")

    if dry_run:
        for u in updates:
            print(f"  [DRY RUN] id={u['row_index']}: Sale Date {u['old_date']} → {u['new_date']}")
        return len(updates)

    with _conn() as con:
        for u in updates:
            listing_id = u["row_index"]
            old_date   = u["old_date"]
            new_date   = u["new_date"]
            note       = f"Postponed from {old_date} to {new_date} — checked {today_str}"

            row = con.execute("SELECT notes FROM listings WHERE id=?", (listing_id,)).fetchone()
            existing_notes = (row["notes"] or "") if row else ""
            new_notes = f"{existing_notes} | {note}".lstrip(" |") if existing_notes else note

            con.execute(
                "UPDATE listings SET sale_date=?, notes=? WHERE id=?",
                (new_date, new_notes, listing_id),
            )

    return len(updates)


def flag_tn_for_manual_check(flags: list[dict], dry_run: bool = False) -> int:
    """Each flag: {row_index (=listing id), reason}"""
    if not flags:
        return 0

    today_str = date.today().strftime("%Y-%m-%d")

    if dry_run:
        for f in flags:
            print(f"  [DRY RUN] id={f['row_index']}: ⚠️ Manual check — {f['reason']}")
        return len(flags)

    with _conn() as con:
        for f in flags:
            listing_id = f["row_index"]
            note = f"⚠️ Manual check ({today_str}): {f['reason']}"

            row = con.execute("SELECT notes FROM listings WHERE id=?", (listing_id,)).fetchone()
            existing_notes = (row["notes"] or "") if row else ""
            new_notes = f"{existing_notes} | {note}".lstrip(" |") if existing_notes else note

            con.execute("UPDATE listings SET notes=? WHERE id=?", (new_notes, listing_id))

    return len(flags)


def write_unknown_trustees(unknown_dict: dict[str, str], dry_run: bool = False) -> int:
    """Log unknown trustee firms — no DB table for these yet, just print."""
    if not unknown_dict:
        return 0
    for name, source_url in unknown_dict.items():
        if dry_run:
            print(f"  [DRY RUN] Unknown trustee: {name}  ({source_url})")
        else:
            print(f"  [TRUSTEE] Unknown firm logged (no DB table yet): {name}")
    return len(unknown_dict)


# ---------------------------------------------------------------------------
# Internal: DB row → pipeline-friendly dict
# ---------------------------------------------------------------------------

def _db_to_listing(row: dict) -> dict:
    """
    Convert a SQLite row dict (snake_case keys) to the camelCase dict format
    that valuation.py / skiptrace.py / heir_research.py expect.
    The `id` key is preserved for use in update functions.
    """
    return {
        "id":                     row["id"],
        "County":                 row.get("county", ""),
        "State":                  row.get("state", ""),
        "Sale Date":              row.get("sale_date", ""),
        "Case Number":            row.get("case_number", "") or "",
        "Plaintiff":              row.get("plaintiff", "") or "",
        "Defendant(s)":           row.get("defendants", "") or "",
        "Street":                 row.get("street", "") or "",
        "City":                   row.get("city", "") or "",
        "Zip":                    row.get("zip", "") or "",
        "Appraised Value":        row.get("appraised_value", "") or "",
        "Judgment / Loan Amount": row.get("judgment", "") or "",
        "Attorney / Firm":        row.get("attorney", "") or "",
        "Estimated Market Value": row.get("est_market_value", "") or "",
        "Estimated Equity":       row.get("est_equity", "") or "",
        "Equity Signal":          row.get("equity_signal", "") or "",
        "Cancelled":              row.get("cancelled", "") or "",
        "Source URL":             row.get("source_url", "") or "",
        "Date Added":             row.get("date_added", "") or "",
        "Notes":                  row.get("notes", "") or "",
        "Owner Name (Primary)":   row.get("owner_primary", "") or "",
        "Owner Name (Secondary)": row.get("owner_secondary", "") or "",
        "Owner Phone(s)":         row.get("owner_phones", "") or "",
        "Owner Email(s)":         row.get("owner_emails", "") or "",
        "Mailing Address":        row.get("mailing_address", "") or "",
        "Deceased":               row.get("deceased", "") or "",
        "Skip Trace Date":        row.get("skiptrace_date", "") or "",
    }
