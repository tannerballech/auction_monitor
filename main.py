"""
main.py — Eagle Creek Auction Monitor

Usage:
    python main.py                              # Pull all sources, write new listings
    python main.py --county kenton boone        # Run specific counties only
    python main.py --dry-run                    # Print results without writing to DB
    python main.py --email-only                 # Only check Gmail sources
    python main.py --web-only                   # Only check web sources
    python main.py --valuate                    # Run market value analysis
    python main.py --valuate --dry-run          # Preview valuations without saving
    python main.py --valuate --county kenton    # Valuate only Kenton listings
    python main.py --skiptrace                  # Skip trace qualifying listings (✅ or 🏆)
    python main.py --skiptrace --dry-run        # Preview which listings would be traced
    python main.py --heirresearch               # Research obits for deceased owners
    python main.py --heirresearch --dry-run     # Preview which listings would be researched
    python main.py --heirskiptrace              # Skip trace heirs in Heir Leads tab (Tracerfy)
    python main.py --heirskiptrace --dry-run    # Preview which heirs would be traced
"""

from __future__ import annotations
import argparse
import traceback
import time
from datetime import date, datetime


# ── Scrapers ──────────────────────────────────────────────────────────────────
from scrapers.kenton import scrape as scrape_kenton
from scrapers.boone import scrape as scrape_boone
from scrapers.fayette_ky import scrape as scrape_fayette
from scrapers.jefferson_ky import scrape as scrape_jefferson_ky
from scrapers.campbell_ky import scrape as scrape_campbell_ky
from scrapers.knox_tn import scrape_knox_tn
from scrapers.hamilton_oh import scrape_hamilton_oh
from scrapers.madison_ky import scrape_madison_ky
from scrapers.franklin_ky import scrape_franklin_ky
from scrapers.jessamine_ky import scrape_jessamine_ky
from scrapers.floyd_in import scrape_floyd_in
from scrapers.clark_in import scrape_clark_in
from scrapers.tn_trustees import internetpostings as _ip_scraper
from scrapers.tn_trustees import mackie_wolf as _mw_scraper
from scrapers.tn_trustees import robertson_anschutz as _rasc_scraper
from scrapers.tn_trustees import brock_scott as _bs_scraper
from scrapers.tn_trustees import capital_city_postings as _ccp_scraper
from scrapers.tn_trustees import mickel_law as _mickel_scraper
from scrapers.tn_trustees import nw_posting_services as _nwps_scraper
from scrapers.tn_trustees import clear_recon as _cr_scraper
from scrapers.tn_trustees import phillip_jones as _pj_scraper
from scrapers.tn_trustees import anchor_posting as _ap_scraper
from scrapers.tn_trustees import foreclosure_postings as _fp_scraper
from scrapers.tn_trustees import better_choice_notices as _bcn_scraper
from gmail_reader import scrape_emails

# ── Storage ───────────────────────────────────────────────────────────────────
from storage import (
    write_new_listings,
    update_blank_fields,
    get_listings_needing_valuation,
    update_valuations,
    get_existing_case_numbers,
    get_existing_rows_by_street,
    update_cancellations,
    get_listings_needing_skiptrace,
    update_skiptraces,
    ensure_skiptrace_header,
    get_listings_needing_heir_research,
    update_heir_research,
    write_heir_leads,
    ensure_heir_research_headers,
    get_heirs_needing_skiptrace,
    update_heir_skiptraces,
    dedup_heir_phones,
)

# ── Pipeline modules ──────────────────────────────────────────────────────────
from valuation import run_valuations
from skiptrace import run_skiptraces
from heir_research import run_heir_research
from heir_skiptrace import skip_trace_heir

from sheets_sync import sync_to_sheets
from ingest_directskip import ingest as ingest_directskip
from directskip_export import generate as generate_directskip_csv
from directskip_upload import run as run_directskip_upload
from phoneburner_export import generate as generate_phoneburner
from phoneburner_push import push as push_phoneburner
from propai_export import generate as generate_propai
from propai_push import push as push_propai
from propai_sync import sync as sync_propai
from scrapers.tn_trustees import rubin_lublin as _rl_scraper
from scrapers.tn_trustees.registry import lookup_trustee, TRUSTEE_REGISTRY
from storage import (
    get_tn_existing_set,
    get_tn_listings_for_check,
    update_tn_postponements,
    flag_tn_for_manual_check,
    ensure_trustee_registry_tab,
    write_unknown_trustees,
)

# ── Scraper registry (simple return-list scrapers — no cancellation tracking) ─
# Note: Kenton and Campbell have been moved to special-case blocks below
# because they return (new_listings, cancellation_updates) tuples.
WEB_SCRAPERS = {
    "boone": scrape_boone,
}


# ── Scrape ────────────────────────────────────────────────────────────────────

def run_scrape(
    counties: list[str] | None = None,
    dry_run: bool = False,
    email_only: bool = False,
    web_only: bool = False,
):
    print("=" * 60)
    print(f"  Eagle Creek Auction Monitor — Scrape")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    all_listings: list[dict] = []
    all_cancellations: dict[int, str] = {}

    # ── Kenton KY ─────────────────────────────────────────────────────────────
    if not email_only and (not counties or any("kenton" in c.lower() for c in counties)):
        print(f"\n[KENTON] Scraping...")
        try:
            existing_kenton = get_existing_case_numbers("Kenton") if not dry_run else {}
            new, cancellations = scrape_kenton(existing=existing_kenton)
            print(f"  Found {len(new)} new listing(s), {len(cancellations)} cancellation(s).")
            all_listings.extend(new)
            all_cancellations.update(cancellations)
        except Exception as e:
            print(f"  [kenton] ERROR: {e}")
            traceback.print_exc()

    # ── Jefferson KY ──────────────────────────────────────────────────────────
    if not email_only and (not counties or any("jefferson" in c.lower() for c in counties)):
        if dry_run:
            print(f"\n[JEFFERSON_KY] Skipping — dry run mode.")
        else:
            print(f"\n[JEFFERSON_KY] Scraping...")
            try:
                existing = get_existing_case_numbers("Jefferson")
                new, cancellations = scrape_jefferson_ky(existing=existing)
                print(f"  Found {len(new)} new listing(s), {len(cancellations)} cancellation(s).")
                all_listings.extend(new)
                all_cancellations.update(cancellations)
            except Exception as e:
                print(f"  [jefferson_ky] ERROR: {e}")
                traceback.print_exc()

    # ── Fayette KY ────────────────────────────────────────────────────────────
    if not email_only and (not counties or any("fayette" in c.lower() for c in counties)):
        if dry_run:
            print(f"\n[FAYETTE] Skipping — dry run mode.")
        else:
            print(f"\n[FAYETTE] Scraping...")
            try:
                existing = get_existing_case_numbers("Fayette")
                new, cancellations = scrape_fayette(existing=existing)
                print(f"  Found {len(new)} new listing(s), {len(cancellations)} cancellation(s).")
                all_listings.extend(new)
                all_cancellations.update(cancellations)
            except Exception as e:
                print(f"  [fayette] ERROR: {e}")
                traceback.print_exc()

    # ── Campbell KY ───────────────────────────────────────────────────────────
    if not email_only and (not counties or any("campbell" in c.lower() for c in counties)):
        print(f"\n[CAMPBELL] Scraping...")
        try:
            existing_campbell = get_existing_rows_by_street("Campbell") if not dry_run else {}
            new, cancellations = scrape_campbell_ky(existing=existing_campbell)
            print(f"  Found {len(new)} new listing(s), {len(cancellations)} cancellation(s).")
            all_listings.extend(new)
            all_cancellations.update(cancellations)
        except Exception as e:
            print(f"  [campbell] ERROR: {e}")
            traceback.print_exc()

    # ── Clark IN ──────────────────────────────────────────────────────────────
    if not email_only and (not counties or any("clark" in c.lower() for c in counties)):
        print(f"\n[CLARK IN] Scraping...")
        try:
            existing_clark = get_existing_rows_by_street("Clark") if not dry_run else {}
            new, cancellations = scrape_clark_in(existing=existing_clark, dry_run=dry_run)
            print(f"  Found {len(new)} new listing(s).")
            all_listings.extend(new)
            all_cancellations.update(cancellations)
        except Exception as e:
            print(f"  [clark_in] ERROR: {e}")
            traceback.print_exc()
    # ── Floyd IN ──────────────────────────────────────────────────────────────
    if not email_only and (not counties or any("floyd" in c.lower() for c in counties)):
        print(f"\n[FLOYD IN] Scraping...")
        try:
            new, cancellations = scrape_floyd_in(dry_run=dry_run)
            print(f"  Found {len(new)} new listing(s).")
            all_listings.extend(new)
            all_cancellations.update(cancellations)
        except Exception as e:
            print(f"  [floyd_in] ERROR: {e}")
            traceback.print_exc()

    # ── Hamilton OH ───────────────────────────────────────────────────────────
    if not email_only and (not counties or any("hamilton" in c.lower() for c in counties)):
        print(f"\n[HAMILTON] Scraping...")
        try:
            existing = get_existing_case_numbers("Hamilton") if not dry_run else {}
            new, cancellations = scrape_hamilton_oh(existing=existing)
            print(f"  Found {len(new)} new listing(s), {len(cancellations)} cancellation(s).")
            all_listings.extend(new)
            all_cancellations.update(cancellations)
        except Exception as e:
            print(f"  [hamilton] ERROR: {e}")
            traceback.print_exc()

    # ── Madison KY ────────────────────────────────────────────────────────────
    if not email_only and (not counties or any("madison" in c.lower() for c in counties)):
        print(f"\n[MADISON] Scraping...")
        try:
            existing = get_existing_case_numbers("Madison") if not dry_run else {}
            new, cancellations = scrape_madison_ky(existing=existing, dry_run=dry_run)
            print(f"  Found {len(new)} new listing(s), {len(cancellations)} cancellation(s).")
            all_listings.extend(new)
            all_cancellations.update(cancellations)
        except Exception as e:
            print(f"  [madison] ERROR: {e}")
            traceback.print_exc()

    # ── Franklin KY ───────────────────────────────────────────────────────────
    if not email_only and (not counties or any("franklin" in c.lower() for c in counties)):
        print(f"\n[FRANKLIN] Scraping...")
        try:
            existing = get_existing_case_numbers("Franklin") if not dry_run else {}
            new, cancellations = scrape_franklin_ky(existing=existing, dry_run=dry_run)
            print(f"  Found {len(new)} new listing(s), {len(cancellations)} cancellation(s).")
            all_listings.extend(new)
            all_cancellations.update(cancellations)
        except Exception as e:
            print(f"  [franklin] ERROR: {e}")
            traceback.print_exc()

    # ── Jessamine KY ──────────────────────────────────────────────────────────
    if not email_only and (not counties or any("jessamine" in c.lower() for c in counties)):
        print(f"\n[JESSAMINE] Scraping...")
        try:
            existing = get_existing_case_numbers("Jessamine") if not dry_run else {}
            new, cancellations = scrape_jessamine_ky(existing=existing, dry_run=dry_run)
            print(f"  Found {len(new)} new listing(s), {len(cancellations)} cancellation(s).")
            all_listings.extend(new)
            all_cancellations.update(cancellations)
        except Exception as e:
            print(f"  [jessamine] ERROR: {e}")
            traceback.print_exc()

    # ── Knox TN ───────────────────────────────────────────────────────────────
    if not email_only and (not counties or any("knox" in c.lower() for c in counties)):
        print(f"\n[KNOX] Scraping...")
        try:
            _TNLEDGER_COUNTIES = [
                "Knox", "Sevier", "Blount", "Cumberland",
                "Hamblen", "Anderson", "Roane", "Jefferson", "Cocke", "Loudon",
            ]
            existing_tnledger: set[str] = set()
            for c in _TNLEDGER_COUNTIES:
                existing_tnledger.update(get_existing_case_numbers(c).keys())
            new = scrape_knox_tn(existing=existing_tnledger, dry_run=dry_run)
            print(f"  Found {len(new)} new listing(s).")
            all_listings.extend(new)
        except Exception as e:
            print(f"  [knox] ERROR: {e}")
            traceback.print_exc()
        _TN_COUNTIES_LOWER = {c.lower() for c in _TNLEDGER_COUNTIES}  # already defined above TNLedger block
        if not email_only and (not counties or any(c.lower() in _TN_COUNTIES_LOWER for c in counties)):
            print("\n[RUBIN LUBLIN] Scraping (discovery mode)...")
            try:
                existing_addr_set = get_tn_existing_set() if not dry_run else set()
                rl_listings, _ = _rl_scraper.scrape_rubin_lublin(
                    existing_addr_set=existing_addr_set,
                    dry_run=dry_run,
                )
                print(f"  Found {len(rl_listings)} new listing(s).")
                all_listings.extend(rl_listings)
            except Exception as e:
                print(f"  [rubin_lublin] ERROR: {e}")
                traceback.print_exc()

    # ── internetpostings.com (discovery) ─────────────────────────────────────────
    if not email_only and (not counties or any(c.lower() in _TN_COUNTIES_LOWER for c in counties)):
        print("\n[INTERNETPOSTINGS] Scraping (discovery mode)...")
        try:
            # Reuse existing_addr_set if already computed for Rubin Lublin this run,
            # otherwise compute it now.  The set covers ALL active TN rows so both
            # scrapers share the same dedup gate.
            if "existing_addr_set" not in dir():
                existing_addr_set = get_tn_existing_set() if not dry_run else set()
            ip_listings, _ = _ip_scraper.scrape_internetpostings(
                existing_addr_set=existing_addr_set,
                dry_run=dry_run,
            )
            print(f"  Found {len(ip_listings)} new listing(s).")
            all_listings.extend(ip_listings)
        except Exception as e:
            print(f"  [internetpostings] ERROR: {e}")
            traceback.print_exc()

    # ── Mackie Wolf (discovery) ──────────────────────────────────────────────
    if not email_only and (not counties or any(c.lower() in _TN_COUNTIES_LOWER for c in counties)):
        print("\n[MACKIE WOLF] Scraping (discovery mode)...")
        try:
            if "existing_addr_set" not in dir():
                existing_addr_set = get_tn_existing_set() if not dry_run else set()
            mw_listings, _ = _mw_scraper.scrape_mackie_wolf(
                existing_addr_set=existing_addr_set,
                dry_run=dry_run,
            )
            print(f"  Found {len(mw_listings)} new listing(s).")
            all_listings.extend(mw_listings)
        except Exception as e:
            print(f"  [mackie_wolf] ERROR: {e}")
            traceback.print_exc()

    # ── Robertson Anschutz (discovery) ───────────────────────────────────────
    if not email_only and (not counties or any(c.lower() in _TN_COUNTIES_LOWER for c in counties)):
        print("\n[ROBERTSON ANSCHUTZ] Scraping (discovery mode)...")
        try:
            if "existing_addr_set" not in dir():
                existing_addr_set = get_tn_existing_set() if not dry_run else set()
            rasc_listings, _ = _rasc_scraper.scrape_robertson_anschutz(
                existing_addr_set=existing_addr_set,
                dry_run=dry_run,
            )
            print(f"  Found {len(rasc_listings)} new listing(s).")
            all_listings.extend(rasc_listings)
        except Exception as e:
            print(f"  [robertson_anschutz] ERROR: {e}")
            traceback.print_exc()

    # ── Brock & Scott (discovery) ─────────────────────────────────────────────
    if not email_only and (not counties or any(c.lower() in _TN_COUNTIES_LOWER for c in counties)):
        print("\n[BROCK & SCOTT] Scraping (discovery mode)...")
        try:
            if "existing_addr_set" not in dir():
                existing_addr_set = get_tn_existing_set() if not dry_run else set()
            bs_listings, _ = _bs_scraper.scrape_brock_scott(
                existing_addr_set=existing_addr_set,
                dry_run=dry_run,
            )
            print(f"  Found {len(bs_listings)} new listing(s).")
            all_listings.extend(bs_listings)
        except Exception as e:
            print(f"  [brock_scott] ERROR: {e}")
            traceback.print_exc()

    # ── Capital City Postings / Padgett (discovery) ───────────────────────────
    if not email_only and (not counties or any(c.lower() in _TN_COUNTIES_LOWER for c in counties)):
        print("\n[PADGETT] Scraping (discovery mode)...")
        try:
            if "existing_addr_set" not in dir():
                existing_addr_set = get_tn_existing_set() if not dry_run else set()
            plg_listings, _ = _ccp_scraper.scrape_padgett(
                existing_addr_set=existing_addr_set,
                dry_run=dry_run,
            )
            print(f"  Found {len(plg_listings)} new listing(s).")
            all_listings.extend(plg_listings)
        except Exception as e:
            print(f"  [capital_city_postings] ERROR: {e}")
            traceback.print_exc()

    # ── Mickel Law Firm (discovery) ───────────────────────────────────────────
    if not email_only and (not counties or any(c.lower() in _TN_COUNTIES_LOWER for c in counties)):
        print("\n[MICKEL] Scraping (discovery mode)...")
        try:
            if "existing_addr_set" not in dir():
                existing_addr_set = get_tn_existing_set() if not dry_run else set()
            mickel_listings, _ = _mickel_scraper.scrape_mickel(
                existing_addr_set=existing_addr_set,
                dry_run=dry_run,
            )
            print(f"  Found {len(mickel_listings)} new listing(s).")
            all_listings.extend(mickel_listings)
        except Exception as e:
            print(f"  [mickel_law] ERROR: {e}")
            traceback.print_exc()

    # ── [NW POSTING SERVICES] — Marinosci + ALAW ──────────────────────────────
    print("\n[NW POSTING SERVICES] Fetching TN listings (Marinosci / ALAW)...")
    if "existing_addr_set" not in dir():
        existing_addr_set = get_tn_existing_set()
    try:
        nwps_listings, _ = _nwps_scraper.scrape_nw_posting_services(
            existing_addr_set, dry_run=args.dry_run
        )
        if nwps_listings:
            print(f"  {len(nwps_listings)} new listing(s) found.")
            all_listings.extend(nwps_listings)
        else:
            print("  No new listings.")
    except Exception as e:
        print(f"  [NW POSTING SERVICES] ERROR: {e}")
        traceback.print_exc()

    # ── [CLEAR RECON] ──────────────────────────────────────────────────────────
    print("\n[CLEAR RECON] Fetching TN listings...")
    if "existing_addr_set" not in dir():
        existing_addr_set = get_tn_existing_set()
    try:
        cr_listings, _ = _cr_scraper.scrape_clear_recon(
            existing_addr_set, dry_run=args.dry_run
        )
        if cr_listings:
            print(f"  {len(cr_listings)} new listing(s) found.")
            all_listings.extend(cr_listings)
        else:
            print("  No new listings.")
    except Exception as e:
        print(f"  [CLEAR RECON] ERROR: {e}")
        traceback.print_exc()

    # ── [PHILLIP JONES] ────────────────────────────────────────────────────────
    print("\n[PHILLIP JONES] Fetching TN listings...")
    if "existing_addr_set" not in dir():
        existing_addr_set = get_tn_existing_set()
    try:
        pj_listings, _ = _pj_scraper.scrape_phillip_jones(
            existing_addr_set, dry_run=args.dry_run
        )
        if pj_listings:
            print(f"  {len(pj_listings)} new listing(s) found.")
            all_listings.extend(pj_listings)
        else:
            print("  No new listings.")
    except Exception as e:
        print(f"  [PHILLIP JONES] ERROR: {e}")
        traceback.print_exc()

    print("\n[ANCHOR POSTING] Fetching TN listings (McMichael Taylor Gray)...")
    if "existing_addr_set" not in dir():
        existing_addr_set = get_tn_existing_set()
    try:
        ap_listings, _ = _ap_scraper.scrape_anchor_posting(
            existing_addr_set, dry_run=args.dry_run
        )
        if ap_listings:
            print(f"  {len(ap_listings)} new listing(s) found.")
            all_listings.extend(ap_listings)
        else:
            print("  No new listings.")
    except Exception as e:
        print(f"  [ANCHOR POSTING] ERROR: {e}")
        traceback.print_exc()

    print("\n[FORECLOSURE POSTINGS] Fetching TN listings (Vylla / Arnold M. Weiss)...")
    if "existing_addr_set" not in dir():
        existing_addr_set = get_tn_existing_set()
    try:
        fp_listings, _ = _fp_scraper.scrape_foreclosure_postings(
            existing_addr_set, dry_run=args.dry_run
        )
        if fp_listings:
            print(f"  {len(fp_listings)} new listing(s) found.")
            all_listings.extend(fp_listings)
        else:
            print("  No new listings.")
    except Exception as e:
        print(f"  [FORECLOSURE POSTINGS] ERROR: {e}")
        traceback.print_exc()

    # ── [BETTER CHOICE NOTICES] — LLG Trustee ─────────────────────────────────
    print("\n[BETTER CHOICE NOTICES] Fetching TN listings (LLG Trustee)...")
    if "existing_addr_set" not in dir():
        existing_addr_set = get_tn_existing_set()
    try:
        bcn_listings, _ = _bcn_scraper.scrape_better_choice_notices(
            existing_addr_set, dry_run=args.dry_run
        )
        if bcn_listings:
            print(f"  {len(bcn_listings)} new listing(s) found.")
            all_listings.extend(bcn_listings)
        else:
            print("  No new listings.")
    except Exception as e:
        print(f"  [BETTER CHOICE NOTICES] ERROR: {e}")
        traceback.print_exc()

    # ── Simple web scrapers (Boone only) ──────────────────────────────────────
    if not email_only:
        scrapers_to_run = WEB_SCRAPERS
        if counties:
            scrapers_to_run = {
                k: v for k, v in WEB_SCRAPERS.items()
                if any(c.lower() in k for c in counties)
            }
        for name, scraper_fn in scrapers_to_run.items():
            print(f"\n[{name.upper()}] Scraping...")
            try:
                results = scraper_fn()
                print(f"  Found {len(results)} listing(s).")
                all_listings.extend(results)
            except Exception as e:
                print(f"  [{name}] ERROR: {e}")
                traceback.print_exc()

    # ── Gmail ─────────────────────────────────────────────────────────────────
    _GMAIL_COUNTIES = {"scott", "rowan"}
    if not web_only and (not counties or any(c.lower() in _GMAIL_COUNTIES for c in counties)):
        print(f"\n[GMAIL] Checking email sources...")
        try:
            email_listings = scrape_emails()
            print(f"  Found {len(email_listings)} listing(s).")
            all_listings.extend(email_listings)
        except Exception as e:
            print(f"  [Gmail] ERROR: {e}")
            traceback.print_exc()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  Total listings found: {len(all_listings)}")
    from collections import Counter
    for county, count in sorted(Counter(
        f"{l.get('County', '?')}, {l.get('State', '?')}" for l in all_listings
    ).items()):
        print(f"    {county}: {count}")

    # ── Write to Sheets ───────────────────────────────────────────────────────
    if dry_run:
        print(f"\n  [DRY RUN] Not writing to DB.")
        _print_sample(all_listings)
    else:
        print(f"\n[DB] Writing new listings...")
        try:
            result = write_new_listings(all_listings)
            print(f"  Added: {result['added']} | Review: {result['needs_review']} | "
                  f"Too soon: {result['skipped_too_soon']} | Dupes: {result['skipped_duplicate']}")
        except Exception as e:
            print(f"  [DB] ERROR: {e}")
            traceback.print_exc()

        try:
            filled = update_blank_fields(all_listings)
            if filled:
                print(f"  Back-filled {filled} blank field(s).")
        except Exception as e:
            print(f"  [DB] ERROR back-filling: {e}")
            traceback.print_exc()

        if all_cancellations:
            print(f"\n[DB] Updating {len(all_cancellations)} cancellation(s)...")
            try:
                updated = update_cancellations(all_cancellations)
                print(f"  Marked {updated} listing(s) as cancelled.")
            except Exception as e:
                print(f"  [DB] ERROR: {e}")
                traceback.print_exc()

        print(f"\n[SYNC] Syncing DB → Sheets...")
        sync_to_sheets()
        print(f"  Sync complete.")

    print(f"\n  Run complete: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)


def run_tn_check(dry_run: bool = False, counties: list[str] | None = None) -> None:
    """
    --tncheck: Cross-reference active TN Auctions rows against live trustee
    listing sites to detect postponements and near-sale absences.

    For each trustee firm with an active scraper, compare sheet rows against
    the firm's current public listing page:
      - Same property, different date  → postponement (update Sale Date + note)
      - Property absent, sale ≤ 14 days out → flag for manual check
      - Property absent, sale > 14 days out → no action (may not be posted yet)

    Trustee firms not in the registry are written to the Trustee Registry tab
    for manual follow-up.
    """
    print("\n[TN CHECK] Loading active TN listings from sheet...")
    tn_rows = get_tn_listings_for_check()
    if not tn_rows:
        print("  No active TN listings found.")
        return
    print(f"  {len(tn_rows)} active TN row(s).")

    # Optionally filter to specific counties
    if counties:
        counties_lower = {c.lower() for c in counties}
        tn_rows = [r for r in tn_rows if r.get("County", "").lower() in counties_lower]
        print(f"  Filtered to {len(tn_rows)} row(s) matching county filter.")

    # Group rows by canonical trustee key
    trustee_groups: dict[str, list[dict]] = {}
    unknown_trustees: dict[str, str] = {}  # name → source_url

    for row in tn_rows:
        firm = row.get("Attorney / Firm", "").strip()
        if not firm:
            continue
        key, entry = lookup_trustee(firm)
        if key is None:
            if firm not in unknown_trustees:
                unknown_trustees[firm] = row.get("Source URL", "")
            continue
        trustee_groups.setdefault(key, []).append(row)

    # Report and record unknown trustees
    if unknown_trustees:
        print(f"\n  {len(unknown_trustees)} unrecognized trustee firm(s) — writing to Trustee Registry tab:")
        for name in unknown_trustees:
            print(f"    - {name}")
        ensure_trustee_registry_tab()
        n = write_unknown_trustees(unknown_trustees, dry_run=dry_run)
        if n:
            print(f"  {n} new row(s) added to Trustee Registry tab.")

    # Run check for each trustee that has a working scraper
    all_postponements: list[dict] = []
    all_flags: list[dict] = []

    for key, rows in trustee_groups.items():
        entry = TRUSTEE_REGISTRY[key]
        scraper_name = entry.get("scraper")
        canonical = entry["canonical_name"]
        status = entry.get("status", "")

        if scraper_name == "rubin_lublin":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            postponements, flags = _rl_scraper.check_existing(rows, dry_run=dry_run)
            print(
                f"    {len(postponements)} postponement(s), "
                f"{len(flags)} manual-check flag(s)."
            )
            all_postponements.extend(postponements)
            all_flags.extend(flags)


        elif scraper_name == "internetpostings":

            # FLG rows are covered by the site-wide internetpostings block below.

            # Acknowledge here to suppress the "not wired" warning.

            print(f"\n  [TN CHECK] {canonical} — {len(rows)} row(s) covered by site-wide internetpostings check.")

        elif scraper_name == "mackie_wolf":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            postponements, flags = _mw_scraper.check_existing(rows, dry_run=dry_run)
            print(
                f"    {len(postponements)} postponement(s), "
                f"{len(flags)} manual-check flag(s)."
            )
            all_postponements.extend(postponements)
            all_flags.extend(flags)

        elif scraper_name == "robertson_anschutz":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            postponements, flags = _rasc_scraper.check_existing(rows, dry_run=dry_run)
            print(
                f"    {len(postponements)} postponement(s), "
                f"{len(flags)} manual-check flag(s)."
            )
            all_postponements.extend(postponements)
            all_flags.extend(flags)

        elif scraper_name == "brock_scott":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            postponements, flags = _bs_scraper.check_existing(rows, dry_run=dry_run)
            print(
                f"    {len(postponements)} postponement(s), "
                f"{len(flags)} manual-check flag(s)."
            )
            all_postponements.extend(postponements)
            all_flags.extend(flags)

        elif scraper_name == "capital_city_postings":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            postponements, flags = _ccp_scraper.check_existing(rows, dry_run=dry_run)
            print(
                f"    {len(postponements)} postponement(s), "
                f"{len(flags)} manual-check flag(s)."
            )
            all_postponements.extend(postponements)
            all_flags.extend(flags)

        elif scraper_name == "mickel_law":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            postponements, flags = _mickel_scraper.check_existing(rows, dry_run=dry_run)
            print(
                f"    {len(postponements)} postponement(s), "
                f"{len(flags)} manual-check flag(s)."
            )
            all_postponements.extend(postponements)
            all_flags.extend(flags)

        elif scraper_name == "nw_posting_services":
            # Marinosci + ALAW both live here — accumulate rows,
            # run one combined API call after the loop.
            if not hasattr(run_tn_check, "_nwps_rows"):
                run_tn_check._nwps_rows = []
            run_tn_check._nwps_rows.extend(rows)
            print(
                f"\n  [TN CHECK] {canonical} — {len(rows)} row(s) queued "
                f"for NW Posting Services combined check."
            )

        elif scraper_name == "clear_recon":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            _, flags = _cr_scraper.check_existing(rows, dry_run=dry_run)
            print(f"    0 postponement(s), {len(flags)} manual-check flag(s).")
            all_flags.extend(flags)

        elif scraper_name == "phillip_jones":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            postponements, flags = _pj_scraper.check_existing(rows, dry_run=dry_run)
            print(
                f"    {len(postponements)} postponement(s), "
                f"{len(flags)} manual-check flag(s)."
            )
            all_postponements.extend(postponements)
            all_flags.extend(flags)

        elif scraper_name == "anchor_posting":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            postponements, flags = _ap_scraper.check_existing(rows, dry_run=dry_run)
            print(f"    {len(postponements)} postponement(s), {len(flags)} manual-check flag(s).")
            all_postponements.extend(postponements)
            all_flags.extend(flags)

        elif scraper_name == "foreclosure_postings":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            postponements, flags = _fp_scraper.check_existing(rows, dry_run=dry_run)
            print(f"    {len(postponements)} postponement(s), {len(flags)} manual-check flag(s).")
            all_postponements.extend(postponements)
            all_flags.extend(flags)

        elif scraper_name == "better_choice_notices":
            print(f"\n  [TN CHECK] {canonical} — checking {len(rows)} row(s)...")
            postponements, flags = _bcn_scraper.check_existing(rows, dry_run=dry_run)
            print(f"    {len(postponements)} postponement(s), {len(flags)} manual-check flag(s).")
            all_postponements.extend(postponements)
            all_flags.extend(flags)

        elif status == "active":

            # Scraper listed as active but not handled above — shouldn't happen

            print(f"\n  [TN CHECK] {canonical} — scraper '{scraper_name}' not wired in run_tn_check(). Skipping.")

        elif status in ("pending", "needs_research"):
            print(f"\n  [TN CHECK] {canonical} — no scraper yet (status: {status}). Skipping {len(rows)} row(s).")

        # no_site and no_scraper: silent skip

    # NW Posting Services combined check (Marinosci + ALAW in one API call)
    nwps_rows = getattr(run_tn_check, "_nwps_rows", [])
    if nwps_rows:
        print(
            f"\n  [TN CHECK] NW Posting Services — checking "
            f"{len(nwps_rows)} row(s) (Marinosci + ALAW combined)..."
        )
        try:
            nwps_postponements, _ = _nwps_scraper.check_existing(
                nwps_rows, dry_run=dry_run
            )
            print(f"    {len(nwps_postponements)} postponement(s).")
            all_postponements.extend(nwps_postponements)
        except Exception as e:
            print(f"  [NW POSTING SERVICES] check ERROR: {e}")
            traceback.print_exc()
        finally:
            if hasattr(run_tn_check, "_nwps_rows"):
                del run_tn_check._nwps_rows

    print(f"\n  [TN CHECK] internetpostings.com — checking all {len(tn_rows)} TN row(s)...")
    try:
        ip_postponements, _ = _ip_scraper.check_existing(tn_rows, dry_run=dry_run)
        print(f"    {len(ip_postponements)} postponement(s) detected.")
        all_postponements.extend(ip_postponements)
    except Exception as e:
        print(f"  [internetpostings] check ERROR: {e}")
        traceback.print_exc()

    # Write results
    if all_postponements:
        print(f"\n  Writing {len(all_postponements)} postponement(s)...")
        n = update_tn_postponements(all_postponements, dry_run=dry_run)
        print(f"  {n} postponement(s) written.")

    if all_flags:
        print(f"  Writing {len(all_flags)} manual-check flag(s)...")
        n = flag_tn_for_manual_check(all_flags, dry_run=dry_run)
        print(f"  {n} row(s) flagged.")

    print("\n[TN CHECK] Done.")

# ── Valuate ───────────────────────────────────────────────────────────────────

def run_valuate(counties: list[str] | None = None, dry_run: bool = False):
    print("=" * 60)
    print(f"  Eagle Creek Auction Monitor — Valuation")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    print(f"\n[DB] Fetching listings needing valuation...")
    try:
        listings = get_listings_needing_valuation(county_filter=counties)
    except Exception as e:
        print(f"  [DB] ERROR: {e}")
        traceback.print_exc()
        return

    if not listings:
        print(f"  No upcoming listings need valuation.")
        return

    print(f"  Found {len(listings)} listing(s) to valuate:")
    for l in listings:
        addr = f"{l.get('Street', '')}, {l.get('City', '')}"
        print(f"    {l.get('County')}, {l.get('State')} — {addr} (Sale: {l.get('Sale Date')})")

    print(f"\n[VALUATION] Running BatchData lookups...")
    try:
        valuated = run_valuations(listings, dry_run=dry_run)
    except Exception as e:
        print(f"  [Valuation] ERROR: {e}")
        traceback.print_exc()
        return

    if not valuated:
        print(f"  No results returned.")
        return

    print(f"\n{'=' * 60}")
    from collections import Counter
    signal_counts: dict[str, int] = Counter(l.get("equity_signal", "❓") for l in valuated)
    print(f"  Signal summary: " + "  ".join(f"{s}: {c}" for s, c in sorted(signal_counts.items())))

    print(f"\n  Detailed results:")
    for l in valuated:
        emv  = l.get("emv")
        debt = l.get("debt")
        addr = f"{l.get('Street', '')}, {l.get('City', '')}"
        print(
            f"\n  {l.get('County')}, {l.get('State')} — {addr}\n"
            f"    EMV: {'${:,.0f}'.format(emv) if emv else '—'}  |  "
            f"Equity: {'${:,.0f}'.format(emv - debt) if emv and debt is not None else '—'}  |  "
            f"Signal: {l.get('equity_signal', '—')}"
        )

    if dry_run:
        print(f"\n  [DRY RUN] Not writing to DB.")
        update_valuations(valuated, dry_run=True)
    else:
        print(f"\n[DB] Writing valuations...")
        try:
            updated_count = update_valuations(valuated)
            print(f"  Updated {updated_count} row(s).")
        except Exception as e:
            print(f"  [DB] ERROR: {e}")
            traceback.print_exc()

        print(f"\n[SYNC] Syncing DB → Sheets...")
        sync_to_sheets()
        print(f"  Sync complete.")

    print(f"\n  Run complete: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)


# ── Skip trace ────────────────────────────────────────────────────────────────

def run_skiptrace(dry_run: bool = False):
    print("=" * 60)
    print(f"  Eagle Creek Auction Monitor — Skip Trace")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    if not dry_run:
        ensure_skiptrace_header()

    print(f"\n[DB] Fetching listings needing skip trace...")
    try:
        listings = get_listings_needing_skiptrace()
    except Exception as e:
        print(f"  [DB] ERROR: {e}")
        traceback.print_exc()
        return

    if not listings:
        print(f"  No listings need skip tracing.")
        print(f"  (Qualifying: equity ✅, 🏆, or ❓, upcoming, not cancelled, not yet traced.)")
        return

    print(f"  Found {len(listings)} listing(s) to skip trace:")
    for l in listings:
        addr = f"{l.get('Street', '')}, {l.get('City', '')}"
        print(f"    {l.get('County')}, {l.get('State')} — {addr} "
              f"(Sale: {l.get('Sale Date')}, Signal: {l.get('Equity Signal')})")

    if dry_run:
        print(f"\n  [DRY RUN] No API calls or writes will be made.")

    print(f"\n[SKIPTRACE] Running BatchData lookups...")
    try:
        results = run_skiptraces(listings, dry_run=dry_run)
    except Exception as e:
        print(f"  [SkipTrace] ERROR: {e}")
        traceback.print_exc()
        return

    succeeded = [r for r in results if not r.get("_skipped") and not r.get("_error")]
    hit       = [r for r in succeeded if r.get("Owner Name (Primary)")]
    no_hit    = [r for r in succeeded if not r.get("Owner Name (Primary)")]
    errored   = [r for r in results if r.get("_error")]

    print(f"\n{'=' * 60}")
    print(f"  Skip Trace Results:")
    print(f"    Contact found: {len(hit)}  |  No hit: {len(no_hit)}  |  Errors: {len(errored)}")

    if hit:
        print(f"\n  Contacts found:")
        for r in hit:
            addr = f"{r.get('Street', '')}, {r.get('City', '')}"
            print(
                f"\n  {r.get('County')}, {r.get('State')} — {addr}\n"
                f"    Owner (Primary):   {r.get('Owner Name (Primary)', '—')}\n"
                f"    Owner (Secondary): {r.get('Owner Name (Secondary)') or '—'}\n"
                f"    Phones:            {r.get('Owner Phone(s)') or '—'}\n"
                f"    Deceased:          {r.get('Deceased') or '—'}"
            )

    if errored:
        print(f"\n  Errors:")
        for r in errored:
            addr = f"{r.get('Street', '')} {r.get('City', '')}".strip()
            print(f"    ⚠ {addr or 'unknown'}: {r['_error']}")

    if dry_run:
        print(f"\n  [DRY RUN] Not writing to DB.")
        update_skiptraces(results, dry_run=True)
    else:
        print(f"\n[DB] Writing skip trace results...")
        try:
            written = update_skiptraces(results)
            print(f"  Updated {written} row(s).")
        except Exception as e:
            print(f"  [DB] ERROR: {e}")
            traceback.print_exc()

        print(f"\n[SYNC] Syncing DB → Sheets...")
        sync_to_sheets()
        print(f"  Sync complete.")

    print(f"\n  Run complete: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)


# ── Heir research ─────────────────────────────────────────────────────────────

def run_heirresearch(dry_run: bool = False):
    print("=" * 60)
    print(f"  Eagle Creek Auction Monitor — Heir Research")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    if not dry_run:
        ensure_heir_research_headers()

    print(f"\n[DB] Fetching listings needing heir research...")
    try:
        listings = get_listings_needing_heir_research()
    except Exception as e:
        print(f"  [DB] ERROR: {e}")
        traceback.print_exc()
        return

    if not listings:
        print(f"  No listings need heir research.")
        print(f"  (Qualifying: Deceased = Yes, upcoming, Heir Research Date blank.)")
        return

    print(f"  Found {len(listings)} listing(s) to research:")
    for l in listings:
        addr = f"{l.get('Street', '')}, {l.get('City', '')}"
        print(
            f"    {l.get('County')}, {l.get('State')} — {addr} "
            f"(Owner: {l.get('Owner Name (Primary)') or '?'}, Sale: {l.get('Sale Date')})"
        )

    if dry_run:
        print(f"\n  [DRY RUN] No Claude calls or writes will be made.")

    print(f"\n[HEIR RESEARCH] Searching obituaries via Claude...")
    try:
        results = run_heir_research(listings, dry_run=dry_run)
    except Exception as e:
        print(f"  [HeirResearch] ERROR: {e}")
        traceback.print_exc()
        return

    found        = [r for r in results if r.get("Obit Found") == "Yes"]
    not_found    = [r for r in results if r.get("Obit Found") == "No"]
    errored      = [r for r in results if r.get("_error") and not r.get("_skipped")]
    total_heirs  = sum(len(r.get("_heirs_list", [])) for r in found)

    print(f"\n{'=' * 60}")
    print(f"  Heir Research Results:")
    print(f"    Obituaries found:    {len(found)}")
    print(f"    Not found:           {len(not_found)}")
    print(f"    Errors:              {len(errored)}")
    print(f"    Total heirs found:   {total_heirs}")

    if found:
        print(f"\n  Obituaries found:")
        for r in found:
            addr       = f"{r.get('Street', '')}, {r.get('City', '')}"
            match      = r.get("Defendant Match", "No")
            match_flag = " ⚠️ DEFENDANT MATCH" if r.get("_defendant_match") else ""
            print(
                f"\n  {r.get('County')}, {r.get('State')} — {addr}{match_flag}\n"
                f"    Owner:    {r.get('Owner Name (Primary)', '—')}\n"
                f"    Heirs:    {r.get('Heirs') or '(none listed)'}\n"
                f"    Match:    {match}"
            )

    if not_found:
        print(f"\n  No obituary found:")
        for r in not_found:
            addr = f"{r.get('Street', '')}, {r.get('City', '')}"
            print(f"    {r.get('County')}, {r.get('State')} — {addr} "
                  f"({r.get('Owner Name (Primary)') or '?'})")

    if errored:
        print(f"\n  Errors:")
        for r in errored:
            addr = f"{r.get('Street', '')} {r.get('City', '')}".strip()
            print(f"    ⚠ {addr}: {r.get('_error')}")

    if dry_run:
        print(f"\n  [DRY RUN] Not writing to DB.")
        update_heir_research(results, dry_run=True)
        write_heir_leads(results, dry_run=True)
    else:
        print(f"\n[DB] Writing heir research results...")
        try:
            written = update_heir_research(results)
            print(f"  Updated {written} Auctions row(s).")
        except Exception as e:
            print(f"  [DB] ERROR writing heir research: {e}")
            traceback.print_exc()

        print(f"[DB] Writing heir leads...")
        try:
            added = write_heir_leads(results)
            print(f"  Added {added} row(s) to Heir Leads.")
        except Exception as e:
            print(f"  [DB] ERROR writing heir leads: {e}")
            traceback.print_exc()

        print(f"\n[SYNC] Syncing DB → Sheets...")
        sync_to_sheets()
        print(f"  Sync complete.")

    print(f"\n  Run complete: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)


# ── Heir skip trace ───────────────────────────────────────────────────────────

def run_heir_skiptrace(dry_run: bool = False):
    """
    Phase 4b — Skip trace heirs in the Heir Leads tab via Tracerfy.

    Reads every Heir Leads row where Skip Traced Date (col N) is blank,
    calls Tracerfy's Instant Trace Lookup (find_owner=False) for each heir,
    and writes Phone(s), Email(s), Mailing Address, and today's date back
    to cols K–N in a single batchUpdate.

    Today's date is always written to prevent re-processing — even on a
    no-hit or a blank-name row. Errors leave the date blank so the row
    stays eligible for retry on the next run.

    dedup_heir_phones() runs unconditionally at the end of every run
    (including dry-run) so duplicate phones are always cleaned up.
    """
    print("=" * 60)
    print(f"  Eagle Creek Auction Monitor — Heir Skip Trace")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    print(f"\n[DB] Fetching heirs needing skip trace...")
    try:
        heirs = get_heirs_needing_skiptrace()
    except Exception as e:
        print(f"  [DB] ERROR: {e}")
        traceback.print_exc()
        return

    if not heirs:
        print(f"  No heirs need skip tracing.")
        print(f"  (Qualifying: Heir Leads rows where Skip Traced Date col N is blank.)")
        print(f"\n[DB] Deduplicating phone numbers across heirs...")
        try:
            deduped = dedup_heir_phones(dry_run=dry_run)
            if deduped:
                print(f"  Modified {deduped} row(s) — duplicate phones removed or flagged.")
            else:
                print(f"  No duplicate phones found.")
        except Exception as e:
            print(f"  [DB] ERROR during phone dedup: {e}")
            traceback.print_exc()
        return

    print(f"  Found {len(heirs)} heir(s) to process.")

    hit_count    = 0
    no_hit_count = 0
    error_count  = 0

    if dry_run:
        print(f"\n  [DRY RUN] No Tracerfy API calls or writes will be made.")
        print(f"\n  Would process:")
        for heir in heirs:
            print(
                f"    Row {heir['row_index']:>4}: {heir['heir_name']!r:30} "
                f"@ {heir['street']}, {heir['city']}, {heir['state']}"
            )

    else:
        today = date.today().isoformat()
        results = []

        print(f"\n[TRACERFY] Running heir skip traces...")
        for heir in heirs:
            row_n     = heir["row_index"]
            heir_name = heir["heir_name"]
            street    = heir["street"]
            city      = heir["city"]
            state     = heir["state"]

            if not heir_name:
                print(f"  Row {row_n:>4}: [blank heir name] — marking done, skipping API call.")
                results.append(
                    {"row_index": row_n, "phones": "", "emails": "", "mailing": "", "date": today}
                )
                continue

            print(f"  Row {row_n:>4}: {heir_name!r} @ {street}, {city}, {state}")

            try:
                result = skip_trace_heir(
                    heir_name=heir_name,
                    street=street,
                    city=city,
                    state=state,
                )

                if result["hit"]:
                    hit_count += 1
                    phones_preview = result["phones"][:50] if result["phones"] else "(none)"
                    print(f"           → HIT   phones: {phones_preview}")
                else:
                    no_hit_count += 1
                    print(f"           → miss  (0 credits deducted)")

                results.append(
                    {
                        "row_index": row_n,
                        "phones":    result["phones"],
                        "emails":    result["emails"],
                        "mailing":   result["mailing"],
                        "date":      today,
                    }
                )

            except RuntimeError as e:
                error_count += 1
                print(f"           ERROR: {e}")
                continue

            time.sleep(2)

        if results:
            print(f"\n[DB] Writing heir skip trace results...")
            try:
                written = update_heir_skiptraces(results)
                print(f"  Updated {written} row(s).")
            except Exception as e:
                print(f"  [DB] ERROR: {e}")
                traceback.print_exc()
        else:
            print(f"\n  Nothing to write (all rows errored).")

        print(f"\n[SYNC] Syncing DB → Sheets...")
        sync_to_sheets()
        print(f"  Sync complete.")

    print(f"\n[DB] Deduplicating phone numbers across heirs...")
    try:
        deduped = dedup_heir_phones(dry_run=dry_run)
        if deduped:
            print(f"  Modified {deduped} row(s) — duplicate phones removed or flagged.")
        else:
            print(f"  No duplicate phones found.")
    except Exception as e:
        print(f"  [DB] ERROR during phone dedup: {e}")
        traceback.print_exc()

    print(f"\n{'=' * 60}")
    print(f"  Heir Skip Trace Results:")
    print(f"    Contacts found:  {hit_count}")
    print(f"    No hit:          {no_hit_count}")
    print(f"    Errors (retry):  {error_count}")

    print(f"\n  Run complete: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _print_sample(listings: list[dict], n: int = 5):
    print(f"\n  Sample listings (first {min(n, len(listings))}):")
    for listing in listings[:n]:
        addr = ", ".join(p for p in [
            listing.get("Street", ""),
            listing.get("City", ""),
            listing.get("Zip", ""),
        ] if p)
        print(f"\n  ── {listing.get('County')}, {listing.get('State')} ──")
        for key in ["Sale Date", "Case Number", "Plaintiff", "Defendant(s)",
                    "Appraised Value", "Judgment / Loan Amount", "Cancelled"]:
            val = listing.get(key, "")
            if val:
                print(f"    {key}: {val}")
        if addr:
            print(f"    Address: {addr}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Eagle Creek Auction Monitor")
    parser.add_argument("--county",        nargs="+",        help="Filter to specific counties")
    parser.add_argument("--dry-run",       action="store_true", help="Preview without writing")
    parser.add_argument("--email-only",    action="store_true", help="Only check Gmail sources")
    parser.add_argument("--web-only",      action="store_true", help="Only check web sources")
    parser.add_argument("--valuate",       action="store_true", help="Run market valuation")
    parser.add_argument("--skiptrace",     action="store_true", help="Run owner skip trace (BatchData)")
    parser.add_argument("--heirresearch",  action="store_true", help="Run heir obituary research")
    parser.add_argument("--heirskiptrace", action="store_true",
                        help="Phase 4b: Skip trace heirs in Heir Leads tab via Tracerfy")
    parser.add_argument(
        "--tncheck",
        action="store_true",
        help="Cross-check active TN listings against live trustee sites for postponements/cancellations.",
    )
    parser.add_argument(
        "--ingest-directskip",
        metavar="CSV_PATH",
        help="Ingest a DirectSkip results CSV into the database.",
    )
    parser.add_argument(
        "--phoneburner",
        action="store_true",
        help="Export a PhoneBurner upload CSV (sales 5–30 days out, 🏆/✅, DirectSkip data required).",
    )
    parser.add_argument(
        "--propai",
        action="store_true",
        help="Export a Prop.ai upload CSV (sales 5–30 days out, 🏆/✅, DirectSkip data required).",
    )
    parser.add_argument(
        "--phoneburner-push",
        action="store_true",
        dest="phoneburner_push",
        help="Push contacts directly to PhoneBurner via API (sales 5–30 days out, 🏆/✅, DirectSkip data required).",
    )
    parser.add_argument(
        "--propai-push",
        action="store_true",
        dest="propai_push",
        help="Create a Prop.ai campaign and upload qualifying leads via API (sales 5–30 days out, 🏆/✅, DirectSkip data required).",
    )
    parser.add_argument(
        "--propai-sync",
        action="store_true",
        dest="propai_sync",
        help="Pull call dispositions from Prop.ai and upsert into propai_results (polls campaigns pushed in the last 30 days).",
    )
    parser.add_argument(
        "--propai-sync-all",
        action="store_true",
        dest="propai_sync_all",
        help="Like --propai-sync but polls every campaign in the account, not just ones tracked locally.",
    )
    parser.add_argument(
        "--directskip-export",
        action="store_true",
        dest="directskip_export",
        help="Generate a DirectSkip upload CSV from unprocessed listings (no upload).",
    )
    parser.add_argument(
        "--directskip-upload",
        action="store_true",
        dest="directskip_upload",
        help="Full DirectSkip cycle: export CSV → upload → poll → download → ingest.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser automation in headless mode (default: headed/visible).",
    )

    args = parser.parse_args()

    if args.directskip_upload:
        print("=" * 60)
        print(f"  Eagle Creek Auction Monitor — DirectSkip Upload")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)
        run_directskip_upload(headless=args.headless, dry_run=args.dry_run)
        if not args.dry_run:
            print(f"\n[SYNC] Syncing DB → Sheets...")
            sync_to_sheets()
            print(f"  Sync complete.")
    elif args.directskip_export:
        print("=" * 60)
        print(f"  Eagle Creek Auction Monitor — DirectSkip Export")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)
        generate_directskip_csv(dry_run=args.dry_run)
    elif args.phoneburner_push:
        print("=" * 60)
        print(f"  Eagle Creek Auction Monitor — PhoneBurner Push")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)
        push_phoneburner(dry_run=args.dry_run)
    elif args.phoneburner:
        print("=" * 60)
        print(f"  Eagle Creek Auction Monitor — PhoneBurner Export")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)
        generate_phoneburner(dry_run=args.dry_run)
    elif args.propai_push:
        print("=" * 60)
        print(f"  Eagle Creek Auction Monitor — Prop.ai Push")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)
        push_propai(dry_run=args.dry_run)
    elif args.propai_sync:
        print("=" * 60)
        print(f"  Eagle Creek Auction Monitor — Prop.ai Sync")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)
        sync_propai(dry_run=args.dry_run)
    elif args.propai_sync_all:
        print("=" * 60)
        print(f"  Eagle Creek Auction Monitor — Prop.ai Sync (All Campaigns)")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)
        sync_propai(dry_run=args.dry_run, all_campaigns=True)
    elif args.propai:
        print("=" * 60)
        print(f"  Eagle Creek Auction Monitor — Prop.ai Export")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)
        generate_propai(dry_run=args.dry_run)
    elif args.ingest_directskip:
        counts = ingest_directskip(args.ingest_directskip, dry_run=args.dry_run)
        if not args.dry_run:
            print(f"\n[SYNC] Syncing DB → Sheets...")
            sync_to_sheets()
            print(f"  Sync complete.")
    elif args.valuate:
        run_valuate(counties=args.county, dry_run=args.dry_run)
    elif args.skiptrace:
        run_skiptrace(dry_run=args.dry_run)
    elif args.heirresearch:
        run_heirresearch(dry_run=args.dry_run)
    elif args.heirskiptrace:
        run_heir_skiptrace(dry_run=args.dry_run)
    elif args.tncheck:
        run_tn_check(dry_run=args.dry_run, counties=args.county)
    else:
        run_scrape(
            counties=args.county,
            dry_run=args.dry_run,
            email_only=args.email_only,
            web_only=args.web_only,
        )