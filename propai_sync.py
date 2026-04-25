"""
propai_sync.py — Pull call dispositions from Prop.ai back into the DB.

Usage:
    python main.py --propai-sync [--dry-run]

For each campaign recorded in propai_pushes within the last 30 days, pages
through GET /leads and upserts results into propai_results. Leads are matched
back to listings via phone number through directskip_persons.

Requires in .env:
    PROPAI_EMAIL=...
    PROPAI_PASSWORD=...
"""

from __future__ import annotations

import os
from collections import Counter
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

from db import _conn

load_dotenv()

FIREBASE_API_KEY = "AIzaSyCpj1eBiZ6lwjMckcNOr84zuBrnrep3mko"
_SIGN_IN_URL = (
    f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
    f"?key={FIREBASE_API_KEY}"
)
API_BASE = "https://ff34s0wbc8.execute-api.us-east-1.amazonaws.com/production"


# ── Auth ──────────────────────────────────────────────────────────────────────

def _firebase_auth(email: str, password: str) -> tuple[str, str]:
    resp = requests.post(_SIGN_IN_URL, json={
        "email": email, "password": password, "returnSecureToken": True,
    }, timeout=15)
    if not resp.ok:
        raise RuntimeError(f"Firebase auth failed {resp.status_code}: {resp.text[:400]}")
    data = resp.json()
    return data["idToken"], data["localId"]


# ── API helper ────────────────────────────────────────────────────────────────

def _api_get(route: str, token: str, uid: str, params: dict | None = None) -> dict:
    resp = requests.get(
        f"{API_BASE}/{route.lstrip('/')}",
        headers={"Authorization": f"Bearer {token}", "x-userid": uid},
        params=params,
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"Prop.ai {resp.status_code} GET {route}: {resp.text[:400]}")
    return resp.json() if resp.content else {}


# ── Phone normalisation ───────────────────────────────────────────────────────

def _norm(phone: str | None) -> str:
    """Strip non-digits, return last 10."""
    digits = "".join(c for c in (phone or "") if c.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits


# ── Phone → listing_id index ─────────────────────────────────────────────────

def _build_phone_index() -> dict[str, int]:
    """Map normalised 10-digit phone → listing_id from directskip_persons."""
    index: dict[str, int] = {}
    with _conn() as con:
        rows = con.execute(
            "SELECT listing_id, phone1, phone2, phone3, phone4, phone5, phone6, phone7 "
            "FROM directskip_persons"
        ).fetchall()
    for r in rows:
        for i in range(1, 8):
            ph = _norm(r[f"phone{i}"])
            if ph and ph not in index:
                index[ph] = r["listing_id"]
    return index


# ── Leads fetching ────────────────────────────────────────────────────────────

def _fetch_all_leads(token: str, uid: str, campaign_id: str) -> list[dict]:
    """Page through GET /leads for one campaign and return every lead."""
    leads: list[dict] = []
    cursor: str | None = None
    while True:
        params: dict = {"campaign_id": campaign_id, "user_id": uid, "limit": 100}
        if cursor:
            params["next_page"] = cursor
        data = _api_get("/leads", token, uid, params=params)
        batch = data.get("leads") or []
        leads.extend(batch)
        next_cur = (data.get("pagination") or {}).get("next_page")
        if not next_cur or not batch:
            break
        cursor = next_cur
    return leads


# ── DB upsert ─────────────────────────────────────────────────────────────────

def _upsert_results(leads: list[dict], phone_index: dict[str, int]) -> tuple[int, int]:
    """
    Upsert lead records into propai_results.
    Returns (rows_written, leads_matched_to_listing).
    """
    today = date.today().isoformat()
    rows = []
    matched = 0
    for lead in leads:
        ph = _norm(lead.get("phone_number"))
        listing_id = phone_index.get(ph)
        if listing_id:
            matched += 1
        rows.append((
            lead.get("lead_id"),
            lead.get("campaign_id"),
            listing_id,
            lead.get("phone_number"),
            lead.get("prospect_name"),
            lead.get("lead_status"),
            lead.get("call_status"),
            lead.get("answered_by"),
            1 if lead.get("callback_requested") else 0,
            lead.get("total_calls") or 0,
            lead.get("last_call_dt"),
            lead.get("call_summary") or "",
            lead.get("analysis") or "",
            lead.get("transcript") or "",
            today,
        ))
    with _conn() as con:
        con.executemany(
            """INSERT INTO propai_results (
                   lead_id, campaign_id, listing_id, phone_number, prospect_name,
                   lead_status, call_status, answered_by, callback_requested,
                   total_calls, last_call_dt, call_summary, analysis, transcript,
                   synced_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(lead_id) DO UPDATE SET
                   lead_status        = excluded.lead_status,
                   call_status        = excluded.call_status,
                   answered_by        = excluded.answered_by,
                   callback_requested = excluded.callback_requested,
                   total_calls        = excluded.total_calls,
                   last_call_dt       = excluded.last_call_dt,
                   call_summary       = excluded.call_summary,
                   analysis           = excluded.analysis,
                   transcript         = excluded.transcript,
                   synced_at          = excluded.synced_at""",
            rows,
        )
    return len(rows), matched


# ── Campaign list ─────────────────────────────────────────────────────────────

def _get_tracked_campaigns() -> list[dict]:
    """Return distinct campaigns pushed in the last 30 days."""
    cutoff = (date.today() - timedelta(days=30)).isoformat()
    with _conn() as con:
        rows = con.execute(
            """SELECT DISTINCT campaign_id, campaign_name, pushed_at
               FROM propai_pushes
               WHERE pushed_at >= ?
               ORDER BY pushed_at DESC""",
            (cutoff,),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Summary helpers ───────────────────────────────────────────────────────────

def _print_campaign_summary(
    campaign_name: str,
    leads: list[dict],
    n_written: int,
    n_matched: int,
) -> None:
    status_counts = Counter(l.get("lead_status") or "Unknown" for l in leads)
    callbacks = [l for l in leads if l.get("callback_requested")]

    print(f"  ✓ '{campaign_name}' — {n_written} leads synced, {n_matched} matched to listings")
    for status, count in sorted(status_counts.items()):
        print(f"      {count:>4}  {status}")

    if callbacks:
        print(f"    *** {len(callbacks)} CALLBACK(S) REQUESTED ***")
        for lead in callbacks:
            summary = (lead.get("call_summary") or "").strip()
            summary_line = f" — {summary[:80]}" if summary else ""
            print(f"      {lead.get('prospect_name','?')} | {lead.get('phone_number','')} | {lead.get('address','')}{summary_line}")


# ── Entry point ───────────────────────────────────────────────────────────────

def sync(dry_run: bool = False) -> None:
    """Pull call dispositions from Prop.ai and upsert into propai_results."""
    campaigns = _get_tracked_campaigns()
    if not campaigns:
        print("  [Prop.ai Sync] No campaigns tracked in the last 30 days.")
        print("  [Prop.ai Sync] Run --propai-push first to create and record a campaign.")
        return

    print(f"  [Prop.ai Sync] {len(campaigns)} campaign(s) to sync.")

    if dry_run:
        for c in campaigns:
            print(f"    {c['pushed_at']}  {c['campaign_name']}  ({c['campaign_id'][:8]}...)")
        return

    email    = os.environ.get("PROPAI_EMAIL", "")
    password = os.environ.get("PROPAI_PASSWORD", "")
    if not email or not password:
        raise RuntimeError("PROPAI_EMAIL and PROPAI_PASSWORD must be set in .env")

    print("  [Prop.ai Sync] Authenticating...")
    token, uid = _firebase_auth(email, password)

    print("  [Prop.ai Sync] Building phone index...")
    phone_index = _build_phone_index()
    print(f"  [Prop.ai Sync] {len(phone_index)} phone numbers indexed.")

    total_leads = 0
    total_callbacks = 0

    for camp in campaigns:
        cid   = camp["campaign_id"]
        cname = camp["campaign_name"] or cid[:8]

        try:
            leads = _fetch_all_leads(token, uid, cid)
        except Exception as e:
            print(f"  ✗ '{cname}': {e}")
            continue

        if not leads:
            print(f"  — '{cname}': no leads returned")
            continue

        n_written, n_matched = _upsert_results(leads, phone_index)
        _print_campaign_summary(cname, leads, n_written, n_matched)
        total_leads += n_written
        total_callbacks += sum(1 for l in leads if l.get("callback_requested"))

    print(f"\n  [Prop.ai Sync] Done — {total_leads} total leads upserted across {len(campaigns)} campaign(s).")
    if total_callbacks:
        print(f"  [Prop.ai Sync] *** {total_callbacks} total callback(s) requested — check the output above ***")
