"""
phoneburner_sync.py — Pull call dispositions from PhoneBurner back into the DB.

Usage:
    python main.py --phoneburner-sync       # date-windowed: last synced → today
    python main.py --phoneburner-sync-all   # all history (2020-01-01 → today)
    python main.py --phoneburner-sync --dry-run

For each dialsession in the window, fetches all calls and upserts into
phoneburner_results. Contacts are matched back to listings via:
  1. phoneburner_contacts (contact_user_id → listing_id)
  2. directskip_persons phone number index (fallback)

After syncing calls, updates:
  - directskip_persons.phone_status  (unknown → working/bad/confirmed_owner)
  - listings.follow_up_status        (active → follow_up/dnc)

Also sweeps all contacts with do_not_call=1 and marks their listing dnc.

Requires: PHONEBURNER_ACCESS_TOKEN in .env
"""

from __future__ import annotations

import os
import time
from collections import defaultdict
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

from db import _conn, init_db

load_dotenv()

BASE_URL = "https://www.phoneburner.com/rest/1"

_FOLLOW_UP_DISPOSITIONS = {"Not Interested", "Unavailable"}
_BAD_PHONE_DISPOSITIONS = {"Bad Phone", "Wrong Number"}
_CONFIRMED_OWNER_DISPOSITIONS = {"Set Appointment"}
_WORKING_DISPOSITIONS = {"No Answer", "Not Interested", "Unavailable", "Set Appointment"}


# -- API helper ----------------------------------------------------------------

def _api(method: str, route: str, **kwargs) -> dict:
    token = os.environ.get("PHONEBURNER_ACCESS_TOKEN", "")
    if not token:
        raise RuntimeError("PHONEBURNER_ACCESS_TOKEN is not set in .env")
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    resp = requests.request(
        method,
        f"{BASE_URL}/{route.lstrip('/')}",
        headers=headers,
        timeout=30,
        **kwargs,
    )
    if not resp.ok:
        raise RuntimeError(
            f"PhoneBurner API error {resp.status_code} on {method} /{route}: {resp.text[:400]}"
        )
    if resp.status_code == 204 or not resp.content:
        return {}
    return resp.json()


# -- Phone normalisation -------------------------------------------------------

def _norm(phone: str | None) -> str:
    digits = "".join(c for c in (phone or "") if c.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits


# -- Index builders ------------------------------------------------------------

def _build_contact_index() -> dict[str, int]:
    """Map contact_user_id -> listing_id from phoneburner_contacts."""
    index: dict[str, int] = {}
    with _conn() as con:
        rows = con.execute(
            "SELECT contact_user_id, listing_id FROM phoneburner_contacts"
        ).fetchall()
    for r in rows:
        if r["contact_user_id"] and r["listing_id"]:
            index[str(r["contact_user_id"])] = r["listing_id"]
    return index


def _build_phone_index() -> dict[str, int]:
    """Map normalised 10-digit phone -> listing_id from directskip_persons."""
    index: dict[str, int] = {}
    with _conn() as con:
        rows = con.execute(
            "SELECT listing_id, phone1, phone2, phone3, phone4, phone5 "
            "FROM directskip_persons"
        ).fetchall()
    for r in rows:
        for i in range(1, 6):
            ph = _norm(r[f"phone{i}"])
            if ph and ph not in index:
                index[ph] = r["listing_id"]
    return index


# -- Sync window ---------------------------------------------------------------

def _get_last_sync_date() -> str:
    """Return the most recent synced_at date in phoneburner_results, or 2020-01-01."""
    with _conn() as con:
        row = con.execute(
            "SELECT MAX(synced_at) AS last FROM phoneburner_results"
        ).fetchone()
    last = (row["last"] or "") if row else ""
    return last[:10] if last else "2020-01-01"


# -- Session + call fetching ---------------------------------------------------

def _fetch_sessions(date_start: str, date_end: str) -> list[dict]:
    """
    Page through GET /dialsession and return all session stubs.
    Each stub has at minimum: dialsession_id, start_when, end_when.
    """
    sessions = []
    page = 1
    while True:
        data = _api("GET", "dialsession", params={
            "date_start": date_start,
            "date_end":   date_end,
            "page_size":  100,
            "page":       page,
        })
        batch_obj = data.get("dialsessions", {})
        if isinstance(batch_obj, dict):
            batch = batch_obj.get("dialsessions", [])
            total_pages = int(batch_obj.get("total_pages", 1) or 1)
        else:
            batch = batch_obj or []
            total_pages = 1

        sessions.extend(batch if isinstance(batch, list) else [])
        if page >= total_pages or not batch:
            break
        page += 1
    return sessions


def _fetch_calls(session_id: str) -> list[dict]:
    """
    GET /dialsession/{id} and return the full calls list.
    PhoneBurner paginates calls at 25 per page; this fetches all pages.
    """
    all_calls: list[dict] = []
    page = 1
    while True:
        data = _api("GET", f"dialsession/{session_id}", params={"page": page})
        outer = data.get("dialsessions", data)
        total_pages = 1
        if isinstance(outer, dict) and "dialsessions" in outer:
            total_pages = int(outer.get("total_pages") or 1)
            ds_obj = outer["dialsessions"]
        else:
            ds_obj = outer
        calls = (ds_obj or {}).get("calls") or []
        if isinstance(calls, list):
            all_calls.extend(calls)
        if page >= total_pages or not calls:
            break
        page += 1
        time.sleep(0.05)
    return all_calls


# -- DB upsert -----------------------------------------------------------------

def _upsert_results(
    calls: list[dict],
    session_id: str,
    contact_index: dict[str, int],
    phone_index: dict[str, int],
) -> int:
    """Upsert call records into phoneburner_results. Returns rows written."""
    today = date.today().isoformat()
    rows = []
    for call in calls:
        contact_user_id = str(call.get("user_id") or call.get("contact_user_id") or "")
        phone = (call.get("phone") or "").strip()

        listing_id = contact_index.get(contact_user_id)
        if listing_id is None:
            listing_id = phone_index.get(_norm(phone))

        disposition = (
            call.get("disposition")
            or call.get("hangup_status")
            or ""
        ).strip()

        rows.append((
            str(call.get("call_id") or call.get("id") or ""),
            session_id,
            listing_id,
            contact_user_id or None,
            phone or None,
            disposition or None,
            1 if call.get("connected") else 0,
            1 if call.get("voicemail") else 0,
            call.get("voicemail_sent") or None,
            (call.get("note") or "").strip() or None,
            call.get("start_when") or None,
            call.get("end_when") or None,
            today,
        ))

    if not rows:
        return 0

    with _conn() as con:
        con.executemany(
            """INSERT INTO phoneburner_results (
                   call_id, dialsession_id, listing_id, contact_user_id, phone,
                   disposition, connected, voicemail, voicemail_sent, note,
                   start_when, end_when, synced_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(call_id) DO UPDATE SET
                   listing_id      = excluded.listing_id,
                   disposition     = excluded.disposition,
                   connected       = excluded.connected,
                   voicemail       = excluded.voicemail,
                   voicemail_sent  = excluded.voicemail_sent,
                   note            = excluded.note,
                   end_when        = excluded.end_when,
                   synced_at       = excluded.synced_at""",
            rows,
        )
    return len(rows)


# -- Status updates ------------------------------------------------------------

def _update_phone_statuses() -> int:
    """
    Set directskip_persons.phone_status based on phoneburner_results.

    Priority (highest wins, never downgrade):
      confirmed_owner > working > unknown
      bad: only if not already confirmed_owner
    """
    updated = 0
    with _conn() as con:
        rows = con.execute("""
            SELECT br.listing_id, br.phone, br.disposition
            FROM phoneburner_results br
            WHERE br.listing_id IS NOT NULL
              AND br.phone IS NOT NULL
              AND br.disposition IS NOT NULL
        """).fetchall()

        best: dict[tuple[int, str], str] = {}
        PRIORITY = {"confirmed_owner": 3, "working": 2, "bad": 1, "unknown": 0}
        for r in rows:
            lid = r["listing_id"]
            ph  = _norm(r["phone"])
            if not ph:
                continue
            d = r["disposition"]
            if d in _CONFIRMED_OWNER_DISPOSITIONS:
                new_status = "confirmed_owner"
            elif d in _BAD_PHONE_DISPOSITIONS:
                new_status = "bad"
            elif d in _WORKING_DISPOSITIONS:
                new_status = "working"
            else:
                continue

            key = (lid, ph)
            if PRIORITY.get(new_status, 0) > PRIORITY.get(best.get(key, "unknown"), 0):
                best[key] = new_status

        for (lid, ph), status in best.items():
            persons = con.execute(
                "SELECT id, phone1, phone2, phone3, phone4, phone5, phone_status "
                "FROM directskip_persons WHERE listing_id = ?",
                (lid,),
            ).fetchall()
            for person_row in persons:
                for col in ("phone1", "phone2", "phone3", "phone4", "phone5"):
                    if _norm(person_row[col]) == ph:
                        current = person_row["phone_status"] or "unknown"
                        if PRIORITY.get(status, 0) > PRIORITY.get(current, 0):
                            con.execute(
                                "UPDATE directskip_persons SET phone_status = ? WHERE id = ?",
                                (status, person_row["id"]),
                            )
                            updated += 1
                        break
    return updated


def _update_follow_up_statuses() -> int:
    """
    Set listings.follow_up_status based on phoneburner_results.
    Any listing with a follow-up disposition gets status follow_up (not dnc).
    """
    updated = 0
    with _conn() as con:
        rows = con.execute("""
            SELECT DISTINCT listing_id
            FROM phoneburner_results
            WHERE listing_id IS NOT NULL
              AND disposition IN ({})
        """.format(",".join("?" * len(_FOLLOW_UP_DISPOSITIONS))),
            list(_FOLLOW_UP_DISPOSITIONS),
        ).fetchall()

        for r in rows:
            con.execute(
                "UPDATE listings SET follow_up_status = 'follow_up' "
                "WHERE id = ? AND (follow_up_status IS NULL OR follow_up_status = 'active')",
                (r["listing_id"],),
            )
            updated += con.execute("SELECT changes()").fetchone()[0]
    return updated


def _sweep_dnc_contacts() -> int:
    """
    GET /contacts?updated_from=<last_sync> and mark any listing dnc
    where the contact has do_not_call=1 or a phone with is_global_dnc=1.
    """
    last = _get_last_sync_date() + " 00:00"
    marked = 0

    page = 1
    while True:
        try:
            data = _api("GET", "contacts", params={
                "updated_from": last,
                "page_size": 100,
                "page": page,
            })
        except Exception as e:
            print(f"  [PB Sync] Warning: DNC sweep error on page {page}: {e}")
            break

        contacts_obj = data.get("contacts", {})
        if isinstance(contacts_obj, dict):
            contacts = contacts_obj.get("contacts", [])
            total_pages = int(contacts_obj.get("total_pages", 1) or 1)
        else:
            contacts = contacts_obj or []
            total_pages = 1

        if not contacts:
            break

        dnc_contact_ids = []
        for c in contacts:
            is_dnc = bool(c.get("do_not_call"))
            if not is_dnc:
                for ph in (c.get("phones") or []):
                    if ph.get("is_global_dnc") or ph.get("do_not_call"):
                        is_dnc = True
                        break
            if is_dnc:
                cid = str(c.get("user_id") or c.get("contact_user_id") or "")
                if cid:
                    dnc_contact_ids.append(cid)

        if dnc_contact_ids:
            with _conn() as con:
                for cid in dnc_contact_ids:
                    row = con.execute(
                        "SELECT listing_id FROM phoneburner_contacts WHERE contact_user_id = ?",
                        (cid,),
                    ).fetchone()
                    if row and row["listing_id"]:
                        con.execute(
                            "UPDATE listings SET follow_up_status = 'dnc' WHERE id = ?",
                            (row["listing_id"],),
                        )
                        marked += con.execute("SELECT changes()").fetchone()[0]

        if page >= total_pages or not contacts:
            break
        page += 1
        time.sleep(0.1)

    return marked


# -- Entry point ---------------------------------------------------------------

def sync(dry_run: bool = False, all_sessions: bool = False) -> None:
    """
    Pull call dispositions from PhoneBurner and upsert into phoneburner_results.

    all_sessions=True fetches all history (2020-01-01 -> today) instead of
    the date-windowed default (last synced_at -> today).
    """
    init_db()  # ensure schema / migrations have run
    today      = date.today().isoformat()
    date_start = "2020-01-01" if all_sessions else _get_last_sync_date()
    date_end   = today

    print(f"  [PB Sync] Fetching sessions from {date_start} to {date_end}...")
    sessions = _fetch_sessions(date_start, date_end)

    if not sessions:
        print("  [PB Sync] No dialsessions found in window.")
        return

    print(f"  [PB Sync] {len(sessions)} session(s) to process.")

    if dry_run:
        for s in sessions[:5]:
            sid  = s.get("dialsession_id") or s.get("id") or "?"
            when = s.get("start_when") or s.get("date") or "?"
            print(f"    {when}  session_id={sid}")
        if len(sessions) > 5:
            print(f"    ... and {len(sessions) - 5} more")
        return

    print("  [PB Sync] Building contact and phone indexes...")
    contact_index = _build_contact_index()
    phone_index   = _build_phone_index()
    print(f"  [PB Sync] {len(contact_index)} contacts indexed, {len(phone_index)} phones indexed.")

    total_calls  = 0
    total_errors = 0

    for i, session in enumerate(sessions, 1):
        sid = str(session.get("dialsession_id") or session.get("id") or "")
        if not sid:
            continue
        try:
            calls = _fetch_calls(sid)
            if calls:
                n = _upsert_results(calls, sid, contact_index, phone_index)
                total_calls += n
            time.sleep(0.1)
        except Exception as e:
            total_errors += 1
            print(f"  [PB Sync] session {sid} error: {e}")

        if i % 10 == 0:
            print(f"  [PB Sync] ... {i}/{len(sessions)} sessions processed ({total_calls} calls so far)")

    print(f"  [PB Sync] Calls upserted: {total_calls}  Errors: {total_errors}")

    print("  [PB Sync] Updating phone statuses...")
    n_phones = _update_phone_statuses()
    print(f"  [PB Sync] {n_phones} phone status updates.")

    print("  [PB Sync] Updating follow-up statuses...")
    n_followup = _update_follow_up_statuses()
    print(f"  [PB Sync] {n_followup} listing(s) marked follow_up.")

    print("  [PB Sync] Sweeping DNC contacts...")
    n_dnc = _sweep_dnc_contacts()
    print(f"  [PB Sync] {n_dnc} listing(s) marked dnc.")

    print(f"  [PB Sync] Done -- {total_calls} calls, {n_phones} phone updates, "
          f"{n_followup} follow_up, {n_dnc} dnc.")
