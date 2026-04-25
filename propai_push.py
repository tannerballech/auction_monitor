"""
propai_push.py — Push qualifying leads directly to a new Prop.ai campaign.

Usage:
    python main.py --propai-push [--dry-run]

Flow:
    1. Authenticate with Firebase → get JWT token
    2. Query qualifying contacts from DB
    3. Build CSV in memory (Prospect Name, Phone Number, Address, City, State, Zip)
    4. Create a new Prop.ai campaign named "Eagle Creek YYYY-MM-DD"
    5. GET /leads_batch_upload → pre-signed Firebase Storage upload URL
    6. PUT CSV to Firebase Storage
    7. PUT /campaigns/{id} to link uploaded batch
    8. GET /campaigns/run/{id} to start the campaign

Requires in .env:
    PROPAI_EMAIL=...
    PROPAI_PASSWORD=...
"""

from __future__ import annotations

import csv
import io
import os
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

# "Preforeclosure by Johnny Mc" agent — confirmed via live API capture
AGENT_ID   = "6eb69f45-5a46-49e8-a256-430e5e105b48"
AGENT_NAME = "Vicky Vale"   # default AI caller name for this agent

# Default campaign settings — update AGENT_NAME above to change what the AI
# says its name is on the call (it fills {{agent_name}} in first_sentence).
_CAMPAIGN_DEFAULTS = {
    "agent_id":               AGENT_ID,
    "agent_name":             AGENT_NAME,
    "follow_up_caller_name":  "Jimmy",
    "company_name":           "Eagle Creek Holdings",
    "first_sentence":         "Hello this is {{agent_name}}. this is the owner of {{address}}? Correct?",
    "prompt_injection":       None,
    "ghl_pipeline_id":        None,
}


# ── Auth ──────────────────────────────────────────────────────────────────────

def _firebase_auth(email: str, password: str) -> tuple[str, str]:
    """Return (idToken, uid) via Firebase email/password sign-in."""
    resp = requests.post(_SIGN_IN_URL, json={
        "email": email,
        "password": password,
        "returnSecureToken": True,
    }, timeout=15)
    if not resp.ok:
        raise RuntimeError(
            f"Firebase auth failed {resp.status_code}: {resp.text[:400]}"
        )
    data = resp.json()
    return data["idToken"], data["localId"]


# ── API helper ────────────────────────────────────────────────────────────────

def _api(method: str, route: str, token: str, uid: str, **kwargs) -> dict:
    """Make an authenticated call to the Prop.ai main API."""
    headers = kwargs.pop("headers", {})
    if "json" in kwargs or "data" not in kwargs:
        headers.setdefault("Content-Type", "application/json")
    headers["Authorization"] = f"Bearer {token}"
    headers["x-userid"] = uid
    resp = requests.request(
        method,
        f"{API_BASE}/{route.lstrip('/')}",
        headers=headers,
        timeout=30,
        **kwargs,
    )
    if not resp.ok:
        raise RuntimeError(
            f"Prop.ai API {resp.status_code} on {method} {route}: {resp.text[:600]}"
        )
    if not resp.content:
        return {}
    return resp.json()


# ── Campaign ──────────────────────────────────────────────────────────────────

def _create_campaign(token: str, uid: str, name: str) -> str:
    """POST /campaigns and return the new campaign_id."""
    payload = {
        "user_id":       uid,
        "status":        "new",
        "campaign_name": name,
        **_CAMPAIGN_DEFAULTS,
    }
    data = _api("POST", "/campaigns", token, uid, json=payload)
    cid = (
        data.get("campaign_id")
        or data.get("id")
        or (data.get("campaign") or {}).get("campaign_id")
        or (data.get("campaign") or {}).get("id")
    )
    if not cid:
        raise RuntimeError(f"Campaign creation response missing ID:\n{data}")
    return str(cid)


# ── Lead upload ───────────────────────────────────────────────────────────────

def _get_upload_url(token: str, uid: str, campaign_id: str) -> tuple[str, str]:
    """
    GET /leads_batch_upload?campaign_id={id}
    Returns (upload_url, storage_path).
    storage_path may be empty if the response doesn't include it.
    """
    data = _api(
        "GET", f"/leads_batch_upload?campaign_id={campaign_id}", token, uid
    )
    url = (
        data.get("upload_url")
        or data.get("url")
        or data.get("signed_url")
        or data.get("uploadUrl")
    )
    if not url:
        raise RuntimeError(
            f"leads_batch_upload response missing upload URL.\n"
            f"Full response: {data}\n"
            f"Please report this so propai_push.py can be updated."
        )
    path = (
        data.get("file_path")
        or data.get("path")
        or data.get("storage_path")
        or data.get("gcs_path")
        or data.get("key")
        or ""
    )
    return url, path


def _upload_csv_to_storage(upload_url: str, csv_bytes: bytes) -> None:
    """PUT the CSV to the Firebase Storage pre-signed URL."""
    resp = requests.put(
        upload_url,
        data=csv_bytes,
        headers={"Content-Type": "text/csv"},
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(
            f"CSV upload to storage failed {resp.status_code}: {resp.text[:400]}"
        )


def _link_batch(
    token: str,
    uid: str,
    campaign_id: str,
    storage_path: str,
    file_name: str,
) -> None:
    """PUT /campaigns/{id} to associate the uploaded CSV batch."""
    batch: dict = {"file_name": file_name}
    if storage_path:
        batch["file_path"] = storage_path
    _api("PUT", f"/campaigns/{campaign_id}", token, uid, json={"batches": [batch]})


def _run_campaign(token: str, uid: str, campaign_id: str) -> None:
    """GET /campaigns/run/{id} — starts the campaign dialer."""
    _api("GET", f"/campaigns/run/{campaign_id}", token, uid)


# ── CSV builder ───────────────────────────────────────────────────────────────

def _build_csv(rows: list[dict]) -> bytes:
    """
    Build the Prop.ai lead CSV in memory.
    One row per non-blank phone number.
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Prospect Name", "Phone Number", "Address", "City", "State", "Zip"])
    for r in rows:
        first = (r["first_name"] or "").strip()
        last  = (r["last_name"]  or "").strip()
        name  = f"{first} {last}".strip()
        addr  = (r["street"] or "").strip()
        city  = (r["city"]   or "").strip()
        state = (r["state"]  or "").strip()
        zip_  = (r["zip"]    or "").strip()
        phones = [r.get(f"phone{i}") for i in range(1, 8)]
        for phone in phones:
            if not (phone or "").strip():
                continue
            writer.writerow([name, phone.strip(), addr, city, state, zip_])
    return buf.getvalue().encode("utf-8")


# ── DB query ──────────────────────────────────────────────────────────────────

def _query_leads(date_min: str, date_max: str) -> list[dict]:
    with _conn() as con:
        rows = con.execute("""
            SELECT
                l.id AS listing_id,
                p.first_name, p.last_name,
                p.phone1, p.phone2, p.phone3,
                p.phone4, p.phone5, p.phone6, p.phone7,
                l.street, l.city, l.state, l.zip,
                l.sale_date
            FROM directskip_persons p
            JOIN listings l ON l.id = p.listing_id
            WHERE l.equity_signal IN ('🏆', '✅')
              AND (l.directskip_date IS NOT NULL AND l.directskip_date != '')
              AND (l.cancelled IS NULL OR LOWER(l.cancelled) != 'yes')
              AND (l.propai_pushed_at IS NULL OR l.propai_pushed_at = '')
              AND l.sale_date BETWEEN ? AND ?
            ORDER BY l.sale_date ASC, p.listing_id, p.person_number
        """, (date_min, date_max)).fetchall()
    return [dict(r) for r in rows]


def _mark_pushed(listing_ids: list[int], pushed_date: str) -> None:
    """Stamp propai_pushed_at on every listing that was just uploaded."""
    if not listing_ids:
        return
    with _conn() as con:
        con.executemany(
            "UPDATE listings SET propai_pushed_at = ? WHERE id = ?",
            [(pushed_date, lid) for lid in listing_ids],
        )


# ── Entry point ───────────────────────────────────────────────────────────────

def push(dry_run: bool = False) -> None:
    """
    Query qualifying leads from the DB and push them to a new Prop.ai campaign.
    """
    today    = date.today()
    date_min = (today + timedelta(days=5)).isoformat()
    date_max = (today + timedelta(days=30)).isoformat()

    rows = _query_leads(date_min, date_max)
    if not rows:
        print("  [Prop.ai] No qualifying contacts — nothing to push.")
        return

    csv_bytes = _build_csv(rows)
    n_leads   = csv_bytes.count(b"\n") - 1  # subtract header line

    print(f"  [Prop.ai] {n_leads} lead row(s) from {len(rows)} person(s).")

    if dry_run:
        campaign_name = f"Eagle Creek {today.isoformat()}"
        print(
            f"  [Prop.ai] [DRY RUN] Would create campaign '{campaign_name}' "
            f"and upload {n_leads} rows."
        )
        for r in rows[:5]:
            phones = [r[f"phone{i}"] for i in range(1, 8) if (r.get(f"phone{i}") or "").strip()]
            print(
                f"    {r['first_name']} {r['last_name']} | "
                f"{r['street']}, {r['city']} | "
                f"{len(phones)} phone(s)"
            )
        if len(rows) > 5:
            print(f"    ... and {len(rows) - 5} more")
        return

    # ── Live push ──────────────────────────────────────────────────────────────
    email    = os.environ.get("PROPAI_EMAIL", "")
    password = os.environ.get("PROPAI_PASSWORD", "")
    if not email or not password:
        raise RuntimeError("PROPAI_EMAIL and PROPAI_PASSWORD must be set in .env")

    print("  [Prop.ai] Authenticating...")
    token, uid = _firebase_auth(email, password)

    campaign_name = f"Eagle Creek {today.isoformat()}"
    print(f"  [Prop.ai] Creating campaign '{campaign_name}'...")
    campaign_id = _create_campaign(token, uid, campaign_name)
    print(f"  [Prop.ai] Campaign ID: {campaign_id}")

    print("  [Prop.ai] Getting upload URL...")
    upload_url, storage_path = _get_upload_url(token, uid, campaign_id)

    print(f"  [Prop.ai] Uploading {n_leads} leads to storage...")
    _upload_csv_to_storage(upload_url, csv_bytes)

    file_name = f"Eagle_Creek_{today.isoformat()}.csv"
    print("  [Prop.ai] Linking batch to campaign...")
    _link_batch(token, uid, campaign_id, storage_path, file_name)

    print("  [Prop.ai] Starting campaign...")
    _run_campaign(token, uid, campaign_id)

    listing_ids = list({r["listing_id"] for r in rows})
    _mark_pushed(listing_ids, today.isoformat())

    print(
        f"  [Prop.ai] ✓ Done — campaign '{campaign_name}' "
        f"running with {n_leads} leads ({len(listing_ids)} listing(s) marked pushed)."
    )
