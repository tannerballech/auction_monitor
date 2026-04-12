"""
propai_probe.py — Probe the Prop.ai webhook endpoint to discover expected payload format.

Usage:
    python propai_probe.py --api-key YOUR_KEY_HERE

Steps:
    1. OPTIONS request  — checks allowed methods / CORS headers
    2. GET request      — see if the endpoint returns anything useful (schema hint, 405, etc.)
    3. POST empty       — see what validation error looks like (reveals required fields)
    4. POST minimal     — try a plausible payload with dummy data

Run from the auction_monitor directory (or anywhere — no project imports needed).
"""

import argparse
import json
import requests

ENDPOINT = "https://spo6bhar3e.execute-api.us-east-1.amazonaws.com/production-rest-api/crm/lead_campaign_call"

# Dummy contact — no real person, clearly fake phone
DUMMY_CONTACT = {
    "first_name":   "Test",
    "last_name":    "Contact",
    "phone":        "5550000000",
    "address":      "123 Test Street",
    "city":         "Louisville",
    "state":        "KY",
    "zip":          "40202",
    "county":       "Jefferson",
    "property_address": "123 Test Street, Louisville KY 40202",
    "sale_date":    "2026-05-01",
    "equity_signal": "✅",
}


def print_response(label, resp):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"  Status : {resp.status_code}")
    print(f"  Headers: {dict(resp.headers)}")
    try:
        body = resp.json()
        print(f"  Body   : {json.dumps(body, indent=2)}")
    except Exception:
        print(f"  Body   : {resp.text[:500]}")


def probe(api_key):
    headers_base = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": "EagleCreekMonitor/1.0",
    }

    # ── Step 1: OPTIONS ──────────────────────────────────────────────────────
    print("\n[1] OPTIONS — checking allowed methods...")
    try:
        r = requests.options(ENDPOINT, headers=headers_base, timeout=10)
        print_response("OPTIONS", r)
    except Exception as e:
        print(f"  OPTIONS failed: {e}")

    # ── Step 2: GET ──────────────────────────────────────────────────────────
    print("\n[2] GET — checking for schema hint or 405...")
    try:
        r = requests.get(ENDPOINT, headers=headers_base, timeout=10)
        print_response("GET", r)
    except Exception as e:
        print(f"  GET failed: {e}")

    # ── Step 3: POST empty body ──────────────────────────────────────────────
    print("\n[3] POST empty body — expecting validation error with field names...")
    try:
        r = requests.post(ENDPOINT, headers=headers_base, json={}, timeout=10)
        print_response("POST empty", r)
    except Exception as e:
        print(f"  POST empty failed: {e}")

    # ── Step 4: POST minimal dummy payload ───────────────────────────────────
    print("\n[4] POST dummy contact — plausible payload...")
    try:
        r = requests.post(ENDPOINT, headers=headers_base, json=DUMMY_CONTACT, timeout=10)
        print_response("POST dummy", r)
    except Exception as e:
        print(f"  POST dummy failed: {e}")

    # ── Step 5: Try X-API-Key header variant ─────────────────────────────────
    print("\n[5] POST dummy with X-API-Key header (alternative auth format)...")
    headers_alt = {**headers_base, "x-api-key": api_key}
    del headers_alt["Authorization"]
    try:
        r = requests.post(ENDPOINT, headers=headers_alt, json=DUMMY_CONTACT, timeout=10)
        print_response("POST X-API-Key", r)
    except Exception as e:
        print(f"  POST X-API-Key failed: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True, help="Your Prop.ai API key")
    args = parser.parse_args()
    probe(args.api_key)