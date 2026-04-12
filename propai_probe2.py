"""
propai_probe2.py — Discover the required payload fields for the Prop.ai webhook.

Auth is confirmed: x-api-key header (not Bearer).
Goal: get a 400 validation error that lists required fields, instead of a 502 crash.

Usage:
    python propai_probe2.py --api-key YOUR_KEY_HERE
    python propai_probe2.py --api-key YOUR_KEY_HERE --campaign-id YOUR_CAMPAIGN_ID
"""

import argparse
import json
import requests

ENDPOINT = "https://spo6bhar3e.execute-api.us-east-1.amazonaws.com/production-rest-api/crm/lead_campaign_call"

DUMMY_PHONE = "5550000001"
DUMMY_NAME_FIRST = "EagleTest"
DUMMY_NAME_LAST = "ProbeOnly"


def post(api_key, payload, label):
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
    }
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  Payload: {json.dumps(payload)}")
    print(f"{'='*60}")
    try:
        r = requests.post(ENDPOINT, headers=headers, json=payload, timeout=10)
        print(f"  Status : {r.status_code}")
        try:
            body = r.json()
            print(f"  Body   : {json.dumps(body, indent=2)}")
        except Exception:
            print(f"  Body   : {r.text[:500]}")
    except Exception as e:
        print(f"  ERROR  : {e}")


def probe(api_key, campaign_id):
    # ── Round 1: Absolute minimum ─────────────────────────────────────────────
    post(api_key, {}, "1. Empty body")

    post(api_key, {"phone": DUMMY_PHONE}, "2. Phone only")

    post(api_key, {
        "phone": DUMMY_PHONE,
        "first_name": DUMMY_NAME_FIRST,
        "last_name": DUMMY_NAME_LAST,
    }, "3. Name + phone")

    # ── Round 2: Add campaign_id variants ────────────────────────────────────
    if campaign_id:
        post(api_key, {
            "campaign_id": campaign_id,
            "phone": DUMMY_PHONE,
            "first_name": DUMMY_NAME_FIRST,
            "last_name": DUMMY_NAME_LAST,
        }, "4. campaign_id + name + phone")

        post(api_key, {
            "campaignId": campaign_id,
            "phone": DUMMY_PHONE,
            "first_name": DUMMY_NAME_FIRST,
            "last_name": DUMMY_NAME_LAST,
        }, "5. campaignId (camelCase) + name + phone")

        post(api_key, {
            "campaign_id": campaign_id,
            "phone_number": DUMMY_PHONE,
            "first_name": DUMMY_NAME_FIRST,
            "last_name": DUMMY_NAME_LAST,
        }, "6. campaign_id + phone_number (alt field name)")
    else:
        print("\n  [Skipping campaign_id tests — pass --campaign-id to enable]")

    # ── Round 3: Try api_key in the body (some APIs expect this) ─────────────
    post(api_key, {
        "api_key": api_key,
        "phone": DUMMY_PHONE,
        "first_name": DUMMY_NAME_FIRST,
        "last_name": DUMMY_NAME_LAST,
    }, "7. api_key in body + name + phone")

    # ── Round 4: GHL-style payload (Prop.ai is GHL white-label) ──────────────
    post(api_key, {
        "phone": DUMMY_PHONE,
        "firstName": DUMMY_NAME_FIRST,
        "lastName": DUMMY_NAME_LAST,
        "name": f"{DUMMY_NAME_FIRST} {DUMMY_NAME_LAST}",
    }, "8. GHL camelCase field names")

    if campaign_id:
        post(api_key, {
            "campaign_id": campaign_id,
            "phone": DUMMY_PHONE,
            "firstName": DUMMY_NAME_FIRST,
            "lastName": DUMMY_NAME_LAST,
            "name": f"{DUMMY_NAME_FIRST} {DUMMY_NAME_LAST}",
            "address": "123 Test Street",
            "city": "Louisville",
            "state": "KY",
            "zip": "40202",
        }, "9. GHL camelCase + campaign_id + full contact")

    # ── Round 5: Inbound webhook style (trigger call on existing contact) ─────
    # The tooltip says "Add this url to a custom webhook in your CRM to
    # automatically initiate voice calls" — so Prop.ai may expect a
    # contact_id or lead_id rather than raw contact fields.
    post(api_key, {
        "contact_id": "test-contact-id-12345",
    }, "10. contact_id only (existing contact trigger style)")

    if campaign_id:
        post(api_key, {
            "campaign_id": campaign_id,
            "contact_id": "test-contact-id-12345",
        }, "11. campaign_id + contact_id")

    print("\n" + "="*60)
    print("  DONE. Look for any 400 or 422 responses above —")
    print("  those will contain the actual required field names.")
    print("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--campaign-id", default=None,
                        help="Optional: Prop.ai campaign ID to test routing")
    args = parser.parse_args()
    probe(args.api_key, args.campaign_id)