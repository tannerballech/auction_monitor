"""
propai_probe3.py — Test GHL webhook envelope format + browser headers.

The Prop.ai inbound webhook says "add this to your CRM webhook" — meaning
their Lambda expects GHL's standard webhook payload, not a custom format.

This probe tries:
  - GHL contact webhook formats (two variants)
  - Phone in E.164 format (+1...)
  - Origin/Referer headers the Prop.ai browser app would send
  - A location_id field (GHL tenancy identifier)

Usage:
    python propai_probe3.py --api-key YOUR_KEY --campaign-id CAMPAIGN_ID
"""

import argparse
import json
import requests

ENDPOINT = "https://spo6bhar3e.execute-api.us-east-1.amazonaws.com/production-rest-api/crm/lead_campaign_call"

# Use E.164 format with +1 prefix — GHL always sends phone this way
DUMMY_PHONE_E164 = "+15550000002"
DUMMY_PHONE_RAW  = "5550000002"


def post(api_key, payload, label, extra_headers=None):
    headers = {
        "x-api-key":    api_key,
        "Content-Type": "application/json",
        "Origin":       "https://app.prop.ai",
        "Referer":      "https://app.prop.ai/",
        "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    if extra_headers:
        headers.update(extra_headers)

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

    # ── GHL standard contact webhook format ───────────────────────────────────
    # GHL sends this when a contact is created/updated
    post(api_key, {
        "type":        "ContactCreate",
        "locationId":  "placeholder_location",
        "id":          "test-contact-abc123",
        "firstName":   "EagleTest",
        "lastName":    "ProbeOnly",
        "phone":       DUMMY_PHONE_E164,
        "email":       "test@eaglecreekinvestments.com",
        "address1":    "123 Test Street",
        "city":        "Louisville",
        "state":       "KY",
        "postalCode":  "40202",
        "campaign_id": campaign_id,
    }, "1. GHL ContactCreate envelope + campaign_id + E.164 phone")

    # ── Same but snake_case type field ────────────────────────────────────────
    post(api_key, {
        "type":        "contact_create",
        "location_id": "placeholder_location",
        "contact_id":  "test-contact-abc123",
        "first_name":  "EagleTest",
        "last_name":   "ProbeOnly",
        "phone":       DUMMY_PHONE_E164,
        "campaign_id": campaign_id,
    }, "2. snake_case GHL variant + E.164 phone")

    # ── Maybe campaign_id goes in URL params, not body ────────────────────────
    print(f"\n{'='*60}")
    print(f"  3. campaign_id as URL query param, not body")
    print(f"{'='*60}")
    headers = {
        "x-api-key":    api_key,
        "Content-Type": "application/json",
        "Origin":       "https://app.prop.ai",
    }
    url_with_param = f"{ENDPOINT}?campaign_id={campaign_id}"
    try:
        r = requests.post(url_with_param, headers=headers, json={
            "firstName": "EagleTest",
            "lastName":  "ProbeOnly",
            "phone":     DUMMY_PHONE_E164,
        }, timeout=10)
        print(f"  Status : {r.status_code}")
        try:
            print(f"  Body   : {json.dumps(r.json(), indent=2)}")
        except Exception:
            print(f"  Body   : {r.text[:500]}")
    except Exception as e:
        print(f"  ERROR  : {e}")

    # ── Minimal with just E.164 phone + campaign_id ───────────────────────────
    post(api_key, {
        "phone":       DUMMY_PHONE_E164,
        "campaign_id": campaign_id,
    }, "4. Minimal E.164 phone + campaign_id only")

    # ── Maybe it's entirely URL-param driven (no body) ────────────────────────
    print(f"\n{'='*60}")
    print(f"  5. All params in URL query string, no body")
    print(f"{'='*60}")
    params = {
        "campaign_id": campaign_id,
        "phone":       DUMMY_PHONE_E164,
        "first_name":  "EagleTest",
        "last_name":   "ProbeOnly",
    }
    try:
        r = requests.post(ENDPOINT, headers=headers, params=params, timeout=10)
        print(f"  Status : {r.status_code}")
        try:
            print(f"  Body   : {json.dumps(r.json(), indent=2)}")
        except Exception:
            print(f"  Body   : {r.text[:500]}")
    except Exception as e:
        print(f"  ERROR  : {e}")

    # ── Try the exact CSV column names Prop.ai uses in its UI ─────────────────
    # Based on: Name, Phone, Address, City, State, Zip (from GUI description)
    post(api_key, {
        "campaign_id": campaign_id,
        "Name":        "EagleTest ProbeOnly",
        "Phone":       DUMMY_PHONE_RAW,
        "Address":     "123 Test Street",
        "City":        "Louisville",
        "State":       "KY",
        "Zip":         "40202",
    }, "6. CSV column names (Title Case) matching Prop.ai GUI upload format")

    print("\n" + "="*60)
    print("  DONE. Any non-502 status is a breakthrough.")
    print("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key",     required=True)
    parser.add_argument("--campaign-id", required=True)
    args = parser.parse_args()
    probe(args.api_key, args.campaign_id)