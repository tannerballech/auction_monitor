"""
propai_probe4.py — Retry with real Stride/GHL location ID.

Previous 502s were likely because "placeholder_location" didn't match
any connected account in Prop.ai's backend.

Real location ID: R806uHHYeoJddARh30FD

Usage:
    python propai_probe4.py --api-key YOUR_KEY --campaign-id CAMPAIGN_ID
"""

import argparse
import json
import requests

ENDPOINT    = "https://spo6bhar3e.execute-api.us-east-1.amazonaws.com/production-rest-api/crm/lead_campaign_call"
LOCATION_ID = "R806uHHYeoJddARh30FD"

# Dummy data — clearly fake, won't call anyone
DUMMY_PHONE = "+15550000003"
DUMMY_FIRST = "EagleTest"
DUMMY_LAST  = "ProbeOnly"


def post(api_key, payload, label):
    headers = {
        "x-api-key":    api_key,
        "Content-Type": "application/json",
        "Origin":       "https://app.prop.ai",
        "Referer":      "https://app.prop.ai/",
    }
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  Payload: {json.dumps(payload)}")
    print(f"{'='*60}")
    r = requests.post(ENDPOINT, headers=headers, json=payload, timeout=10)
    print(f"  Status : {r.status_code}")
    try:
        print(f"  Body   : {json.dumps(r.json(), indent=2)}")
    except Exception:
        print(f"  Body   : {r.text[:500]}")


def probe(api_key, campaign_id):

    # 1. Real locationId + campaign_id + minimal contact
    post(api_key, {
        "locationId":  LOCATION_ID,
        "campaign_id": campaign_id,
        "phone":       DUMMY_PHONE,
        "firstName":   DUMMY_FIRST,
        "lastName":    DUMMY_LAST,
    }, "1. Real locationId + campaign_id + E.164 phone (camelCase)")

    # 2. snake_case version
    post(api_key, {
        "location_id": LOCATION_ID,
        "campaign_id": campaign_id,
        "phone":       DUMMY_PHONE,
        "first_name":  DUMMY_FIRST,
        "last_name":   DUMMY_LAST,
    }, "2. Real location_id snake_case + campaign_id")

    # 3. Full GHL contact webhook envelope with real locationId
    post(api_key, {
        "type":        "ContactCreate",
        "locationId":  LOCATION_ID,
        "campaign_id": campaign_id,
        "id":          "test-contact-abc999",
        "firstName":   DUMMY_FIRST,
        "lastName":    DUMMY_LAST,
        "phone":       DUMMY_PHONE,
        "address1":    "123 Test Street",
        "city":        "Louisville",
        "state":       "KY",
        "postalCode":  "40202",
    }, "3. Full GHL ContactCreate envelope with real locationId")

    # 4. Maybe Prop.ai uses their own internal location/account field name
    post(api_key, {
        "account_id":  LOCATION_ID,
        "campaign_id": campaign_id,
        "phone":       DUMMY_PHONE,
        "first_name":  DUMMY_FIRST,
        "last_name":   DUMMY_LAST,
    }, "4. account_id instead of locationId")

    # 5. Minimal — just location + phone, no name
    post(api_key, {
        "locationId":  LOCATION_ID,
        "campaign_id": campaign_id,
        "phone":       DUMMY_PHONE,
    }, "5. Absolute minimum: locationId + campaign_id + phone")

    # 6. Raw phone (no +1) in case E.164 causes issues
    post(api_key, {
        "locationId":  LOCATION_ID,
        "campaign_id": campaign_id,
        "phone":       "5550000003",
        "first_name":  DUMMY_FIRST,
        "last_name":   DUMMY_LAST,
    }, "6. Non-E.164 phone (no +1 prefix)")

    print("\n" + "="*60)
    print("  DONE. Any non-502 is a breakthrough.")
    print("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key",     required=True)
    parser.add_argument("--campaign-id", required=True)
    args = parser.parse_args()
    probe(args.api_key, args.campaign_id)