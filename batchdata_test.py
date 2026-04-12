"""
BatchData API test script.
Hits the property lookup endpoint with a single address and dumps the full
JSON response so we can identify the exact field names for valuation and
mortgage-liens data before integrating into valuation.py.

Usage:
    python batchdata_test.py
"""

import json
import requests
import sys
import os

# ── Config ────────────────────────────────────────────────────────────────────
# Pull key from config.py, or override here for a quick test
try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from config import BATCHDATA_API_KEY
except ImportError:
    BATCHDATA_API_KEY = "YOUR_KEY_HERE"

print(len(BATCHDATA_API_KEY))

ENDPOINT = "https://api.batchdata.com/api/v1/property/lookup/all-attributes"

# ── Test address — swap this out for any property on your sheet ───────────────
TEST_ADDRESS = {
    "street": "412 BRADLEY AVE",   # Hamilton OH example from session notes
    "city":   "CINCINNATI",
    "state":  "OH",
    "zip":    "45215",
}

# ── Request ───────────────────────────────────────────────────────────────────
payload = {
    "requests": [
        {
            "address": TEST_ADDRESS,
            "options": {
                # Request all three datasets so we can see every field name
                "datasets": ["basic", "valuation", "mortgage-liens"],
            },
        }
    ]
}

headers = {
    "Authorization": f"Bearer {BATCHDATA_API_KEY}",
    "Content-Type":  "application/json",
}

print(f"Sending request to {ENDPOINT}")
print(f"Address: {TEST_ADDRESS}")
print("-" * 60)

resp = requests.post(ENDPOINT, json=payload, headers=headers, timeout=30)

print(f"HTTP status: {resp.status_code}")
print(f"Response headers: {dict(resp.headers)}")
print(f"Response length: {len(resp.content)} bytes")
print(f"Raw response text: {repr(resp.text[:500])}")
print()

if not resp.text.strip():
    print("ERROR: Empty response body. Check API key and token permissions.")
    sys.exit(1)

if resp.status_code != 200:
    print("ERROR RESPONSE:")
    print(resp.text)
    sys.exit(1)

data = resp.json()

# ── Print full response pretty-printed ───────────────────────────────────────
print("FULL RESPONSE:")
print(json.dumps(data, indent=2, default=str))

# ── Also extract just the sections we care about ──────────────────────────────
try:
    prop = data["results"]["properties"][0]

    print("\n" + "=" * 60)
    print("VALUATION SECTION:")
    print(json.dumps(prop.get("valuation"), indent=2, default=str))

    print("\n" + "=" * 60)
    print("MORTGAGE / LIENS SECTION:")
    print(json.dumps(prop.get("mortgage") or prop.get("mortgageLiens")
                     or prop.get("mortgage_liens") or prop.get("liens"), indent=2, default=str))

    print("\n" + "=" * 60)
    print("BASIC SECTION:")
    print(json.dumps(prop.get("basic") or prop.get("property"), indent=2, default=str))

    print("\n" + "=" * 60)
    print("TOP-LEVEL KEYS IN PROPERTY OBJECT:")
    print(list(prop.keys()))

except (KeyError, IndexError) as e:
    print(f"\nCould not extract sections: {e}")
    print("Check the full response above.")