"""
test_skiptrace.py — One-off script to validate BatchData skip trace API access.

Run from your project root:
    python test_skiptrace.py

Uses a hardcoded test address so you can confirm the endpoint is accessible on
your plan and inspect the raw response shape before the real skiptrace.py is wired in.
"""

import json
import requests
from config import BATCHDATA_API_KEY

SKIP_TRACE_URL = "https://api.batchdata.com/api/v1/property/skip-trace"

# ── Test address — swap for any real property you want to verify ──────────────
TEST_ADDRESS = {
    "street": "600 S 4th St",
    "city":   "Louisville",
    "state":  "KY",
    "zip":    "40202",
}
# ─────────────────────────────────────────────────────────────────────────────

def test_skip_trace():
    payload = {
        "requests": [
            {
                "propertyAddress": {
                    "street": TEST_ADDRESS["street"],
                    "city":   TEST_ADDRESS["city"],
                    "state":  TEST_ADDRESS["state"],
                    "zip":    TEST_ADDRESS["zip"],
                }
            }
        ]
    }

    headers = {
        "Authorization": f"Bearer {BATCHDATA_API_KEY}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    print(f"Calling BatchData skip trace for: {TEST_ADDRESS['street']}, "
          f"{TEST_ADDRESS['city']}, {TEST_ADDRESS['state']} {TEST_ADDRESS['zip']}")
    print(f"URL: {SKIP_TRACE_URL}\n")

    resp = requests.post(SKIP_TRACE_URL, json=payload, headers=headers, timeout=30)

    print(f"HTTP status: {resp.status_code}")
    print(f"Response headers: {dict(resp.headers)}\n")

    if resp.status_code != 200:
        print("ERROR — non-200 response body:")
        print(resp.text)
        return

    data = resp.json()
    print("── Raw JSON response ─────────────────────────────────────────────")
    print(json.dumps(data, indent=2))
    print("──────────────────────────────────────────────────────────────────\n")

    # ── Quick field inspection ─────────────────────────────────────────────
    results = (
        data.get("data", {}).get("results")
        or data.get("results")
        or []
    )

    if not results:
        print("No results array found. Check the raw JSON above for the correct path.")
        return

    r = results[0]
    print(f"Top-level keys in results[0]: {list(r.keys())}\n")

    # Owners
    owners = r.get("owners") or r.get("ownerInfo") or []
    print(f"Number of owner records: {len(owners)}")
    for i, owner in enumerate(owners):
        print(f"\n  Owner {i+1} keys: {list(owner.keys())}")

        # Name
        name_fields = owner.get("names") or owner.get("name") or []
        print(f"    Names: {name_fields}")

        # Phones
        phones = owner.get("phones") or owner.get("phoneNumbers") or []
        print(f"    Phones: {phones}")

        # Emails
        emails = owner.get("emails") or owner.get("email") or []
        print(f"    Emails: {emails}")

        # Mailing address
        mailing = (
            owner.get("mailingAddress")
            or owner.get("address")
            or {}
        )
        print(f"    Mailing address: {mailing}")

        # Deceased
        deceased = owner.get("isDeceased") or owner.get("deceased")
        print(f"    isDeceased: {deceased}")

    print("\n── Done. Paste the full raw JSON above into your next message ──")
    print("   so we can confirm field names before finalising skiptrace.py.")


if __name__ == "__main__":
    test_skip_trace()