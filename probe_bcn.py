"""
probe_bcn.py — BetterChoiceNotices.com API Discovery
Run from the project root:  python probe_bcn.py

Attempts to find the API endpoint that populates the TN listing table.
Tries common REST/GraphQL/ASP.NET/Next.js patterns.

If a candidate returns JSON with address data, prints the full response
structure so you can identify field names for the scraper.

Also accepts a URL directly if you've already captured it in DevTools:
    python probe_bcn.py --url "https://betterchoicenotices.com/api/..."
"""

import argparse
import json
import sys
import time

import requests

BASE = "https://betterchoicenotices.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE + "/",
    "X-Requested-With": "XMLHttpRequest",
}

# Candidate endpoint paths — ordered by likelihood based on site tech stack
# (appears to be React + possible ASP.NET Core or Next.js backend)
CANDIDATES = [
    # REST-style
    ("GET",  "/api/notices?state=TN"),
    ("GET",  "/api/notices?state=TN&pageSize=100"),
    ("GET",  "/api/listings?state=TN"),
    ("GET",  "/api/sales?state=TN"),
    ("GET",  "/api/foreclosures?state=TN"),
    ("GET",  "/api/postings?state=TN"),
    ("GET",  "/api/search?state=TN"),
    ("GET",  "/api/notices/search?state=TN"),
    # Next.js API routes
    ("GET",  "/api/notices"),
    ("GET",  "/api/getNotices?state=TN"),
    ("GET",  "/api/getTNNotices"),
    # POST variants
    ("POST", "/api/notices/search"),
    ("POST", "/api/search"),
    ("POST", "/api/notices"),
    # ASP.NET controller style
    ("GET",  "/Notice/GetAll?state=TN"),
    ("GET",  "/Notice/Search?state=TN"),
    ("GET",  "/Foreclosure/GetAll?state=TN"),
    ("GET",  "/Home/GetNotices?state=TN"),
    ("POST", "/Notice/Search"),
    ("POST", "/Foreclosure/Search"),
    # Power Apps / Dataverse style (less likely but possible)
    ("GET",  "/api/data/v9.0/notices?$filter=state eq 'TN'"),
    # GraphQL
    ("POST", "/graphql"),
    ("POST", "/api/graphql"),
]

POST_BODIES = [
    {"state": "TN"},
    {"state": "TN", "pageSize": 100},
    {"state": "TN", "county": ""},
    {"query": "query { notices(state: \"TN\") { id address county saleDate } }"},
]


def _is_useful(resp: requests.Response) -> bool:
    """Return True if the response looks like it contains listing data."""
    if resp.status_code != 200:
        return False
    ct = resp.headers.get("Content-Type", "")
    if "json" not in ct and "javascript" not in ct:
        return False
    text = resp.text.strip()
    if not text or text.startswith("<!"):
        return False
    # Looks like a JSON array or object
    return text.startswith(("{", "["))


def _try(method: str, path: str, body=None) -> None:
    url = BASE + path
    try:
        if method == "GET":
            resp = requests.get(url, headers=HEADERS, timeout=10)
        else:
            resp = requests.post(url, json=body, headers=HEADERS, timeout=10)

        status = resp.status_code
        ct = resp.headers.get("Content-Type", "")[:50]
        length = len(resp.content)

        if _is_useful(resp):
            print(f"\n{'='*60}")
            print(f"✅ HIT:  {method} {path}")
            print(f"   Status: {status}  Content-Type: {ct}  Length: {length}")
            try:
                data = resp.json()
                print(f"\n   Response preview (first item if list):")
                if isinstance(data, list):
                    print(f"   Total items: {len(data)}")
                    if data:
                        print(json.dumps(data[0], indent=4)[:2000])
                elif isinstance(data, dict):
                    # Show top-level keys and first item of any list values
                    print(f"   Top-level keys: {list(data.keys())}")
                    for k, v in data.items():
                        if isinstance(v, list) and v:
                            print(f"\n   data['{k}'][0]:")
                            print(json.dumps(v[0], indent=4)[:2000])
                            break
                        elif isinstance(v, (str, int, float, bool)):
                            print(f"   data['{k}'] = {v!r}")
            except Exception as e:
                print(f"   JSON parse error: {e}")
                print(f"   Raw: {resp.text[:500]}")
            print(f"{'='*60}")
        else:
            print(f"   {status:3}  {method:4}  {path}  [{ct}]  {length}b")

    except requests.exceptions.ConnectionError:
        print(f"   ERR  {method:4}  {path}  [connection refused]")
    except requests.exceptions.Timeout:
        print(f"   TMO  {method:4}  {path}  [timeout]")
    except Exception as e:
        print(f"   ERR  {method:4}  {path}  [{e}]")


def probe_all():
    print(f"Probing {BASE} for API endpoints...\n")

    for method, path in CANDIDATES:
        if method == "POST":
            for body in POST_BODIES:
                _try(method, path, body)
                time.sleep(0.3)
        else:
            _try(method, path)
        time.sleep(0.3)

    print("\nDone. If nothing showed ✅, the endpoint needs DevTools capture.")
    print("Open the site, click Search (State=Tennessee), then:")
    print("  DevTools → Network → XHR/Fetch → find the request")
    print("  Then run:  python probe_bcn.py --url <full_url> [--method POST] [--body '{\"state\":\"TN\"}'")


def probe_url(url: str, method: str = "GET", body: str | None = None):
    """Probe a specific URL captured from DevTools."""
    print(f"\nProbing specific URL: {method} {url}")
    parsed_body = json.loads(body) if body else None

    try:
        if method.upper() == "GET":
            resp = requests.get(url, headers=HEADERS, timeout=15)
        else:
            resp = requests.post(url, json=parsed_body, headers=HEADERS, timeout=15)

        print(f"Status: {resp.status_code}")
        print(f"Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
        print(f"Content-Length: {len(resp.content)} bytes")

        try:
            data = resp.json()
            if isinstance(data, list):
                print(f"\nResponse is a list of {len(data)} items.")
                if data:
                    print("\nFirst item:")
                    print(json.dumps(data[0], indent=2))
                    if len(data) > 1:
                        print("\nSecond item:")
                        print(json.dumps(data[1], indent=2))
            else:
                print("\nResponse:")
                print(json.dumps(data, indent=2)[:3000])
        except Exception:
            print("\nRaw response:")
            print(resp.text[:2000])

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Probe BetterChoiceNotices.com API")
    parser.add_argument("--url",    help="Specific URL to probe (from DevTools)")
    parser.add_argument("--method", default="GET", help="HTTP method (default: GET)")
    parser.add_argument("--body",   help="JSON body string for POST requests")
    args = parser.parse_args()

    if args.url:
        probe_url(args.url, args.method, args.body)
    else:
        probe_all()