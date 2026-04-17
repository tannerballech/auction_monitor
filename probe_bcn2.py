"""Quick probe to get all unique customer_name values from BCN."""
import requests, json, time

API = "https://api.betterchoicenotices.com/api/notices/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://betterchoicenotices.com/",
}

params = {
    "stateId": 44,
    "page_size": 100,
    "searchFromDate": "2026-04-13",
    "searchToDate": "2026-04-13",
}

customers = {}  # customer_name → {count, sample_case_nums}
total_rows = None
page = 1

while True:
    params["page"] = page
    resp = requests.get(API, params=params, headers=HEADERS, timeout=20)
    data = resp.json()

    if not data:
        break

    if total_rows is None:
        total_rows = data[0].get("row_count", 0)
        print(f"Total rows: {total_rows}")

    for row in data:
        cn = row.get("customer_name", "UNKNOWN")
        case = row.get("law_firm_case_number", "")
        if cn not in customers:
            customers[cn] = {"count": 0, "samples": []}
        customers[cn]["count"] += 1
        if len(customers[cn]["samples"]) < 3:
            customers[cn]["samples"].append(case)

    fetched = page * 100
    print(f"Page {page}: {len(data)} rows (total fetched: {min(fetched, total_rows)})")

    if len(data) < 100 or fetched >= total_rows:
        break
    page += 1
    time.sleep(0.5)

print("\n=== UNIQUE CUSTOMER NAMES ===")
for name, info in sorted(customers.items(), key=lambda x: -x[1]["count"]):
    print(f"  {info['count']:3}x  {name!r}")
    print(f"         samples: {info['samples']}")