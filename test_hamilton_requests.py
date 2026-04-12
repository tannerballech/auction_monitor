# test_hamilton_requests.py
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
}

BASE = "https://hamilton.sheriffsaleauction.ohio.gov"

# Test 1: current/next auction date (AUCTIONDATE=0)
url = f"{BASE}/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=0"
resp = requests.get(url, headers=HEADERS, timeout=15)
print(f"Status: {resp.status_code}  |  Length: {len(resp.text)} bytes")

soup = BeautifulSoup(resp.text, "html.parser")
print("\n--- Page text (first 3000 chars) ---")
print(soup.get_text(separator="\n", strip=True)[:3000])

# Test 2: look for the Next Auction link to understand AUCTIONDATE parameter
print("\n--- All links containing 'AUCTIONDATE' ---")
for a in soup.find_all("a", href=True):
    if "AUCTIONDATE" in a["href"].upper():
        print(f"  Text: '{a.get_text(strip=True)}'  |  href: {a['href']}")

# Test 3: look for the property listing table
print("\n--- Tables found ---")
for i, table in enumerate(soup.find_all("table")):
    text = table.get_text(separator=" ", strip=True)[:200]
    print(f"  Table {i}: {text}")