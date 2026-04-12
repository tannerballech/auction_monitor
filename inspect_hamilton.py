# inspect_hamilton_v4.py
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

BASE = "https://hamilton.sheriffsaleauction.ohio.gov"

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        # ── Load current auction date ─────────────────────────────────────────
        url = f"{BASE}/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=0"
        page.goto(url, wait_until="networkidle")
        page.wait_for_timeout(2000)

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        # ── What date is showing? ─────────────────────────────────────────────
        print("=== PAGE TEXT (first 2000 chars) ===")
        print(soup.get_text(separator="\n", strip=True)[:2000])

        # ── Find all AUCTIONDATE navigation links ─────────────────────────────
        print("\n=== AUCTIONDATE LINKS ===")
        for a in soup.find_all("a", href=True):
            if "AUCTIONDATE" in a["href"].upper():
                print(f"  '{a.get_text(strip=True)}' -> {a['href']}")

        # ── Dump all tables ───────────────────────────────────────────────────
        print("\n=== TABLES ===")
        for i, table in enumerate(soup.find_all("table")):
            text = table.get_text(separator=" | ", strip=True)
            print(f"\n  -- Table {i} --")
            print(f"  Classes: {table.get('class')}")
            print(f"  Text: {text[:500]}")

        # ── Try navigating to next auction date ───────────────────────────────
        next_link = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "AUCTIONDATE" in href.upper() and "Next" in a.get_text():
                next_link = BASE + href if href.startswith("/") else href
                break

        if next_link:
            print(f"\n=== LOADING NEXT AUCTION: {next_link} ===")
            page.goto(next_link, wait_until="networkidle")
            page.wait_for_timeout(2000)

            html2 = page.content()
            soup2 = BeautifulSoup(html2, "html.parser")

            print("Page text (first 2000 chars):")
            print(soup2.get_text(separator="\n", strip=True)[:2000])

            print("\nTables:")
            for i, table in enumerate(soup2.find_all("table")):
                text = table.get_text(separator=" | ", strip=True)
                print(f"\n  -- Table {i} --")
                print(f"  Classes: {table.get('class')}")
                print(f"  Text: {text[:600]}")
        else:
            print("\nNo next auction link found.")

        browser.close()

if __name__ == "__main__":
    run()