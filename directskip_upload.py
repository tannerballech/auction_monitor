"""
directskip_upload.py — Automate the full DirectSkip batch cycle:
  1. Generate the upload CSV from the DB
  2. Log into app.directskip.com
  3. Upload the CSV and map columns (4-step wizard)
  4. Poll until the order is marked Done
  5. Download the result file to exports/
  6. Ingest the results into the DB

Usage:
    python main.py --directskip-upload [--dry-run] [--headless]

Requires in .env:
    DIRECTSKIP_EMAIL=tanner@eaglecreeklands.com
    DIRECTSKIP_PASSWORD=your_password_here

Steps 1-2 of the wizard are fully automated.
Steps 3-4 are auto-submitted with default options (usually just a review
and a final confirm). On the first run, omit --headless so you can watch.
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

from directskip_export import generate as export_csv
from ingest_directskip import ingest

load_dotenv()

BASE_URL    = "https://app.directskip.com"
EXPORTS_DIR = Path(__file__).parent / "exports"

# Column mapping: DirectSkip form field name → CSV column header
COLUMN_MAP = {
    "header_lastname":         "Last Name",
    "header_firstname":        "First Name",
    "header_address":          "Mailing Address",
    "header_city":             "Mailing City",
    "header_state":            "Mailing State",
    "header_zip":              "Mailing Zip",
    "header_property_address": "Property Address",
    "header_property_city":    "Property City",
    "header_property_state":   "Property State",
    "header_property_zip":     "Property Zip",
    "header_custom_field1":    "Custom Field 1",
    "header_custom_field2":    "Custom Field 2",
    "header_custom_field3":    "Custom Field 3",
}

POLL_INTERVAL_SECS = 60    # check order status every 60 seconds
POLL_TIMEOUT_SECS  = 3600  # give up after 1 hour


async def _run(csv_path: Path, headless: bool) -> Path | None:
    """Core async Playwright workflow. Returns the downloaded result path."""
    email    = os.environ.get("DIRECTSKIP_EMAIL", "")
    password = os.environ.get("DIRECTSKIP_PASSWORD", "")
    if not email or not password:
        raise RuntimeError("DIRECTSKIP_EMAIL and DIRECTSKIP_PASSWORD must be set in .env")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        ctx     = await browser.new_context(accept_downloads=True)
        page    = await ctx.new_page()

        # ── Step 0: Login ─────────────────────────────────────────────────────
        print("  [DS] Logging in...")
        await page.goto(f"{BASE_URL}/login.php")
        await page.fill("#email",    email)
        await page.fill("#password", password)
        await page.click("input[type=submit]")

        try:
            await page.wait_for_url(f"{BASE_URL}/index.php", timeout=15_000)
        except PWTimeout:
            # Some accounts land on a different page after login — check for logout link
            if "login.php" in page.url:
                raise RuntimeError("Login failed — check DIRECTSKIP_EMAIL / DIRECTSKIP_PASSWORD in .env")
        print("  [DS] Logged in ✓")

        # ── Step 1: Upload file ───────────────────────────────────────────────
        print("  [DS] Navigating to new order...")
        await page.goto(f"{BASE_URL}/neworder.php")
        await page.wait_for_selector("#filename", timeout=10_000)

        await page.set_input_files("#filename", str(csv_path))
        # Ensure the header row checkbox is checked
        if not await page.is_checked("#header_row"):
            await page.check("#header_row")

        print("  [DS] Uploading file (Step 1)...")
        await page.click("input[name=submit_step1]")

        # ── Step 2: Column mapping ────────────────────────────────────────────
        await page.wait_for_selector("select[name=header_lastname]", timeout=20_000)
        print("  [DS] Mapping columns (Step 2)...")

        for field_name, csv_col in COLUMN_MAP.items():
            try:
                await page.select_option(f"select[name={field_name}]", label=csv_col)
            except Exception:
                pass  # field may not be present for all account configs

        await page.click("input[name=submit_step2]")

        # ── Step 3: Data preview — just a "Save" button, no decisions needed ──
        await page.wait_for_selector("input[type=submit]", timeout=15_000)
        print("  [DS] Step 3: data preview — confirming...")
        await page.click("input[type=submit]")

        # ── Step 4: Order summary / payment ───────────────────────────────────
        await page.wait_for_selector("body", timeout=10_000)
        print("  [DS] Step 4: checking payment method...")

        # Detect whether Stripe is present (= no token balance) or a plain
        # submit button is shown (= tokens will cover the order).
        stripe_present = await page.locator("iframe[src*='stripe.com']").count() > 0
        token_balance_text = await page.locator("body").inner_text()
        import re as _re
        token_match = _re.search(r'balance of (\d+)', token_balance_text)
        token_balance = int(token_match.group(1)) if token_match else 0

        if stripe_present and token_balance == 0:
            raise RuntimeError(
                "DirectSkip Step 4 requires a credit card — your token balance is 0. "
                "Purchase tokens at https://buy.directskip.com/ so orders can be "
                "submitted automatically without card entry."
            )

        # Tokens available — look for a plain submit/pay button (not inside Stripe iframe)
        submit = page.locator("input[type=submit]:not(iframe *), button[type=submit]:not(iframe *)").first
        print(f"  [DS] Step 4: submitting with {token_balance} token(s)...")
        await submit.click()

        # ── Wait to land on files.php ─────────────────────────────────────────
        try:
            await page.wait_for_url(f"{BASE_URL}/files.php", timeout=15_000)
        except PWTimeout:
            # May redirect to files.php with a query string
            if "files.php" not in page.url:
                await page.goto(f"{BASE_URL}/files.php")

        # ── Get the most recent order ID ──────────────────────────────────────
        await page.wait_for_selector("table tr", timeout=10_000)
        first_data_row = page.locator("table tr").nth(1)
        order_id = (await first_data_row.locator("td").nth(0).inner_text()).strip()
        print(f"  [DS] Order placed: #{order_id}")

        # ── Poll for completion ───────────────────────────────────────────────
        print(f"  [DS] Waiting for order #{order_id} to complete (polling every {POLL_INTERVAL_SECS}s)...")
        deadline = time.time() + POLL_TIMEOUT_SECS

        while time.time() < deadline:
            await page.goto(f"{BASE_URL}/files.php")
            await page.wait_for_selector("table tr", timeout=10_000)

            # Find the row for our order
            row_locator = page.locator(f"table tr:has-text('{order_id}')")
            try:
                status_cell = row_locator.locator("td").nth(6)
                status_text = (await status_cell.inner_text()).strip()
            except Exception:
                status_text = ""

            print(f"  [DS] Order #{order_id} status: {status_text[:60]}")

            if "Done" in status_text or "done" in status_text.lower():
                print(f"  [DS] Order #{order_id} complete ✓")
                break
            if "Abandoned" in status_text or "Error" in status_text:
                raise RuntimeError(f"DirectSkip order #{order_id} failed: {status_text}")

            await asyncio.sleep(POLL_INTERVAL_SECS)
        else:
            raise RuntimeError(f"Timed out waiting for DirectSkip order #{order_id} after {POLL_TIMEOUT_SECS}s")

        # ── Download result ───────────────────────────────────────────────────
        print(f"  [DS] Downloading result for order #{order_id}...")
        download_link = row_locator.locator("a:has-text('Download')")
        download_href = await download_link.get_attribute("href")

        EXPORTS_DIR.mkdir(exist_ok=True)
        result_path = EXPORTS_DIR / f"directskip_result_{date.today().isoformat()}_{order_id}.csv"

        async with page.expect_download() as dl_info:
            await page.goto(f"{BASE_URL}/{download_href}")
        download = await dl_info.value
        await download.save_as(str(result_path))

        print(f"  [DS] Downloaded → {result_path}")
        await browser.close()
        return result_path


def run(headless: bool = False, dry_run: bool = False) -> None:
    """
    Full cycle: export → upload → poll → download → ingest.
    headless=False (default) keeps the browser visible so you can watch.
    """
    # 1. Generate the CSV
    print("  [DS] Generating upload CSV...")
    csv_path = export_csv(dry_run=dry_run)
    if csv_path is None:
        return  # nothing to do (dry run or no qualifying rows)

    if dry_run:
        return

    # 2. Run Playwright workflow
    result_path = asyncio.run(_run(csv_path, headless=headless))
    if result_path is None:
        print("  [DS] Upload workflow returned no result path.")
        return

    # 3. Ingest the results
    print(f"\n  [DS] Ingesting results from {result_path.name}...")
    counts = ingest(str(result_path), dry_run=False)
    print(
        f"  [DS] Ingest complete — "
        f"{counts['matched']} matched, {counts['unmatched']} unmatched, "
        f"{counts['persons']} persons, {counts['relatives']} relatives"
    )
