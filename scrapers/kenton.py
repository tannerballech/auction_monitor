"""
scrapers/kenton.py — Kenton County KY
Source: tmcsales.info/{month}-{year}.html
Format: Clean structured text with case#, parties, address, appraisal, JOS amount.
Strategy: Fetch current + next 2 months pages, parse with regex.

Return signature: (new_listings, cancellation_updates)
  new_listings        — active (non-cancelled) listings not already in the sheet
  cancellation_updates — {row_index: "Yes"} for rows newly marked cancelled
"""

from __future__ import annotations
import re
from datetime import datetime, date
from calendar import month_name

from .base import empty_listing, fetch_html, normalize_date, clean_money, split_standard_address

COUNTY = "Kenton"
STATE = "KY"
BASE_URL = "https://tmcsales.info"


def get_month_urls() -> list[str]:
    """Return URLs for the current month and next 2 months."""
    today = date.today()
    urls = []
    for offset in range(3):
        m = (today.month - 1 + offset) % 12 + 1
        y = today.year + ((today.month - 1 + offset) // 12)
        month_str = month_name[m].lower()
        urls.append(f"{BASE_URL}/{month_str}-{y}.html")
    return urls


def scrape(existing: dict | None = None) -> tuple[list[dict], dict[int, str]]:
    """
    Returns (new_listings, cancellation_updates).

    existing: {case_number: (row_index, already_cancelled)} from
              sheets_writer.get_existing_case_numbers("Kenton").
              Used to match cancelled listings against existing sheet rows.
    """
    if existing is None:
        existing = {}

    all_parsed: list[dict] = []
    seen_cases: set[str] = set()

    for url in get_month_urls():
        html = fetch_html(url)
        if not html:
            continue
        all_parsed.extend(_parse_page(html, url, seen_cases))

    # Split into active and cancelled
    new_listings: list[dict] = []
    cancellation_updates: dict[int, str] = {}

    for listing in all_parsed:
        case_num  = listing.get("Case Number", "")
        cancelled = listing.get("Cancelled", "").strip().lower() == "yes"

        if cancelled:
            if case_num in existing:
                row_index, already_cancelled = existing[case_num]
                if not already_cancelled:
                    cancellation_updates[row_index] = "Yes"
            # Never write cancelled listings as new rows
            continue

        new_listings.append(listing)

    print(
        f"  [Kenton] Found {len(new_listings)} active listing(s), "
        f"{len(cancellation_updates)} cancellation(s)."
    )
    return new_listings, cancellation_updates


def _parse_page(html: str, source_url: str, seen_cases: set) -> list[dict]:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    body_text = soup.get_text("\n")

    current_sale_date = ""
    date_pattern = re.compile(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\s*[-–]",
        re.IGNORECASE,
    )

    sections = re.split(
        r"((?:January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}\s*[-–])",
        body_text,
        flags=re.IGNORECASE,
    )

    listings = []
    i = 0
    while i < len(sections):
        section = sections[i]
        dm = date_pattern.search(section)
        if dm:
            current_sale_date = normalize_date(
                f"{dm.group(1)} {dm.group(2)}, {dm.group(3)}"
            )
            i += 1
            continue

        case_blocks = re.split(r"\n(?=\d{2}-[A-Z]{2}-\d+)", section)
        for block in case_blocks:
            block = block.strip()
            if not block:
                continue
            listing = _parse_block(block, current_sale_date, source_url)
            if listing and listing["Case Number"] not in seen_cases:
                seen_cases.add(listing["Case Number"])
                listings.append(listing)

        i += 1

    return listings


def _parse_block(block: str, sale_date: str, source_url: str) -> dict | None:
    lines = [l.strip() for l in block.strip().splitlines() if l.strip()]
    if not lines:
        return None

    listing = empty_listing(COUNTY, STATE, source_url)
    listing["Sale Date"] = sale_date

    # Line 0: case number + parties
    first_line = lines[0]
    case_match = re.match(r"(\d{2}-[A-Z]{2}-\d+)\s*(.*)", first_line)
    if not case_match:
        return None

    listing["Case Number"] = case_match.group(1)
    parties_text = case_match.group(2).strip()

    vs_match = re.search(r"\s+vs\.?\s+", parties_text, re.IGNORECASE)
    if vs_match:
        listing["Plaintiff"]    = parties_text[: vs_match.start()].strip()
        listing["Defendant(s)"] = parties_text[vs_match.end() :].strip()
    else:
        listing["Plaintiff"] = parties_text

    # Remaining lines: address, attorney/appraisal, JOS, cancelled
    for line in lines[1:]:
        # Full address line containing KY + zip
        if re.search(r",\s*KY\s*\d{5}", line) and not listing["Street"]:
            street, city, zip_code = split_standard_address(line.strip())
            listing["Street"] = street
            listing["City"]   = city
            listing["Zip"]    = zip_code
            continue

        # Appraisal
        appraisal_match = re.search(r"Appraisal\s+\$?([\d,]+)", line, re.IGNORECASE)
        if appraisal_match:
            listing["Appraised Value"] = "$" + appraisal_match.group(1)

        # JOS amount
        jos_match = re.search(r"JOS\s+(\$[\d,]+\.?\d*)", line, re.IGNORECASE)
        if jos_match:
            listing["Judgment / Loan Amount"] = clean_money(jos_match.group(1))

        # Attorney (line with phone number)
        phone_match = re.search(r"(\d{3}[-.]?\d{3}[-.]?\d{4})", line)
        if phone_match and not listing["Attorney / Firm"]:
            atty = line[: phone_match.start()].strip().rstrip("-– ").strip()
            listing["Attorney / Firm"] = atty if atty else ""

        # Cancelled
        if re.search(r"\bCancelled\b", line, re.IGNORECASE):
            listing["Cancelled"] = "Yes"

    return listing if listing["Case Number"] else None