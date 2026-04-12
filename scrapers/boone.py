"""
scrapers/boone.py — Boone County KY
Source: https://apps6.boonecountyky.org/BCMCSalesApp
Format: Structured web app. Each listing is a labeled block with:
  Case Number, Case Name, Sale Date, Property Address, PIDN,
  Appraised at, Loan Amount, Taxes Check, Additional Info, Subject to Mortgage
Strategy: BeautifulSoup parse of labeled dt/dd or bold/field structure.
"""

from __future__ import annotations
import re
from bs4 import BeautifulSoup

from .base import empty_listing, fetch_html, normalize_date, clean_money, split_standard_address

COUNTY = "Boone"
STATE = "KY"
URL = "https://apps6.boonecountyky.org/BCMCSalesApp"


def scrape() -> list[dict]:
    html = fetch_html(URL)
    if not html:
        print(f"  [Boone] Could not fetch page.")
        return []

    listings = _parse(html)
    print(f"  [Boone] Found {len(listings)} listings.")
    return listings


def _parse(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    listings = []

    text = soup.get_text("\n")
    lines = [l.strip() for l in text.splitlines()]

    case_starts = [i for i, l in enumerate(lines) if l == "Case Number:"]

    for start_idx in case_starts:
        listing = _extract_block(lines, start_idx)
        if listing:
            listings.append(listing)

    return listings


def _extract_block(lines: list[str], start_idx: int) -> dict | None:
    """
    Extract a single listing from lines starting at the 'Case Number:' label.
    Boone's structure is:
      Case Number:
      {value}
      Case Name:
      {value}
      Sale Date:
      {value}
      Property Address:
      {value}
      PIDN:
      {value}
      Appraised at:
      {value}    Loan Amount: {value}
      Taxes Check:
      {value}
      Additional Info:
      {value}
      Subject to Mortgage:
      {value}
    """
    listing = empty_listing(COUNTY, STATE, URL)

    def get_val(label: str) -> str:
        """Find label in lines after start_idx and return next non-empty, non-label line."""
        labels = {
            "case number": "Case Number:",
            "case name": "Case Name:",
            "sale date": "Sale Date:",
            "property address": "Property Address:",
            "appraised at": "Appraised at:",
            "loan amount": "Loan Amount:",
            "additional info": "Additional Info:",
            "subject to mortgage": "Subject to Mortgage:",
            "taxes check": "Taxes Check:",
        }
        target = labels.get(label.lower(), label)
        for i in range(start_idx, min(start_idx + 40, len(lines))):
            if lines[i] == target:
                for j in range(i + 1, min(i + 5, len(lines))):
                    if lines[j] and lines[j] not in labels.values():
                        return lines[j]
        return ""

    case_num = get_val("case number")
    if not case_num:
        return None

    listing["Case Number"] = case_num

    # Parse Case Name into Plaintiff vs Defendant
    case_name = get_val("case name")
    if case_name:
        vs_match = re.search(r"\s+V\s+", case_name, re.IGNORECASE)
        if vs_match:
            listing["Plaintiff"]    = case_name[: vs_match.start()].strip().title()
            listing["Defendant(s)"] = case_name[vs_match.end() :].strip().title()
        else:
            listing["Plaintiff"] = case_name.title()

    listing["Sale Date"] = normalize_date(get_val("sale date"))

    # Split the combined address into Street / City / Zip
    raw_address = get_val("property address")
    listing["Street"], listing["City"], listing["Zip"] = split_standard_address(raw_address)

    # Appraised at and Loan Amount often appear on the same line
    # e.g. "$275,000.00    Loan Amount:$163,391.17"
    appr_line = get_val("appraised at")
    if appr_line:
        amounts = re.findall(r"\$[\d,]+\.?\d*", appr_line)
        if amounts:
            listing["Appraised Value"] = clean_money(amounts[0])

    loan_in_appr = re.search(r"Loan Amount:\s*(\$[\d,]+\.?\d*)", appr_line, re.IGNORECASE)
    if loan_in_appr:
        listing["Judgment / Loan Amount"] = clean_money(loan_in_appr.group(1))
    else:
        loan_val = get_val("loan amount")
        if loan_val:
            amounts = re.findall(r"\$[\d,]+\.?\d*", loan_val)
            if amounts:
                listing["Judgment / Loan Amount"] = clean_money(amounts[0])

    additional = get_val("additional info")
    if additional:
        listing["Attorney / Firm"] = additional.strip()

    subject = get_val("subject to mortgage")
    if subject and subject.lower() == "yes":
        listing["Notes"] = "Subject to existing mortgage"

    return listing