# Eagle Creek Auction Monitor

Automatically pulls foreclosure/MC sale listings from 11 county sources
and writes them into a master Google Sheet.

---

## Quick Start

### 1. Install dependencies

```bash
cd auction_monitor
pip install -r requirements.txt
```

### 2. Set up Google Cloud credentials

You need ONE credentials.json that covers both Gmail and Sheets.

1. Go to https://console.cloud.google.com/
2. Create a new project (or use existing)
3. Enable these two APIs:
   - **Gmail API**
   - **Google Sheets API**
4. Go to **Credentials → Create Credentials → OAuth 2.0 Client ID**
5. Application type: **Desktop App**
6. Download the JSON and save it as `credentials.json` in this folder
7. On first run, a browser window will open asking you to authorize. 
   Click through — this only happens once. Two token files will be saved.

### 3. Create your Google Sheet

1. Go to Google Sheets and create a new blank spreadsheet
2. Copy the spreadsheet ID from the URL:
   `https://docs.google.com/spreadsheets/d/YOUR_ID_IS_HERE/edit`
3. Open `config.py` and paste it as `SPREADSHEET_ID`

### 4. Add your Anthropic API key

In `config.py`, set:
```python
ANTHROPIC_API_KEY = "sk-ant-..."
```

Get your key from https://console.anthropic.com/

### 5. Run it

```bash
# Full run (all sources)
python main.py

# Dry run — see what would be added without writing to Sheets
python main.py --dry-run

# Specific counties only
python main.py --county kenton boone

# Email sources only
python main.py --email-only

# Web sources only (skip Gmail)
python main.py --web-only
```

---

## Setting Up Weekly Cron (When Ready)

To run every Monday morning at 7am:

```bash
crontab -e
```

Add this line (adjust the path to your Python and project):
```
0 7 * * 1 /usr/bin/python3 /path/to/auction_monitor/main.py >> /path/to/auction_monitor/cron.log 2>&1
```

**Windows alternative (Task Scheduler):**
- Action: `python C:\path\to\auction_monitor\main.py`
- Trigger: Weekly, Monday 7:00 AM

---

## Source Map

| County | State | Source | Format | Notes |
|--------|-------|--------|--------|-------|
| Scott | KY | Gmail (kgross@carrowaylaw.com) | Email → Claude parse | — |
| Rowan | KY | Gmail (budsalyer4@gmail.com) | Email → Claude parse | — |
| Kenton | KY | tmcsales.info/{month}-{year}.html | Structured text | Most reliable |
| Boone | KY | apps6.boonecountyky.org/BCMCSalesApp | Structured web app | Very clean |
| Jessamine | KY | jessaminemc.com | PHP table | Fallback to Claude |
| Fayette | KY | faycom.info | Dynamic PHP | Often 403s — may need manual |
| Jefferson | KY | jeffcomm.org/upcoming-sales.php | Table | Fallback to Claude |
| Campbell | KY | kypublicnotice.com (LINK Kenton Reader) | Legal notices | Claude parse |
| Hamilton | OH | hamilton.sheriffsaleauction.ohio.gov | RealAuction (JS) | Claude parse |
| Jefferson | IN | legacy.sri-taxsale.com | SRI table | Fallback to Claude |
| Knox | TN | tnpublicnotice.com | Legal notices | Claude parse; non-judicial trustee sales |

---

## Google Sheet Columns

| Column | Description |
|--------|-------------|
| County | County name |
| State | State abbreviation |
| Sale Date | Date of auction (YYYY-MM-DD when parseable) |
| Case Number | Court case number |
| Plaintiff | Lender / party bringing the action |
| Defendant(s) | Property owner(s) / named parties |
| Property Address | Full street address |
| Appraised Value | Court-ordered appraisal |
| Judgment / Loan Amount | Judgment of sale or outstanding loan |
| Attorney / Firm | Plaintiff's attorney |
| Estimated Market Value | Claude's AVM estimate (populated by `--valuate`) |
| Estimated Equity | EMV minus Judgment (populated by `--valuate`) |
| Equity Signal | 🏆 Home Run / ✅ Decent / ⚠️ Tight / ❌ Underwater / ❓ Unclear |
| Cancelled | "Yes" if sale is cancelled |
| Source URL | Where the listing came from |
| Date Added | When this script added it |
| Notes | Comp summary, confidence level, caveats |

---

## Running Market Valuations (Phase 2)

After scraping, run the valuator separately:

```bash
# Valuate all upcoming listings missing a market value estimate
python main.py --valuate

# Preview without writing to Sheets
python main.py --valuate --dry-run

# Only valuate a specific county
python main.py --valuate --county boone kenton
```

**What it does for each property:**
1. Searches for the address on Zillow/Redfin/Realtor for an AVM estimate
2. Finds 3 recent comparable sales in the same neighborhood
3. Returns an estimated value with confidence level
4. Calculates equity: `(EMV - Judgment) / EMV`
5. Assigns an equity signal based on your thresholds (configurable in `config.py`)

**Equity signal thresholds** (edit in `config.py → EQUITY_THRESHOLDS`):
- 🏆 **Home Run** — 40%+ equity
- ✅ **Decent** — 25–39% equity
- ⚠️ **Tight** — 10–24% equity
- ❌ **Underwater** — below 10% or negative
- ❓ **Unclear** — no judgment amount to compare against

**Note:** Valuation only runs on upcoming sales (Sale Date ≥ today) not marked Cancelled.
Properties with no judgment/loan amount still get an EMV estimate, but equity is left blank.

---

## Troubleshooting

**Fayette keeps returning nothing:**
faycom.info blocks automated fetches. Options:
- Manually copy/paste the page text into a file, add a local file reader
- Contact the MC office to ask about an email notification

**Gmail auth isn't working:**
Delete `gmail_token.json` and re-run. A browser window will open.

**Script runs but sheet isn't updating:**
- Check `SPREADSHEET_ID` in config.py
- Make sure the Google account you authorized has edit access to the sheet

**Claude parse returning no listings:**
The page structure may have changed. Run with `--dry-run` and check what 
raw text is being sent to Claude. You may need to adjust the extractor.

---

## Roadmap

- [x] Phase 1 — Scrape all 11 county sources → Google Sheet
- [x] Phase 2 — Market value analysis (Claude + web search, equity signals)
- [ ] Phase 3 — Skip trace API integration (BatchData or DirectSkip)
- [ ] Phase 4 — Deceased owner detection + obituary research
- [ ] Phase 5 — Prop.ai lead push for qualified listings
- [ ] Phase 6 — Cron scheduling
