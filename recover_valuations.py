#!/usr/bin/env python3
"""
recover_valuations.py  —  ONE-TIME RECOVERY SCRIPT

Writes the pre-computed valuation results from the 2026-04-02 full run
that were not saved due to the Sheets API rate limit error at row 49.

Does NOT call BatchData. Reads the sheet to find which rows still have
blank EMV (col M), matches them by street address, and writes using a
single batchUpdate call.

Usage:
    python recover_valuations.py --dry-run   # preview matches
    python recover_valuations.py             # write to sheet
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import config

# ---------------------------------------------------------------------------
# All computed results from the 2026-04-02 BatchData run.
# Key: lowercase street address  (must match col G in sheet, stripped/lowered)
# Value: dict with emv, equity, signal  (equity is "" when no debt available)
# ---------------------------------------------------------------------------
RESULTS = {
    # Clark IN
    "225 w. utica st.":          {"emv": "$321,793", "equity": "",          "signal": "❓"},
    "604 biggs rd.":             {"emv": "$237,597", "equity": "",          "signal": "❓"},
    "11527 independence way":    {"emv": "$379,606", "equity": "",          "signal": "❓"},
    "516 millwood place":        {"emv": "$296,669", "equity": "",          "signal": "❓"},
    "12122 meriwether dr.":      {"emv": "$242,422", "equity": "",          "signal": "❓"},
    "6559 ashley springs ct.":   {"emv": "$319,170", "equity": "",          "signal": "❓"},
    "1505 willow dr":            {"emv": "$280,407", "equity": "",          "signal": "❓"},
    "1208 east court ave.":      {"emv": "$163,860", "equity": "",          "signal": "❓"},
    "3517 crescent road":        {"emv": "$232,009", "equity": "",          "signal": "❓"},

    # Cumberland TN
    "1352 cook road":            {"emv": "$283,000", "equity": "",          "signal": "❓"},
    "39 braun cove":             {"emv": "$194,000", "equity": "$39,556",   "signal": "⚠️"},

    # Hamilton OH  (Apr 8)
    "1940 sundale ave":          {"emv": "$132,404", "equity": "$66,348",   "signal": "🏆"},
    "7264 swirlwood ln.":        {"emv": "$140,871", "equity": "",          "signal": "❓"},
    "3712 homelawn ave":         {"emv": "$180,312", "equity": "",          "signal": "❓"},
    "5453 starcrest dr.":        {"emv": "$270,784", "equity": "$121,569",  "signal": "🏆"},
    "843 findlay st":            {"emv": "$197,000", "equity": "$-243,000", "signal": "❌"},
    "4309 saint martins pl.":    {"emv": "$178,219", "equity": "$47,861",   "signal": "✅"},
    "6576 newbridge dr":         {"emv": "$193,099", "equity": "$50,098",   "signal": "✅"},
    "5584 biscayne ave":         {"emv": "$243,919", "equity": "$79,087",   "signal": "✅"},
    "10737 lemarie dr":          {"emv": "$271,491", "equity": "",          "signal": "❓"},
    "3636 liberty st.":          {"emv": "$225,081", "equity": "$12,704",   "signal": "❌"},
    # Hamilton OH (Apr 22)
    "261 joliet ave":            {"emv": "$208,886", "equity": "$132,994",  "signal": "🏆"},
    "1520 blair ave":            {"emv": "$333,035", "equity": "$19,857",   "signal": "❌"},
    "3586 rackacres dr":         {"emv": "$389,006", "equity": "$107,365",  "signal": "✅"},
    "5465 childs ave":           {"emv": "$244,144", "equity": "$28,012",   "signal": "⚠️"},
    "4408 glenway ave":          {"emv": "$163,168", "equity": "$17,067",   "signal": "⚠️"},
    "2222 quebec rd":            {"emv": "$163,168", "equity": "$17,067",   "signal": "⚠️"},
    "3010 ferguson rd":          {"emv": "$173,639", "equity": "$113,243",  "signal": "🏆"},
    "11953 hamden drive":        {"emv": "$236,969", "equity": "",          "signal": "❓"},
    "3056 portsmouth avenue":    {"emv": "$628,123", "equity": "$472,694",  "signal": "🏆"},
    # Hamilton OH (May 6)
    "951 patricia lane":         {"emv": "$261,945", "equity": "$125,738",  "signal": "🏆"},
    "932 glasgow dr":            {"emv": "$245,560", "equity": "$137,260",  "signal": "🏆"},
    "5001 kellogg ave #d-34":    {"emv": "$0",       "equity": "",          "signal": "❓"},
    "4680 kirby ave":            {"emv": "$194,920", "equity": "$15,030",   "signal": "❌"},
    "420 westgate dr":           {"emv": "$251,443", "equity": "$113,684",  "signal": "🏆"},
    "8707 desoto drive":         {"emv": "$103,288", "equity": "",          "signal": "❓"},
    "221 crescent ave":          {"emv": "$238,058", "equity": "$83,278",   "signal": "✅"},
    "7051 daybreak dr":          {"emv": "$499,076", "equity": "$104,631",  "signal": "⚠️"},
    "3448 moonridge dr":         {"emv": "$298,067", "equity": "$82,808",   "signal": "✅"},
    # Hamilton OH (Jun 17)
    "108 lebanon road":          {"emv": "$269,970", "equity": "$139,106",  "signal": "🏆"},

    # Roane TN
    "755 bazel road":            {"emv": "$208,505", "equity": "",          "signal": "❓"},
    "130 catherine lane":        {"emv": "$326,360", "equity": "$133,712",  "signal": "🏆"},
    "368 emory river road":      {"emv": "$305,195", "equity": "",          "signal": "❓"},
    "416 lakewood road":         {"emv": "$388,130", "equity": "",          "signal": "❓"},
    "110 vann dr.":              {"emv": "$211,802", "equity": "$52,161",   "signal": "⚠️"},

    # Floyd IN
    "1114 carriage ln":          {"emv": "$335,431", "equity": "$220,197",  "signal": "🏆"},
    "207 greendale dr":          {"emv": "$233,223", "equity": "$96,764",   "signal": "🏆"},
    "1113 hildreth st":          {"emv": "$177,429", "equity": "",          "signal": "❓"},
    "3017 julian dr":            {"emv": "$310,807", "equity": "$25,407",   "signal": "❌"},
    "3977 kepley rd":            {"emv": "$235,141", "equity": "$156,713",  "signal": "🏆"},

    # Sevier TN
    "1315 pullen road":          {"emv": "$299,000", "equity": "",          "signal": "❓"},
    "1819 cody view way":        {"emv": "$618,000", "equity": "",          "signal": "❓"},
    "2336 breezy road":          {"emv": "$649,900", "equity": "$-62,600",  "signal": "❌"},
    "904 indian gap road":       {"emv": "$234,000", "equity": "$26,227",   "signal": "⚠️"},
    "222 lazy lane":             {"emv": "$185,000", "equity": "",          "signal": "❓"},
    "2022 center road":          {"emv": "$367,000", "equity": "$127,122",  "signal": "✅"},
    "1502 black oak dr":         {"emv": "$231,000", "equity": "$10,076",   "signal": "❌"},

    # Jefferson KY (Apr 10)
    "4122 lake dreamland rd.":   {"emv": "$108,069", "equity": "$64,633",   "signal": "🏆"},
    "2231 duncan st.":           {"emv": "$180,000", "equity": "$118,629",  "signal": "🏆"},
    "2819 northwestern pkwy":    {"emv": "$76,071",  "equity": "$32,485",   "signal": "🏆"},
    "1045 so. 32nd st.":         {"emv": "$85,000",  "equity": "$44,743",   "signal": "🏆"},
    "2412 dumesnil street":      {"emv": "$187,000", "equity": "$118,813",  "signal": "🏆"},
    "4 grand avenue court":      {"emv": "$67,086",  "equity": "$58,306",   "signal": "🏆"},
    "9105 maple rd.":            {"emv": "$560,000", "equity": "$555,849",  "signal": "🏆"},
    "7076 wildwood circle, apt. 174": {"emv": "$113,947", "equity": "$107,672", "signal": "🏆"},
    "2419 ralph avenue":         {"emv": "$189,675", "equity": "$62,263",   "signal": "✅"},
    "902 loretto avenue":        {"emv": "$71,952",  "equity": "$-67,696",  "signal": "❌"},
    "7920 barbour manor dr.":    {"emv": "$402,009", "equity": "$12,467",   "signal": "❌"},
    "823 winkler avenue":        {"emv": "$150,840", "equity": "$-1,197",   "signal": "❌"},
    "317 freeman ave":           {"emv": "$136,129", "equity": "$38,565",   "signal": "✅"},
    "2901 montana avenue":       {"emv": "$193,514", "equity": "$80,277",   "signal": "🏆"},
    "9709 polaris drive":        {"emv": "$236,341", "equity": "$46,178",   "signal": "⚠️"},
    "3419 grand avenue":         {"emv": "$63,337",  "equity": "$-57,568",  "signal": "❌"},
    "428 north 34th street":     {"emv": "$108,589", "equity": "$20,566",   "signal": "⚠️"},
    "4914 bluebird avenue":      {"emv": "$206,563", "equity": "$91,158",   "signal": "🏆"},
    "10202 fairmount road":      {"emv": "$435,900", "equity": "$120,181",  "signal": "✅"},
    "10204 fairmount road":      {"emv": "$159,000", "equity": "$-156,719", "signal": "❌"},
    "4416 wilmoth avenue":       {"emv": "$161,262", "equity": "$63,671",   "signal": "✅"},
    "9711 brooks bend road":     {"emv": "$269,423", "equity": "$34,134",   "signal": "⚠️"},
    "4409 naneen drive":         {"emv": "$250,038", "equity": "$87,363",   "signal": "✅"},
    "12603 westport ridge way":  {"emv": "$163,386", "equity": "$102,555",  "signal": "🏆"},
    "7806 edna m rd.":           {"emv": "$168,388", "equity": "$131,313",  "signal": "🏆"},
    "5209 firwood lane":         {"emv": "$260,500", "equity": "$44,084",   "signal": "⚠️"},
    "1611 pershing avenue":      {"emv": "$275,456", "equity": "$42,951",   "signal": "⚠️"},
    "2631 saint xavier street":  {"emv": "$67,474",  "equity": "$-13,156",  "signal": "❌"},
    "3627 kahlert avenue":       {"emv": "$138,663", "equity": "$62,551",   "signal": "🏆"},
    "2317 mary catherine drive": {"emv": "$158,166", "equity": "$22,271",   "signal": "⚠️"},
    "4153 roosevelt avenue":     {"emv": "$241,480", "equity": "$-81,856",  "signal": "❌"},
    "1206 fairdale rd.":         {"emv": "$192,260", "equity": "$554",      "signal": "❌"},
    "3606 stacy court":          {"emv": "$319,642", "equity": "$3,786",    "signal": "❌"},
    "124 claremont avenue":      {"emv": "$337,091", "equity": "$63,017",   "signal": "⚠️"},
    "3213 furman boulevard":     {"emv": "$270,013", "equity": "$113,215",  "signal": "🏆"},
    "4721 cane run road":        {"emv": "$219,739", "equity": "$-6,643",   "signal": "❌"},
    "1121 lone oak avenue":      {"emv": "$202,238", "equity": "$88,798",   "signal": "🏆"},
    # Jefferson KY (Apr 24)
    "520 winkler ave.":          {"emv": "$131,000", "equity": "$97,984",   "signal": "🏆"},
    "2120 pirtle st.":           {"emv": "$87,074",  "equity": "$64,325",   "signal": "🏆"},
    "224 shawnee dr.":           {"emv": "$226,369", "equity": "$201,590",  "signal": "🏆"},
    "2404 w. madison st.":       {"emv": "$53,430",  "equity": "$31,022",   "signal": "🏆"},
    "1322 s. 32nd st.":          {"emv": "$74,523",  "equity": "$27,059",   "signal": "✅"},
    "2100 west burnett avenue":  {"emv": "$140,515", "equity": "$86,390",   "signal": "🏆"},
    "1769 west hill street":     {"emv": "$197,000", "equity": "$164,597",  "signal": "🏆"},
    "1771 west hill street":     {"emv": "$63,807",  "equity": "$-53,850",  "signal": "❌"},
    "419 rosewood court":        {"emv": "$193,241", "equity": "$185,703",  "signal": "🏆"},
    "1617 haskin avenue":        {"emv": "$135,077", "equity": "$129,059",  "signal": "🏆"},
    "6600 holly lake drive":     {"emv": "$227,736", "equity": "$149,559",  "signal": "🏆"},
    "839 south 37th st":         {"emv": "$98,581",  "equity": "$-32,984",  "signal": "❌"},
    "5814 dellrose dr.":         {"emv": "$198,377", "equity": "$104,790",  "signal": "🏆"},
    "6712 seminole avenue":      {"emv": "$351,995", "equity": "$2,078",    "signal": "❌"},
    "2201 date st.":             {"emv": "$83,130",  "equity": "$39,028",   "signal": "🏆"},
    "1301 crosstimbers drive":   {"emv": "$496,776", "equity": "$303,097",  "signal": "🏆"},
    "6436 clover trace cir.":    {"emv": "$254,276", "equity": "$-12,117",  "signal": "❌"},
    "660 s. 22nd street":        {"emv": "$90,216",  "equity": "$217",      "signal": "❌"},
    "212 hampton place court":   {"emv": "$172,992", "equity": "$46,282",   "signal": "✅"},
    "6105 erica way":            {"emv": "$275,886", "equity": "$27,085",   "signal": "❌"},
    "336 east southside court":  {"emv": "$169,412", "equity": "$83,276",   "signal": "🏆"},
    "2605 butler road":          {"emv": "$160,633", "equity": "$112,302",  "signal": "🏆"},
    "4616 andalusia lane":       {"emv": "$250,322", "equity": "$106,489",  "signal": "🏆"},
    "2221 west kentucky street": {"emv": "$173,568", "equity": "$-19,267",  "signal": "❌"},
    "2501 emma katherine lane":  {"emv": "$214,599", "equity": "$115,092",  "signal": "🏆"},
    "6702 barbrook road":        {"emv": "$164,645", "equity": "$-13,375",  "signal": "❌"},
    "4716 cliff avenue":         {"emv": "$149,387", "equity": "$111,664",  "signal": "🏆"},
    "1717 meadowgate lane":      {"emv": "$309,382", "equity": "$130,027",  "signal": "🏆"},
    "1006 meadow hill road":     {"emv": "$197,937", "equity": "$69,370",   "signal": "✅"},
    "105 south 46th street":     {"emv": "$140,356", "equity": "$3,491",    "signal": "❌"},
    "5904 gloria lane":          {"emv": "$222,339", "equity": "$-93,198",  "signal": "❌"},
    # Jefferson KY (May 15)
    "1400 beech st.":            {"emv": "$108,559", "equity": "$89,237",   "signal": "🏆"},
    "2819 rodman st.":           {"emv": "$107,155", "equity": "$87,967",   "signal": "🏆"},
    "2221 osage ave.":           {"emv": "$80,349",  "equity": "$63,320",   "signal": "🏆"},
    "837 louis coleman jr. drive": {"emv": "$68,060","equity": "$21,469",   "signal": "✅"},
    "1785 bolling avenue":       {"emv": "$49,738",  "equity": "$37,619",   "signal": "🏆"},
    "951 east oak street":       {"emv": "$287,000", "equity": "$269,849",  "signal": "🏆"},
    "212 north 21st street":     {"emv": "$72,457",  "equity": "$45,985",   "signal": "🏆"},
    "5401 crosstree place":      {"emv": "$169,541", "equity": "$162,575",  "signal": "🏆"},
    "126 w. garrett street":     {"emv": "$115,118", "equity": "$109,543",  "signal": "🏆"},
    "6710 morning star way":     {"emv": "$219,923", "equity": "$216,717",  "signal": "🏆"},
    "7316 brook meadow drive":   {"emv": "$325,822", "equity": "$23,653",   "signal": "❌"},
    "303 babe drive":            {"emv": "$430,858", "equity": "$115,068",  "signal": "✅"},
    "4959 winding spring circle":{"emv": "$245,372", "equity": "$24,225",   "signal": "❌"},
    "12300 saratoga view ct.":   {"emv": "$533,475", "equity": "$175,733",  "signal": "✅"},
    "3640 vermont avenue":       {"emv": "$101,287", "equity": "$52,894",   "signal": "🏆"},
    "2118 ronnie ave":           {"emv": "$208,401", "equity": "$116,480",  "signal": "🏆"},
    "7412 terry road":           {"emv": "$205,386", "equity": "$17,763",   "signal": "❌"},
    "3024 hartlage ct.":         {"emv": "$218,253", "equity": "$-5,319",   "signal": "❌"},
    "2910 explorer drive":       {"emv": "$207,865", "equity": "$143,117",  "signal": "🏆"},
    "4811 lawrie lane":          {"emv": "$233,102", "equity": "$101,990",  "signal": "🏆"},
    "4617 peachtree avenue":     {"emv": "$192,053", "equity": "$100,638",  "signal": "🏆"},
    "7309 chestnut tree lane":   {"emv": "$277,746", "equity": "$47,619",   "signal": "⚠️"},
    "1531 garland avenue":       {"emv": "$134,310", "equity": "$36,586",   "signal": "✅"},
    "214 iroquois ave.":         {"emv": "$157,495", "equity": "$75,887",   "signal": "🏆"},
    "3905 poplar level road":    {"emv": "$222,339", "equity": "$-93,198",  "signal": "❌"},
    "1709 washington blvd":      {"emv": "$203,788", "equity": "$16,575",   "signal": "❌"},
    "6315 tioga rd.":            {"emv": "$309,363", "equity": "$146,894",  "signal": "🏆"},
    "8303 smithton rd.":         {"emv": "$296,277", "equity": "$20,975",   "signal": "❌"},
    "11508 leemont dr.":         {"emv": "$225,702", "equity": "$140,866",  "signal": "🏆"},
    "2014 kendall lane":         {"emv": "$246,921", "equity": "$20,383",   "signal": "❌"},
    "2637 landor avenue, unit 3g": {"emv": "$112,599","equity": "$57,355",  "signal": "🏆"},
    "3812 garfield avenue":      {"emv": "$163,807", "equity": "$-27,424",  "signal": "❌"},
    "9312 pinto court":          {"emv": "$269,459", "equity": "$72,468",   "signal": "✅"},
    "3719 parker avenue":        {"emv": "$99,085",  "equity": "$57,179",   "signal": "🏆"},
    "2116 peabody lane":         {"emv": "$218,594", "equity": "$70,527",   "signal": "✅"},

    # Campbell KY
    "114 rossford ave":          {"emv": "$314,789", "equity": "$236,922",  "signal": "🏆"},
    "904 columbia st":           {"emv": "$201,668", "equity": "$155,508",  "signal": "🏆"},

    # Boone KY
    "7775 cedar wood circle":    {"emv": "$266,048", "equity": "$121,868",  "signal": "🏆"},
    "1051 spectacular bid drive":{"emv": "$585,227", "equity": "$575,272",  "signal": "🏆"},
    "18 lynn street":            {"emv": "$217,453", "equity": "$112,281",  "signal": "🏆"},
    "1930 blk gun club road":    {"emv": "$228,981", "equity": "$225,245",  "signal": "🏆"},

    # Jessamine KY
    "100 lindsey drive":         {"emv": "$232,955", "equity": "$97,822",   "signal": "🏆"},
    "1689 marble creek lane":    {"emv": "$192,966", "equity": "$140,822",  "signal": "🏆"},
    "125 christopher drive":     {"emv": "$230,706", "equity": "$48,358",   "signal": "⚠️"},
    "124 courtney drive":        {"emv": "$266,725", "equity": "$173,555",  "signal": "🏆"},

    # Knox TN
    "3906 luwana lane":          {"emv": "$249,169", "equity": "$-32,081",  "signal": "❌"},
    "1942 tree tops ln":         {"emv": "$847,210", "equity": "$619,233",  "signal": "🏆"},
    "5615 thorngrove pike":      {"emv": "$364,554", "equity": "$311,367",  "signal": "🏆"},
    "600 c idlewood ln":         {"emv": "$236,003", "equity": "$211,016",  "signal": "🏆"},
    "1104 plyley street":        {"emv": "$215,367", "equity": "$162,109",  "signal": "🏆"},
    "1025 morrell road":         {"emv": "$492,741", "equity": "$83,022",   "signal": "⚠️"},
    "316 strader road":          {"emv": "$282,000", "equity": "$216,000",  "signal": "🏆"},
    "429 chickamauga avenue":    {"emv": "$229,207", "equity": "",          "signal": "❓"},
    "8318 schroeder rd":         {"emv": "$288,883", "equity": "",          "signal": "❓"},
    "1946 clove ln":             {"emv": "$221,630", "equity": "$65,961",   "signal": "✅"},
    "5813 penshurst ct":         {"emv": "$512,718", "equity": "$327,741",  "signal": "🏆"},
    "7944 whitcomb road":        {"emv": "$365,894", "equity": "$83,149",   "signal": "⚠️"},
    "1411 cunningham road west": {"emv": "$249,000", "equity": "",          "signal": "❓"},

    # Hamblen TN
    "2660 mountain view drive":  {"emv": "$283,000", "equity": "$156,571",  "signal": "🏆"},
    "2273 belmont drive":        {"emv": "$290,000", "equity": "",          "signal": "❓"},

    # Blount TN
    "910 loblolly ln":           {"emv": "$502,282", "equity": "$146,665",  "signal": "✅"},
    "228 laurel valley road":    {"emv": "$279,191", "equity": "$90,169",   "signal": "✅"},
    "1659 middlesettlements road":{"emv":"$339,882", "equity": "$235,982",  "signal": "🏆"},
    "425 bayberry terrace":      {"emv": "$318,685", "equity": "",          "signal": "❓"},
    "125 garwood lane":          {"emv": "$454,425", "equity": "$27,905",   "signal": "❌"},

    # Anderson TN
    "1933 mountain road":        {"emv": "$298,000", "equity": "$42,215",   "signal": "⚠️"},
    "113 bermuda rd.":           {"emv": "$270,000", "equity": "$51,307",   "signal": "⚠️"},
    "318 east drive":            {"emv": "$231,000", "equity": "$8,037",    "signal": "❌"},

    # Jefferson TN
    "824 pleasant view drive":   {"emv": "$298,120", "equity": "$29,219",   "signal": "❌"},
    "1310 tabitha dr.":          {"emv": "$272,000", "equity": "$31,362",   "signal": "⚠️"},

    # Loudon TN
    "1228 buford court":         {"emv": "$343,010", "equity": "$217,489",  "signal": "🏆"},

    # Cocke TN
    "680 hale brook road":       {"emv": "$295,000", "equity": "",          "signal": "❓"},
    "1805 mountain ridge road":  {"emv": "$151,871", "equity": "$87,115",   "signal": "🏆"},
}


# ---------------------------------------------------------------------------
# Sheet config
# ---------------------------------------------------------------------------
SPREADSHEET_ID = config.SPREADSHEET_ID
TAB_MAIN  = "Auctions"
SCOPES    = ["https://www.googleapis.com/auth/spreadsheets"]
TOKEN     = "sheets_token.json"
COL_STREET    = 6   # G
COL_EMV       = 12  # M
COL_EQUITY    = 13  # N
COL_SIGNAL    = 14  # O


def _get_service():
    creds = None
    if os.path.exists(TOKEN):
        creds = Credentials.from_authorized_user_file(TOKEN, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN, "w") as f:
            f.write(creds.to_json())
    return build("sheets", "v4", credentials=creds)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    svc = _get_service()

    # Read current sheet
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{TAB_MAIN}!A:S",
    ).execute()
    rows = result.get("values", [])

    print(f"Read {len(rows)-1} data rows from sheet.")

    data    = []
    matched = 0
    skipped_already_written = 0
    skipped_no_match = 0

    for i, row in enumerate(rows[1:], start=2):
        # Skip if EMV already written
        emv_val = row[COL_EMV].strip() if len(row) > COL_EMV else ""
        if emv_val:
            skipped_already_written += 1
            continue

        street = row[COL_STREET].strip() if len(row) > COL_STREET else ""
        key    = street.lower()

        if key not in RESULTS:
            skipped_no_match += 1
            if street:
                print(f"  [NO MATCH] Row {i}: {street!r}")
            continue

        res = RESULTS[key]
        emv    = res["emv"]
        equity = res["equity"]
        signal = res["signal"]

        # Skip placeholder $0 entry (5001 KELLOGG AVE unit — BatchData returned nothing)
        if emv == "$0":
            skipped_no_match += 1
            continue

        if args.dry_run:
            print(f"  [WOULD WRITE] Row {i}: {street} → {signal} {emv}")
        else:
            data.append({
                "range":  f"{TAB_MAIN}!M{i}:O{i}",
                "values": [[emv, equity, signal]],
            })
            print(f"  [QUEUED] Row {i}: {street} → {signal} {emv}")
        matched += 1

    print(f"\nMatched: {matched} | Already written: {skipped_already_written} | No match: {skipped_no_match}")

    if args.dry_run:
        print("\nDry run — nothing written.")
        return

    if not data:
        print("Nothing to write.")
        return

    print(f"\nWriting {matched} rows in single batchUpdate...")
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()
    print(f"Done. {matched} rows written.")


if __name__ == "__main__":
    main()