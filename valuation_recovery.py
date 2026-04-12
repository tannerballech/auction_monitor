"""
valuation_recovery.py
One-time script to write all valuation results from the March 17 run
into the Google Sheet using a single batchUpdate call.

Matches rows by normalizing the address (lowercase, strip punctuation/spaces)
against col G of the Auctions tab. Only writes to rows where col K is blank.

Run once:
    python valuation_recovery.py
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sheets_writer import _get_service, TAB_MAIN, SPREADSHEET_ID

# ── All results from the March 17 run ────────────────────────────────────────
# Format: "address as it appears in sheet": ("$EMV", "$Equity or ''", "signal", "notes")
# Address key is normalized below for matching.

RESULTS = {
    "2 Beverly Cir, Wilder, KY 41071":                          ("$93,000",     "",           "❓", "Confidence: high (88) | Range: $79,050–$106,950 | No debt data available"),
    "45 Burney Ln, Fort Thomas, KY 41075":                      ("$315,921",    "",           "❓", "Confidence: high (85) | Range: $268,533–$363,309 | No debt data available"),
    "707 Walnut St, Dayton, KY 41074":                          ("$199,538",    "$39,176",    "⚠️", "Confidence: high (82) | Range: $169,607–$229,469 | Debt: est. lien balance (BatchData)"),
    "234 Evergreen Ave, Southgate, KY 41071":                   ("$180,000",    "$114,416",   "🏆", "Confidence: high (80) | Range: $153,000–$207,000 | Debt: est. lien balance (BatchData)"),
    "700 Smith Hiteman Rd, Alexandria, KY 41001":               ("$447,191",    "",           "❓", "Confidence: high (85) | Range: $380,112–$514,270 | No debt data available"),
    "126 4th Ave, Dayton, KY 41074":                            ("$129,546",    "",           "❓", "Confidence: high (83) | Range: $110,114–$148,978 | No debt data available"),
    "45 17th St, Newport, KY 41071":                            ("$268,580",    "$85,571",    "✅", "Confidence: high (85) | Range: $228,293–$308,867 | Debt: est. lien balance (BatchData)"),
    "2985 Rich Rd., Morning View, KY 41063":                    ("$219,354",    "$164,884",   "🏆", "Confidence: high (85) | Range: $186,451–$252,257"),
    "901 Doe Ridge Dr., Erlanger, KY 41018":                    ("$480,915",    "$190,749",   "✅", "Confidence: high (88) | Range: $408,778–$553,052"),
    "5212 Cody Rd., Independence, KY 41051":                    ("$218,517",    "$171,336",   "🏆", "Confidence: high (85) | Range: $185,739–$251,295"),
    "4558 Feiser Rd., Ryland Heights, KY 41015":                ("$224,935",    "$84,544",    "✅", "Confidence: high (85) | Range: $191,195–$258,675"),
    "2529 Thirs Drive, Villa Hills, KY 41017":                  ("$594,267",    "$586,553",   "🏆", "Confidence: high (88) | Range: $505,127–$683,407"),
    "120 Trevor Street, Covington, KY 41011":                   ("$92,735",     "$91,899",    "🏆", "Confidence: high (82) | Range: $78,825–$106,645"),
    "4104 Rankin Drive, Erlanger, KY 41018":                    ("$234,645",    "$131,683",   "🏆", "Confidence: high (85) | Range: $199,448–$269,842"),
    "5219 Eureka Drive, Taylor Mill, KY 41015":                 ("$259,404",    "$133,308",   "🏆", "Confidence: high (85) | Range: $220,493–$298,315"),
    "2326 Dixie Highway, Ft. Mitchell, KY 41017":               ("$506,055",    "$103,721",   "⚠️", "Confidence: high (88) | Range: $430,147–$581,963"),
    "3306 Hulbert Avenue, Erlanger, KY 41018":                  ("$230,515",    "$4,618",     "❌", "Confidence: high (85) | Range: $195,938–$265,092"),
    "77 McMillan Drive, Independence, KY 41051":                ("$209,799",    "$186,906",   "🏆", "Confidence: high (85) | Range: $178,329–$241,269"),
    "3519 Mary Street, Erlanger, KY 41018":                     ("$181,766",    "$61,572",    "✅", "Confidence: high (85) | Range: $154,501–$209,031"),
    "3302 Carlisle Avenue, Covington, KY 41015":                ("$174,670",    "$93,056",    "🏆", "Confidence: high (85) | Range: $148,470–$200,870"),
    "3505 Jacqueline Drive, Erlanger, KY 41018":                ("$223,318",    "$103,124",   "🏆", "Confidence: high (85) | Range: $189,820–$256,816"),
    "8655 Lely Court, Florence, KY  41042":                     ("$360,602",    "$197,211",   "🏆", "Confidence: high (88) | Range: $306,512–$414,692"),
    "10772 Gleneagle Drive, Union, KY  41091":                  ("$259,811",    "$183,269",   "🏆", "Confidence: high (85) | Range: $220,839–$298,783"),
    "2524 Winners Post Way, Burlington, KY 41005":              ("$411,364",    "$-18,573",   "❌", "Confidence: high (88) | Range: $349,659–$473,069"),
    "1829 Waverly Drive, Florence, KY  41042":                  ("$415,829",    "$409,224",   "🏆", "Confidence: high (88) | Range: $353,455–$478,203"),
    "9 Sweetbriar Avenue, Florence, KY  41042":                 ("$239,623",    "$30,799",    "⚠️", "Confidence: high (85) | Range: $203,680–$275,566"),
    "6459 Summerfield Drive, Florence, KY 41042":               ("$189,000",    "$186,777",   "🏆", "Confidence: high (85) | Range: $160,650–$217,350"),
    "1800 Hamilton Court, Florence, KY  41042":                 ("$208,387",    "$55,588",    "✅", "Confidence: high (85) | Range: $177,129–$239,645"),
    "313 Honeysuckle Terrace, Florence, KY 41042":              ("$229,427",    "$52,971",    "⚠️", "Confidence: high (85) | Range: $195,013–$263,841"),
    "2263 Teal Briar Lane Apt. 208, Burlington, KY 41005":      ("$165,843",    "$104,009",   "🏆", "Confidence: high (85) | Range: $140,967–$190,719"),
    "3229 Feeley Road, Burlington, KY  41005":                  ("$269,494",    "$186,421",   "🏆", "Confidence: high (85) | Range: $229,070–$309,918"),
    "315 Marian Drive Louisville 40218, Louisville, KY":        ("$303,559",    "$222,169",   "🏆", "Confidence: high (88) | Range: $258,025–$349,093"),
    "3748 Red River Drive, Lexington, KY":                      ("$191,538",    "$124,920",   "🏆", "Confidence: high (85) | Range: $162,807–$220,269"),
    "804 Bennett Avenue, Lexington, KY":                        ("$135,469",    "$86,793",    "🏆", "Confidence: high (82) | Range: $115,148–$155,790"),
    "127 Locust Avenue, Lexington, KY":                         ("$167,736",    "$23,626",    "⚠️", "Confidence: high (85) | Range: $142,576–$192,896"),
    "788 Graftons Mill Lane, Lexington, KY":                    ("$306,558",    "$148,551",   "🏆", "Confidence: high (88) | Range: $260,574–$352,542"),
    "584 Cecil Way, Lexington, KY":                             ("$288,288",    "$282,728",   "🏆", "Confidence: high (85) | Range: $245,045–$331,531"),
    "520 Brook Farm Court, Lexington, KY":                      ("$254,642",    "$129,218",   "🏆", "Confidence: high (88) | Range: $216,446–$292,838"),
    "8710 Finchwood Lane, Knoxville, TN 37924":                 ("$358,859",    "$185,732",   "🏆", "Confidence: high (85) | Range: $304,030–$413,688 | Debt: est. lien balance (BatchData)"),
    "3369 Robeson Road, Sevierville, TN 37862":                 ("$1,650,000",  "$190,500",   "⚠️", "Confidence: high (88) | Range: $1,402,500–$1,897,500 | Debt: est. lien balance (BatchData)"),
    "1731 Hollister Drive, Alcoa, TN 37701":                    ("$519,877",    "$267,877",   "🏆", "Confidence: high (85) | Range: $441,895–$597,859 | Debt: est. lien balance (BatchData)"),
    "12624 Needlepoint Drive, Knoxville, TN 37934":             ("$451,585",    "$17,735",    "❌", "Confidence: high (88) | Range: $383,847–$519,323 | Debt: est. lien balance (BatchData)"),
    "6513 Lazy Creek Way, Knoxville, TN 37918":                 ("$211,797",    "$71,321",    "✅", "Confidence: high (85) | Range: $179,927–$243,667 | Debt: est. lien balance (BatchData)"),
    "1943 Warrensburg Road, Whitesburg, TN 37891":              ("$139,882",    "$5,539",     "❌", "Confidence: high (82) | Range: $118,900–$160,864"),
    "1209 Park Lane, Andersonville, TN 37705":                  ("$464,436",    "$255,735",   "🏆", "Confidence: high (85) | Range: $394,771–$534,101 | Debt: est. lien balance (BatchData)"),
    "2038 Boyd St., Knoxville, TN 37921":                       ("$158,109",    "$105,309",   "🏆", "Confidence: high (85) | Range: $134,393–$181,825 | Debt: est. lien balance (BatchData)"),
    "6513 Trousdale Rd, Knoxville, TN 37921":                   ("$364,001",    "$31,208",    "❌", "Confidence: high (88) | Range: $309,401–$418,601 | Debt: est. lien balance (BatchData)"),
    "8336 Sunset Heights Drive, Knoxville, TN 37914":           ("$352,565",    "$197,889",   "🏆", "Confidence: high (85) | Range: $299,680–$405,450 | Debt: est. lien balance (BatchData)"),
    "1816 Francis Rd, Knoxville, TN 37909":                     ("$304,386",    "$141,240",   "🏆", "Confidence: high (85) | Range: $258,728–$350,044 | Debt: est. lien balance (BatchData)"),
    "103 Canterbury Road, Oak Ridge, TN 37830":                 ("$383,863",    "$2,514",     "❌", "Confidence: high (88) | Range: $326,283–$441,443 | Debt: est. lien balance (BatchData)"),
    "1709 N Liberty Hill Rd, Morristown, TN 37814":             ("$191,651",    "$-11,726",   "❌", "Confidence: high (82) | Range: $162,903–$220,399"),
    "5847 Larch Circle, Morristown, TN 37814":                  ("$376,117",    "",           "❓", "Confidence: high (85) | Range: $319,699–$432,535 | No debt data available"),
    "121 Shady Lane, Harriman, TN 37748":                       ("$191,607",    "",           "❓", "Confidence: high (82) | Range: $162,866–$220,348 | No debt data available"),
    "4049 Roundtop Drive, Sevierville, TN 37862":               ("$1,249,000",  "$86,500",    "❌", "Confidence: high (88) | Range: $1,061,650–$1,436,350 | Debt: est. lien balance (BatchData)"),
    "1812 Wrights Ferry Rd, Knoxville, TN 37919":               ("$587,321",    "",           "❓", "Confidence: high (85) | Range: $499,223–$675,419 | No debt data available"),
    "113 West Bryn Mawr Circle, Oak Ridge, TN 37830":           ("$301,651",    "$63,395",    "⚠️", "Confidence: high (85) | Range: $256,403–$346,899 | Debt: est. lien balance (BatchData)"),
    "700 West Charles Street, Morristown, TN 37813":            ("$200,089",    "",           "❓", "Confidence: high (82) | Range: $170,076–$230,102 | No debt data available"),
    "204 McCall Rd, Maryville, TN 37804":                       ("$307,096",    "$165,745",   "🏆", "Confidence: high (85) | Range: $261,032–$353,160 | Debt: est. lien balance (BatchData)"),
    "7024 Mayfair Street, Talbott, TN 37877":                   ("$234,823",    "",           "❓", "Confidence: high (82) | Range: $199,599–$270,047 | No debt data available"),
    "941 Emory Church Rd, Knoxville, TN 37922":                 ("$2,545,416",  "$1,247,930", "🏆", "Confidence: high (88) | Range: $2,163,604–$2,927,228 | Debt: est. lien balance (BatchData)"),
    "449 Grace Hill Dr, Crossville, TN 38571":                  ("$331,985",    "",           "❓", "Confidence: high (82) | Range: $282,187–$381,783 | No debt data available"),
    "129 Shore Ln., Crossville, TN 38558":                      ("$574,770",    "$190,996",   "✅", "Confidence: high (85) | Range: $488,555–$661,000 | Debt: est. lien balance (BatchData)"),
    "125 Bent Tree Drive, Crossville, TN 38555":                ("$312,438",    "$40,544",    "⚠️", "Confidence: high (85) | Range: $265,572–$359,304 | Debt: est. lien balance (BatchData)"),
    "211 South Castle Street, Knoxville, TN 37914":             ("$338,321",    "$141,456",   "🏆", "Confidence: high (85) | Range: $287,573–$389,069 | Debt: est. lien balance (BatchData)"),
    "3505 Rocky Ridge Rd, Cosby, TN 37722":                     ("$315,103",    "$207,350",   "🏆", "Confidence: high (82) | Range: $267,838–$362,368 | Debt: est. lien balance (BatchData)"),
    "4001 Washington Pike, Knoxville, TN 37917":                ("$238,090",    "$213,090",   "🏆", "Confidence: high (85) | Range: $202,377–$273,803 | Debt: est. lien balance (BatchData)"),
    "450 HAWKEN DRIVE, WALLAND, TN 37886":                      ("$84,017",     "$-65,983",   "❌", "Confidence: high (80) | Range: $71,414–$96,620 | Debt: est. lien balance (BatchData)"),
    "2408 SOUTHVIEW DRIVE, MARYVILLE, TN 37803":                ("$240,871",    "$173,176",   "🏆", "Confidence: high (85) | Range: $204,740–$277,002 | Debt: est. lien balance (BatchData)"),
    "3409 Harvey Dr, Knoxville, TN 37918":                      ("$172,252",    "",           "❓", "Confidence: high (82) | Range: $146,414–$198,090 | No debt data available"),
    "8244 Zodiac Ln, Powell, TN 37849":                         ("$432,280",    "$95,856",    "⚠️", "Confidence: high (85) | Range: $367,438–$497,122"),
    "2404 HONEY GROVE LN, KNOXVILLE, TN 37923":                 ("$306,018",    "$238,149",   "🏆", "Confidence: high (85) | Range: $260,115–$351,921 | Debt: est. lien balance (BatchData)"),
    "3820 Shandee Ln, Morristown, TN 37814":                    ("$275,774",    "$155,969",   "🏆", "Confidence: high (85) | Range: $234,408–$317,140"),
    "5101 McAnally Circle, Morristown, TN 37814":               ("$223,637",    "$161,411",   "🏆", "Confidence: high (85) | Range: $190,091–$257,183 | Debt: est. lien balance (BatchData)"),
    "1872 Allensville Ridge, Sevierville, TN 37876":            ("$438,167",    "$285,456",   "🏆", "Confidence: high (85) | Range: $372,242–$504,092 | Debt: est. lien balance (BatchData)"),
    "7201 Stagecoach Trail, Knoxville, TN 37909":               ("$466,979",    "$116,356",   "⚠️", "Confidence: high (85) | Range: $396,932–$537,026 | Debt: est. lien balance (BatchData)"),
    "1425 Foxfire Circle, Seymour, TN 37865":                   ("$438,167",    "$285,456",   "🏆", "Confidence: high (85) | Range: $372,442–$503,892 | Debt: est. lien balance (BatchData)"),
    "232 W. Edison St., Alcoa, TN 37701":                       ("$229,971",    "$177,613",   "🏆", "Confidence: high (85) | Range: $195,475–$264,467 | Debt: est. lien balance (BatchData)"),
    "1933 Carroll Road, Morristown, TN 37813":                  ("$180,425",    "",           "❓", "Confidence: high (82) | Range: $153,361–$207,489 | No debt data available"),
    "6808 N. Ruggles Ferry Pike, Knoxville, TN 37924":          ("$216,456",    "$66,395",    "✅", "Confidence: high (85) | Range: $183,988–$248,924 | Debt: est. lien balance (BatchData)"),
    "7444 Lyle Bend Ln, Knoxville, TN 37918":                   ("$281,742",    "$23,470",    "❌", "Confidence: high (85) | Range: $239,481–$324,003 | Debt: est. lien balance (BatchData)"),
    "1732 Leconte Dr, Maryville, TN 37803":                     ("$413,647",    "$112,458",   "✅", "Confidence: high (85) | Range: $351,600–$475,694 | Debt: est. lien balance (BatchData)"),
    "3305 Gose Cove Ln, Knoxville, TN 37931":                   ("$561,570",    "$269,383",   "🏆", "Confidence: high (88) | Range: $477,335–$645,806"),
    "2010 Swarthmore Ln, Maryville, TN 37804":                  ("$274,920",    "$93,549",    "✅", "Confidence: high (85) | Range: $233,682–$316,158 | Debt: est. lien balance (BatchData)"),
    "6442 Western Ave., Knoxville, TN 37921":                   ("$352,445",    "$173,604",   "🏆", "Confidence: high (85) | Range: $299,578–$405,312 | Debt: est. lien balance (BatchData)"),
    "4861 Masters Drive, Maryville, TN 37801":                  ("$564,017",    "$111,786",   "⚠️", "Confidence: high (85) | Range: $479,414–$648,620 | Debt: est. lien balance (BatchData)"),
    "1657 Maremont Rd, Knoxville, TN 37918":                    ("$371,496",    "$9,818",     "❌", "Confidence: high (85) | Range: $315,772–$427,220 | Debt: est. lien balance (BatchData)"),
    "1411 Dick Lonas Road, Knoxville, TN 37909":                ("$305,376",    "$294,026",   "🏆", "Confidence: high (85) | Range: $259,570–$351,182 | Debt: est. lien balance (BatchData)"),
    "529 PEANUT RD, PARROTTSVILLE, TN 37843":                   ("$270,736",    "",           "❓", "Confidence: high (80) | Range: $230,126–$311,346 | No debt data available"),
    "5123 Avis Ln, Knoxville, TN 37914":                        ("$243,284",    "$51,313",    "⚠️", "Confidence: high (85) | Range: $206,791–$279,777 | Debt: est. lien balance (BatchData)"),
    "1395 BLUETS RD, NEWPORT, TN 37821":                        ("$210,251",    "",           "❓", "Confidence: high (80) | Range: $178,713–$241,789 | No debt data available"),
    "125 Garwood Lane, Maryville, TN 37803":                    ("$446,678",    "$20,158",    "❌", "Confidence: high (85) | Range: $379,676–$513,680"),
    "250 Ollis Rd, Oliver Springs, TN 37840":                   ("$355,066",    "$125,944",   "✅", "Confidence: high (82) | Range: $301,806–$408,326 | Debt: est. lien balance (BatchData)"),
    "1441 Murrell Rd, Morristown, TN 37814":                    ("$227,000",    "$79,269",    "✅", "Confidence: high (82) | Range: $192,950–$261,050"),
    "820 FORSYTHE ST, KNOXVILLE, TN 37917":                     ("$259,049",    "",           "❓", "Confidence: high (85) | Range: $220,192–$297,906 | No debt data available"),
    "1411 Mountain Hill Lane, Knoxville, TN 37931":             ("$423,126",    "$197,106",   "🏆", "Confidence: high (85) | Range: $359,657–$486,595 | Debt: est. lien balance (BatchData)"),
    "181 Bradrock St, Crossville, TN 38571":                    ("$282,704",    "$-25,629",   "❌", "Confidence: high (82) | Range: $240,298–$325,110 | Debt: est. lien balance (BatchData)"),
    "4160 Chamberlain Lane, Sevierville, TN 37862":             ("$599,999",    "$-60,001",   "❌", "Confidence: high (85) | Range: $509,999–$689,999 | Debt: est. lien balance (BatchData)"),
    "529 Confederate Drive, Knoxville, TN 37922":               ("$338,818",    "$47,890",    "⚠️", "Confidence: high (85) | Range: $287,995–$389,641 | Debt: est. lien balance (BatchData)"),
    "7043 Yellow Oak Lane, Knoxville, TN 37931":                ("$381,682",    "$249,653",   "🏆", "Confidence: high (85) | Range: $324,430–$438,934 | Debt: est. lien balance (BatchData)"),
    "7326 Lyle Bend Lane, Knoxville, TN 37918":                 ("$288,393",    "$44,911",    "⚠️", "Confidence: high (85) | Range: $245,134–$331,652 | Debt: est. lien balance (BatchData)"),
    "432 Sugarwood Dr, Farragut, TN 37934":                     ("$799,041",    "$744,343",   "🏆", "Confidence: high (88) | Range: $679,185–$918,897 | Debt: est. lien balance (BatchData)"),
    "8537 Norman Lane, Powell, TN 37849":                       ("$287,736",    "",           "❓", "Confidence: high (85) | Range: $244,576–$330,896 | No debt data available"),
    "407 Overhill Road, Knoxville, TN 37914":                   ("$332,875",    "$261,291",   "🏆", "Confidence: high (85) | Range: $282,944–$382,806 | Debt: est. lien balance (BatchData)"),
    "1707 MORNINGSIDE DR, MORRISTOWN, TN 37814":                ("$558,360",    "",           "❓", "Confidence: high (82) | Range: $474,606–$642,114 | No debt data available"),
    "10024 Rutledge Pike, Corryton, TN 37721":                  ("$291,869",    "$66,869",    "⚠️", "Confidence: high (85) | Range: $248,089–$335,649 | Debt: est. lien balance (BatchData)"),
    "4783 FRED JENNINGS RD, WALLAND, TN 37886":                 ("$181,325",    "$65,089",    "✅", "Confidence: high (80) | Range: $154,126–$208,524 | Debt: est. lien balance (BatchData)"),
    "3444 Big Springs Ridge Rd, Friendsville, TN 37737":        ("$347,488",    "$122,488",   "✅", "Confidence: high (82) | Range: $295,365–$399,611 | Debt: est. lien balance (BatchData)"),
    "424 LAKEVIEW DR, CROSSVILLE, TN 38558":                    ("$253,969",    "",           "❓", "Confidence: high (82) | Range: $215,874–$292,064 | No debt data available"),
    "1247 CLOYDS CHURCH RD, GREENBACK, TN 37742":               ("$322,726",    "$123,939",   "✅", "Confidence: high (82) | Range: $274,317–$371,135 | Debt: est. lien balance (BatchData)"),
    "115 WINTER ST, MARYVILLE, TN 37801":                       ("$193,010",    "",           "❓", "Confidence: high (82) | Range: $164,059–$221,962 | No debt data available"),
    "412 BRADLEY AVE, CINCINNATI, 45215, OH":                   ("$159,592",    "$51,256",    "✅", "Confidence: high (85) | Range: $135,653–$183,530 | Debt: est. lien balance (BatchData)"),
    "8929 CAVALIER DRIVE, CINCINNATI, 45231, OH":               ("$272,544",    "$73,050",    "✅", "Confidence: high (85) | Range: $231,662–$313,426 | Debt: est. lien balance (BatchData)"),
    "2691 LEHMAN RD. APT. B6, CINCINNATI, 45204, OH":           ("$82,115",     "$60,121",    "🏆", "Confidence: high (82) | Range: $69,798–$94,432 | Debt: est. lien balance (BatchData)"),
    "108 LEBANON ROAD, LOVELAND, 45140, OH":                    ("$269,970",    "$139,106",   "🏆", "Confidence: high (85) | Range: $229,475–$310,465 | Debt: est. lien balance (BatchData)"),
    "10885 FALLSINGTON COURT, CINCINNATI, 45242, OH":           ("$267,443",    "",           "❓", "Confidence: high (85) | Range: $227,327–$307,559 | No debt data available"),
    "6832 FOX HILL LANE, CINCINNATI, 45236, OH":                ("$695,131",    "",           "❓", "Confidence: high (88) | Range: $590,861–$799,401 | No debt data available"),
    "323 BROOKFOREST DR, CINCINNATI, 45238, OH":                ("$220,367",    "",           "❓", "Confidence: high (85) | Range: $187,312–$253,422 | No debt data available"),
    "10597 BREEDSHILL DRIVE, CINCINNATI, 45231, OH":            ("$218,566",    "$35,372",    "⚠️", "Confidence: high (85) | Range: $185,781–$251,351 | Debt: est. lien balance (BatchData)"),
    "4248 LOUBELL LANE, CINCINNATI, 45205, OH":                 ("$197,612",    "$192,929",   "🏆", "Confidence: high (85) | Range: $167,970–$227,254 | Debt: est. lien balance (BatchData)"),
}


def _normalize(s: str) -> str:
    """Lowercase, strip punctuation and extra whitespace for fuzzy matching."""
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def main():
    svc = _get_service()

    # Read current sheet
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{TAB_MAIN}!A:Q",
    ).execute()
    rows = result.get("values", [])
    print(f"Read {len(rows)} rows from sheet (including header).")

    # Build normalized address → (row_number, current_emv)
    # row_number is 1-based Sheets index
    sheet_index = {}
    for i, row in enumerate(rows[1:], start=2):
        address = row[6].strip() if len(row) > 6 else ""
        emv     = row[10].strip() if len(row) > 10 else ""
        if address:
            sheet_index[_normalize(address)] = (i, emv, address)

    # Build batchUpdate payload
    data = []
    matched = 0
    skipped_already_written = 0
    unmatched = []

    for raw_addr, (emv_str, equity_str, signal, notes) in RESULTS.items():
        key = _normalize(raw_addr)
        if key not in sheet_index:
            unmatched.append(raw_addr)
            continue

        row_num, current_emv, sheet_addr = sheet_index[key]

        if current_emv:
            print(f"  [SKIP - already written] Row {row_num}: {sheet_addr}")
            skipped_already_written += 1
            continue

        notes_out = f"[BatchData] {notes}"

        data.append({
            "range":  f"{TAB_MAIN}!K{row_num}:M{row_num}",
            "values": [[emv_str, equity_str, signal]],
        })
        data.append({
            "range":  f"{TAB_MAIN}!Q{row_num}",
            "values": [[notes_out]],
        })

        print(f"  [WRITE] Row {row_num} — {signal} {emv_str}  ({sheet_addr})")
        matched += 1

    if unmatched:
        print(f"\n  Addresses not found in sheet ({len(unmatched)}):")
        for a in unmatched:
            print(f"    {a}")

    if not data:
        print("\nNothing to write — all rows already have EMV or no matches found.")
        return

    print(f"\nWriting {matched} row(s) via single batchUpdate...")
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()

    print(f"\nDone.")
    print(f"  Written:               {matched}")
    print(f"  Already had EMV:       {skipped_already_written}")
    print(f"  Not found in sheet:    {len(unmatched)}")


if __name__ == "__main__":
    main()