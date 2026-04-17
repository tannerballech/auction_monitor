"""
recover_skiptrace.py
One-time recovery script: writes the 184 skip trace results from the
April 14 2026 run that fetched successfully but failed to write due to
WinError 10053 (connection aborted on large batchUpdate payload).

Recovers: Owner Name (Primary/Secondary), Phones, Deceased, Skip Trace Date.
Email and Mailing Address are NOT recoverable from console output — those
columns will remain blank for this batch and can be filled by a re-run
once the chunked batchUpdate fix is in place.

Run once from project root:
    python recover_skiptrace.py
    python recover_skiptrace.py --dry-run
"""

import argparse
from datetime import date

# ---------------------------------------------------------------------------
# All 184 results parsed from the April 14 console output
# Format: (street_lower, city_lower, owner_primary, owner_secondary, phones, deceased)
# Phones is a comma-separated string; "" means no phones found
# ---------------------------------------------------------------------------

RESULTS = [
    # Clark, IN
    ("604 biggs rd.",      "memphis",     "Brandon E Swan",          "", "(502) 551-8601",                                    "No"),
    ("11527 independence way", "sellersburg", "Aaron Wayne Reel",    "", "(502) 689-1274, (502) 295-1292",                    "No"),
    ("516 millwood place", "clarksville", "Michaela A Parrish",      "", "(502) 644-8405, (502) 819-3982, (812) 941-8528",    "No"),
    ("6559 ashley springs ct.", "charlestown", "Brettny Jordan Buckner", "", "",                                              "No"),
    ("3517 crescent road", "charlestown", "Richard Joseph Armstrong", "", "",                                                  "Yes"),
    # Hamilton, OH
    ("261 joliet ave",     "cincinnati",  "Christopher J Chambers",  "", "(513) 602-4153",                                    "No"),
    ("3010 ferguson rd",   "cincinnati",  "Tammie T Govan",          "", "(513) 344-6004, (513) 389-2464",                    "No"),
    ("11953 hamden drive", "cincinnati",  "Clifford R Kenney",       "", "",                                                  "Yes"),
    ("951 patricia lane",  "cincinnati",  "Tristian David Wright",   "", "(513) 557-9065",                                    "No"),
    ("420 westgate dr",    "cleves",      "Ryan A Phelps",           "", "(832) 406-6860, (832) 372-2434",                    "No"),
    ("8707 desoto drive",  "cincinnati",  "James B Wilder",          "", "(513) 519-4446, (513) 914-5805",                    "No"),
    ("3448 moonridge dr",  "cincinnati",  "Jayson Steven Essell",    "", "(513) 574-1198",                                    "No"),
    # Jessamine, KY
    ("100 lindsey drive",  "nicholasville", "Mitchell M Cole",       "", "(859) 536-6993",                                    "No"),
    ("1689 marble creek lane", "nicholasville", "Ronald W Loman",    "", "(859) 885-8502",                                    "No"),
    # Jefferson, KY
    ("1617 haskin avenue", "louisville",  "Joann H Estes",           "", "",                                                  "No"),
    ("5401 crosstree place", "louisville", "Sheila K Emerson",       "", "(502) 905-3952",                                    "No"),
    ("126 w. garrett street", "louisville", "Charles Adcock",        "", "",                                                  "No"),
    # Blount, TN
    ("425 bayberry terrace", "maryville", "Daniel Van Vanzandt",     "", "(865) 268-5605",                                    "No"),
    # Franklin, KY
    ("309 birch drive",    "frankfort",   "Kellie Renee Black",      "", "(502) 319-3726",                                    "No"),
    # Knox, TN
    ("941 emory church rd", "knoxville",  "James Edward Mckinnon",   "", "",                                                  "No"),
    # Kenton, KY
    ("4358 siffel court",  "covington",   "Marjorie Siffel",         "", "",                                                  "Yes"),
    # Jefferson, KY (cont)
    ("303 babe drive",     "fairdale",    "Johnny Ray Ross",         "", "(910) 467-7382",                                    "No"),
    ("2118 ronnie ave",    "louisville",  "Linda J Haines",          "", "",                                                  "No"),
    # Kenton, KY
    ("739 winston hill drive", "taylor mill", "Jerry W Meget",       "", "",                                                  "Yes"),
    # Hamblen, TN
    ("5847 larch circle",  "morristown",  "Thomas J Withers",        "", "",                                                  "No"),
    ("2660 mountain view drive", "morristown", "Derrick Jimmie Hamilton", "", "",                                             "No"),
    # Knox, TN
    ("8336 sunset heights drive", "knoxville", "William Ryan Morris", "", "(865) 712-4421, (615) 439-5489",                  "No"),
    ("1411 cunningham road west", "seymour",  "Samuel A Webb",        "", "",                                                 "Yes"),
    # Blount, TN
    ("126 johnson mountain way", "townsend", "Mary Jon Clark",        "", "",                                                 "No"),
    # Cocke, TN
    ("680 hale brook road", "newport",    "John H Belda",            "", "",                                                  "Yes"),
    # Knox, TN
    ("1816 francis rd",    "knoxville",   "Rhonda Rene Seagraves",   "", "",                                                  "No"),
    # Sevier, TN
    ("222 lazy lane",      "pigeon forge", "James R Cavin",          "", "",                                                  "Yes"),
    # Knox, TN
    ("1806 bradshaw garden", "knoxville", "Jack Willard Jones",      "", "(865) 689-8576",                                    "No"),
    # Jefferson, KY
    ("1301 crosstimbers drive", "louisville", "John Travis Hinkebein", "", "(502) 767-3429, (502) 495-6117",                  "No"),
    # Blount, TN
    ("1731 hollister drive", "alcoa",     "Fariborz M Bzorgi",       "", "",                                                  "No"),
    # Hamblen, TN
    ("5101 mcanally circle", "morristown", "James Odell Bullard",    "", "(423) 258-1314",                                    "No"),
    # Loudon, TN
    ("1228 buford court",  "greenback",   "Kathy W Parson",          "", "",                                                  "No"),
    # Campbell, KY
    ("114 rossford ave",   "fort thomas", "Nancy A Smith",           "", "(859) 760-3733, (859) 261-7653, (859) 441-0025",   "No"),
    # Knox, TN
    ("6442 western ave.",  "knoxville",   "Colton M Norrod",         "", "(865) 210-2218",                                    "No"),
    # Kenton, KY
    ("32 park avenue",     "elsmere",     "Raymond Goans",           "", "(513) 242-0673",                                    "No"),
    # Jefferson, KY
    ("214 iroquois ave.",  "louisville",  "Ring Ping",               "", "",                                                  "No"),
    ("212 hampton place court", "louisville", "Kathy L Cooks",       "", "(502) 290-0552",                                    "No"),
    ("2910 explorer drive", "louisville", "Karin A Spurling",        "", "(502) 298-2653, (502) 495-2424",                    "Yes"),
    ("1400 beech st.",     "louisville",  "Theresa Mary Adkins",     "", "",                                                  "No"),
    ("2819 rodman st.",    "louisville",  "Sarah L King",            "", "",                                                  "No"),
    ("2221 osage ave.",    "louisville",  "Danielle Monique Bowen",  "", "(502) 718-0536",                                    "No"),
    ("5814 dellrose dr.",  "louisville",  "Kirsten M Gibson",        "", "",                                                  "No"),
    ("12300 saratoga view ct.", "louisville", "Mustafa Al Obaidi",   "", "(502) 309-0164",                                    "No"),
    ("6315 tioga rd.",     "louisville",  "James Durrell Knox",      "", "(502) 776-3767",                                    "No"),
    # Cumberland, TN
    ("449 grace hill dr",  "crossville",  "Paul Lee Maynard",        "", "",                                                  "No"),
    # Knox, TN
    ("5813 penshurst ct",  "powell",      "Andrew D May",            "", "(865) 809-6766, (865) 249-6956",                    "No"),
    # Blount, TN
    ("3813 valentine road", "maryville",  "Tamara S Cruze",          "", "",                                                  "No"),
    ("2408 southview drive", "maryville", "Jarett Hopson",           "", "(865) 564-2898, (865) 973-5221, (865) 385-2971",   "No"),
    # Knox, TN
    ("5615 thorngrove pike", "knoxville", "Lee F Monday",            "", "",                                                  "No"),
    ("1946 clove ln",      "knoxville",   "Cody J Fritz",            "", "(865) 228-1398",                                    "No"),
    # Roane, TN
    ("368 emory river road", "harriman",  "Charles F Kaldenbach",    "", "(865) 882-2527, (865) 234-7071",                    "No"),
    # Hamblen, TN
    ("1516 lakeview cir",  "morristown",  "Teresa R Kimbrough",      "", "(865) 978-5423",                                    "No"),
    # Jefferson, KY
    ("6710 morning star way", "louisville", "Richard W W Elpers",    "", "",                                                  "No"),
    ("6600 holly lake drive", "louisville", "Mary J Edelen",         "", "(502) 375-8755",                                    "No"),
    ("1531 garland avenue", "louisville", "Sharon Renee Bachelor",   "", "(301) 996-7315, (910) 523-8310, (301) 262-8235",   "No"),
    ("1006 meadow hill road", "louisville", "Larry A Cramer",        "", "",                                                  "No"),
    ("2637 landor avenue",  "louisville", "Cynthia Gutierrez",       "", "",                                                  "No"),
    ("3719 parker avenue", "louisville",  "Elnora T Tyus",           "", "(502) 785-4066",                                    "No"),
    ("2116 peabody lane",  "louisville",  "Rosa M Duran Castro",     "", "",                                                  "No"),
    # Franklin, KY
    ("348 green fields lane", "frankfort", "Kayte C Shaw",           "", "",                                                  "No"),
    # Jefferson, KY
    ("4616 andalusia lane", "louisville", "Jacob T Wheat",           "", "",                                                  "No"),
    ("1769 west hill street", "louisville", "Christopher P Furlow",  "", "",                                                  "No"),
    ("837 louis coleman jr. drive", "louisville", "Jason S Weaver",  "", "(502) 408-1414, (502) 714-1959",                   "No"),
    ("1785 bolling avenue", "louisville", "Edward Shaun Wilson",     "", "(502) 618-9345, (678) 365-8184",                   "No"),
    ("951 east oak street", "louisville", "Roland M Schuyler",       "", "",                                                  "No"),
    ("212 north 21st street", "louisville", "Regina A Bell",         "", "(502) 851-9448",                                    "No"),
    # Campbell, KY
    ("904 columbia st",    "newport",     "Nolan H Rechtin",         "", "",                                                  "No"),
    # Knox, TN
    ("211 south castle street", "knoxville", "Pamela A Hall",        "", "(615) 445-3082",                                    "No"),
    ("7711 cooper meadows lane", "knoxville", "Evan James Byrd",     "", "",                                                  "No"),
    # Anderson, TN
    ("111 cumberland view ests", "rocky top", "Brandy Leeann Goodman", "", "(865) 426-9251",                                  "No"),
    # Knox, TN
    ("227 e morelia ave",  "knoxville",   "Eric A Perry",            "", "",                                                  "No"),
    ("118 west moody avenue", "knoxville", "Mohammed A Hossain",     "", "(865) 573-5757",                                    "No"),
    ("7619 windwood dr",   "powell",      "Randall B Ross",          "", "(865) 207-2077",                                    "No"),
    ("2545 seaton avenue", "knoxville",   "Lanceford Earl Sexton",   "", "",                                                  "No"),
    ("7619 windwood drive", "powell",     "Randall B Ross",          "", "(865) 207-2077",                                    "No"),
    ("5708 boones creek lane", "knoxville", "Carrie Lynn Taylor",    "", "",                                                  "No"),
    # Davidson, TN
    ("1101 harpeth mill court", "nashville", "Levry Sisk",           "", "(615) 557-0116",                                    "No"),
    # Marion, TN
    ("2571 valley view hwy", "jasper",    "Cindy Lorriane Condra",   "", "",                                                  "No"),
    # McMinn, TN
    ("404 georgia ave",    "etowah",      "Teddy R Mealor",          "", "",                                                  "No"),
    # Davidson, TN
    ("304 sarver ave",     "madison",     "Virgil L Sherrod",        "", "",                                                  "Yes"),
    ("212 town park drive", "nashville",  "Apolinar Silva Chavez",   "", "",                                                  "No"),
    # Wilson, TN
    ("200 posey hill rd",  "mount juliet", "Connie Louise Bradley",  "", "(615) 641-5165, (615) 758-3728",                   "No"),
    # Marion, TN
    ("20816 river canyon road", "chattanooga", "David Franklin Hawkins", "", "",                                              "No"),
    # Sumner, TN
    ("144 raindrop lane",  "hendersonville", "Tommy Buchanan",       "", "(812) 262-1579",                                    "No"),
    # Davidson, TN
    ("565 mill station drive", "nashville", "Antonio D Mccray",      "", "",                                                  "No"),
    ("1951 graceland drive", "goodlettsville", "Iris Garcia",        "", "(615) 582-3855",                                    "No"),
    ("5124 southfork boulevard", "old hickory", "Grisel Moguel",     "", "(312) 479-1095",                                    "No"),
    ("3779 pin hook road", "antioch",     "Lanitra A Oats",          "", "(618) 319-1931, (618) 490-1173",                   "No"),
    ("914 delray drive",   "nashville",   "Jonathan Kyle Austin",    "", "",                                                  "No"),
    # Knox, TN
    ("8710 finchwood lane", "knoxville",  "Angie Sauceman",          "", "(865) 963-5557",                                    "No"),
    # Davidson, TN
    ("420 shoreline circle", "antioch",   "Deandra L Nelson",        "", "(615) 582-8475",                                    "No"),
    # Wilson, TN
    ("300 tyne blvd",      "old hickory", "Jessica T Hart",          "", "(615) 920-5788",                                    "No"),
    # Rutherford, TN
    ("4804 chelanie circle", "murfreesboro", "Nicholas Antonio Wilcox", "", "",                                               "No"),
    # Davidson, TN
    ("515 emerald ct",     "nashville",   "Shelton Wardell Cammon",  "", "(305) 979-7143, (615) 474-6431, (615) 649-8373",   "No"),
    ("535 amquiwood court", "madison",    "Miranda C Lager",         "", "(615) 579-7240, (818) 846-4738",                   "No"),
    ("4864 peppertree drive", "antioch",  "James D Sampson",         "", "",                                                  "No"),
    # Sumner, TN
    ("111 longboat court", "gallatin",    "Jeffrey Todd Abner",      "", "",                                                  "No"),
    # Rutherford, TN
    ("1313 fall parkway",  "murfreesboro", "Ethan T Czereda",        "", "",                                                  "No"),
    # Anderson, TN
    ("117 parsons road",   "oak ridge",   "Lucia A Kelly",           "", "",                                                  "No"),
    # Rutherford, TN
    ("180 center street",  "lavergne",    "Destinie Laquittia Akins", "", "(615) 471-1962",                                   "No"),
    # Williamson, TN
    ("1311 moher boulevard", "franklin",  "Kathy Renee Wells",       "", "(615) 807-1750",                                    "No"),
    # Knox, TN
    ("1411 dick lonas road", "knoxville", "Odese Fasha Cummings",    "", "(865) 978-7244, (865) 773-8018",                   "No"),
    # Davidson, TN
    ("817 joseph avenue",  "nashville",   "Gertrude L Collier",      "", "",                                                  "No"),
    ("7524 woodstream dr", "nashville",   "Jack Edward Cornett",     "", "",                                                  "No"),
    # Bradley, TN
    ("3843 woodcrest circle nw", "cleveland", "Ronald Henry",        "", "",                                                  "No"),
    # Davidson, TN
    ("4417 saunders ave",  "nashville",   "Douglas Jay Lipsey",      "", "(713) 585-2973",                                    "No"),
    # McMinn, TN
    ("158 county road 315", "sweetwater", "Missy Pilkey",            "", "",                                                  "No"),
    # Knox, TN
    ("1812 wrights ferry rd", "knoxville", "Thomas Raymond Coates",  "", "(423) 693-8795, (865) 694-0810",                   "No"),
    # Sumner, TN
    ("281 e morris drive", "gallatin",    "Terry G Manfred",         "", "(615) 461-8305",                                    "No"),
    ("105 b thurman kepley rd", "portland", "Amanda Kay Webb",       "", "",                                                  "No"),
    # Davidson, TN
    ("424 jessie dr",      "nashville",   "Geraldine Woodson Sawyers", "", "(615) 612-4579",                                  "No"),
    ("239 s downs circle", "goodlettsville", "Graceshous Grashyia Rose Shearon", "", "(609) 315-3821",                       "No"),
    # Wilson, TN
    ("315 matterhorn dr",  "old hickory", "James Smith",             "", "",                                                  "No"),
    # Rutherford, TN
    ("2539 patricia cir",  "murfreesboro", "Charitta Shanchetz Roberts", "", "(281) 785-0421",                               "No"),
    # McMinn, TN
    ("268 county rd 778",  "athens",      "Mel Rowland",             "", "(423) 263-1323",                                    "No"),
    # Wilson, TN
    ("401 eastland ave",   "lebanon",     "Jaime P Montalvo",        "", "",                                                  "No"),
    # Knox, TN
    ("1116 roswell rd",    "knoxville",   "Martha Elizabeth Clay",   "", "",                                                  "No"),
    # Williamson, TN
    ("5951 pine wood rd",  "franklin",    "Jason Alexander Jerkins", "", "",                                                  "No"),
    # Davidson, TN
    ("2869 creekbend dr",  "nashville",   "Mark B Norris",           "", "(615) 255-5554",                                    "No"),
    # Wilson, TN
    ("1381 walnut hill road", "lebanon",  "John Christopher Jacobs", "", "(615) 962-3535, (615) 668-2548, (931) 668-2548",   "No"),
    # Sumner, TN
    ("221 mansfield drive", "gallatin",   "Kierstin R Williams",     "", "",                                                  "No"),
    # Davidson, TN
    ("5740 stone brook dr", "brentwood",  "Maria B Proia",           "", "",                                                  "No"),
    # Blount, TN
    ("358 telford st",     "alcoa",       "Robert G Vaughn",         "", "",                                                  "No"),
    # Knox, TN
    ("8023 johnson vista", "knoxville",   "Raymond J Freed",         "", "",                                                  "No"),
    # Davidson, TN
    ("3005 brightwood avenue", "nashville", "Norma F Hale",          "", "",                                                  "No"),
    # Rutherford, TN
    ("10028 roanoke drive", "murfreesboro", "Ashley E Stripling",    "", "",                                                  "No"),
    # McMinn, TN
    ("3111 sanders road",  "athens",      "Roger Davis",             "", "",                                                  "No"),
    # Blount, TN
    ("1425 foxfire circle", "seymour",    "John W Dittus",           "", "",                                                  "No"),
    # Knox, TN
    ("8328 lucas lane",    "powell",      "Roberto Cervantes-Ruelas", "", "(717) 321-0292, (717) 677-4487",                  "No"),
    # Bradley, TN
    ("321 shady hollow circle southeast", "cleveland", "Caleb C Parker", "", "(423) 472-1183",                               "No"),
    # Rutherford, TN
    ("318 valley forge court", "la vergne", "Chanz M Farmer",        "", "(615) 480-8228",                                   "No"),
    # Knox, TN
    ("638 witherspoon lane", "knoxville", "Bryan G Hoang",           "", "(408) 250-9691, (512) 712-4094",                   "No"),
    # Davidson, TN
    ("116 brookfield avenue", "nashville", "Kevin Austin Henderson", "", "",                                                  "No"),
    ("548 brewer drive",   "nashville",   "Donald Wheeler",          "", "(615) 833-2696",                                    "Yes"),
    # Sumner, TN
    ("1018 notting hill drive", "gallatin", "Juliet B Moss",         "", "(615) 584-2947",                                    "No"),
    # Williamson, TN
    ("5111 cornwall drive", "brentwood",  "Jeffrey P Andrews",       "", "(615) 579-5074",                                    "No"),
    # Bradley, TN
    ("274 old harrison trail", "mcdonald", "Robert P Bienvenu",      "", "(423) 331-3690",                                    "No"),
    # Davidson, TN
    ("4282 brick church pike", "whites creek", "Shammah Construction Group Llc", "", "(615) 275-6139",                       "No"),
    ("4608 dakota avenue", "nashville",   "Heather Bohn",            "", "",                                                  "No"),
    ("1815 9th ave. n.",   "nashville",   "Kelaus Gresham",          "", "",                                                  "No"),
    # Rutherford, TN
    ("4873 bradyville pk.", "murfreesboro", "Sergio Alberto Romero", "", "(706) 761-0406, (706) 617-7105, (470) 253-8803, (706) 610-8228", "No"),
    # Davidson, TN
    ("141 antler ridge circle", "nashville", "Krista Leann Stooksbury", "", "",                                               "No"),
    # Rutherford, TN
    ("525 e. college street", "murfreesboro", "Lynn R Clayton",      "", "",                                                  "No"),
    # Bradley, TN
    ("695 van davis rd. nw", "georgetown", "Whitney N Davis",        "", "(423) 760-8693",                                    "No"),
    # Davidson, TN
    ("1040 mulberry way",  "nashville",   "Meshelda Ann Thompson",   "", "(615) 474-0698",                                    "No"),
    # McMinn, TN
    ("1095 county road 750", "athens",    "Joy Ann Mapp",            "", "(423) 829-7011",                                    "No"),
    # Knox, TN
    ("5713 gabuory lane",  "knoxville",   "Patricia L Colbert",      "", "",                                                  "No"),
    # Blount, TN
    ("343 headrick view drive", "maryville", "Angela Anita Hicks",   "", "",                                                  "No"),
    # Knox, TN
    ("328 wardley road",   "knoxville",   "David Keith Yeary",       "", "(865) 202-3995, (865) 309-2223",                   "No"),
    # Davidson, TN
    ("3801 dunbar dr",     "nashville",   "Audrey Clementine Walls", "", "(615) 649-8390",                                    "No"),
    # Blount, TN
    ("2010 swarthmore ln", "maryville",   "Heather N Moates",        "", "",                                                  "No"),
    ("1942 tree tops ln",  "seymour",     "Eric J Yopp",             "", "(865) 774-6665",                                    "No"),
    ("1228 buford court",  "greenback",   "Kathy W Parson",          "", "",                                                  "No"),
    # Davidson, TN
    ("6921 somerset farms cir", "nashville", "Douglas Lance Vanarsdale", "", "",                                              "No"),
    # Hamilton, OH (new batch)
    ("2730 queenswood dr", "cincinnati",  "Louis B Schulte",         "", "(513) 404-9273, (513) 979-4051",                   "No"),
    ("11066 donora lane",  "cincinnati",  "Israel Perez Leon",       "", "",                                                  "No"),
    ("3458 statewood dr",  "cincinnati",  "Patti Bolan",             "", "(832) 414-0699, (513) 238-8604",                   "No"),
    ("1480 larann ln",     "cincinnati",  "Djuan Dante Murray",      "", "",                                                  "No"),
    ("6442 home city ave", "cincinnati",  "Amanda M Hughes",         "", "",                                                  "No"),
    # Williamson, TN
    ("459 franklin rd",    "franklin",    "Charles J Fuller",        "", "",                                                  "Yes"),
    # Marion, TN
    ("433 hass rd",        "jasper",      "Helen J Wittry",          "", "",                                                  "No"),
    # Sumner, TN
    ("736 north palmers chapel road", "white house", "Christopher Jared Michael", "", "",                                    "No"),
    # McMinn, TN
    ("2336 county road 750", "calhoun",   "Regina Joann Starkey",    "", "(907) 729-3250",                                    "No"),
    # Blount, TN (duplicate address, same owner — both rows need updating)
    ("1942 tree tops ln",  "seymour",     "Eric J Yopp",             "", "(865) 774-6665",                                    "No"),
    # Knox, TN
    ("941 emory church rd", "knoxville",  "James Edward Mckinnon",   "", "",                                                  "No"),
    # Rutherford, TN
    ("2618 ritz ln",       "murfreesboro", "John Claud Law",         "", "",                                                  "No"),
    # Davidson, TN
    ("4119 grays pt rd",   "joelton",     "Virtie Patricia Estes",   "", "(615) 268-9649",                                    "No"),
    ("55 benzing rd",      "antioch",     "William B Jordan",        "", "(615) 293-7453",                                    "No"),
    ("1101 white mountain ln", "antioch", "Kamadi S Camp",           "", "",                                                  "No"),
    ("3316 calais cir",    "antioch",     "Hermelando Corona Corona", "", "",                                                  "No"),
    # Blount, TN
    ("249 main road",      "maryville",   "Janice R Frank",          "", "",                                                  "No"),
    # Sumner, TN
    ("4002 william mack ln", "portland",  "Dina R De Oliveira Soares", "", "",                                               "No"),
    # Marion, TN
    ("456 paul hackworth rd", "whitwell", "Laura Roya Allahiari",    "", "(301) 932-8136, (410) 835-2940",                   "No"),
    # Davidson, TN
    ("4564 south trace boulevard", "old hickory", "Marlon Angelo Woods", "", "(912) 638-3376, (615) 352-3720",               "No"),
    # Bradley, TN
    ("1610 13th st. se",   "cleveland",   "Patrick J Thomas",        "", "(423) 464-0374",                                    "No"),
    # Davidson, TN
    ("4413 stoneview dr",  "antioch",     "Anataence Anatole",       "", "(615) 478-6734",                                    "No"),
    # McMinn, TN
    ("401 e. arrell st.",  "niota",       "James Rufus Decker",      "", "",                                                  "No"),
    # Wilson, TN
    ("456 weeping elm rd.", "mount juliet", "Lynn C Powell",         "", "",                                                  "No"),
    # Davidson, TN
    ("3400 panorama drive", "nashville",  "James Edwin Ball",        "", "(615) 891-4037",                                    "No"),
]


# ---------------------------------------------------------------------------
# Write to sheet
# ---------------------------------------------------------------------------

def main(dry_run: bool = False):
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    from sheets_writer import _get_service, TAB_MAIN, SPREADSHEET_ID

    today = date.today().isoformat()
    print(f"Recovery script — writing {len(RESULTS)} skip trace results")
    print(f"Dry run: {dry_run}\n")

    # Read the sheet to build street_num → row_index lookup
    print("Reading sheet...")
    svc = _get_service()
    result = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{TAB_MAIN}!A:Z",
    ).execute()
    rows = result.get("values", [])
    print(f"  {len(rows)} rows in sheet")

    # Build lookup: street_number_lower → list of (row_idx, street_lower, city_lower)
    # Col G (index 6) = Street, Col H (index 7) = City, Col Z (index 25) = Skip Trace Date
    import re
    def street_num(s):
        m = re.match(r"^(\d+)", s.strip())
        return m.group(1) if m else ""

    sheet_index: dict[str, list[tuple]] = {}
    for i, row in enumerate(rows[1:], start=2):
        if len(row) < 7:
            continue
        st = row[6].strip().lower() if len(row) > 6 else ""
        ci = row[7].strip().lower() if len(row) > 7 else ""
        already_traced = row[25].strip() if len(row) > 25 else ""
        sn = street_num(st)
        if sn:
            sheet_index.setdefault(sn, []).append((i, st, ci, already_traced))

    # Match results to row indices
    matched = []
    unmatched = []
    skipped_already_traced = []

    for (st_lower, ci_lower, owner1, owner2, phones, deceased) in RESULTS:
        sn = street_num(st_lower)
        candidates = sheet_index.get(sn, [])

        hit = None
        for (row_idx, sheet_st, sheet_ci, already_traced) in candidates:
            # Match on street number + first word of street name
            st_word = re.sub(r"^\d+\s*", "", st_lower).split()
            sh_word = re.sub(r"^\d+\s*", "", sheet_st).split()
            if st_word and sh_word and st_word[0] == sh_word[0]:
                hit = (row_idx, already_traced)
                break

        if hit is None:
            unmatched.append(st_lower)
            continue

        row_idx, already_traced = hit
        if already_traced:
            skipped_already_traced.append((st_lower, row_idx))
            continue

        matched.append((row_idx, owner1, owner2, phones, deceased))

    print(f"\nMatched:               {len(matched)}")
    print(f"Already traced:        {len(skipped_already_traced)}")
    print(f"Unmatched (no row):    {len(unmatched)}")
    if unmatched:
        for s in unmatched[:10]:
            print(f"  - {s}")

    if dry_run:
        print(f"\n[DRY RUN] Would write {len(matched)} rows. Exiting.")
        return

    if not matched:
        print("Nothing to write.")
        return

    # Build update payload
    # Columns: T=Owner Primary, U=Owner Secondary, V=Phones, W=Email, X=Mailing, Y=Deceased, Z=Date
    data = []
    for (row_idx, owner1, owner2, phones, deceased) in matched:
        data.append({"range": f"{TAB_MAIN}!T{row_idx}", "values": [[owner1]]})
        data.append({"range": f"{TAB_MAIN}!U{row_idx}", "values": [[owner2]]})
        data.append({"range": f"{TAB_MAIN}!V{row_idx}", "values": [[phones]]})
        data.append({"range": f"{TAB_MAIN}!W{row_idx}", "values": [[""]]})      # email — not in console
        data.append({"range": f"{TAB_MAIN}!X{row_idx}", "values": [[""]]})      # mailing — not in console
        data.append({"range": f"{TAB_MAIN}!Y{row_idx}", "values": [[deceased]]})
        data.append({"range": f"{TAB_MAIN}!Z{row_idx}", "values": [[today]]})

    # Write in chunks of 200 range entries (~28 rows) to avoid WinError 10053
    CHUNK_SIZE = 200
    total_chunks = (len(data) + CHUNK_SIZE - 1) // CHUNK_SIZE
    written = 0

    print(f"\nWriting {len(data)} range entries in {total_chunks} chunk(s)...")
    for i in range(0, len(data), CHUNK_SIZE):
        chunk = data[i:i + CHUNK_SIZE]
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": chunk},
        ).execute()
        written += len(chunk) // 7   # 7 columns per row
        print(f"  Chunk {i // CHUNK_SIZE + 1}/{total_chunks}: {len(chunk)} entries written")

    print(f"\nDone. {written} row(s) updated.")
    print("Note: Email and Mailing Address columns left blank (not in console output).")
    print("Re-run --skiptrace after the chunked fix is applied to fill those fields.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(dry_run=args.dry_run)