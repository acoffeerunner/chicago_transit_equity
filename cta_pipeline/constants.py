"""Constants used throughout the CTA pipeline."""

import re

# File paths - Raw data (output from fetch scripts)
RAW_DATA_DIR_BSKY = "data/posts/bsky"
RAW_DATA_DIR_REDDIT = "data/posts/reddit"
POSTS_PATH_BSKY = "data/posts/bsky/bsky_posts.csv"
COMMENTS_PATH_BSKY = "data/posts/bsky/bsky_comments.csv"
POSTS_PATH_REDDIT = "data/posts/reddit/reddit_posts.csv"
COMMENTS_PATH_REDDIT = "data/posts/reddit/reddit_comments.csv"

# File paths - Processed data (output from pipeline)
OUTPUT_DIR_BSKY = "data/processed/bsky"
OUTPUT_DIR_REDDIT = "data/processed/reddit"
GTFS_STOPS_PATH = "data/gtfs/stops.txt"

# Reddit subreddits to fetch from
REDDIT_SUBREDDITS = [
    "Chicago",
    "AskChicago",
    "CarFreeChicago",
    "AskCHI",
    "cta",
    "ChicagoUrbanism",
    "WindyCity",
    "ChicagoNWSide",
    "greatNWSide",
]

# Bluesky search queries
BLUESKY_QUERIES = [
    "cta AND train",
    "cta AND bus",
    "cta AND line",
    "chicago AND train",
    "chicago AND bus",
    "chicago AND line",
]

# Batch processing
DEFAULT_BATCH_SIZE = 128

# Thread detection constants
CONTINUATION_MARKERS = [
    "Also",
    "And",
    "Plus",
    "Another thing",
    "On top of that",
    "Not to mention",
    "Additionally",
    "Furthermore",
    "Oh and",
    "Same with",
    "Speaking of",
    "Related:",
]

THREAD_SIMILARITY_HIGH = 0.5
THREAD_SIMILARITY_MODERATE = 0.3
THREAD_TIME_GAP_SECONDS = 180

# Continuation marker pattern for thread detection
CONTINUATION_PATTERN = re.compile(
    r"^(" + "|".join([re.escape(m) for m in CONTINUATION_MARKERS]) + r")\b",
    re.IGNORECASE,
)

# GTFS stop data - unambiguous train stations (40 unique names)
UNAMBIGUOUS_TRAIN = {
    "35th/archer",
    "54th/cermak",
    "69th",
    "79th",
    "87th",
    "95th/dan ryan",
    "addison",
    "ashland/63rd",
    "belmont",
    "bryn mawr",
    "cermak-chinatown",
    "cottage grove",
    "davis",
    "dempster",
    "dempster-skokie",
    "francisco",
    "granville",
    "harlem",
    "harlem/lake",
    "kimball",
    "lasalle/van buren",
    "linden",
    "logan square",
    "loyola",
    "merchandise mart",
    "midway",
    "montrose",
    "noyes",
    "o'hare",
    "ohare",
    "oakton-skokie",
    "paulina",
    "quincy",
    "rockwell",
    "rosemont",
    "sheridan",
    "southport",
    "sox-35th",
    "thorndale",
    "uic-halsted",
    "jefferson park",
}

# Ambiguous stations (require transit context - 35 names)
AMBIGUOUS_TRAIN = {
    "chicago",
    "washington",
    "lake",
    "jackson",
    "madison",
    "clark",
    "state",
    "adams",
    "monroe",
    "grand",
    "division",
    "austin",
    "western",
    "california",
    "central",
    "kedzie",
    "pulaski",
    "cicero",
    "kostner",
    "fullerton",
    "wilson",
    "irving",
    "diversey",
    "armitage",
    "lawrence",
    "morse",
    "jarvis",
    "howard",
    "north",
    "halsted",
    "ashland",
    "racine",
    "damen",
    "polk",
    "clybourn",
}

# Context patterns for ambiguous station names
STATION_CONTEXT_PATTERNS = [
    r"\bat\s+({station})\b",
    r"\b({station})\s+station\b",
    r"\b({station})\s+stop\b",
    r"\b({station})\s+platform\b",
    r"\b(red|blue|green|brown|orange|purple|pink|yellow)\s+(?:line\s+)?(?:at|to|from)\s+({station})\b",
    r"\btransfer\s+(?:at|to)\s+({station})\b",
]

# User intersection pattern: "State and Lake" → normalize to intersection
USER_INTERSECTION_PATTERN = re.compile(
    r"\b(?:at\s+|near\s+|to\s+|from\s+)?"
    r"([a-z0-9]+(?:th|st|nd|rd)?)\s+(?:and|&|/)\s+([a-z0-9]+(?:th|st|nd|rd)?)\b",
    re.IGNORECASE,
)

# Sarcasm detection patterns
SARCASM_PATTERNS = [
    r"\bdamn i love\b",
    r"\bgotta love\b.{0,30}\b(cta|train|bus)\b",
    r"\bnothing like\b.{0,30}\b(smoke|delay|late|stuck)\b",
    r"\bthanks\s+cta\b",
    r"\b(love|loving|great|wonderful|amazing)\b.{0,50}\b(smoke|smoking|delay|delayed|late|dirty|disgusting|stuck|broken)\b",
]

# Time of day keywords
MORNING_KEYWORDS = ["morning", "breakfast", "am commute", "sunrise", "early"]
AFTERNOON_KEYWORDS = ["afternoon", "lunch", "noon", "midday"]
EVENING_KEYWORDS = ["evening", "dinner", "sunset"]
NIGHT_KEYWORDS = ["night", "late", "midnight", "after dark", "pm commute"]

# Route extraction constants
BUS_REGEX = re.compile(r"\b(\d{1,3})\s*(bus|route)\b", re.IGNORECASE)

BUS_ROUTES = [
    "bus_1",
    "bus_2",
    "bus_3",
    "bus_4",
    "bus_X4",
    "bus_N5",
    "bus_6",
    "bus_7",
    "bus_8",
    "bus_8A",
    "bus_9",
    "bus_X9",
    "bus_11",
    "bus_12",
    "bus_J14",
    "bus_15",
    "bus_18",
    "bus_20",
    "bus_21",
    "bus_22",
    "bus_24",
    "bus_26",
    "bus_28",
    "bus_29",
    "bus_31",
    "bus_30",
    "bus_34",
    "bus_35",
    "bus_36",
    "bus_37",
    "bus_39",
    "bus_43",
    "bus_44",
    "bus_47",
    "bus_48",
    "bus_49",
    "bus_49B",
    "bus_X49",
    "bus_50",
    "bus_51",
    "bus_52",
    "bus_52A",
    "bus_53",
    "bus_53A",
    "bus_54",
    "bus_54A",
    "bus_54B",
    "bus_55",
    "bus_55A",
    "bus_55N",
    "bus_56",
    "bus_57",
    "bus_59",
    "bus_60",
    "bus_62",
    "bus_62H",
    "bus_63",
    "bus_63W",
    "bus_65",
    "bus_66",
    "bus_67",
    "bus_68",
    "bus_70",
    "bus_71",
    "bus_72",
    "bus_73",
    "bus_74",
    "bus_75",
    "bus_76",
    "bus_77",
    "bus_78",
    "bus_79",
    "bus_80",
    "bus_81",
    "bus_81W",
    "bus_82",
    "bus_84",
    "bus_95",
    "bus_85",
    "bus_85A",
    "bus_86",
    "bus_87",
    "bus_88",
    "bus_90",
    "bus_91",
    "bus_92",
    "bus_93",
    "bus_94",
    "bus_96",
    "bus_97",
    "bus_100",
    "bus_103",
    "bus_106",
    "bus_108",
    "bus_111",
    "bus_111A",
    "bus_112",
    "bus_115",
    "bus_119",
    "bus_120",
    "bus_121",
    "bus_124",
    "bus_125",
    "bus_126",
    "bus_134",
    "bus_135",
    "bus_136",
    "bus_143",
    "bus_146",
    "bus_147",
    "bus_148",
    "bus_151",
    "bus_152",
    "bus_155",
    "bus_156",
    "bus_157",
    "bus_165",
    "bus_169",
    "bus_171",
    "bus_172",
    "bus_192",
    "bus_201",
    "bus_206",
]
BUS_ROUTES = [bus_route.lower() for bus_route in BUS_ROUTES]

LINE_NAMES = ["red", "blue", "green", "orange", "brown", "purple", "pink", "yellow"]

# Pattern 1: "X line"
SINGLE_LINE_PATTERN = re.compile(
    r"\b(" + "|".join(LINE_NAMES) + r")\s+lines?\b", re.IGNORECASE
)

# Pattern 2: "the X at [station]" or "the X to [station]"
LINE_AT_STATION_PATTERN = re.compile(
    r"\bthe\s+(" + "|".join(LINE_NAMES) + r")\s+(?:at|to|from)\b", re.IGNORECASE
)

# Pattern 3: "[Color] to [Color]" (transfers)
LINE_TRANSFER_PATTERN = re.compile(
    r"\b(" + "|".join(LINE_NAMES) + r")\s+to\s+(" + "|".join(LINE_NAMES) + r")\b",
    re.IGNORECASE,
)

# Pattern 4: "caught/take/took/ride the [Color]"
LINE_VERB_PATTERN = re.compile(
    r"\b(?:caught|catch|take|took|ride|riding|rode|on)\s+the\s+("
    + "|".join(LINE_NAMES)
    + r")\b",
    re.IGNORECASE,
)

# Pattern 5: "[Color] train" (without "line")
LINE_TRAIN_PATTERN = re.compile(
    r"\b(" + "|".join(LINE_NAMES) + r")\s+(?:train|trains)\b", re.IGNORECASE
)

# Pattern 6: for multiple line mentions
LINE_LIST_PATTERN = re.compile(
    r"\b("
    + "|".join(LINE_NAMES)
    + r")(?:\s*(?:,|and|&)\s*("
    + "|".join(LINE_NAMES)
    + r"))+\s*lines?\b",
    re.IGNORECASE,
)

# Pattern for multiple bus mentions
BUS_LIST_PATTERN = re.compile(
    r"\bbuse?s?\s+([\d]+[A-Za-z]?)(?:\s*(?:,|and|&)\s*([\d]+[A-Za-z]?))+\b",
    re.IGNORECASE,
)

# Pattern for single bus mentions
SINGLE_BUS_PATTERN = re.compile(
    r"""
    \bbus\s+(\d{1,3}[A-Za-z]?)\b |           # "bus 66"
    \b(\d{1,3}[A-Za-z]?)\s+bus\b |           # "66 bus"
    \broute\s+(\d{1,3}[A-Za-z]?)\b |         # "route 66"
    \b(\d{1,3}[A-Za-z]?)\s+route\b           # "66 route"
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Pattern for bus mentions in form "take/catch/wait for the #66"
# Fixed: require "the" for ride/rode/riding to avoid false positives like "ride #1" (first ride)
HASHTAG_VERB_PATTERN = re.compile(
    r"\b(?:take|took|catch|caught|wait|waiting|missed|miss)\s+(?:the\s+)?#(\d{1,3}[A-Za-z]?)\b"
    r"|\b(?:rode|ride|riding)\s+the\s+#(\d{1,3}[A-Za-z]?)\b",
    re.IGNORECASE,
)

# Pattern for bus mentions in form "the #156 is/was/arrives"
HASHTAG_THE_PATTERN = re.compile(
    r"\bthe\s+#(\d{1,3}[A-Za-z]?)\s+(?:is|was|isn't|wasn't|arrives?|arrived|comes?|came|runs?|ran|takes?|took|should|will|won't|doesn't|didn't)\b",
    re.IGNORECASE,
)

# Pattern for bus mentions in forms "a 156 scheduled" or "the 156 scheduled"
BUS_SCHEDULED_PATTERN = re.compile(
    r"\b(?:a|the|now\s+a)\s+(\d{1,3}[A-Za-z]?)\s+(?:scheduled|arriving|coming|running|due|expected|departed|leaving)\b",
    re.IGNORECASE,
)

# Transit classification constants
TRANSIT_KEYWORDS = [
    "cta",
    "train",
    "bus",
    "station",
    "subway",
    "the l",
    "the el",
    "l train",
    "red line",
    "blue line",
    "green line",
    "orange line",
    "brown line",
    "purple line",
    "pink line",
    "yellow line",
]

TRANSIT_PATTERN = re.compile(
    r"\b(" + "|".join([re.escape(kw) for kw in TRANSIT_KEYWORDS]) + r")\b",
    re.IGNORECASE,
)

TRANSIT_ANCHORS = [
    "CTA train experience",
    "Chicago bus commute",
    "riding the L train",
    "waiting at the train station",
    "taking public transit in Chicago",
    "delayed on the red line",
    "the bus was late",
    "my train commute",
]

NON_TRANSIT_ANCHORS = [
    "trans rights are human rights",
    "transgender community",
    "LGBTQ pride",
    "I love this city",
    "Chicago is beautiful",
    "general opinion about life",
    "political statement",
    "Comparing CTA and WMATA",
    "Comparing CTA and MTA",
    "Comparing CTA and MBTA",
]

# Semantic similarity thresholds
SEM_THRESHOLD = 0.375
SEM_MARGIN = 0.05

# Grounding keywords for transit semantic classification
TRANSIT_GROUNDING_KEYWORDS = {
    "cta",
    "train",
    "bus",
    "station",
    "subway",
    "the l",
    "the el",
    "l train",
    "el train",
    "red line",
    "blue line",
    "green line",
    "orange line",
    "brown line",
    "purple line",
    "pink line",
    "yellow line",
}

# Feedback classification constants
FEEDBACK_ANCHORS = [
    "I had a problem with the CTA train",
    "The bus was delayed and caused issues",
    "My commute experience was frustrating",
    "The train was dirty or unsafe",
    "Train station was dirty or unsafe",
    "Train station services or elevators are not working",
    "My CTA ride was positive or negative",
    "Service was slow, late, crowded, or broken",
    "I am giving an opinion about CTA service",
    "Delays due to line disruptions",
    "Late to work",
    "Late to meeting",
    "Train stopped",
    "Bus stopped",
    "better than driving",
    "I love taking the train",
    "I love taking the bus",
    "I love the Red Line",
    "I love the Blue Line",
    "I love the Brown Line",
    "I love the Green Line",
    "I love the Orange Line",
    "I love the Pink Line",
    "I love the Purple Line",
    "I love the Yellow Line",
    "I hate taking the train",
    "I hate taking the bus",
    "I hate the Red Line",
    "I hate the Blue Line",
    "I hate the Brown Line",
    "I hate the Green Line",
    "I hate the Orange Line",
    "I hate the Pink Line",
    "I hate the Purple Line",
    "I hate the Yellow Line",
    "The CTA train is faster and easier than driving",
    "Taking the train is more convenient than a taxi or Uber",
    "I prefer CTA because it is faster and cheaper than driving",
    "The CTA train is slower than driving",
    "Taking the train is less convenient than a taxi or Uber",
    "I prefer driving because it is faster than taking the train",
    "I prefer driving because it is faster than taking the bus",
    "I like the new train station",
    "The new station looks great",
    "The renovated station is nice",
    "I'm impressed by the station upgrades",
    "The accessibility features are good",
    "The station feels modern and clean",
    "I'm done with CTA",
    "I give up on CTA",
    "CTA is unreliable",
    "I can't depend on CTA anymore",
    "This is the last straw with CTA",
    "CTA has failed me again",
    "I'm so frustrated with the transit system",
    "Someone was doing drugs on the train",
    "I saw something illegal on the train",
    "There was a fight on the bus",
    "I felt unsafe on CTA train",
    "I felt unsafe on CTA bus",
    "Witnessed crime on public transit",
    "People smoking on the train",
    "Drug use on the CTA",
    "The bus never came",
    "The bus skipped my stop",
    "Bus passed without stopping",
    "Ghost bus on the tracker",
    "The bus doesn't run on schedule",
    "CTA isn't running the bus route",
    "The tracker said a bus was coming but it never showed",
    "Bus just sat there and didn't move",
    "Everyone on the train was friendly",
    "Had a great experience on CTA today",
    "The train ride was amazing",
    "People on the bus were nice",
    "Good vibes on the train",
    "CTA was actually pleasant today",
    "The bus stop signs are outdated",
    "The station needs updating",
    "CTA signage is wrong",
    "The information at the stop is incorrect",
    "The schedule posted is wrong",
    "Bus stop has no shelter",
    "Waited forever for the bus",
    "The next train isn't for 15 minutes",
    "Long wait between trains",
    "Had to wait too long for the bus",
    "Frequency is terrible",
    "Trains don't come often enough",
]

NONFEEDBACK_ANCHORS = [
    # News/announcements
    "CTA announces new service changes",
    "CTA is introducing a new bus network",
    "New routes coming to Chicago transit",
    "Service alert for the Brown Line",
    "CTA station opened on this date",
    "The new station is now open to the public",
    "Construction completed on the Red Line",
    "Official CTA press release",
    "CTA service update announcement",
    # Greetings/generic
    "Thank you for riding CTA",
    "Welcome aboard the train",
    "Next stop downtown",
    # Stories/nostalgia (not service feedback)
    "I remember when the train used to",
    "One time on the train something funny happened",
    "Back in the day the CTA was different",
    "A story about riding the train",
    # Games/media/hypothetical
    "Train simulator game",
    "Video game about driving trains",
    "I want to play a CTA simulation",
    "Watching a video about trains",
    # Questions/information seeking
    "Does the Brown Line go to the airport",
    "What time does the last train run",
    "How do I get to downtown from here",
    "Is the Red Line running today",
    "When does the bus come",
    "Which train goes to the airport",
    "How much is the fare",
    # Events/activities near transit
    "Event happening near the station",
    "Meet me at the Brown Line stop",
    "The parade is along the train route",
    "Fun fact about the CTA",
    "TIL the CTA runs a train that runs on brown and orange lines",
    "Today I learned that the CTA runs a train that runs on brown and orange lines",
    "Comparing CTA and WMATA",
    "Comparing CTA and MTA",
    "Comparing CTA and MBTA",
    "Comparing WMATA and CTA",
    "Comparing MTA and CTA",
    "Comparing MBTA and CTA",
    "The CTA is better than WMATA",
    "The WMATA is better than CTA",
    "The CTA is worse than WMATA",
    "The WMATA is worse than CTA",
    "The CTA is better than MTA",
    "The MTA is better than CTA",
    "The CTA is worse than MTA",
    "The MTA is worse than CTA",
    "The CTA is better than MBTA",
    "The MBTA is better than CTA",
    "The CTA is worse than MBTA",
    "The MBTA is worse than CTA",
    "I love that folks are learning this about the CTA",
]

FEEDBACK_KEYWORDS = [
    "love",
    "like",
    "hate",
    "dislike",
    "despise",
    "delayed",
    "delay",
    "late",
    "slow",
    "slower",
    "crowded",
    "crash",
    "crashed",
    "packed",
    "dangerous",
    "unsafe",
    "fight",
    "harassment",
    "dirty",
    "gross",
    "broken",
    "smells",
    "smell",
    "stuck",
    "broke down",
    "stopped",
    "annoying",
    "frustrating",
    "awful",
    "terrible",
    "smooth",
    "on time",
    "quick",
    "quicker",
    "fast",
    "faster",
    "easy",
    "clean",
    "comfortable",
    "uncomfortable",
    "fun",
    "better",
    "wait",
]

FEEDBACK_PATTERN = re.compile(
    r"\b(" + "|".join([re.escape(kw) for kw in FEEDBACK_KEYWORDS]) + r")\b",
    re.IGNORECASE,
)

# Feedback rule match threshold
FEEDBACK_RULE_THRESHOLD = 0.525

# Model configuration
SBERT_MODEL_NAME = "sentence-transformers/all-distilroberta-v1"
SENTIMENT_MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"

# Time zone
CHICAGO_TZ = "America/Chicago"

# To handle fractional timestamps
FRACTION_RE = re.compile(r"(\.\d+)(?=([+-]\d\d:\d\d|Z)$)")



