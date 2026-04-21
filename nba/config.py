# nba/config.py
# NBA-specific configuration for odds-pipeline.

SPORT = "basketball_nba"

REGIONS = ["us"]

# Markets to fetch from Odds-API.
# Add or remove to control API quota usage.
# Options: h2h, spreads, totals
MARKETS = [
    "h2h",
    "spreads",
    "totals",
]

PLAYER_PROP_MARKETS = [
    "player_points",
    "player_rebounds",
    "player_assists",
]

# Bookmakers to include.
# Fewer bookmakers = fewer API requests consumed.
BOOKMAKERS = [
    "draftkings",
    "fanduel",
    "betmgm",
]

ODDS_FORMAT = "american"

# How many days back to fetch scores for
SCORES_DAYS_FROM = 3
