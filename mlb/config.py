# mlb/config.py
# MLB-specific configuration for odds-pipeline.

SPORT = "baseball_mlb"

REGIONS = ["us"]

# Game-level markets to fetch from Odds-API.
MARKETS = [
    "h2h",
    "spreads",
    "totals",
]

# Batter player-prop markets — MVP targets these three.
# Post-MVP: batter_rbis, batter_runs_scored, batter_stolen_bases,
#           batter_hits_runs_rbis; plus pitcher props.
PLAYER_PROP_MARKETS = [
    "batter_hits",
    "batter_total_bases",
    "batter_home_runs",
]

BOOKMAKERS = [
    "draftkings",
    "fanduel",
    "betmgm",
]

ODDS_FORMAT = "american"

# How many days back to fetch scores for
SCORES_DAYS_FROM = 3
