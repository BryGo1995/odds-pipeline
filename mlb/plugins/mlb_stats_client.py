# mlb/plugins/mlb_stats_client.py
"""Thin HTTP client over the public MLB Stats API (statsapi.mlb.com).

Fetches teams, players, and per-game batter logs for ingestion into the
mlb_teams, mlb_players, and mlb_player_game_logs tables.
"""
import logging
import time
from datetime import date

import requests

_BASE_URL = "https://statsapi.mlb.com/api/v1"
_SPORT_ID = 1  # 1 = MLB in statsapi.mlb.com's sport taxonomy
_DEFAULT_TIMEOUT = 30
_USER_AGENT = "odds-pipeline/mlb (github.com/BryGo1995/odds-pipeline)"
_RETRY_BACKOFFS = [30, 60, 120]
