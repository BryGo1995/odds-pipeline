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


def _get(path, params, delay_seconds):
    """Issue a GET against statsapi.mlb.com with retry on 429 / 5xx / network errors.

    Sleeps `delay_seconds` before the first attempt. Retries up to 3 times with
    backoffs of 30s, 60s, 120s. Raises on terminal failure or any 4xx other than
    429.
    """
    time.sleep(delay_seconds)
    for attempt, backoff in enumerate(_RETRY_BACKOFFS, start=1):
        try:
            r = requests.get(
                _BASE_URL + path,
                params=params,
                headers={"User-Agent": _USER_AGENT},
                timeout=_DEFAULT_TIMEOUT,
            )
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt == len(_RETRY_BACKOFFS):
                    r.raise_for_status()
                time.sleep(backoff)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.Timeout, requests.ConnectionError):
            if attempt == len(_RETRY_BACKOFFS):
                raise
            time.sleep(backoff)
    # unreachable — loop either returns or raises
    raise RuntimeError("_get exhausted retries without returning or raising")
