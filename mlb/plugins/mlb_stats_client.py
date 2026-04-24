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


def fetch_teams(delay_seconds=0.2):
    """Fetch active MLB teams. Returns list of dicts matching mlb_teams columns."""
    data = _get(
        "/teams",
        {"sportId": _SPORT_ID, "activeStatus": "Y"},
        delay_seconds,
    )
    return [
        {
            "team_id": t["id"],
            "full_name": t["name"],
            "abbreviation": t["abbreviation"],
            "league": (t.get("league") or {}).get("name"),
            "division": (t.get("division") or {}).get("name"),
        }
        for t in data.get("teams", [])
    ]


def fetch_players(season, active_only=True, delay_seconds=0.2):
    """Fetch MLB players for a season. Returns list of dicts matching mlb_players columns.

    team_abbreviation is always None at this layer — transformers resolve it via
    the mlb_teams join.
    """
    params = {"season": season}
    if active_only:
        params["activeStatus"] = "Y"
    data = _get("/sports/1/players", params, delay_seconds)

    players = []
    for p in data.get("people", []):
        current_team = p.get("currentTeam") or {}
        players.append({
            "player_id": p["id"],
            "full_name": p["fullName"],
            "position": (p.get("primaryPosition") or {}).get("abbreviation"),
            "bats": (p.get("batSide") or {}).get("code"),
            "throws": (p.get("pitchHand") or {}).get("code"),
            "team_id": current_team.get("id"),
            "team_abbreviation": None,
            "is_active": bool(p.get("active", False)),
        })
    return players


def _extract_batter_lines(boxscore, game_meta):
    """Extract batter rows from one boxscore JSON payload.

    game_meta carries schedule-level context (game_pk, date, team IDs, abbreviations)
    that the boxscore itself doesn't include.
    """
    matchup = f"{game_meta['away_abbr']} @ {game_meta['home_abbr']}"
    season = game_meta["game_date"][:4]
    rows = []
    teams = boxscore.get("teams") or {}
    for side in ("home", "away"):
        side_obj = teams.get(side) or {}
        team_id = (side_obj.get("team") or {}).get("id")
        opponent_team_id = (
            game_meta["away_team_id"] if side == "home" else game_meta["home_team_id"]
        )
        players = side_obj.get("players") or {}
        for _, p in players.items():
            batting = (p.get("stats") or {}).get("batting") or {}
            pa = batting.get("plateAppearances") or 0
            if pa <= 0:
                continue
            rows.append({
                "player_id": (p.get("person") or {}).get("id"),
                "mlb_game_pk": game_meta["game_pk"],
                "season": season,
                "game_date": game_meta["game_date"],
                "matchup": matchup,
                "team_id": team_id,
                "opponent_team_id": opponent_team_id,
                "plate_appearances": pa,
                "at_bats": batting.get("atBats"),
                "hits": batting.get("hits"),
                "doubles": batting.get("doubles"),
                "triples": batting.get("triples"),
                "home_runs": batting.get("homeRuns"),
                "rbi": batting.get("rbi"),
                "runs": batting.get("runs"),
                "walks": batting.get("baseOnBalls"),
                "strikeouts": batting.get("strikeOuts"),
                "stolen_bases": batting.get("stolenBases"),
                "total_bases": batting.get("totalBases"),
            })
    return rows


def fetch_batter_game_logs(start_date, end_date, delay_seconds=0.2):
    """Fetch per-game batter lines for all Final games in [start_date, end_date].

    start_date / end_date accept `datetime.date` or 'YYYY-MM-DD' strings.

    Strategy: GET /schedule for the date range, then GET /game/{gamePk}/boxscore
    per Final-status game. Bench players (plateAppearances=0) are filtered out.
    Individual boxscore failures are logged and skipped. Raises ValueError if
    every boxscore in the range fails.

    Returns list of dicts matching mlb_player_game_logs columns.
    """
    start_str = start_date.isoformat() if isinstance(start_date, date) else start_date
    end_str = end_date.isoformat() if isinstance(end_date, date) else end_date

    schedule = _get(
        "/schedule",
        {"sportId": _SPORT_ID, "startDate": start_str, "endDate": end_str},
        delay_seconds,
    )

    games = []
    for date_entry in schedule.get("dates", []):
        game_date_str = date_entry.get("date")
        for g in date_entry.get("games", []):
            status = (g.get("status") or {}).get("abstractGameState")
            if status != "Final":
                continue
            teams = g.get("teams") or {}
            away = (teams.get("away") or {}).get("team") or {}
            home = (teams.get("home") or {}).get("team") or {}
            games.append({
                "game_pk": str(g["gamePk"]),
                "game_date": game_date_str,
                "away_team_id": away.get("id"),
                "away_abbr": away.get("abbreviation"),
                "home_team_id": home.get("id"),
                "home_abbr": home.get("abbreviation"),
            })

    rows = []
    fetch_failures = 0
    for g in games:
        try:
            box = _get(f"/game/{g['game_pk']}/boxscore", {}, delay_seconds)
        except requests.HTTPError as e:
            logging.warning("Skipping gamePk=%s: %s", g["game_pk"], e)
            fetch_failures += 1
            continue
        rows.extend(_extract_batter_lines(box, g))

    if games and fetch_failures == len(games):
        raise ValueError("All boxscore fetches failed for the date range.")

    return rows
