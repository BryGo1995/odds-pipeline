# plugins/nba_api_client.py
import json
import time

import requests
from nba_api.stats.endpoints import (
    CommonAllPlayers,
    PlayerGameLogs,
    LeagueGameLog,
    LeagueDashTeamStats,
)
from nba_api.stats.library.http import STATS_HEADERS

# stats.nba.com (Akamai CDN) requires browser-like headers including Sec-Fetch-* to avoid silent drops
STATS_HEADERS["Origin"] = "https://www.nba.com"
STATS_HEADERS["Referer"] = "https://www.nba.com/"
STATS_HEADERS["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
STATS_HEADERS["Sec-Fetch-Site"] = "same-site"
STATS_HEADERS["Sec-Fetch-Mode"] = "cors"
STATS_HEADERS["Sec-Fetch-Dest"] = "empty"

_DEFAULT_TIMEOUT = 120


def _call_with_retry(endpoint_cls, delay_seconds, **kwargs):
    """Call an nba_api endpoint class, retrying on HTTP 429 or empty-body responses."""
    time.sleep(delay_seconds)
    try:
        return endpoint_cls(timeout=_DEFAULT_TIMEOUT, **kwargs)
    except requests.exceptions.HTTPError as e:
        if hasattr(e, "response") and e.response is not None and e.response.status_code == 429:
            time.sleep(30)
            return endpoint_cls(timeout=_DEFAULT_TIMEOUT, **kwargs)
        raise
    except json.JSONDecodeError:
        # Akamai CDN occasionally returns an empty body (silent block); wait and retry once
        time.sleep(30)
        return endpoint_cls(timeout=_DEFAULT_TIMEOUT, **kwargs)


def fetch_players(delay_seconds=1):
    """Fetch all active NBA players. Returns list of dicts."""
    result = _call_with_retry(CommonAllPlayers, delay_seconds, is_only_current_season=1)
    df = result.get_data_frames()[0]
    active = df[df["ROSTERSTATUS"] == 1]
    return [
        {
            "player_id": int(row["PERSON_ID"]),
            "full_name": row["DISPLAY_FIRST_LAST"],
            "team_id": int(row["TEAM_ID"]) if row["TEAM_ID"] else None,
            "team_abbreviation": row["TEAM_ABBREVIATION"] or None,
            "position": row.get("POSITION") or None,
            "is_active": True,
        }
        for _, row in active.iterrows()
    ]


def fetch_player_game_logs(season, delay_seconds=1):
    """
    Fetch all player game logs for a season (basic + advanced merged).
    Returns list of dicts. season format: '2024-25'.
    """
    base = _call_with_retry(
        PlayerGameLogs, delay_seconds,
        season_nullable=season,
        measure_type_player_game_logs_nullable="Base",
    )
    adv = _call_with_retry(
        PlayerGameLogs, delay_seconds,
        season_nullable=season,
        measure_type_player_game_logs_nullable="Advanced",
    )
    base_df = base.get_data_frames()[0]
    adv_df = adv.get_data_frames()[0][["PLAYER_ID", "GAME_ID", "USG_PCT"]]
    merged = base_df.merge(adv_df, on=["PLAYER_ID", "GAME_ID"], how="left")
    rows = []
    for _, row in merged.iterrows():
        rows.append({
            "player_id": int(row["PLAYER_ID"]),
            "nba_game_id": row["GAME_ID"],
            "season": season,
            "game_date": row["GAME_DATE"],
            "matchup": row["MATCHUP"],
            "team_id": int(row["TEAM_ID"]) if row.get("TEAM_ID") else None,
            "wl": row.get("WL"),
            "min": float(row["MIN"]) if row.get("MIN") is not None else None,
            "fga": int(row["FGA"]) if row.get("FGA") is not None else None,
            "fta": int(row["FTA"]) if row.get("FTA") is not None else None,
            "usg_pct": float(row["USG_PCT"]) if row.get("USG_PCT") is not None else None,
            "pts": int(row["PTS"]) if row.get("PTS") is not None else None,
            "reb": int(row["REB"]) if row.get("REB") is not None else None,
            "ast": int(row["AST"]) if row.get("AST") is not None else None,
            "blk": int(row["BLK"]) if row.get("BLK") is not None else None,
            "stl": int(row["STL"]) if row.get("STL") is not None else None,
            "plus_minus": int(row["PLUS_MINUS"]) if row.get("PLUS_MINUS") is not None else None,
        })
    return rows


def fetch_team_game_logs(season, delay_seconds=1):
    """Fetch all team game logs for a season. Returns list of dicts."""
    result = _call_with_retry(
        LeagueGameLog, delay_seconds,
        season=season,
        player_or_team_abbreviation="T",
    )
    df = result.get_data_frames()[0]
    return [
        {
            "team_id": int(row["TEAM_ID"]),
            "team_abbreviation": row["TEAM_ABBREVIATION"],
            "nba_game_id": row["GAME_ID"],
            "season": season,
            "game_date": row["GAME_DATE"],
            "matchup": row["MATCHUP"],
            "wl": row.get("WL"),
            "pts": int(row["PTS"]) if row.get("PTS") is not None else None,
            "plus_minus": int(row["PLUS_MINUS"]) if row.get("PLUS_MINUS") is not None else None,
        }
        for _, row in df.iterrows()
    ]


def fetch_team_season_stats(season, delay_seconds=1):
    """
    Fetch season-level team stats (pace, ratings, paint defense).
    Merges Advanced + Opponent measure types. Returns list of dicts.
    """
    adv_result = _call_with_retry(
        LeagueDashTeamStats, delay_seconds,
        season=season,
        measure_type_detailed_defense="Advanced",
        per_mode_detailed="PerGame",
    )
    opp_result = _call_with_retry(
        LeagueDashTeamStats, delay_seconds,
        season=season,
        measure_type_detailed_defense="Opponent",
        per_mode_detailed="PerGame",
    )
    adv_df = adv_result.get_data_frames()[0][["TEAM_ID", "TEAM_NAME", "PACE", "OFF_RATING", "DEF_RATING"]]
    opp_df = opp_result.get_data_frames()[0][["TEAM_ID", "OPP_PTS"]]
    merged = adv_df.merge(opp_df, on="TEAM_ID", how="left")
    return [
        {
            "team_id": int(row["TEAM_ID"]),
            "team_name": row["TEAM_NAME"],
            "season": season,
            "pace": float(row["PACE"]) if row.get("PACE") is not None else None,
            "off_rating": float(row["OFF_RATING"]) if row.get("OFF_RATING") is not None else None,
            "def_rating": float(row["DEF_RATING"]) if row.get("DEF_RATING") is not None else None,
            "opp_pts_pg": float(row["OPP_PTS"]) if row.get("OPP_PTS") is not None else None,
        }
        for _, row in merged.iterrows()
    ]
