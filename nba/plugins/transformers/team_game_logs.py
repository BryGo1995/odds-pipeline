_COLUMNS = [
    "team_id", "team_abbreviation", "nba_game_id", "season",
    "game_date", "matchup", "wl", "pts", "plus_minus",
    "min", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
    "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf",
]
_KEY_COLUMNS = {"team_id", "nba_game_id"}
_UPDATE_COLUMNS = [c for c in _COLUMNS if c not in _KEY_COLUMNS]

# DO UPDATE (not DO NOTHING) so re-fetching a season fills in the box score
# columns on rows stored before they existed (#38) and picks up stat
# corrections. The daily run re-fetches the whole season, so the WHERE skips
# rows that haven't changed instead of rewriting all of them (#41).
_UPSERT_SQL = f"""
    INSERT INTO team_game_logs ({", ".join(_COLUMNS)})
    VALUES ({", ".join(["%s"] * len(_COLUMNS))})
    ON CONFLICT (team_id, nba_game_id) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATE_COLUMNS)},
        fetched_at = NOW()
    WHERE ({", ".join(f"team_game_logs.{c}" for c in _UPDATE_COLUMNS)})
          IS DISTINCT FROM ({", ".join(f"EXCLUDED.{c}" for c in _UPDATE_COLUMNS)})
"""


def transform_team_game_logs(conn, raw_logs):
    if not raw_logs:
        return
    with conn.cursor() as cur:
        for log in raw_logs:
            cur.execute(_UPSERT_SQL, tuple(log.get(c) for c in _COLUMNS))
    conn.commit()
