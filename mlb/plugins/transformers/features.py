# mlb/plugins/transformers/features.py
"""
Feature engineering for the MLB batter-prop ML model.

Mirrors nba/plugins/transformers/features.py. Output columns match the NBA
parquet schema so the trainer/scorer can be ported with minimal divergence.

Design deviations from NBA:
- `is_home` cannot be parsed from `matchup` alone — MLB game logs store
  "AWAY @ HOME" for every player on both teams. We resolve it by JOINing
  mlb_teams (player team) and matching against the home side of `matchup`.
"""
from datetime import date

import pandas as pd

from shared.plugins.odds_math import american_odds_to_implied_prob

MLB_PROP_STAT_MAP = {
    "batter_hits":        "hits",
    "batter_total_bases": "total_bases",
}

_PROP_TYPES = list(MLB_PROP_STAT_MAP.keys())


def build_features(conn, game_date: str) -> pd.DataFrame:
    """
    Build one feature row per (player, prop_type, bookmaker) for game_date.
    actual_result / actual_stat_value are NULL when the game has not yet been played.

    Args:
        conn: psycopg2 connection to data Postgres
        game_date: ISO date string, e.g. "2026-04-30"

    Returns:
        DataFrame matching the parquet schema. Empty if no MLB props found.
    """
    query = """
        WITH prop_windows AS (
            SELECT
                pp.mlb_player_id                                       AS player_id,
                pp.player_name,
                (g.commence_time AT TIME ZONE 'America/New_York')::date AS game_date,
                pp.game_id,
                pp.prop_type,
                pp.bookmaker,
                pp.point                                               AS line,
                pp.price,
                FIRST_VALUE(pp.point) OVER w_asc                       AS opening_line,
                ROW_NUMBER()          OVER w_desc                      AS rn
            FROM player_props pp
            JOIN games g ON pp.game_id = g.game_id
            WHERE pp.mlb_player_id IS NOT NULL
              AND g.sport = 'baseball_mlb'
              AND pp.outcome = 'Over'
              AND pp.prop_type = ANY(%s)
              AND (g.commence_time AT TIME ZONE 'America/New_York')::date = %s
            WINDOW
                w_asc  AS (PARTITION BY pp.mlb_player_id, pp.game_id, pp.bookmaker, pp.prop_type
                            ORDER BY pp.last_update ASC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                w_desc AS (PARTITION BY pp.mlb_player_id, pp.game_id, pp.bookmaker, pp.prop_type
                            ORDER BY pp.last_update DESC)
        )
        SELECT
            pw.player_id,
            pw.player_name,
            pw.game_date,
            pw.prop_type,
            pw.bookmaker,
            pw.line,
            pw.price,
            pw.line - pw.opening_line                AS line_movement,
            pgl.matchup,
            pt.abbreviation                          AS team_abbreviation,
            SPLIT_PART(pgl.matchup, ' @ ', 2)        AS home_abbr,
            pgl.hits,
            pgl.total_bases,
            pgl.home_runs
        FROM prop_windows pw
        LEFT JOIN mlb_player_game_logs pgl
               ON pgl.player_id = pw.player_id
              AND pgl.game_date  = pw.game_date
        LEFT JOIN mlb_teams pt
               ON pt.team_id = pgl.team_id
        WHERE pw.rn = 1
    """
    with conn.cursor() as cur:
        cur.execute(query, (_PROP_TYPES, game_date))
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=col_names)

    df["implied_prob_over"] = df["price"].apply(american_odds_to_implied_prob)
    df["is_home"] = df.apply(
        lambda r: (r["team_abbreviation"] == r["home_abbr"])
        if (r.get("team_abbreviation") and r.get("home_abbr")) else None,
        axis=1,
    )
    df["actual_stat_value"] = df.apply(
        lambda r: r.get(MLB_PROP_STAT_MAP[r["prop_type"]]) if r["prop_type"] in MLB_PROP_STAT_MAP else None,
        axis=1,
    )
    df["actual_result"] = df.apply(
        lambda r: (1 if r["actual_stat_value"] > r["line"] else 0)
        if pd.notna(r.get("actual_stat_value")) else None,
        axis=1,
    )

    player_ids = df["player_id"].dropna().unique().tolist()
    rolling_df = _compute_rolling_stats(conn, player_ids, game_date)
    rest_df    = _compute_rest_days(conn, player_ids, game_date)

    df = df.merge(rolling_df, on=["player_id", "prop_type"], how="left")
    df = df.merge(rest_df,    on="player_id",                how="left")

    drop_cols = [
        "matchup", "team_abbreviation", "home_abbr",
        "price", "hits", "total_bases", "home_runs", "game_id",
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    cols = [
        "player_id", "player_name", "game_date", "prop_type", "bookmaker",
        "line", "implied_prob_over", "line_movement",
        "rolling_avg_5g", "rolling_avg_10g", "rolling_avg_20g", "rolling_std_10g",
        "is_home", "rest_days",
        "actual_result", "actual_stat_value",
    ]
    return df[[c for c in cols if c in df.columns]]


def _compute_rolling_stats(conn, player_ids: list, before_date: str) -> pd.DataFrame:
    """
    For each (player_id, prop_type), compute rolling averages of the corresponding
    stat column over the player's last 5/10/20 games prior to before_date.

    Returns DataFrame with columns:
        player_id, prop_type, rolling_avg_5g, rolling_avg_10g, rolling_avg_20g, rolling_std_10g
    """
    empty = pd.DataFrame(columns=[
        "player_id", "prop_type",
        "rolling_avg_5g", "rolling_avg_10g", "rolling_avg_20g", "rolling_std_10g",
    ])
    if not player_ids:
        return empty

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT player_id, game_date, hits, total_bases, home_runs
            FROM mlb_player_game_logs
            WHERE player_id = ANY(%s) AND game_date < %s
            ORDER BY player_id, game_date ASC
            """,
            (player_ids, before_date),
        )
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

    if not rows:
        return empty

    logs = pd.DataFrame(rows, columns=col_names)
    results = []

    for prop_type, stat_col in MLB_PROP_STAT_MAP.items():
        if stat_col not in logs.columns:
            continue
        grp = logs.groupby("player_id")[stat_col]
        rolling_5  = grp.apply(lambda s: s.iloc[-5:].mean())
        rolling_10 = grp.apply(lambda s: s.iloc[-10:].mean())
        rolling_20 = grp.apply(lambda s: s.iloc[-20:].mean())
        std_10     = grp.apply(lambda s: s.iloc[-10:].std() if len(s) >= 2 else None)

        results.append(pd.DataFrame({
            "player_id":       rolling_5.index,
            "prop_type":       prop_type,
            "rolling_avg_5g":  rolling_5.values,
            "rolling_avg_10g": rolling_10.values,
            "rolling_avg_20g": rolling_20.values,
            "rolling_std_10g": std_10.values,
        }))

    return pd.concat(results, ignore_index=True) if results else empty


def _compute_rest_days(conn, player_ids: list, game_date: str) -> pd.DataFrame:
    """
    For each player, compute the number of days since their last game before game_date.
    Returns DataFrame with columns: player_id, rest_days
    """
    if not player_ids:
        return pd.DataFrame(columns=["player_id", "rest_days"])

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (player_id)
                player_id,
                game_date AS last_game_date
            FROM mlb_player_game_logs
            WHERE player_id = ANY(%s) AND game_date < %s
            ORDER BY player_id, game_date DESC
            """,
            (player_ids, game_date),
        )
        rows = cur.fetchall()

    target = date.fromisoformat(game_date)
    result = {pid: None for pid in player_ids}
    for player_id, last_game_date in rows:
        result[player_id] = (target - last_game_date).days

    return pd.DataFrame([
        {"player_id": pid, "rest_days": days}
        for pid, days in result.items()
    ])
