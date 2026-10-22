# nba/plugins/transformers/features.py
"""
Feature engineering for the NBA player prop ML model.

build_features(conn, game_date) queries player_props and player_game_logs,
computes rolling stats and implied probability, and returns a DataFrame
ready to write as Parquet.
"""
import glob
import os
from datetime import date, timedelta

import pandas as pd

from shared.plugins.odds_math import american_odds_to_implied_prob

PROP_STAT_MAP = {
    "player_points":           "pts",
    "player_rebounds":         "reb",
    "player_assists":          "ast",
    "player_threes":           "fg3m",
    "player_threes_attempts":  "fg3a",
}

_PROP_TYPES = list(PROP_STAT_MAP.keys())


def build_features(conn, game_date: str) -> pd.DataFrame:
    """
    Build one feature row per (player, prop_type, bookmaker) for game_date.
    actual_result / actual_stat_value are NULL when the game has not yet been played.

    Args:
        conn: psycopg2 connection to data Postgres
        game_date: ISO date string, e.g. "2026-03-26"

    Returns:
        DataFrame matching the Parquet schema. Empty if no props found.
    """
    query = """
        WITH prop_windows AS (
            SELECT
                pp.nba_player_id                                       AS player_id,
                pp.player_name,
                (g.commence_time AT TIME ZONE 'America/New_York')::date AS game_date,
                pp.game_id,
                pp.prop_type,
                pp.bookmaker,
                pp.point                                               AS line,
                pp.price,
                g.home_team,
                g.away_team,
                FIRST_VALUE(pp.point) OVER w_asc                       AS opening_line,
                ROW_NUMBER()          OVER w_desc                      AS rn
            FROM player_props pp
            JOIN games g ON pp.game_id = g.game_id
            WHERE pp.nba_player_id IS NOT NULL
              AND pp.outcome = 'Over'
              AND pp.prop_type = ANY(%s)
              AND (g.commence_time AT TIME ZONE 'America/New_York')::date = %s
            WINDOW
                w_asc  AS (PARTITION BY pp.nba_player_id, pp.game_id, pp.bookmaker, pp.prop_type
                            ORDER BY pp.last_update ASC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                w_desc AS (PARTITION BY pp.nba_player_id, pp.game_id, pp.bookmaker, pp.prop_type
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
            pw.line - pw.opening_line          AS line_movement,
            -- Pre-game the only team source is the current roster; when backfilling
            -- a played game, that game's log gives the team at the time.
            CASE
                WHEN t.full_name = pw.home_team THEN TRUE
                WHEN t.full_name = pw.away_team THEN FALSE
            END                                AS is_home
        FROM prop_windows pw
        LEFT JOIN player_game_logs pgl
               ON pgl.player_id = pw.player_id
              AND pgl.game_date  = pw.game_date
        LEFT JOIN players p ON p.player_id = pw.player_id
        LEFT JOIN teams t   ON t.team_id = COALESCE(pgl.team_id, p.team_id)
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
    df = attach_labels(conn, df)

    player_ids = df["player_id"].dropna().unique().tolist()
    rolling_df = _compute_rolling_stats(conn, player_ids, game_date)
    rest_df    = _compute_rest_days(conn, player_ids, game_date)

    df = df.merge(rolling_df, on=["player_id", "prop_type"], how="left")
    df = df.merge(rest_df,    on="player_id",                how="left")

    drop_cols = ["price", "home_team", "away_team", "game_id"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    cols = [
        "player_id", "player_name", "game_date", "prop_type", "bookmaker",
        "line", "implied_prob_over", "line_movement",
        "rolling_avg_5g", "rolling_avg_10g", "rolling_avg_20g", "rolling_std_10g",
        "is_home", "rest_days",
        "actual_result", "actual_stat_value",
    ]
    return df[[c for c in cols if c in df.columns]]


def attach_labels(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    Set actual_stat_value / actual_result for each row from player_game_logs,
    matched on (player_id, game_date). Both are NULL when no game log exists
    yet (game not played, or player did not play).
    """
    df = df.copy()
    game_dates = pd.to_datetime(df["game_date"]).dt.date
    player_ids = sorted({int(p) for p in df["player_id"].dropna()})
    stat_cols = sorted(set(PROP_STAT_MAP.values()))

    logs = {}
    if player_ids:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT player_id, game_date, {", ".join(stat_cols)}
                FROM player_game_logs
                WHERE player_id = ANY(%s) AND game_date = ANY(%s)
                """,
                (player_ids, sorted(set(game_dates))),
            )
            for player_id, game_date, *stats in cur.fetchall():
                logs[(player_id, game_date)] = dict(zip(stat_cols, stats))

    values = []
    for player_id, game_date, prop_type in zip(df["player_id"], game_dates, df["prop_type"]):
        log = logs.get((int(player_id), game_date)) if pd.notna(player_id) else None
        values.append(log.get(PROP_STAT_MAP[prop_type]) if log and prop_type in PROP_STAT_MAP else None)

    df["actual_stat_value"] = values
    df["actual_result"] = [
        (1 if value > line else 0) if value is not None else None
        for value, line in zip(values, df["line"])
    ]
    return df


def label_feature_files(conn, features_dir: str, before_date: str, lookback_days: int | None = 7) -> int:
    """
    Re-attach labels to feature files for the lookback_days before before_date
    (every file before it when lookback_days is None). Features are built
    pre-game, so labels only exist once the stats pipeline has loaded that
    day's game logs. Returns the number of files rewritten.
    """
    end = date.fromisoformat(before_date)
    start = end - timedelta(days=lookback_days) if lookback_days is not None else None
    rewritten = 0

    for path in sorted(glob.glob(f"{features_dir}/props_features_*.parquet")):
        file_date = date.fromisoformat(os.path.basename(path)[len("props_features_"):-len(".parquet")])
        if file_date >= end or (start is not None and file_date < start):
            continue

        df = pd.read_parquet(path)
        labeled = attach_labels(conn, df)
        unchanged = all(
            col in df.columns
            and labeled[col].astype("Float64").equals(df[col].astype("Float64"))
            for col in ("actual_stat_value", "actual_result")
        )
        if unchanged:
            continue

        tmp_path = f"{path}.tmp"
        labeled.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, path)
        rewritten += 1

    return rewritten


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
            SELECT player_id, game_date, pts, reb, ast, fg3m, fg3a
            FROM player_game_logs
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

    for prop_type, stat_col in PROP_STAT_MAP.items():
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
            FROM player_game_logs
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
