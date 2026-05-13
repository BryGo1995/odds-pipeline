# mlb/plugins/ml/settle.py
"""
Settle daily MLB recommendations against actual game outcomes.

settle_recommendations(conn) looks up each unsettled recommendation in
mlb_player_game_logs (via mlb_player_name_mappings), records actual_result
and actual_stat_value, and triggers a Slack recap when a game date's top-10
are fully resolved.

Doubleheader handling: stats are aggregated per (player_id, game_date) via
SUM/GROUP BY, so a player with two same-day game logs has their stats summed
before settlement. Single-game days collapse to a one-row sum (no behavioral
difference vs a direct join).
"""
import logging
from datetime import date

from mlb.plugins.transformers.features import MLB_PROP_STAT_MAP

try:
    from shared.plugins.slack_notifier import notify_picks_settled
except ImportError:
    notify_picks_settled = None

log = logging.getLogger(__name__)

_STAT_COLS = list(MLB_PROP_STAT_MAP.values())  # ['hits', 'total_bases']


def settle_recommendations(conn) -> None:
    """
    Settle unsettled recommendations and send Slack recap for any game date
    whose top-10 picks are now fully resolved.
    """
    newly_settled_dates = set()

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH agg AS (
                SELECT player_id, game_date,
                       SUM(hits)        AS hits,
                       SUM(total_bases) AS total_bases,
                       SUM(home_runs)   AS home_runs
                FROM mlb_player_game_logs
                GROUP BY player_id, game_date
            )
            SELECT
                r.id,
                r.player_name,
                r.prop_type,
                r.line,
                r.game_date,
                agg.hits,
                agg.total_bases,
                agg.home_runs
            FROM recommendations r
            JOIN mlb_player_name_mappings m ON m.odds_api_name = r.player_name
            JOIN agg                          ON agg.player_id = m.mlb_player_id
                                             AND agg.game_date = r.game_date
            WHERE r.settled_at IS NULL
              AND r.game_date < CURRENT_DATE
              AND r.sport = 'MLB'
            """
        )
        resolvable = cur.fetchall()

        cur.execute(
            """
            SELECT id FROM recommendations
            WHERE settled_at IS NULL
              AND game_date < CURRENT_DATE - INTERVAL '7 days'
              AND sport = 'MLB'
            """
        )
        stale_ids = [row[0] for row in cur.fetchall()]

    _settle_resolvable(conn, resolvable, newly_settled_dates)
    _mark_stale(conn, stale_ids, newly_settled_dates)

    conn.commit()

    _notify_completed_dates(conn, newly_settled_dates)


def _settle_resolvable(conn, rows, newly_settled_dates: set) -> None:
    # rows: id, player_name, prop_type, line, game_date, hits, total_bases, home_runs
    stats_offset = 5

    for row in rows:
        rec_id, player_name, prop_type, line, game_date = row[:5]
        stat_col = MLB_PROP_STAT_MAP.get(prop_type)
        if not stat_col:
            log.warning("Unknown prop_type '%s' for rec id=%d — skipping", prop_type, rec_id)
            continue

        stat_val = row[stats_offset + _STAT_COLS.index(stat_col)]
        if stat_val is None:
            continue

        actual_result = float(stat_val) > float(line)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE recommendations
                SET actual_result = %s, actual_stat_value = %s, settled_at = NOW()
                WHERE id = %s
                """,
                (actual_result, stat_val, rec_id),
            )
        newly_settled_dates.add(game_date)


def _mark_stale(conn, stale_ids: list, newly_settled_dates: set) -> None:
    if not stale_ids:
        return
    log.warning("Marking %d stale recommendation(s) as unresolvable: ids=%s", len(stale_ids), stale_ids)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE recommendations
            SET settled_at = NOW()
            WHERE id = ANY(%s) AND settled_at IS NULL
            """,
            (stale_ids,),
        )


def _notify_completed_dates(conn, newly_settled_dates: set) -> None:
    """For each date settled in this run, check if all top-10 recs are now done."""
    if not newly_settled_dates:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT game_date
            FROM recommendations
            WHERE game_date = ANY(%s)
              AND rank <= 10
              AND sport = 'MLB'
            GROUP BY game_date
            HAVING COUNT(*) FILTER (WHERE settled_at IS NULL) = 0
            """,
            (list(newly_settled_dates),),
        )
        completed_dates = [row[0] for row in cur.fetchall()]

    for game_date in completed_dates:
        _send_recap(conn, game_date)


def _send_recap(conn, game_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT player_name, prop_type, line, outcome, actual_result,
                   actual_stat_value, edge
            FROM recommendations
            WHERE game_date = %s AND rank <= 10 AND sport = 'MLB'
            ORDER BY rank
            """,
            (game_date,),
        )
        rows = cur.fetchall()

    results = [
        {
            "player_name":      r[0],
            "prop_type":        r[1],
            "line":             float(r[2]) if r[2] is not None else None,
            "outcome":          r[3],
            "actual_result":    r[4],
            "actual_stat_value": float(r[5]) if r[5] is not None else None,
            "edge":             float(r[6]) if r[6] is not None else None,
        }
        for r in rows
    ]
    if notify_picks_settled is not None:
        notify_picks_settled(game_date, results, sport="mlb")
