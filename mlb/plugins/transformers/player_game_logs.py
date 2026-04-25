import logging

logger = logging.getLogger(__name__)


def transform_player_game_logs(conn, raw_logs):
    """Upsert MLB batter game-log rows into mlb_player_game_logs.

    Uses per-row SAVEPOINT so one bad row (e.g. FK violation on
    player_id) doesn't abort the whole batch. ON CONFLICT DO NOTHING
    makes re-ingestion (e.g. the daily 3-day lookback) idempotent.
    """
    if not raw_logs:
        return
    with conn.cursor() as cur:
        for log in raw_logs:
            cur.execute("SAVEPOINT pgl_row")
            try:
                cur.execute(
                    """
                    INSERT INTO mlb_player_game_logs
                        (player_id, mlb_game_pk, season, game_date, matchup,
                         team_id, opponent_team_id,
                         plate_appearances, at_bats, hits, doubles, triples,
                         home_runs, rbi, runs, walks, strikeouts,
                         stolen_bases, total_bases)
                    VALUES (%s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s)
                    ON CONFLICT (player_id, mlb_game_pk) DO NOTHING
                    """,
                    (
                        log["player_id"],
                        log["mlb_game_pk"],
                        log["season"],
                        log["game_date"],
                        log.get("matchup"),
                        log.get("team_id"),
                        log.get("opponent_team_id"),
                        log.get("plate_appearances"),
                        log.get("at_bats"),
                        log.get("hits"),
                        log.get("doubles"),
                        log.get("triples"),
                        log.get("home_runs"),
                        log.get("rbi"),
                        log.get("runs"),
                        log.get("walks"),
                        log.get("strikeouts"),
                        log.get("stolen_bases"),
                        log.get("total_bases"),
                    ),
                )
                cur.execute("RELEASE SAVEPOINT pgl_row")
            except Exception as exc:
                logger.warning(
                    "game_log row failed: player_id=%s mlb_game_pk=%s: %s",
                    log.get("player_id"), log.get("mlb_game_pk"), exc,
                )
                cur.execute("ROLLBACK TO SAVEPOINT pgl_row")
                cur.execute("RELEASE SAVEPOINT pgl_row")
    conn.commit()
