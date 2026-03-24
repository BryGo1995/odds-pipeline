def transform_player_game_logs(conn, raw_logs):
    if not raw_logs:
        return
    with conn.cursor() as cur:
        for log in raw_logs:
            cur.execute("SAVEPOINT pgl_row")
            try:
                cur.execute(
                    """
                    INSERT INTO player_game_logs
                        (player_id, nba_game_id, season, game_date, matchup,
                         team_id, wl, fga, min, fta, usg_pct,
                         pts, reb, ast, blk, stl, plus_minus)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (player_id, nba_game_id) DO NOTHING
                    """,
                    (
                        log["player_id"],
                        log["nba_game_id"],
                        log["season"],
                        log["game_date"],
                        log.get("matchup"),
                        log.get("team_id"),
                        log.get("wl"),
                        log.get("fga"),
                        log.get("min"),
                        log.get("fta"),
                        log.get("usg_pct"),
                        log.get("pts"),
                        log.get("reb"),
                        log.get("ast"),
                        log.get("blk"),
                        log.get("stl"),
                        log.get("plus_minus"),
                    ),
                )
                cur.execute("RELEASE SAVEPOINT pgl_row")
            except Exception:
                # Skip rows with unknown player IDs (FK violation) or other row-level errors
                cur.execute("ROLLBACK TO SAVEPOINT pgl_row")
                cur.execute("RELEASE SAVEPOINT pgl_row")
    conn.commit()
