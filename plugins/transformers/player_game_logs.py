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
                         team_id, wl, min,
                         fgm, fga, fg_pct,
                         fg3m, fg3a, fg3_pct,
                         ftm, fta, ft_pct,
                         usg_pct,
                         pts, reb, oreb, dreb,
                         ast, blk, stl, tov, pf, plus_minus)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s)
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
                        log.get("min"),
                        log.get("fgm"),
                        log.get("fga"),
                        log.get("fg_pct"),
                        log.get("fg3m"),
                        log.get("fg3a"),
                        log.get("fg3_pct"),
                        log.get("ftm"),
                        log.get("fta"),
                        log.get("ft_pct"),
                        log.get("usg_pct"),
                        log.get("pts"),
                        log.get("reb"),
                        log.get("oreb"),
                        log.get("dreb"),
                        log.get("ast"),
                        log.get("blk"),
                        log.get("stl"),
                        log.get("tov"),
                        log.get("pf"),
                        log.get("plus_minus"),
                    ),
                )
                cur.execute("RELEASE SAVEPOINT pgl_row")
            except Exception:
                # Skip rows with unknown player IDs (FK violation) or other row-level errors
                cur.execute("ROLLBACK TO SAVEPOINT pgl_row")
                cur.execute("RELEASE SAVEPOINT pgl_row")
    conn.commit()
