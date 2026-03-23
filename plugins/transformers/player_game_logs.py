def transform_player_game_logs(conn, raw_logs):
    if not raw_logs:
        return
    with conn.cursor() as cur:
        for log in raw_logs:
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
                    log["player_id"],          # 0
                    log["nba_game_id"],        # 1
                    log["season"],             # 2
                    log["game_date"],          # 3
                    log.get("matchup"),        # 4
                    log.get("team_id"),        # 5
                    log.get("wl"),             # 6
                    log.get("fga"),            # 7
                    log.get("min"),            # 8
                    log.get("fta"),            # 9
                    log.get("usg_pct"),        # 10
                    log.get("pts"),            # 11
                    log.get("reb"),            # 12
                    log.get("ast"),            # 13
                    log.get("blk"),            # 14
                    log.get("stl"),            # 15
                    log.get("plus_minus"),     # 16
                ),
            )
    conn.commit()
