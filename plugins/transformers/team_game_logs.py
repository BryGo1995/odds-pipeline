def transform_team_game_logs(conn, raw_logs):
    if not raw_logs:
        return
    with conn.cursor() as cur:
        for log in raw_logs:
            cur.execute(
                """
                INSERT INTO team_game_logs
                    (team_id, team_abbreviation, nba_game_id, season,
                     game_date, matchup, wl, pts, plus_minus)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_id, nba_game_id) DO NOTHING
                """,
                (
                    log["team_id"],
                    log.get("team_abbreviation"),
                    log["nba_game_id"],
                    log["season"],
                    log["game_date"],
                    log.get("matchup"),
                    log.get("wl"),
                    log.get("pts"),
                    log.get("plus_minus"),
                ),
            )
    conn.commit()
