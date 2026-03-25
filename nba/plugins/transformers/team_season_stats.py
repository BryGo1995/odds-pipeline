def transform_team_season_stats(conn, raw_stats):
    if not raw_stats:
        return
    with conn.cursor() as cur:
        for s in raw_stats:
            cur.execute(
                """
                INSERT INTO team_season_stats
                    (team_id, team_name, season, pace, off_rating, def_rating, opp_pts_pg)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_id, season) DO UPDATE SET
                    pace       = EXCLUDED.pace,
                    off_rating = EXCLUDED.off_rating,
                    def_rating = EXCLUDED.def_rating,
                    opp_pts_pg = EXCLUDED.opp_pts_pg,
                    fetched_at = NOW()
                """,
                (
                    s["team_id"],
                    s.get("team_name"),
                    s["season"],
                    s.get("pace"),
                    s.get("off_rating"),
                    s.get("def_rating"),
                    s.get("opp_pts_pg"),
                ),
            )
    conn.commit()
