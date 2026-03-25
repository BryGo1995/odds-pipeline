def transform_teams(conn, raw_teams):
    if not raw_teams:
        return
    with conn.cursor() as cur:
        for t in raw_teams:
            cur.execute(
                """
                INSERT INTO teams (team_id, full_name, abbreviation, city, nickname)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (team_id) DO UPDATE SET
                    full_name    = EXCLUDED.full_name,
                    abbreviation = EXCLUDED.abbreviation,
                    city         = EXCLUDED.city,
                    nickname     = EXCLUDED.nickname,
                    fetched_at   = NOW()
                """,
                (
                    t["team_id"],
                    t["full_name"],
                    t["abbreviation"],
                    t.get("city"),
                    t.get("nickname"),
                ),
            )
    conn.commit()
