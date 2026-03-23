def transform_players(conn, raw_players):
    if not raw_players:
        return
    with conn.cursor() as cur:
        for p in raw_players:
            cur.execute(
                """
                INSERT INTO players
                    (player_id, full_name, position, team_id, team_abbreviation, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET
                    full_name         = EXCLUDED.full_name,
                    position          = EXCLUDED.position,
                    team_id           = EXCLUDED.team_id,
                    team_abbreviation = EXCLUDED.team_abbreviation,
                    is_active         = EXCLUDED.is_active,
                    fetched_at        = NOW()
                """,
                (
                    p["player_id"],
                    p["full_name"],
                    p.get("position"),
                    p.get("team_id"),
                    p.get("team_abbreviation"),
                    p.get("is_active", True),
                ),
            )
    conn.commit()
