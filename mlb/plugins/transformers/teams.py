def transform_teams(conn, raw_teams):
    """Upsert MLB team rows into the mlb_teams table."""
    if not raw_teams:
        return
    with conn.cursor() as cur:
        for t in raw_teams:
            cur.execute(
                """
                INSERT INTO mlb_teams
                    (team_id, full_name, abbreviation, league, division)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (team_id) DO UPDATE SET
                    full_name    = EXCLUDED.full_name,
                    abbreviation = EXCLUDED.abbreviation,
                    league       = EXCLUDED.league,
                    division     = EXCLUDED.division,
                    fetched_at   = NOW()
                """,
                (
                    t["team_id"],
                    t["full_name"],
                    t["abbreviation"],
                    t.get("league"),
                    t.get("division"),
                ),
            )
    conn.commit()
