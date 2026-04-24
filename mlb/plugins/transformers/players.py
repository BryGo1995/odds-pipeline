def transform_players(conn, raw_players):
    """Upsert MLB player rows into mlb_players, resolving team_abbreviation
    from mlb_teams. The client leaves team_abbreviation as None; this
    transformer fills it via a single join against mlb_teams.
    """
    if not raw_players:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT team_id, abbreviation FROM mlb_teams")
        team_abbr = dict(cur.fetchall())  # {team_id: abbreviation}

        for p in raw_players:
            tid = p.get("team_id")
            cur.execute(
                """
                INSERT INTO mlb_players
                    (player_id, full_name, position, bats, throws,
                     team_id, team_abbreviation, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET
                    full_name         = EXCLUDED.full_name,
                    position          = EXCLUDED.position,
                    bats              = EXCLUDED.bats,
                    throws            = EXCLUDED.throws,
                    team_id           = EXCLUDED.team_id,
                    team_abbreviation = EXCLUDED.team_abbreviation,
                    is_active         = EXCLUDED.is_active,
                    fetched_at        = NOW()
                """,
                (
                    p["player_id"],
                    p["full_name"],
                    p.get("position"),
                    p.get("bats"),
                    p.get("throws"),
                    tid,
                    team_abbr.get(tid),
                    p.get("is_active", True),
                ),
            )
    conn.commit()
