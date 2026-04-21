# plugins/transformers/events.py

def transform_events(conn, raw_events):
    if not raw_events:
        return
    with conn.cursor() as cur:
        for event in raw_events:
            cur.execute(
                """
                INSERT INTO games (game_id, home_team, away_team, commence_time, sport)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE SET
                    home_team     = EXCLUDED.home_team,
                    away_team     = EXCLUDED.away_team,
                    commence_time = EXCLUDED.commence_time
                """,
                (
                    event["id"],
                    event["home_team"],
                    event["away_team"],
                    event["commence_time"],
                    event["sport_key"],
                ),
            )
    conn.commit()
