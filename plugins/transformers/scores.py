# plugins/transformers/scores.py

def transform_scores(conn, raw_scores):
    if not raw_scores:
        return
    with conn.cursor() as cur:
        for game in raw_scores:
            scores = game.get("scores") or []
            home_score, away_score = None, None
            for s in scores:
                if s["name"] == game["home_team"]:
                    home_score = int(s["score"]) if s.get("score") else None
                elif s["name"] == game["away_team"]:
                    away_score = int(s["score"]) if s.get("score") else None
            cur.execute(
                """
                INSERT INTO scores (game_id, home_score, away_score, completed, last_update)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE SET
                    home_score   = EXCLUDED.home_score,
                    away_score   = EXCLUDED.away_score,
                    completed    = EXCLUDED.completed,
                    last_update  = EXCLUDED.last_update
                """,
                (
                    game["id"],
                    home_score,
                    away_score,
                    game.get("completed", False),
                    game.get("last_update"),
                ),
            )
    conn.commit()
