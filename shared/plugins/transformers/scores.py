# plugins/transformers/scores.py
import logging


def transform_scores(conn, raw_scores):
    if not raw_scores:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT game_id FROM games")
        known_game_ids = {row[0] for row in cur.fetchall()}

        skipped = []
        for game in raw_scores:
            game_id = game["id"]
            if game_id not in known_game_ids:
                skipped.append(game_id)
                continue

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
                    game_id,
                    home_score,
                    away_score,
                    game.get("completed", False),
                    game.get("last_update"),
                ),
            )

    if skipped:
        logging.warning(
            "Skipped %d score(s) with no matching game in 'games' table: %s",
            len(skipped),
            skipped,
        )
    conn.commit()
