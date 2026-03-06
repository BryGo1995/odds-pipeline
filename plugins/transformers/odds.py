# plugins/transformers/odds.py

def transform_odds(conn, raw_odds):
    if not raw_odds:
        return
    with conn.cursor() as cur:
        for game in raw_odds:
            game_id = game["id"]
            for bookmaker in game.get("bookmakers", []):
                bookmaker_key = bookmaker["key"]
                for market in bookmaker.get("markets", []):
                    market_key = market["key"]
                    if market_key.startswith("player_"):
                        continue  # player props handled by separate transformer
                    last_update = market.get("last_update")
                    for outcome in market.get("outcomes", []):
                        cur.execute(
                            """
                            INSERT INTO odds
                                (game_id, bookmaker, market_type, outcome_name, price, point, last_update)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                game_id,
                                bookmaker_key,
                                market_key,
                                outcome["name"],
                                outcome.get("price"),
                                outcome.get("point"),
                                last_update,
                            ),
                        )
    conn.commit()
