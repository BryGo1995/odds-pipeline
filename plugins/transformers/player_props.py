# plugins/transformers/player_props.py

def transform_player_props(conn, raw_odds):
    if not raw_odds:
        return
    with conn.cursor() as cur:
        for game in raw_odds:
            game_id = game["id"]
            for bookmaker in game.get("bookmakers", []):
                bookmaker_key = bookmaker["key"]
                for market in bookmaker.get("markets", []):
                    market_key = market["key"]
                    if not market_key.startswith("player_"):
                        continue
                    last_update = market.get("last_update")
                    for outcome in market.get("outcomes", []):
                        cur.execute(
                            """
                            INSERT INTO player_props
                                (game_id, bookmaker, player_name, prop_type, outcome, price, point, last_update)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                game_id,
                                bookmaker_key,
                                outcome.get("description"),  # player name is in description field
                                market_key,
                                outcome["name"],             # Over / Under
                                outcome.get("price"),
                                outcome.get("point"),
                                last_update,
                            ),
                        )
    conn.commit()
