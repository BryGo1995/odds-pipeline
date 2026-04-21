"""
Transforms raw odds data into the normalized `odds` table.

Note: This transformer uses plain INSERT (no ON CONFLICT) because the odds table
is an append-only historical log — each fetch represents a point-in-time snapshot
of lines, enabling line movement tracking. The transform DAG is designed to run
once per ingest cycle; running it multiple times against the same raw data will
create duplicate rows.
"""

# Game-level markets go into `odds`. Anything not in this set is treated as a
# player-level / prop market and routed to `player_props` by a separate transformer.
GAME_LEVEL_MARKETS = {"h2h", "spreads", "totals"}


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
                    if market_key not in GAME_LEVEL_MARKETS:
                        continue  # player/prop markets handled by transform_player_props
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
