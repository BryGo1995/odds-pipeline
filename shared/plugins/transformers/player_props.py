"""
Transforms raw odds data into the normalized `player_props` table.

Note: This transformer uses plain INSERT (no ON CONFLICT) because player_props
is an append-only historical log for tracking line movement over time. The
transform DAG should run once per ingest cycle to avoid duplicate rows.
"""
from shared.plugins.transformers.odds import GAME_LEVEL_MARKETS


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
                    if market_key in GAME_LEVEL_MARKETS:
                        continue  # game-level markets handled by transform_odds
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
