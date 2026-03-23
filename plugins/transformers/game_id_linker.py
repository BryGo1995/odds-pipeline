import logging

logger = logging.getLogger(__name__)


def parse_matchup_teams(matchup: str):
    """
    Parse home and away team abbreviations from nba_api MATCHUP string.
    'DEN vs. LAL' → home='DEN', away='LAL'  (DEN is home)
    'DEN @ LAL'   → home='LAL', away='DEN'  (DEN is away, LAL is home)
    Returns (home_abbrev, away_abbrev).
    """
    if " vs. " in matchup:
        home, away = matchup.split(" vs. ")
        return home.strip(), away.strip()
    elif " @ " in matchup:
        away, home = matchup.split(" @ ")
        return home.strip(), away.strip()
    raise ValueError(f"Unrecognised matchup format: {matchup!r}")


def link_nba_game_ids(conn):
    """
    For each unique game in team_game_logs, find the matching game in the games table
    by date (ET-converted) and team abbreviations, then set games.nba_game_id.
    Unmatched games are logged as warnings.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT nba_game_id, game_date, matchup
            FROM team_game_logs
            WHERE nba_game_id IS NOT NULL
            """
        )
        rows = cur.fetchall()

    if not rows:
        return

    updated = 0
    unmatched = []

    with conn.cursor() as cur:
        for nba_game_id, game_date, matchup in rows:
            try:
                home_abbrev, away_abbrev = parse_matchup_teams(matchup)
            except ValueError as e:
                logger.warning("Skipping unparseable matchup: %s", e)
                continue

            # Primary match: exact date (ET) + team abbreviations
            cur.execute(
                """
                UPDATE games
                SET nba_game_id = %s
                WHERE nba_game_id IS NULL
                  AND DATE(commence_time AT TIME ZONE 'America/New_York') = %s
                  AND home_team ILIKE %s
                  AND away_team ILIKE %s
                """,
                (nba_game_id, game_date, f"%{home_abbrev}%", f"%{away_abbrev}%"),
            )
            if cur.rowcount > 0:
                updated += cur.rowcount
                continue

            # Fallback: ±1 day window
            cur.execute(
                """
                UPDATE games
                SET nba_game_id = %s
                WHERE nba_game_id IS NULL
                  AND DATE(commence_time AT TIME ZONE 'America/New_York')
                      BETWEEN %s::date - INTERVAL '1 day' AND %s::date + INTERVAL '1 day'
                  AND home_team ILIKE %s
                  AND away_team ILIKE %s
                """,
                (nba_game_id, game_date, game_date, f"%{home_abbrev}%", f"%{away_abbrev}%"),
            )
            if cur.rowcount > 0:
                updated += cur.rowcount
            else:
                unmatched.append(nba_game_id)

    conn.commit()

    if unmatched:
        logger.warning(
            "link_nba_game_ids: %d game(s) could not be matched to games table: %s",
            len(unmatched), unmatched,
        )
    logger.info("link_nba_game_ids: updated %d game(s)", updated)
