# plugins/watchdog.py
import pendulum

# How far back to look for game days. Long enough to count a multi-week
# outage, short enough that the off-season gap ends the streak.
LOOKBACK_DAYS = 30


def consecutive_missed(slates):
    """
    Given (game_date, has_recommendations) pairs, newest first, return the
    game dates without recommendations up to the newest one that has them.
    """
    missed = []
    for game_date, has_recs in slates:
        if has_recs:
            break
        missed.append(game_date)
    return missed


def find_missed_slates(conn, games_sport, rec_sport, as_of, lookback_days=LOOKBACK_DAYS):
    """
    Return (missed, last_rec_date) for the sport as of the as_of slate date.

    missed is the list of consecutive game days (newest first, ending at
    as_of) that had games in `games` but no rows in `recommendations`.
    last_rec_date is the newest game_date with recommendations, or None.
    See #51 — the daily chain failed for 11 days before anyone noticed.
    """
    start = pendulum.parse(as_of).subtract(days=lookback_days).to_date_string()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.game_date,
                   EXISTS (
                       SELECT 1 FROM recommendations r
                       WHERE r.sport = %s AND r.game_date = s.game_date
                   )
            FROM (
                SELECT DISTINCT (commence_time AT TIME ZONE 'America/New_York')::date AS game_date
                FROM games
                WHERE sport = %s
            ) s
            WHERE s.game_date BETWEEN %s AND %s
            ORDER BY s.game_date DESC
            """,
            (rec_sport, games_sport, start, as_of),
        )
        slates = cur.fetchall()

        cur.execute(
            "SELECT MAX(game_date) FROM recommendations WHERE sport = %s AND game_date <= %s",
            (rec_sport, as_of),
        )
        last_rec_date = cur.fetchone()[0]

    return consecutive_missed(slates), last_rec_date
