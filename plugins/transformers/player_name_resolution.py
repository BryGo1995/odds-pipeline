# plugins/transformers/player_name_resolution.py
import logging
import unicodedata

from rapidfuzz import fuzz

from plugins.slack_notifier import send_slack_message

CONFIDENCE_THRESHOLD = 95.0

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """Lowercase, strip accents, remove Jr./Sr./II/III/IV suffixes."""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower().strip()
    for suffix in [" jr.", " jr", " sr.", " sr", " iii", " ii", " iv"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
            break
    return name


def resolve_player_ids(conn, slack_webhook_url=None):
    """
    Find all Odds API player names in player_props not yet in player_name_mappings.
    Fuzzy-match against players table. Insert high-confidence matches (>=95%).
    Send Slack alert for unresolved names (<95%).
    Then populate player_props.nba_player_id for all resolved names.
    """
    with conn.cursor() as cur:
        # Unresolved names: in player_props but not yet in player_name_mappings
        cur.execute(
            """
            SELECT DISTINCT pp.player_name
            FROM player_props pp
            WHERE pp.player_name IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM player_name_mappings m
                WHERE m.odds_api_name = pp.player_name
              )
            """
        )
        unresolved = [row[0] for row in cur.fetchall()]

        if not unresolved:
            return

        # Load all known players for matching
        cur.execute("SELECT player_id, full_name FROM players")
        known_players = cur.fetchall()  # list of (player_id, full_name)

    unresolved_names = []

    with conn.cursor() as cur:
        for odds_name in unresolved:
            norm_odds = normalize_name(odds_name)
            best_score = 0.0
            best_player_id = None

            for player_id, full_name in known_players:
                score = fuzz.ratio(norm_odds, normalize_name(full_name))
                if score > best_score:
                    best_score = score
                    best_player_id = player_id

            if best_score >= CONFIDENCE_THRESHOLD:
                cur.execute(
                    """
                    INSERT INTO player_name_mappings (odds_api_name, nba_player_id, confidence)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (odds_api_name) DO NOTHING
                    """,
                    (odds_name, best_player_id, best_score),
                )
            else:
                logger.warning("Low confidence match for '%s': score=%.1f", odds_name, best_score)
                unresolved_names.append((odds_name, best_score))

        # Backfill player_props.nba_player_id for all mapped names
        cur.execute(
            """
            UPDATE player_props pp
            SET nba_player_id = m.nba_player_id
            FROM player_name_mappings m
            WHERE pp.player_name = m.odds_api_name
              AND pp.nba_player_id IS NULL
            """
        )

    conn.commit()

    if unresolved_names and slack_webhook_url:
        lines = "\n".join(
            f"  • {name} (best score: {score:.1f})"
            for name, score in unresolved_names
        )
        send_slack_message(
            slack_webhook_url,
            f":warning: *Player name resolution* — {len(unresolved_names)} unresolved name(s) need manual review:\n{lines}",
        )
