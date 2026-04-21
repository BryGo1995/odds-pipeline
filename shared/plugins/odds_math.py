"""Pure math helpers for converting between odds formats and probabilities.

Sport-agnostic — no imports from sport-specific packages.
"""


def american_odds_to_implied_prob(price) -> float:
    """Convert American odds (e.g. -110, +200) to implied probability in [0, 1]."""
    price = float(price)
    if price > 0:
        return 100.0 / (price + 100.0)
    return abs(price) / (abs(price) + 100.0)
