import math

import pytest

from shared.plugins.odds_math import american_odds_to_implied_prob


def test_negative_odds_favorite():
    # -110 → 110 / (110 + 100) = 0.5238...
    assert math.isclose(american_odds_to_implied_prob(-110), 110 / 210, rel_tol=1e-9)


def test_positive_odds_underdog():
    # +200 → 100 / (200 + 100) = 0.3333...
    assert math.isclose(american_odds_to_implied_prob(200), 100 / 300, rel_tol=1e-9)


def test_plus_one_hundred_even_money():
    assert math.isclose(american_odds_to_implied_prob(100), 0.5, rel_tol=1e-9)


def test_string_input_is_coerced():
    assert math.isclose(american_odds_to_implied_prob("-110"), 110 / 210, rel_tol=1e-9)


@pytest.mark.parametrize("price,expected", [
    (-200, 200 / 300),
    (-150, 150 / 250),
    (+150, 100 / 250),
    (+500, 100 / 600),
])
def test_known_pairs(price, expected):
    assert math.isclose(american_odds_to_implied_prob(price), expected, rel_tol=1e-9)
