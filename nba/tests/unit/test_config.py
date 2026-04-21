def test_nba_config_imports():
    from nba.config import (
        SPORT, REGIONS, MARKETS, PLAYER_PROP_MARKETS, BOOKMAKERS,
        ODDS_FORMAT, SCORES_DAYS_FROM,
    )
    assert SPORT == "basketball_nba"
    assert "player_points" in PLAYER_PROP_MARKETS
    assert "h2h" in MARKETS
