def test_mlb_config_imports():
    from mlb.config import (
        SPORT, REGIONS, MARKETS, PLAYER_PROP_MARKETS, BOOKMAKERS,
        ODDS_FORMAT, SCORES_DAYS_FROM,
    )
    assert SPORT == "baseball_mlb"
    assert PLAYER_PROP_MARKETS == ["batter_hits", "batter_total_bases", "batter_home_runs"]
    assert "h2h" in MARKETS
