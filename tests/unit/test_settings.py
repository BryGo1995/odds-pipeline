# tests/unit/test_settings.py

def test_sport_is_nba():
    from config.settings import SPORT
    assert SPORT == "basketball_nba"


def test_markets_is_list_with_player_props():
    from config.settings import PLAYER_PROP_MARKETS
    assert isinstance(PLAYER_PROP_MARKETS, list)
    assert "player_points" in PLAYER_PROP_MARKETS
    assert "player_rebounds" in PLAYER_PROP_MARKETS
    assert "player_assists" in PLAYER_PROP_MARKETS


def test_bookmakers_is_nonempty_list():
    from config.settings import BOOKMAKERS
    assert isinstance(BOOKMAKERS, list)
    assert len(BOOKMAKERS) > 0


def test_regions_is_nonempty_list():
    from config.settings import REGIONS
    assert isinstance(REGIONS, list)
    assert len(REGIONS) > 0
