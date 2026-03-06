# tests/unit/test_settings.py

def test_sport_is_nba():
    from config.settings import SPORT
    assert SPORT == "basketball_nba"


def test_markets_is_list_with_player_props():
    from config.settings import MARKETS
    assert isinstance(MARKETS, list)
    assert "player_props" in MARKETS


def test_bookmakers_is_nonempty_list():
    from config.settings import BOOKMAKERS
    assert isinstance(BOOKMAKERS, list)
    assert len(BOOKMAKERS) > 0


def test_regions_is_nonempty_list():
    from config.settings import REGIONS
    assert isinstance(REGIONS, list)
    assert len(REGIONS) > 0
