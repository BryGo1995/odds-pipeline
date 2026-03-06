# tests/unit/test_odds_api_client.py
import pytest
from unittest.mock import MagicMock, patch


def make_mock_response(json_data, status_code=200):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    return mock


def test_fetch_events_calls_correct_url():
    from plugins.odds_api_client import fetch_events
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([{"id": "abc"}])
        result = fetch_events(api_key="test_key", sport="basketball_nba")
        url = mock_get.call_args[0][0]
        assert "basketball_nba/events" in url
        assert result == [{"id": "abc"}]


def test_fetch_odds_sends_markets_and_bookmakers():
    from plugins.odds_api_client import fetch_odds
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([])
        fetch_odds(
            api_key="test_key",
            sport="basketball_nba",
            regions=["us"],
            markets=["h2h", "player_props"],
            bookmakers=["draftkings"],
        )
        params = mock_get.call_args[1]["params"]
        assert params["markets"] == "h2h,player_props"
        assert params["bookmakers"] == "draftkings"


def test_fetch_scores_calls_correct_url_with_days_from():
    from plugins.odds_api_client import fetch_scores
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([])
        fetch_scores(api_key="test_key", sport="basketball_nba", days_from=3)
        url = mock_get.call_args[0][0]
        assert "basketball_nba/scores" in url
        params = mock_get.call_args[1]["params"]
        assert params["daysFrom"] == 3


def test_fetch_raises_on_http_error():
    import requests
    from plugins.odds_api_client import fetch_events
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("401")
        mock_get.return_value = mock_response
        with pytest.raises(requests.HTTPError):
            fetch_events(api_key="bad_key", sport="basketball_nba")
