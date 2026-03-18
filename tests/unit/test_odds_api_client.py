# tests/unit/test_odds_api_client.py
import pytest
from unittest.mock import MagicMock, patch


def make_mock_response(json_data, status_code=200, remaining=500):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data
    mock.raise_for_status = MagicMock()
    mock.headers = {"x-requests-remaining": str(remaining)}
    return mock


def test_fetch_events_calls_correct_url():
    from plugins.odds_api_client import fetch_events
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([{"id": "abc"}])
        data, remaining = fetch_events(api_key="test_key", sport="basketball_nba")
        url = mock_get.call_args[0][0]
        assert "basketball_nba/events" in url
        assert data == [{"id": "abc"}]


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


def test_fetch_events_returns_data_and_remaining():
    from plugins.odds_api_client import fetch_events
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([{"id": "abc"}], remaining=423)
        data, remaining = fetch_events(api_key="test_key", sport="basketball_nba")
        assert data == [{"id": "abc"}]
        assert remaining == 423


def test_fetch_odds_returns_data_and_remaining():
    from plugins.odds_api_client import fetch_odds
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([], remaining=300)
        data, remaining = fetch_odds(
            api_key="test_key",
            sport="basketball_nba",
            regions=["us"],
            markets=["h2h"],
            bookmakers=["draftkings"],
        )
        assert remaining == 300


def test_fetch_scores_returns_data_and_remaining():
    from plugins.odds_api_client import fetch_scores
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([], remaining=0)
        data, remaining = fetch_scores(api_key="test_key", sport="basketball_nba")
        assert remaining == 0


def test_fetch_events_forwards_extra_params():
    from plugins.odds_api_client import fetch_events
    with patch("plugins.odds_api_client.requests.get") as mock_get:
        mock_get.return_value = make_mock_response([])
        fetch_events(
            api_key="test_key",
            sport="basketball_nba",
            commenceTimeFrom="2024-01-01T00:00:00Z",
        )
        params = mock_get.call_args[1]["params"]
        assert params["commenceTimeFrom"] == "2024-01-01T00:00:00Z"
