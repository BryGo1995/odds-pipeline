# mlb/tests/unit/test_mlb_stats_client.py
from unittest.mock import MagicMock, patch

import requests


def _mock_response(status_code, json_data=None):
    """Build a MagicMock response object mimicking requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            f"{status_code}", response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


def test_module_imports():
    from mlb.plugins import mlb_stats_client
    assert mlb_stats_client._BASE_URL == "https://statsapi.mlb.com/api/v1"
    assert mlb_stats_client._SPORT_ID == 1


# --- _get helper ---

def test_get_success_returns_json():
    from mlb.plugins.mlb_stats_client import _get
    with patch("mlb.plugins.mlb_stats_client.requests.get") as mock_get, \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        mock_get.return_value = _mock_response(200, {"ok": True})
        result = _get("/teams", {"sportId": 1}, delay_seconds=0)
    assert result == {"ok": True}
    mock_get.assert_called_once()
    call_kwargs = mock_get.call_args.kwargs
    assert call_kwargs["params"] == {"sportId": 1}
    assert call_kwargs["headers"]["User-Agent"].startswith("odds-pipeline/mlb")
    assert call_kwargs["timeout"] == 30


def test_get_retries_on_429_then_succeeds():
    from mlb.plugins.mlb_stats_client import _get
    responses = [_mock_response(429), _mock_response(200, {"ok": True})]
    with patch("mlb.plugins.mlb_stats_client.requests.get", side_effect=responses), \
         patch("mlb.plugins.mlb_stats_client.time.sleep") as mock_sleep:
        result = _get("/x", {}, delay_seconds=0)
    assert result == {"ok": True}
    # Two sleep calls: one pre-request (delay_seconds=0) and one backoff (30s)
    sleep_args = [c.args[0] for c in mock_sleep.call_args_list]
    assert 30 in sleep_args


def test_get_retries_on_500_then_succeeds():
    from mlb.plugins.mlb_stats_client import _get
    responses = [_mock_response(500), _mock_response(200, {"ok": True})]
    with patch("mlb.plugins.mlb_stats_client.requests.get", side_effect=responses), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = _get("/x", {}, delay_seconds=0)
    assert result == {"ok": True}


def test_get_exhausts_retries_and_raises():
    from mlb.plugins.mlb_stats_client import _get
    with patch("mlb.plugins.mlb_stats_client.requests.get",
               return_value=_mock_response(429)), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        import pytest as _pytest
        with _pytest.raises(requests.HTTPError):
            _get("/x", {}, delay_seconds=0)


def test_get_does_not_retry_on_404():
    from mlb.plugins.mlb_stats_client import _get
    with patch("mlb.plugins.mlb_stats_client.requests.get",
               return_value=_mock_response(404)) as mock_get, \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        import pytest as _pytest
        with _pytest.raises(requests.HTTPError):
            _get("/x", {}, delay_seconds=0)
    assert mock_get.call_count == 1


def test_get_retries_on_timeout():
    from mlb.plugins.mlb_stats_client import _get
    side_effects = [requests.Timeout("slow"), _mock_response(200, {"ok": True})]
    with patch("mlb.plugins.mlb_stats_client.requests.get", side_effect=side_effects), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = _get("/x", {}, delay_seconds=0)
    assert result == {"ok": True}
