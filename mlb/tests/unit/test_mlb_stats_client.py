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
