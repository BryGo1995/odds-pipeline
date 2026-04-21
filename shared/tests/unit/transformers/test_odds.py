# tests/unit/transformers/test_odds.py
from unittest.mock import MagicMock

SAMPLE_ODDS = [
    {
        "id": "abc123",
        "bookmakers": [
            {
                "key": "draftkings",
                "markets": [
                    {
                        "key": "h2h",
                        "last_update": "2024-01-15T00:00:00Z",
                        "outcomes": [
                            {"name": "Los Angeles Lakers", "price": -110},
                            {"name": "Boston Celtics", "price": -110},
                        ],
                    }
                ],
            }
        ],
    }
]


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def test_transform_odds_inserts_one_row_per_outcome():
    from shared.plugins.transformers.odds import transform_odds
    mock_conn, mock_cursor = _make_mock_conn()
    transform_odds(conn=mock_conn, raw_odds=SAMPLE_ODDS)
    assert mock_cursor.execute.call_count == 2  # 2 outcomes
    mock_conn.commit.assert_called_once()


def test_transform_odds_skips_player_props_market():
    from shared.plugins.transformers.odds import transform_odds
    mock_conn, mock_cursor = _make_mock_conn()
    raw = [{"id": "x", "bookmakers": [{"key": "dk", "markets": [
        {"key": "player_points", "outcomes": [{"name": "Over", "price": -115}]}
    ]}]}]
    transform_odds(conn=mock_conn, raw_odds=raw)
    mock_cursor.execute.assert_not_called()


def test_transform_odds_empty():
    from shared.plugins.transformers.odds import transform_odds
    mock_conn, mock_cursor = _make_mock_conn()
    transform_odds(conn=mock_conn, raw_odds=[])
    mock_cursor.execute.assert_not_called()


def test_odds_skips_mlb_batter_prop_markets():
    """batter_* markets should be skipped by transform_odds (they're player-level, not game-level)."""
    from shared.plugins.transformers.odds import transform_odds
    mock_conn, mock_cursor = _make_mock_conn()
    raw = [{
        "id": "mlb-game-1",
        "bookmakers": [{
            "key": "draftkings",
            "markets": [
                {
                    "key": "h2h",
                    "last_update": "2026-04-20T15:00:00Z",
                    "outcomes": [
                        {"name": "Home", "price": -120},
                        {"name": "Away", "price": +100},
                    ],
                },
                {
                    "key": "batter_hits",
                    "last_update": "2026-04-20T15:00:00Z",
                    "outcomes": [
                        {"description": "Mookie Betts", "name": "Over", "price": -110, "point": 1.5},
                    ],
                },
            ],
        }],
    }]

    transform_odds(conn=mock_conn, raw_odds=raw)

    # Only h2h should be inserted into odds; batter_hits should be skipped
    inserts = [
        call.args for call in mock_cursor.execute.call_args_list
        if "INSERT INTO odds" in call.args[0]
    ]
    assert len(inserts) == 2  # 2 h2h outcomes
    for _, params in inserts:
        assert params[2] == "h2h"  # market_type column
