# tests/unit/transformers/test_player_props.py
from unittest.mock import MagicMock

SAMPLE_PLAYER_PROPS = [
    {
        "id": "abc123",
        "bookmakers": [
            {
                "key": "draftkings",
                "markets": [
                    {
                        "key": "player_points",
                        "last_update": "2024-01-15T00:00:00Z",
                        "outcomes": [
                            {"name": "Over",  "description": "LeBron James", "price": -115, "point": 24.5},
                            {"name": "Under", "description": "LeBron James", "price": -105, "point": 24.5},
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


def test_transform_player_props_inserts_one_row_per_outcome():
    from plugins.transformers.player_props import transform_player_props
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_props(conn=mock_conn, raw_odds=SAMPLE_PLAYER_PROPS)
    assert mock_cursor.execute.call_count == 2
    args = mock_cursor.execute.call_args_list[0][0][1]
    assert args[2] == "LeBron James"   # player_name
    assert args[3] == "player_points"  # prop_type
    assert args[4] == "Over"           # outcome
    assert args[5] == -115             # price
    assert args[6] == 24.5             # point


def test_transform_player_props_skips_non_prop_markets():
    from plugins.transformers.player_props import transform_player_props
    mock_conn, mock_cursor = _make_mock_conn()
    raw = [{"id": "x", "bookmakers": [{"key": "dk", "markets": [
        {"key": "h2h", "outcomes": [{"name": "Lakers"}]}
    ]}]}]
    transform_player_props(conn=mock_conn, raw_odds=raw)
    mock_cursor.execute.assert_not_called()
