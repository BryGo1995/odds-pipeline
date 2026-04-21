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
    from shared.plugins.transformers.player_props import transform_player_props
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
    from shared.plugins.transformers.player_props import transform_player_props
    mock_conn, mock_cursor = _make_mock_conn()
    raw = [{"id": "x", "bookmakers": [{"key": "dk", "markets": [
        {"key": "h2h", "outcomes": [{"name": "Lakers"}]}
    ]}]}]
    transform_player_props(conn=mock_conn, raw_odds=raw)
    mock_cursor.execute.assert_not_called()


def test_transform_player_props_skips_empty_list():
    from shared.plugins.transformers.player_props import transform_player_props
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_props(conn=mock_conn, raw_odds=[])
    mock_cursor.execute.assert_not_called()


def test_player_props_includes_mlb_batter_markets():
    """batter_* markets should be inserted into player_props."""
    from shared.plugins.transformers.player_props import transform_player_props
    mock_conn, mock_cursor = _make_mock_conn()
    raw = [{
        "id": "mlb-game-1",
        "bookmakers": [{
            "key": "draftkings",
            "markets": [
                {
                    "key": "h2h",
                    "last_update": "2026-04-20T15:00:00Z",
                    "outcomes": [{"name": "Home", "price": -120}],
                },
                {
                    "key": "batter_hits",
                    "last_update": "2026-04-20T15:00:00Z",
                    "outcomes": [
                        {"description": "Mookie Betts", "name": "Over", "price": -110, "point": 1.5},
                    ],
                },
                {
                    "key": "batter_home_runs",
                    "last_update": "2026-04-20T15:00:00Z",
                    "outcomes": [
                        {"description": "Aaron Judge", "name": "Over", "price": +300, "point": 0.5},
                    ],
                },
            ],
        }],
    }]

    transform_player_props(conn=mock_conn, raw_odds=raw)

    inserts = [
        call.args for call in mock_cursor.execute.call_args_list
        if "INSERT INTO player_props" in call.args[0]
    ]
    # h2h is skipped; 1 batter_hits outcome + 1 batter_home_runs outcome
    assert len(inserts) == 2
    prop_types = {params[3] for _, params in inserts}
    assert prop_types == {"batter_hits", "batter_home_runs"}
