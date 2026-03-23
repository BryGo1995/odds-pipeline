from unittest.mock import MagicMock, patch


def test_normalize_name_strips_accents():
    from plugins.transformers.player_name_resolution import normalize_name
    assert normalize_name("Nikola Jokić") == "nikola jokic"


def test_normalize_name_removes_jr_suffix():
    from plugins.transformers.player_name_resolution import normalize_name
    assert normalize_name("Gary Trent Jr.") == "gary trent"
    assert normalize_name("Gary Trent Jr") == "gary trent"


def test_normalize_name_removes_sr_suffix():
    from plugins.transformers.player_name_resolution import normalize_name
    assert normalize_name("Marcus Morris Sr.") == "marcus morris"


def test_normalize_name_removes_roman_numeral_suffixes():
    from plugins.transformers.player_name_resolution import normalize_name
    assert normalize_name("Otto Porter III") == "otto porter"
    assert normalize_name("Gary Payton II") == "gary payton"


def test_normalize_name_lowercases_and_strips_whitespace():
    from plugins.transformers.player_name_resolution import normalize_name
    assert normalize_name("  LeBron James  ") == "lebron james"


def test_resolve_player_ids_inserts_high_confidence_match():
    from plugins.transformers.player_name_resolution import resolve_player_ids

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Simulate: 1 unresolved name in player_props, 1 player in players table
    mock_cursor.fetchall.side_effect = [
        [("Nikola Jokic",)],           # unresolved names from player_props
        [(2544, "Nikola Jokić")],       # players in players table
    ]

    with patch("plugins.transformers.player_name_resolution.send_slack_message") as mock_slack:
        resolve_player_ids(mock_conn, slack_webhook_url="http://example.com")

    # Should insert 1 mapping (high confidence)
    insert_calls = [
        c for c in mock_cursor.execute.call_args_list
        if "INSERT INTO player_name_mappings" in str(c)
    ]
    assert len(insert_calls) == 1
    args = insert_calls[0][0][1]
    assert args[0] == "Nikola Jokic"  # odds_api_name
    assert args[1] == 2544            # nba_player_id
    assert args[2] >= 95.0            # confidence

    # No Slack alert for high confidence match
    mock_slack.assert_not_called()


def test_resolve_player_ids_sends_slack_for_low_confidence():
    from plugins.transformers.player_name_resolution import resolve_player_ids

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.side_effect = [
        [("Zyx Unkown Player",)],      # name with no good match
        [(9999, "Totally Different")],  # only player in DB
    ]

    with patch("plugins.transformers.player_name_resolution.send_slack_message") as mock_slack:
        resolve_player_ids(mock_conn, slack_webhook_url="http://example.com")

    mock_slack.assert_called_once()
    # Should NOT insert a mapping for low confidence
    insert_calls = [
        c for c in mock_cursor.execute.call_args_list
        if "INSERT INTO player_name_mappings" in str(c)
    ]
    assert len(insert_calls) == 0
