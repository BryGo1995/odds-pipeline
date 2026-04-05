# nba/tests/unit/test_slack_notifier_settle.py
from datetime import date
from unittest.mock import patch


_GAME_DATE = date(2026, 4, 3)

_RESULTS = [
    {"player_name": "LeBron James",  "prop_type": "player_points",   "line": 24.5, "outcome": "Over", "actual_result": True,  "actual_stat_value": 27.0, "edge": 0.11},
    {"player_name": "Steph Curry",   "prop_type": "player_threes",   "line": 3.5,  "outcome": "Over", "actual_result": True,  "actual_stat_value": 5.0,  "edge": 0.09},
    {"player_name": "Jayson Tatum",  "prop_type": "player_rebounds", "line": 7.5,  "outcome": "Over", "actual_result": False, "actual_stat_value": 6.0,  "edge": 0.07},
]


def test_notify_picks_settled_posts_to_slack():
    from shared.plugins.slack_notifier import notify_picks_settled
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, _RESULTS)
    mock_post.assert_called_once()


def test_notify_picks_settled_includes_hit_rate():
    from shared.plugins.slack_notifier import notify_picks_settled
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, _RESULTS)
    text = mock_post.call_args.args[0]
    assert "2/3" in text
    assert "67%" in text or "66%" in text


def test_notify_picks_settled_includes_player_names():
    from shared.plugins.slack_notifier import notify_picks_settled
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, _RESULTS)
    text = mock_post.call_args.args[0]
    assert "LeBron James" in text
    assert "Jayson Tatum" in text


def test_notify_picks_settled_marks_hits_and_misses():
    from shared.plugins.slack_notifier import notify_picks_settled
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, _RESULTS)
    text = mock_post.call_args.args[0]
    assert "✅" in text
    assert "❌" in text


def test_notify_picks_settled_skips_when_no_webhook():
    from shared.plugins.slack_notifier import notify_picks_settled
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", None), \
         patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, _RESULTS)
    mock_post.assert_not_called()


def test_notify_picks_settled_shows_unresolvable_as_unknown():
    from shared.plugins.slack_notifier import notify_picks_settled
    results_with_unknown = [
        {"player_name": "DNP Player", "prop_type": "player_points", "line": 20.0,
         "outcome": "Over", "actual_result": None, "actual_stat_value": None, "edge": 0.05},
    ]
    with patch("shared.plugins.slack_notifier._WEBHOOK_URL", "https://hooks.slack.com/test"), \
         patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, results_with_unknown)
    text = mock_post.call_args.args[0]
    assert "DNP Player" in text
    assert "\u2014" in text  # unresolvable shown with dash
