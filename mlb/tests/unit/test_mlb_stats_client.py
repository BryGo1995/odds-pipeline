# mlb/tests/unit/test_mlb_stats_client.py
from unittest.mock import MagicMock, patch

import pytest
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
        with pytest.raises(requests.HTTPError):
            _get("/x", {}, delay_seconds=0)


def test_get_does_not_retry_on_404():
    from mlb.plugins.mlb_stats_client import _get
    with patch("mlb.plugins.mlb_stats_client.requests.get",
               return_value=_mock_response(404)) as mock_get, \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        with pytest.raises(requests.HTTPError):
            _get("/x", {}, delay_seconds=0)
    assert mock_get.call_count == 1


def test_get_retries_on_timeout():
    from mlb.plugins.mlb_stats_client import _get
    side_effects = [requests.Timeout("slow"), _mock_response(200, {"ok": True})]
    with patch("mlb.plugins.mlb_stats_client.requests.get", side_effect=side_effects), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = _get("/x", {}, delay_seconds=0)
    assert result == {"ok": True}


# --- fetch_teams ---

def test_fetch_teams_maps_fields():
    from mlb.plugins.mlb_stats_client import fetch_teams
    fixture = {"teams": [{
        "id": 108,
        "name": "Los Angeles Angels",
        "abbreviation": "LAA",
        "league": {"id": 103, "name": "American League"},
        "division": {"id": 200, "name": "American League West"},
    }]}
    with patch("mlb.plugins.mlb_stats_client.requests.get",
               return_value=_mock_response(200, fixture)) as mock_get, \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = fetch_teams(delay_seconds=0)

    assert result == [{
        "team_id": 108,
        "full_name": "Los Angeles Angels",
        "abbreviation": "LAA",
        "league": "American League",
        "division": "American League West",
    }]
    # Ensure correct endpoint + params
    call = mock_get.call_args
    assert call.args[0] == "https://statsapi.mlb.com/api/v1/teams"
    assert call.kwargs["params"] == {"sportId": 1, "activeStatus": "Y"}


def test_fetch_teams_handles_missing_division():
    from mlb.plugins.mlb_stats_client import fetch_teams
    fixture = {"teams": [{
        "id": 999,
        "name": "Some Team",
        "abbreviation": "XXX",
        "league": {"name": "American League"},
        # no 'division' key
    }]}
    with patch("mlb.plugins.mlb_stats_client.requests.get",
               return_value=_mock_response(200, fixture)), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = fetch_teams(delay_seconds=0)

    assert len(result) == 1
    assert result[0]["division"] is None
    assert result[0]["league"] == "American League"


# --- fetch_players ---

def test_fetch_players_active_only_appends_param():
    from mlb.plugins.mlb_stats_client import fetch_players
    with patch("mlb.plugins.mlb_stats_client.requests.get",
               return_value=_mock_response(200, {"people": []})) as mock_get, \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        fetch_players(season="2026", active_only=True, delay_seconds=0)
    assert mock_get.call_args.kwargs["params"] == {
        "season": "2026", "activeStatus": "Y"
    }

    with patch("mlb.plugins.mlb_stats_client.requests.get",
               return_value=_mock_response(200, {"people": []})) as mock_get, \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        fetch_players(season="2026", active_only=False, delay_seconds=0)
    assert mock_get.call_args.kwargs["params"] == {"season": "2026"}


def test_fetch_players_maps_fields():
    from mlb.plugins.mlb_stats_client import fetch_players
    fixture = {"people": [
        {
            "id": 660271,
            "fullName": "Shohei Ohtani",
            "primaryPosition": {"abbreviation": "DH"},
            "batSide": {"code": "L"},
            "pitchHand": {"code": "R"},
            "currentTeam": {"id": 119},
            "active": True,
        },
        {
            "id": 592450,
            "fullName": "Gerrit Cole",
            "primaryPosition": {"abbreviation": "P"},
            "batSide": {"code": "R"},
            "pitchHand": {"code": "R"},
            "currentTeam": {"id": 147},
            "active": True,
        },
    ]}
    with patch("mlb.plugins.mlb_stats_client.requests.get",
               return_value=_mock_response(200, fixture)), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = fetch_players(season="2026", delay_seconds=0)

    assert result == [
        {
            "player_id": 660271,
            "full_name": "Shohei Ohtani",
            "position": "DH",
            "bats": "L",
            "throws": "R",
            "team_id": 119,
            "team_abbreviation": None,
            "is_active": True,
        },
        {
            "player_id": 592450,
            "full_name": "Gerrit Cole",
            "position": "P",
            "bats": "R",
            "throws": "R",
            "team_id": 147,
            "team_abbreviation": None,
            "is_active": True,
        },
    ]


def test_fetch_players_free_agent_has_null_team():
    from mlb.plugins.mlb_stats_client import fetch_players
    fixture = {"people": [{
        "id": 100,
        "fullName": "Free Agent",
        "primaryPosition": {"abbreviation": "OF"},
        "batSide": {"code": "R"},
        "pitchHand": {"code": "R"},
        # no 'currentTeam' key
        "active": True,
    }]}
    with patch("mlb.plugins.mlb_stats_client.requests.get",
               return_value=_mock_response(200, fixture)), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = fetch_players(season="2026", delay_seconds=0)
    assert len(result) == 1
    assert result[0]["team_id"] is None


# --- fetch_batter_game_logs ---

def _schedule_fixture(games_by_date):
    """games_by_date: {date_str: [game_dict, ...]}"""
    return {"dates": [
        {"date": d, "games": g} for d, g in games_by_date.items()
    ]}


def _game(game_pk, status, home_id, home_abbr, away_id, away_abbr):
    return {
        "gamePk": game_pk,
        "status": {"abstractGameState": status},
        "teams": {
            "home": {"team": {"id": home_id, "abbreviation": home_abbr}},
            "away": {"team": {"id": away_id, "abbreviation": away_abbr}},
        },
    }


def _boxscore_fixture(home_team_id, away_team_id, home_batters, away_batters):
    """home_batters / away_batters: list of dicts like
    {"id": 660271, "pa": 4, "ab": 3, "h": 1, ...}
    """
    def _player_entry(b):
        return {
            "person": {"id": b["id"]},
            "stats": {"batting": {
                "plateAppearances": b["pa"],
                "atBats": b.get("ab", 0),
                "hits": b.get("h", 0),
                "doubles": b.get("2b", 0),
                "triples": b.get("3b", 0),
                "homeRuns": b.get("hr", 0),
                "rbi": b.get("rbi", 0),
                "runs": b.get("r", 0),
                "baseOnBalls": b.get("bb", 0),
                "strikeOuts": b.get("k", 0),
                "stolenBases": b.get("sb", 0),
                "totalBases": b.get("tb", 0),
            }},
        }
    return {"teams": {
        "home": {
            "team": {"id": home_team_id},
            "players": {f"ID{b['id']}": _player_entry(b) for b in home_batters},
        },
        "away": {
            "team": {"id": away_team_id},
            "players": {f"ID{b['id']}": _player_entry(b) for b in away_batters},
        },
    }}


def test_fetch_batter_game_logs_single_game():
    from mlb.plugins.mlb_stats_client import fetch_batter_game_logs

    schedule = _schedule_fixture({"2026-04-22": [
        _game(745123, "Final", home_id=136, home_abbr="SEA",
              away_id=108, away_abbr="LAA"),
    ]})
    boxscore = _boxscore_fixture(
        home_team_id=136, away_team_id=108,
        home_batters=[{"id": 1, "pa": 4, "ab": 3, "h": 2, "tb": 3, "rbi": 1}],
        away_batters=[{"id": 2, "pa": 3, "ab": 3, "h": 1, "tb": 1}],
    )

    with patch("mlb.plugins.mlb_stats_client.requests.get",
               side_effect=[_mock_response(200, schedule),
                            _mock_response(200, boxscore)]), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = fetch_batter_game_logs("2026-04-22", "2026-04-22", delay_seconds=0)

    assert len(result) == 2
    home_row = next(r for r in result if r["team_id"] == 136)
    away_row = next(r for r in result if r["team_id"] == 108)
    assert home_row["mlb_game_pk"] == "745123"
    assert home_row["opponent_team_id"] == 108
    assert home_row["matchup"] == "LAA @ SEA"
    assert home_row["season"] == "2026"
    assert home_row["game_date"] == "2026-04-22"
    assert home_row["plate_appearances"] == 4
    assert home_row["hits"] == 2
    assert home_row["total_bases"] == 3
    assert away_row["opponent_team_id"] == 136
    assert away_row["matchup"] == "LAA @ SEA"


def test_fetch_batter_game_logs_skips_non_final_games():
    from mlb.plugins.mlb_stats_client import fetch_batter_game_logs

    schedule = _schedule_fixture({"2026-04-22": [
        _game(100, "Final",       home_id=1, home_abbr="AAA", away_id=2, away_abbr="BBB"),
        _game(200, "Preview",     home_id=1, home_abbr="AAA", away_id=2, away_abbr="BBB"),
        _game(300, "Live",        home_id=1, home_abbr="AAA", away_id=2, away_abbr="BBB"),
    ]})
    final_box = _boxscore_fixture(
        home_team_id=1, away_team_id=2,
        home_batters=[{"id": 10, "pa": 4, "h": 1}],
        away_batters=[],
    )

    with patch("mlb.plugins.mlb_stats_client.requests.get",
               side_effect=[_mock_response(200, schedule),
                            _mock_response(200, final_box)]) as mock_get, \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = fetch_batter_game_logs("2026-04-22", "2026-04-22", delay_seconds=0)

    # Only the Final boxscore was fetched (1 schedule + 1 boxscore = 2 calls)
    assert mock_get.call_count == 2
    assert len(result) == 1
    assert result[0]["mlb_game_pk"] == "100"


def test_fetch_batter_game_logs_excludes_bench_players():
    from mlb.plugins.mlb_stats_client import fetch_batter_game_logs

    schedule = _schedule_fixture({"2026-04-22": [
        _game(100, "Final", home_id=1, home_abbr="AAA", away_id=2, away_abbr="BBB"),
    ]})
    box = _boxscore_fixture(
        home_team_id=1, away_team_id=2,
        home_batters=[
            {"id": 10, "pa": 4, "h": 1},
            {"id": 11, "pa": 0},  # bench — must be filtered
        ],
        away_batters=[],
    )

    with patch("mlb.plugins.mlb_stats_client.requests.get",
               side_effect=[_mock_response(200, schedule),
                            _mock_response(200, box)]), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = fetch_batter_game_logs("2026-04-22", "2026-04-22", delay_seconds=0)

    assert len(result) == 1
    assert result[0]["player_id"] == 10


def test_fetch_batter_game_logs_skips_failed_boxscore(caplog):
    from mlb.plugins.mlb_stats_client import fetch_batter_game_logs

    schedule = _schedule_fixture({"2026-04-22": [
        _game(100, "Final", home_id=1, home_abbr="AAA", away_id=2, away_abbr="BBB"),
        _game(200, "Final", home_id=3, home_abbr="CCC", away_id=4, away_abbr="DDD"),
    ]})
    good_box = _boxscore_fixture(
        home_team_id=3, away_team_id=4,
        home_batters=[{"id": 50, "pa": 4, "h": 1}],
        away_batters=[],
    )
    # First boxscore fails with 500 (will retry 3x then raise); second succeeds.
    responses = [
        _mock_response(200, schedule),
        _mock_response(500), _mock_response(500), _mock_response(500),
        _mock_response(200, good_box),
    ]

    with patch("mlb.plugins.mlb_stats_client.requests.get", side_effect=responses), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"), \
         caplog.at_level("WARNING"):
        result = fetch_batter_game_logs("2026-04-22", "2026-04-22", delay_seconds=0)

    assert len(result) == 1
    assert result[0]["player_id"] == 50
    assert any("100" in rec.message for rec in caplog.records)


def test_fetch_batter_game_logs_raises_when_all_boxscores_fail():
    from mlb.plugins.mlb_stats_client import fetch_batter_game_logs

    schedule = _schedule_fixture({"2026-04-22": [
        _game(100, "Final", home_id=1, home_abbr="AAA", away_id=2, away_abbr="BBB"),
    ]})
    # Schedule succeeds, then boxscore fails all 3 retries.
    responses = [
        _mock_response(200, schedule),
        _mock_response(500), _mock_response(500), _mock_response(500),
    ]
    with patch("mlb.plugins.mlb_stats_client.requests.get", side_effect=responses), \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        with pytest.raises(ValueError, match="All boxscore fetches failed"):
            fetch_batter_game_logs("2026-04-22", "2026-04-22", delay_seconds=0)


def test_fetch_batter_game_logs_accepts_date_objects():
    from mlb.plugins.mlb_stats_client import fetch_batter_game_logs
    from datetime import date as _date

    schedule = _schedule_fixture({})  # no dates → no games
    with patch("mlb.plugins.mlb_stats_client.requests.get",
               return_value=_mock_response(200, schedule)) as mock_get, \
         patch("mlb.plugins.mlb_stats_client.time.sleep"):
        result = fetch_batter_game_logs(_date(2026, 4, 22), _date(2026, 4, 23),
                                        delay_seconds=0)
    assert result == []
    params = mock_get.call_args.kwargs["params"]
    assert params["startDate"] == "2026-04-22"
    assert params["endDate"] == "2026-04-23"
