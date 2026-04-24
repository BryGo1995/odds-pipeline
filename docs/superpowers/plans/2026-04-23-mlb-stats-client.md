# MLB Stats Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `mlb/plugins/mlb_stats_client.py` — a thin HTTP client over `statsapi.mlb.com` exposing `fetch_teams`, `fetch_players`, and `fetch_batter_game_logs`, with unit tests.

**Architecture:** Single module, pure functions returning `list[dict]` keyed to the columns of `mlb_teams`, `mlb_players`, and `mlb_player_game_logs`. One private `_get` helper handles HTTP + retry (3 attempts with 30/60/120s backoffs on 429 / 5xx / Timeout / ConnectionError). Game-log fetch is per-game: schedule → boxscore per `gamePk`. All tests mock `requests.get`; no live API calls.

**Tech Stack:** Python 3, `requests` (already in `requirements.txt`), `pytest`, `unittest.mock`. No new dependencies.

**Spec reference:** `docs/superpowers/specs/2026-04-23-mlb-stats-client-design.md`

---

## File Structure

**To create:**
- `mlb/plugins/__init__.py` — empty package marker.
- `mlb/plugins/mlb_stats_client.py` — the client module (all constants, `_get`, three public functions, `_extract_batter_lines` helper).
- `mlb/tests/unit/test_mlb_stats_client.py` — all unit tests + a local `_mock_response` helper.

**Not modified:**
- `pytest.ini` — already includes `mlb/tests`.
- `requirements.txt` — no new deps.
- `mlb/tests/unit/__init__.py` — already exists.

Everything for this piece of work lives in two new source files. No existing code is touched.

---

## Task 1: Scaffold `mlb/plugins/` package

**Files:**
- Create: `mlb/plugins/__init__.py`
- Create: `mlb/plugins/mlb_stats_client.py` (placeholder — module-level constants only)
- Create: `mlb/tests/unit/test_mlb_stats_client.py` (import-only smoke test)

- [ ] **Step 1: Create the plugins package**

Create `mlb/plugins/__init__.py` with content:

```python
```

(Empty file. Package marker only.)

- [ ] **Step 2: Create the client module with constants**

Create `mlb/plugins/mlb_stats_client.py` with:

```python
# mlb/plugins/mlb_stats_client.py
"""Thin HTTP client over the public MLB Stats API (statsapi.mlb.com).

Fetches teams, players, and per-game batter logs for ingestion into the
mlb_teams, mlb_players, and mlb_player_game_logs tables.
"""
import logging
import time
from datetime import date

import requests

_BASE_URL = "https://statsapi.mlb.com/api/v1"
_SPORT_ID = 1  # 1 = MLB in statsapi.mlb.com's sport taxonomy
_DEFAULT_TIMEOUT = 30
_USER_AGENT = "odds-pipeline/mlb (github.com/BryGo1995/odds-pipeline)"
_RETRY_BACKOFFS = [30, 60, 120]
```

- [ ] **Step 3: Create an import-only smoke test**

Create `mlb/tests/unit/test_mlb_stats_client.py` with:

```python
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
```

- [ ] **Step 4: Run the smoke test**

Run: `pytest mlb/tests/unit/test_mlb_stats_client.py -v`

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/__init__.py mlb/plugins/mlb_stats_client.py mlb/tests/unit/test_mlb_stats_client.py
git commit -m "feat(mlb): scaffold mlb_stats_client module

Step 2 of issue #6. Adds the mlb/plugins package and a stub
mlb_stats_client.py with HTTP client constants, plus a _mock_response
test helper and an import smoke test.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Implement `_get` helper with retry

**Files:**
- Modify: `mlb/plugins/mlb_stats_client.py` (add `_get`)
- Modify: `mlb/tests/unit/test_mlb_stats_client.py` (add `_get` tests)

- [ ] **Step 1: Write failing tests for `_get`**

Append to `mlb/tests/unit/test_mlb_stats_client.py`:

```python
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
```

- [ ] **Step 2: Run tests — they must fail**

Run: `pytest mlb/tests/unit/test_mlb_stats_client.py -v -k "test_get"`

Expected: all 6 `_get` tests fail with `ImportError: cannot import name '_get'`.

- [ ] **Step 3: Implement `_get`**

Append to `mlb/plugins/mlb_stats_client.py`:

```python
def _get(path, params, delay_seconds):
    """Issue a GET against statsapi.mlb.com with retry on 429 / 5xx / network errors.

    Sleeps `delay_seconds` before the first attempt. Retries up to 3 times with
    backoffs of 30s, 60s, 120s. Raises on terminal failure or any 4xx other than
    429.
    """
    time.sleep(delay_seconds)
    for attempt, backoff in enumerate(_RETRY_BACKOFFS, start=1):
        try:
            r = requests.get(
                _BASE_URL + path,
                params=params,
                headers={"User-Agent": _USER_AGENT},
                timeout=_DEFAULT_TIMEOUT,
            )
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt == len(_RETRY_BACKOFFS):
                    r.raise_for_status()
                time.sleep(backoff)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.Timeout, requests.ConnectionError):
            if attempt == len(_RETRY_BACKOFFS):
                raise
            time.sleep(backoff)
    # unreachable — loop either returns or raises
    raise RuntimeError("_get exhausted retries without returning or raising")
```

- [ ] **Step 4: Run tests — they must pass**

Run: `pytest mlb/tests/unit/test_mlb_stats_client.py -v`

Expected: all 7 tests pass (1 smoke test + 6 `_get` tests).

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/mlb_stats_client.py mlb/tests/unit/test_mlb_stats_client.py
git commit -m "feat(mlb): add _get helper with retry on 429/5xx/network errors

Retries up to 3 times with 30/60/120s backoffs. Does not retry on 4xx
other than 429 — those are programmer errors and should fail fast.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Implement `fetch_teams`

**Files:**
- Modify: `mlb/plugins/mlb_stats_client.py` (add `fetch_teams`)
- Modify: `mlb/tests/unit/test_mlb_stats_client.py` (add `fetch_teams` tests)

- [ ] **Step 1: Write failing tests**

Append to `mlb/tests/unit/test_mlb_stats_client.py`:

```python
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
```

- [ ] **Step 2: Run tests — they must fail**

Run: `pytest mlb/tests/unit/test_mlb_stats_client.py -v -k "fetch_teams"`

Expected: 2 fails with `ImportError: cannot import name 'fetch_teams'`.

- [ ] **Step 3: Implement `fetch_teams`**

Append to `mlb/plugins/mlb_stats_client.py`:

```python
def fetch_teams(delay_seconds=0.2):
    """Fetch active MLB teams. Returns list of dicts matching mlb_teams columns."""
    data = _get(
        "/teams",
        {"sportId": _SPORT_ID, "activeStatus": "Y"},
        delay_seconds,
    )
    return [
        {
            "team_id": t["id"],
            "full_name": t["name"],
            "abbreviation": t["abbreviation"],
            "league": (t.get("league") or {}).get("name"),
            "division": (t.get("division") or {}).get("name"),
        }
        for t in data.get("teams", [])
    ]
```

- [ ] **Step 4: Run tests — they must pass**

Run: `pytest mlb/tests/unit/test_mlb_stats_client.py -v`

Expected: all 9 tests pass (1 + 6 + 2).

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/mlb_stats_client.py mlb/tests/unit/test_mlb_stats_client.py
git commit -m "feat(mlb): add fetch_teams

Maps /teams?sportId=1&activeStatus=Y to rows keyed for mlb_teams.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Implement `fetch_players`

**Files:**
- Modify: `mlb/plugins/mlb_stats_client.py` (add `fetch_players`)
- Modify: `mlb/tests/unit/test_mlb_stats_client.py` (add `fetch_players` tests)

- [ ] **Step 1: Write failing tests**

Append to `mlb/tests/unit/test_mlb_stats_client.py`:

```python
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
```

- [ ] **Step 2: Run tests — they must fail**

Run: `pytest mlb/tests/unit/test_mlb_stats_client.py -v -k "fetch_players"`

Expected: 3 fails with `ImportError: cannot import name 'fetch_players'`.

- [ ] **Step 3: Implement `fetch_players`**

Append to `mlb/plugins/mlb_stats_client.py`:

```python
def fetch_players(season, active_only=True, delay_seconds=0.2):
    """Fetch MLB players for a season. Returns list of dicts matching mlb_players columns.

    team_abbreviation is always None at this layer — transformers resolve it via
    the mlb_teams join.
    """
    params = {"season": season}
    if active_only:
        params["activeStatus"] = "Y"
    data = _get("/sports/1/players", params, delay_seconds)

    players = []
    for p in data.get("people", []):
        current_team = p.get("currentTeam") or {}
        players.append({
            "player_id": p["id"],
            "full_name": p["fullName"],
            "position": (p.get("primaryPosition") or {}).get("abbreviation"),
            "bats": (p.get("batSide") or {}).get("code"),
            "throws": (p.get("pitchHand") or {}).get("code"),
            "team_id": current_team.get("id"),
            "team_abbreviation": None,
            "is_active": bool(p.get("active", False)),
        })
    return players
```

- [ ] **Step 4: Run tests — they must pass**

Run: `pytest mlb/tests/unit/test_mlb_stats_client.py -v`

Expected: all 12 tests pass (1 + 6 + 2 + 3).

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/mlb_stats_client.py mlb/tests/unit/test_mlb_stats_client.py
git commit -m "feat(mlb): add fetch_players

Maps /sports/1/players?season=YYYY to rows keyed for mlb_players. Leaves
team_abbreviation=None; transformers will resolve it via mlb_teams join.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Implement `fetch_batter_game_logs`

**Files:**
- Modify: `mlb/plugins/mlb_stats_client.py` (add `fetch_batter_game_logs` + `_extract_batter_lines`)
- Modify: `mlb/tests/unit/test_mlb_stats_client.py` (add game-log tests)

- [ ] **Step 1: Write failing tests**

Append to `mlb/tests/unit/test_mlb_stats_client.py`:

```python
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
    import pytest as _pytest

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
        with _pytest.raises(ValueError, match="All boxscore fetches failed"):
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
```

- [ ] **Step 2: Run tests — they must fail**

Run: `pytest mlb/tests/unit/test_mlb_stats_client.py -v -k "fetch_batter_game_logs"`

Expected: 6 fails with `ImportError: cannot import name 'fetch_batter_game_logs'`.

- [ ] **Step 3: Implement `_extract_batter_lines` and `fetch_batter_game_logs`**

Append to `mlb/plugins/mlb_stats_client.py`:

```python
def _extract_batter_lines(boxscore, game_meta):
    """Extract batter rows from one boxscore JSON payload.

    game_meta carries schedule-level context (game_pk, date, team IDs, abbreviations)
    that the boxscore itself doesn't include.
    """
    matchup = f"{game_meta['away_abbr']} @ {game_meta['home_abbr']}"
    season = game_meta["game_date"][:4]
    rows = []
    teams = boxscore.get("teams") or {}
    for side in ("home", "away"):
        side_obj = teams.get(side) or {}
        team_id = (side_obj.get("team") or {}).get("id")
        opponent_team_id = (
            game_meta["away_team_id"] if side == "home" else game_meta["home_team_id"]
        )
        players = side_obj.get("players") or {}
        for _, p in players.items():
            batting = (p.get("stats") or {}).get("batting") or {}
            pa = batting.get("plateAppearances") or 0
            if pa <= 0:
                continue
            rows.append({
                "player_id": (p.get("person") or {}).get("id"),
                "mlb_game_pk": game_meta["game_pk"],
                "season": season,
                "game_date": game_meta["game_date"],
                "matchup": matchup,
                "team_id": team_id,
                "opponent_team_id": opponent_team_id,
                "plate_appearances": pa,
                "at_bats": batting.get("atBats"),
                "hits": batting.get("hits"),
                "doubles": batting.get("doubles"),
                "triples": batting.get("triples"),
                "home_runs": batting.get("homeRuns"),
                "rbi": batting.get("rbi"),
                "runs": batting.get("runs"),
                "walks": batting.get("baseOnBalls"),
                "strikeouts": batting.get("strikeOuts"),
                "stolen_bases": batting.get("stolenBases"),
                "total_bases": batting.get("totalBases"),
            })
    return rows


def fetch_batter_game_logs(start_date, end_date, delay_seconds=0.2):
    """Fetch per-game batter lines for all Final games in [start_date, end_date].

    start_date / end_date accept `datetime.date` or 'YYYY-MM-DD' strings.

    Strategy: GET /schedule for the date range, then GET /game/{gamePk}/boxscore
    per Final-status game. Bench players (plateAppearances=0) are filtered out.
    Individual boxscore failures are logged and skipped. Raises ValueError if
    every boxscore in the range fails.

    Returns list of dicts matching mlb_player_game_logs columns.
    """
    start_str = start_date.isoformat() if isinstance(start_date, date) else start_date
    end_str = end_date.isoformat() if isinstance(end_date, date) else end_date

    schedule = _get(
        "/schedule",
        {"sportId": _SPORT_ID, "startDate": start_str, "endDate": end_str},
        delay_seconds,
    )

    games = []
    for date_entry in schedule.get("dates", []):
        game_date_str = date_entry.get("date")
        for g in date_entry.get("games", []):
            status = (g.get("status") or {}).get("abstractGameState")
            if status != "Final":
                continue
            teams = g.get("teams") or {}
            away = (teams.get("away") or {}).get("team") or {}
            home = (teams.get("home") or {}).get("team") or {}
            games.append({
                "game_pk": str(g["gamePk"]),
                "game_date": game_date_str,
                "away_team_id": away.get("id"),
                "away_abbr": away.get("abbreviation"),
                "home_team_id": home.get("id"),
                "home_abbr": home.get("abbreviation"),
            })

    rows = []
    fetch_failures = 0
    for g in games:
        try:
            box = _get(f"/game/{g['game_pk']}/boxscore", {}, delay_seconds)
        except requests.HTTPError as e:
            logging.warning("Skipping gamePk=%s: %s", g["game_pk"], e)
            fetch_failures += 1
            continue
        rows.extend(_extract_batter_lines(box, g))

    if games and fetch_failures == len(games):
        raise ValueError("All boxscore fetches failed for the date range.")

    return rows
```

- [ ] **Step 4: Run tests — they must pass**

Run: `pytest mlb/tests/unit/test_mlb_stats_client.py -v`

Expected: all 18 tests pass (1 + 6 + 2 + 3 + 6).

- [ ] **Step 5: Run the full project test suite to catch regressions**

Run: `pytest`

Expected: all tests pass, including unrelated NBA and shared tests. If anything unrelated fails, stop and investigate — don't amend this commit.

- [ ] **Step 6: Commit**

```bash
git add mlb/plugins/mlb_stats_client.py mlb/tests/unit/test_mlb_stats_client.py
git commit -m "feat(mlb): add fetch_batter_game_logs (schedule + boxscore pattern)

Fetches Final-status games in a date range via /schedule, then one
/game/{pk}/boxscore call per game. Extracts batter lines with PA > 0,
keyed to mlb_player_game_logs columns. Individual boxscore failures are
logged and skipped; raises ValueError only if every boxscore fails.

Closes the 'step 2' scope of issue #6. DAGs and transformers follow in
step 3.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Completion criteria

- All 18 tests in `mlb/tests/unit/test_mlb_stats_client.py` pass.
- `pytest` (full suite) passes — no regressions in NBA or shared tests.
- `mlb/plugins/mlb_stats_client.py` exposes exactly three public functions: `fetch_teams`, `fetch_players`, `fetch_batter_game_logs`.
- No new entries in `requirements.txt`.
- No changes outside `mlb/plugins/` and `mlb/tests/unit/`.

After the final commit, add a comment to issue #6 noting that step 2 is complete and linking the commit(s) (per the project's issue-driven workflow).
