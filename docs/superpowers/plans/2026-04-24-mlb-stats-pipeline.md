# MLB Stats Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing `mlb_stats_client` into four transformers and two DAGs (daily + backfill) so `mlb_teams`, `mlb_players`, `mlb_player_game_logs`, and `mlb_player_name_mappings` are populated automatically.

**Architecture:** Four pure-function transformers do `INSERT … ON CONFLICT` upserts. A daily DAG waits on `mlb_odds_pipeline`, fetches teams / players / 3-day batter game-log window, upserts in FK-safe order, then resolves Odds-API names → `mlb_player_id`. A manual-trigger backfill DAG seeds historical seasons (year-range param, fixed Mar 15 – Nov 15 window per season). Tests mock the DB via `MagicMock` for transformers and use `DagBag` for structural DAG assertions.

**Tech Stack:** Python 3, psycopg2, `rapidfuzz` (already in `requirements.txt`), Apache Airflow, pytest, `unittest.mock`. No new dependencies.

**Spec reference:** `docs/superpowers/specs/2026-04-24-mlb-stats-pipeline-design.md`

---

## File Structure

**To create (source):**
- `mlb/plugins/transformers/__init__.py` — package marker
- `mlb/plugins/transformers/teams.py` — `transform_teams(conn, raw_teams)`
- `mlb/plugins/transformers/players.py` — `transform_players(conn, raw_players)`
- `mlb/plugins/transformers/player_game_logs.py` — `transform_player_game_logs(conn, raw_logs)`
- `mlb/plugins/transformers/player_name_resolution.py` — `normalize_name`, `resolve_player_ids(conn, slack_webhook_url=None)`
- `mlb/dags/mlb_stats_pipeline_dag.py` — daily DAG
- `mlb/dags/mlb_stats_backfill_dag.py` — manual-trigger DAG

**To create (tests):**
- `mlb/tests/unit/conftest.py` — airflow + pendulum stubs (no nba_api)
- `mlb/tests/unit/transformers/__init__.py`
- `mlb/tests/unit/transformers/test_teams.py`
- `mlb/tests/unit/transformers/test_players.py`
- `mlb/tests/unit/transformers/test_player_game_logs.py`
- `mlb/tests/unit/test_player_name_resolution.py`
- `mlb/tests/unit/test_mlb_stats_pipeline_dag.py`
- `mlb/tests/unit/test_mlb_stats_backfill_dag.py`

**Not modified:** `mlb/plugins/mlb_stats_client.py`, `requirements.txt`, `pytest.ini`, any NBA or shared code.

---

## Task 1: Scaffold transformer package + test conftest

**Files:**
- Create: `mlb/plugins/transformers/__init__.py` (empty)
- Create: `mlb/tests/unit/conftest.py` (airflow + pendulum stubs)
- Create: `mlb/tests/unit/transformers/__init__.py` (empty)

- [ ] **Step 1: Create transformers package marker**

Create `mlb/plugins/transformers/__init__.py` with:

```python
```

(Empty file.)

- [ ] **Step 2: Create test-directory package marker**

Create `mlb/tests/unit/transformers/__init__.py` with:

```python
```

(Empty file.)

- [ ] **Step 3: Create the test conftest**

Create `mlb/tests/unit/conftest.py` with:

```python
# mlb/tests/unit/conftest.py
"""
Stub out airflow + pendulum modules so that plugins and DAG modules can be
imported without those packages installed (e.g. in a bare unit-test env).
Tests that need real Airflow behaviour (e.g. DagBag) import it inside the
test body and require a real Airflow install.
"""
import importlib.util
import sys
from unittest.mock import MagicMock


def _stub_airflow():
    """Insert minimal stubs for airflow into sys.modules."""
    if "airflow" in sys.modules:
        return  # real Airflow is installed — leave it alone

    sys.modules.setdefault("airflow", MagicMock())
    sys.modules.setdefault("airflow.models", MagicMock())
    sys.modules.setdefault("airflow.models.param", MagicMock())
    sys.modules.setdefault("airflow.operators", MagicMock())
    sys.modules.setdefault("airflow.operators.python", MagicMock())
    sys.modules.setdefault("airflow.sensors", MagicMock())
    sys.modules.setdefault("airflow.sensors.external_task", MagicMock())


def _stub_pendulum():
    """Stub pendulum so DAGs can be imported without it installed."""
    if "pendulum" in sys.modules or importlib.util.find_spec("pendulum") is not None:
        return
    pendulum_stub = MagicMock()
    pendulum_stub.datetime = MagicMock(return_value=MagicMock())
    sys.modules.setdefault("pendulum", pendulum_stub)


_stub_airflow()
_stub_pendulum()
```

- [ ] **Step 4: Verify both packages import cleanly**

Run: `python -c "import mlb.plugins.transformers; print('transformers ok')"`
Expected: `transformers ok`

Run: `pytest mlb/tests/unit/ -v --collect-only 2>&1 | head -20`
Expected: collection succeeds (includes existing `test_mlb_stats_client.py` tests; no new tests yet).

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/transformers/__init__.py mlb/tests/unit/conftest.py mlb/tests/unit/transformers/__init__.py
git commit -m "feat(mlb): scaffold transformer package + test conftest

Step 3 of issue #6. Adds empty mlb/plugins/transformers/ package and
mlb/tests/unit/conftest.py stubbing airflow + pendulum so transformers
and DAG modules can be imported without those packages installed.
Mirrors nba/tests/unit/conftest.py minus the nba_api stub.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `transform_teams`

**Files:**
- Create: `mlb/plugins/transformers/teams.py`
- Create: `mlb/tests/unit/transformers/test_teams.py`

- [ ] **Step 1: Write failing tests**

Create `mlb/tests/unit/transformers/test_teams.py` with:

```python
# mlb/tests/unit/transformers/test_teams.py
from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_TEAMS = [
    {
        "team_id": 108,
        "full_name": "Los Angeles Angels",
        "abbreviation": "LAA",
        "league": "American League",
        "division": "American League West",
    },
    {
        "team_id": 119,
        "full_name": "Los Angeles Dodgers",
        "abbreviation": "LAD",
        "league": "National League",
        "division": "National League West",
    },
]


def test_transform_teams_empty_is_noop():
    from mlb.plugins.transformers.teams import transform_teams
    mock_conn, mock_cursor = _make_mock_conn()
    transform_teams(mock_conn, [])
    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()


def test_transform_teams_upserts_all_rows():
    from mlb.plugins.transformers.teams import transform_teams
    mock_conn, mock_cursor = _make_mock_conn()
    transform_teams(mock_conn, SAMPLE_TEAMS)
    assert mock_cursor.execute.call_count == 2
    mock_conn.commit.assert_called_once()


def test_transform_teams_bind_params_order():
    from mlb.plugins.transformers.teams import transform_teams
    mock_conn, mock_cursor = _make_mock_conn()
    transform_teams(mock_conn, [SAMPLE_TEAMS[0]])
    _, args = mock_cursor.execute.call_args.args
    assert args == (108, "Los Angeles Angels", "LAA",
                    "American League", "American League West")


def test_transform_teams_handles_missing_division():
    from mlb.plugins.transformers.teams import transform_teams
    mock_conn, mock_cursor = _make_mock_conn()
    minimal = {"team_id": 999, "full_name": "Some Team", "abbreviation": "XXX"}
    transform_teams(mock_conn, [minimal])
    _, args = mock_cursor.execute.call_args.args
    assert args == (999, "Some Team", "XXX", None, None)
```

- [ ] **Step 2: Run tests — must fail**

Run: `pytest mlb/tests/unit/transformers/test_teams.py -v`
Expected: 4 fails with `ImportError: cannot import name 'transform_teams'`.

- [ ] **Step 3: Implement transformer**

Create `mlb/plugins/transformers/teams.py` with:

```python
def transform_teams(conn, raw_teams):
    """Upsert MLB team rows into the mlb_teams table."""
    if not raw_teams:
        return
    with conn.cursor() as cur:
        for t in raw_teams:
            cur.execute(
                """
                INSERT INTO mlb_teams
                    (team_id, full_name, abbreviation, league, division)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (team_id) DO UPDATE SET
                    full_name    = EXCLUDED.full_name,
                    abbreviation = EXCLUDED.abbreviation,
                    league       = EXCLUDED.league,
                    division     = EXCLUDED.division,
                    fetched_at   = NOW()
                """,
                (
                    t["team_id"],
                    t["full_name"],
                    t["abbreviation"],
                    t.get("league"),
                    t.get("division"),
                ),
            )
    conn.commit()
```

- [ ] **Step 4: Run tests — must pass**

Run: `pytest mlb/tests/unit/transformers/test_teams.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/transformers/teams.py mlb/tests/unit/transformers/test_teams.py
git commit -m "feat(mlb): add transform_teams

Upserts mlb_teams rows with INSERT ... ON CONFLICT (team_id) DO UPDATE.
Mirrors nba/plugins/transformers/teams.py shape.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `transform_players`

**Files:**
- Create: `mlb/plugins/transformers/players.py`
- Create: `mlb/tests/unit/transformers/test_players.py`

- [ ] **Step 1: Write failing tests**

Create `mlb/tests/unit/transformers/test_players.py` with:

```python
# mlb/tests/unit/transformers/test_players.py
from unittest.mock import MagicMock


def _make_mock_conn(teams_fetchall=None):
    """Build a mock conn/cursor.

    teams_fetchall: what the `SELECT team_id, abbreviation FROM mlb_teams`
    fetchall() should return. Default: empty.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = teams_fetchall or []
    return mock_conn, mock_cursor


SAMPLE_PLAYERS = [
    {
        "player_id": 660271,
        "full_name": "Shohei Ohtani",
        "position": "DH",
        "bats": "L",
        "throws": "R",
        "team_id": 119,
        "team_abbreviation": None,
        "is_active": True,
    }
]


def test_transform_players_empty_is_noop():
    from mlb.plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn()
    transform_players(mock_conn, [])
    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()


def test_transform_players_resolves_team_abbreviation_from_mlb_teams():
    from mlb.plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn(teams_fetchall=[(119, "LAD"), (108, "LAA")])
    transform_players(mock_conn, SAMPLE_PLAYERS)
    # execute calls: 1 SELECT on mlb_teams + 1 INSERT per player = 2 total
    assert mock_cursor.execute.call_count == 2
    insert_args = mock_cursor.execute.call_args.args[1]
    # bind-param order: player_id, full_name, position, bats, throws,
    #                   team_id, team_abbreviation, is_active
    assert insert_args == (660271, "Shohei Ohtani", "DH", "L", "R",
                           119, "LAD", True)
    mock_conn.commit.assert_called_once()


def test_transform_players_missing_team_leaves_abbreviation_none():
    from mlb.plugins.transformers.players import transform_players
    # mlb_teams has no entry for team_id=119 → abbreviation should be None
    mock_conn, mock_cursor = _make_mock_conn(teams_fetchall=[(108, "LAA")])
    transform_players(mock_conn, SAMPLE_PLAYERS)
    insert_args = mock_cursor.execute.call_args.args[1]
    assert insert_args[6] is None  # team_abbreviation slot


def test_transform_players_free_agent_null_team():
    from mlb.plugins.transformers.players import transform_players
    mock_conn, mock_cursor = _make_mock_conn(teams_fetchall=[(119, "LAD")])
    fa = dict(SAMPLE_PLAYERS[0], team_id=None)
    transform_players(mock_conn, [fa])
    insert_args = mock_cursor.execute.call_args.args[1]
    assert insert_args[5] is None  # team_id slot
    assert insert_args[6] is None  # team_abbreviation slot
```

- [ ] **Step 2: Run tests — must fail**

Run: `pytest mlb/tests/unit/transformers/test_players.py -v`
Expected: 4 fails with `ImportError: cannot import name 'transform_players'`.

- [ ] **Step 3: Implement transformer**

Create `mlb/plugins/transformers/players.py` with:

```python
def transform_players(conn, raw_players):
    """Upsert MLB player rows into mlb_players, resolving team_abbreviation
    from mlb_teams. The client leaves team_abbreviation as None; this
    transformer fills it via a single join against mlb_teams.
    """
    if not raw_players:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT team_id, abbreviation FROM mlb_teams")
        team_abbr = dict(cur.fetchall())  # {team_id: abbreviation}

        for p in raw_players:
            tid = p.get("team_id")
            cur.execute(
                """
                INSERT INTO mlb_players
                    (player_id, full_name, position, bats, throws,
                     team_id, team_abbreviation, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET
                    full_name         = EXCLUDED.full_name,
                    position          = EXCLUDED.position,
                    bats              = EXCLUDED.bats,
                    throws            = EXCLUDED.throws,
                    team_id           = EXCLUDED.team_id,
                    team_abbreviation = EXCLUDED.team_abbreviation,
                    is_active         = EXCLUDED.is_active,
                    fetched_at        = NOW()
                """,
                (
                    p["player_id"],
                    p["full_name"],
                    p.get("position"),
                    p.get("bats"),
                    p.get("throws"),
                    tid,
                    team_abbr.get(tid),
                    p.get("is_active", True),
                ),
            )
    conn.commit()
```

- [ ] **Step 4: Run tests — must pass**

Run: `pytest mlb/tests/unit/transformers/test_players.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/transformers/players.py mlb/tests/unit/transformers/test_players.py
git commit -m "feat(mlb): add transform_players

Upserts mlb_players rows, resolving team_abbreviation via a single
lookup against mlb_teams. Mirrors nba/plugins/transformers/players.py
shape with MLB-specific bats/throws columns.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `transform_player_game_logs`

**Files:**
- Create: `mlb/plugins/transformers/player_game_logs.py`
- Create: `mlb/tests/unit/transformers/test_player_game_logs.py`

- [ ] **Step 1: Write failing tests**

Create `mlb/tests/unit/transformers/test_player_game_logs.py` with:

```python
# mlb/tests/unit/transformers/test_player_game_logs.py
from unittest.mock import MagicMock


def _make_mock_conn():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


SAMPLE_LOG = {
    "player_id": 660271,
    "mlb_game_pk": "745123",
    "season": "2026",
    "game_date": "2026-04-22",
    "matchup": "LAA @ SEA",
    "team_id": 108,
    "opponent_team_id": 136,
    "plate_appearances": 4,
    "at_bats": 3,
    "hits": 2,
    "doubles": 0,
    "triples": 0,
    "home_runs": 1,
    "rbi": 2,
    "runs": 1,
    "walks": 1,
    "strikeouts": 1,
    "stolen_bases": 0,
    "total_bases": 5,
}


def test_transform_pgl_empty_is_noop():
    from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [])
    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()


def test_transform_pgl_bind_params_order():
    from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    transform_player_game_logs(mock_conn, [SAMPLE_LOG])
    # execute calls: SAVEPOINT, INSERT, RELEASE → 3 per row
    assert mock_cursor.execute.call_count == 3
    # The middle call is the INSERT; its bind params are the game-log values
    insert_call = mock_cursor.execute.call_args_list[1]
    args = insert_call.args[1]
    assert args == (
        660271, "745123", "2026", "2026-04-22", "LAA @ SEA",
        108, 136,
        4, 3, 2, 0, 0,
        1, 2, 1, 1, 1,
        0, 5,
    )
    mock_conn.commit.assert_called_once()


def test_transform_pgl_savepoint_isolates_failed_row():
    """One row's INSERT raises — SAVEPOINT is rolled back, other rows still land."""
    from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Sequence of execute calls across 2 rows:
    #   row 1: SAVEPOINT, INSERT (RAISES), ROLLBACK TO, RELEASE
    #   row 2: SAVEPOINT, INSERT (ok),    RELEASE
    call_results = [
        None,                                # SAVEPOINT  (row 1)
        Exception("FK violation"),           # INSERT     (row 1) — raises
        None,                                # ROLLBACK TO (row 1)
        None,                                # RELEASE     (row 1)
        None,                                # SAVEPOINT  (row 2)
        None,                                # INSERT     (row 2) — ok
        None,                                # RELEASE     (row 2)
    ]
    def _execute(*a, **kw):
        result = call_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result
    mock_cursor.execute.side_effect = _execute

    row1 = dict(SAMPLE_LOG, player_id=999999)  # will be the failing one
    row2 = SAMPLE_LOG
    transform_player_game_logs(mock_conn, [row1, row2])

    # Row 1: SAVEPOINT, INSERT(raises), ROLLBACK TO, RELEASE = 4
    # Row 2: SAVEPOINT, INSERT(ok),                 RELEASE = 3
    # Total = 7
    assert mock_cursor.execute.call_count == 7
    mock_conn.commit.assert_called_once()


def test_transform_pgl_doubleheader_two_rows_same_date():
    """Two game-log rows for the same player on the same date but different
    mlb_game_pk (doubleheader) — both insert, no dedup."""
    from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
    mock_conn, mock_cursor = _make_mock_conn()
    game1 = dict(SAMPLE_LOG, mlb_game_pk="745123")
    game2 = dict(SAMPLE_LOG, mlb_game_pk="745124")
    transform_player_game_logs(mock_conn, [game1, game2])
    # 3 execute calls per row × 2 rows = 6
    assert mock_cursor.execute.call_count == 6
    mock_conn.commit.assert_called_once()
```

- [ ] **Step 2: Run tests — must fail**

Run: `pytest mlb/tests/unit/transformers/test_player_game_logs.py -v`
Expected: 4 fails with `ImportError: cannot import name 'transform_player_game_logs'`.

- [ ] **Step 3: Implement transformer**

Create `mlb/plugins/transformers/player_game_logs.py` with:

```python
def transform_player_game_logs(conn, raw_logs):
    """Upsert MLB batter game-log rows into mlb_player_game_logs.

    Uses per-row SAVEPOINT so one bad row (e.g. FK violation on
    player_id) doesn't abort the whole batch. ON CONFLICT DO NOTHING
    makes re-ingestion (e.g. the daily 3-day lookback) idempotent.
    """
    if not raw_logs:
        return
    with conn.cursor() as cur:
        for log in raw_logs:
            cur.execute("SAVEPOINT pgl_row")
            try:
                cur.execute(
                    """
                    INSERT INTO mlb_player_game_logs
                        (player_id, mlb_game_pk, season, game_date, matchup,
                         team_id, opponent_team_id,
                         plate_appearances, at_bats, hits, doubles, triples,
                         home_runs, rbi, runs, walks, strikeouts,
                         stolen_bases, total_bases)
                    VALUES (%s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s)
                    ON CONFLICT (player_id, mlb_game_pk) DO NOTHING
                    """,
                    (
                        log["player_id"],
                        log["mlb_game_pk"],
                        log["season"],
                        log["game_date"],
                        log.get("matchup"),
                        log.get("team_id"),
                        log.get("opponent_team_id"),
                        log.get("plate_appearances"),
                        log.get("at_bats"),
                        log.get("hits"),
                        log.get("doubles"),
                        log.get("triples"),
                        log.get("home_runs"),
                        log.get("rbi"),
                        log.get("runs"),
                        log.get("walks"),
                        log.get("strikeouts"),
                        log.get("stolen_bases"),
                        log.get("total_bases"),
                    ),
                )
                cur.execute("RELEASE SAVEPOINT pgl_row")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT pgl_row")
                cur.execute("RELEASE SAVEPOINT pgl_row")
    conn.commit()
```

- [ ] **Step 4: Run tests — must pass**

Run: `pytest mlb/tests/unit/transformers/test_player_game_logs.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/transformers/player_game_logs.py mlb/tests/unit/transformers/test_player_game_logs.py
git commit -m "feat(mlb): add transform_player_game_logs

Upserts mlb_player_game_logs rows with SAVEPOINT-isolated per-row
inserts and ON CONFLICT (player_id, mlb_game_pk) DO NOTHING for
idempotent re-ingest. Mirrors nba/plugins/transformers/player_game_logs.py
shape with MLB batter columns.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: `player_name_resolution`

**Files:**
- Create: `mlb/plugins/transformers/player_name_resolution.py`
- Create: `mlb/tests/unit/test_player_name_resolution.py`

- [ ] **Step 1: Write failing tests**

Create `mlb/tests/unit/test_player_name_resolution.py` with:

```python
# mlb/tests/unit/test_player_name_resolution.py
from unittest.mock import MagicMock, patch


def _make_conn_with_cursors(unresolved_names, known_players):
    """Build a mock conn whose two `with conn.cursor()` blocks share a cursor.

    The first cursor use calls fetchall() twice: once for unresolved names,
    once for known players. Subsequent fetches aren't expected.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.side_effect = [
        [(name,) for name in unresolved_names],
        known_players,
    ]
    return mock_conn, mock_cursor


def _get_insert_mapping_calls(mock_cursor):
    """Return bind params for INSERT INTO mlb_player_name_mappings calls."""
    return [
        call.args[1]
        for call in mock_cursor.execute.call_args_list
        if "INSERT INTO mlb_player_name_mappings" in call.args[0]
    ]


def _get_update_player_props_calls(mock_cursor):
    return [
        call for call in mock_cursor.execute.call_args_list
        if "UPDATE player_props" in call.args[0]
    ]


def _get_unresolved_query_calls(mock_cursor):
    return [
        call for call in mock_cursor.execute.call_args_list
        if "FROM player_props pp" in call.args[0]
        and "NOT EXISTS" in call.args[0]
    ]


def test_resolve_exact_match_inserts_mapping():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=["Shohei Ohtani"],
        known_players=[(660271, "Shohei Ohtani")],
    )
    resolve_player_ids(mock_conn)
    inserts = _get_insert_mapping_calls(mock_cursor)
    assert len(inserts) == 1
    odds_name, mlb_player_id, confidence = inserts[0]
    assert odds_name == "Shohei Ohtani"
    assert mlb_player_id == 660271
    assert confidence == 100.0
    mock_conn.commit.assert_called_once()


def test_resolve_fuzzy_match_above_threshold_inserts():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    # 'Shohei Otani' vs 'Shohei Ohtani' — rapidfuzz scores ~96
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=["Shohei Otani"],
        known_players=[(660271, "Shohei Ohtani")],
    )
    resolve_player_ids(mock_conn)
    inserts = _get_insert_mapping_calls(mock_cursor)
    assert len(inserts) == 1
    assert inserts[0][1] == 660271
    assert inserts[0][2] >= 95.0


def test_resolve_below_threshold_no_insert_but_slack_alert():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=["Completely Unknown Name"],
        known_players=[(660271, "Shohei Ohtani")],
    )
    with patch("mlb.plugins.transformers.player_name_resolution.send_slack_message") as mock_slack:
        resolve_player_ids(mock_conn, slack_webhook_url="https://hooks.slack.example/x")

    assert _get_insert_mapping_calls(mock_cursor) == []
    mock_slack.assert_called_once()
    args, _ = mock_slack.call_args
    assert args[0] == "https://hooks.slack.example/x"
    assert "Completely Unknown Name" in args[1]
    assert "[MLB]" in args[1]


def test_resolve_no_slack_when_webhook_url_none():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=["Completely Unknown Name"],
        known_players=[(660271, "Shohei Ohtani")],
    )
    with patch("mlb.plugins.transformers.player_name_resolution.send_slack_message") as mock_slack:
        resolve_player_ids(mock_conn, slack_webhook_url=None)
    mock_slack.assert_not_called()


def test_resolve_unresolved_query_filters_on_baseball_mlb():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=[], known_players=[])
    resolve_player_ids(mock_conn)
    # With no unresolved, the function returns before running the UPDATE —
    # but the unresolved-names SELECT must have run once, with the MLB sport
    # filter embedded in the SQL string.
    unresolved_calls = _get_unresolved_query_calls(mock_cursor)
    assert len(unresolved_calls) == 1
    assert "g.sport = 'baseball_mlb'" in unresolved_calls[0].args[0]


def test_resolve_backfills_player_props_mlb_player_id():
    from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
    mock_conn, mock_cursor = _make_conn_with_cursors(
        unresolved_names=["Shohei Ohtani"],
        known_players=[(660271, "Shohei Ohtani")],
    )
    resolve_player_ids(mock_conn)
    update_calls = _get_update_player_props_calls(mock_cursor)
    assert len(update_calls) == 1
    sql = update_calls[0].args[0]
    assert "mlb_player_id = m.mlb_player_id" in sql
    assert "g.sport = 'baseball_mlb'" in sql
    assert "pp.mlb_player_id IS NULL" in sql
```

- [ ] **Step 2: Run tests — must fail**

Run: `pytest mlb/tests/unit/test_player_name_resolution.py -v`
Expected: 6 fails with `ImportError: cannot import name 'resolve_player_ids'`.

- [ ] **Step 3: Implement resolver**

Create `mlb/plugins/transformers/player_name_resolution.py` with:

```python
# mlb/plugins/transformers/player_name_resolution.py
import logging
import unicodedata

from rapidfuzz import fuzz

from shared.plugins.slack_notifier import send_slack_message

CONFIDENCE_THRESHOLD = 95.0

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """Lowercase, strip accents, remove Jr./Sr./II/III/IV suffixes."""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = name.lower().strip()
    for suffix in [" jr.", " jr", " sr.", " sr", " iii", " ii", " iv"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
            break
    return name


def resolve_player_ids(conn, slack_webhook_url=None):
    """
    Find Odds-API player names in player_props (for MLB games) that aren't
    yet in mlb_player_name_mappings. Fuzzy-match against mlb_players and
    insert high-confidence matches (>=95). Send Slack alert for unresolved.
    Then backfill player_props.mlb_player_id for all mapped names.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT pp.player_name
            FROM player_props pp
            JOIN games g ON g.game_id = pp.game_id
            WHERE pp.player_name IS NOT NULL
              AND g.sport = 'baseball_mlb'
              AND NOT EXISTS (
                SELECT 1 FROM mlb_player_name_mappings m
                WHERE m.odds_api_name = pp.player_name
              )
            """
        )
        unresolved = [row[0] for row in cur.fetchall()]

        if not unresolved:
            return

        cur.execute("SELECT player_id, full_name FROM mlb_players")
        known_players = cur.fetchall()

    unresolved_names = []

    with conn.cursor() as cur:
        for odds_name in unresolved:
            norm_odds = normalize_name(odds_name)
            best_score = 0.0
            best_player_id = None

            for player_id, full_name in known_players:
                score = fuzz.ratio(norm_odds, normalize_name(full_name))
                if score > best_score:
                    best_score = score
                    best_player_id = player_id

            if best_score >= CONFIDENCE_THRESHOLD:
                cur.execute(
                    """
                    INSERT INTO mlb_player_name_mappings
                        (odds_api_name, mlb_player_id, confidence)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (odds_api_name) DO NOTHING
                    """,
                    (odds_name, best_player_id, best_score),
                )
            else:
                logger.warning(
                    "Low confidence match for '%s': score=%.1f",
                    odds_name, best_score,
                )
                unresolved_names.append((odds_name, best_score))

        # Backfill player_props.mlb_player_id for mapped names on MLB games.
        cur.execute(
            """
            UPDATE player_props pp
            SET mlb_player_id = m.mlb_player_id
            FROM mlb_player_name_mappings m, games g
            WHERE pp.player_name = m.odds_api_name
              AND g.game_id = pp.game_id
              AND g.sport = 'baseball_mlb'
              AND pp.mlb_player_id IS NULL
            """
        )

    conn.commit()

    if unresolved_names and slack_webhook_url:
        lines = "\n".join(
            f"  • {name} (best score: {score:.1f})"
            for name, score in unresolved_names
        )
        send_slack_message(
            slack_webhook_url,
            f"[MLB] :warning: *Player name resolution* — "
            f"{len(unresolved_names)} unresolved name(s) need manual review:\n{lines}",
        )
```

- [ ] **Step 4: Run tests — must pass**

Run: `pytest mlb/tests/unit/test_player_name_resolution.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add mlb/plugins/transformers/player_name_resolution.py mlb/tests/unit/test_player_name_resolution.py
git commit -m "feat(mlb): add player_name_resolution

Fuzzy-matches Odds-API batter names against mlb_players (≥95 threshold),
inserts into mlb_player_name_mappings, and backfills
player_props.mlb_player_id for MLB games. Slack alerts tagged [MLB]
for unresolved names. Mirrors nba/plugins/transformers/player_name_resolution.py
with sport filter flipped to 'baseball_mlb'.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Daily DAG (`mlb_stats_pipeline_dag`)

**Files:**
- Create: `mlb/dags/mlb_stats_pipeline_dag.py`
- Create: `mlb/tests/unit/test_mlb_stats_pipeline_dag.py`

- [ ] **Step 1: Write failing tests**

Create `mlb/tests/unit/test_mlb_stats_pipeline_dag.py` with:

```python
# mlb/tests/unit/test_mlb_stats_pipeline_dag.py
"""Structural tests for mlb_stats_pipeline DAG.

These tests require real Airflow installed (DagBag). When run in an env
where Airflow is stubbed by conftest, they'll fail early with an
ImportError — that's expected; run this file only with Airflow available.
"""


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="mlb/dags/", include_examples=False)
    assert "mlb_stats_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_stats_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {
        "wait_for_mlb_odds_pipeline",
        "fetch_teams", "fetch_players", "fetch_batter_game_logs",
        "transform_teams", "transform_players", "transform_player_game_logs",
        "resolve_player_ids",
    }


def test_dag_task_chain():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_stats_pipeline"]

    # Sensor feeds all 3 fetch tasks
    sensor_downstream = {t.task_id for t in dag.get_task("wait_for_mlb_odds_pipeline").downstream_list}
    assert sensor_downstream == {"fetch_teams", "fetch_players", "fetch_batter_game_logs"}

    # Each fetch feeds its corresponding transform
    assert {"transform_teams"} == {t.task_id for t in dag.get_task("fetch_teams").downstream_list}
    # fetch_players must feed transform_players (and may feed more via chain)
    assert "transform_players" in {t.task_id for t in dag.get_task("fetch_players").downstream_list}
    assert "transform_player_game_logs" in {
        t.task_id for t in dag.get_task("fetch_batter_game_logs").downstream_list
    }

    # FK-safe transform ordering: teams → players → player_game_logs
    assert "transform_players" in {
        t.task_id for t in dag.get_task("transform_teams").downstream_list
    }
    assert "transform_player_game_logs" in {
        t.task_id for t in dag.get_task("transform_players").downstream_list
    }

    # transform_player_game_logs → resolve_player_ids
    assert "resolve_player_ids" in {
        t.task_id for t in dag.get_task("transform_player_game_logs").downstream_list
    }


def test_dag_has_slack_callback():
    from airflow.models import DagBag
    from shared.plugins.slack_notifier import notify_failure
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_stats_pipeline"]
    assert dag.on_failure_callback is notify_failure


def test_sensor_targets_mlb_odds_pipeline():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_stats_pipeline"]
    sensor = dag.get_task("wait_for_mlb_odds_pipeline")
    assert sensor.external_dag_id == "mlb_odds_pipeline"
```

- [ ] **Step 2: Run tests — must fail**

Run: `pytest mlb/tests/unit/test_mlb_stats_pipeline_dag.py -v`
Expected: all 5 tests fail with `'mlb_stats_pipeline'` key not present (DAG file doesn't exist yet).

- [ ] **Step 3: Implement the DAG**

Create `mlb/dags/mlb_stats_pipeline_dag.py` with:

```python
# mlb/dags/mlb_stats_pipeline_dag.py
from datetime import date, timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from shared.plugins.db_client import get_data_db_conn, store_raw_response
from mlb.plugins.mlb_stats_client import (
    fetch_teams,
    fetch_players,
    fetch_batter_game_logs,
)
from mlb.plugins.transformers.teams import transform_teams
from mlb.plugins.transformers.players import transform_players
from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
from mlb.plugins.transformers.player_name_resolution import resolve_player_ids
from shared.plugins.slack_notifier import notify_failure

import os

INGEST_DELAY_SECONDS = 1.0

# Namespace raw_api_responses.endpoint values so NBA and MLB rows don't collide.
EP_TEAMS = "mlb_api/teams"
EP_PLAYERS = "mlb_api/players"
EP_PGL = "mlb_api/player_game_logs"


def _current_season():
    """MLB season equals the current calendar year — no split-year format."""
    return str(date.today().year)


def _game_log_window():
    """Return (start_date, end_date) for the daily 3-day lookback."""
    today = date.today()
    return today - timedelta(days=3), today - timedelta(days=1)


def _get_latest_raw(conn, endpoint):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT response FROM raw_api_responses
            WHERE endpoint = %s AND status = 'success'
            ORDER BY fetched_at DESC LIMIT 1
            """,
            (endpoint,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"No successful raw data for endpoint '{endpoint}'. Run ingest first.")
    return row[0]


# ---------------------------------------------------------------------------
# Ingest tasks
# ---------------------------------------------------------------------------

def fetch_teams_task(**context):
    conn = get_data_db_conn()
    try:
        data = fetch_teams(delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, EP_TEAMS, {}, data)
    except Exception:
        store_raw_response(conn, EP_TEAMS, {}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_players_task(**context):
    conn = get_data_db_conn()
    season = _current_season()
    try:
        data = fetch_players(season=season, delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, EP_PLAYERS, {"season": season}, data)
    except Exception:
        store_raw_response(conn, EP_PLAYERS, {"season": season}, None, status="error")
        raise
    finally:
        conn.close()


def fetch_batter_game_logs_task(**context):
    conn = get_data_db_conn()
    start, end = _game_log_window()
    params = {"start_date": start.isoformat(), "end_date": end.isoformat()}
    try:
        data = fetch_batter_game_logs(start, end, delay_seconds=INGEST_DELAY_SECONDS)
        store_raw_response(conn, EP_PGL, params, data)
    except Exception:
        store_raw_response(conn, EP_PGL, params, None, status="error")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Transform tasks
# ---------------------------------------------------------------------------

def transform_teams_task(**context):
    conn = get_data_db_conn()
    try:
        transform_teams(conn, _get_latest_raw(conn, EP_TEAMS))
    finally:
        conn.close()


def transform_players_task(**context):
    conn = get_data_db_conn()
    try:
        transform_players(conn, _get_latest_raw(conn, EP_PLAYERS))
    finally:
        conn.close()


def transform_player_game_logs_task(**context):
    conn = get_data_db_conn()
    try:
        transform_player_game_logs(conn, _get_latest_raw(conn, EP_PGL))
    finally:
        conn.close()


def resolve_player_ids_task(**context):
    conn = get_data_db_conn()
    try:
        slack_url = os.environ.get("SLACK_WEBHOOK_URL")
        resolve_player_ids(conn, slack_webhook_url=slack_url)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="mlb_stats_pipeline",
    default_args=default_args,
    description="Fetch and transform MLB teams, players, and batter game logs",
    schedule_interval="20 15 * * *",  # 8:20am MT (3:20pm UTC)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    catchup=False,
    tags=["mlb", "stats"],
    on_failure_callback=notify_failure,
) as dag:
    wait_for_odds = ExternalTaskSensor(
        task_id="wait_for_mlb_odds_pipeline",
        external_dag_id="mlb_odds_pipeline",
        external_task_id=None,
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
        execution_delta=timedelta(minutes=20),
    )

    t_fetch_teams   = PythonOperator(task_id="fetch_teams",              python_callable=fetch_teams_task)
    t_fetch_players = PythonOperator(task_id="fetch_players",            python_callable=fetch_players_task)
    t_fetch_pgl     = PythonOperator(task_id="fetch_batter_game_logs",   python_callable=fetch_batter_game_logs_task)

    t_xform_teams   = PythonOperator(task_id="transform_teams",              python_callable=transform_teams_task)
    t_xform_players = PythonOperator(task_id="transform_players",            python_callable=transform_players_task)
    t_xform_pgl     = PythonOperator(task_id="transform_player_game_logs",   python_callable=transform_player_game_logs_task)
    t_resolve       = PythonOperator(task_id="resolve_player_ids",           python_callable=resolve_player_ids_task)

    wait_for_odds >> [t_fetch_teams, t_fetch_players, t_fetch_pgl]

    t_fetch_teams >> t_xform_teams
    t_fetch_players >> t_xform_players
    t_fetch_pgl >> t_xform_pgl

    # FK-safe transform ordering: teams populate abbrev lookup; players satisfy
    # mlb_player_game_logs FK
    t_xform_teams >> t_xform_players
    t_xform_players >> t_xform_pgl
    t_xform_pgl >> t_resolve
```

- [ ] **Step 4: Run tests — must pass**

Run: `pytest mlb/tests/unit/test_mlb_stats_pipeline_dag.py -v`
Expected: 5 passed.

- [ ] **Step 5: Run the full MLB test suite to catch regressions**

Run: `pytest mlb/tests/unit/ -v 2>&1 | tail -10`
Expected: all tests pass (18 from client + 4 teams + 4 players + 4 pgl + 6 resolver + 5 DAG = 41). If anything unrelated breaks, stop and investigate — don't amend this commit.

- [ ] **Step 6: Commit**

```bash
git add mlb/dags/mlb_stats_pipeline_dag.py mlb/tests/unit/test_mlb_stats_pipeline_dag.py
git commit -m "feat(mlb): add mlb_stats_pipeline DAG (daily)

Waits on mlb_odds_pipeline via ExternalTaskSensor, then fans out into
fetch_teams / fetch_players / fetch_batter_game_logs (3-day lookback),
serializes transforms in FK-safe order (teams → players → game_logs),
and ends with resolve_player_ids. Runs daily at 8:20am MT.
Settlement is intentionally NOT included — it lives in the future
mlb_score_dag per umbrella spec.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Backfill DAG (`mlb_stats_backfill_dag`)

**Files:**
- Create: `mlb/dags/mlb_stats_backfill_dag.py`
- Create: `mlb/tests/unit/test_mlb_stats_backfill_dag.py`

- [ ] **Step 1: Write failing tests**

Create `mlb/tests/unit/test_mlb_stats_backfill_dag.py` with:

```python
# mlb/tests/unit/test_mlb_stats_backfill_dag.py
"""Structural + behavior tests for mlb_stats_backfill DAG.

The DagBag-based tests require real Airflow installed. The run_backfill
validation tests can run without it (function-level).
"""
from unittest.mock import patch

import pytest


def test_dag_loads_without_errors():
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="mlb/dags/", include_examples=False)
    assert "mlb_stats_backfill" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag_has_no_schedule():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="mlb/dags/", include_examples=False).dags["mlb_stats_backfill"]
    assert dag.schedule_interval is None


def test_run_backfill_rejects_bad_season_start_format():
    from mlb.dags.mlb_stats_backfill_dag import run_backfill
    with pytest.raises(ValueError, match="season_start"):
        run_backfill(params={"season_start": "not-a-year", "season_end": "2025"})


def test_run_backfill_rejects_bad_season_end_format():
    from mlb.dags.mlb_stats_backfill_dag import run_backfill
    with pytest.raises(ValueError, match="season_end"):
        run_backfill(params={"season_start": "2023", "season_end": "25-26"})


def test_run_backfill_rejects_end_before_start():
    from mlb.dags.mlb_stats_backfill_dag import run_backfill
    with pytest.raises(ValueError, match="must be <="):
        run_backfill(params={"season_start": "2025", "season_end": "2023"})


def test_run_backfill_iterates_seasons_and_calls_fetches():
    """3 seasons → fetch_teams called once, fetch_players + fetch_batter_game_logs
    called 3 times each with correct year-stamped date windows."""
    from mlb.dags import mlb_stats_backfill_dag as mod

    with patch.object(mod, "get_data_db_conn") as mock_get_conn, \
         patch.object(mod, "store_raw_response"), \
         patch.object(mod, "fetch_teams", return_value=[]) as m_teams, \
         patch.object(mod, "fetch_players", return_value=[]) as m_players, \
         patch.object(mod, "fetch_batter_game_logs", return_value=[]) as m_pgl, \
         patch.object(mod, "transform_teams"), \
         patch.object(mod, "transform_players"), \
         patch.object(mod, "transform_player_game_logs"), \
         patch.object(mod, "resolve_player_ids"), \
         patch.object(mod, "time"):
        mock_get_conn.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = None
        mod.run_backfill(params={"season_start": "2023", "season_end": "2025"})

    assert m_teams.call_count == 1
    assert m_players.call_count == 3
    assert m_pgl.call_count == 3

    # Each fetch_batter_game_logs call pins its dates to the season year
    years_called = sorted([call.args[0][:4] for call in m_pgl.call_args_list])
    assert years_called == ["2023", "2024", "2025"]
    # Start/end month-day should be 03-15 → 11-15
    for call in m_pgl.call_args_list:
        start, end = call.args[0], call.args[1]
        assert start.endswith("-03-15")
        assert end.endswith("-11-15")
```

- [ ] **Step 2: Run tests — must fail**

Run: `pytest mlb/tests/unit/test_mlb_stats_backfill_dag.py -v`
Expected: 6 fails (module not found / `run_backfill` import error / DagBag key missing).

- [ ] **Step 3: Implement the backfill DAG**

Create `mlb/dags/mlb_stats_backfill_dag.py` with:

```python
# mlb/dags/mlb_stats_backfill_dag.py
import os
import re
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from shared.plugins.db_client import get_data_db_conn, store_raw_response
from mlb.plugins.mlb_stats_client import (
    fetch_teams,
    fetch_players,
    fetch_batter_game_logs,
)
from mlb.plugins.transformers.teams import transform_teams
from mlb.plugins.transformers.players import transform_players
from mlb.plugins.transformers.player_game_logs import transform_player_game_logs
from mlb.plugins.transformers.player_name_resolution import resolve_player_ids

BACKFILL_DELAY_SECONDS = 1.0
SEASON_PATTERN = re.compile(r"^\d{4}$")
SEASON_START_MONTH_DAY = "03-15"
SEASON_END_MONTH_DAY = "11-15"

EP_TEAMS = "mlb_api/teams"
EP_PLAYERS = "mlb_api/players"
EP_PGL = "mlb_api/player_game_logs"


def _season_range(season_start: str, season_end: str):
    """Yield year-string seasons from start to end inclusive, e.g. '2023'..'2025'."""
    for year in range(int(season_start), int(season_end) + 1):
        yield str(year)


def run_backfill(**context):
    params = context["params"]
    season_start = params.get("season_start", "2023")
    season_end = params.get("season_end", "2025")

    if not SEASON_PATTERN.match(season_start):
        raise ValueError(
            f"Invalid season_start format '{season_start}'. Expected YYYY (e.g. 2023)."
        )
    if not SEASON_PATTERN.match(season_end):
        raise ValueError(
            f"Invalid season_end format '{season_end}'. Expected YYYY (e.g. 2025)."
        )
    if season_start > season_end:
        raise ValueError(
            f"season_start '{season_start}' must be <= season_end '{season_end}'."
        )

    conn = get_data_db_conn()
    try:
        # Teams: fetch once (MLB team set is stable across the backfill range)
        teams = fetch_teams(delay_seconds=BACKFILL_DELAY_SECONDS)
        store_raw_response(conn, EP_TEAMS, {}, teams)
        transform_teams(conn, teams)

        for season in _season_range(season_start, season_end):
            # Historical rosters — active_only=False so retired/traded players
            # exist in mlb_players before their game logs land (FK constraint).
            players = fetch_players(
                season=season, active_only=False,
                delay_seconds=BACKFILL_DELAY_SECONDS,
            )
            store_raw_response(conn, EP_PLAYERS, {"season": season}, players)
            transform_players(conn, players)

            start_date = f"{season}-{SEASON_START_MONTH_DAY}"
            end_date = f"{season}-{SEASON_END_MONTH_DAY}"
            logs = fetch_batter_game_logs(
                start_date, end_date,
                delay_seconds=BACKFILL_DELAY_SECONDS,
            )
            store_raw_response(
                conn, EP_PGL, {"season": season}, logs,
            )
            transform_player_game_logs(conn, logs)
            time.sleep(BACKFILL_DELAY_SECONDS)

        resolve_player_ids(
            conn, slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL"),
        )
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="mlb_stats_backfill",
    default_args=default_args,
    description="Historical MLB batter stats seed (manual trigger)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlb", "stats", "backfill"],
    params={
        "season_start": Param(
            "2023", type="string",
            description="Start season YYYY (e.g. 2023)",
        ),
        "season_end": Param(
            "2025", type="string",
            description="End season YYYY (e.g. 2025)",
        ),
    },
) as dag:
    PythonOperator(task_id="run_backfill", python_callable=run_backfill)
```

- [ ] **Step 4: Run tests — must pass**

Run: `pytest mlb/tests/unit/test_mlb_stats_backfill_dag.py -v`
Expected: 6 passed.

- [ ] **Step 5: Run the full MLB test suite for final regression check**

Run: `pytest mlb/tests/unit/ -v 2>&1 | tail -10`
Expected: all tests pass (18 client + 4 teams + 4 players + 4 pgl + 6 resolver + 5 pipeline DAG + 6 backfill DAG = **47 tests**). Stop and investigate anything unrelated that breaks.

- [ ] **Step 6: Run the WHOLE project test suite**

Run: `pytest 2>&1 | tail -5`
Expected: count increases by +47 (the MLB tests) vs. the pre-step-3 baseline; no **new** unrelated failures. Pre-existing failures in NBA DAG mocking / integration tests may still show up — confirm they match the prior baseline and are unrelated to this work.

- [ ] **Step 7: Commit**

```bash
git add mlb/dags/mlb_stats_backfill_dag.py mlb/tests/unit/test_mlb_stats_backfill_dag.py
git commit -m "feat(mlb): add mlb_stats_backfill DAG (manual season range)

Seeds historical MLB seasons one year at a time, using active_only=False
for fetch_players (so historical rosters satisfy FK constraint before
game logs land). Fixed Mar 15 – Nov 15 window per season covers spring
training through World Series. Single sequential task — a failure mid-
range resumes cleanly by re-triggering with updated season_start.
Closes step 3 of issue #6.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Completion criteria

- All 47 MLB unit tests pass (`pytest mlb/tests/unit/ -v`).
- Full project `pytest` run: MLB adds exactly 47 passing tests; no new unrelated failures.
- Both DAGs load without errors under `DagBag` (real Airflow required).
- `mlb/plugins/transformers/` exposes `transform_teams`, `transform_players`, `transform_player_game_logs`, `resolve_player_ids`, `normalize_name`.
- `mlb/dags/` exposes `mlb_stats_pipeline` (daily at 8:20am MT) and `mlb_stats_backfill` (manual).
- No new dependencies in `requirements.txt`.
- No changes outside `mlb/` and the two docs files.

After the final commit, add a comment to issue #6 noting that step 3 is complete and listing commits, per the issue-driven workflow.
