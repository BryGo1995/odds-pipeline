# Multi-Sport Architecture Refactor — Phase 0 + Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the `SportConfig` dataclass (Phase 0) and migrate the six identical-shape NBA/MLB DAGs (odds_pipeline, odds_backfill, feature, feature_backfill, train, score) to shared DAG factories (Phase 1). Net change: ~−800 LOC, no behavior change to scoring/training, no DAG ID changes.

**Architecture:** Each per-sport package exports one `SportConfig` instance (`NBA_SPORT`, `MLB_SPORT`) that captures sport-specific knobs (Odds-API config, ML config, scheduling, prop stat map). Six shared factory functions in `shared/dag_factories/` accept a `SportConfig` and return a fully built `airflow.DAG`. Per-sport DAG files become 5-line thin entrypoints that import the factory and pass the sport. Until Phase 3 lands shared ML modules, factories accept ML callables (score/train functions) as parameters that get passed in from the per-sport DAG file.

**Tech Stack:** Python 3.14, Airflow, pytest, pendulum. Existing test infrastructure stubs out airflow/pendulum/mlflow via `conftest.py` in each test dir; the MLB conftest's `_SmartDAGMock` captures task wiring for DAG attribute assertions.

**Spec:** `docs/superpowers/specs/2026-05-19-multi-sport-architecture-design.md`

---

## File Map

### Created

| File | Responsibility |
|---|---|
| `shared/sport_config.py` | `SportConfig` dataclass — pure data, no behavior |
| `nba/sport.py` | Exports `NBA_SPORT: SportConfig` |
| `mlb/sport.py` | Exports `MLB_SPORT: SportConfig` |
| `shared/dag_factories/__init__.py` | Package marker |
| `shared/dag_factories/train.py` | `build_train_dag(sport, train_all_models_fn)` |
| `shared/dag_factories/score.py` | `build_score_dag(sport, score_fn)` |
| `shared/dag_factories/feature.py` | `build_feature_dag(sport, build_features_fn)` |
| `shared/dag_factories/feature_backfill.py` | `build_feature_backfill_dag(sport, build_features_fn)` |
| `shared/dag_factories/odds_backfill.py` | `build_odds_backfill_dag(sport)` |
| `shared/dag_factories/odds_pipeline.py` | `build_odds_pipeline_dag(sport)` |
| `shared/tests/unit/test_sport_config.py` | Dataclass instantiation + field validation |
| `shared/tests/unit/test_sport_instances.py` | Asserts `NBA_SPORT`/`MLB_SPORT` match `nba/config.py` and `mlb/config.py` constants |
| `shared/tests/unit/test_dag_factories.py` | Factory unit tests (one assertion block per factory) |

### Modified (rewritten as thin entrypoints, ~5 LOC each)

| File | New responsibility |
|---|---|
| `nba/dags/nba_train_dag.py` | Import factory, pass `NBA_SPORT` + train fn |
| `nba/dags/nba_score_dag.py` | Same pattern |
| `nba/dags/nba_feature_dag.py` | Same |
| `nba/dags/nba_feature_backfill_dag.py` | Same |
| `nba/dags/nba_odds_backfill_dag.py` | Same |
| `nba/dags/nba_odds_pipeline_dag.py` | Same |
| `mlb/dags/mlb_*` (six files) | Same pattern, with `MLB_SPORT` |

### Not modified in this plan (deferred to later phases)

- `nba/dags/nba_stats_pipeline_dag.py`, `nba/dags/nba_stats_backfill_dag.py` (Phase 2 — sport-shaped stats DAGs)
- `mlb/dags/mlb_stats_pipeline_dag.py`, `mlb/dags/mlb_stats_backfill_dag.py` (Phase 2)
- `nba/plugins/ml/*`, `mlb/plugins/ml/*` (Phase 3 — shared ML modules)
- `nba/config.py`, `mlb/config.py` (kept side-by-side with `sport.py` during transition; removed at end of Phase 3)
- `sql/migrations/*`, `/data/features/*.parquet` (Phase 4 — data cleanup)

---

## Conventions

- **Test runner:** `pytest <path> -v` from repo root. `pytest.ini` sets `pythonpath = .` so imports like `from shared.sport_config import SportConfig` work.
- **Test isolation:** Unit tests stub airflow/pendulum/mlflow via existing conftest files; no docker required. Integration tests (not used in this plan) require `docker compose up data-postgres`.
- **Commits:** Conventional commit format. One commit per task. Co-author line per repo convention:
  ```
  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```
- **DAG IDs are stable.** Factory builds them as `f"{sport.sport_slug}_<type>"` — produces `nba_train_dag`, `mlb_odds_pipeline`, etc., matching existing IDs exactly. The Airflow UI history continues uninterrupted.
- **Filenames are not renamed in Phase 0/1.** The existing `nba_train_dag.py`/`mlb_train_dag.py` filenames stay. Renaming is deferred to keep this plan focused; the spec's rename suggestion can land as a tidy-up later.

---

## Phase 0 — Foundations

### Task 1: Add `SportConfig` dataclass

**Files:**
- Create: `shared/sport_config.py`
- Create: `shared/tests/unit/test_sport_config.py`

- [ ] **Step 1: Write the failing test**

Create `shared/tests/unit/test_sport_config.py`:

```python
"""Unit tests for SportConfig dataclass."""
import datetime as dt
import pytest

from shared.sport_config import SportConfig


def _example_config(**overrides) -> SportConfig:
    """Helper: build a SportConfig with sensible defaults; override fields as needed."""
    kwargs = dict(
        sport_id="basketball_nba",
        sport_label="NBA",
        sport_slug="nba",
        markets=("h2h", "spreads", "totals"),
        prop_markets=("player_points",),
        bookmakers=("draftkings",),
        regions=("us",),
        odds_format="american",
        scores_days_from=3,
        prop_stat_map={"player_points": "pts"},
        per_prop_features=("implied_prob_over",),
        min_training_rows=50,
        start_date=dt.datetime(2024, 1, 1),
        odds_pipeline_cron="0 15 * * *",
        stats_pipeline_cron="20 15 * * *",
        feature_dag_cron="40 15 * * *",
        score_dag_cron="0 16 * * *",
        train_dag_cron="0 10 * * 1",
    )
    kwargs.update(overrides)
    return SportConfig(**kwargs)


def test_instantiates_with_all_fields():
    cfg = _example_config()
    assert cfg.sport_id == "basketball_nba"
    assert cfg.sport_label == "NBA"
    assert cfg.sport_slug == "nba"


def test_is_frozen():
    cfg = _example_config()
    with pytest.raises((AttributeError, Exception)):
        cfg.sport_id = "anything_else"  # type: ignore[misc]


def test_model_name_derived_from_slug():
    assert _example_config(sport_slug="nba").model_name == "nba_prop_model"
    assert _example_config(sport_slug="mlb").model_name == "mlb_prop_model"


def test_features_filename_prefix_derived_from_slug():
    assert _example_config(sport_slug="nba").features_filename_prefix == "nba_props_features"
    assert _example_config(sport_slug="mlb").features_filename_prefix == "mlb_props_features"


def test_raw_endpoint_namespaces_with_slug():
    cfg = _example_config(sport_slug="nba")
    assert cfg.raw_endpoint("odds") == "nba_odds"
    assert cfg.raw_endpoint("events") == "nba_events"
    assert cfg.raw_endpoint("player_props") == "nba_player_props"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest shared/tests/unit/test_sport_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'shared.sport_config'`

- [ ] **Step 3: Write the dataclass**

Create `shared/sport_config.py`:

```python
"""
SportConfig — the contract every sport's package implements.

Pure data: holds Odds-API knobs, ML hyperparameters, scheduling cron strings,
and prop_stat_map. Sport-specific *behavior* (e.g. settle JOIN queries, stats
client) lives in sport packages and is passed to shared code as callables.
"""
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SportConfig:
    # --- Identity ----------------------------------------------------------
    sport_id: str            # Odds-API sport key, e.g. "basketball_nba"
    sport_label: str         # Display label, e.g. "NBA" — written to recommendations.sport
    sport_slug: str          # Lowercase slug, e.g. "nba" — DAG IDs, endpoint prefixes,
                             # parquet filenames, MLflow model names

    # --- Odds-API ----------------------------------------------------------
    markets: tuple[str, ...]
    prop_markets: tuple[str, ...]
    bookmakers: tuple[str, ...]
    regions: tuple[str, ...]
    odds_format: str
    scores_days_from: int

    # --- ML ----------------------------------------------------------------
    prop_stat_map: dict[str, str]
    per_prop_features: tuple[str, ...]
    min_training_rows: int

    # --- Scheduling --------------------------------------------------------
    start_date: datetime
    odds_pipeline_cron: str
    stats_pipeline_cron: str
    feature_dag_cron: str
    score_dag_cron: str
    train_dag_cron: str

    # --- Derived properties -----------------------------------------------
    @property
    def model_name(self) -> str:
        """Base MLflow model name. Per-prop models are suffixed (e.g. nba_prop_model_player_points)."""
        return f"{self.sport_slug}_prop_model"

    @property
    def features_filename_prefix(self) -> str:
        """Glob prefix for daily feature parquet files."""
        return f"{self.sport_slug}_props_features"

    def raw_endpoint(self, name: str) -> str:
        """Namespaced raw_api_responses.endpoint value, e.g. 'nba_odds'."""
        return f"{self.sport_slug}_{name}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest shared/tests/unit/test_sport_config.py -v`
Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add shared/sport_config.py shared/tests/unit/test_sport_config.py
git commit -m "$(cat <<'EOF'
feat(shared): add SportConfig dataclass

Pure-data contract for per-sport configuration: Odds-API knobs, ML
hyperparameters, schedule cron strings, prop_stat_map. Derived helpers
for model_name, parquet filename prefix, and raw_api_responses endpoint
namespacing.

Foundation for the multi-sport architecture refactor (Phase 0).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Add `NBA_SPORT` instance with consistency tests

**Files:**
- Create: `nba/sport.py`
- Create: `shared/tests/unit/test_sport_instances.py`

- [ ] **Step 1: Write the failing test**

Create `shared/tests/unit/test_sport_instances.py`:

```python
"""Consistency tests: NBA_SPORT / MLB_SPORT must match the values in nba/config.py and mlb/config.py.

These tests guarantee Phase 0 introduces no behavior change. They will be
removed in Phase 3 once nba/config.py and mlb/config.py are deleted.
"""
import pytest


# ---------------------------------------------------------------------------
# NBA
# ---------------------------------------------------------------------------

def test_nba_sport_matches_nba_config():
    from nba.sport import NBA_SPORT
    from nba import config as nba_config

    assert NBA_SPORT.sport_id == nba_config.SPORT
    assert tuple(NBA_SPORT.markets) == tuple(nba_config.MARKETS)
    assert tuple(NBA_SPORT.prop_markets) == tuple(nba_config.PLAYER_PROP_MARKETS)
    assert tuple(NBA_SPORT.bookmakers) == tuple(nba_config.BOOKMAKERS)
    assert tuple(NBA_SPORT.regions) == tuple(nba_config.REGIONS)
    assert NBA_SPORT.odds_format == nba_config.ODDS_FORMAT
    assert NBA_SPORT.scores_days_from == nba_config.SCORES_DAYS_FROM


def test_nba_sport_identity_fields():
    from nba.sport import NBA_SPORT
    assert NBA_SPORT.sport_label == "NBA"
    assert NBA_SPORT.sport_slug == "nba"
    assert NBA_SPORT.model_name == "nba_prop_model"
    assert NBA_SPORT.features_filename_prefix == "nba_props_features"


def test_nba_sport_prop_stat_map_matches_transformer():
    from nba.sport import NBA_SPORT
    from nba.plugins.transformers.features import PROP_STAT_MAP
    assert NBA_SPORT.prop_stat_map == PROP_STAT_MAP


def test_nba_sport_schedule_matches_existing_dags():
    """Cron strings must match the literals in the existing per-sport DAG files."""
    from nba.sport import NBA_SPORT
    assert NBA_SPORT.odds_pipeline_cron == "0 15 * * *"
    assert NBA_SPORT.stats_pipeline_cron == "20 15 * * *"
    assert NBA_SPORT.feature_dag_cron == "40 15 * * *"
    assert NBA_SPORT.score_dag_cron == "0 16 * * *"
    assert NBA_SPORT.train_dag_cron == "0 10 * * 1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest shared/tests/unit/test_sport_instances.py -v -k nba`
Expected: FAIL with `ModuleNotFoundError: No module named 'nba.sport'`

- [ ] **Step 3: Implement `nba/sport.py`**

Create `nba/sport.py`:

```python
"""NBA SportConfig instance — single source of truth for NBA-specific configuration."""
import pendulum

from nba import config as _nba_config
from nba.plugins.transformers.features import PROP_STAT_MAP
from shared.sport_config import SportConfig

NBA_SPORT = SportConfig(
    sport_id=_nba_config.SPORT,
    sport_label="NBA",
    sport_slug="nba",
    markets=tuple(_nba_config.MARKETS),
    prop_markets=tuple(_nba_config.PLAYER_PROP_MARKETS),
    bookmakers=tuple(_nba_config.BOOKMAKERS),
    regions=tuple(_nba_config.REGIONS),
    odds_format=_nba_config.ODDS_FORMAT,
    scores_days_from=_nba_config.SCORES_DAYS_FROM,
    prop_stat_map=PROP_STAT_MAP,
    per_prop_features=(
        "implied_prob_over",
        "line_movement",
        "rolling_avg_5g",
        "rolling_avg_10g",
        "rolling_avg_20g",
        "rolling_std_10g",
        "is_home",
        "rest_days",
    ),
    min_training_rows=50,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    odds_pipeline_cron="0 15 * * *",   # 8am MT
    stats_pipeline_cron="20 15 * * *", # 8:20am MT
    feature_dag_cron="40 15 * * *",    # 8:40am MT
    score_dag_cron="0 16 * * *",       # 9am MT
    train_dag_cron="0 10 * * 1",       # Mon 3am MT
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest shared/tests/unit/test_sport_instances.py -v -k nba`
Expected: 4 NBA tests PASS.

- [ ] **Step 5: Commit**

```bash
git add nba/sport.py shared/tests/unit/test_sport_instances.py
git commit -m "$(cat <<'EOF'
feat(nba): add NBA_SPORT SportConfig instance

Single source of truth for NBA-specific configuration. Sources Odds-API
knobs from nba/config.py and PROP_STAT_MAP from the features transformer.
Schedule cron strings match the literals in existing DAG files.

Includes consistency tests that pin NBA_SPORT to the existing nba/config.py
values to prove Phase 0 introduces no behavior change.

Phase 0/5.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Add `MLB_SPORT` instance with consistency tests

**Files:**
- Create: `mlb/sport.py`
- Modify: `shared/tests/unit/test_sport_instances.py` (append MLB tests)

- [ ] **Step 1: Write the failing test**

Append to `shared/tests/unit/test_sport_instances.py`:

```python


# ---------------------------------------------------------------------------
# MLB
# ---------------------------------------------------------------------------

def test_mlb_sport_matches_mlb_config():
    from mlb.sport import MLB_SPORT
    from mlb import config as mlb_config

    assert MLB_SPORT.sport_id == mlb_config.SPORT
    assert tuple(MLB_SPORT.markets) == tuple(mlb_config.MARKETS)
    assert tuple(MLB_SPORT.prop_markets) == tuple(mlb_config.PLAYER_PROP_MARKETS)
    assert tuple(MLB_SPORT.bookmakers) == tuple(mlb_config.BOOKMAKERS)
    assert tuple(MLB_SPORT.regions) == tuple(mlb_config.REGIONS)
    assert MLB_SPORT.odds_format == mlb_config.ODDS_FORMAT
    assert MLB_SPORT.scores_days_from == mlb_config.SCORES_DAYS_FROM


def test_mlb_sport_identity_fields():
    from mlb.sport import MLB_SPORT
    assert MLB_SPORT.sport_label == "MLB"
    assert MLB_SPORT.sport_slug == "mlb"
    assert MLB_SPORT.model_name == "mlb_prop_model"
    assert MLB_SPORT.features_filename_prefix == "mlb_props_features"


def test_mlb_sport_prop_stat_map_matches_transformer():
    from mlb.sport import MLB_SPORT
    from mlb.plugins.transformers.features import MLB_PROP_STAT_MAP
    assert MLB_SPORT.prop_stat_map == MLB_PROP_STAT_MAP


def test_mlb_sport_schedule_matches_existing_dags():
    from mlb.sport import MLB_SPORT
    assert MLB_SPORT.odds_pipeline_cron == "0 15 * * *"
    assert MLB_SPORT.stats_pipeline_cron == "20 15 * * *"
    assert MLB_SPORT.feature_dag_cron == "40 15 * * *"
    assert MLB_SPORT.score_dag_cron == "0 16 * * *"
    assert MLB_SPORT.train_dag_cron == "0 10 * * 1"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest shared/tests/unit/test_sport_instances.py -v -k mlb`
Expected: FAIL with `ModuleNotFoundError: No module named 'mlb.sport'`

- [ ] **Step 3: Implement `mlb/sport.py`**

Create `mlb/sport.py`:

```python
"""MLB SportConfig instance — single source of truth for MLB-specific configuration."""
import pendulum

from mlb import config as _mlb_config
from mlb.plugins.transformers.features import MLB_PROP_STAT_MAP
from shared.sport_config import SportConfig

MLB_SPORT = SportConfig(
    sport_id=_mlb_config.SPORT,
    sport_label="MLB",
    sport_slug="mlb",
    markets=tuple(_mlb_config.MARKETS),
    prop_markets=tuple(_mlb_config.PLAYER_PROP_MARKETS),
    bookmakers=tuple(_mlb_config.BOOKMAKERS),
    regions=tuple(_mlb_config.REGIONS),
    odds_format=_mlb_config.ODDS_FORMAT,
    scores_days_from=_mlb_config.SCORES_DAYS_FROM,
    prop_stat_map=MLB_PROP_STAT_MAP,
    per_prop_features=(
        "implied_prob_over",
        "line_movement",
        "rolling_avg_5g",
        "rolling_avg_10g",
        "rolling_avg_20g",
        "rolling_std_10g",
        "is_home",
        "rest_days",
    ),
    min_training_rows=50,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Denver"),
    odds_pipeline_cron="0 15 * * *",
    stats_pipeline_cron="20 15 * * *",
    feature_dag_cron="40 15 * * *",
    score_dag_cron="0 16 * * *",
    train_dag_cron="0 10 * * 1",
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest shared/tests/unit/test_sport_instances.py -v`
Expected: 8 tests PASS (4 NBA + 4 MLB).

- [ ] **Step 5: Commit**

```bash
git add mlb/sport.py shared/tests/unit/test_sport_instances.py
git commit -m "$(cat <<'EOF'
feat(mlb): add MLB_SPORT SportConfig instance

Single source of truth for MLB-specific configuration. Mirrors the NBA_SPORT
structure with MLB-specific values (sport_id, prop markets, prop_stat_map).
Consistency tests pin MLB_SPORT to mlb/config.py.

Phase 0/5.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

**Phase 0 complete.** `shared/sport_config.py`, `nba/sport.py`, `mlb/sport.py` exist with consistency tests; no existing code wired to them yet.

---

## Phase 1 — Six DAG Factories

### Task 4: `train` DAG factory

Simplest factory: a single `PythonOperator`, no sensors. Builds the pattern for the rest.

**Files:**
- Create: `shared/dag_factories/__init__.py`
- Create: `shared/dag_factories/train.py`
- Create: `shared/tests/unit/conftest.py`
- Create: `shared/tests/unit/test_dag_factories.py`
- Modify: `nba/dags/nba_train_dag.py`
- Modify: `mlb/dags/mlb_train_dag.py`

- [ ] **Step 1: Add empty package marker**

Create `shared/dag_factories/__init__.py`:

```python
```
(Empty file — marker for Python to treat the directory as a package.)

- [ ] **Step 2: Add a shared/tests/unit conftest with smart airflow stubs**

`shared/tests/unit/` has no conftest.py today, so factory tests would fail to import airflow. Create `shared/tests/unit/conftest.py` (mirrors the MLB conftest's smart DAG-introspection stubs; future plan can deduplicate by extracting to `shared/tests/conftest_helpers.py`):

```python
"""
Airflow + pendulum stubs for shared/tests/unit/.

The _SmartDAGMock captures DAG kwargs and tracks operators registered inside
the `with dag:` block, so factory tests can assert dag_id, tags, tasks,
dependencies, default_args, etc., without a real Airflow install.

If real Airflow is installed in the environment, this module is a no-op.
"""
import importlib.util
import sys
from unittest.mock import MagicMock


class _SmartOperatorMock:
    """Operator factory that registers each created operator with the active DAG."""

    def __call__(self, *args, **kwargs):
        op = MagicMock()
        for k, v in kwargs.items():
            setattr(op, k, v)
        op.upstream_list = []
        op.downstream_list = []

        def _rshift(other):
            others = other if isinstance(other, list) else [other]
            for o in others:
                op.downstream_list.append(o)
                o.upstream_list.append(op)
            return other

        op.__rshift__ = MagicMock(side_effect=_rshift)
        if _SmartDAGMock._current is not None:
            _SmartDAGMock._current.tasks.append(op)
        return op


class _SmartDAGMock:
    _current = None

    def __call__(self, *args, **kwargs):
        if args:
            kwargs.setdefault("dag_id", args[0])
        dag_instance = MagicMock()
        for key, value in kwargs.items():
            setattr(dag_instance, key, value)
        dag_instance.tasks = []
        dag_instance.get_task = lambda task_id: next(
            (t for t in dag_instance.tasks if t.task_id == task_id),
            MagicMock(task_id=task_id, upstream_list=[], downstream_list=[]),
        )

        def _enter(*_):
            _SmartDAGMock._current = dag_instance
            return dag_instance

        def _exit(*_, **__):
            _SmartDAGMock._current = None
            return False

        dag_instance.__enter__ = MagicMock(side_effect=_enter)
        dag_instance.__exit__ = MagicMock(side_effect=_exit)
        return dag_instance


def _stub_airflow():
    existing = sys.modules.get("airflow")
    if existing is not None and not isinstance(existing, MagicMock):
        return  # real Airflow installed — leave alone

    if existing is None:
        airflow_mock = MagicMock()
        sys.modules["airflow"] = airflow_mock
        sys.modules.setdefault("airflow.models", MagicMock())
        sys.modules.setdefault("airflow.models.param", MagicMock())
        sys.modules.setdefault("airflow.operators", MagicMock())
        sys.modules.setdefault("airflow.operators.python", MagicMock())
        sys.modules.setdefault("airflow.sensors", MagicMock())
        sys.modules.setdefault("airflow.sensors.external_task", MagicMock())
    else:
        airflow_mock = existing

    if not isinstance(airflow_mock.DAG, _SmartDAGMock):
        airflow_mock.DAG = _SmartDAGMock()

    sys.modules["airflow.operators.python"].PythonOperator = _SmartOperatorMock()
    sys.modules["airflow.sensors.external_task"].ExternalTaskSensor = _SmartOperatorMock()


def _stub_pendulum():
    if "pendulum" in sys.modules or importlib.util.find_spec("pendulum") is not None:
        return
    pendulum_stub = MagicMock()
    pendulum_stub.datetime = MagicMock(return_value=MagicMock())
    sys.modules.setdefault("pendulum", pendulum_stub)


_stub_airflow()
_stub_pendulum()
```

- [ ] **Step 3: Write the failing factory test**

Create `shared/tests/unit/test_dag_factories.py`:

```python
"""Unit tests for shared DAG factories.

The conftest.py in this directory installs _SmartDAGMock/_SmartOperatorMock,
so we can introspect DAGs returned by the factories without requiring a
real Airflow install.
"""
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# train factory
# ---------------------------------------------------------------------------

@pytest.fixture
def train_fn_mock():
    """A mock train_all_models callable; factory invokes it from the python_callable."""
    return MagicMock(return_value={"player_points": "run_abc"})


def test_train_factory_builds_nba_dag(train_fn_mock):
    from nba.sport import NBA_SPORT
    from shared.dag_factories.train import build_train_dag

    dag = build_train_dag(NBA_SPORT, train_all_models_fn=train_fn_mock)

    assert dag.dag_id == "nba_train_dag"
    assert dag.schedule_interval == "0 10 * * 1"
    assert dag.catchup is False
    assert "nba" in dag.tags
    assert "ml" in dag.tags
    assert len(dag.tasks) == 1
    assert dag.tasks[0].task_id == "train_model"


def test_train_factory_builds_mlb_dag(train_fn_mock):
    from mlb.sport import MLB_SPORT
    from shared.dag_factories.train import build_train_dag

    dag = build_train_dag(MLB_SPORT, train_all_models_fn=train_fn_mock)

    assert dag.dag_id == "mlb_train_dag"
    assert dag.schedule_interval == "0 10 * * 1"
    assert "mlb" in dag.tags
    assert len(dag.tasks) == 1
    assert dag.tasks[0].task_id == "train_model"


def test_train_factory_default_args_retries(train_fn_mock):
    from nba.sport import NBA_SPORT
    from shared.dag_factories.train import build_train_dag

    dag = build_train_dag(NBA_SPORT, train_all_models_fn=train_fn_mock)
    assert dag.default_args["retries"] == 1
```

- [ ] **Step 4: Run test to verify it fails**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k train`
Expected: FAIL with `ModuleNotFoundError: No module named 'shared.dag_factories.train'`

- [ ] **Step 5: Implement the factory**

Create `shared/dag_factories/train.py`:

```python
"""Shared factory for the per-sport train DAG.

Each sport's `<sport>/dags/<sport>_train_dag.py` shrinks to a 5-line entrypoint
that imports this factory and passes its SportConfig + train_all_models callable.

Phase 3 will move the callable into shared/ml/train.py and remove the
train_all_models_fn parameter.
"""
from datetime import timedelta
from typing import Callable

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.plugins.slack_notifier import notify_failure, notify_model_ready
from shared.sport_config import SportConfig

_DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def build_train_dag(sport: SportConfig, train_all_models_fn: Callable) -> DAG:
    """
    Build a sport-specific train DAG.

    train_all_models_fn: zero-arg callable returning {prop_type: mlflow_run_id}.
    """
    def _run_train(**context):
        run_ids = train_all_models_fn()
        context["ti"].xcom_push(key="mlflow_run_ids", value=run_ids)

    with DAG(
        dag_id=f"{sport.sport_slug}_train_dag",
        default_args=_DEFAULT_ARGS,
        description=f"Train per-prop-type XGBoost {sport.sport_label} prop models and log to MLflow",
        schedule_interval=sport.train_dag_cron,
        start_date=sport.start_date,
        catchup=False,
        tags=[sport.sport_slug, "ml"],
        on_success_callback=notify_model_ready,
        on_failure_callback=notify_failure,
    ) as dag:
        PythonOperator(task_id="train_model", python_callable=_run_train)

    return dag
```

- [ ] **Step 6: Run factory tests to verify they pass**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k train`
Expected: 3 tests PASS.

- [ ] **Step 7: Rewrite NBA train DAG file as thin entrypoint**

Replace contents of `nba/dags/nba_train_dag.py`:

```python
"""NBA train DAG — thin entrypoint over the shared factory."""
from nba.plugins.ml.train import train_all_models
from nba.sport import NBA_SPORT
from shared.dag_factories.train import build_train_dag

dag = build_train_dag(NBA_SPORT, train_all_models_fn=train_all_models)
```

- [ ] **Step 8: Rewrite MLB train DAG file as thin entrypoint**

Replace contents of `mlb/dags/mlb_train_dag.py`:

```python
"""MLB train DAG — thin entrypoint over the shared factory."""
from mlb.plugins.ml.train import train_all_models
from mlb.sport import MLB_SPORT
from shared.dag_factories.train import build_train_dag

dag = build_train_dag(MLB_SPORT, train_all_models_fn=train_all_models)
```

- [ ] **Step 9: Verify existing tests still pass**

Run: `pytest nba/tests/unit mlb/tests/unit shared/tests -v --ignore=nba/tests/unit/test_schema.py`
Expected: All previously-passing tests still pass, plus the 3 new factory tests.

- [ ] **Step 10: Verify DAG modules import cleanly under the test stubs**

Run:
```bash
python -c "
import sys
# Pre-load the shared/tests/unit conftest so airflow stubs are installed
import importlib.util
spec = importlib.util.spec_from_file_location('_conftest', 'shared/tests/unit/conftest.py')
_c = importlib.util.module_from_spec(spec); spec.loader.exec_module(_c)
import nba.dags.nba_train_dag as d1
import mlb.dags.mlb_train_dag as d2
print('NBA train dag_id:', d1.dag.dag_id)
print('MLB train dag_id:', d2.dag.dag_id)
"
```
Expected output:
```
NBA train dag_id: nba_train_dag
MLB train dag_id: mlb_train_dag
```

- [ ] **Step 11: Commit**

```bash
git add shared/dag_factories/__init__.py shared/dag_factories/train.py \
        shared/tests/unit/conftest.py shared/tests/unit/test_dag_factories.py \
        nba/dags/nba_train_dag.py mlb/dags/mlb_train_dag.py
git commit -m "$(cat <<'EOF'
refactor(dags): extract train DAG to shared factory

Move the duplicated train DAG body (36 LOC × 2 sports = 72 LOC) into
shared/dag_factories/train.py. Per-sport DAG files become 5-line entrypoints
that pass SportConfig + train_all_models callable.

The train_all_models_fn parameter is Phase-1 scaffolding; Phase 3 will move
the function into shared/ml/train.py and drop the parameter.

Phase 1/5.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: `score` DAG factory

Adds an `ExternalTaskSensor` ahead of the score task.

**Files:**
- Create: `shared/dag_factories/score.py`
- Modify: `shared/tests/unit/test_dag_factories.py` (append score tests)
- Modify: `nba/dags/nba_score_dag.py`
- Modify: `mlb/dags/mlb_score_dag.py`

- [ ] **Step 1: Write the failing test**

Append to `shared/tests/unit/test_dag_factories.py`:

```python


# ---------------------------------------------------------------------------
# score factory
# ---------------------------------------------------------------------------

@pytest.fixture
def score_fn_mock():
    """A mock score(conn, game_date) callable."""
    return MagicMock()


def test_score_factory_builds_nba_dag(score_fn_mock):
    from nba.sport import NBA_SPORT
    from shared.dag_factories.score import build_score_dag

    dag = build_score_dag(NBA_SPORT, score_fn=score_fn_mock)

    assert dag.dag_id == "nba_score_dag"
    assert dag.schedule_interval == "0 16 * * *"
    assert "nba" in dag.tags
    assert "ml" in dag.tags
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"wait_for_nba_feature_dag", "score"}


def test_score_factory_builds_mlb_dag(score_fn_mock):
    from mlb.sport import MLB_SPORT
    from shared.dag_factories.score import build_score_dag

    dag = build_score_dag(MLB_SPORT, score_fn=score_fn_mock)

    assert dag.dag_id == "mlb_score_dag"
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"wait_for_mlb_feature_dag", "score"}


def test_score_factory_sensor_has_correct_external_dag_id(score_fn_mock):
    from nba.sport import NBA_SPORT
    from shared.dag_factories.score import build_score_dag

    dag = build_score_dag(NBA_SPORT, score_fn=score_fn_mock)
    sensor = next(t for t in dag.tasks if t.task_id == "wait_for_nba_feature_dag")
    assert sensor.external_dag_id == "nba_feature_dag"
    assert sensor.mode == "reschedule"
    assert sensor.timeout == 3600


def test_score_factory_default_args_retries(score_fn_mock):
    from nba.sport import NBA_SPORT
    from shared.dag_factories.score import build_score_dag

    dag = build_score_dag(NBA_SPORT, score_fn=score_fn_mock)
    assert dag.default_args["retries"] == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k score`
Expected: FAIL with `ModuleNotFoundError: No module named 'shared.dag_factories.score'`

- [ ] **Step 3: Implement the factory**

Create `shared/dag_factories/score.py`:

```python
"""Shared factory for the per-sport score DAG.

Sensor → score task. The score callable is injected as a parameter during
Phase 1; Phase 3 will move it into shared/ml/score.py.
"""
from datetime import timedelta
from typing import Callable

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from shared.plugins.db_client import get_data_db_conn
from shared.plugins.slack_notifier import notify_failure, notify_score_ready
from shared.sport_config import SportConfig

_DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def build_score_dag(sport: SportConfig, score_fn: Callable) -> DAG:
    """
    Build a sport-specific score DAG.

    score_fn: callable with signature (conn, game_date: str) -> None.
    """
    def _run_score(**context):
        game_date = context["ds"]
        conn = get_data_db_conn()
        try:
            score_fn(conn, game_date)
        finally:
            conn.close()

    with DAG(
        dag_id=f"{sport.sport_slug}_score_dag",
        default_args=_DEFAULT_ARGS,
        description=f"Score today's {sport.sport_label} props and write ranked recommendations",
        schedule_interval=sport.score_dag_cron,
        start_date=sport.start_date,
        catchup=False,
        tags=[sport.sport_slug, "ml"],
        on_success_callback=notify_score_ready,
        on_failure_callback=notify_failure,
    ) as dag:
        wait_for_features = ExternalTaskSensor(
            task_id=f"wait_for_{sport.sport_slug}_feature_dag",
            external_dag_id=f"{sport.sport_slug}_feature_dag",
            external_task_id=None,
            mode="reschedule",
            poke_interval=60,
            timeout=3600,
            execution_delta=timedelta(minutes=20),
        )
        t_score = PythonOperator(task_id="score", python_callable=_run_score)

        wait_for_features >> t_score

    return dag
```

- [ ] **Step 4: Run factory tests to verify they pass**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k score`
Expected: 4 tests PASS.

- [ ] **Step 5: Rewrite NBA score DAG file**

Replace contents of `nba/dags/nba_score_dag.py`:

```python
"""NBA score DAG — thin entrypoint over the shared factory."""
from nba.plugins.ml.score import score
from nba.sport import NBA_SPORT
from shared.dag_factories.score import build_score_dag

dag = build_score_dag(NBA_SPORT, score_fn=score)
```

- [ ] **Step 6: Rewrite MLB score DAG file**

Replace contents of `mlb/dags/mlb_score_dag.py`:

```python
"""MLB score DAG — thin entrypoint over the shared factory."""
from mlb.plugins.ml.score import score
from mlb.sport import MLB_SPORT
from shared.dag_factories.score import build_score_dag

dag = build_score_dag(MLB_SPORT, score_fn=score)
```

- [ ] **Step 7: Verify tests still pass**

Run: `pytest nba/tests/unit mlb/tests/unit shared/tests -v --ignore=nba/tests/unit/test_schema.py`
Expected: All tests pass, including the new 4 score factory tests.

- [ ] **Step 8: Commit**

```bash
git add shared/dag_factories/score.py shared/tests/unit/test_dag_factories.py \
        nba/dags/nba_score_dag.py mlb/dags/mlb_score_dag.py
git commit -m "$(cat <<'EOF'
refactor(dags): extract score DAG to shared factory

Move the score DAG body (54 LOC × 2 sports = 108 LOC) into a shared factory.
Per-sport entrypoint files become 5-line wrappers.

Sensor + task pattern: the factory builds the wait_for_{slug}_feature_dag
ExternalTaskSensor and the score task; per-sport files supply the sport's
score callable.

Phase 1/5.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: `feature` DAG factory

Same sensor + task pattern as score, but task writes parquet via the per-sport `build_features` callable.

**Files:**
- Create: `shared/dag_factories/feature.py`
- Modify: `shared/tests/unit/test_dag_factories.py`
- Modify: `nba/dags/nba_feature_dag.py`
- Modify: `mlb/dags/mlb_feature_dag.py`

- [ ] **Step 1: Write the failing test**

Append to `shared/tests/unit/test_dag_factories.py`:

```python


# ---------------------------------------------------------------------------
# feature factory
# ---------------------------------------------------------------------------

@pytest.fixture
def build_features_fn_mock():
    """A mock build_features(conn, game_date) callable returning an empty-ish DataFrame."""
    import pandas as pd
    mock = MagicMock()
    mock.return_value = pd.DataFrame()  # empty by default
    return mock


def test_feature_factory_builds_nba_dag(build_features_fn_mock):
    from nba.sport import NBA_SPORT
    from shared.dag_factories.feature import build_feature_dag

    dag = build_feature_dag(NBA_SPORT, build_features_fn=build_features_fn_mock)

    assert dag.dag_id == "nba_feature_dag"
    assert dag.schedule_interval == "40 15 * * *"
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"wait_for_nba_stats_pipeline", "build_features"}


def test_feature_factory_builds_mlb_dag(build_features_fn_mock):
    from mlb.sport import MLB_SPORT
    from shared.dag_factories.feature import build_feature_dag

    dag = build_feature_dag(MLB_SPORT, build_features_fn=build_features_fn_mock)

    assert dag.dag_id == "mlb_feature_dag"
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"wait_for_mlb_stats_pipeline", "build_features"}


def test_feature_factory_sensor_waits_on_stats_pipeline(build_features_fn_mock):
    from nba.sport import NBA_SPORT
    from shared.dag_factories.feature import build_feature_dag

    dag = build_feature_dag(NBA_SPORT, build_features_fn=build_features_fn_mock)
    sensor = next(t for t in dag.tasks if t.task_id == "wait_for_nba_stats_pipeline")
    assert sensor.external_dag_id == "nba_stats_pipeline"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k feature`
Expected: FAIL with `ModuleNotFoundError: No module named 'shared.dag_factories.feature'`

- [ ] **Step 3: Implement the factory**

Create `shared/dag_factories/feature.py`:

```python
"""Shared factory for the per-sport feature DAG.

Daily: waits for the per-sport stats pipeline, then builds prop features
and writes a parquet file. Filename prefix comes from sport.features_filename_prefix.
"""
import logging
import os
from datetime import timedelta
from typing import Callable

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from shared.plugins.db_client import get_data_db_conn
from shared.plugins.slack_notifier import notify_failure
from shared.sport_config import SportConfig

_DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

_FEATURES_DIR = os.environ.get("FEATURES_DIR", "/data/features")


def build_feature_dag(sport: SportConfig, build_features_fn: Callable) -> DAG:
    """
    Build a sport-specific feature DAG.

    build_features_fn: callable with signature (conn, game_date: str) -> pd.DataFrame.
    """
    log = logging.getLogger(__name__)

    def _run_build_features(**context):
        game_date = context["ds"]
        output_path = f"{_FEATURES_DIR}/{sport.features_filename_prefix}_{game_date}.parquet"
        conn = get_data_db_conn()
        try:
            df = build_features_fn(conn, game_date)
            if df.empty:
                log.warning("No %s features generated for %s", sport.sport_label, game_date)
                return
            df.to_parquet(output_path, index=False)
            log.info("Wrote %d %s rows to %s", len(df), sport.sport_label, output_path)
        finally:
            conn.close()

    with DAG(
        dag_id=f"{sport.sport_slug}_feature_dag",
        default_args=_DEFAULT_ARGS,
        description=f"Build {sport.sport_label} prop features from Postgres and write to Parquet",
        schedule_interval=sport.feature_dag_cron,
        start_date=sport.start_date,
        catchup=False,
        tags=[sport.sport_slug, "ml"],
        on_failure_callback=notify_failure,
    ) as dag:
        wait_for_stats = ExternalTaskSensor(
            task_id=f"wait_for_{sport.sport_slug}_stats_pipeline",
            external_dag_id=f"{sport.sport_slug}_stats_pipeline",
            external_task_id=None,
            mode="reschedule",
            poke_interval=60,
            timeout=3600,
            execution_delta=timedelta(minutes=20),
        )
        t_build = PythonOperator(task_id="build_features", python_callable=_run_build_features)

        wait_for_stats >> t_build

    return dag
```

**Note on the parquet filename change:** The NBA feature parquet filename moves from `props_features_<date>.parquet` to `nba_props_features_<date>.parquet` (because `sport.features_filename_prefix == "nba_props_features"`). This is the Phase-4 migration arriving early. The downstream score DAG (Task 5) reads `_FEATURES_DIR/<prefix>_<date>.parquet` via `nba/plugins/ml/score.py::load_todays_features`, which still uses the old path. After this commit, NBA scoring will read **missing files** until either (a) the parquet files are renamed on disk (Phase 4 task), or (b) NBA's `load_todays_features` is updated to use the new prefix.

For Phase 1 we accept that NBA feature DAG and score DAG are temporarily on inconsistent filename conventions. The conn-side score test still passes because it mocks the file load. Production NBA scoring will need the on-disk rename before next run — do it as a follow-up task in this same PR:

- [ ] **Step 4: Run factory tests to verify they pass**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k feature`
Expected: 3 tests PASS.

- [ ] **Step 5: Rewrite NBA feature DAG file**

Replace contents of `nba/dags/nba_feature_dag.py`:

```python
"""NBA feature DAG — thin entrypoint over the shared factory."""
from nba.plugins.transformers.features import build_features
from nba.sport import NBA_SPORT
from shared.dag_factories.feature import build_feature_dag

dag = build_feature_dag(NBA_SPORT, build_features_fn=build_features)
```

- [ ] **Step 6: Rewrite MLB feature DAG file**

Replace contents of `mlb/dags/mlb_feature_dag.py`:

```python
"""MLB feature DAG — thin entrypoint over the shared factory."""
from mlb.plugins.transformers.features import build_features
from mlb.sport import MLB_SPORT
from shared.dag_factories.feature import build_feature_dag

dag = build_feature_dag(MLB_SPORT, build_features_fn=build_features)
```

- [ ] **Step 7: Update NBA `load_todays_features` to read the new prefix**

Modify `nba/plugins/ml/score.py` lines around the `load_todays_features` helper:

Locate this block (around line 24):
```python
def load_todays_features(game_date: str, features_dir: str = FEATURES_DIR) -> pd.DataFrame:
    """Load the Parquet feature file for game_date via DuckDB."""
    path = f"{features_dir}/props_features_{game_date}.parquet"
```

Change the path line to:
```python
    path = f"{features_dir}/nba_props_features_{game_date}.parquet"
```

(The NBA train.py `load_training_data` glob will be updated in Phase 3 along with the on-disk rename in Phase 4. For now this targeted change keeps NBA scoring functional after the feature DAG writes to the new prefix.)

- [ ] **Step 8: Rename existing NBA parquet files on disk (in dev environment only)**

If you have local feature files from prior runs:
```bash
for f in /data/features/props_features_*.parquet; do
  [ -e "$f" ] && mv "$f" "${f/props_features_/nba_props_features_}"
done
```

If `/data/features` is empty (clean dev), skip this step.

- [ ] **Step 9: Verify tests still pass**

Run: `pytest nba/tests/unit mlb/tests/unit shared/tests -v --ignore=nba/tests/unit/test_schema.py`
Expected: All tests pass. If any existing NBA test asserts the bare `props_features_` prefix, update that assertion to `nba_props_features_`.

- [ ] **Step 10: Commit**

```bash
git add shared/dag_factories/feature.py shared/tests/unit/test_dag_factories.py \
        nba/dags/nba_feature_dag.py mlb/dags/mlb_feature_dag.py \
        nba/plugins/ml/score.py
git commit -m "$(cat <<'EOF'
refactor(dags): extract feature DAG to shared factory

Move the daily feature-build DAG (63 LOC × 2 sports) into a shared factory.
Parquet filename now comes from sport.features_filename_prefix — NBA moves
from props_features_*.parquet to nba_props_features_*.parquet for naming
consistency with MLB. NBA's load_todays_features path updated to match.

Phase 1/5; partial Phase 4 (parquet naming) lands here to keep NBA feature
DAG and NBA score DAG on a consistent prefix from this commit forward.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: `feature_backfill` DAG factory

Manual-trigger DAG with `Param`s for date range. No sensor.

**Files:**
- Create: `shared/dag_factories/feature_backfill.py`
- Modify: `shared/tests/unit/test_dag_factories.py`
- Modify: `nba/dags/nba_feature_backfill_dag.py`
- Modify: `mlb/dags/mlb_feature_backfill_dag.py`

- [ ] **Step 1: Write the failing test**

Append to `shared/tests/unit/test_dag_factories.py`:

```python


# ---------------------------------------------------------------------------
# feature_backfill factory
# ---------------------------------------------------------------------------

def test_feature_backfill_factory_builds_nba_dag(build_features_fn_mock):
    from nba.sport import NBA_SPORT
    from shared.dag_factories.feature_backfill import build_feature_backfill_dag

    dag = build_feature_backfill_dag(NBA_SPORT, build_features_fn=build_features_fn_mock)

    assert dag.dag_id == "nba_feature_backfill"
    assert dag.schedule_interval is None
    assert "backfill" in dag.tags
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"run_backfill"}


def test_feature_backfill_factory_builds_mlb_dag(build_features_fn_mock):
    from mlb.sport import MLB_SPORT
    from shared.dag_factories.feature_backfill import build_feature_backfill_dag

    dag = build_feature_backfill_dag(MLB_SPORT, build_features_fn=build_features_fn_mock)

    assert dag.dag_id == "mlb_feature_backfill"
    assert dag.schedule_interval is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k feature_backfill`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement the factory**

Create `shared/dag_factories/feature_backfill.py`:

```python
"""Shared factory for the per-sport feature backfill DAG.

Manual-trigger DAG that loops over date_from..date_to, calling the per-sport
build_features callable for each date and writing parquet files. Skips dates
where the output file already exists.
"""
import logging
import os
from datetime import date, datetime, timedelta
from typing import Callable

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from shared.plugins.db_client import get_data_db_conn
from shared.sport_config import SportConfig

_DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

_FEATURES_DIR = os.environ.get("FEATURES_DIR", "/data/features")


def build_feature_backfill_dag(sport: SportConfig, build_features_fn: Callable) -> DAG:
    """build_features_fn: callable (conn, game_date: str) -> pd.DataFrame."""
    log = logging.getLogger(__name__)

    def _run_backfill(**context):
        params = context["params"]
        date_from = date.fromisoformat(params["date_from"])
        date_to = date.fromisoformat(params["date_to"])

        conn = get_data_db_conn()
        try:
            current = date_from
            while current <= date_to:
                game_date = current.isoformat()
                output_path = f"{_FEATURES_DIR}/{sport.features_filename_prefix}_{game_date}.parquet"

                if os.path.exists(output_path):
                    log.info("Skipping %s — file already exists", game_date)
                    current += timedelta(days=1)
                    continue

                df = build_features_fn(conn, game_date)
                if df.empty:
                    log.info("No %s prop data for %s — skipping", sport.sport_label, game_date)
                else:
                    df.to_parquet(output_path, index=False)
                    log.info("Wrote %d %s rows to %s", len(df), sport.sport_label, output_path)

                current += timedelta(days=1)
        finally:
            conn.close()

    with DAG(
        dag_id=f"{sport.sport_slug}_feature_backfill",
        default_args=_DEFAULT_ARGS,
        description=f"On-demand historical {sport.sport_label} feature file generation for ML training",
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=[sport.sport_slug, "ml", "backfill"],
        params={
            "date_from": Param(..., type="string", description="Start date YYYY-MM-DD"),
            "date_to":   Param(..., type="string", description="End date YYYY-MM-DD"),
        },
    ) as dag:
        PythonOperator(task_id="run_backfill", python_callable=_run_backfill)

    return dag
```

- [ ] **Step 4: Run factory tests to verify they pass**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k feature_backfill`
Expected: 2 tests PASS.

- [ ] **Step 5: Rewrite NBA feature backfill DAG file**

Replace contents of `nba/dags/nba_feature_backfill_dag.py`:

```python
"""NBA feature backfill DAG — thin entrypoint over the shared factory."""
from nba.plugins.transformers.features import build_features
from nba.sport import NBA_SPORT
from shared.dag_factories.feature_backfill import build_feature_backfill_dag

dag = build_feature_backfill_dag(NBA_SPORT, build_features_fn=build_features)
```

- [ ] **Step 6: Rewrite MLB feature backfill DAG file**

Replace contents of `mlb/dags/mlb_feature_backfill_dag.py`:

```python
"""MLB feature backfill DAG — thin entrypoint over the shared factory."""
from mlb.plugins.transformers.features import build_features
from mlb.sport import MLB_SPORT
from shared.dag_factories.feature_backfill import build_feature_backfill_dag

dag = build_feature_backfill_dag(MLB_SPORT, build_features_fn=build_features)
```

- [ ] **Step 7: Verify tests still pass**

Run: `pytest nba/tests/unit mlb/tests/unit shared/tests -v --ignore=nba/tests/unit/test_schema.py`
Expected: All tests pass.

- [ ] **Step 8: Commit**

```bash
git add shared/dag_factories/feature_backfill.py shared/tests/unit/test_dag_factories.py \
        nba/dags/nba_feature_backfill_dag.py mlb/dags/mlb_feature_backfill_dag.py
git commit -m "$(cat <<'EOF'
refactor(dags): extract feature_backfill DAG to shared factory

Move the manual-trigger feature backfill DAG (65 LOC × 2 sports) into a
shared factory. Parquet filenames use sport.features_filename_prefix, so
backfills land on the same naming convention as the daily feature DAG.

Phase 1/5.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: `odds_backfill` DAG factory

Manual-trigger DAG with `Param`s; calls Odds-API for events/odds/scores; uses `sport.raw_endpoint()` for endpoint names.

**Files:**
- Create: `shared/dag_factories/odds_backfill.py`
- Modify: `shared/tests/unit/test_dag_factories.py`
- Modify: `nba/dags/nba_odds_backfill_dag.py`
- Modify: `mlb/dags/mlb_odds_backfill_dag.py`

- [ ] **Step 1: Write the failing test**

Append to `shared/tests/unit/test_dag_factories.py`:

```python


# ---------------------------------------------------------------------------
# odds_backfill factory
# ---------------------------------------------------------------------------

def test_odds_backfill_factory_builds_nba_dag():
    from nba.sport import NBA_SPORT
    from shared.dag_factories.odds_backfill import build_odds_backfill_dag

    dag = build_odds_backfill_dag(NBA_SPORT)
    assert dag.dag_id == "nba_odds_backfill"
    assert dag.schedule_interval is None
    assert "backfill" in dag.tags
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"run_backfill"}


def test_odds_backfill_factory_builds_mlb_dag():
    from mlb.sport import MLB_SPORT
    from shared.dag_factories.odds_backfill import build_odds_backfill_dag

    dag = build_odds_backfill_dag(MLB_SPORT)
    assert dag.dag_id == "mlb_odds_backfill"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k odds_backfill`
Expected: FAIL.

- [ ] **Step 3: Implement the factory**

Create `shared/dag_factories/odds_backfill.py`:

```python
"""Shared factory for the per-sport odds backfill DAG.

Manual-trigger DAG that fetches events/odds/scores from the Odds-API over a
specified date range. Uses sport.raw_endpoint() to namespace raw_api_responses
endpoint values consistently (e.g. 'nba_events', 'mlb_odds').

Note: this commit unifies NBA's endpoint naming with MLB's (NBA previously
used bare 'events'/'odds'/'scores'). The Phase-4 SQL migration updates
pre-existing rows in raw_api_responses.
"""
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from shared.plugins.db_client import get_data_db_conn, store_raw_response
from shared.plugins.odds_api_client import fetch_events, fetch_odds, fetch_scores
from shared.plugins.transformers.events import transform_events
from shared.plugins.transformers.odds import transform_odds
from shared.plugins.transformers.player_props import transform_player_props
from shared.plugins.transformers.scores import transform_scores
from shared.sport_config import SportConfig

_DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

_BACKFILL_SLEEP_SECONDS = 2


def build_odds_backfill_dag(sport: SportConfig) -> DAG:
    def _run_backfill(**context):
        api_key = os.environ["ODDS_API_KEY"]
        params = context["params"]
        date_from = params.get("date_from")
        date_to = params.get("date_to")

        extra_event_params = {}
        if date_from:
            extra_event_params["commenceTimeFrom"] = f"{date_from}T00:00:00Z"
        if date_to:
            extra_event_params["commenceTimeTo"] = f"{date_to}T23:59:59Z"

        conn = get_data_db_conn()
        try:
            events_result = fetch_events(api_key=api_key, sport=sport.sport_id, **extra_event_params)
            events = events_result[0] if isinstance(events_result, tuple) else events_result
            store_raw_response(
                conn,
                sport.raw_endpoint("events"),
                {"sport": sport.sport_id, **extra_event_params},
                events,
            )
            transform_events(conn, events)
            time.sleep(_BACKFILL_SLEEP_SECONDS)

            odds_result = fetch_odds(
                api_key=api_key,
                sport=sport.sport_id,
                regions=list(sport.regions),
                markets=list(sport.markets),
                bookmakers=list(sport.bookmakers),
            )
            odds = odds_result[0] if isinstance(odds_result, tuple) else odds_result
            store_raw_response(conn, sport.raw_endpoint("odds"), {"sport": sport.sport_id}, odds)
            transform_odds(conn, odds)
            transform_player_props(conn, odds)
            time.sleep(_BACKFILL_SLEEP_SECONDS)

            scores_result = fetch_scores(api_key=api_key, sport=sport.sport_id, days_from=30)
            scores = scores_result[0] if isinstance(scores_result, tuple) else scores_result
            store_raw_response(
                conn,
                sport.raw_endpoint("scores"),
                {"sport": sport.sport_id, "days_from": 30},
                scores,
            )
            transform_scores(conn, scores)
        finally:
            conn.close()

    with DAG(
        dag_id=f"{sport.sport_slug}_odds_backfill",
        default_args=_DEFAULT_ARGS,
        description=f"On-demand historical {sport.sport_label} odds backfill",
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=[sport.sport_slug, "backfill"],
        params={
            "date_from": Param(None, type=["null", "string"], description="Start date YYYY-MM-DD"),
            "date_to":   Param(None, type=["null", "string"], description="End date YYYY-MM-DD"),
        },
    ) as dag:
        PythonOperator(task_id="run_backfill", python_callable=_run_backfill)

    return dag
```

**Note on the events_result tuple handling:** The current NBA backfill code calls `fetch_events()` and expects a single return value; the MLB backfill destructures `events, _ = fetch_events(...)`. The actual `fetch_events` signature returns `(data, remaining)` in some call sites (per the `shared/plugins/odds_api_client.py`) and possibly just `data` in older NBA code. Defensive handling: check if the result is a tuple and unpack appropriately. This isolates the factory from the inconsistency; either calling convention works.

- [ ] **Step 4: Run factory tests to verify they pass**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k odds_backfill`
Expected: 2 tests PASS.

- [ ] **Step 5: Rewrite NBA odds backfill DAG file**

Replace contents of `nba/dags/nba_odds_backfill_dag.py`:

```python
"""NBA odds backfill DAG — thin entrypoint over the shared factory."""
from nba.sport import NBA_SPORT
from shared.dag_factories.odds_backfill import build_odds_backfill_dag

dag = build_odds_backfill_dag(NBA_SPORT)
```

- [ ] **Step 6: Rewrite MLB odds backfill DAG file**

Replace contents of `mlb/dags/mlb_odds_backfill_dag.py`:

```python
"""MLB odds backfill DAG — thin entrypoint over the shared factory."""
from mlb.sport import MLB_SPORT
from shared.dag_factories.odds_backfill import build_odds_backfill_dag

dag = build_odds_backfill_dag(MLB_SPORT)
```

- [ ] **Step 7: Verify tests still pass**

Run: `pytest nba/tests/unit mlb/tests/unit shared/tests -v --ignore=nba/tests/unit/test_schema.py`
Expected: All tests pass.

- [ ] **Step 8: Commit**

```bash
git add shared/dag_factories/odds_backfill.py shared/tests/unit/test_dag_factories.py \
        nba/dags/nba_odds_backfill_dag.py mlb/dags/mlb_odds_backfill_dag.py
git commit -m "$(cat <<'EOF'
refactor(dags): extract odds_backfill DAG to shared factory

Move the manual-trigger odds backfill DAG (79 LOC NBA + 83 LOC MLB) into a
shared factory. Endpoint names in raw_api_responses now come from
sport.raw_endpoint() — NBA aligns with MLB on the {slug}_{name} convention.

The Phase-4 SQL migration (separate plan) will update pre-existing NBA rows
from bare 'events'/'odds'/'scores' to 'nba_events'/'nba_odds'/'nba_scores'.
Factories are forward-compatible until then.

Phase 1/5.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: `odds_pipeline` DAG factory (the big one)

The most complex of the six: 8 tasks (3 fetches + 3 transforms + player_props fetch + player_props transform) with non-trivial dependency wiring.

**Files:**
- Create: `shared/dag_factories/odds_pipeline.py`
- Modify: `shared/tests/unit/test_dag_factories.py`
- Modify: `nba/dags/nba_odds_pipeline_dag.py`
- Modify: `mlb/dags/mlb_odds_pipeline_dag.py`

- [ ] **Step 1: Write the failing test**

Append to `shared/tests/unit/test_dag_factories.py`:

```python


# ---------------------------------------------------------------------------
# odds_pipeline factory
# ---------------------------------------------------------------------------

def test_odds_pipeline_factory_builds_nba_dag():
    from nba.sport import NBA_SPORT
    from shared.dag_factories.odds_pipeline import build_odds_pipeline_dag

    dag = build_odds_pipeline_dag(NBA_SPORT)

    assert dag.dag_id == "nba_odds_pipeline"
    assert dag.schedule_interval == "0 15 * * *"
    assert "nba" in dag.tags
    assert "odds" in dag.tags

    expected = {
        "fetch_events",
        "fetch_odds",
        "fetch_scores",
        "transform_events",
        "transform_odds",
        "transform_scores",
        "fetch_player_props",
        "transform_player_props",
    }
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == expected


def test_odds_pipeline_factory_builds_mlb_dag():
    from mlb.sport import MLB_SPORT
    from shared.dag_factories.odds_pipeline import build_odds_pipeline_dag

    dag = build_odds_pipeline_dag(MLB_SPORT)

    assert dag.dag_id == "mlb_odds_pipeline"
    assert "mlb" in dag.tags
    task_ids = {t.task_id for t in dag.tasks}
    assert len(task_ids) == 8


def test_odds_pipeline_default_args_retries():
    from nba.sport import NBA_SPORT
    from shared.dag_factories.odds_pipeline import build_odds_pipeline_dag

    dag = build_odds_pipeline_dag(NBA_SPORT)
    assert dag.default_args["retries"] == 3
    assert dag.default_args["retry_exponential_backoff"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k odds_pipeline`
Expected: FAIL.

- [ ] **Step 3: Implement the factory**

Create `shared/dag_factories/odds_pipeline.py`:

```python
"""Shared factory for the per-sport daily odds pipeline DAG.

The 8-task DAG (events + odds + scores + player_props × fetch+transform)
with the dependency wiring:

  [fetch_events, fetch_odds, fetch_scores] -> transform_events
  transform_events -> [transform_odds, transform_scores, fetch_player_props]
  fetch_player_props -> transform_player_props

All endpoint names in raw_api_responses come from sport.raw_endpoint(),
which gives the namespaced {slug}_{name} convention.
"""
import logging
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from shared.plugins.db_client import get_data_db_conn, store_raw_response
from shared.plugins.odds_api_client import (
    fetch_events, fetch_odds, fetch_player_props, fetch_scores,
)
from shared.plugins.slack_notifier import notify_failure
from shared.plugins.transformers.events import transform_events
from shared.plugins.transformers.odds import transform_odds
from shared.plugins.transformers.player_props import transform_player_props
from shared.plugins.transformers.scores import transform_scores
from shared.sport_config import SportConfig

_DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def _fetch_and_store(endpoint_name, fetch_fn, fetch_kwargs, ti):
    api_key = os.environ["ODDS_API_KEY"]
    conn = get_data_db_conn()
    try:
        data, remaining = fetch_fn(api_key=api_key, **fetch_kwargs)
        store_raw_response(
            conn, endpoint=endpoint_name, params=fetch_kwargs, response=data, status="success",
        )
        ti.xcom_push(key="api_remaining", value=remaining)
    except Exception:
        store_raw_response(
            conn, endpoint=endpoint_name, params=fetch_kwargs, response=None, status="error",
        )
        raise
    finally:
        conn.close()


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
        raise ValueError(f"No successful raw data found for endpoint: '{endpoint}'.")
    return row[0]


def build_odds_pipeline_dag(sport: SportConfig) -> DAG:
    log = logging.getLogger(__name__)

    # ----- ingest callables -----
    def _fetch_events_task(**context):
        _fetch_and_store(
            sport.raw_endpoint("events"),
            fetch_events,
            {"sport": sport.sport_id},
            context["ti"],
        )

    def _fetch_odds_task(**context):
        _fetch_and_store(
            sport.raw_endpoint("odds"),
            fetch_odds,
            {
                "sport": sport.sport_id,
                "regions": list(sport.regions),
                "markets": list(sport.markets),
                "bookmakers": list(sport.bookmakers),
                "odds_format": sport.odds_format,
            },
            context["ti"],
        )

    def _fetch_scores_task(**context):
        _fetch_and_store(
            sport.raw_endpoint("scores"),
            fetch_scores,
            {"sport": sport.sport_id, "days_from": sport.scores_days_from},
            context["ti"],
        )

    # ----- transform callables -----
    def _transform_events_task(**_):
        conn = get_data_db_conn()
        try:
            transform_events(conn, _get_latest_raw(conn, sport.raw_endpoint("events")))
        finally:
            conn.close()

    def _transform_odds_task(**_):
        conn = get_data_db_conn()
        try:
            transform_odds(conn, _get_latest_raw(conn, sport.raw_endpoint("odds")))
        finally:
            conn.close()

    def _transform_scores_task(**_):
        conn = get_data_db_conn()
        try:
            transform_scores(conn, _get_latest_raw(conn, sport.raw_endpoint("scores")))
        finally:
            conn.close()

    # ----- player props (multi-game fetch + transform) -----
    def _fetch_player_props_task(**context):
        ti = context["ti"]
        api_key = os.environ["ODDS_API_KEY"]
        conn = get_data_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT game_id FROM games
                    WHERE commence_time >= CURRENT_DATE
                      AND sport = %s
                    ORDER BY commence_time
                    """,
                    (sport.sport_id,),
                )
                game_ids = [row[0] for row in cur.fetchall()]

            if not game_ids:
                log.warning(
                    "No today/upcoming %s games found — skipping player props fetch.",
                    sport.sport_label,
                )
                ti.xcom_push(key="skipped", value=True)
                return

            fetch_kwargs = {
                "sport": sport.sport_id,
                "regions": list(sport.regions),
                "markets": list(sport.prop_markets),
                "bookmakers": list(sport.bookmakers),
                "odds_format": sport.odds_format,
            }
            all_props = []
            remaining = 0
            for game_id in game_ids:
                try:
                    data, remaining = fetch_player_props(
                        api_key=api_key, event_id=game_id, **fetch_kwargs,
                    )
                    all_props.append(data)
                except Exception as e:
                    log.warning("Failed to fetch %s player props for event %s: %s",
                                sport.sport_label, game_id, e)

            store_raw_response(
                conn,
                endpoint=sport.raw_endpoint("player_props"),
                params=fetch_kwargs,
                response=all_props,
                status="success" if all_props else "error",
            )
            ti.xcom_push(key="api_remaining", value=remaining)

            if not all_props:
                raise ValueError(
                    f"All {sport.sport_label} player props fetches failed. "
                    "Check API key and event IDs."
                )
        finally:
            conn.close()

    def _transform_player_props_task(**context):
        ti = context["ti"]
        if ti.xcom_pull(task_ids="fetch_player_props", key="skipped") is True:
            log.info(
                "%s player props fetch was skipped (no games) — nothing to transform.",
                sport.sport_label,
            )
            return
        conn = get_data_db_conn()
        try:
            row = _get_latest_raw(conn, sport.raw_endpoint("player_props"))
            transform_player_props(conn, row)
        finally:
            conn.close()

    # ----- DAG assembly -----
    with DAG(
        dag_id=f"{sport.sport_slug}_odds_pipeline",
        default_args=_DEFAULT_ARGS,
        description=(
            f"Fetch and transform {sport.sport_label} odds, scores, "
            f"and player props from The-Odds-API"
        ),
        schedule_interval=sport.odds_pipeline_cron,
        start_date=sport.start_date,
        catchup=False,
        tags=[sport.sport_slug, "odds"],
        on_failure_callback=notify_failure,
    ) as dag:
        t_fetch_events  = PythonOperator(task_id="fetch_events",           python_callable=_fetch_events_task)
        t_fetch_odds    = PythonOperator(task_id="fetch_odds",             python_callable=_fetch_odds_task)
        t_fetch_scores  = PythonOperator(task_id="fetch_scores",           python_callable=_fetch_scores_task)
        t_xform_events  = PythonOperator(task_id="transform_events",       python_callable=_transform_events_task)
        t_xform_odds    = PythonOperator(task_id="transform_odds",         python_callable=_transform_odds_task)
        t_xform_scores  = PythonOperator(task_id="transform_scores",       python_callable=_transform_scores_task)
        t_fetch_props   = PythonOperator(task_id="fetch_player_props",     python_callable=_fetch_player_props_task)
        t_xform_props   = PythonOperator(task_id="transform_player_props", python_callable=_transform_player_props_task)

        [t_fetch_events, t_fetch_odds, t_fetch_scores] >> t_xform_events
        t_xform_events >> [t_xform_odds, t_xform_scores, t_fetch_props]
        t_fetch_props >> t_xform_props

    return dag
```

- [ ] **Step 4: Run factory tests to verify they pass**

Run: `pytest shared/tests/unit/test_dag_factories.py -v -k odds_pipeline`
Expected: 3 tests PASS.

- [ ] **Step 5: Rewrite NBA odds pipeline DAG file**

Replace contents of `nba/dags/nba_odds_pipeline_dag.py`:

```python
"""NBA odds pipeline DAG — thin entrypoint over the shared factory."""
from nba.sport import NBA_SPORT
from shared.dag_factories.odds_pipeline import build_odds_pipeline_dag

dag = build_odds_pipeline_dag(NBA_SPORT)
```

- [ ] **Step 6: Rewrite MLB odds pipeline DAG file**

Replace contents of `mlb/dags/mlb_odds_pipeline_dag.py`:

```python
"""MLB odds pipeline DAG — thin entrypoint over the shared factory."""
from mlb.sport import MLB_SPORT
from shared.dag_factories.odds_pipeline import build_odds_pipeline_dag

dag = build_odds_pipeline_dag(MLB_SPORT)
```

- [ ] **Step 7: Run full test suite**

Run: `pytest nba/tests/unit mlb/tests/unit shared/tests -v --ignore=nba/tests/unit/test_schema.py`
Expected: All previously-passing tests still pass + all factory tests pass (count should be original baseline + ~17 new tests across the 6 factories).

- [ ] **Step 8: Commit**

```bash
git add shared/dag_factories/odds_pipeline.py shared/tests/unit/test_dag_factories.py \
        nba/dags/nba_odds_pipeline_dag.py mlb/dags/mlb_odds_pipeline_dag.py
git commit -m "$(cat <<'EOF'
refactor(dags): extract odds_pipeline DAG to shared factory

Move the 8-task daily odds pipeline DAG (228 LOC NBA + 236 LOC MLB ≈ 464 LOC
total) into a shared factory. Per-sport entrypoint files shrink to 4 lines.

Endpoint names in raw_api_responses now come from sport.raw_endpoint() —
NBA aligns with MLB on the {slug}_{name} convention (Phase-4 SQL migration
will update pre-existing rows).

Phase 1/5 — completes the six-factory extraction.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Phase-1 verification + README update

Validate end-to-end that all DAGs parse with real Airflow, and update documentation. The SQL migration to unify pre-existing NBA endpoint rows is **deferred to Phase 4** — the factories are forward-compatible: they read and write `nba_events`/`nba_odds`/etc. consistently, and pre-existing rows with bare endpoint names simply become legacy data that future queries don't reference.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Verify all DAGs parse via DagBag (requires Docker)**

Start Postgres + Airflow:
```bash
docker compose up -d data-postgres airflow-webserver
```

Wait ~30s for services to come up, then run:
```bash
docker compose exec airflow-webserver python -c "
from airflow.models import DagBag
db = DagBag(include_examples=False)
print(f'Loaded {len(db.dags)} DAGs')
print('Errors:', db.import_errors)
for dag_id in sorted(db.dags):
    print(f'  - {dag_id}')
"
```
Expected output includes all DAGs with no import errors:
```
Loaded 12 DAGs
Errors: {}
  - mlb_feature_backfill
  - mlb_feature_dag
  - mlb_odds_backfill
  - mlb_odds_pipeline
  - mlb_score_dag
  - mlb_stats_backfill
  - mlb_stats_pipeline
  - mlb_train_dag
  - nba_feature_backfill
  - nba_feature_dag
  - nba_odds_backfill
  - nba_odds_pipeline
  - nba_score_dag
  - nba_stats_backfill
  - nba_stats_pipeline
  - nba_train_dag
```

If errors appear, debug them before proceeding. Common issues:
- Missing import (factory or sport module not found)
- `pendulum` start_date timezone mismatch
- Stale `__pycache__` — run `find . -path ./venv -prune -o -name __pycache__ -type d -print -exec rm -rf {} +`

- [ ] **Step 2: Manual trigger one DAG per sport (smoke test)**

In the Airflow UI (http://localhost:8080):
- Trigger `nba_train_dag` and watch it succeed (uses existing nba_prop_model_* MLflow models).
- Trigger `mlb_train_dag` similarly.

If you don't want to run real training: trigger `nba_odds_backfill` with params `{"date_from": "2024-01-15", "date_to": "2024-01-15"}` — exercises events/odds/scores fetch + store_raw_response using the new `nba_events`/`nba_odds`/`nba_scores` endpoints.

- [ ] **Step 3: Update README architecture section**

Locate the "## Architecture" section in `README.md` (around line 9-27). Replace the diagram and surrounding prose to reflect the new structure:

```markdown
## Architecture

```
SportConfig (shared/sport_config.py)
       │
       ├── NBA_SPORT  (nba/sport.py)  ──► factories build NBA DAGs
       └── MLB_SPORT  (mlb/sport.py)  ──► factories build MLB DAGs

Shared DAG factories (shared/dag_factories/)
  - odds_pipeline, odds_backfill
  - feature, feature_backfill
  - train, score

Per-sport packages
  - nba/dags/*.py            # 5-line entrypoints over the factories
  - nba/plugins/              # NBA-specific stats client + transformers + ML
  - mlb/dags/*.py            # same shape
  - mlb/plugins/
```

Each per-sport DAG file is a thin entrypoint:
```python
# nba/dags/nba_score_dag.py
from nba.plugins.ml.score import score
from nba.sport import NBA_SPORT
from shared.dag_factories.score import build_score_dag

dag = build_score_dag(NBA_SPORT, score_fn=score)
```
```

(Keep the existing per-DAG schedule table below this section.)

- [ ] **Step 4: Run final test suite**

Run: `pytest nba/tests/unit mlb/tests/unit shared/tests -v --ignore=nba/tests/unit/test_schema.py`
Expected: All tests pass.

Also verify the integration tests still pass (they don't touch the factory-refactored code but assert schema/raw_api_responses behavior):
```bash
docker compose up data-postgres -d
pytest nba/tests/unit/test_schema.py nba/tests/integration/ -v
```
Expected: All pass.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "$(cat <<'EOF'
docs: update README architecture section for SportConfig + factory pattern

Closes Phase 0 + Phase 1 of the multi-sport architecture refactor.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase 0 + Phase 1 verification checklist

Before opening the PR, run through this checklist:

- [ ] `pytest nba/tests mlb/tests shared/tests -v --ignore=nba/tests/unit/test_schema.py` — all green
- [ ] `pytest nba/tests/unit/test_schema.py nba/tests/integration/ -v` (with data-postgres up) — all green
- [ ] `docker compose exec airflow-webserver python -c "from airflow.models import DagBag; db = DagBag(include_examples=False); print('errors:', db.import_errors); print('dag_ids:', sorted(db.dags))"` — 0 errors, 16 dag_ids
- [ ] Manual trigger of one NBA + one MLB DAG in the UI succeeds end-to-end
- [ ] `grep -rE '"(events|odds|scores|player_props)"' nba/ mlb/ shared/ --include='*.py'` — only matches inside `sport.raw_endpoint()` callers (no bare endpoint strings remain in code)
- [ ] `wc -l nba/dags/*.py mlb/dags/*.py` — six per-sport DAG files (those touched in Phase 1) are each ≤10 LOC

Expected net LOC change for the PR: **approximately −800 lines** across `nba/dags/` and `mlb/dags/`, offset by ~400 lines of factories + tests.

---

## Notes for the engineer

- **Why factories take an ML callable instead of importing from `shared/ml`?** Phase 3 hasn't shipped yet; `shared/ml/*` doesn't exist. Passing the callable avoids if/elif on `sport_slug` inside factories. When Phase 3 lands, factories will internalize the import and the parameter goes away.
- **Why is the parquet rename inside Phase 1 (Task 6) instead of Phase 4?** Because Task 6 changes the feature DAG's output filename to `<slug>_props_features_<date>.parquet`. If we waited for Phase 4 to update the score DAG's read path, NBA scoring would be broken between Tasks 6 and Phase 4. Landing the on-disk rename + the score-read path change in the same commit keeps NBA scoring functional throughout.
- **Why isn't the SQL migration in this plan?** The factories are forward-compatible: they write namespaced endpoints (`nba_events`/`nba_odds`/etc.) and read with the same names within a single pipeline run. Pre-existing rows with bare endpoint names become legacy data that future queries don't reference. The cleanup migration is deferred to the Phase 4 plan to keep this PR focused on the structural refactor.
- **What about `nba/config.py` and `mlb/config.py`?** They stay. `NBA_SPORT`/`MLB_SPORT` source values from them. Both are removed in Phase 3 alongside the ML module migration (the consistency tests in `test_sport_instances.py` also go then).
