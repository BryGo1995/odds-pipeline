# Recommendation Accuracy Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the feedback loop on daily player prop recommendations by recording whether each pick hit once game stats are settled, surfacing results via the odds-admin API and a Slack summary.

**Architecture:** A `settle_recommendations` task is added at the end of `nba_stats_pipeline`. It joins unsettled `recommendations` rows against `player_game_logs` (via `player_name_mappings`) to compute `actual_result` and `actual_stat_value`, then sends a Slack recap for any game date whose top-10 are fully resolved. The odds-admin FastAPI app gets updated model/schema/router to expose the new columns and an accuracy summary endpoint.

**Tech Stack:** psycopg2, pytest (MagicMock), FastAPI, SQLAlchemy async, httpx (test client), Airflow PythonOperator

---

## File Map

**odds-pipeline (repo root: `/home/bryang/Dev_Space/python_projects/odds-pipeline`):**
- Create: `sql/migrations/005_recommendation_outcomes.sql`
- Create: `nba/plugins/ml/settle.py`
- Create: `nba/tests/unit/ml/test_settle.py`
- Modify: `shared/plugins/slack_notifier.py` — add `notify_picks_settled`
- Modify: `nba/dags/nba_stats_pipeline_dag.py` — add `settle_recommendations` task
- Modify: `nba/tests/unit/test_nba_stats_pipeline_dag.py` — update task set + chain assertions

**odds-admin (repo root: `/home/bryang/Dev_Space/python_projects/odds-admin`):**
- Modify: `api/models.py` — add 3 columns to `PropRecommendation`
- Modify: `api/schemas.py` — extend `PropRecommendationItem`, add `AccuracySummaryItem`
- Modify: `api/routers/recommendations.py` — add `/recommendations/accuracy` endpoint
- Modify: `tests/test_recommendations.py` — add outcome field tests + accuracy endpoint tests

---

## Task 1: Schema migration

**Files:**
- Create: `sql/migrations/005_recommendation_outcomes.sql`

- [ ] **Step 1: Create the migration file**

```sql
-- sql/migrations/005_recommendation_outcomes.sql
-- Migration 005: add outcome tracking columns to recommendations
-- Idempotent: safe to run multiple times.

ALTER TABLE recommendations
  ADD COLUMN IF NOT EXISTS actual_result      BOOLEAN       NULL,
  ADD COLUMN IF NOT EXISTS actual_stat_value  NUMERIC(10,2) NULL,
  ADD COLUMN IF NOT EXISTS settled_at         TIMESTAMPTZ   NULL;

CREATE INDEX IF NOT EXISTS idx_recs_settled ON recommendations (game_date, settled_at)
  WHERE settled_at IS NULL;
```

- [ ] **Step 2: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
git add sql/migrations/005_recommendation_outcomes.sql
git commit -m "feat: add outcome columns to recommendations (migration 005)"
```

---

## Task 2: settle.py — core settle logic

**Files:**
- Create: `nba/plugins/ml/settle.py`
- Create: `nba/tests/unit/ml/test_settle.py`

- [ ] **Step 1: Write the failing tests**

Create `nba/tests/unit/ml/test_settle.py`:

```python
# nba/tests/unit/ml/test_settle.py
from unittest.mock import MagicMock, call, patch
from datetime import date


def _make_cursor(rows=None, stale_ids=None, unsettled_newly=None):
    """Return a mock cursor that returns different row sets on successive fetchall calls."""
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    side_effects = []
    if rows is not None:
        side_effects.append(rows)          # resolvable rows
    if stale_ids is not None:
        side_effects.append(stale_ids)     # stale ids
    if unsettled_newly is not None:
        side_effects.append(unsettled_newly)  # newly settled dates check
    cursor.fetchall.side_effect = side_effects
    return cursor


def test_settle_hits_correct_stat_column_for_points():
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(
        rows=[
            # id, player_name, prop_type, line, game_date, pts, reb, ast, fg3m, fg3a
            (1, "LeBron James", "player_points", 24.5, date(2026, 4, 3), 27, 8, 7, 2, 5),
        ],
        stale_ids=[],
        unsettled_newly=[],
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    update_calls = [
        c for c in cursor.execute.call_args_list
        if "UPDATE" in str(c) and "actual_result" in str(c)
    ]
    assert len(update_calls) == 1
    args = update_calls[0].args[1]
    assert args[0] is True    # 27 > 24.5
    assert args[1] == 27      # actual_stat_value
    assert args[2] == 1       # rec id


def test_settle_records_miss():
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(
        rows=[
            (2, "Jayson Tatum", "player_rebounds", 7.5, date(2026, 4, 3), 18, 6, 4, 1, 3),
        ],
        stale_ids=[],
        unsettled_newly=[],
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    update_calls = [
        c for c in cursor.execute.call_args_list
        if "UPDATE" in str(c) and "actual_result" in str(c)
    ]
    assert len(update_calls) == 1
    args = update_calls[0].args[1]
    assert args[0] is False   # 6 < 7.5


def test_settle_skips_unknown_prop_type():
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(
        rows=[
            (3, "Some Player", "player_steals", 1.5, date(2026, 4, 3), 10, 5, 3, 1, 2),
        ],
        stale_ids=[],
        unsettled_newly=[],
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    update_calls = [
        c for c in cursor.execute.call_args_list
        if "UPDATE" in str(c) and "actual_result" in str(c)
    ]
    assert len(update_calls) == 0


def test_settle_marks_stale_recs_unresolvable():
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(rows=[], stale_ids=[(99,), (100,)], unsettled_newly=[])
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    stale_calls = [
        c for c in cursor.execute.call_args_list
        if "UPDATE" in str(c) and "settled_at" in str(c) and "actual_result" not in str(c)
    ]
    assert len(stale_calls) == 1
    assert [99, 100] == list(stale_calls[0].args[1][0])


def test_settle_commits():
    from nba.plugins.ml.settle import settle_recommendations

    cursor = _make_cursor(rows=[], stale_ids=[], unsettled_newly=[])
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled"):
        settle_recommendations(conn)

    conn.commit.assert_called_once()


def test_settle_sends_slack_when_top10_complete():
    from nba.plugins.ml.settle import settle_recommendations

    # One rec just settled; after settling, game_date 2026-04-03 has all top-10 done.
    # 4 fetchall calls:
    #   1. resolvable rows
    #   2. stale ids
    #   3. completed dates (HAVING query in _notify_completed_dates)
    #   4. recap rows (SELECT in _send_recap)
    recap_rows = [
        ("Player A", "player_points", 20.0, "Over", True, 25.0, 0.10),
    ]
    cursor = _make_cursor(
        rows=[
            (1, "Player A", "player_points", 20.0, date(2026, 4, 3), 25, 5, 3, 2, 4),
        ],
        stale_ids=[],
        unsettled_newly=[(date(2026, 4, 3),)],
    )
    # Append the 4th side_effect for _send_recap's fetchall
    cursor.fetchall.side_effect = list(cursor.fetchall.side_effect) + [recap_rows]
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("nba.plugins.ml.settle.notify_picks_settled") as mock_notify:
        settle_recommendations(conn)

    mock_notify.assert_called_once()
    call_args = mock_notify.call_args
    assert call_args.args[0] == date(2026, 4, 3)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/ml/test_settle.py -v
```

Expected: `ModuleNotFoundError: No module named 'nba.plugins.ml.settle'`

- [ ] **Step 3: Implement settle.py**

Create `nba/plugins/ml/settle.py`:

```python
# nba/plugins/ml/settle.py
"""
Settle daily recommendations against actual game outcomes.

settle_recommendations(conn) looks up each unsettled recommendation in
player_game_logs (via player_name_mappings), records actual_result and
actual_stat_value, and triggers a Slack recap when a game date's top-10
are fully resolved.
"""
import logging
from datetime import date

from nba.plugins.transformers.features import PROP_STAT_MAP
from shared.plugins.slack_notifier import notify_picks_settled

log = logging.getLogger(__name__)

_STAT_COLS = list(PROP_STAT_MAP.values())  # ['pts', 'reb', 'ast', 'fg3m', 'fg3a']


def settle_recommendations(conn) -> None:
    """
    Settle unsettled recommendations and send Slack recap for any game date
    whose top-10 picks are now fully resolved.
    """
    newly_settled_dates = set()

    with conn.cursor() as cur:
        # Fetch all unsettled recs that have a matching game log entry
        cur.execute(
            """
            SELECT
                r.id,
                r.player_name,
                r.prop_type,
                r.line,
                r.game_date,
                pgl.pts,
                pgl.reb,
                pgl.ast,
                pgl.fg3m,
                pgl.fg3a
            FROM recommendations r
            JOIN player_name_mappings m ON m.odds_api_name = r.player_name
            JOIN player_game_logs pgl
                ON pgl.player_id = m.nba_player_id
               AND pgl.game_date = r.game_date
            WHERE r.settled_at IS NULL
              AND r.game_date < CURRENT_DATE
            """
        )
        resolvable = cur.fetchall()

        # Fetch stale recs (>7 days old) that never resolved — mark unresolvable
        cur.execute(
            """
            SELECT id FROM recommendations
            WHERE settled_at IS NULL
              AND game_date < CURRENT_DATE - INTERVAL '7 days'
            """
        )
        stale_ids = [row[0] for row in cur.fetchall()]

    _settle_resolvable(conn, resolvable, newly_settled_dates)
    _mark_stale(conn, stale_ids, newly_settled_dates)

    conn.commit()

    _notify_completed_dates(conn, newly_settled_dates)


def _settle_resolvable(conn, rows, newly_settled_dates: set) -> None:
    stat_col_index = {col: i for i, col in enumerate(_STAT_COLS)}
    # rows: id, player_name, prop_type, line, game_date, pts, reb, ast, fg3m, fg3a
    stats_offset = 5  # first stat column index in the row tuple

    for row in rows:
        rec_id, player_name, prop_type, line, game_date = row[:5]
        stat_col = PROP_STAT_MAP.get(prop_type)
        if not stat_col:
            log.warning("Unknown prop_type '%s' for rec id=%d — skipping", prop_type, rec_id)
            continue

        stat_val = row[stats_offset + _STAT_COLS.index(stat_col)]
        if stat_val is None:
            continue

        actual_result = float(stat_val) > float(line)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE recommendations
                SET actual_result = %s, actual_stat_value = %s, settled_at = NOW()
                WHERE id = %s
                """,
                (actual_result, stat_val, rec_id),
            )
        newly_settled_dates.add(game_date)


def _mark_stale(conn, stale_ids: list, newly_settled_dates: set) -> None:
    if not stale_ids:
        return
    log.warning("Marking %d stale recommendation(s) as unresolvable: ids=%s", len(stale_ids), stale_ids)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE recommendations
            SET settled_at = NOW()
            WHERE id = ANY(%s) AND settled_at IS NULL
            """,
            (stale_ids,),
        )


def _notify_completed_dates(conn, newly_settled_dates: set) -> None:
    """For each date settled in this run, check if all top-10 recs are now done."""
    if not newly_settled_dates:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT game_date
            FROM recommendations
            WHERE game_date = ANY(%s)
              AND rank <= 10
            GROUP BY game_date
            HAVING COUNT(*) FILTER (WHERE settled_at IS NULL) = 0
            """,
            (list(newly_settled_dates),),
        )
        completed_dates = [row[0] for row in cur.fetchall()]

    for game_date in completed_dates:
        _send_recap(conn, game_date)


def _send_recap(conn, game_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT player_name, prop_type, line, outcome, actual_result,
                   actual_stat_value, edge
            FROM recommendations
            WHERE game_date = %s AND rank <= 10
            ORDER BY rank
            """,
            (game_date,),
        )
        rows = cur.fetchall()

    results = [
        {
            "player_name":      r[0],
            "prop_type":        r[1],
            "line":             float(r[2]) if r[2] is not None else None,
            "outcome":          r[3],
            "actual_result":    r[4],
            "actual_stat_value": float(r[5]) if r[5] is not None else None,
            "edge":             float(r[6]) if r[6] is not None else None,
        }
        for r in rows
    ]
    notify_picks_settled(game_date, results)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/ml/test_settle.py -v
```

Expected: all 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add nba/plugins/ml/settle.py nba/tests/unit/ml/test_settle.py
git commit -m "feat: add settle_recommendations module with tests"
```

---

## Task 3: notify_picks_settled in slack_notifier.py

**Files:**
- Modify: `shared/plugins/slack_notifier.py`
- Create: `nba/tests/unit/test_slack_notifier_settle.py`

- [ ] **Step 1: Write the failing tests**

Create `nba/tests/unit/test_slack_notifier_settle.py`:

```python
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
    with patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, _RESULTS)
    mock_post.assert_called_once()


def test_notify_picks_settled_includes_hit_rate():
    from shared.plugins.slack_notifier import notify_picks_settled
    with patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, _RESULTS)
    text = mock_post.call_args.args[0]
    assert "2/3" in text
    assert "67%" in text or "66%" in text


def test_notify_picks_settled_includes_player_names():
    from shared.plugins.slack_notifier import notify_picks_settled
    with patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, _RESULTS)
    text = mock_post.call_args.args[0]
    assert "LeBron James" in text
    assert "Jayson Tatum" in text


def test_notify_picks_settled_marks_hits_and_misses():
    from shared.plugins.slack_notifier import notify_picks_settled
    with patch("shared.plugins.slack_notifier._post") as mock_post:
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
    with patch("shared.plugins.slack_notifier._post") as mock_post:
        notify_picks_settled(_GAME_DATE, results_with_unknown)
    text = mock_post.call_args.args[0]
    assert "DNP Player" in text
    assert "—" in text  # unresolvable shown with dash
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/test_slack_notifier_settle.py -v
```

Expected: `ImportError` or `AttributeError: module has no attribute 'notify_picks_settled'`

- [ ] **Step 3: Add notify_picks_settled to slack_notifier.py**

Add after the `notify_model_ready` function in `shared/plugins/slack_notifier.py`:

```python
def notify_picks_settled(game_date, results: list[dict]) -> None:
    """
    Post a picks recap to Slack for a settled game date.

    Args:
        game_date: datetime.date of the game day
        results:   list of dicts with keys:
                   player_name, prop_type, line, outcome, actual_result,
                   actual_stat_value, edge
                   actual_result=None means unresolvable (DNP / postponed)
    """
    if not _WEBHOOK_URL:
        logger.warning("Slack notification skipped: SLACK_WEBHOOK_URL not set")
        return

    date_str = game_date.strftime("%b %-d, %Y")

    hits   = sum(1 for r in results if r["actual_result"] is True)
    total  = sum(1 for r in results if r["actual_result"] is not None)
    pct    = int(round(hits / total * 100)) if total else 0
    avgedge = (
        sum(r["edge"] for r in results if r["edge"] is not None) / len(results)
        if results else 0.0
    )

    header = (
        f"📊 Picks recap — {date_str}\n"
        f"Top-{len(results)}: {hits}/{total} hit ({pct}%) | Avg edge: {avgedge:+.3f}"
    )

    lines = []
    _PROP_LABELS = {
        "player_points":           "Points",
        "player_rebounds":         "Rebounds",
        "player_assists":          "Assists",
        "player_threes":           "3-Pointers",
        "player_threes_attempts":  "3PA",
    }
    for r in results:
        prop_label = _PROP_LABELS.get(r["prop_type"], r["prop_type"])
        line_str   = f"O {r['line']}"
        edge_str   = f"+{r['edge']:.3f}" if r["edge"] is not None else "n/a"

        if r["actual_result"] is True:
            emoji   = "✅"
            stat_str = str(int(r["actual_stat_value"])) if r["actual_stat_value"] is not None else "—"
        elif r["actual_result"] is False:
            emoji   = "❌"
            stat_str = str(int(r["actual_stat_value"])) if r["actual_stat_value"] is not None else "—"
        else:
            emoji   = "❓"
            stat_str = "—"

        lines.append(
            f"{emoji} {r['player_name']} — {prop_label} {line_str} | Scored: {stat_str} | Edge: {edge_str}"
        )

    text = header + "\n\n" + "\n".join(lines)
    _post(text)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/test_slack_notifier_settle.py -v
```

Expected: all 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add shared/plugins/slack_notifier.py nba/tests/unit/test_slack_notifier_settle.py
git commit -m "feat: add notify_picks_settled Slack notification with tests"
```

---

## Task 4: Add settle_recommendations task to nba_stats_pipeline

**Files:**
- Modify: `nba/dags/nba_stats_pipeline_dag.py`
- Modify: `nba/tests/unit/test_nba_stats_pipeline_dag.py`

- [ ] **Step 1: Update the DAG tests to reflect the new task**

In `nba/tests/unit/test_nba_stats_pipeline_dag.py`, update `test_dag_has_expected_tasks` and `test_dag_task_chain`:

```python
def test_dag_has_expected_tasks():
    from airflow.models import DagBag
    dag = DagBag(dag_folder="nba/dags/", include_examples=False).dags["nba_stats_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {
        "wait_for_nba_odds_pipeline",
        "fetch_teams", "fetch_players", "fetch_player_game_logs",
        "fetch_team_game_logs", "fetch_team_season_stats",
        "transform_teams", "transform_players",
        "transform_player_game_logs", "transform_team_game_logs", "transform_team_season_stats",
        "resolve_player_ids", "link_nba_game_ids",
        "settle_recommendations",
    }
```

Also add at the end of `test_dag_task_chain`:

```python
    # link_nba_game_ids >> settle_recommendations
    assert "settle_recommendations" in [
        t.task_id for t in dag.get_task("link_nba_game_ids").downstream_list
    ]
```

- [ ] **Step 2: Run updated tests to verify they fail**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/test_nba_stats_pipeline_dag.py -v
```

Expected: `test_dag_has_expected_tasks` FAILS — `settle_recommendations` not in task set

- [ ] **Step 3: Add the task to nba_stats_pipeline_dag.py**

At the top of `nba/dags/nba_stats_pipeline_dag.py`, add to the imports:

```python
from nba.plugins.ml.settle import settle_recommendations as _settle_recommendations
```

Add the wrapper function after `run_link_nba_game_ids`:

```python
def run_settle_recommendations(**context):
    conn = get_data_db_conn()
    try:
        _settle_recommendations(conn)
    finally:
        conn.close()
```

Add the task inside the `with DAG(...)` block, after `t_link_games`:

```python
    t_settle = PythonOperator(
        task_id="settle_recommendations",
        python_callable=run_settle_recommendations,
    )
```

Update the dependency chain (last line of the DAG block):

```python
    t_resolve >> t_link_games >> t_settle
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/test_nba_stats_pipeline_dag.py -v
```

Expected: all tests PASS

- [ ] **Step 5: Run full test suite to check for regressions**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
pytest nba/tests/unit/ -v
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add nba/dags/nba_stats_pipeline_dag.py nba/tests/unit/test_nba_stats_pipeline_dag.py
git commit -m "feat: add settle_recommendations task to nba_stats_pipeline DAG"
```

---

## Task 5: odds-admin — model, schema, and existing test updates

**Files:**
- Modify: `api/models.py`
- Modify: `api/schemas.py`
- Modify: `tests/test_recommendations.py`

- [ ] **Step 1: Write failing tests for the new outcome fields**

Add to the bottom of `tests/test_recommendations.py`:

```python
# ── Outcome fields on existing endpoints ─────────────────────────────────────

async def test_list_recommendations_includes_outcome_fields(client, db):
    today = date(2026, 3, 28)
    rec = _rec("LeBron James", "player_points", "DraftKings", 1, today)
    rec.actual_result = True
    rec.actual_stat_value = Decimal("27.0")
    from datetime import datetime, timezone
    rec.settled_at = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
    db.add(rec)
    await db.commit()

    response = await client.get("/api/recommendations?date=2026-03-28")
    data = response.json()
    assert len(data) == 1
    assert data[0]["actual_result"] is True
    assert float(data[0]["actual_stat_value"]) == 27.0
    assert data[0]["settled_at"] is not None


async def test_list_recommendations_outcome_null_when_unsettled(client, db):
    today = date(2026, 3, 28)
    db.add(_rec("Player A", "player_points", "DraftKings", 1, today))
    await db.commit()

    response = await client.get("/api/recommendations?date=2026-03-28")
    data = response.json()
    assert data[0]["actual_result"] is None
    assert data[0]["actual_stat_value"] is None
    assert data[0]["settled_at"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
pytest tests/test_recommendations.py -v -k "outcome"
```

Expected: `AttributeError` — `PropRecommendation` has no attribute `actual_result`

- [ ] **Step 3: Update models.py**

Add to the `PropRecommendation` class in `api/models.py` after the `game_date` field:

```python
    actual_result:     Mapped[bool | None]    = mapped_column(Boolean)
    actual_stat_value: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    settled_at:        Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
```

Ensure `from datetime import datetime, date` is already imported (it is).

- [ ] **Step 4: Update schemas.py**

Add to `PropRecommendationItem` in `api/schemas.py` after `game_date`:

```python
    actual_result:     bool | None = None
    actual_stat_value: Decimal | None = None
    settled_at:        datetime | None = None
```

Also add the new `AccuracySummaryItem` schema after `PropRecommendationItem`:

```python
class AccuracySummaryItem(_Base):
    game_date:       date
    top_10_hits:     int
    top_10_total:    int
    top_10_hit_rate: float | None
    avg_edge_hits:   float | None
    avg_edge_misses: float | None
    settled:         int
    total:           int
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
pytest tests/test_recommendations.py -v -k "outcome"
```

Expected: both new tests PASS

- [ ] **Step 6: Run full test suite to check for regressions**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
pytest tests/ -v
```

Expected: all tests PASS

- [ ] **Step 7: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add api/models.py api/schemas.py tests/test_recommendations.py
git commit -m "feat: add outcome columns to PropRecommendation model and schema"
```

---

## Task 6: odds-admin — /recommendations/accuracy endpoint

**Files:**
- Modify: `api/routers/recommendations.py`
- Modify: `tests/test_recommendations.py`

- [ ] **Step 1: Write failing tests for the accuracy endpoint**

Add to the bottom of `tests/test_recommendations.py`:

```python
# ── GET /api/recommendations/accuracy ────────────────────────────────────────

async def test_accuracy_returns_zeros_when_no_recs(client):
    response = await client.get("/api/recommendations/accuracy?date=2026-03-28")
    assert response.status_code == 200
    data = response.json()
    assert data["top_10_hits"] == 0
    assert data["top_10_total"] == 0
    assert data["top_10_hit_rate"] is None
    assert data["total"] == 0


async def test_accuracy_counts_hits_and_misses(client, db):
    game_date = date(2026, 3, 28)
    for i, (result, edge) in enumerate([(True, 0.10), (True, 0.08), (False, 0.06)], start=1):
        r = _rec(f"Player {i}", "player_points", "DraftKings", i, game_date, edge=edge)
        r.actual_result = result
        r.actual_stat_value = Decimal("25.0")
        from datetime import datetime, timezone
        r.settled_at = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
        db.add(r)
    await db.commit()

    response = await client.get("/api/recommendations/accuracy?date=2026-03-28")
    data = response.json()
    assert data["top_10_hits"] == 2
    assert data["top_10_total"] == 3
    assert abs(data["top_10_hit_rate"] - 2/3) < 0.01
    assert data["settled"] == 3
    assert data["total"] == 3


async def test_accuracy_avg_edge_split(client, db):
    game_date = date(2026, 3, 28)
    # 2 hits with edge 0.10 and 0.08; 1 miss with edge 0.06
    for i, (result, edge) in enumerate([(True, 0.10), (True, 0.08), (False, 0.06)], start=1):
        r = _rec(f"Player {i}", "player_points", "DraftKings", i, game_date, edge=edge)
        r.actual_result = result
        r.actual_stat_value = Decimal("25.0")
        from datetime import datetime, timezone
        r.settled_at = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
        db.add(r)
    await db.commit()

    response = await client.get("/api/recommendations/accuracy?date=2026-03-28")
    data = response.json()
    assert abs(data["avg_edge_hits"] - 0.09) < 0.001    # (0.10 + 0.08) / 2
    assert abs(data["avg_edge_misses"] - 0.06) < 0.001


async def test_accuracy_excludes_ranks_above_10(client, db):
    game_date = date(2026, 3, 28)
    # Rank 1-10: mix of hits/misses; rank 11: hit that should NOT count
    for i in range(1, 12):
        r = _rec(f"Player {i}", "player_points", "DraftKings", i, game_date)
        r.actual_result = True
        r.actual_stat_value = Decimal("25.0")
        from datetime import datetime, timezone
        r.settled_at = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
        db.add(r)
    await db.commit()

    response = await client.get("/api/recommendations/accuracy?date=2026-03-28")
    data = response.json()
    assert data["top_10_total"] == 10
    assert data["total"] == 11


async def test_accuracy_defaults_to_today(client, db):
    import datetime as dt
    today = dt.date.today()
    r = _rec("Today Player", "player_points", "DraftKings", 1, today)
    r.actual_result = True
    r.actual_stat_value = Decimal("25.0")
    from datetime import datetime, timezone
    r.settled_at = datetime.now(tz=timezone.utc)
    db.add(r)
    await db.commit()

    response = await client.get("/api/recommendations/accuracy")
    data = response.json()
    assert data["top_10_hits"] == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
pytest tests/test_recommendations.py -v -k "accuracy"
```

Expected: `404 Not Found` — endpoint does not exist yet

- [ ] **Step 3: Add the accuracy endpoint to routers/recommendations.py**

Add to the imports at the top of `api/routers/recommendations.py`:

```python
from sqlalchemy import func
from api.schemas import AccuracySummaryItem
```

Add after the `top_recommendations` function:

```python
@router.get("/recommendations/accuracy", response_model=AccuracySummaryItem)
async def recommendations_accuracy(
    date: str = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    game_date = (
        datetime.date.fromisoformat(date)
        if date
        else datetime.date.today()
    )

    top10_q = (
        select(PropRecommendation)
        .where(
            PropRecommendation.game_date == game_date,
            PropRecommendation.rank <= 10,
        )
    )
    top10_result = await db.execute(top10_q)
    top10_recs = top10_result.scalars().all()

    all_q = select(func.count()).where(PropRecommendation.game_date == game_date)
    total = (await db.execute(all_q)).scalar_one()

    settled_top10 = [r for r in top10_recs if r.actual_result is not None]
    hits = [r for r in settled_top10 if r.actual_result is True]
    misses = [r for r in settled_top10 if r.actual_result is False]

    top_10_total = len(settled_top10)
    top_10_hits = len(hits)
    top_10_hit_rate = (top_10_hits / top_10_total) if top_10_total > 0 else None

    def _avg_edge(recs):
        edges = [float(r.edge) for r in recs if r.edge is not None]
        return sum(edges) / len(edges) if edges else None

    settled_all_q = (
        select(func.count())
        .where(
            PropRecommendation.game_date == game_date,
            PropRecommendation.settled_at.isnot(None),
        )
    )
    settled = (await db.execute(settled_all_q)).scalar_one()

    return AccuracySummaryItem(
        game_date=game_date,
        top_10_hits=top_10_hits,
        top_10_total=top_10_total,
        top_10_hit_rate=top_10_hit_rate,
        avg_edge_hits=_avg_edge(hits),
        avg_edge_misses=_avg_edge(misses),
        settled=settled,
        total=total,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
pytest tests/test_recommendations.py -v -k "accuracy"
```

Expected: all 5 accuracy tests PASS

- [ ] **Step 5: Run full test suite to check for regressions**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
pytest tests/ -v
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add api/routers/recommendations.py api/schemas.py tests/test_recommendations.py
git commit -m "feat: add /recommendations/accuracy endpoint with tests"
```
