# Recommendation Accuracy Tracking — Design Spec

**Issue:** BryGo1995/odds-pipeline#9
**Date:** 2026-04-04
**Status:** Approved

## Overview

Close the feedback loop on daily player prop recommendations by recording whether each pick hit once game stats are settled. Track all recommendations with outcomes, surface top-10 accuracy as the primary metric. Deliver results via a new odds-admin API endpoint and a daily Slack summary.

---

## 1. Schema

New migration (`005_recommendation_outcomes.sql`) adds three nullable columns to `recommendations`:

```sql
ALTER TABLE recommendations
  ADD COLUMN actual_result      BOOLEAN       NULL,
  ADD COLUMN actual_stat_value  NUMERIC(10,2) NULL,
  ADD COLUMN settled_at         TIMESTAMPTZ   NULL;
```

- `settled_at IS NULL, actual_result IS NULL` = pending (not yet attempted)
- `settled_at IS NOT NULL, actual_result = TRUE` = hit (actual stat > line)
- `settled_at IS NOT NULL, actual_result = FALSE` = miss
- `settled_at IS NOT NULL, actual_result IS NULL` = unresolvable (7-day timeout, no game log found)

---

## 2. Settle Logic

New module: `nba/plugins/ml/settle.py`, function `settle_recommendations(conn)`.

**Steps:**
1. Query all `recommendations` where `actual_result IS NULL AND game_date < CURRENT_DATE`
2. For each rec, resolve the player's `nba_player_id` via `player_name_mappings` (`odds_api_name → nba_player_id`)
3. Look up the matching row in `player_game_logs` by `player_id` and `game_date`
4. Use `PROP_STAT_MAP` to identify the correct stat column (e.g. `player_points → pts`)
5. `UPDATE recommendations SET actual_result = (stat > line), actual_stat_value = stat, settled_at = NOW()`
6. If no game log found (DNP, postponed), leave `actual_result = NULL` — retried on next run
7. After settling, send a Slack summary for any `game_date` where all top-10 recommendations have `settled_at IS NOT NULL` (i.e. every top-10 rec is either resolved with an outcome or marked unresolvable)

**Unresolvable recs:** After 7 days with `actual_result IS NULL`, mark as unresolvable by setting `actual_result = NULL` and `settled_at = NOW()` with a logged warning. This prevents old stale recs from blocking Slack notifications indefinitely. A separate `unresolvable` boolean flag is not needed — `settled_at IS NOT NULL AND actual_result IS NULL` is the signal.

---

## 3. Slack Notification

New function `notify_picks_settled(game_date, results)` in `shared/plugins/slack_notifier.py`.

Sent once per game date when all top-10 recommendations for that date are settled (or the 7-day timeout is reached).

**Format:**
```
📊 Picks recap — Apr 3, 2026
Top-10: 6/10 hit (60%) | Avg edge: +0.082

✅ LeBron James — Points O 24.5 | Scored: 27 | Edge: +0.11
✅ Steph Curry — Threes O 3.5 | Scored: 5 | Edge: +0.09
❌ Jayson Tatum — Rebounds O 7.5 | Scored: 6 | Edge: +0.07
...
```

---

## 4. DAG Change

`nba_stats_pipeline` gains a final task `settle_recommendations` after `link_nba_game_ids`:

```python
t_settle = PythonOperator(
    task_id="settle_recommendations",
    python_callable=run_settle_recommendations,
)
t_link_games >> t_settle
```

No new DAG is created.

---

## 5. odds-admin Changes

### models.py
Add to `PropRecommendation`:
```python
actual_result:     Mapped[bool | None]    = mapped_column(Boolean)
actual_stat_value: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
settled_at:        Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
```

### schemas.py
Add to `PropRecommendationItem`:
```python
actual_result:     bool | None
actual_stat_value: Decimal | None
settled_at:        datetime | None
```

Add new schema `AccuracySummaryItem`:
```python
class AccuracySummaryItem(_Base):
    game_date:        date
    top_10_hits:      int
    top_10_total:     int
    top_10_hit_rate:  float | None
    avg_edge_hits:    float | None
    avg_edge_misses:  float | None
    settled:          int
    total:            int
```

### routers/recommendations.py
New endpoint added to existing router:
```
GET /recommendations/accuracy?date=YYYY-MM-DD
```
Returns `AccuracySummaryItem` for the given date (defaults to today). Computes hit rate and avg edge split across hits/misses for the top-10 ranked recommendations.

Existing endpoints (`/recommendations`, `/recommendations/top`) automatically return outcome fields once populated — no routing changes needed.

---

## 6. File Changelist

**odds-pipeline:**
- `sql/migrations/005_recommendation_outcomes.sql` — new
- `nba/plugins/ml/settle.py` — new
- `nba/dags/nba_stats_pipeline_dag.py` — add `settle_recommendations` task
- `shared/plugins/slack_notifier.py` — add `notify_picks_settled`

**odds-admin:**
- `api/models.py` — extend `PropRecommendation`
- `api/schemas.py` — extend `PropRecommendationItem`, add `AccuracySummaryItem`
- `api/routers/recommendations.py` — add `/recommendations/accuracy` endpoint
