# odds-admin MLB Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thread `sport` through odds-admin so the user can switch between NBA and MLB via a global top-nav selector and see full parity on Games, Players, and Recommendations pages.

**Architecture:** A backend `Sport` enum + FastAPI `?sport=` query param threads through every sport-aware endpoint (defaults to NBA so existing behavior is preserved). For player endpoints, the router inline-dispatches between NBA models (`Player`, `PlayerGameLog`) and new MLB models (`MlbPlayer`, `MlbPlayerGameLog`). Frontend gains a `SportContext` persisted in `localStorage`, a top-nav `<SportSelector/>`, and a per-page hook that re-fetches when the sport flips. No DB migrations — the schema is already multi-sport.

**Tech Stack:** Python 3.11, FastAPI 0.111, SQLAlchemy 2.0 async, pytest, React 18, TypeScript, Vite, Tailwind. All work happens in the **odds-admin** repo (sibling to odds-pipeline at `/home/bryang/Dev_Space/python_projects/odds-admin`).

**Spec:** `odds-pipeline/docs/superpowers/specs/2026-05-14-odds-admin-mlb-integration-design.md`

---

## Pre-flight

- [ ] **Step 1: Create feature branch in odds-admin**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git status
git checkout -b feature/odds-admin-mlb
```

Expected: clean working tree, branch switched.

- [ ] **Step 2: Verify baseline backend tests pass**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
python3 -m pytest -q
```

Expected: all green. Record the pass count — every new test added by this plan increases it; nothing existing should turn red.

- [ ] **Step 3: Confirm Docker stack is up (for end-of-phase manual verification)**

```bash
docker compose -f /home/bryang/Dev_Space/python_projects/odds-pipeline/docker-compose.yml ps
```

Expected: `odds-pipeline-odds-admin-1`, `odds-pipeline-data-postgres-1` both `Up`.

---

# Phase 1: Infrastructure + Recommendations

This phase lays the cross-cutting groundwork (`Sport` enum, SportContext, top-nav selector, shared prop label map) and ships MLB on the Recommendations page. After Phase 1 the user can flip NBA ↔ MLB in the nav and see MLB picks on `/recommendations`. Games and Players still show NBA only — that's Phase 2/3.

## Task 1: Backend `Sport` enum + FastAPI dependency

**Files:**
- Create: `api/sport.py`

- [ ] **Step 1: Create `api/sport.py`**

```python
# api/sport.py
"""Sport enum and FastAPI dependency for sport-aware endpoints."""
from enum import Enum

from fastapi import Query


class Sport(str, Enum):
    NBA = "NBA"
    MLB = "MLB"


# Map the short API code to the long form stored in games.sport
# (Odds-API style: "basketball_nba", "baseball_mlb").
GAMES_SPORT_TO_LONG: dict[Sport, str] = {
    Sport.NBA: "basketball_nba",
    Sport.MLB: "baseball_mlb",
}


def sport_dep(sport: Sport = Query(default=Sport.NBA)) -> Sport:
    """FastAPI dependency: read ?sport= from query, default NBA."""
    return sport
```

- [ ] **Step 2: Commit**

```bash
git add api/sport.py
git commit -m "feat(api): add Sport enum and sport_dep FastAPI dependency"
```

## Task 2: Add `sport` field to `PropRecommendationItem` schema

**Files:**
- Modify: `api/schemas.py` (PropRecommendationItem class)
- Test: `tests/test_schemas.py` (existing) or `tests/test_recommendations.py`

- [ ] **Step 1: Write a failing test in `tests/test_recommendations.py`**

Append at the bottom of the file:

```python
async def test_list_recommendations_response_includes_sport(client, db):
    from datetime import date
    today = date(2026, 3, 28)
    rec = _rec("LeBron James", "player_points", "DraftKings", 1, today, edge=0.13)
    rec.sport = "NBA"
    db.add(rec)
    await db.commit()

    response = await client.get("/api/recommendations?date=2026-03-28")
    data = response.json()
    assert data[0]["sport"] == "NBA"
```

You also need to extend the `_rec` helper in this file to accept and store `sport`. Update the helper near the top of the file (around line 9):

```python
def _rec(
    player_name: str,
    prop_type: str,
    bookmaker: str,
    rank: int,
    game_date: date,
    edge: float = 0.10,
    model_prob: float = 0.60,
    implied_prob: float = 0.50,
    line: float = 24.5,
    outcome: str = "Over",
    model_version: str = "v3",
    sport: str = "NBA",
) -> PropRecommendation:
    return PropRecommendation(
        player_name=player_name,
        prop_type=prop_type,
        bookmaker=bookmaker,
        line=Decimal(str(line)),
        outcome=outcome,
        model_prob=Decimal(str(model_prob)),
        implied_prob=Decimal(str(implied_prob)),
        edge=Decimal(str(edge)),
        rank=rank,
        model_version=model_version,
        game_date=game_date,
        sport=sport,
    )
```

- [ ] **Step 2: Run test, verify it fails**

```bash
python3 -m pytest tests/test_recommendations.py::test_list_recommendations_response_includes_sport -v
```

Expected: FAIL — `KeyError: 'sport'` or the field missing from the response shape.

- [ ] **Step 3: Add `sport` to the schema**

In `api/schemas.py`, edit `PropRecommendationItem` (around line 86-101) to add the field. Place it after `game_date`:

```python
class PropRecommendationItem(_Base):
    id: int
    player_name: str | None
    prop_type: str | None
    bookmaker: str | None
    line: Decimal | None
    outcome: str | None
    model_prob: Decimal | None
    implied_prob: Decimal | None
    edge: Decimal | None
    rank: int | None
    model_version: str | None
    game_date:         date | None
    sport:             str | None = None
    actual_result:     bool | None = None
    actual_stat_value: Decimal | None = None
    settled_at:        datetime | None = None
```

- [ ] **Step 4: Run test, verify it passes**

```bash
python3 -m pytest tests/test_recommendations.py::test_list_recommendations_response_includes_sport -v
```

Expected: PASS.

- [ ] **Step 5: Run the full recommendations test suite to make sure nothing else broke**

```bash
python3 -m pytest tests/test_recommendations.py -v
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add api/schemas.py tests/test_recommendations.py
git commit -m "feat(schema): expose sport on PropRecommendationItem"
```

## Task 3: Add `?sport=` filter to `GET /api/recommendations`

**Files:**
- Modify: `api/routers/recommendations.py` (list_recommendations function)
- Test: `tests/test_recommendations.py`

- [ ] **Step 1: Write two failing tests for sport filtering**

Append to `tests/test_recommendations.py`:

```python
async def test_list_recommendations_defaults_to_nba(client, db):
    from datetime import date
    today = date(2026, 3, 28)
    db.add(_rec("LeBron James", "player_points", "DraftKings", 1, today, sport="NBA"))
    db.add(_rec("Juan Soto", "batter_hits", "DraftKings", 1, today, sport="MLB"))
    await db.commit()

    # No ?sport= → defaults to NBA
    response = await client.get("/api/recommendations?date=2026-03-28")
    data = response.json()
    assert len(data) == 1
    assert data[0]["player_name"] == "LeBron James"


async def test_list_recommendations_filters_by_sport_mlb(client, db):
    from datetime import date
    today = date(2026, 3, 28)
    db.add(_rec("LeBron James", "player_points", "DraftKings", 1, today, sport="NBA"))
    db.add(_rec("Juan Soto", "batter_hits", "DraftKings", 1, today, sport="MLB"))
    await db.commit()

    response = await client.get("/api/recommendations?date=2026-03-28&sport=MLB")
    data = response.json()
    assert len(data) == 1
    assert data[0]["player_name"] == "Juan Soto"
    assert data[0]["sport"] == "MLB"
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
python3 -m pytest tests/test_recommendations.py::test_list_recommendations_defaults_to_nba tests/test_recommendations.py::test_list_recommendations_filters_by_sport_mlb -v
```

Expected: both FAIL — the default test fails because the endpoint returns both rows (no sport filter), the MLB-specific test fails for the same reason.

- [ ] **Step 3: Add sport filter to `list_recommendations`**

Replace `api/routers/recommendations.py` in full:

```python
import datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from api.database import get_db
from api.models import PropRecommendation
from api.schemas import PropRecommendationItem, AccuracySummaryItem
from api.sport import Sport, sport_dep

router = APIRouter()


@router.get("/recommendations", response_model=list[PropRecommendationItem])
async def list_recommendations(
    date: str = Query(default=None),
    sport: Sport = Depends(sport_dep),
    db: AsyncSession = Depends(get_db),
):
    game_date = (
        datetime.date.fromisoformat(date)
        if date
        else datetime.date.today()
    )
    result = await db.execute(
        select(PropRecommendation)
        .where(
            PropRecommendation.game_date == game_date,
            PropRecommendation.sport == sport.value,
        )
        .order_by(PropRecommendation.rank)
    )
    return result.scalars().all()


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


@router.get("/recommendations/top", response_model=list[PropRecommendationItem])
async def top_recommendations(
    date: str = Query(default=None),
    limit: int = Query(default=5, ge=1, le=20),
    db: AsyncSession = Depends(get_db),
):
    game_date = (
        datetime.date.fromisoformat(date)
        if date
        else datetime.date.today()
    )
    result = await db.execute(
        select(PropRecommendation)
        .where(PropRecommendation.game_date == game_date)
        .order_by(PropRecommendation.rank)
        .limit(limit)
    )
    return result.scalars().all()
```

We'll modify the `top` and `accuracy` endpoints in Tasks 4 and 5 — only `list_recommendations` changes here.

- [ ] **Step 4: Add the `sport` column to the `PropRecommendation` SQLAlchemy model**

The existing model is missing the `sport` column. In `api/models.py`, edit the `PropRecommendation` class (around line 111-128) to add `sport`:

```python
class PropRecommendation(Base):
    __tablename__ = "recommendations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    player_name: Mapped[str | None] = mapped_column(String(100))
    prop_type: Mapped[str | None] = mapped_column(String(100))
    bookmaker: Mapped[str | None] = mapped_column(String(50))
    line: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    outcome: Mapped[str | None] = mapped_column(String(20))
    model_prob: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    implied_prob: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    edge: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    rank: Mapped[int | None] = mapped_column(Integer)
    model_version: Mapped[str | None] = mapped_column(String(50))
    game_date:         Mapped[date | None]     = mapped_column(Date)
    sport:             Mapped[str | None]      = mapped_column(String(20))
    actual_result:     Mapped[bool | None]     = mapped_column(Boolean)
    actual_stat_value: Mapped[Decimal | None]  = mapped_column(Numeric(10, 2))
    settled_at:        Mapped[datetime | None]  = mapped_column(DateTime(timezone=True))
```

- [ ] **Step 5: Run new tests, verify they pass**

```bash
python3 -m pytest tests/test_recommendations.py::test_list_recommendations_defaults_to_nba tests/test_recommendations.py::test_list_recommendations_filters_by_sport_mlb -v
```

Expected: both PASS.

- [ ] **Step 6: Run full recommendations test suite to catch regressions**

```bash
python3 -m pytest tests/test_recommendations.py -v
```

Expected: all green. Existing tests pass because their fixtures default `sport="NBA"` (via the helper we updated in Task 2).

- [ ] **Step 7: Commit**

```bash
git add api/models.py api/routers/recommendations.py tests/test_recommendations.py
git commit -m "feat(api): filter /api/recommendations by ?sport= (default NBA)"
```

## Task 4: Add `?sport=` filter to `GET /api/recommendations/top`

**Files:**
- Modify: `api/routers/recommendations.py` (top_recommendations function)
- Test: `tests/test_recommendations.py`

- [ ] **Step 1: Write a failing MLB test**

Append to `tests/test_recommendations.py`:

```python
async def test_top_recommendations_filters_by_sport(client, db):
    from datetime import date
    today = date(2026, 3, 28)
    db.add(_rec("NBA1", "player_points", "DK", 1, today, edge=0.15, sport="NBA"))
    db.add(_rec("NBA2", "player_points", "DK", 2, today, edge=0.12, sport="NBA"))
    db.add(_rec("MLB1", "batter_hits",  "DK", 1, today, edge=0.10, sport="MLB"))
    db.add(_rec("MLB2", "batter_hits",  "DK", 2, today, edge=0.08, sport="MLB"))
    await db.commit()

    response = await client.get("/api/recommendations/top?date=2026-03-28&sport=MLB&limit=5")
    data = response.json()
    assert {row["player_name"] for row in data} == {"MLB1", "MLB2"}
```

- [ ] **Step 2: Run test, verify it fails**

```bash
python3 -m pytest tests/test_recommendations.py::test_top_recommendations_filters_by_sport -v
```

Expected: FAIL — response contains all four rows.

- [ ] **Step 3: Add sport filter to `top_recommendations`**

In `api/routers/recommendations.py`, replace the `top_recommendations` function:

```python
@router.get("/recommendations/top", response_model=list[PropRecommendationItem])
async def top_recommendations(
    date: str = Query(default=None),
    limit: int = Query(default=5, ge=1, le=20),
    sport: Sport = Depends(sport_dep),
    db: AsyncSession = Depends(get_db),
):
    game_date = (
        datetime.date.fromisoformat(date)
        if date
        else datetime.date.today()
    )
    result = await db.execute(
        select(PropRecommendation)
        .where(
            PropRecommendation.game_date == game_date,
            PropRecommendation.sport == sport.value,
        )
        .order_by(PropRecommendation.rank)
        .limit(limit)
    )
    return result.scalars().all()
```

- [ ] **Step 4: Run test, verify it passes**

```bash
python3 -m pytest tests/test_recommendations.py::test_top_recommendations_filters_by_sport -v
```

Expected: PASS.

- [ ] **Step 5: Run full file**

```bash
python3 -m pytest tests/test_recommendations.py -v
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add api/routers/recommendations.py tests/test_recommendations.py
git commit -m "feat(api): filter /api/recommendations/top by ?sport= (default NBA)"
```

## Task 5: Add `?sport=` filter to `GET /api/recommendations/accuracy`

**Files:**
- Modify: `api/routers/recommendations.py` (recommendations_accuracy function)
- Test: `tests/test_recommendations.py`

- [ ] **Step 1: Write a failing MLB test**

Append to `tests/test_recommendations.py`:

```python
async def test_accuracy_filters_by_sport(client, db):
    from datetime import date
    today = date(2026, 3, 28)
    # 2 NBA top-10 settled, both hits
    db.add(_rec("NBA1", "player_points", "DK", 1, today, sport="NBA"))
    db.add(_rec("NBA2", "player_points", "DK", 2, today, sport="NBA"))
    # 2 MLB top-10 settled, one hit one miss
    db.add(_rec("MLB1", "batter_hits",   "DK", 1, today, sport="MLB"))
    db.add(_rec("MLB2", "batter_hits",   "DK", 2, today, sport="MLB"))
    await db.flush()
    # Mark each row's actual_result manually
    from api.models import PropRecommendation
    from sqlalchemy import update
    await db.execute(update(PropRecommendation).where(PropRecommendation.player_name == "NBA1").values(actual_result=True))
    await db.execute(update(PropRecommendation).where(PropRecommendation.player_name == "NBA2").values(actual_result=True))
    await db.execute(update(PropRecommendation).where(PropRecommendation.player_name == "MLB1").values(actual_result=True))
    await db.execute(update(PropRecommendation).where(PropRecommendation.player_name == "MLB2").values(actual_result=False))
    await db.commit()

    response = await client.get("/api/recommendations/accuracy?date=2026-03-28&sport=MLB")
    data = response.json()
    assert data["top_10_total"] == 2
    assert data["top_10_hits"] == 1  # only the MLB hit, not NBA's
    assert data["total"] == 2        # only MLB rows count
```

- [ ] **Step 2: Run test, verify it fails**

```bash
python3 -m pytest tests/test_recommendations.py::test_accuracy_filters_by_sport -v
```

Expected: FAIL — accuracy returns sport-agnostic totals.

- [ ] **Step 3: Add sport filter to `recommendations_accuracy`**

In `api/routers/recommendations.py`, replace the `recommendations_accuracy` function:

```python
@router.get("/recommendations/accuracy", response_model=AccuracySummaryItem)
async def recommendations_accuracy(
    date: str = Query(default=None),
    sport: Sport = Depends(sport_dep),
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
            PropRecommendation.sport == sport.value,
            PropRecommendation.rank <= 10,
        )
    )
    top10_result = await db.execute(top10_q)
    top10_recs = top10_result.scalars().all()

    all_q = (
        select(func.count())
        .where(
            PropRecommendation.game_date == game_date,
            PropRecommendation.sport == sport.value,
        )
    )
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
            PropRecommendation.sport == sport.value,
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

- [ ] **Step 4: Run test, verify it passes**

```bash
python3 -m pytest tests/test_recommendations.py::test_accuracy_filters_by_sport -v
```

Expected: PASS.

- [ ] **Step 5: Run full backend suite for regressions**

```bash
python3 -m pytest -q
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add api/routers/recommendations.py tests/test_recommendations.py
git commit -m "feat(api): filter /api/recommendations/accuracy by ?sport= (default NBA)"
```

## Task 6: Frontend `SportContext` provider

**Files:**
- Create: `web/src/lib/sport.tsx`

- [ ] **Step 1: Create `web/src/lib/sport.tsx`**

```tsx
import { createContext, useContext, useState, type ReactNode } from "react";

export type Sport = "NBA" | "MLB";

const STORAGE_KEY = "odds-admin.sport";

interface SportContextValue {
  sport: Sport;
  setSport: (s: Sport) => void;
}

const SportContext = createContext<SportContextValue | null>(null);

function readStoredSport(): Sport {
  if (typeof window === "undefined") return "NBA";
  const stored = window.localStorage.getItem(STORAGE_KEY);
  return stored === "MLB" || stored === "NBA" ? stored : "NBA";
}

export function SportProvider({ children }: { children: ReactNode }) {
  const [sport, setSportState] = useState<Sport>(readStoredSport);

  const setSport = (s: Sport) => {
    setSportState(s);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(STORAGE_KEY, s);
    }
  };

  return (
    <SportContext.Provider value={{ sport, setSport }}>
      {children}
    </SportContext.Provider>
  );
}

export function useSport(): SportContextValue {
  const ctx = useContext(SportContext);
  if (!ctx) {
    throw new Error("useSport must be used inside <SportProvider/>");
  }
  return ctx;
}
```

- [ ] **Step 2: Type-check the new file**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/lib/sport.tsx
git commit -m "feat(web): add SportContext provider and useSport hook"
```

## Task 7: Frontend `SportSelector` component

**Files:**
- Create: `web/src/components/SportSelector.tsx`

- [ ] **Step 1: Create the component**

```tsx
// web/src/components/SportSelector.tsx
import { useSport, type Sport } from "@/lib/sport";

const SPORT_LABEL: Record<Sport, string> = {
  NBA: "🏀 NBA",
  MLB: "⚾ MLB",
};

export function SportSelector() {
  const { sport, setSport } = useSport();

  return (
    <select
      value={sport}
      onChange={(e) => setSport(e.target.value as Sport)}
      className="bg-slate-800 border border-slate-700 rounded px-2 py-1 text-sm text-slate-100 focus:outline-none focus:border-blue-400"
      aria-label="Sport"
    >
      <option value="NBA">{SPORT_LABEL.NBA}</option>
      <option value="MLB">{SPORT_LABEL.MLB}</option>
    </select>
  );
}
```

- [ ] **Step 2: Type-check**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/components/SportSelector.tsx
git commit -m "feat(web): add SportSelector dropdown"
```

## Task 8: Wire `SportProvider` + `SportSelector` into `App.tsx`

**Files:**
- Modify: `web/src/App.tsx`

- [ ] **Step 1: Edit `web/src/App.tsx`**

Replace the entire file:

```tsx
import { BrowserRouter, Routes, Route, Link, useLocation } from "react-router-dom";
import { GamesPage } from "./pages/GamesPage";
import { PlayersPage } from "./pages/PlayersPage";
import { PlayerDetailPage } from "./pages/PlayerDetailPage";
import { RecommendationsPage } from "./pages/RecommendationsPage";
import { SportProvider } from "./lib/sport";
import { SportSelector } from "./components/SportSelector";
import { cn } from "@/lib/utils";

function NavLink({ to, children }: { to: string; children: React.ReactNode }) {
  const { pathname } = useLocation();
  const active = pathname === to || (to !== "/" && pathname.startsWith(to));
  return (
    <Link
      to={to}
      className={cn(
        "text-sm transition-colors pb-0.5",
        active
          ? "text-slate-100 border-b-2 border-blue-400"
          : "text-slate-500 hover:text-slate-300"
      )}
    >
      {children}
    </Link>
  );
}

function Layout() {
  return (
    <div className="min-h-screen bg-slate-950">
      <nav className="bg-slate-900 border-b border-slate-800 px-4 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="text-slate-100 font-bold text-base tracking-wide">
            Odds Admin
          </span>
          <SportSelector />
        </div>
        <div className="flex gap-4 text-sm">
          <NavLink to="/">Games</NavLink>
          <NavLink to="/players">Players</NavLink>
          <NavLink to="/recommendations">Recommendations</NavLink>
        </div>
      </nav>

      <Routes>
        <Route path="/" element={<GamesPage />} />
        <Route path="/players" element={<PlayersPage />} />
        <Route path="/players/:id" element={<PlayerDetailPage />} />
        <Route path="/recommendations" element={<RecommendationsPage />} />
      </Routes>
    </div>
  );
}

export default function App() {
  return (
    <SportProvider>
      <BrowserRouter>
        <Layout />
      </BrowserRouter>
    </SportProvider>
  );
}
```

- [ ] **Step 2: Type-check**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/App.tsx
git commit -m "feat(web): mount SportProvider and SportSelector in nav"
```

## Task 9: Create shared `propTypes.ts` label map

**Files:**
- Create: `web/src/lib/propTypes.ts`

- [ ] **Step 1: Create the label map**

```ts
// web/src/lib/propTypes.ts
/**
 * Display-friendly label for a prop_type code returned by the backend.
 * Falls back to a tidied-up version of the raw code (e.g., "player_points"
 * → "Points") when the code is unknown.
 */
export const PROP_TYPE_LABEL: Record<string, string> = {
  // NBA
  player_points: "Points",
  player_rebounds: "Rebounds",
  player_assists: "Assists",
  // MLB
  batter_hits: "Hits",
  batter_total_bases: "Total Bases",
};

export function propTypeLabel(code: string | null | undefined): string {
  if (!code) return "—";
  if (code in PROP_TYPE_LABEL) return PROP_TYPE_LABEL[code];
  // Fallback: strip common prefixes, title-case
  return code
    .replace(/^player_/, "")
    .replace(/^batter_/, "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}
```

- [ ] **Step 2: Type-check**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/lib/propTypes.ts
git commit -m "feat(web): add shared propTypeLabel helper covering NBA and MLB markets"
```

## Task 10: Thread `sport` into recommendation fetch fns + TS type

**Files:**
- Modify: `web/src/api/client.ts`

- [ ] **Step 1: Edit `web/src/api/client.ts`**

Find the `PropRecommendationItem` interface (around line 73-87) and add `sport`:

```ts
export interface PropRecommendationItem {
  id: number;
  player_name: string | null;
  prop_type: string | null;
  bookmaker: string | null;
  line: number | null;
  outcome: string | null;
  model_prob: number | null;
  implied_prob: number | null;
  edge: number | null;
  rank: number | null;
  model_version: string | null;
  game_date: string | null;
  sport: string | null;
}
```

Find `fetchRecommendations` and `fetchTopRecommendations` (toward the bottom of the file) and modify them to accept a `sport` parameter:

```ts
import type { Sport } from "@/lib/sport";

// ... earlier interfaces unchanged ...

export async function fetchRecommendations(
  date: string,
  sport: Sport,
): Promise<PropRecommendationItem[]> {
  const res = await fetch(`${BASE}/recommendations?date=${date}&sport=${sport}`);
  if (!res.ok) throw new Error("Failed to fetch recommendations");
  return res.json();
}

export async function fetchTopRecommendations(
  date: string,
  sport: Sport,
  limit = 5,
): Promise<PropRecommendationItem[]> {
  const res = await fetch(
    `${BASE}/recommendations/top?date=${date}&sport=${sport}&limit=${limit}`
  );
  if (!res.ok) throw new Error("Failed to fetch top recommendations");
  return res.json();
}
```

The `import type { Sport }` line goes at the very top of the file alongside any other imports.

- [ ] **Step 2: Type-check (expect failures from RecommendationsPage call sites)**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: TypeScript errors in `RecommendationsPage.tsx` (callers haven't been updated yet). That's fine — we fix them in the next task.

- [ ] **Step 3: Commit (compilation errors get fixed in next task)**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/api/client.ts
git commit -m "feat(web): thread sport through fetchRecommendations and fetchTopRecommendations"
```

## Task 11: Wire sport into `RecommendationsPage`

**Files:**
- Modify: `web/src/pages/RecommendationsPage.tsx`

- [ ] **Step 1: Update `RecommendationsPage` to consume `useSport` and refetch on sport change**

Open `web/src/pages/RecommendationsPage.tsx` and:

1. Add the import near the top alongside the others:

```tsx
import { useSport } from "@/lib/sport";
import { propTypeLabel } from "@/lib/propTypes";
```

2. Inside the `RecommendationsPage` function (around line 233), after `const today = ...;`, read the sport from context:

```tsx
export function RecommendationsPage() {
  const today = toDateString(new Date());
  const { sport } = useSport();
  const [date, setDate] = useState(today);
  // ... rest unchanged ...
```

3. Update the fetch effect to include `sport` in its calls and dependency array (around line 241-257):

```tsx
  useEffect(() => {
    let isCurrent = true;
    setLoading(true);
    setError(null);
    Promise.all([
      fetchTopRecommendations(date, sport),
      fetchRecommendations(date, sport),
    ])
      .then(([top, all]) => {
        if (!isCurrent) return;
        setTopPicks(top);
        setAllRecs(all);
      })
      .catch((e) => { if (isCurrent) setError(e.message); })
      .finally(() => { if (isCurrent) setLoading(false); });
    return () => { isCurrent = false; };
  }, [date, sport]);
```

4. Replace the `prop_type` displays to use `propTypeLabel`. Two spots:

In `TopPicksModule`, around line 60-62:

```tsx
                <p className="text-xs text-violet-400">
                  {propTypeLabel(pick.prop_type)}
                </p>
```

In `RecommendationsTable`'s row rendering (around line 213) and the filter dropdown (around line 167-171), do the same substitution. For the filter dropdown:

```tsx
          {propTypes.map((t) => (
            <option key={t} value={t}>
              {t === "all" ? "All Prop Types" : propTypeLabel(t)}
            </option>
          ))}
```

For the row cell:

```tsx
                  <td className="px-2 py-1.5 text-violet-400">{propTypeLabel(row.prop_type)}</td>
```

- [ ] **Step 2: Type-check**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 3: Manual browser verification**

The Vite dev server proxies `/api` to `http://localhost:8002`, so a local uvicorn must be running on port 8002. In one terminal:

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
python3 -m uvicorn api.main:app --reload --port 8002
```

In another:

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npm run dev
```

Open http://localhost:5173/recommendations.

Verify:
1. Sport selector is visible in the top-left nav and starts on the value last used (or NBA).
2. Switching to NBA shows today's NBA recommendations (rank-ordered).
3. Switching to MLB shows today's MLB recommendations (rank-ordered). Counts should match the DB:
   ```sql
   SELECT sport, count(*) FROM recommendations
   WHERE game_date = CURRENT_DATE GROUP BY sport;
   ```
4. Refreshing the page preserves the last selected sport.
5. Prop-type labels for MLB show as "Hits" / "Total Bases" (not raw `batter_hits`).

- [ ] **Step 4: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/pages/RecommendationsPage.tsx
git commit -m "feat(web): RecommendationsPage honors sport context and uses shared prop labels"
```

## Phase 1 close-out

- [ ] **Step 1: Run the full backend test suite**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
python3 -m pytest -q
```

Expected: all green. Pass count should equal the baseline + the new tests added in Tasks 2–5 (5 new tests total).

- [ ] **Step 2: Type-check the frontend**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

End of Phase 1. The app could be merged here and would ship an MLB-aware Recommendations page. Games and Players still default to NBA.

---

# Phase 2: Games

Goal: switching the top-nav selector also filters the Games page. MLB games and their odds/props/scores are reachable through the existing per-game routes.

## Task 12: Add `?sport=` filter to `GET /api/games`

**Files:**
- Modify: `api/routers/games.py`
- Test: `tests/test_games.py`

- [ ] **Step 1: Write the failing MLB test**

Append to `tests/test_games.py`:

```python
async def test_list_games_filters_by_sport_mlb(client, db):
    db.add(Game(
        game_id="nba_g1",
        home_team="Lakers",
        away_team="Celtics",
        commence_time=datetime(2026, 3, 19, 19, 0),
        season="2025-26",
        sport="basketball_nba",
    ))
    db.add(Game(
        game_id="mlb_g1",
        home_team="Yankees",
        away_team="Red Sox",
        commence_time=datetime(2026, 5, 14, 19, 0),
        season="2026",
        sport="baseball_mlb",
    ))
    await db.commit()

    response = await client.get("/api/games?sport=MLB")
    data = response.json()
    assert len(data) == 1
    assert data[0]["game_id"] == "mlb_g1"


async def test_list_games_defaults_to_nba(client, db):
    db.add(Game(
        game_id="nba_g1",
        home_team="Lakers",
        away_team="Celtics",
        commence_time=datetime(2026, 3, 19, 19, 0),
        season="2025-26",
        sport="basketball_nba",
    ))
    db.add(Game(
        game_id="mlb_g1",
        home_team="Yankees",
        away_team="Red Sox",
        commence_time=datetime(2026, 5, 14, 19, 0),
        season="2026",
        sport="baseball_mlb",
    ))
    await db.commit()

    response = await client.get("/api/games")  # no sport param
    data = response.json()
    assert len(data) == 1
    assert data[0]["game_id"] == "nba_g1"
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
python3 -m pytest tests/test_games.py::test_list_games_filters_by_sport_mlb tests/test_games.py::test_list_games_defaults_to_nba -v
```

Expected: both FAIL — current endpoint returns both games regardless.

- [ ] **Step 3: Add sport filter to `list_games`**

Replace `api/routers/games.py` in full:

```python
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from api.database import get_db
from api.models import Game
from api.schemas import GameListItem, ScoreEmbed
from api.sport import Sport, sport_dep, GAMES_SPORT_TO_LONG

router = APIRouter()


@router.get("/games", response_model=list[GameListItem])
async def list_games(
    sport: Sport = Depends(sport_dep),
    db: AsyncSession = Depends(get_db),
):
    long_sport = GAMES_SPORT_TO_LONG[sport]
    result = await db.execute(
        select(Game)
        .where(Game.sport == long_sport)
        .options(selectinload(Game.score))
        .order_by(Game.commence_time.desc())
    )
    games = result.scalars().all()

    items = []
    for g in games:
        score_embed = None
        if g.score is not None:
            score_embed = ScoreEmbed(
                home_score=g.score.home_score,
                away_score=g.score.away_score,
                completed=g.score.completed or False,
            )
        items.append(
            GameListItem(
                game_id=g.game_id,
                home_team=g.home_team,
                away_team=g.away_team,
                commence_time=g.commence_time,
                season=g.season,
                sport=g.sport,
                score=score_embed,
            )
        )
    return items
```

- [ ] **Step 4: Run new tests, verify they pass**

```bash
python3 -m pytest tests/test_games.py -v
```

Expected: all green, including the existing NBA tests (which all use `sport="basketball_nba"`).

- [ ] **Step 5: Commit**

```bash
git add api/routers/games.py tests/test_games.py
git commit -m "feat(api): filter /api/games by ?sport= (default NBA)"
```

## Task 13: Thread `sport` into `fetchGames`

**Files:**
- Modify: `web/src/api/client.ts`

- [ ] **Step 1: Edit `fetchGames`**

In `web/src/api/client.ts`, find `fetchGames` (around line 90-94) and update:

```ts
export async function fetchGames(sport: Sport): Promise<Game[]> {
  const res = await fetch(`${BASE}/games?sport=${sport}`);
  if (!res.ok) throw new Error("Failed to fetch games");
  return res.json();
}
```

`Sport` is already imported in Task 10.

- [ ] **Step 2: Type-check (expect failures in GamesPage)**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: an error at GamesPage about the missing argument. Fixed in the next task.

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/api/client.ts
git commit -m "feat(web): thread sport through fetchGames"
```

## Task 14: Wire sport into `GamesPage`

**Files:**
- Modify: `web/src/pages/GamesPage.tsx`

- [ ] **Step 1: Update `GamesPage`**

Open `web/src/pages/GamesPage.tsx`:

1. Add import near the top:

```tsx
import { useSport } from "@/lib/sport";
```

2. Update the component to read sport and refetch on change. Replace the body (everything from `export function GamesPage()` onward) with:

```tsx
export function GamesPage() {
  const { sport } = useSport();
  const [games, setGames] = useState<Game[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let isCurrent = true;
    setLoading(true);
    setError(null);
    fetchGames(sport)
      .then((data) => { if (isCurrent) setGames(data); })
      .catch((e) => { if (isCurrent) setError(e.message); })
      .finally(() => { if (isCurrent) setLoading(false); });
    return () => { isCurrent = false; };
  }, [sport]);

  if (loading) {
    return <p className="px-6 py-8 text-slate-500">Loading games...</p>;
  }
  if (error) {
    return (
      <p className="px-6 py-8 text-red-400">
        Failed to load games: {error}
      </p>
    );
  }
  if (games.length === 0) {
    return (
      <p className="px-6 py-8 text-slate-500">No games found in the database.</p>
    );
  }

  const grouped = groupGamesByDate(games);

  return (
    <div className="max-w-4xl mx-auto px-4 py-6">
      {grouped.map(([dateLabel, dateGames]) => (
        <div key={dateLabel} className="mb-8">
          <p className="text-[11px] font-bold uppercase tracking-widest text-slate-500 pb-2 mb-3 border-b border-slate-800">
            {getRelativeLabel(dateLabel)}
          </p>
          {dateGames.map((game) => (
            <GameCard key={game.game_id} game={game} />
          ))}
        </div>
      ))}
    </div>
  );
}
```

- [ ] **Step 2: Type-check**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/pages/GamesPage.tsx
git commit -m "feat(web): GamesPage filters by sport context"
```

## Task 15: Update `PlayerPropsTab` to support MLB prop filters

**Files:**
- Modify: `web/src/components/PlayerPropsTab.tsx`

- [ ] **Step 1: Generalize the filter and use shared labels**

Open `web/src/components/PlayerPropsTab.tsx`. The current filter is hardcoded to NBA prop types. Replace the whole file with a sport-aware version:

```tsx
import { useMemo, useState } from "react";
import { cn } from "@/lib/utils";
import { propTypeLabel } from "@/lib/propTypes";
import { useSport } from "@/lib/sport";
import type { PlayerPropItem } from "@/api/client";

const NBA_PROP_TYPES = ["player_points", "player_rebounds", "player_assists"];
const MLB_PROP_TYPES = ["batter_hits", "batter_total_bases"];

function groupByPlayer(props: PlayerPropItem[]): Map<string, PlayerPropItem[]> {
  const map = new Map<string, PlayerPropItem[]>();
  for (const p of props) {
    const name = p.player_name ?? "Unknown";
    if (!map.has(name)) map.set(name, []);
    map.get(name)!.push(p);
  }
  return map;
}

interface PlayerPropsTabProps {
  props: PlayerPropItem[];
}

export function PlayerPropsTab({ props }: PlayerPropsTabProps) {
  const { sport } = useSport();
  const propTypeOptions = useMemo(
    () => (sport === "MLB" ? MLB_PROP_TYPES : NBA_PROP_TYPES),
    [sport],
  );

  const [filter, setFilter] = useState<string>("all");

  // Reset filter when sport changes so we don't keep an invalid prop_type selected
  useMemo(() => setFilter("all"), [sport]);

  const filtered =
    filter === "all" ? props : props.filter((p) => p.prop_type === filter);

  const grouped = groupByPlayer(filtered);

  const buttons: { value: string; label: string }[] = [
    { value: "all", label: "All" },
    ...propTypeOptions.map((pt) => ({ value: pt, label: propTypeLabel(pt) })),
  ];

  return (
    <div className="py-2">
      <div className="flex gap-2 px-4 pb-3">
        {buttons.map((b) => (
          <button
            key={b.value}
            onClick={() => setFilter(b.value)}
            className={cn(
              "text-[10px] font-bold tracking-wide px-2 py-1 rounded cursor-pointer",
              filter === b.value
                ? "bg-blue-950 text-blue-400"
                : "bg-slate-800 text-slate-500 hover:text-slate-400"
            )}
          >
            {b.label}
          </button>
        ))}
      </div>

      {grouped.size === 0 ? (
        <p className="px-4 py-3 text-sm text-slate-500">No player props available.</p>
      ) : (
        <div className="flex flex-col gap-3 px-4 pb-3">
          {Array.from(grouped.entries()).map(([player, playerProps]) => (
            <div key={player}>
              <p className="text-[13px] font-bold text-slate-200 mb-2 pb-1 border-b border-slate-800">
                {player}
              </p>
              <table className="w-full text-xs">
                <thead>
                  <tr>
                    {["Bookmaker", "Prop", "Line", "Outcome", "Price"].map((h) => (
                      <th
                        key={h}
                        className="text-left px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500"
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {playerProps.map((p, i) => (
                    <tr key={i} className="border-t border-slate-900">
                      <td className="px-2 py-1.5 text-slate-300 font-medium">{p.bookmaker}</td>
                      <td className="px-2 py-1.5 text-violet-400 font-semibold">
                        {propTypeLabel(p.prop_type)}
                      </td>
                      <td className="px-2 py-1.5 text-blue-400 font-semibold">
                        {p.point ?? "—"}
                      </td>
                      <td className="px-2 py-1.5 text-slate-400">{p.outcome}</td>
                      <td
                        className={cn(
                          "px-2 py-1.5 font-semibold",
                          p.price !== null && p.price > 0
                            ? "text-green-400"
                            : "text-red-400"
                        )}
                      >
                        {p.price !== null
                          ? p.price > 0
                            ? `+${p.price}`
                            : String(p.price)
                          : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Type-check**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/components/PlayerPropsTab.tsx
git commit -m "feat(web): PlayerPropsTab supports MLB prop filters via sport context"
```

## Phase 2 close-out

- [ ] **Step 1: Manual browser verification**

With the local uvicorn on port 8002 and `npm run dev` both running (see Task 11 Step 3 for the commands):

1. Open http://localhost:5173/ — flip sport to MLB. Expect MLB games to show; NBA games hidden.
2. Click into an MLB game card. The Player Props tab should show MLB markets (Hits / Total Bases filters work).
3. Flip back to NBA. Games list refreshes to NBA games.

- [ ] **Step 2: Run backend tests**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
python3 -m pytest -q
```

Expected: all green.

End of Phase 2. The app now ships MLB on Games + Recommendations. Players page is still NBA-only — Phase 3.

---

# Phase 3: Players

Goal: Players page and Player detail page honor the sport selector. MLB players come from `mlb_players`; their game logs from `mlb_player_game_logs`; their props join through `player_props.mlb_player_id`.

## Task 16: Add `MlbPlayer` and `MlbPlayerGameLog` models + extend `PlayerProp`

**Files:**
- Modify: `api/models.py`
- Test: `tests/test_models.py`

- [ ] **Step 1: Write failing model tests**

Append to `tests/test_models.py`:

```python
def test_mlb_player_model():
    from api.models import MlbPlayer
    assert MlbPlayer.__tablename__ == "mlb_players"


def test_mlb_player_game_log_model():
    from api.models import MlbPlayerGameLog
    assert MlbPlayerGameLog.__tablename__ == "mlb_player_game_logs"


def test_player_prop_has_mlb_player_id():
    from api.models import PlayerProp
    assert hasattr(PlayerProp, "mlb_player_id")
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
python3 -m pytest tests/test_models.py -v
```

Expected: 3 new tests FAIL with `ImportError` or `AttributeError`.

- [ ] **Step 3: Add the models**

In `api/models.py`:

1. At the top, the existing imports cover what we need. No changes there.

2. Add `mlb_player_id` to `PlayerProp` (after the existing `nba_player_id` line, around line 67-68):

```python
    mlb_player_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("mlb_players.player_id")
    )
```

3. After the existing `PlayerGameLog` class, append:

```python
class MlbPlayer(Base):
    __tablename__ = "mlb_players"

    player_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    full_name: Mapped[str] = mapped_column(String)
    position: Mapped[str | None] = mapped_column(String)
    bats: Mapped[str | None] = mapped_column(String)
    throws: Mapped[str | None] = mapped_column(String)
    team_id: Mapped[int | None] = mapped_column(Integer)
    team_abbreviation: Mapped[str | None] = mapped_column(String)
    is_active: Mapped[bool | None] = mapped_column(Boolean)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MlbPlayerGameLog(Base):
    __tablename__ = "mlb_player_game_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    player_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("mlb_players.player_id")
    )
    mlb_game_pk: Mapped[str] = mapped_column(String)
    season: Mapped[str] = mapped_column(String)
    game_date: Mapped[date] = mapped_column(Date)
    matchup: Mapped[str | None] = mapped_column(String)
    team_id: Mapped[int | None] = mapped_column(Integer)
    opponent_team_id: Mapped[int | None] = mapped_column(Integer)
    plate_appearances: Mapped[int | None] = mapped_column(Integer)
    at_bats: Mapped[int | None] = mapped_column(Integer)
    hits: Mapped[int | None] = mapped_column(Integer)
    doubles: Mapped[int | None] = mapped_column(Integer)
    triples: Mapped[int | None] = mapped_column(Integer)
    home_runs: Mapped[int | None] = mapped_column(Integer)
    rbi: Mapped[int | None] = mapped_column(Integer)
    runs: Mapped[int | None] = mapped_column(Integer)
    walks: Mapped[int | None] = mapped_column(Integer)
    strikeouts: Mapped[int | None] = mapped_column(Integer)
    stolen_bases: Mapped[int | None] = mapped_column(Integer)
    total_bases: Mapped[int | None] = mapped_column(Integer)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
python3 -m pytest tests/test_models.py -v
```

Expected: all green.

- [ ] **Step 5: Run the full suite (the new models must be importable without breaking SQLite test fixtures)**

```bash
python3 -m pytest -q
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add api/models.py tests/test_models.py
git commit -m "feat(api): add MlbPlayer, MlbPlayerGameLog models; map PlayerProp.mlb_player_id"
```

## Task 17: Add MLB player schemas

**Files:**
- Modify: `api/schemas.py`

- [ ] **Step 1: Append MLB schemas to `api/schemas.py`**

At the bottom of the file:

```python
class MlbPlayerListItem(_Base):
    player_id: int
    full_name: str
    position: str | None
    bats: str | None
    throws: str | None
    team_id: int | None
    team_abbreviation: str | None
    is_active: bool | None


class MlbPlayerGameLogItem(_Base):
    game_date: date
    matchup: str | None
    season: str
    plate_appearances: int | None
    at_bats: int | None
    hits: int | None
    doubles: int | None
    triples: int | None
    home_runs: int | None
    rbi: int | None
    runs: int | None
    walks: int | None
    strikeouts: int | None
    stolen_bases: int | None
    total_bases: int | None
```

- [ ] **Step 2: Smoke-check by running the suite**

```bash
python3 -m pytest -q
```

Expected: all green.

- [ ] **Step 3: Commit**

```bash
git add api/schemas.py
git commit -m "feat(schema): add MlbPlayerListItem and MlbPlayerGameLogItem"
```

## Task 18: Sport-dispatch on `GET /api/players`

**Files:**
- Modify: `api/routers/players.py`
- Test: `tests/test_players.py`

- [ ] **Step 1: Add an MLB helper to the test file**

Open `tests/test_players.py`. After the existing `_player` helper (around line 7-14), add:

```python
from api.models import MlbPlayer, MlbPlayerGameLog

def _mlb_player(player_id: int, full_name: str, is_active: bool, **kwargs) -> MlbPlayer:
    return MlbPlayer(
        player_id=player_id,
        full_name=full_name,
        is_active=is_active,
        fetched_at=datetime(2026, 5, 14, 6, 0),
        **kwargs,
    )
```

- [ ] **Step 2: Write a failing MLB list test**

Append to `tests/test_players.py`:

```python
# ── GET /api/players?sport=MLB ────────────────────────────────────────────────

async def test_list_players_returns_mlb_players_when_sport_mlb(client, db):
    db.add(_player(1, "LeBron James", is_active=True, team_abbreviation="LAL"))
    db.add(_mlb_player(660271, "Juan Soto", is_active=True, team_abbreviation="NYM"))
    await db.commit()

    response = await client.get("/api/players?sport=MLB")
    data = response.json()
    assert len(data) == 1
    assert data[0]["full_name"] == "Juan Soto"
    assert data[0]["player_id"] == 660271


async def test_list_players_defaults_to_nba(client, db):
    db.add(_player(1, "LeBron James", is_active=True))
    db.add(_mlb_player(660271, "Juan Soto", is_active=True))
    await db.commit()

    response = await client.get("/api/players")
    data = response.json()
    assert len(data) == 1
    assert data[0]["full_name"] == "LeBron James"
```

- [ ] **Step 3: Run, verify failing**

```bash
python3 -m pytest tests/test_players.py::test_list_players_returns_mlb_players_when_sport_mlb tests/test_players.py::test_list_players_defaults_to_nba -v
```

Expected: both FAIL — `/api/players` ignores sport.

- [ ] **Step 4: Add sport dispatch to `list_players`**

Replace the imports at the top of `api/routers/players.py` and the `list_players` function:

```python
from typing import Union

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, exists, and_, func

from api.database import get_db
from api.models import Player, PlayerGameLog, PlayerProp, MlbPlayer, MlbPlayerGameLog
from api.schemas import (
    PlayerListItem,
    PlayerGameLogItem,
    PlayerPropItem,
    MlbPlayerListItem,
    MlbPlayerGameLogItem,
)
from api.sport import Sport, sport_dep

router = APIRouter()


@router.get(
    "/players",
    response_model=Union[list[PlayerListItem], list[MlbPlayerListItem]],
)
async def list_players(
    sport: Sport = Depends(sport_dep),
    db: AsyncSession = Depends(get_db),
):
    if sport == Sport.NBA:
        current_season_log = select(PlayerGameLog.id).where(
            and_(
                PlayerGameLog.player_id == Player.player_id,
                PlayerGameLog.season == "2025-26",
            )
        )
        result = await db.execute(
            select(Player)
            .where(or_(Player.is_active == True, exists(current_season_log)))
            .order_by(Player.full_name)
        )
        return result.scalars().all()
    else:
        result = await db.execute(
            select(MlbPlayer)
            .where(MlbPlayer.is_active == True)
            .order_by(MlbPlayer.full_name)
        )
        return result.scalars().all()
```

Leave the other endpoints (`get_player`, `get_player_props`, `get_player_game_logs`) unchanged for now — Tasks 19–21 cover those.

- [ ] **Step 5: Run tests, verify they pass**

```bash
python3 -m pytest tests/test_players.py -v
```

Expected: all green (existing NBA tests still pass with the `sport=NBA` default).

- [ ] **Step 6: Commit**

```bash
git add api/routers/players.py tests/test_players.py
git commit -m "feat(api): sport-dispatch GET /api/players (NBA vs MLB tables)"
```

## Task 19: Sport-dispatch on `GET /api/players/{id}`

**Files:**
- Modify: `api/routers/players.py`
- Test: `tests/test_players.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_players.py`:

```python
async def test_get_player_mlb(client, db):
    db.add(_mlb_player(660271, "Juan Soto", is_active=True, team_abbreviation="NYM", position="OF"))
    await db.commit()

    response = await client.get("/api/players/660271?sport=MLB")
    assert response.status_code == 200
    data = response.json()
    assert data["full_name"] == "Juan Soto"
    assert data["team_abbreviation"] == "NYM"


async def test_get_player_mlb_not_found(client):
    response = await client.get("/api/players/9999?sport=MLB")
    assert response.status_code == 404
```

- [ ] **Step 2: Run, verify failing**

```bash
python3 -m pytest tests/test_players.py::test_get_player_mlb tests/test_players.py::test_get_player_mlb_not_found -v
```

Expected: FAIL — `get_player` currently only queries the NBA `Player` table.

- [ ] **Step 3: Add sport dispatch to `get_player`**

Replace the `get_player` function in `api/routers/players.py`:

```python
@router.get(
    "/players/{player_id}",
    response_model=Union[PlayerListItem, MlbPlayerListItem],
)
async def get_player(
    player_id: int,
    sport: Sport = Depends(sport_dep),
    db: AsyncSession = Depends(get_db),
):
    if sport == Sport.NBA:
        result = await db.execute(
            select(Player).where(Player.player_id == player_id)
        )
    else:
        result = await db.execute(
            select(MlbPlayer).where(MlbPlayer.player_id == player_id)
        )
    player = result.scalar_one_or_none()
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found")
    return player
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
python3 -m pytest tests/test_players.py -v
```

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add api/routers/players.py tests/test_players.py
git commit -m "feat(api): sport-dispatch GET /api/players/{id}"
```

## Task 20: Sport-dispatch on `GET /api/players/{id}/props`

**Files:**
- Modify: `api/routers/players.py`
- Test: `tests/test_players.py`

- [ ] **Step 1: Add an MLB prop helper, then a failing test**

In `tests/test_players.py`, after the existing `_prop` helper (around line 39-51), add an MLB-keyed prop helper:

```python
def _mlb_prop(player_id: int, bookmaker: str, prop_type: str, outcome: str,
              point: float, price: float, fetched_at: datetime) -> PlayerProp:
    return PlayerProp(
        game_id="g1",
        bookmaker=bookmaker,
        player_name="Test Batter",
        prop_type=prop_type,
        outcome=outcome,
        price=price,
        point=point,
        mlb_player_id=player_id,
        fetched_at=fetched_at,
    )
```

Then append a test at the bottom of the file:

```python
async def test_get_player_props_mlb(client, db):
    db.add(_mlb_player(660271, "Juan Soto", is_active=True))
    await db.flush()
    db.add(_mlb_prop(660271, "DraftKings", "batter_hits", "Over", 0.5, -130,
                      datetime(2026, 5, 14, 10, 0)))
    db.add(_mlb_prop(660271, "DraftKings", "batter_total_bases", "Over", 1.5, -110,
                      datetime(2026, 5, 14, 10, 0)))
    await db.commit()

    response = await client.get("/api/players/660271/props?sport=MLB")
    data = response.json()
    assert len(data) == 2
    prop_types = {r["prop_type"] for r in data}
    assert prop_types == {"batter_hits", "batter_total_bases"}
```

- [ ] **Step 2: Run, verify failing**

```bash
python3 -m pytest tests/test_players.py::test_get_player_props_mlb -v
```

Expected: FAIL — the endpoint filters on `nba_player_id`, so MLB props are invisible.

- [ ] **Step 3: Add sport dispatch to `get_player_props`**

Replace `get_player_props` in `api/routers/players.py`:

```python
@router.get("/players/{player_id}/props", response_model=list[PlayerPropItem])
async def get_player_props(
    player_id: int,
    sport: Sport = Depends(sport_dep),
    db: AsyncSession = Depends(get_db),
):
    join_col = PlayerProp.nba_player_id if sport == Sport.NBA else PlayerProp.mlb_player_id
    subq = (
        select(
            PlayerProp.bookmaker,
            PlayerProp.prop_type,
            PlayerProp.outcome,
            func.max(PlayerProp.fetched_at).label("max_fetched_at"),
        )
        .where(join_col == player_id)
        .group_by(PlayerProp.bookmaker, PlayerProp.prop_type, PlayerProp.outcome)
        .subquery()
    )
    result = await db.execute(
        select(PlayerProp)
        .join(
            subq,
            and_(
                PlayerProp.bookmaker == subq.c.bookmaker,
                PlayerProp.prop_type == subq.c.prop_type,
                PlayerProp.outcome == subq.c.outcome,
                PlayerProp.fetched_at == subq.c.max_fetched_at,
            ),
        )
        .where(join_col == player_id)
        .order_by(PlayerProp.bookmaker, PlayerProp.prop_type, PlayerProp.outcome)
    )
    return result.scalars().all()
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
python3 -m pytest tests/test_players.py -v
```

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add api/routers/players.py tests/test_players.py
git commit -m "feat(api): sport-dispatch /api/players/{id}/props (nba_player_id vs mlb_player_id)"
```

## Task 21: Sport-dispatch on `GET /api/players/{id}/game-logs`

**Files:**
- Modify: `api/routers/players.py`
- Test: `tests/test_players.py`

- [ ] **Step 1: Add MLB game-log helper, then a failing test**

After the existing `_game_log` helper in `tests/test_players.py`, add:

```python
def _mlb_game_log(player_id: int, season: str, game_date: date) -> MlbPlayerGameLog:
    return MlbPlayerGameLog(
        player_id=player_id,
        mlb_game_pk=f"mlb_{player_id}_{game_date}",
        season=season,
        game_date=game_date,
        matchup="NYM @ ATL",
        plate_appearances=4,
        at_bats=3,
        hits=2,
        doubles=1,
        triples=0,
        home_runs=0,
        rbi=1,
        runs=1,
        walks=1,
        strikeouts=0,
        stolen_bases=0,
        total_bases=3,
        fetched_at=datetime(2026, 5, 14, 6, 0),
    )
```

Append a test:

```python
async def test_get_player_game_logs_mlb(client, db):
    db.add(_mlb_player(660271, "Juan Soto", is_active=True))
    await db.flush()
    db.add(_mlb_game_log(660271, "2026", date(2026, 5, 14)))
    db.add(_mlb_game_log(660271, "2026", date(2026, 5, 13)))
    await db.commit()

    response = await client.get("/api/players/660271/game-logs?sport=MLB&season=2026")
    data = response.json()
    assert len(data) == 2
    assert data[0]["game_date"] == "2026-05-14"  # ordered desc
    assert data[0]["hits"] == 2
    assert data[0]["total_bases"] == 3
```

- [ ] **Step 2: Run, verify failing**

```bash
python3 -m pytest tests/test_players.py::test_get_player_game_logs_mlb -v
```

Expected: FAIL — the endpoint queries `PlayerGameLog` (NBA) only.

- [ ] **Step 3: Add sport dispatch to `get_player_game_logs`**

Replace `get_player_game_logs` in `api/routers/players.py`:

```python
@router.get(
    "/players/{player_id}/game-logs",
    response_model=Union[list[PlayerGameLogItem], list[MlbPlayerGameLogItem]],
)
async def get_player_game_logs(
    player_id: int,
    season: str = "2025-26",
    sport: Sport = Depends(sport_dep),
    db: AsyncSession = Depends(get_db),
):
    if sport == Sport.NBA:
        result = await db.execute(
            select(PlayerGameLog)
            .where(
                and_(
                    PlayerGameLog.player_id == player_id,
                    PlayerGameLog.season == season,
                )
            )
            .order_by(PlayerGameLog.game_date.desc())
        )
    else:
        result = await db.execute(
            select(MlbPlayerGameLog)
            .where(
                and_(
                    MlbPlayerGameLog.player_id == player_id,
                    MlbPlayerGameLog.season == season,
                )
            )
            .order_by(MlbPlayerGameLog.game_date.desc())
        )
    return result.scalars().all()
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
python3 -m pytest tests/test_players.py -v
```

Expected: all green.

- [ ] **Step 5: Run full backend suite**

```bash
python3 -m pytest -q
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add api/routers/players.py tests/test_players.py
git commit -m "feat(api): sport-dispatch /api/players/{id}/game-logs"
```

## Task 22: Add MLB TS interfaces and thread `sport` through player fetch fns

**Files:**
- Modify: `web/src/api/client.ts`

- [ ] **Step 1: Add MLB interfaces and update fetch fns**

Open `web/src/api/client.ts`. Find the existing `PlayerListItem` and `PlayerGameLogItem` interfaces. Below them (still above the `BASE` constant), add:

```ts
export interface MlbPlayerListItem {
  player_id: number;
  full_name: string;
  position: string | null;
  bats: string | null;
  throws: string | null;
  team_id: number | null;
  team_abbreviation: string | null;
  is_active: boolean | null;
}

export interface MlbPlayerGameLogItem {
  game_date: string;
  matchup: string | null;
  season: string;
  plate_appearances: number | null;
  at_bats: number | null;
  hits: number | null;
  doubles: number | null;
  triples: number | null;
  home_runs: number | null;
  rbi: number | null;
  runs: number | null;
  walks: number | null;
  strikeouts: number | null;
  stolen_bases: number | null;
  total_bases: number | null;
}
```

Then update `fetchPlayers`, `fetchPlayer`, `fetchPlayerProps`, `fetchPlayerGameLogs`:

```ts
export async function fetchPlayers(
  sport: Sport,
): Promise<PlayerListItem[] | MlbPlayerListItem[]> {
  const res = await fetch(`${BASE}/players?sport=${sport}`);
  if (!res.ok) throw new Error("Failed to fetch players");
  return res.json();
}

export async function fetchPlayer(
  playerId: number,
  sport: Sport,
): Promise<PlayerListItem | MlbPlayerListItem> {
  const res = await fetch(`${BASE}/players/${playerId}?sport=${sport}`);
  if (!res.ok) throw new Error("Player not found");
  return res.json();
}

export async function fetchPlayerProps(
  playerId: number,
  sport: Sport,
): Promise<PlayerPropItem[]> {
  const res = await fetch(`${BASE}/players/${playerId}/props?sport=${sport}`);
  if (!res.ok) throw new Error("Failed to fetch player props");
  return res.json();
}

export async function fetchPlayerGameLogs(
  playerId: number,
  sport: Sport,
  season = "2025-26",
): Promise<PlayerGameLogItem[] | MlbPlayerGameLogItem[]> {
  const res = await fetch(
    `${BASE}/players/${playerId}/game-logs?sport=${sport}&season=${season}`,
  );
  if (!res.ok) throw new Error("Failed to fetch player game logs");
  return res.json();
}
```

- [ ] **Step 2: Type-check (expect failures in PlayersPage/PlayerDetailPage)**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: errors at consumer sites — fixed in the next tasks.

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/api/client.ts
git commit -m "feat(web): add MLB player interfaces and thread sport through player fetch fns"
```

## Task 23: Extract `NbaStatsTable` from `PlayerDetailPage`

**Files:**
- Create: `web/src/components/NbaStatsTable.tsx`
- Modify: `web/src/pages/PlayerDetailPage.tsx`

- [ ] **Step 1: Create `web/src/components/NbaStatsTable.tsx`**

```tsx
// web/src/components/NbaStatsTable.tsx
import { cn } from "@/lib/utils";
import type { PlayerGameLogItem } from "@/api/client";

type StatCategory = "all" | "pts" | "reb" | "ast";

function formatStat(val: number | null): string {
  return val !== null ? String(val) : "—";
}

function formatMin(val: number | null): string {
  if (val === null) return "—";
  return val.toFixed(1);
}

function formatUsg(val: number | null): string {
  if (val === null) return "—";
  return `${(val * 100).toFixed(1)}%`;
}

interface NbaStatsTableProps {
  logs: PlayerGameLogItem[];
  statCategory: StatCategory;
}

export function NbaStatsTable({ logs, statCategory }: NbaStatsTableProps) {
  const highlightCol = (col: StatCategory): string =>
    statCategory !== "all" && statCategory === col
      ? "bg-blue-950/40 text-blue-300 font-semibold"
      : "";

  if (logs.length === 0) {
    return <p className="text-sm text-slate-500">No stats available for this season.</p>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr>
            {[
              { label: "Date", col: null },
              { label: "Matchup", col: null },
              { label: "W/L", col: null },
              { label: "MIN", col: null },
              { label: "PTS", col: "pts" as StatCategory },
              { label: "REB", col: "reb" as StatCategory },
              { label: "AST", col: "ast" as StatCategory },
              { label: "BLK", col: null },
              { label: "STL", col: null },
              { label: "USG%", col: null },
              { label: "+/-", col: null },
            ].map(({ label, col }) => (
              <th
                key={label}
                className={cn(
                  "text-left px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500",
                  col && statCategory === col ? "text-blue-400" : "",
                )}
              >
                {label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {logs.map((log, i) => (
            <tr key={i} className="border-t border-slate-800 hover:bg-slate-900/50">
              <td className="px-2 py-1.5 text-slate-400">{log.game_date}</td>
              <td className="px-2 py-1.5 text-slate-300">{log.matchup ?? "—"}</td>
              <td
                className={cn(
                  "px-2 py-1.5 font-semibold",
                  log.wl === "W" ? "text-green-400" : log.wl === "L" ? "text-red-400" : "text-slate-500",
                )}
              >
                {log.wl ?? "—"}
              </td>
              <td className="px-2 py-1.5 text-slate-400">{formatMin(log.min)}</td>
              <td className={cn("px-2 py-1.5", highlightCol("pts"))}>{formatStat(log.pts)}</td>
              <td className={cn("px-2 py-1.5", highlightCol("reb"))}>{formatStat(log.reb)}</td>
              <td className={cn("px-2 py-1.5", highlightCol("ast"))}>{formatStat(log.ast)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.blk)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.stl)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatUsg(log.usg_pct)}</td>
              <td
                className={cn(
                  "px-2 py-1.5 font-semibold",
                  log.plus_minus !== null && log.plus_minus > 0
                    ? "text-green-400"
                    : log.plus_minus !== null && log.plus_minus < 0
                    ? "text-red-400"
                    : "text-slate-400",
                )}
              >
                {log.plus_minus !== null
                  ? log.plus_minus > 0
                    ? `+${log.plus_minus}`
                    : String(log.plus_minus)
                  : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export type { StatCategory };
```

- [ ] **Step 2: Commit (incremental — PlayerDetailPage still uses inline table; cleanup in Task 25)**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/components/NbaStatsTable.tsx
git commit -m "feat(web): extract NbaStatsTable component (preparation for sport branching)"
```

## Task 24: Create `MlbStatsTable` component

**Files:**
- Create: `web/src/components/MlbStatsTable.tsx`

- [ ] **Step 1: Create `web/src/components/MlbStatsTable.tsx`**

```tsx
// web/src/components/MlbStatsTable.tsx
import type { MlbPlayerGameLogItem } from "@/api/client";

function formatStat(val: number | null): string {
  return val !== null ? String(val) : "—";
}

interface MlbStatsTableProps {
  logs: MlbPlayerGameLogItem[];
}

export function MlbStatsTable({ logs }: MlbStatsTableProps) {
  if (logs.length === 0) {
    return <p className="text-sm text-slate-500">No stats available for this season.</p>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr>
            {[
              "Date", "Matchup", "PA", "AB", "H", "2B", "3B", "HR", "RBI", "R", "BB", "K", "SB", "TB",
            ].map((label) => (
              <th
                key={label}
                className="text-left px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500"
              >
                {label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {logs.map((log, i) => (
            <tr key={i} className="border-t border-slate-800 hover:bg-slate-900/50">
              <td className="px-2 py-1.5 text-slate-400">{log.game_date}</td>
              <td className="px-2 py-1.5 text-slate-300">{log.matchup ?? "—"}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.plate_appearances)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.at_bats)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.hits)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.doubles)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.triples)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.home_runs)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.rbi)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.runs)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.walks)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.strikeouts)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.stolen_bases)}</td>
              <td className="px-2 py-1.5 text-slate-400">{formatStat(log.total_bases)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 2: Type-check**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors in the new file (existing pages may still have errors from Task 22).

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/components/MlbStatsTable.tsx
git commit -m "feat(web): add MlbStatsTable component"
```

## Task 25: Wire sport into `PlayerDetailPage`

**Files:**
- Modify: `web/src/pages/PlayerDetailPage.tsx`

- [ ] **Step 1: Replace `web/src/pages/PlayerDetailPage.tsx`**

```tsx
import { useEffect, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { ChevronLeft } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  fetchPlayer,
  fetchPlayerProps,
  fetchPlayerGameLogs,
} from "@/api/client";
import type {
  PlayerListItem,
  MlbPlayerListItem,
  PlayerPropItem,
  PlayerGameLogItem,
  MlbPlayerGameLogItem,
} from "@/api/client";
import { useSport } from "@/lib/sport";
import { propTypeLabel } from "@/lib/propTypes";
import { NbaStatsTable, type StatCategory } from "@/components/NbaStatsTable";
import { MlbStatsTable } from "@/components/MlbStatsTable";

const NBA_SEASONS = ["2025-26", "2024-25", "2023-24"];
const MLB_SEASONS = ["2026", "2025", "2024"];
const LAST_N_OPTIONS = [5, 10, 20] as const;

const STAT_CATEGORY_LABELS: Record<StatCategory, string> = {
  all: "All",
  pts: "Points",
  reb: "Rebounds",
  ast: "Assists",
};

function groupPropsByBookmaker(
  props: PlayerPropItem[]
): Map<string, PlayerPropItem[]> {
  const map = new Map<string, PlayerPropItem[]>();
  for (const p of props) {
    const bk = p.bookmaker ?? "Unknown";
    if (!map.has(bk)) map.set(bk, []);
    map.get(bk)!.push(p);
  }
  return map;
}

function formatPrice(price: number | null): string {
  if (price === null) return "—";
  return price > 0 ? `+${price}` : String(price);
}

export function PlayerDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const playerId = Number(id);
  const { sport } = useSport();

  const [player, setPlayer] = useState<PlayerListItem | MlbPlayerListItem | null>(null);
  const [props, setProps] = useState<PlayerPropItem[]>([]);
  const [nbaLogs, setNbaLogs] = useState<PlayerGameLogItem[]>([]);
  const [mlbLogs, setMlbLogs] = useState<MlbPlayerGameLogItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const seasons = sport === "MLB" ? MLB_SEASONS : NBA_SEASONS;
  const [season, setSeason] = useState(seasons[0]);
  const [lastN, setLastN] = useState<number | null>(null);
  const [statCategory, setStatCategory] = useState<StatCategory>("all");

  // Reset season + logs when sport changes (different season formats per sport)
  useEffect(() => {
    setSeason(sport === "MLB" ? MLB_SEASONS[0] : NBA_SEASONS[0]);
    setStatCategory("all");
  }, [sport]);

  // Fetch player + props on player or sport change
  useEffect(() => {
    let isCurrent = true;
    setLoading(true);
    setError(null);
    Promise.all([
      fetchPlayer(playerId, sport),
      fetchPlayerProps(playerId, sport),
    ])
      .then(([playerData, propsData]) => {
        if (!isCurrent) return;
        setPlayer(playerData);
        setProps(propsData);
      })
      .catch((e) => { if (isCurrent) setError(e.message); })
      .finally(() => { if (isCurrent) setLoading(false); });
    return () => { isCurrent = false; };
  }, [playerId, sport]);

  // Fetch game logs on player, sport, or season change
  useEffect(() => {
    let isCurrent = true;
    fetchPlayerGameLogs(playerId, sport, season)
      .then((logs) => {
        if (!isCurrent) return;
        if (sport === "MLB") {
          setMlbLogs(logs as MlbPlayerGameLogItem[]);
          setNbaLogs([]);
        } else {
          setNbaLogs(logs as PlayerGameLogItem[]);
          setMlbLogs([]);
        }
      })
      .catch((e) => { if (isCurrent) setError(e.message); });
    return () => { isCurrent = false; };
  }, [playerId, sport, season]);

  if (loading) {
    return <p className="px-6 py-8 text-slate-500">Loading...</p>;
  }
  if (error || !player) {
    return (
      <div className="px-6 py-8">
        <p className="text-red-400 mb-3">{error ?? "Player not found."}</p>
        <button
          onClick={() => navigate("/players")}
          className="text-sm text-blue-400 hover:text-blue-300 flex items-center gap-1"
        >
          <ChevronLeft className="w-4 h-4" /> Back to Players
        </button>
      </div>
    );
  }

  const propsByBookmaker = groupPropsByBookmaker(props);

  // Apply lastN to whichever log array is active
  const displayedNbaLogs = lastN ? nbaLogs.slice(0, lastN) : nbaLogs;
  const displayedMlbLogs = lastN ? mlbLogs.slice(0, lastN) : mlbLogs;

  const isMlb = sport === "MLB";
  const mlbPlayer = isMlb ? (player as MlbPlayerListItem) : null;

  return (
    <div className="max-w-4xl mx-auto px-4 py-6">
      <button
        onClick={() => navigate("/players")}
        className="text-sm text-slate-500 hover:text-slate-300 flex items-center gap-1 mb-4"
      >
        <ChevronLeft className="w-4 h-4" /> Players
      </button>

      <div className="mb-6">
        <h1 className="text-xl font-bold text-slate-100">{player.full_name}</h1>
        <p className="text-sm text-slate-500 mt-0.5">
          {player.position ?? "—"} · {player.team_abbreviation ?? "—"}
          {mlbPlayer && (mlbPlayer.bats || mlbPlayer.throws) && (
            <span className="ml-3 text-slate-600">
              B/T: {mlbPlayer.bats ?? "—"}/{mlbPlayer.throws ?? "—"}
            </span>
          )}
        </p>
      </div>

      <section className="mb-8">
        <p className="text-[11px] font-bold uppercase tracking-widest text-slate-500 pb-2 mb-3 border-b border-slate-800">
          Lines by Bookmaker
        </p>
        {propsByBookmaker.size === 0 ? (
          <p className="text-sm text-slate-500">No props available.</p>
        ) : (
          <div className="flex flex-wrap gap-3">
            {Array.from(propsByBookmaker.entries()).map(([bookmaker, bkProps]) => (
              <div
                key={bookmaker}
                className="bg-slate-900 border border-slate-800 rounded-lg p-4 min-w-[200px]"
              >
                <p className="text-[11px] font-bold uppercase tracking-wide text-slate-400 mb-3">
                  {bookmaker}
                </p>
                <table className="w-full text-xs">
                  <thead>
                    <tr>
                      {["Prop", "Line", "Side", "Price"].map((h) => (
                        <th
                          key={h}
                          className="text-left pr-3 pb-1 text-[10px] uppercase tracking-wide text-slate-600"
                        >
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {bkProps.map((p, i) => (
                      <tr key={i} className="border-t border-slate-800">
                        <td className="pr-3 py-1.5 text-violet-400 font-semibold">
                          {propTypeLabel(p.prop_type)}
                        </td>
                        <td className="pr-3 py-1.5 text-blue-400 font-semibold">
                          {p.point ?? "—"}
                        </td>
                        <td className="pr-3 py-1.5 text-slate-400">{p.outcome}</td>
                        <td
                          className={cn(
                            "py-1.5 font-semibold",
                            p.price !== null && p.price > 0
                              ? "text-green-400"
                              : "text-red-400",
                          )}
                        >
                          {formatPrice(p.price)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ))}
          </div>
        )}
      </section>

      <section>
        <p className="text-[11px] font-bold uppercase tracking-widest text-slate-500 pb-2 mb-3 border-b border-slate-800">
          Game Log
        </p>

        <div className="flex flex-wrap gap-3 mb-4 items-center">
          <div className="flex gap-1">
            {seasons.map((s) => (
              <button
                key={s}
                onClick={() => setSeason(s)}
                className={cn(
                  "text-[10px] font-bold tracking-wide px-2 py-1 rounded cursor-pointer",
                  season === s
                    ? "bg-blue-950 text-blue-400"
                    : "bg-slate-800 text-slate-500 hover:text-slate-400",
                )}
              >
                {s}
              </button>
            ))}
          </div>

          <span className="text-slate-700 text-xs">|</span>

          <div className="flex gap-1">
            <button
              onClick={() => setLastN(null)}
              className={cn(
                "text-[10px] font-bold tracking-wide px-2 py-1 rounded cursor-pointer",
                lastN === null
                  ? "bg-blue-950 text-blue-400"
                  : "bg-slate-800 text-slate-500 hover:text-slate-400",
              )}
            >
              All
            </button>
            {LAST_N_OPTIONS.map((n) => (
              <button
                key={n}
                onClick={() => setLastN(n)}
                className={cn(
                  "text-[10px] font-bold tracking-wide px-2 py-1 rounded cursor-pointer",
                  lastN === n
                    ? "bg-blue-950 text-blue-400"
                    : "bg-slate-800 text-slate-500 hover:text-slate-400",
                )}
              >
                L{n}
              </button>
            ))}
          </div>

          {!isMlb && (
            <>
              <span className="text-slate-700 text-xs">|</span>
              <div className="flex gap-1">
                {(["all", "pts", "reb", "ast"] as StatCategory[]).map((cat) => (
                  <button
                    key={cat}
                    onClick={() => setStatCategory(cat)}
                    className={cn(
                      "text-[10px] font-bold tracking-wide px-2 py-1 rounded cursor-pointer",
                      statCategory === cat
                        ? "bg-blue-950 text-blue-400"
                        : "bg-slate-800 text-slate-500 hover:text-slate-400",
                    )}
                  >
                    {STAT_CATEGORY_LABELS[cat]}
                  </button>
                ))}
              </div>
            </>
          )}
        </div>

        {isMlb
          ? <MlbStatsTable logs={displayedMlbLogs} />
          : <NbaStatsTable logs={displayedNbaLogs} statCategory={statCategory} />
        }
      </section>
    </div>
  );
}
```

- [ ] **Step 2: Type-check**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/pages/PlayerDetailPage.tsx
git commit -m "feat(web): PlayerDetailPage branches stats table by sport, supports MLB seasons"
```

## Task 26: Wire sport into `PlayersPage` and show bats/throws when MLB

**Files:**
- Modify: `web/src/pages/PlayersPage.tsx`

- [ ] **Step 1: Replace `web/src/pages/PlayersPage.tsx`**

```tsx
import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { fetchPlayers } from "@/api/client";
import type { PlayerListItem, MlbPlayerListItem } from "@/api/client";
import { useSport } from "@/lib/sport";

type AnyPlayer = PlayerListItem | MlbPlayerListItem;

function isMlbPlayer(p: AnyPlayer): p is MlbPlayerListItem {
  return "bats" in p;
}

export function PlayersPage() {
  const { sport } = useSport();
  const [players, setPlayers] = useState<AnyPlayer[]>([]);
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  useEffect(() => {
    let isCurrent = true;
    setLoading(true);
    setError(null);
    fetchPlayers(sport)
      .then((data) => { if (isCurrent) setPlayers(data); })
      .catch((e) => { if (isCurrent) setError(e.message); })
      .finally(() => { if (isCurrent) setLoading(false); });
    return () => { isCurrent = false; };
  }, [sport]);

  const filtered = players.filter((p) =>
    p.full_name.toLowerCase().includes(search.toLowerCase()),
  );

  if (loading) {
    return <p className="px-6 py-8 text-slate-500">Loading players...</p>;
  }
  if (error) {
    return <p className="px-6 py-8 text-red-400">Failed to load players: {error}</p>;
  }

  return (
    <div className="max-w-4xl mx-auto px-4 py-6">
      <input
        type="text"
        placeholder="Search players..."
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        className="w-full mb-4 px-3 py-2 bg-slate-800 border border-slate-700 rounded text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:border-blue-400"
      />

      {filtered.length === 0 ? (
        <p className="text-slate-500 text-sm">No players found.</p>
      ) : (
        <div className="flex flex-col gap-1">
          {filtered.map((player) => (
            <button
              key={player.player_id}
              onClick={() => navigate(`/players/${player.player_id}`)}
              className="flex items-center gap-4 px-4 py-3 bg-slate-900 border border-slate-800 rounded-lg text-left hover:bg-slate-800/50 transition-colors"
            >
              <div className="flex-1">
                <p className="text-sm font-medium text-slate-100">{player.full_name}</p>
                <p className="text-[11px] text-slate-500 mt-0.5">
                  {player.position ?? "—"} · {player.team_abbreviation ?? "—"}
                  {isMlbPlayer(player) && (player.bats || player.throws) && (
                    <span className="ml-2 text-slate-600">
                      · B/T {player.bats ?? "—"}/{player.throws ?? "—"}
                    </span>
                  )}
                </p>
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Type-check**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git add web/src/pages/PlayersPage.tsx
git commit -m "feat(web): PlayersPage honors sport context and surfaces MLB bats/throws"
```

## Phase 3 close-out

- [ ] **Step 1: Run the full backend test suite**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
python3 -m pytest -q
```

Expected: all green. Pass count = baseline + ~13 new tests across Phases 1–3.

- [ ] **Step 2: Type-check the frontend**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin/web
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 3: Manual browser verification (full parity)**

Make sure both the local uvicorn (`python3 -m uvicorn api.main:app --reload --port 8002` from odds-admin root) and `npm run dev` (from `web/`) are running, then exercise:

1. **Recommendations:** flip NBA ↔ MLB. Counts match DB:
   ```sql
   SELECT sport, count(*) FROM recommendations
   WHERE game_date = CURRENT_DATE GROUP BY sport;
   ```
2. **Games:** flip NBA ↔ MLB. Game lists match DB:
   ```sql
   SELECT sport, count(*) FROM games
   WHERE commence_time::date = CURRENT_DATE GROUP BY sport;
   ```
3. **Players:** with MLB selected, the list should show MLB roster names (Juan Soto, etc.). The search box should filter them.
4. **Player detail (MLB):** click an MLB player with recent props (use a name that appears in today's MLB recommendations). Verify:
   - Header shows position and team; "B/T: L/R" (or similar) appears if `mlb_players.bats`/`throws` is populated
   - Lines by Bookmaker shows MLB markets (Hits, Total Bases) with labels not raw codes
   - Game Log shows PA / AB / H / 2B / 3B / HR / RBI / R / BB / K / SB / TB columns
   - Season selector shows MLB seasons (2026, 2025, 2024)
5. **Player detail (NBA):** flip back to NBA, open a player from the NBA list. Verify the previous-version NBA UI (PTS/REB/AST highlights, 2025-26 / 2024-25 / 2023-24 seasons) still works exactly.
6. **Persistence:** refresh the page on each route — last-selected sport persists.

- [ ] **Step 4: Final summary check on the branch**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git log --oneline main..HEAD | head -30
```

Expected: ~25-30 commits making up the three phases.

---

# Merge

- [ ] **Step 1: Merge to main (per user preference: one branch, single merge)**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-admin
git checkout main
git merge --no-ff feature/odds-admin-mlb -m "Merge branch 'feature/odds-admin-mlb'"
git log --oneline -5
```

- [ ] **Step 2: Delete the feature branch**

```bash
git branch -d feature/odds-admin-mlb
```

- [ ] **Step 3: Rebuild the Docker image so the production container picks up the new code**

```bash
cd /home/bryang/Dev_Space/python_projects/odds-pipeline
docker compose up odds-admin --build -d
docker compose ps odds-admin
```

Expected: `odds-pipeline-odds-admin-1` shows `Up` after the rebuild.

- [ ] **Step 4: Final live smoke check**

Open http://localhost:8001 — flip sports, click around. Match against expectations from Phase 3 Step 3.

---

# Done.

Full odds-admin MLB parity is live. Next-up roadmap candidates (out of scope for this plan):

- Add an Accuracy block to `RecommendationsPage` (the backend endpoint exists; no consumer yet)
- Sport-aware bookmaker/market admin views if the user expands to in-game markets
- NFL: add `Sport.NFL`, extend `GAMES_SPORT_TO_LONG`, repeat the dispatch pattern — at this point an inline pattern crosses three branches and extracting a `PlayerService` becomes attractive
