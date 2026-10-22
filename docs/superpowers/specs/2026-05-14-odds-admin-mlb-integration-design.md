# odds-admin MLB Integration Design

**Date:** 2026-05-14
**Status:** Approved
**Repo affected:** `odds-admin` (sibling repo at `../odds-admin`)
**Related:** odds-pipeline issue #5 (customer-facing recommendations UI), MLB pipeline live since 2026-05-11

## Goal

odds-admin currently shows NBA-only data even though the shared Postgres database is already multi-sport (MLB pipeline writes alongside NBA as of the bootstrap merge). Thread the existing `sport` axis through odds-admin so the user can switch between NBA and MLB at the top of the app and see full parity: Games, Players, and Recommendations all honor the selected sport.

## Non-goals

- No DB migrations. Schema is already multi-sport (`games.sport`, `recommendations.sport`, `player_props.nba_player_id` + `.mlb_player_id`, parallel `mlb_*` tables).
- No new endpoints. Only query-param additions to existing endpoints.
- No NFL prep work. The dispatch pattern extends naturally; we won't pre-bake enum entries.
- No URL-routing redesign. Sport lives in React context, not in path segments.
- No team/roster pages. (No NBA equivalent today either.)
- No bookmaker/market work. MLB markets (`batter_hits`, `batter_total_bases`) are already populated in `player_props`.

## Architecture

Three layers, sport flows top-down:

```
┌────────────────────────────────────────────────────────────────┐
│ Frontend (React)                                               │
│   SportContext (NBA | MLB, persisted in localStorage)          │
│   → top-nav <SportSelector/>                                   │
│   → every fetch fn passes ?sport= to the API                   │
│   → PlayerDetailPage swaps <NbaStatsTable/> vs <MlbStatsTable/>│
└────────────────────────────────────────────────────────────────┘
                          │
                          ▼ ?sport=NBA | MLB
┌────────────────────────────────────────────────────────────────┐
│ Backend (FastAPI)                                              │
│   Sport enum + sport_dep() FastAPI dependency                  │
│   Players router: dispatches to NBA vs MLB models internally   │
│   Games/Recs routers: filter rows by games.sport / recs.sport  │
└────────────────────────────────────────────────────────────────┘
                          │
                          ▼ SQLAlchemy
┌────────────────────────────────────────────────────────────────┐
│ Data (Postgres — already multi-sport)                          │
│   games.sport ✓  recommendations.sport ✓                       │
│   player_props.nba_player_id + .mlb_player_id ✓                │
│   players + mlb_players (parallel)                             │
│   player_game_logs + mlb_player_game_logs (parallel)           │
└────────────────────────────────────────────────────────────────┘
```

### Key architectural decisions

1. **No schema migrations.** Every column we need exists.
2. **Inline sport dispatch in routers.** Each players-router function reads `sport`, picks the model class, runs the query. Extract a `PlayerService` later only if NFL makes the dispatch ugly (YAGNI).
3. **Short sport codes in the API: `NBA`, `MLB`.** `games.sport` stores the long Odds-API form (`basketball_nba`/`baseball_mlb`); the router layer maps to/from short codes so the API contract stays clean. `recommendations.sport` already uses short codes.
4. **`sport` is a query parameter, not a path prefix.** `/api/games?sport=MLB`, not `/api/mlb/games`. Matches the global-toggle UX and keeps a single React route tree.
5. **`sport=NBA` is the default everywhere.** Preserves existing UI/test behavior so the rollout is risk-free for NBA.

## Backend changes

### New SQLAlchemy models (`api/models.py`)

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
    player_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("mlb_players.player_id"))
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

### Extend `PlayerProp` model

Add the existing-but-unmapped column:

```python
mlb_player_id: Mapped[int | None] = mapped_column(
    Integer, ForeignKey("mlb_players.player_id")
)
```

### New helper (`api/sport.py`)

```python
from enum import Enum
from fastapi import Query

class Sport(str, Enum):
    NBA = "NBA"
    MLB = "MLB"

# Map short codes ↔ games.sport long form
GAMES_SPORT_TO_LONG = {
    Sport.NBA: "basketball_nba",
    Sport.MLB: "baseball_mlb",
}

def sport_dep(sport: Sport = Query(default=Sport.NBA)) -> Sport:
    return sport
```

### Endpoint changes

All sport-aware endpoints add a `?sport=` query param. Default `NBA` preserves current behavior.

| Endpoint | Change |
|---|---|
| `GET /api/games?sport=` | Filter `Game.sport == GAMES_SPORT_TO_LONG[sport]` |
| `GET /api/games/{id}/odds` | Unchanged (game-scoped) |
| `GET /api/games/{id}/props` | Unchanged (game-scoped) |
| `GET /api/games/{id}/scores` | Unchanged (game-scoped) |
| `GET /api/players?sport=` | Dispatch: `Player` for NBA, `MlbPlayer` for MLB |
| `GET /api/players/{id}?sport=` | Dispatch on sport |
| `GET /api/players/{id}/props?sport=` | Join on `nba_player_id` (NBA) or `mlb_player_id` (MLB) |
| `GET /api/players/{id}/game-logs?sport=` | Dispatch to `PlayerGameLog` or `MlbPlayerGameLog`; returns sport-specific schema |
| `GET /api/recommendations?sport=` | Filter `recommendations.sport == sport` |
| `GET /api/recommendations/top?sport=` | Same |
| `GET /api/recommendations/accuracy?sport=` | Same |

### New schemas (`api/schemas.py`)

```python
class MlbPlayerListItem(BaseModel):
    player_id: int
    full_name: str
    position: str | None
    bats: str | None
    throws: str | None
    team_id: int | None
    team_abbreviation: str | None
    is_active: bool | None
    model_config = ConfigDict(from_attributes=True)

class MlbPlayerGameLogItem(BaseModel):
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
    model_config = ConfigDict(from_attributes=True)
```

Add `sport: str` to existing `PropRecommendationItem`.

### Dispatch pattern

Inline dispatch per endpoint. Example:

```python
@router.get("/players")
async def list_players(
    sport: Sport = Depends(sport_dep),
    db: AsyncSession = Depends(get_db),
) -> list[PlayerListItem] | list[MlbPlayerListItem]:
    if sport == Sport.NBA:
        # existing NBA query against Player
    else:
        result = await db.execute(
            select(MlbPlayer)
            .where(MlbPlayer.is_active == True)
            .order_by(MlbPlayer.full_name)
        )
        return result.scalars().all()
```

For endpoints whose response shape differs by sport, the response is typed as a `Union` of two list shapes; FastAPI infers the discriminator from object structure. Acceptable trade-off — the OpenAPI doc will list both shapes, and the frontend already knows which it expects from its own sport context.

## Frontend changes

### New context (`web/src/lib/sport.tsx`)

```tsx
type Sport = "NBA" | "MLB";

const SportContext = createContext<{
  sport: Sport;
  setSport: (s: Sport) => void;
} | null>(null);

const STORAGE_KEY = "odds-admin.sport";

export function SportProvider({ children }) {
  const [sport, setSportState] = useState<Sport>(() => {
    const stored = localStorage.getItem(STORAGE_KEY);
    return stored === "MLB" || stored === "NBA" ? stored : "NBA";
  });

  const setSport = (s: Sport) => {
    setSportState(s);
    localStorage.setItem(STORAGE_KEY, s);
  };

  return <SportContext.Provider value={{ sport, setSport }}>{children}</SportContext.Provider>;
}

export function useSport() {
  const ctx = useContext(SportContext);
  if (!ctx) throw new Error("useSport must be inside <SportProvider/>");
  return ctx;
}
```

### New component (`web/src/components/SportSelector.tsx`)

Dropdown next to the "Odds Admin" logo in the top nav. Two options: `🏀 NBA` / `⚾ MLB`. Reads/writes via `useSport`. Re-renders trigger downstream refetches via `sport` query key.

### API client updates

Add a `sport: Sport` parameter (appended as `?sport=`) to every fetch fn that calls a sport-aware endpoint:

- `fetchGames`
- `fetchPlayers`, `fetchPlayer`, `fetchPlayerProps`, `fetchPlayerGameLogs`
- `fetchRecommendations`, `fetchTopRecommendations`, `fetchAccuracy`

### Page-by-page changes

| Page / Component | Change |
|---|---|
| `App.tsx` | Wrap routes in `<SportProvider/>`; add `<SportSelector/>` to nav |
| `GamesPage` | `const { sport } = useSport()` → `fetchGames(sport)`; refetch on sport change |
| `GameCard` / `OddsTab` / `ScoreTab` | Unchanged (game-scoped) |
| `PlayerPropsTab` | Use shared `PROP_TYPE_LABEL` map covering NBA + MLB prop types |
| `PlayersPage` | Pass sport from context to `fetchPlayers`; single row layout works for both (name + team + position); show bats/throws only when MLB |
| `PlayerDetailPage` | Pass sport from context; render `<NbaStatsTable/>` or `<MlbStatsTable/>` for game logs based on sport |
| `RecommendationsPage` | Pass sport from context; add small sport badge in TopPicksModule for clarity |

### New / extracted components

- **`MlbStatsTable.tsx`** — columns: `Date | Matchup | PA | AB | H | 2B | 3B | HR | RBI | R | BB | K | SB | TB`
- **`NbaStatsTable.tsx`** — extracted from `PlayerDetailPage.tsx` for symmetry. Same columns as today.

### Sport-aware prop-type label map (`web/src/lib/propTypes.ts`)

```ts
export const PROP_TYPE_LABEL: Record<string, string> = {
  // NBA
  player_points: "Points",
  player_rebounds: "Rebounds",
  player_assists: "Assists",
  // MLB
  batter_hits: "Hits",
  batter_total_bases: "Total Bases",
};
```

### Refetch on sport change

Include `sport` in the query/effect dependency list so changing the selector triggers a fresh fetch. The codebase uses plain `useEffect` fetches today (no react-query); we'll follow the existing pattern and add `sport` to the dependency array on each page.

## Data flow example: viewing MLB recommendations

1. User clicks `⚾ MLB` in top-nav selector → `SportProvider.setSport("MLB")` writes localStorage and re-renders consumers
2. `RecommendationsPage` re-runs effect with `sport="MLB"` → `fetchRecommendations("MLB", date)` → `GET /api/recommendations?sport=MLB&date=2026-05-14`
3. Backend `list_recommendations` reads `sport=MLB`, runs `select(PropRecommendation).where(sport == "MLB", game_date == "2026-05-14")`
4. Response includes the `sport` field on each row; UI renders the existing table; TopPicksModule shows a small `⚾ MLB` badge

## Error handling

- **Backend, invalid sport value:** FastAPI's enum coercion returns 422 automatically. No custom handling needed.
- **Backend, no rows for sport:** Return empty list (existing behavior).
- **Frontend, fetch failure:** Existing per-page error handling (loading/error states) covers the new params without change.
- **Frontend, localStorage parse error or first visit:** SportProvider falls back to `"NBA"`.

## Testing strategy

| Layer | Tests |
|---|---|
| Backend models | Smoke test: `MlbPlayer` and `MlbPlayerGameLog` map to real DB tables and round-trip a single row |
| Backend routers | For each sport-aware endpoint: one NBA test (existing behavior preserved) + one MLB test (new). Verify default `sport=NBA` matches pre-change response for NBA fixtures |
| Backend dispatch | One test per dual-shape endpoint confirming the response model resolves correctly for each sport |
| Frontend | odds-admin has no frontend test framework today. Don't introduce one as part of this work — verify the UI by running the Vite dev server and exercising each page for both sports manually. This matches the existing project conventions |

**Manual UI verification checklist** (run against the Vite dev server during phase completion):
- Switching the top-nav selector NBA ↔ MLB updates all visible pages within one re-render
- Page refresh preserves the last selected sport
- Games page shows the right list of games per sport
- Recommendations page shows the right picks per sport (and the row count matches what's in the DB)
- Player detail page shows the correct stats table for the selected sport

**No-regression target:** all existing backend odds-admin pytest tests must pass without modification. Since every endpoint defaults to `sport=NBA`, the NBA test fixtures need no updates.

## Rollout phasing

One feature branch (`feature/odds-admin-mlb`), three vertical slices each commit-grouped, single merge at the end.

| Phase | Slice | Rationale |
|---|---|---|
| 1 | **Infrastructure + Recommendations** — Sport enum, SportContext, top-nav selector, `?sport=` on recs endpoints, RecommendationsPage uses context | Smallest surface; recs schema is already generic; gets MLB picks visible first and validates the threading pattern |
| 2 | **Games** — `?sport=` on `/api/games`; GamesPage filters; MLB prop-type labels in PlayerPropsTab | Game-scoped endpoints (odds/props/scores) need no changes; this is mostly UI threading |
| 3 | **Players** — `MlbPlayer` and `MlbPlayerGameLog` models + schemas, sport-dispatched player endpoints, `MlbStatsTable`, `NbaStatsTable` extraction | Biggest lift; relies on patterns proven in phases 1+2 |

Each phase ends in a state we could ship — useful safety if scope tightens mid-implementation.

## Risks & open questions

- **OpenAPI clarity with union responses.** The `/players` and `/players/{id}/game-logs` endpoints return `Union[NbaShape, MlbShape]`. Acceptable trade-off because the frontend is the only consumer and it always knows which shape it expects from context. If this becomes confusing, we can split into `/players/nba` and `/players/mlb` later — but that contradicts the chosen query-param architecture, so we'll only do so under demonstrated pain.
- **Bats/throws columns on `MlbPlayer`.** Confirmed present in `mlb_players` (verified 2026-05-14 via `\d mlb_players`).
- **Sport selector iconography.** Spec assumes emoji (🏀, ⚾). If they render poorly in the user's terminal/browser context, swap for short text badges. UI polish, not architectural.
- **Future NFL:** When adding NFL, the inline dispatch will gain a third branch. If that crosses a comfort threshold, extract a `PlayerService` — no spec change needed here.

## Acceptance criteria

- Switching the top-nav selector between NBA and MLB updates all three pages (Games, Players, Recommendations) within one re-render
- Refreshing the browser preserves the last selected sport
- Every existing NBA test still passes unchanged
- New MLB tests cover: at least one happy path per sport-aware endpoint, plus the players/game-logs sport dispatch
- `/api/recommendations?sport=MLB&date=<today>` returns the live MLB picks visible in the database
- `/api/players?sport=MLB` returns the live `mlb_players` rows (currently active or with a game log this season)
- MLB `PlayerDetailPage` shows the batter stats table (PA/AB/H/2B/3B/HR/RBI/R/BB/K/SB/TB) for the selected player's recent game logs

## Implementation surface summary

| Repo file (new or changed) | Type | Phase |
|---|---|---|
| `odds-admin/api/sport.py` | new | 1 |
| `odds-admin/api/models.py` | add `MlbPlayer`, `MlbPlayerGameLog`; extend `PlayerProp` | 3 |
| `odds-admin/api/schemas.py` | add MLB list/game-log items; add `sport` to `PropRecommendationItem` | 1 (rec) / 3 (player) |
| `odds-admin/api/routers/recommendations.py` | add `?sport=` filter | 1 |
| `odds-admin/api/routers/games.py` | add `?sport=` filter | 2 |
| `odds-admin/api/routers/players.py` | sport dispatch on all four endpoints | 3 |
| `odds-admin/web/src/lib/sport.tsx` | new SportContext + provider + hook | 1 |
| `odds-admin/web/src/components/SportSelector.tsx` | new | 1 |
| `odds-admin/web/src/App.tsx` | wrap provider; mount selector | 1 |
| `odds-admin/web/src/api/*` | thread `sport` through fetch fns | 1 → 3 |
| `odds-admin/web/src/pages/RecommendationsPage.tsx` | use sport | 1 |
| `odds-admin/web/src/pages/GamesPage.tsx` | use sport | 2 |
| `odds-admin/web/src/pages/PlayersPage.tsx` | use sport; show bats/throws for MLB | 3 |
| `odds-admin/web/src/pages/PlayerDetailPage.tsx` | use sport; sport-conditional stats table | 3 |
| `odds-admin/web/src/components/MlbStatsTable.tsx` | new | 3 |
| `odds-admin/web/src/components/NbaStatsTable.tsx` | extract from PlayerDetailPage | 3 |
| `odds-admin/web/src/components/PlayerPropsTab.tsx` | use shared prop-type label map | 2 |
| `odds-admin/web/src/lib/propTypes.ts` | new shared label map (NBA + MLB entries together) | 1 |
| Test files mirroring each of the above | new / extended | matching phase |
