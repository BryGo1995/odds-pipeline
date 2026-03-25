-- Migration 002: teams table, referential integrity, schema quality improvements
-- Idempotent: safe to run multiple times (uses IF NOT EXISTS / IF EXISTS guards)

-- ============================================================
-- 1. TEAMS TABLE
-- Central source of truth for NBA team metadata. All team_id
-- references across players, game logs, and season stats point here.
-- Backfill via the fetch_teams() transformer before validating FKs below.
-- ============================================================
CREATE TABLE IF NOT EXISTS teams (
    team_id      INT         PRIMARY KEY,
    full_name    TEXT        NOT NULL,
    abbreviation TEXT        NOT NULL,
    city         TEXT,
    nickname     TEXT,
    conference   TEXT,
    division     TEXT,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_teams_abbreviation ON teams (abbreviation);

-- ============================================================
-- 2. FK CONSTRAINTS POINTING TO teams
--    Added as NOT VALID so they don't block on existing rows.
--    After backfilling teams, validate with:
--      ALTER TABLE players          VALIDATE CONSTRAINT fk_players_team;
--      ALTER TABLE player_game_logs VALIDATE CONSTRAINT fk_pgl_team;
--      ALTER TABLE team_game_logs   VALIDATE CONSTRAINT fk_tgl_team;
--      ALTER TABLE team_season_stats VALIDATE CONSTRAINT fk_tss_team;
-- ============================================================
ALTER TABLE players
    ADD CONSTRAINT fk_players_team
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
    NOT VALID;

-- NOTE: player_game_logs.team_id is intentionally excluded from FK enforcement.
-- The NBA API returns G-League and two-way contract team IDs (e.g. 1610616833)
-- that are not present in the static teams dataset. A FK here would break
-- pipeline inserts for those players.

ALTER TABLE team_game_logs
    ADD CONSTRAINT fk_tgl_team
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
    NOT VALID;

ALTER TABLE team_season_stats
    ADD CONSTRAINT fk_tss_team
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
    NOT VALID;

-- ============================================================
-- 3. UNIQUE CONSTRAINT ON games.nba_game_id
--    Enables a clean, unambiguous join:
--      player_game_logs pgl JOIN games g ON pgl.nba_game_id = g.nba_game_id
--
--    NOTE: We intentionally do NOT add a FK from log tables → games.nba_game_id.
--    Player/team stats are inserted before the linker runs, so a hard FK would
--    require reordering the pipeline. The UNIQUE constraint is sufficient to
--    guarantee correct joins once the linker has populated games.nba_game_id.
-- ============================================================
ALTER TABLE games
    ADD CONSTRAINT uq_games_nba_game_id UNIQUE (nba_game_id);

-- The plain index is now redundant; the UNIQUE constraint creates its own
DROP INDEX IF EXISTS idx_games_nba_game_id;

-- ============================================================
-- 4. FIX games.commence_time: TIMESTAMP → TIMESTAMPTZ
--    The game_id_linker already compensates with AT TIME ZONE 'America/New_York'.
--    Storing as TIMESTAMPTZ makes the offset explicit and removes that workaround.
--    Existing values are assumed to be UTC (Odds API returns UTC).
-- ============================================================
ALTER TABLE games
    ALTER COLUMN commence_time TYPE TIMESTAMPTZ
    USING commence_time AT TIME ZONE 'UTC';

-- ============================================================
-- 5. PREVENT DUPLICATE ODDS ROWS
--    Including last_update allows historical snapshots (different timestamps
--    = different rows). Same (game, bookmaker, market, outcome, timestamp)
--    twice in one fetch will conflict and be ignored with ON CONFLICT DO NOTHING.
--    Note: if last_update is NULL, Postgres treats NULLs as distinct — add a
--    NOT NULL to last_update if you want NULL to also be deduplicated.
-- ============================================================
ALTER TABLE odds
    ADD CONSTRAINT uq_odds_entry
    UNIQUE (game_id, bookmaker, market_type, outcome_name, last_update);

-- ============================================================
-- 6. ENFORCE NOT NULL ON player_game_logs.player_id
--    Pre-check before running:
--      SELECT COUNT(*) FROM player_game_logs WHERE player_id IS NULL;
--    Only execute this block if that returns 0.
-- ============================================================
ALTER TABLE player_game_logs
    ALTER COLUMN player_id SET NOT NULL;

-- ============================================================
-- 7. COMPOSITE INDEX FOR PRIMARY PROP + STATS JOIN PATTERN
--    Speeds up the most common analytical query:
--      player_props pp
--      JOIN player_game_logs pgl ON pp.nba_player_id = pgl.player_id
--                                AND pp.game_id relates to pgl.nba_game_id
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_props_player_game
    ON player_props (nba_player_id, game_id);

-- ============================================================
-- 8. SCORES TABLE: promote game_id to PK, drop surrogate id
--    game_id is already UNIQUE — the serial id column is dead weight.
-- ============================================================
ALTER TABLE scores DROP CONSTRAINT IF EXISTS scores_game_id_key;
ALTER TABLE scores DROP CONSTRAINT IF EXISTS scores_pkey;
ALTER TABLE scores DROP COLUMN IF EXISTS id;
ALTER TABLE scores ADD PRIMARY KEY (game_id);
DROP INDEX IF EXISTS idx_scores_game_id;  -- superseded by the PK index
