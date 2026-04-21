-- Migration 006: MLB foundation tables + sport-scoping of shared tables
-- Idempotent: safe to run multiple times.

-- ---------------------------------------------------------------------------
-- MLB-specific tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS mlb_players (
    player_id         INT PRIMARY KEY,
    full_name         TEXT NOT NULL,
    position          TEXT,
    bats              TEXT,
    throws            TEXT,
    team_id           INT,
    team_abbreviation TEXT,
    is_active         BOOLEAN,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mlb_players_full_name ON mlb_players (full_name);

CREATE TABLE IF NOT EXISTS mlb_player_game_logs (
    id                SERIAL PRIMARY KEY,
    player_id         INT REFERENCES mlb_players(player_id),
    mlb_game_pk       TEXT NOT NULL,
    season            TEXT NOT NULL,
    game_date         DATE NOT NULL,
    matchup           TEXT,
    team_id           INT,
    opponent_team_id  INT,
    plate_appearances INT,
    at_bats           INT,
    hits              INT,
    doubles           INT,
    triples           INT,
    home_runs         INT,
    rbi               INT,
    runs              INT,
    walks             INT,
    strikeouts        INT,
    stolen_bases      INT,
    total_bases       INT,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, mlb_game_pk)
);

CREATE INDEX IF NOT EXISTS idx_mlb_pgl_player_id ON mlb_player_game_logs (player_id);
CREATE INDEX IF NOT EXISTS idx_mlb_pgl_game_date ON mlb_player_game_logs (game_date);
CREATE INDEX IF NOT EXISTS idx_mlb_pgl_season    ON mlb_player_game_logs (season);

CREATE TABLE IF NOT EXISTS mlb_teams (
    team_id      INT PRIMARY KEY,
    full_name    TEXT NOT NULL,
    abbreviation TEXT NOT NULL,
    league       TEXT,
    division     TEXT,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mlb_player_name_mappings (
    odds_api_name TEXT PRIMARY KEY,
    mlb_player_id INT REFERENCES mlb_players(player_id),
    confidence    FLOAT,
    verified      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Widen player_props with mlb_player_id (one of {nba_player_id, mlb_player_id}
-- is non-null per row; games.sport tells queries which to read)
-- ---------------------------------------------------------------------------

ALTER TABLE player_props
  ADD COLUMN IF NOT EXISTS mlb_player_id INT REFERENCES mlb_players(player_id);

CREATE INDEX IF NOT EXISTS idx_player_props_mlb_player ON player_props (mlb_player_id);

-- ---------------------------------------------------------------------------
-- Scope recommendations by sport
-- Add column with DEFAULT 'NBA' (backfills existing rows), then drop the
-- default so future inserts must specify sport explicitly.
-- ---------------------------------------------------------------------------

ALTER TABLE recommendations
  ADD COLUMN IF NOT EXISTS sport VARCHAR(20) NOT NULL DEFAULT 'NBA';

ALTER TABLE recommendations
  ALTER COLUMN sport DROP DEFAULT;

CREATE INDEX IF NOT EXISTS idx_recs_sport_date ON recommendations (sport, game_date);
