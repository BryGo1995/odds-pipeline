-- Migration 001: Add NBA player stats tables
-- Idempotent: safe to run multiple times

CREATE TABLE IF NOT EXISTS players (
    player_id         INT PRIMARY KEY,
    full_name         TEXT NOT NULL,
    position          TEXT,
    team_id           INT,
    team_abbreviation TEXT,
    is_active         BOOLEAN,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_players_full_name ON players (full_name);

CREATE TABLE IF NOT EXISTS player_game_logs (
    id               SERIAL PRIMARY KEY,
    player_id        INT REFERENCES players(player_id),
    nba_game_id      TEXT NOT NULL,
    season           TEXT NOT NULL,
    game_date        DATE NOT NULL,
    matchup          TEXT,
    team_id          INT,
    wl               TEXT,
    min              FLOAT,
    fga              INT,
    fta              INT,
    usg_pct          FLOAT,
    pts              INT,
    reb              INT,
    ast              INT,
    blk              INT,
    stl              INT,
    plus_minus       INT,
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, nba_game_id)
);

CREATE INDEX IF NOT EXISTS idx_pgl_player_id   ON player_game_logs (player_id);
CREATE INDEX IF NOT EXISTS idx_pgl_game_date   ON player_game_logs (game_date);
CREATE INDEX IF NOT EXISTS idx_pgl_season      ON player_game_logs (season);

CREATE TABLE IF NOT EXISTS team_game_logs (
    id                SERIAL PRIMARY KEY,
    team_id           INT NOT NULL,
    team_abbreviation TEXT,
    nba_game_id       TEXT NOT NULL,
    season            TEXT NOT NULL,
    game_date         DATE NOT NULL,
    matchup           TEXT,
    wl                TEXT,
    pts               INT,
    plus_minus        INT,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, nba_game_id)
);

CREATE INDEX IF NOT EXISTS idx_tgl_team_id   ON team_game_logs (team_id);
CREATE INDEX IF NOT EXISTS idx_tgl_game_date ON team_game_logs (game_date);

CREATE TABLE IF NOT EXISTS team_season_stats (
    id                SERIAL PRIMARY KEY,
    team_id           INT NOT NULL,
    team_abbreviation TEXT,
    season            TEXT NOT NULL,
    pace              FLOAT,
    off_rating        FLOAT,
    def_rating        FLOAT,
    opp_pts_paint_pg  FLOAT,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, season)
);

CREATE TABLE IF NOT EXISTS player_name_mappings (
    odds_api_name TEXT PRIMARY KEY,
    nba_player_id INT REFERENCES players(player_id),
    confidence    FLOAT,
    verified      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Modify existing tables
ALTER TABLE games ADD COLUMN IF NOT EXISTS nba_game_id TEXT;
ALTER TABLE player_props ADD COLUMN IF NOT EXISTS nba_player_id INT REFERENCES players(player_id);

CREATE INDEX IF NOT EXISTS idx_games_nba_game_id       ON games (nba_game_id);
CREATE INDEX IF NOT EXISTS idx_player_props_nba_player ON player_props (nba_player_id);
