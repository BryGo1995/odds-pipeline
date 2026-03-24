-- Raw layer: preserves full API responses for replayability
CREATE TABLE IF NOT EXISTS raw_api_responses (
    id          SERIAL PRIMARY KEY,
    endpoint    VARCHAR(50) NOT NULL,
    params      JSONB,
    response    JSONB,
    fetched_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    status      VARCHAR(20) NOT NULL DEFAULT 'success'
);

CREATE INDEX IF NOT EXISTS idx_raw_endpoint   ON raw_api_responses(endpoint);
CREATE INDEX IF NOT EXISTS idx_raw_fetched_at ON raw_api_responses(fetched_at);

-- Normalized layer
CREATE TABLE IF NOT EXISTS games (
    game_id         VARCHAR(100) PRIMARY KEY,
    home_team       VARCHAR(100),
    away_team       VARCHAR(100),
    commence_time   TIMESTAMP,
    season          VARCHAR(20),
    sport           VARCHAR(50),
    nba_game_id     TEXT
);

CREATE TABLE IF NOT EXISTS odds (
    id              SERIAL PRIMARY KEY,
    game_id         VARCHAR(100) REFERENCES games(game_id),
    bookmaker       VARCHAR(50),
    market_type     VARCHAR(50),
    outcome_name    VARCHAR(100),
    price           NUMERIC(10, 2),
    point           NUMERIC(10, 2),
    last_update     TIMESTAMP,
    fetched_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_odds_game_id   ON odds(game_id);
CREATE INDEX IF NOT EXISTS idx_odds_bookmaker ON odds(bookmaker);

CREATE TABLE IF NOT EXISTS player_props (
    id              SERIAL PRIMARY KEY,
    game_id         VARCHAR(100) REFERENCES games(game_id),
    bookmaker       VARCHAR(50),
    player_name     VARCHAR(100),
    prop_type       VARCHAR(100),
    outcome         VARCHAR(20),
    price           NUMERIC(10, 2),
    point           NUMERIC(10, 2),
    nba_player_id   INT REFERENCES players(player_id),
    last_update     TIMESTAMP,
    fetched_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_props_game_id ON player_props(game_id);
CREATE INDEX IF NOT EXISTS idx_props_player  ON player_props(player_name);
CREATE INDEX IF NOT EXISTS idx_props_type    ON player_props(prop_type);

CREATE TABLE IF NOT EXISTS scores (
    id              SERIAL PRIMARY KEY,
    game_id         VARCHAR(100) UNIQUE REFERENCES games(game_id),
    home_score      INTEGER,
    away_score      INTEGER,
    completed       BOOLEAN DEFAULT FALSE,
    last_update     TIMESTAMP,
    fetched_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scores_game_id ON scores(game_id);

-- NBA player stats tables
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
    id         SERIAL PRIMARY KEY,
    team_id    INT NOT NULL,
    team_name  TEXT,
    season     TEXT NOT NULL,
    pace       FLOAT,
    off_rating FLOAT,
    def_rating FLOAT,
    opp_pts_pg FLOAT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, season)
);

CREATE TABLE IF NOT EXISTS player_name_mappings (
    odds_api_name TEXT PRIMARY KEY,
    nba_player_id INT REFERENCES players(player_id),
    confidence    FLOAT,
    verified      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_games_nba_game_id       ON games (nba_game_id);
CREATE INDEX IF NOT EXISTS idx_player_props_nba_player ON player_props (nba_player_id);
