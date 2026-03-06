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
    sport           VARCHAR(50)
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
