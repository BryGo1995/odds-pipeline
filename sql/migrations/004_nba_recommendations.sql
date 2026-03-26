-- Migration 004: recommendations table
-- Output of nba_score_dag — one row per player/prop/bookmaker per game_date.
-- Idempotent: safe to run multiple times.

CREATE TABLE IF NOT EXISTS recommendations (
    id            SERIAL       PRIMARY KEY,
    player_name   VARCHAR(100) NOT NULL,
    prop_type     VARCHAR(100) NOT NULL,
    bookmaker     VARCHAR(50)  NOT NULL,
    line          NUMERIC(10, 2),
    outcome       VARCHAR(20),
    model_prob    NUMERIC(6, 4),
    implied_prob  NUMERIC(6, 4),
    edge          NUMERIC(6, 4),
    rank          INT,
    model_version VARCHAR(50),
    game_date     DATE         NOT NULL,
    scored_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_recs_game_date ON recommendations (game_date);
CREATE INDEX IF NOT EXISTS idx_recs_edge      ON recommendations (game_date, edge DESC);
