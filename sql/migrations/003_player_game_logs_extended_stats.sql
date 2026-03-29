-- Migration 003: Add extended shooting, turnover, and foul columns to player_game_logs
-- Idempotent: uses ADD COLUMN IF NOT EXISTS

ALTER TABLE player_game_logs
    ADD COLUMN IF NOT EXISTS fgm     INT,
    ADD COLUMN IF NOT EXISTS fg_pct  FLOAT,
    ADD COLUMN IF NOT EXISTS fg3m    INT,
    ADD COLUMN IF NOT EXISTS fg3a    INT,
    ADD COLUMN IF NOT EXISTS fg3_pct FLOAT,
    ADD COLUMN IF NOT EXISTS ftm     INT,
    ADD COLUMN IF NOT EXISTS ft_pct  FLOAT,
    ADD COLUMN IF NOT EXISTS oreb    INT,
    ADD COLUMN IF NOT EXISTS dreb    INT,
    ADD COLUMN IF NOT EXISTS tov     INT,
    ADD COLUMN IF NOT EXISTS pf      INT;
