-- Migration 007: Add full box score columns to team_game_logs (#38)
-- Opponent-strength features (#39) derive pace, defensive rating and
-- stats allowed from these, point-in-time per game.
-- Idempotent: uses ADD COLUMN IF NOT EXISTS

ALTER TABLE team_game_logs
    ADD COLUMN IF NOT EXISTS min  INT,
    ADD COLUMN IF NOT EXISTS fgm  INT,
    ADD COLUMN IF NOT EXISTS fga  INT,
    ADD COLUMN IF NOT EXISTS fg3m INT,
    ADD COLUMN IF NOT EXISTS fg3a INT,
    ADD COLUMN IF NOT EXISTS ftm  INT,
    ADD COLUMN IF NOT EXISTS fta  INT,
    ADD COLUMN IF NOT EXISTS oreb INT,
    ADD COLUMN IF NOT EXISTS dreb INT,
    ADD COLUMN IF NOT EXISTS reb  INT,
    ADD COLUMN IF NOT EXISTS ast  INT,
    ADD COLUMN IF NOT EXISTS stl  INT,
    ADD COLUMN IF NOT EXISTS blk  INT,
    ADD COLUMN IF NOT EXISTS tov  INT,
    ADD COLUMN IF NOT EXISTS pf   INT;
