-- sql/migrations/005_recommendation_outcomes.sql
-- Migration 005: add outcome tracking columns to recommendations
-- Idempotent: safe to run multiple times.

ALTER TABLE recommendations
  ADD COLUMN IF NOT EXISTS actual_result      BOOLEAN       NULL,
  ADD COLUMN IF NOT EXISTS actual_stat_value  NUMERIC(10,2) NULL,
  ADD COLUMN IF NOT EXISTS settled_at         TIMESTAMPTZ   NULL;

CREATE INDEX IF NOT EXISTS idx_recs_settled ON recommendations (game_date, settled_at)
  WHERE settled_at IS NULL;
