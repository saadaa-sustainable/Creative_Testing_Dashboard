-- Migration: ad_results lifecycle table
-- Date: 2026-05-09
-- Purpose: track per-ad lifecycle status using a 14-day buffer from ad_created_date
--   Winner          → impressions_14d ≥ 50,000 (locked in once crossed)
--   Result Pending  → today ≤ evaluation_end_date AND impressions_14d < 50,000
--   Failed          → today > evaluation_end_date AND impressions_14d < 50,000

CREATE TABLE IF NOT EXISTS ad_results (
    account_name         TEXT        NOT NULL,
    ad_id                TEXT        NOT NULL,
    ad_name              TEXT,
    campaign_name        TEXT,
    ad_status            TEXT,
    ad_created_date      DATE,
    evaluation_end_date  DATE,
    impressions_14d      BIGINT      DEFAULT 0,
    crossed_threshold_at DATE,
    result_status        TEXT        NOT NULL,
    last_computed_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (account_name, ad_id)
);

CREATE INDEX IF NOT EXISTS idx_ad_results_status
    ON ad_results (result_status);

CREATE INDEX IF NOT EXISTS idx_ad_results_account_status
    ON ad_results (account_name, result_status);

CREATE INDEX IF NOT EXISTS idx_ad_results_eval_end
    ON ad_results (evaluation_end_date);

CREATE INDEX IF NOT EXISTS idx_ad_results_created
    ON ad_results (ad_created_date);
