-- ad_result_snapshots — historical verdict checkpoints per ad.
--
-- The Ads Analyse "Category" column reflects an ad's LIFETIME metrics, so a
-- creative that won early and then decayed reads as whatever it is today.
-- These two checkpoints freeze the inputs at the moments people actually
-- judge a test by:
--
-- Metrics come from backfill_table UNION primary_table deduped per
-- (ad_id, date) with MAX — the same pairing ae_table_view reads, so these
-- verdicts and the live Category column are computed off the same numbers.
--
--   d14_*  metrics accumulated over the ad's first 14 days
--          (ad_created_date .. ad_created_date + 14, inclusive — same window
--           as result_classifier.py's WINDOW_DAYS)
--   k50_*  metrics accumulated up to and including the day the ad's
--          cumulative impressions first crossed 50,000
--
-- We store the raw METRICS, not a verdict label, so the dashboard recomputes
-- both labels through the same F1-F4 threshold inputs as the live Category
-- column. Change a threshold and the history re-reads consistently.

CREATE TABLE IF NOT EXISTS public.ad_result_snapshots (
    -- Keyed on ad_id alone, matching ae_table_view's grain (it groups by
    -- ad_id and carries the latest account_name), so the dashboard can map
    -- snapshots onto table rows by ad_id with no ambiguity.
    ad_id                 TEXT        NOT NULL,
    account_name          TEXT,
    ad_name               TEXT,
    ad_created_date       DATE        NOT NULL,

    -- ── first-14-days checkpoint ──────────────────────────────────────
    d14_end_date          DATE,       -- ad_created_date + 14
    -- FALSE while the ad is still inside its first 14 days: the window has
    -- not closed, so the verdict is not yet a verdict. The UI shows "—".
    d14_complete          BOOLEAN     NOT NULL DEFAULT FALSE,
    d14_impressions       BIGINT      NOT NULL DEFAULT 0,
    d14_spend             NUMERIC(14,2) NOT NULL DEFAULT 0,
    d14_conv_value        NUMERIC(14,2) NOT NULL DEFAULT 0,
    d14_ncp_count         BIGINT      NOT NULL DEFAULT 0,
    d14_ftewv_count       BIGINT      NOT NULL DEFAULT 0,
    d14_roas              NUMERIC(12,4),
    d14_cost_per_ncp      NUMERIC(12,2),
    d14_cost_per_ftewv    NUMERIC(12,2),

    -- ── 50k-impressions checkpoint ────────────────────────────────────
    -- NULL crossed_at = the ad never reached 50,000 impressions.
    k50_crossed_at        DATE,
    k50_days_to_cross     INT,
    k50_impressions       BIGINT,
    k50_spend             NUMERIC(14,2),
    k50_conv_value        NUMERIC(14,2),
    k50_ncp_count         BIGINT,
    k50_ftewv_count       BIGINT,
    k50_roas              NUMERIC(12,4),
    k50_cost_per_ncp      NUMERIC(12,2),
    k50_cost_per_ftewv    NUMERIC(12,2),

    last_computed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (ad_id)
);

CREATE INDEX IF NOT EXISTS ad_result_snapshots_created_ix
    ON public.ad_result_snapshots (ad_created_date);

-- The dashboard reads this with the anon key, same as ae_table_view.
ALTER TABLE public.ad_result_snapshots ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
         WHERE schemaname = 'public'
           AND tablename  = 'ad_result_snapshots'
           AND policyname = 'ad_result_snapshots_anon_read'
    ) THEN
        CREATE POLICY ad_result_snapshots_anon_read
            ON public.ad_result_snapshots
            FOR SELECT USING (true);
    END IF;
END $$;

GRANT SELECT ON public.ad_result_snapshots TO anon, authenticated;
