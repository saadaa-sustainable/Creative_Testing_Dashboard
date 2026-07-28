-- Extend get_ireach_incremental_analysis to support level_arg='ad'.
-- Formula (unchanged, same as campaign/adset/account):
--   incr_reach       = MAX(0, cum_at_end - cum_at_start_prev)
--   cum_at_start_prev= cumulative_reach on the last day BEFORE from_date
--   cum_at_end       = cumulative_reach on the last day ON or BEFORE to_date
--   spend            = SUM of daily spend across the window
--   cost_per_1k_incr = spend * 1000 / incr_reach   (NULL when incr_reach = 0)
--
-- For level='ad', spend comes from primary_table ∪ backfill_table with
-- primary-wins dedup on (ad_id, date) — Meta doesn't publish a separate
-- ireach_ad_daily table (the /insights level=ad endpoint just returns raw
-- per-day reach, which we already store in primary_table.reach).

CREATE OR REPLACE FUNCTION public.get_ireach_incremental_analysis(
  from_date date,
  to_date   date,
  level_arg text DEFAULT 'campaign'
)
RETURNS TABLE(
  entity_id         text,
  entity_name       text,
  account_name      text,
  n_days            integer,
  cum_at_start_prev bigint,
  cum_at_end        bigint,
  incr_reach        bigint,
  spend             numeric,
  cost_per_1k_incr  numeric
)
LANGUAGE sql STABLE AS $$
  WITH
  -- 1. Cumulative reach on the last known day BEFORE from_date (per entity).
  cum_start AS (
    SELECT DISTINCT ON (entity_id)
      entity_id, entity_name, account_name,
      cumulative_reach AS cum_at_start_prev
    FROM public.ireach_cumulative_daily
    WHERE level = level_arg AND date < from_date
    ORDER BY entity_id, date DESC
  ),
  -- 2. Cumulative reach on the last day ON or BEFORE to_date (per entity).
  cum_end AS (
    SELECT DISTINCT ON (entity_id)
      entity_id, entity_name, account_name,
      cumulative_reach AS cum_at_end,
      date             AS end_date
    FROM public.ireach_cumulative_daily
    WHERE level = level_arg AND date >= from_date AND date <= to_date
    ORDER BY entity_id, date DESC
  ),
  -- 3. Spend + delivering-day count per entity for the window.
  --    Split by level because each level has its own source table.
  spend_camp AS (
    SELECT campaign_id::text AS entity_id,
           SUM(spend_daily)::numeric AS spend,
           COUNT(DISTINCT date)::int AS n_days
    FROM public.ireach_campaign_daily
    WHERE level_arg = 'campaign'
      AND date BETWEEN from_date AND to_date
    GROUP BY campaign_id
  ),
  spend_adset AS (
    SELECT adset_id::text AS entity_id,
           SUM(spend_daily)::numeric AS spend,
           COUNT(DISTINCT date)::int AS n_days
    FROM public.ireach_adset_daily
    WHERE level_arg = 'adset'
      AND date BETWEEN from_date AND to_date
    GROUP BY adset_id
  ),
  spend_account AS (
    SELECT account_id::text AS entity_id,
           SUM(spend_daily)::numeric AS spend,
           COUNT(DISTINCT date)::int AS n_days
    FROM public.ireach_campaign_daily
    WHERE level_arg = 'account'
      AND date BETWEEN from_date AND to_date
    GROUP BY account_id
  ),
  -- 3d. AD-level spend from primary_table ∪ backfill_table with
  --     primary-wins dedup on (ad_id, date). Only evaluated when level='ad'.
  spend_ad_union AS (
    SELECT ad_id::text AS entity_id, date,
           amount_spent_inr::numeric AS spend, 1 AS priority
      FROM public.primary_table
     WHERE level_arg = 'ad'
       AND ad_id IS NOT NULL AND ad_id <> ''
       AND date BETWEEN from_date AND to_date
    UNION ALL
    SELECT ad_id::text, date, amount_spent_inr::numeric, 2
      FROM public.backfill_table
     WHERE level_arg = 'ad'
       AND ad_id IS NOT NULL AND ad_id <> ''
       AND date BETWEEN from_date AND to_date
  ),
  spend_ad_dedup AS (
    SELECT DISTINCT ON (entity_id, date) entity_id, date, spend
      FROM spend_ad_union ORDER BY entity_id, date, priority
  ),
  spend_ad AS (
    SELECT entity_id,
           SUM(spend)::numeric AS spend,
           COUNT(DISTINCT date)::int AS n_days
      FROM spend_ad_dedup GROUP BY entity_id
  ),
  spend_all AS (
    SELECT * FROM spend_camp
    UNION ALL SELECT * FROM spend_adset
    UNION ALL SELECT * FROM spend_account
    UNION ALL SELECT * FROM spend_ad
  )
  SELECT
    e.entity_id,
    e.entity_name,
    e.account_name,
    COALESCE(sp.n_days, 0)                                              AS n_days,
    COALESCE(s.cum_at_start_prev, 0)::bigint                            AS cum_at_start_prev,
    e.cum_at_end::bigint                                                AS cum_at_end,
    GREATEST(0, e.cum_at_end - COALESCE(s.cum_at_start_prev, 0))::bigint AS incr_reach,
    COALESCE(sp.spend, 0)::numeric                                      AS spend,
    CASE
      WHEN GREATEST(0, e.cum_at_end - COALESCE(s.cum_at_start_prev, 0)) > 0
      THEN (COALESCE(sp.spend, 0) * 1000.0
           / GREATEST(0, e.cum_at_end - COALESCE(s.cum_at_start_prev, 0)))::numeric
      ELSE NULL
    END                                                                 AS cost_per_1k_incr
  FROM cum_end e
  LEFT JOIN cum_start  s USING (entity_id)
  LEFT JOIN spend_all sp USING (entity_id);
$$;

GRANT EXECUTE ON FUNCTION public.get_ireach_incremental_analysis(date, date, text) TO anon, authenticated, service_role;
