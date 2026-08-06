-- Speed up get_ae_metrics_by_window so 90-day windows return under the
-- PostgREST anon statement_timeout. Previously live-aggregated primary_table
-- (~765k rows, ~63s) — now reads from ae_daily_90d (~194k rows, pre-dedup'd
-- primary/backfill grid, ~1-3s). Falls back to live aggregation for windows
-- that reach older than ae_daily_90d's 90-day coverage.
--
-- Semantics preserved: only ads with SUM(impressions) > 0 in the window are
-- returned; days_active counts only days where the ad actually delivered.

CREATE OR REPLACE FUNCTION public.get_ae_metrics_by_window(from_date date, to_date date)
RETURNS TABLE(
  ad_id       text,
  days_active integer,
  impressions bigint,
  reach_sum   bigint,
  reach_peak  bigint,
  spend       numeric,
  purchases   numeric,
  conv_value  numeric,
  link_clicks bigint,
  ftewv       numeric,
  ncp         numeric
)
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET statement_timeout TO '60s'
AS $function$
DECLARE
  min_covered_date date;
BEGIN
  SELECT MIN(date) INTO min_covered_date FROM public.ae_daily_90d;

  IF min_covered_date IS NOT NULL AND from_date >= min_covered_date THEN
    -- Fast path: window fits within pre-aggregated ae_daily_90d
    RETURN QUERY
    SELECT
      d.ad_id::text,
      COUNT(*) FILTER (WHERE d.impressions > 0)::int  AS days_active,
      COALESCE(SUM(d.impressions),      0)::bigint    AS impressions,
      COALESCE(SUM(d.reach),            0)::bigint    AS reach_sum,
      COALESCE(MAX(d.reach),            0)::bigint    AS reach_peak,
      COALESCE(SUM(d.amount_spent),     0)::numeric   AS spend,
      COALESCE(SUM(d.purchases),        0)::numeric   AS purchases,
      COALESCE(SUM(d.conversion_value), 0)::numeric   AS conv_value,
      COALESCE(SUM(d.link_clicks_raw),  0)::bigint    AS link_clicks,
      COALESCE(SUM(d.ftewv_count),      0)::numeric   AS ftewv,
      COALESCE(SUM(d.ncp_count),        0)::numeric   AS ncp
    FROM public.ae_daily_90d d
    WHERE d.date BETWEEN from_date AND to_date
    GROUP BY d.ad_id
    HAVING SUM(d.impressions) > 0;
  ELSE
    -- Slow path: live-aggregate primary + backfill for windows older than
    -- ae_daily_90d's coverage. Rare — user has to pick a custom range that
    -- reaches past 90 days ago.
    RETURN QUERY
    WITH u AS (
      SELECT ad_id::text AS ad_id, date, impressions, reach,
             amount_spent_inr,
             purchases::numeric                         AS purchases,
             conversion_value::numeric                  AS conv_value,
             COALESCE(inline_link_clicks, outbound_clicks)::bigint AS link_clicks,
             ftewv_count::numeric                       AS ftewv_count,
             ncp_count::numeric                         AS ncp_count,
             1 AS pri
        FROM public.primary_table
       WHERE ad_id IS NOT NULL
         AND impressions IS NOT NULL AND impressions > 0
         AND date BETWEEN from_date AND to_date
      UNION ALL
      SELECT ad_id::text, date, impressions, reach,
             amount_spent_inr,
             NULL::numeric,
             conversion_value::numeric,
             outbound_clicks::bigint,
             ftewv_count::numeric,
             ncp_count::numeric,
             2 AS pri
        FROM public.backfill_table
       WHERE ad_id IS NOT NULL
         AND impressions IS NOT NULL AND impressions > 0
         AND date BETWEEN from_date AND to_date
    ),
    dedup AS (
      SELECT DISTINCT ON (u.ad_id, u.date)
        u.ad_id, u.date, u.impressions, u.reach, u.amount_spent_inr,
        u.purchases, u.conv_value, u.link_clicks, u.ftewv_count, u.ncp_count
      FROM u
      ORDER BY u.ad_id, u.date, u.pri
    )
    SELECT
      dedup.ad_id,
      COUNT(*)::int                    AS days_active,
      SUM(dedup.impressions)::bigint   AS impressions,
      SUM(dedup.reach)::bigint         AS reach_sum,
      MAX(dedup.reach)::bigint         AS reach_peak,
      SUM(dedup.amount_spent_inr)::numeric AS spend,
      SUM(dedup.purchases)::numeric    AS purchases,
      SUM(dedup.conv_value)::numeric   AS conv_value,
      SUM(dedup.link_clicks)::bigint   AS link_clicks,
      SUM(dedup.ftewv_count)::numeric  AS ftewv,
      SUM(dedup.ncp_count)::numeric    AS ncp
    FROM dedup
    GROUP BY dedup.ad_id;
  END IF;
END;
$function$;
