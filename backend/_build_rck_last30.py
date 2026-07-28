"""Build public.rck_last30 — per-ad last-30-day aggregates for every ad in a
campaign with 'RCK' in its name, regardless of current ad_status. Includes
paused / archived / with_issues ads that had ANY delivery in the window.

Idempotent DROP + CREATE. RLS enabled.
"""
import os, sys, io, psycopg2
from dotenv import load_dotenv
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
load_dotenv(override=True)
conn = psycopg2.connect(os.environ['SUPABASE_DB_URL'], connect_timeout=30, keepalives=1)
conn.autocommit = False

DDL = """
DROP TABLE IF EXISTS public.rck_last30;

CREATE TABLE public.rck_last30 (
    ad_id             text PRIMARY KEY,
    account_name      text,
    ad_name           text,
    ad_status         text,
    campaign_id       text,
    campaign_name     text,
    adset_id          text,
    adset_name        text,
    asset_id          text,
    first_seen_date   date,
    last_seen_date    date,
    days_active       integer,
    -- delivery
    impressions       bigint,
    reach             bigint,
    frequency         numeric(10,4),
    amount_spent      numeric(14,2),
    cost_per_1000     numeric(14,2),
    cpc_link          numeric(14,2),
    ctr_pct           numeric(10,4),
    outbound_clicks   bigint,
    link_clicks_raw   bigint,
    thruplays         bigint,
    three_sec_plays   bigint,
    post_engagements  bigint,
    engagement_count  bigint,
    video_play_time   numeric(14,2),
    -- conversions
    conv_value        numeric(14,2),
    purchases         numeric(14,2),
    ci_count          numeric(14,2),
    atc_count         numeric(14,2),
    roas_ma           numeric(10,4),
    checkout_compl_pct numeric(10,4),
    cr_link_clicks_pct numeric(10,4),
    atc_lc_pct        numeric(10,4),
    ci_atc_pct        numeric(10,4),
    ftewv_count       numeric(14,2),
    cost_per_ftewv    numeric(14,2),
    ncp_count         numeric(14,2),
    cost_per_ncp      numeric(14,2),
    ltv_reach         bigint,
    ltv_frequency     numeric(10,4),
    -- Shopify (L30 attribution)
    shopify_orders    integer NOT NULL DEFAULT 0,
    shopify_sales     numeric(14,2) NOT NULL DEFAULT 0,
    shopify_roas      numeric(10,4),
    shop_minus_meta   numeric(14,2),
    shop_vs_meta_pct  numeric(10,4),
    -- UTM / matched modes
    utm_content_top   text,
    utm_term_top      text,
    utm_campaign_top  text,
    matched_value_top text,
    matched_tier_top  text,
    refreshed_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX rck_last30_campaign_idx ON public.rck_last30 (campaign_name);
CREATE INDEX rck_last30_status_idx   ON public.rck_last30 (ad_status);

ALTER TABLE public.rck_last30 ENABLE ROW LEVEL SECURITY;
-- No policies -> only service_role reads (Apps Script pattern)
"""

INSERT_SQL = """
INSERT INTO public.rck_last30
WITH w AS (
  SELECT (CURRENT_DATE - 30)::date AS start_dt,
         (CURRENT_DATE)::date      AS end_dt
),
-- Any ad whose CAMPAIGN name contains RCK AND had a row in the window
scope AS (
  SELECT DISTINCT ad_id
    FROM (
      SELECT ad_id FROM public.primary_table, w
       WHERE campaign_name ILIKE '%%RCK%%'
         AND date BETWEEN w.start_dt AND w.end_dt
         AND ad_id IS NOT NULL AND ad_id <> ''
      UNION
      SELECT ad_id FROM public.backfill_table, w
       WHERE campaign_name ILIKE '%%RCK%%'
         AND date BETWEEN w.start_dt AND w.end_dt
         AND ad_id IS NOT NULL AND ad_id <> ''
    ) x
),
-- Latest metadata per ad_id (primary wins)
meta AS (
  SELECT DISTINCT ON (ad_id) ad_id, ad_name, ad_status, account_name,
         campaign_id, campaign_name, adset_id, adset_name
    FROM public.primary_table
   WHERE ad_id IN (SELECT ad_id FROM scope)
   ORDER BY ad_id, date DESC
),
-- Daily row union with primary-wins dedup
combined AS (
  SELECT ad_id, date, amount_spent_inr, conversion_value, impressions, reach,
         purchases, inline_link_clicks, initiate_checkout, add_to_cart,
         ftewv_count, ncp_count, outbound_clicks, thruplays, three_sec_video_plays,
         post_engagements, engagement_count, video_play_time, ltv_reach, ltv_frequency,
         1 AS pr
    FROM public.primary_table, w
   WHERE ad_id IN (SELECT ad_id FROM scope) AND date BETWEEN w.start_dt AND w.end_dt
  UNION ALL
  SELECT ad_id, date, amount_spent_inr, conversion_value, impressions, reach,
         NULL::numeric AS purchases,
         NULL::bigint  AS inline_link_clicks,
         NULL::numeric AS initiate_checkout,
         NULL::numeric AS add_to_cart,
         ftewv_count, ncp_count, outbound_clicks, thruplays, three_sec_video_plays,
         post_engagements,
         NULL::bigint AS engagement_count,
         video_play_time, ltv_reach, ltv_frequency,
         2 AS pr
    FROM public.backfill_table, w
   WHERE ad_id IN (SELECT ad_id FROM scope) AND date BETWEEN w.start_dt AND w.end_dt
),
dedup AS (
  SELECT DISTINCT ON (ad_id, date) *
    FROM combined ORDER BY ad_id, date, pr
),
agg AS (
  SELECT ad_id,
         COUNT(DISTINCT date)                                 AS days_active,
         MIN(date)                                            AS first_seen_date,
         MAX(date)                                            AS last_seen_date,
         SUM(impressions)::bigint                             AS impressions,
         MAX(reach)::bigint                                   AS reach,
         SUM(amount_spent_inr)::numeric(14,2)                 AS amount_spent,
         SUM(outbound_clicks)::bigint                         AS outbound_clicks,
         SUM(inline_link_clicks)::bigint                      AS link_clicks_raw,
         SUM(thruplays)::bigint                               AS thruplays,
         SUM(three_sec_video_plays)::bigint                   AS three_sec_plays,
         SUM(post_engagements)::bigint                        AS post_engagements,
         SUM(engagement_count)::bigint                        AS engagement_count,
         SUM(video_play_time)::numeric(14,2)                  AS video_play_time,
         SUM(conversion_value)::numeric(14,2)                 AS conv_value,
         SUM(purchases)::numeric(14,2)                        AS purchases,
         SUM(initiate_checkout)::numeric(14,2)                AS ci_count,
         SUM(add_to_cart)::numeric(14,2)                      AS atc_count,
         SUM(ftewv_count)::numeric(14,2)                      AS ftewv_count,
         SUM(ncp_count)::numeric(14,2)                        AS ncp_count,
         SUM(ltv_reach)::bigint                               AS ltv_reach,
         AVG(ltv_frequency)::numeric(10,4)                    AS ltv_frequency
    FROM dedup GROUP BY ad_id
),
shop AS (
  SELECT ad_id,
         COUNT(*)::int                                    AS shopify_orders,
         COALESCE(SUM(total_price),0)::numeric(14,2)      AS shopify_sales,
         MODE() WITHIN GROUP (ORDER BY utm_content)   FILTER (WHERE utm_content   IS NOT NULL AND utm_content   <> '') AS utm_content_top,
         MODE() WITHIN GROUP (ORDER BY utm_term)      FILTER (WHERE utm_term      IS NOT NULL AND utm_term      <> '') AS utm_term_top,
         MODE() WITHIN GROUP (ORDER BY utm_campaign)  FILTER (WHERE utm_campaign  IS NOT NULL AND utm_campaign  <> '') AS utm_campaign_top,
         MODE() WITHIN GROUP (ORDER BY matched_value) FILTER (WHERE matched_value IS NOT NULL AND matched_value <> '') AS matched_value_top,
         MODE() WITHIN GROUP (ORDER BY matched_tier)  FILTER (WHERE matched_tier  IS NOT NULL) AS matched_tier_top
    FROM public.shopify_ad_attribution, w
   WHERE ad_id IN (SELECT ad_id FROM scope)
     AND order_created_at::date BETWEEN w.start_dt AND w.end_dt
   GROUP BY ad_id
)
SELECT
    m.ad_id, m.account_name, m.ad_name, m.ad_status,
    m.campaign_id, m.campaign_name, m.adset_id, m.adset_name,
    aa.asset_id,
    a.first_seen_date, a.last_seen_date, a.days_active,

    a.impressions, a.reach,
    CASE WHEN a.reach > 0 THEN (a.impressions::numeric / a.reach)::numeric(10,4) ELSE 0 END AS frequency,
    a.amount_spent,
    CASE WHEN a.reach > 0 THEN (a.amount_spent / a.reach * 1000)::numeric(14,2) ELSE 0 END AS cost_per_1000,
    CASE WHEN a.link_clicks_raw > 0 THEN (a.amount_spent / a.link_clicks_raw)::numeric(14,2) ELSE 0 END AS cpc_link,
    CASE WHEN a.impressions > 0 THEN (a.link_clicks_raw::numeric / a.impressions * 100)::numeric(10,4) ELSE 0 END AS ctr_pct,
    a.outbound_clicks, a.link_clicks_raw, a.thruplays, a.three_sec_plays,
    a.post_engagements, a.engagement_count, a.video_play_time,

    a.conv_value, a.purchases, a.ci_count, a.atc_count,
    CASE WHEN a.amount_spent > 0 THEN (a.conv_value / a.amount_spent)::numeric(10,4) ELSE 0 END AS roas_ma,
    CASE WHEN a.ci_count > 0 THEN (a.purchases / a.ci_count * 100)::numeric(10,4) ELSE 0 END AS checkout_compl_pct,
    CASE WHEN a.link_clicks_raw > 0 THEN (a.purchases / a.link_clicks_raw * 100)::numeric(10,4) ELSE 0 END AS cr_link_clicks_pct,
    CASE WHEN a.link_clicks_raw > 0 THEN (a.atc_count::numeric / a.link_clicks_raw * 100)::numeric(10,4) ELSE 0 END AS atc_lc_pct,
    CASE WHEN a.atc_count > 0 THEN (a.ci_count::numeric / a.atc_count * 100)::numeric(10,4) ELSE 0 END AS ci_atc_pct,
    a.ftewv_count,
    CASE WHEN a.ftewv_count > 0 THEN (a.amount_spent / a.ftewv_count)::numeric(14,2) ELSE 0 END AS cost_per_ftewv,
    a.ncp_count,
    CASE WHEN a.ncp_count > 0 THEN (a.amount_spent / a.ncp_count)::numeric(14,2) ELSE 0 END AS cost_per_ncp,
    a.ltv_reach, a.ltv_frequency,

    COALESCE(s.shopify_orders, 0)                                AS shopify_orders,
    COALESCE(s.shopify_sales,  0)                                AS shopify_sales,
    CASE WHEN a.amount_spent > 0
         THEN (COALESCE(s.shopify_sales,0) / a.amount_spent)::numeric(10,4)
         ELSE NULL END                                            AS shopify_roas,
    CASE WHEN a.conv_value > 0
         THEN (COALESCE(s.shopify_sales,0) - a.conv_value)::numeric(14,2)
         ELSE NULL END                                            AS shop_minus_meta,
    CASE WHEN a.conv_value > 0
         THEN ((COALESCE(s.shopify_sales,0) - a.conv_value) / a.conv_value * 100)::numeric(10,4)
         ELSE NULL END                                            AS shop_vs_meta_pct,

    s.utm_content_top, s.utm_term_top, s.utm_campaign_top,
    s.matched_value_top, s.matched_tier_top,
    now() AS refreshed_at

  FROM meta m
  LEFT JOIN agg a USING (ad_id)
  LEFT JOIN shop s USING (ad_id)
  LEFT JOIN public.ad_asset_ids aa USING (ad_id)
 ORDER BY a.amount_spent DESC NULLS LAST;
"""

try:
    with conn.cursor() as cur:
        print("[1/2] Drop + create public.rck_last30 ...")
        cur.execute(DDL)
        print("[2/2] Populating ...")
        cur.execute(INSERT_SQL)
        n = cur.rowcount
        print(f"      rows inserted: {n:,}")
        cur.execute("""SELECT ad_status, COUNT(*), SUM(amount_spent)::numeric(14,2)
                         FROM public.rck_last30 GROUP BY ad_status ORDER BY 2 DESC;""")
        print("\n      ad_status distribution:")
        for st, cnt, spend in cur.fetchall():
            print(f"        {(st or '<null>'):<18} {cnt:>4,}  spend=Rs{spend or 0:,.0f}")
        cur.execute("""SELECT campaign_name, COUNT(*), SUM(amount_spent)::numeric(14,2), SUM(shopify_orders)
                         FROM public.rck_last30 GROUP BY campaign_name ORDER BY 3 DESC NULLS LAST;""")
        print("\n      per RCK campaign:")
        for cn, cnt, spend, ord_ in cur.fetchall():
            print(f"        {(cn or '?')[:60]:<62}  ads={cnt:>3}  spend=Rs{spend or 0:,.0f}  L30_orders={ord_}")
    conn.commit()
    print("\n[COMMIT] rck_last30 built.")
except Exception as e:
    conn.rollback()
    print(f"\n[ROLLBACK] {type(e).__name__}: {e}")
    raise
finally:
    conn.close()
