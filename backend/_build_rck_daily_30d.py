"""Build public.rck_daily_30d — day-wise breakdown for every ad in an RCK
campaign, last 30 days.

Scope:
  · campaign_name ILIKE '%RCK%'
  · Includes ALL statuses (ACTIVE / PAUSED / CAMPAIGN_PAUSED / WITH_ISSUES /
    ARCHIVED / ADSET_PAUSED). Any ad that had a Meta row in the window
    qualifies — no ad_status filter, unlike ae_daily_30d.

Schema mirrors ae_daily_30d (same 30 columns), grain (ad_id, date). RLS
enabled — only service_role reads.
"""
import os, sys, io, psycopg2
from dotenv import load_dotenv
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
load_dotenv(override=True)
conn = psycopg2.connect(os.environ['SUPABASE_DB_URL'], connect_timeout=30, keepalives=1)
conn.autocommit = False

DDL = """
DROP TABLE IF EXISTS public.rck_daily_30d;

CREATE TABLE public.rck_daily_30d (
    ad_id             text        NOT NULL,
    date              date        NOT NULL,
    account_name      text,
    ad_name           text,
    ad_status         text,
    campaign_id       text,
    campaign_name     text,
    adset_id          text,
    adset_name        text,
    impressions       bigint,
    reach             bigint,
    frequency         numeric(10,4),
    amount_spent      numeric(14,2),
    outbound_clicks   bigint,
    link_clicks_raw   bigint,
    thruplays         bigint,
    three_sec_plays   bigint,
    post_engagements  bigint,
    engagement_count  bigint,
    video_play_time   numeric(14,2),
    conversion_value  numeric(14,2),
    purchases         numeric(14,2),
    ci_count          numeric(14,2),
    atc_count         numeric(14,2),
    purchase_roas     numeric(10,4),
    ftewv_count       numeric(14,2),
    cost_per_ftewv    numeric(14,2),
    ncp_count         numeric(14,2),
    cost_per_ncp      numeric(14,2),
    ltv_reach         bigint,
    ltv_frequency     numeric(10,4),
    shopify_orders    integer     NOT NULL DEFAULT 0,
    shopify_sales     numeric(14,2) NOT NULL DEFAULT 0,
    shopify_roas      numeric(10,4),
    refreshed_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ad_id, date)
);
CREATE INDEX rck_daily_30d_date_idx      ON public.rck_daily_30d (date);
CREATE INDEX rck_daily_30d_campaign_idx  ON public.rck_daily_30d (campaign_id);
CREATE INDEX rck_daily_30d_adset_idx     ON public.rck_daily_30d (adset_id);
CREATE INDEX rck_daily_30d_status_idx    ON public.rck_daily_30d (ad_status);

ALTER TABLE public.rck_daily_30d ENABLE ROW LEVEL SECURITY;
"""

INSERT_SQL = """
INSERT INTO public.rck_daily_30d (
    ad_id, date,
    account_name, ad_name, ad_status, campaign_id, campaign_name, adset_id, adset_name,
    impressions, reach, frequency, amount_spent,
    outbound_clicks, link_clicks_raw, thruplays, three_sec_plays, post_engagements,
    engagement_count, video_play_time,
    conversion_value, purchases, ci_count, atc_count, purchase_roas,
    ftewv_count, cost_per_ftewv, ncp_count, cost_per_ncp,
    ltv_reach, ltv_frequency,
    shopify_orders, shopify_sales, shopify_roas
)
WITH
w AS (
  SELECT (CURRENT_DATE - 30)::date AS start_dt,
         (CURRENT_DATE - 1)::date  AS end_dt
),
-- All RCK ads with any Meta row in the window, no ad_status filter
scope_ads AS (
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
combined AS (
  SELECT p.ad_id, p.date, p.account_name, p.ad_name, p.ad_status,
         p.campaign_id, p.campaign_name, p.adset_id, p.adset_name,
         p.impressions, p.reach, p.frequency, p.amount_spent_inr,
         p.outbound_clicks, p.inline_link_clicks, p.thruplays, p.three_sec_video_plays,
         p.post_engagements, p.engagement_count, p.video_play_time,
         p.conversion_value, p.purchases, p.initiate_checkout, p.add_to_cart, p.purchase_roas,
         p.ftewv_count, p.cost_per_ftewv, p.ncp_count, p.cost_per_ncp,
         p.ltv_reach, p.ltv_frequency,
         1 AS priority
    FROM public.primary_table p, w
   WHERE p.date BETWEEN w.start_dt AND w.end_dt
     AND p.ad_id IN (SELECT ad_id FROM scope_ads)
  UNION ALL
  SELECT b.ad_id, b.date, b.account_name, b.ad_name, b.ad_status,
         b.campaign_id, b.campaign_name, b.adset_id, b.adset_name,
         b.impressions, b.reach, b.frequency, b.amount_spent_inr,
         b.outbound_clicks, NULL::bigint AS inline_link_clicks, b.thruplays, b.three_sec_video_plays,
         b.post_engagements, NULL::bigint AS engagement_count, b.video_play_time,
         b.conversion_value, NULL::numeric AS purchases,
         NULL::numeric AS initiate_checkout, NULL::numeric AS add_to_cart, b.purchase_roas,
         b.ftewv_count, b.cost_per_ftewv, b.ncp_count, b.cost_per_ncp,
         b.ltv_reach, b.ltv_frequency,
         2 AS priority
    FROM public.backfill_table b, w
   WHERE b.date BETWEEN w.start_dt AND w.end_dt
     AND b.ad_id IN (SELECT ad_id FROM scope_ads)
),
dedup AS (
  SELECT DISTINCT ON (ad_id, date) *
    FROM combined ORDER BY ad_id, date, priority
),
shop_daily AS (
  SELECT ad_id, (order_created_at)::date AS date,
         COUNT(*) AS shopify_orders,
         COALESCE(SUM(total_price),0)::numeric(14,2) AS shopify_sales
    FROM public.shopify_ad_attribution, w
   WHERE order_created_at::date BETWEEN w.start_dt AND w.end_dt
     AND ad_id IN (SELECT ad_id FROM scope_ads)
   GROUP BY ad_id, (order_created_at)::date
)
SELECT
    d.ad_id, d.date,
    d.account_name, d.ad_name, d.ad_status, d.campaign_id, d.campaign_name,
    d.adset_id, d.adset_name,
    d.impressions, d.reach, d.frequency, d.amount_spent_inr,
    d.outbound_clicks, d.inline_link_clicks, d.thruplays, d.three_sec_video_plays,
    d.post_engagements, d.engagement_count, d.video_play_time,
    d.conversion_value, d.purchases, d.initiate_checkout, d.add_to_cart, d.purchase_roas,
    d.ftewv_count, d.cost_per_ftewv, d.ncp_count, d.cost_per_ncp,
    d.ltv_reach, d.ltv_frequency,
    COALESCE(s.shopify_orders, 0),
    COALESCE(s.shopify_sales,  0),
    CASE WHEN d.amount_spent_inr > 0
         THEN (COALESCE(s.shopify_sales, 0) / d.amount_spent_inr)::numeric(10,4)
         ELSE NULL END AS shopify_roas
  FROM dedup d
  LEFT JOIN shop_daily s ON s.ad_id = d.ad_id AND s.date = d.date
 ORDER BY d.ad_id, d.date;
"""

try:
    with conn.cursor() as cur:
        print("[1/3] Drop + create public.rck_daily_30d ...")
        cur.execute(DDL)
        print("[2/3] Populating (window = last 30 days ending yesterday) ...")
        cur.execute(INSERT_SQL)
        print(f"      rows inserted: {cur.rowcount:,}")
        cur.execute("""SELECT COUNT(*), COUNT(DISTINCT ad_id), COUNT(DISTINCT date),
                              MIN(date)::text, MAX(date)::text,
                              SUM(amount_spent)::numeric(14,2),
                              SUM(conversion_value)::numeric(14,2),
                              SUM(shopify_orders), SUM(shopify_sales)::numeric(14,2)
                         FROM public.rck_daily_30d;""")
        r = cur.fetchone()
        print(f"[3/3] Sanity:")
        print(f"      total rows           : {r[0]:,}")
        print(f"      distinct ad_ids      : {r[1]:,}")
        print(f"      distinct dates       : {r[2]}  ({r[3]} to {r[4]})")
        print(f"      total spend          : Rs{r[5] or 0:,.2f}")
        print(f"      total Meta conv val  : Rs{r[6] or 0:,.2f}")
        print(f"      total Shopify orders : {r[7] or 0:,}")
        print(f"      total Shopify sales  : Rs{r[8] or 0:,.2f}")

        cur.execute("""SELECT campaign_name, COUNT(DISTINCT ad_id) AS ads,
                              SUM(amount_spent)::numeric(14,2) AS spend,
                              SUM(shopify_orders) AS ord_sum
                         FROM public.rck_daily_30d
                        GROUP BY campaign_name ORDER BY 3 DESC NULLS LAST;""")
        print("\n      per RCK campaign:")
        for cn, ads, spend, ord_ in cur.fetchall():
            print(f"        {(cn or '?')[:60]:<62}  ads={ads:>3}  spend=Rs{spend or 0:,.0f}  L30_orders={ord_ or 0}")

    conn.commit()
    print("\n[COMMIT] rck_daily_30d built.")
except Exception as e:
    conn.rollback()
    print(f"\n[ROLLBACK] {type(e).__name__}: {e}")
    raise
finally:
    conn.close()
