"""Create + populate public.ae_daily_30d — daywise ad×date rollup for the last
30 days, aligned with per-day Shopify attribution.

Scope:
  - Any ad_id that had ad_status = ACTIVE on at least one day in the window
    (regardless of current status — includes ads that were active but now paused).
  - For those ads, one row per (ad_id, date) where the ad had activity
    (spend, impressions, or Shopify orders) in the window.

Source tables:
  - Meta metrics : DISTINCT ON (ad_id, date) from primary_table ∪ backfill_table
                   (primary_table wins on conflict — more authoritative recent data).
  - Shopify     : shopify_ad_attribution grouped by (ad_id, DATE(order_created_at)).

RLS: enabled on ae_daily_30d. No policies = only service_role can read
(matches the Google Apps Script + dashboard pattern).

Idempotent: DROP + CREATE on every run. Safe to re-run daily.
"""
import os, sys, io, psycopg2
from dotenv import load_dotenv
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
load_dotenv(override=True)
conn = psycopg2.connect(os.environ['SUPABASE_DB_URL'], connect_timeout=30, keepalives=1,
                        keepalives_idle=30, keepalives_interval=10, keepalives_count=5)
conn.autocommit = False

DAYS = 30   # window length; today - 30 days .. today - 1 day  (30 full days ending yesterday)

DDL = """
DROP TABLE IF EXISTS public.ae_daily_30d;

CREATE TABLE public.ae_daily_30d (
    ad_id             text        NOT NULL,
    date              date        NOT NULL,
    -- structural metadata (as recorded on that day)
    account_name      text,
    ad_name           text,
    ad_status         text,
    campaign_id       text,
    campaign_name     text,
    adset_id          text,
    adset_name        text,
    -- delivery metrics
    impressions       bigint,
    reach             bigint,
    frequency         numeric(10,4),
    amount_spent      numeric(14,2),
    -- engagement / funnel
    outbound_clicks   bigint,
    link_clicks_raw   bigint,               -- inline_link_clicks from primary_table
    thruplays         bigint,
    three_sec_plays   bigint,
    post_engagements  bigint,
    engagement_count  bigint,
    video_play_time   numeric(14,2),
    -- conversions (raw counts — rates derived per day in the Apps Script)
    conversion_value  numeric(14,2),
    purchases         numeric(14,2),
    ci_count          numeric(14,2),        -- initiate_checkout
    atc_count         numeric(14,2),        -- add_to_cart
    purchase_roas     numeric(10,4),
    ftewv_count       numeric(14,2),
    cost_per_ftewv    numeric(14,2),
    ncp_count         numeric(14,2),
    cost_per_ncp      numeric(14,2),
    ltv_reach         bigint,
    ltv_frequency     numeric(10,4),
    -- Shopify (daywise attribution)
    shopify_orders    integer     NOT NULL DEFAULT 0,
    shopify_sales     numeric(14,2) NOT NULL DEFAULT 0,
    shopify_roas      numeric(10,4),
    refreshed_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ad_id, date)
);
-- source_table intentionally dropped (removed per user request 2026-07-25)
CREATE INDEX ae_daily_30d_date_idx      ON public.ae_daily_30d (date);
CREATE INDEX ae_daily_30d_status_idx    ON public.ae_daily_30d (ad_status);
CREATE INDEX ae_daily_30d_campaign_idx  ON public.ae_daily_30d (campaign_id);
CREATE INDEX ae_daily_30d_adset_idx     ON public.ae_daily_30d (adset_id);

ALTER TABLE public.ae_daily_30d ENABLE ROW LEVEL SECURITY;
-- No policies → only service_role can read; matches the pattern already used by
-- ae_shopify_enriched. Add a SELECT policy for anon later if the dashboard needs it.
"""

INSERT_SQL = """
INSERT INTO public.ae_daily_30d (
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
  SELECT (CURRENT_DATE - %s::int)::date AS start_dt,
         (CURRENT_DATE - 1)::date       AS end_dt
),
-- Ads that were ACTIVE on at least one day in the window (any table)
scope_ads AS (
  SELECT DISTINCT ad_id
    FROM (
      SELECT ad_id FROM public.primary_table, w
       WHERE date BETWEEN w.start_dt AND w.end_dt
         AND UPPER(COALESCE(ad_status,'')) = 'ACTIVE'
         AND ad_id IS NOT NULL AND ad_id <> ''
      UNION
      SELECT ad_id FROM public.backfill_table, w
       WHERE date BETWEEN w.start_dt AND w.end_dt
         AND UPPER(COALESCE(ad_status,'')) = 'ACTIVE'
         AND ad_id IS NOT NULL AND ad_id <> ''
    ) x
),
-- All daily rows for those ads in the window (regardless of status on that day),
-- dedup'd: primary_table wins, backfill_table fills gaps.
-- (Explicit column list because primary/backfill have different arities.)
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
    FROM combined
    ORDER BY ad_id, date, priority
),
-- Daywise Shopify attribution for these ads
shop_daily AS (
  SELECT ad_id, (order_created_at)::date AS date,
         COUNT(*)                       AS shopify_orders,
         COALESCE(SUM(total_price), 0)::numeric(14,2) AS shopify_sales
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
        print("[1/3] Dropping + creating public.ae_daily_30d ...")
        cur.execute(DDL)

        print(f"[2/3] Populating (window = last {DAYS} days ending yesterday) ...")
        cur.execute(INSERT_SQL, (DAYS,))
        n_inserted = cur.rowcount
        print(f"      rows inserted: {n_inserted:,}")

        # Summary
        cur.execute("""SELECT COUNT(*), COUNT(DISTINCT ad_id), COUNT(DISTINCT date),
                              MIN(date)::text, MAX(date)::text,
                              SUM(amount_spent)::numeric(14,2),
                              SUM(conversion_value)::numeric(14,2),
                              SUM(shopify_orders), SUM(shopify_sales)::numeric(14,2)
                         FROM public.ae_daily_30d;""")
        r = cur.fetchone()
        print(f"[3/3] Sanity:")
        print(f"      total rows           : {r[0]:,}")
        print(f"      distinct ad_ids      : {r[1]:,}")
        print(f"      distinct dates       : {r[2]}  ({r[3]} to {r[4]})")
        print(f"      total spend          : Rs{r[5] or 0:,.2f}")
        print(f"      total Meta conv val  : Rs{r[6] or 0:,.2f}")
        print(f"      total Shopify orders : {r[7] or 0:,}")
        print(f"      total Shopify sales  : Rs{r[8] or 0:,.2f}")

        # Status distribution of scope ads
        cur.execute("""SELECT ad_status, COUNT(DISTINCT ad_id)
                         FROM public.ae_daily_30d
                        GROUP BY ad_status ORDER BY 2 DESC;""")
        print("\n      ad_status distribution across scope ads (latest day-status per ad):")
        for st, n in cur.fetchall():
            print(f"        {(st or '<null>'):<18}  {n:>6,}")

    conn.commit()
    print("\n[COMMIT] ae_daily_30d built + populated.")

except Exception as e:
    conn.rollback()
    print(f"\n[ROLLBACK] {type(e).__name__}: {e}")
    raise
finally:
    conn.close()
