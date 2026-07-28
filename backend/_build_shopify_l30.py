"""Build public.shopify_ad_attribution_l30 — pre-filtered L30 order-attribution
subset with indexes for the Ad Intelligence UI.

Consumer: dashboard.js aiFetchOrders(). When the picked date range is inside
the last 30 days, the UI hits this table instead of shopify_ad_attribution
(721k rows) — ~25× smaller so the "Load rows" pagination goes from 50-60s
to <5s.

Refresh cadence: called at the end of _fetch_new_orders_only.py so it's
always in sync with the latest incremental fetch. Idempotent DROP+CREATE.
"""
import os, sys, io, psycopg2
from dotenv import load_dotenv
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
load_dotenv(override=True)
conn = psycopg2.connect(os.environ['SUPABASE_DB_URL'], connect_timeout=30, keepalives=1)
conn.autocommit = False

DDL = """
DROP TABLE IF EXISTS public.shopify_ad_attribution_l30;

CREATE TABLE public.shopify_ad_attribution_l30 (
  order_id            text PRIMARY KEY,
  order_created_at    timestamptz NOT NULL,
  ordered_item        text,
  total_price         numeric(14,2),
  utm_source          text,
  utm_medium          text,
  utm_campaign        text,
  utm_content         text,
  utm_term            text,
  ad_id               text,
  ad_name             text,
  campaign_name       text,
  adset_id            text,
  has_match           boolean,
  matched_value       text,
  matched_tier        text,
  customer_id         text,
  customer_num_orders integer,
  contact_email       text,
  last_synced_at      timestamptz,
  refreshed_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX shopify_l30_date_idx      ON public.shopify_ad_attribution_l30 (order_created_at DESC);
CREATE INDEX shopify_l30_ad_idx        ON public.shopify_ad_attribution_l30 (ad_id);
CREATE INDEX shopify_l30_tier_idx      ON public.shopify_ad_attribution_l30 (matched_tier);
CREATE INDEX shopify_l30_has_match_idx ON public.shopify_ad_attribution_l30 (has_match);

ALTER TABLE public.shopify_ad_attribution_l30 ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_read_all ON public.shopify_ad_attribution_l30 FOR SELECT USING (true);
"""

INSERT_SQL = """
INSERT INTO public.shopify_ad_attribution_l30
  (order_id, order_created_at, ordered_item, total_price,
   utm_source, utm_medium, utm_campaign, utm_content, utm_term,
   ad_id, ad_name, campaign_name, adset_id,
   has_match, matched_value, matched_tier,
   customer_id, customer_num_orders, contact_email, last_synced_at)
SELECT
  order_id, order_created_at, ordered_item, total_price,
  utm_source, utm_medium, utm_campaign, utm_content, utm_term,
  ad_id, ad_name, campaign_name, adset_id,
  has_match, matched_value, matched_tier,
  customer_id, customer_num_orders, contact_email, last_synced_at
FROM public.shopify_ad_attribution
WHERE order_created_at::date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE;
"""

try:
    with conn.cursor() as cur:
        print("[1/2] Drop + create public.shopify_ad_attribution_l30 ...")
        cur.execute(DDL)
        print("[2/2] Populating (last 30 days from shopify_ad_attribution) ...")
        cur.execute(INSERT_SQL)
        n = cur.rowcount
        cur.execute("""SELECT COUNT(*),
                              MIN(order_created_at)::date::text,
                              MAX(order_created_at)::date::text,
                              SUM(CASE WHEN has_match THEN 1 ELSE 0 END),
                              SUM(total_price)::numeric(14,2)
                         FROM public.shopify_ad_attribution_l30;""")
        r = cur.fetchone()
        print(f"      rows inserted        : {n:,}")
        print(f"      date range           : {r[1]} → {r[2]}")
        print(f"      rows with has_match  : {r[3]:,} ({r[3]/r[0]*100:.1f}%)")
        print(f"      total sales          : Rs{r[4] or 0:,.0f}")
    conn.commit()
    print("\n[COMMIT] shopify_ad_attribution_l30 rebuilt.")
except Exception as e:
    conn.rollback()
    print(f"\n[ROLLBACK] {type(e).__name__}: {e}")
    raise
finally:
    conn.close()
