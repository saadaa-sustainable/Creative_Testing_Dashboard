"""refresh_google_ads_summary.py — rebuild public.google_ads_summary
(one row per customer_id, ad_id) from public.google_ads_primary.

Mirrors the shape of Meta's summary_table: lifetime aggregates + latest
metadata per ad.  TRUNCATE+INSERT each run — the source is ~small enough
(a few hundred k rows) that a full rebuild is cleaner than incremental.

USAGE
  python refresh_google_ads_summary.py
"""
import os, sys, time, psycopg2
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv(override=True)

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")

# One row per ad, lifetime — pick the most-recent ad_name / status /
# final_urls / ad_group / campaign metadata so the summary reflects
# current identity even if the ad was renamed mid-life.  Aggregates
# sum across all dates for this ad in google_ads_primary.
REBUILD_SQL = """
TRUNCATE public.google_ads_summary;

INSERT INTO public.google_ads_summary (
  customer_id, customer_name, currency_code,
  campaign_id, campaign_name, campaign_status,
  advertising_channel_type, advertising_channel_sub_type,
  ad_group_id, ad_group_name, ad_group_status,
  ad_id, ad_name, ad_type, ad_status, final_urls,
  first_seen, last_seen, days_active,
  total_impressions, total_clicks, total_cost,
  total_conversions, total_conversion_value,
  total_all_conversions, total_all_conv_value,
  avg_ctr, avg_cpc, cost_per_conversion, roas,
  refreshed_at
)
WITH latest AS (
  SELECT DISTINCT ON (customer_id, ad_id)
         customer_id, ad_id,
         customer_name, currency_code,
         campaign_id, campaign_name, campaign_status,
         advertising_channel_type, advertising_channel_sub_type,
         ad_group_id, ad_group_name, ad_group_status,
         ad_name, ad_type, ad_status, final_urls
    FROM public.google_ads_primary
   ORDER BY customer_id, ad_id, date DESC
),
agg AS (
  SELECT customer_id, ad_id,
         MIN(date) AS first_seen,
         MAX(date) AS last_seen,
         COUNT(DISTINCT date)::int AS days_active,
         COALESCE(SUM(impressions),0)::bigint       AS total_impressions,
         COALESCE(SUM(clicks),0)::bigint            AS total_clicks,
         COALESCE(SUM(cost),0)::numeric(14,2)       AS total_cost,
         COALESCE(SUM(conversions),0)::numeric(14,3)          AS total_conversions,
         COALESCE(SUM(conversion_value),0)::numeric(14,2)     AS total_conversion_value,
         COALESCE(SUM(all_conversions),0)::numeric(14,3)      AS total_all_conversions,
         COALESCE(SUM(all_conversions_value),0)::numeric(14,2) AS total_all_conv_value
    FROM public.google_ads_primary
   GROUP BY customer_id, ad_id
)
SELECT l.customer_id, l.customer_name, l.currency_code,
       l.campaign_id, l.campaign_name, l.campaign_status,
       l.advertising_channel_type, l.advertising_channel_sub_type,
       l.ad_group_id, l.ad_group_name, l.ad_group_status,
       l.ad_id, l.ad_name, l.ad_type, l.ad_status, l.final_urls,
       a.first_seen, a.last_seen, a.days_active,
       a.total_impressions, a.total_clicks, a.total_cost,
       a.total_conversions, a.total_conversion_value,
       a.total_all_conversions, a.total_all_conv_value,
       CASE WHEN a.total_impressions > 0
            THEN (a.total_clicks::numeric / a.total_impressions)
            ELSE NULL END AS avg_ctr,
       CASE WHEN a.total_clicks > 0
            THEN (a.total_cost / a.total_clicks)::numeric(14,4)
            ELSE NULL END AS avg_cpc,
       CASE WHEN a.total_conversions > 0
            THEN (a.total_cost / a.total_conversions)::numeric(14,4)
            ELSE NULL END AS cost_per_conversion,
       CASE WHEN a.total_cost > 0
            THEN (a.total_conversion_value / a.total_cost)::numeric(14,4)
            ELSE NULL END AS roas,
       NOW() AS refreshed_at
  FROM latest l
  JOIN agg    a USING (customer_id, ad_id);
"""

t0 = time.time()
print("[*] connecting to Meta ads DB (google_ads_* lives there) …", flush=True)
conn = psycopg2.connect(DB_URL, connect_timeout=30)
try:
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '10min'")
        cur.execute(REBUILD_SQL)
        cur.execute("SELECT COUNT(*) FROM public.google_ads_summary")
        n = cur.fetchone()[0]
    conn.commit()
finally:
    conn.close()

print(f"[ok] rebuilt public.google_ads_summary  ads={n:,}  in {time.time()-t0:.1f}s")
