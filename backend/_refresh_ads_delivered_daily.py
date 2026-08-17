"""_refresh_ads_delivered_daily.py — rebuild the small helper table that
backs the get_delivery_ads RPC.

Why this table: get_delivery_ads was previously a live DISTINCT scan of
primary_table + backfill_table (~2M rows), taking 40-90s and timing out
on Supabase's 60s HTTP cap. This helper stores just DISTINCT (ad_id, date)
pairs where impressions > 0 — ~340k rows, indexed on both columns, so
any date-range query returns in <300ms.

Refresh strategy: full rebuild each run (~12s). Simpler than incremental
upsert and safe to interrupt.

Ordering: must run AFTER primary_sync + propagate_primary_to_backfill so
the latest delivery data is picked up.
"""
import os, sys, io, time, psycopg2
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
load_dotenv(override=True)

conn = psycopg2.connect(os.environ['SUPABASE_DB_URL'], connect_timeout=30, keepalives=1)
conn.autocommit = False
cur = conn.cursor()

DDL = """
CREATE TABLE IF NOT EXISTS public.ads_delivered_daily (
  ad_id text NOT NULL,
  date  date NOT NULL,
  PRIMARY KEY (ad_id, date)
);
CREATE INDEX IF NOT EXISTS ads_delivered_date_idx ON public.ads_delivered_daily (date);
"""

REBUILD = """
TRUNCATE public.ads_delivered_daily;
INSERT INTO public.ads_delivered_daily (ad_id, date)
SELECT DISTINCT ad_id::text, date FROM (
  SELECT ad_id, date FROM public.primary_table
   WHERE ad_id IS NOT NULL AND ad_id <> '' AND impressions > 0
  UNION ALL
  SELECT ad_id, date FROM public.backfill_table
   WHERE ad_id IS NOT NULL AND ad_id <> '' AND impressions > 0
) u;
"""

print('[1/2] Ensure schema ...')
cur.execute(DDL)

print('[2/2] Truncate + rebuild ...')
t0 = time.time()
cur.execute(REBUILD)
inserted = cur.rowcount
conn.commit()

cur.execute("SELECT count(*), min(date), max(date) FROM public.ads_delivered_daily")
n, dmin, dmax = cur.fetchone()
print(f'      rows                   : {n:,}')
print(f'      date range             : {dmin} -> {dmax}')
print(f'      rebuild duration       : {time.time()-t0:.1f}s')
print('[COMMIT] ads_delivered_daily rebuilt.')
