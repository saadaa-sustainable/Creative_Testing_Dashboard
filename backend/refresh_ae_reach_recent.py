"""Refresh the ae_reach_recent materialized view.

Called by _run_full_update.py after refresh_ae_daily_agg. Uses direct
psycopg2 with a lifted statement_timeout so the ~1M-row UNION over
primary_table + backfill_table can complete (typically ~10s).

View definition (see migration ae_reach_recent_dedup, 2026-07-13):
  * UNION primary_table + backfill_table
  * DISTINCT ON (ad_id, date) with primary winning — same dedup as
    ae_daily_agg_mat, so consumers stay in agreement
  * row_number() DESC over date → pick each ad's latest & previous
    delivered days
  * incremental_reach = latest_reach - previous_reach (signed;
    negative means reach dropped between the two anchor days)

The prior UNION-ALL-without-dedup version made latest_date and
previous_date land on the same day for ~40% of ads whenever the
overlap window covered both tables, spuriously zeroing out
incremental_reach across the dashboard's Ads Analyse view."""
from __future__ import annotations
import os, sys, time, psycopg2
from dotenv import load_dotenv
try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DB = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB: raise SystemExit("Missing SUPABASE_DB_URL")

t0 = time.time()
with psycopg2.connect(DB) as conn, conn.cursor() as cur:
    cur.execute("SET LOCAL statement_timeout = '600s'")
    cur.execute("REFRESH MATERIALIZED VIEW public.ae_reach_recent")
    cur.execute("SELECT COUNT(*) FROM public.ae_reach_recent")
    n = cur.fetchone()[0]
    conn.commit()
print(f"[ok] refreshed ae_reach_recent  ·  {n:,} rows  ·  {time.time()-t0:.1f}s")
