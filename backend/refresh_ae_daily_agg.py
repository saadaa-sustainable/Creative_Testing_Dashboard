"""
refresh_ae_daily_agg.py — call the DB refresh function that rebuilds
public.ae_daily_agg_mat from primary_table + backfill_table.

The mat table backs the /rpc/get_ae_metrics_by_window,
/rpc/get_reach_by_window, and /rpc/get_delivery_ads RPCs. Refreshing
it after primary/backfill land keeps the dashboard's windowed metrics
in sync with the freshest daily rows.

Runs a single SELECT public.refresh_ae_daily_agg() — all the actual
work happens server-side in one transaction.
"""
from __future__ import annotations
import os, sys, time, psycopg2
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: raise SystemExit("Missing SUPABASE_DB_URL in .env")

def main():
    t0 = time.time()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT public.refresh_ae_daily_agg()")
            cur.execute("SELECT COUNT(*) FROM public.ae_daily_agg_mat")
            n = cur.fetchone()[0]
        conn.commit()
    print(f"[ok] refreshed ae_daily_agg_mat  ·  {n:,} rows  ·  {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
