"""Refresh the campaign / adset incremental-reach materialized views.

Both views UNION primary_table + backfill_table, dedup on (ad_id,
date) with primary winning, then sum reach + spend up to the
campaign / adset grain. Refreshed after refresh_ae_daily_agg so the
whole reach-tables set stays in sync.
"""
from __future__ import annotations
import os, sys, time, psycopg2
from dotenv import load_dotenv
try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DB = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB: raise SystemExit("Missing SUPABASE_DB_URL")

VIEWS = ["public.ireach_campaign_daily", "public.ireach_adset_daily"]

with psycopg2.connect(DB) as conn:
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '600s'")
        for v in VIEWS:
            t0 = time.time()
            cur.execute(f"REFRESH MATERIALIZED VIEW {v}")
            cur.execute(f"SELECT COUNT(*) FROM {v}")
            n = cur.fetchone()[0]
            print(f"[ok] {v}  {n:,} rows  {time.time()-t0:.1f}s")
        conn.commit()
