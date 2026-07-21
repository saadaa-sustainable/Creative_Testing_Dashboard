"""Refresh public.new_incr_table + new_incr_adset_table + new_incr_camp_table.

Calls the three rebuild_new_incr_* stored functions:
  - rebuild_new_incr_table()         → ad-level cum/incr from primary_table+backfill_table
  - rebuild_new_incr_adset_table()   → adset-level cum/incr from primary_adset_table
  - rebuild_new_incr_camp_table()    → campaign-level cum/incr from primary_camp_table

All three are idempotent (TRUNCATE + INSERT). Called from _run_full_update.py."""
import os, sys, time, psycopg2
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv(override=True)

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")

REBUILDS = [
    ("new_incr_table",       "rebuild_new_incr_table"),
    ("new_incr_adset_table", "rebuild_new_incr_adset_table"),
    ("new_incr_camp_table",  "rebuild_new_incr_camp_table"),
]

for table, fn in REBUILDS:
    t0 = time.time()
    print(f"[*] rebuilding public.{table} …", flush=True)
    conn = psycopg2.connect(DB_URL, connect_timeout=30)
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '15min'")
            cur.execute(f"SELECT public.{fn}()")
            n = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    print(f"[ok] public.{table}: rows={n:,}  in {time.time()-t0:.1f}s", flush=True)
