"""Refresh public.new_incr_table by calling rebuild_new_incr_table().
Ad-level daily reach + cumulative + delta series, reconstructed from
primary_table + backfill_table.  See migration create_new_incr_table_ad_level
for the schema + rebuild body.

Idempotent — always TRUNCATE + INSERT.  Called from _run_full_update.py."""
import os, sys, time, psycopg2
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv(override=True)

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")

t0 = time.time()
print(f"[*] connecting to Meta ads DB …", flush=True)
conn = psycopg2.connect(DB_URL, connect_timeout=30)
try:
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '15min'")
        cur.execute("SELECT public.rebuild_new_incr_table()")
        n = cur.fetchone()[0]
    conn.commit()
finally:
    conn.close()

print(f"[ok] rebuilt public.new_incr_table  rows={n:,}  in {time.time()-t0:.1f}s")
