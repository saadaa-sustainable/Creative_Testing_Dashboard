"""
sync_landing_page_sessions.py — pull Shopify session data from
Saada_Shopify_Data (source project) and roll up to Meta_ads_data
(destination project).landing_page_sessions_daily.

Source has a ~6M-row raw sessions table split across UTM dims.  We call
its RPC public.get_landing_page_sessions_agg(from, to) via PostgREST to
get the (date, path) grain server-side (avoids pulling 6M rows across
the wire).  Destination write goes via psycopg2 to the Meta_ads_data DB.

USAGE
  python sync_landing_page_sessions.py                    # incremental
  python sync_landing_page_sessions.py --since 2025-01-01 # rebuild from date
  python sync_landing_page_sessions.py --full             # rebuild everything
  python sync_landing_page_sessions.py --dry-run

Env (backend/.env):
  SUPABASE_DB_URL            Meta_ads_data pooler URL (destination write)
  SHOPIFY_DATA_SUPABASE_URL  https://<project>.supabase.co  (source read)
  SHOPIFY_DATA_ANON_KEY      anon key for source project
  (Fallback if those env vars aren't set: derive from SUPABASE_URL_SHOPIFY,
   SUPABASE_ANON_SHOPIFY, or hardcode from list_projects.)
"""
import os, sys, io, argparse, time, requests, psycopg2
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from psycopg2.extras import execute_values

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='backslashreplace')

load_dotenv()

# Destination — Meta_ads_data pooler
DST_DB = (os.environ.get('SUPABASE_DB_URL') or '').strip()
if not DST_DB: sys.exit('Missing SUPABASE_DB_URL in .env')

# Source — Saada_Shopify_Data (project id siymyhhrpzzbowfqtauf)
SRC_URL = (os.environ.get('SHOPIFY_DATA_SUPABASE_URL')
           or os.environ.get('SUPABASE_URL_SHOPIFY')
           or 'https://siymyhhrpzzbowfqtauf.supabase.co').strip()
SRC_KEY = (os.environ.get('SHOPIFY_DATA_ANON_KEY')
           or os.environ.get('SUPABASE_ANON_SHOPIFY')
           or os.environ.get('SHOPIFY_DATA_SUPABASE_ANON_KEY')
           or '').strip()
if not SRC_KEY:
    sys.exit('Missing SHOPIFY_DATA_ANON_KEY (or SUPABASE_ANON_SHOPIFY) in .env')

LOG_FILE = 'logs/sync_landing_page_sessions.log'
def log(*a):
    msg = ' '.join(str(x) for x in a)
    print(msg, flush=True)
    os.makedirs('logs', exist_ok=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f: f.write(msg + '\n')

UPSERT_SQL = """
INSERT INTO public.landing_page_sessions_daily
    (session_date, landing_page_path, sessions, online_store_visitors,
     sessions_with_cart_additions, sessions_that_reached_checkout, bounces)
VALUES %s
ON CONFLICT (session_date, landing_page_path) DO UPDATE SET
    sessions                       = EXCLUDED.sessions,
    online_store_visitors          = EXCLUDED.online_store_visitors,
    sessions_with_cart_additions   = EXCLUDED.sessions_with_cart_additions,
    sessions_that_reached_checkout = EXCLUDED.sessions_that_reached_checkout,
    bounces                        = EXCLUDED.bounces,
    synced_at                      = NOW()
"""

def fetch_source_agg_daily(day_iso):
    """Call source RPC for ONE day. Small payload (~5-10k rows) which
    PostgREST returns in a single response. Iterating day-by-day keeps
    memory low and lets us commit progress incrementally."""
    url = f"{SRC_URL}/rest/v1/rpc/get_landing_page_sessions_agg"
    headers = {'apikey': SRC_KEY, 'Authorization': 'Bearer ' + SRC_KEY,
               'Content-Type': 'application/json', 'Prefer': 'count=none'}
    body = {'from_date': day_iso, 'to_date': day_iso}
    r = requests.post(url, headers=headers, json=body, timeout=60)
    if not r.ok:
        raise RuntimeError(f"source RPC HTTP {r.status_code}: {r.text[:200]}")
    return r.json() or []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', help='YYYY-MM-DD start (inclusive)')
    ap.add_argument('--until', help='YYYY-MM-DD end (inclusive; default today)')
    ap.add_argument('--full',  action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    dst = psycopg2.connect(DST_DB, connect_timeout=30); dst.autocommit = False

    if args.full:
        since = '2022-10-01'
    elif args.since:
        since = args.since
    else:
        with dst.cursor() as cur:
            cur.execute("SELECT MAX(session_date) FROM public.landing_page_sessions_daily")
            row = cur.fetchone()
        if row and row[0]:
            since = (row[0] - timedelta(days=3)).isoformat()   # 3-day overlap
        else:
            since = '2025-01-01'
    until = args.until or date.today().isoformat()

    log(f"[*] window: {since} → {until}")
    log(f"[*] src: {SRC_URL}   dst: (Meta_ads_data)")

    d_from = datetime.strptime(since, '%Y-%m-%d').date()
    d_to   = datetime.strptime(until, '%Y-%m-%d').date()
    n_days = (d_to - d_from).days + 1
    log(f"[*] {n_days} days to sync")

    t0 = time.time(); total = 0; day_count = 0
    d = d_from
    while d <= d_to:
        rows = fetch_source_agg_daily(d.isoformat())
        if rows:
            batch = [(r['session_date'], r['landing_page_path'],
                      r['sessions'], r['online_store_visitors'],
                      r['sessions_with_cart_additions'],
                      r['sessions_that_reached_checkout'],
                      r['bounces']) for r in rows]
            if not args.dry_run:
                with dst.cursor() as dcur:
                    execute_values(dcur, UPSERT_SQL, batch, page_size=1000)
                dst.commit()
            total += len(batch)
        day_count += 1
        if day_count % 10 == 0 or day_count == n_days:
            elapsed = time.time() - t0
            log(f"  ... day {day_count}/{n_days}  total={total:,} rows  "
                f"elapsed={elapsed:.0f}s  ({(total/max(elapsed,1)):.0f} rows/s)")
        d += timedelta(days=1)

    dst.close()
    log(f"[ok] synced {total:,} rows across {day_count} days in {(time.time()-t0):.0f}s"
        + (" (dry-run)" if args.dry_run else ""))

if __name__ == '__main__':
    main()
