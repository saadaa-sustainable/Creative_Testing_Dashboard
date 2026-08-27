"""sync_sessions_by_utm.py — mirror per-day × per-utm_source rollup
from Saadaa_Shopify_Data.sessions into Meta_ads_data.sessions_by_utm_source_daily.

Feeds the Landing Page → UTM Source subview (top-5 sources timeseries chart).

Source : SHOPIFY_DATA_URL / SHOPIFY_DATA_ANON_KEY  (siymyhhrpzzbowfqtauf)
Target : SUPABASE_DB_URL                             (rtkohjfzyzhizkebdsuy)

Usage
  python sync_sessions_by_utm.py                # gap-fill: max(date)+1 → today with 3d overlap
  python sync_sessions_by_utm.py --since 2026-05-01
  python sync_sessions_by_utm.py --full         # 2024-01-01 → today
"""
from __future__ import annotations
import os, sys, argparse, json, urllib.request, urllib.error
from datetime import date, timedelta
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
except Exception: pass

load_dotenv(Path(__file__).parent / '.env', override=True)

SRC_URL = (os.environ.get('SHOPIFY_DATA_URL') or '').rstrip('/')
SRC_KEY = os.environ.get('SHOPIFY_DATA_ANON_KEY') or ''
DB_URL  = os.environ.get('SUPABASE_DB_URL', '').strip()
if not (SRC_URL and SRC_KEY and DB_URL):
    sys.exit('[fatal] need SHOPIFY_DATA_URL + SHOPIFY_DATA_ANON_KEY + SUPABASE_DB_URL in .env')


def http_get(path, extra=None, timeout=30):
    hdrs = {'apikey': SRC_KEY, 'Authorization': f'Bearer {SRC_KEY}', 'Accept': 'application/json'}
    if extra: hdrs.update(extra)
    req = urllib.request.Request(f'{SRC_URL}{path}', headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def fetch_raw_sessions(since_iso, until_iso):
    """Page through source project's sessions table for the given date range.
    Only pulls the columns we aggregate on client-side.

    IMPORTANT: PostgREST's anon role has a default row cap of 1000 per
    request. Previously PAGE=5000 with a `Range: 0-4999` request → PG
    silently returned 1000 rows → `len(chunk) < PAGE` was true → loop
    broke on page 0, leaving 200k+ rows unfetched. That's why the target
    table stopped updating on 2026-08-10. Fix: page in 1000-row chunks
    (matching the cap) and only break when the response is truly empty."""
    rows = []
    page = 0
    PAGE = 1000
    cols = 'session_date,utm_source,sessions,online_store_visitors'
    while True:
        lo, hi = page * PAGE, (page + 1) * PAGE - 1
        st, _, body = http_get(
            f'/rest/v1/sessions?select={cols}'
            f'&session_date=gte.{since_iso}&session_date=lte.{until_iso}',
            extra={'Range-Unit': 'items', 'Range': f'{lo}-{hi}'},
        )
        if st not in (200, 206):
            raise RuntimeError(f'source fetch failed {st}: {body[:200]}')
        chunk = json.loads(body)
        if not chunk: break
        rows.extend(chunk)
        print(f'  [page {page}] +{len(chunk):,} rows  running total {len(rows):,}', flush=True)
        if len(chunk) < PAGE: break
        page += 1
        # Safety belt — 100k pages × 1k = 100M rows, more than any sane
        # window should ever produce. Guards against silent infinite loop.
        if page > 100_000:
            print(f'  [!] safety break at page {page}', flush=True)
            break
    return rows


def aggregate(rows):
    """Fold (session_date, utm_source) -> (sessions, visitors)."""
    agg = {}
    for r in rows:
        d = r.get('session_date')
        s = (r.get('utm_source') or '').strip() or '(direct)'
        key = (d, s)
        bin = agg.get(key)
        if not bin:
            bin = agg[key] = [0, 0]
        bin[0] += int(r.get('sessions') or 0)
        bin[1] += int(r.get('online_store_visitors') or 0)
    return agg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--since')
    ap.add_argument('--until')
    ap.add_argument('--full', action='store_true')
    args = ap.parse_args()

    conn = psycopg2.connect(DB_URL, connect_timeout=30); conn.autocommit = False
    cur = conn.cursor()

    if args.full:
        since = '2024-01-01'
    elif args.since:
        since = args.since
    else:
        cur.execute('SELECT MAX(session_date) FROM public.sessions_by_utm_source_daily')
        row = cur.fetchone()
        since = ((row[0] - timedelta(days=3)) if row and row[0]
                 else (date.today() - timedelta(days=89))).isoformat()
    until = args.until or date.today().isoformat()
    print(f'[*] window: {since} → {until}')

    rows = fetch_raw_sessions(since, until)
    print(f'[fetched] {len(rows):,} raw session rows')

    agg = aggregate(rows)
    print(f'[aggregated] {len(agg):,} (day × utm_source) buckets')

    payload = [(d, src, sess, vis) for (d, src), (sess, vis) in agg.items()]
    execute_values(
        cur,
        """insert into public.sessions_by_utm_source_daily
             (session_date, utm_source, sessions, online_store_visitors, synced_at)
           values %s
           on conflict (session_date, utm_source) do update set
             sessions              = excluded.sessions,
             online_store_visitors = excluded.online_store_visitors,
             synced_at             = now()""",
        payload,
        template='(%s, %s, %s, %s, now())',
        page_size=1000,
    )
    conn.commit()
    print(f'[upserted] {len(payload):,} rows')
    cur.close(); conn.close()


if __name__ == '__main__':
    main()
