"""sync_sessions_by_utm_page.py — mirror per-day × per-page × per-utm rollup
from Saada_Shopify_Data.sessions into
public.sessions_by_utm_page_daily.

Enables Pages master to filter sessions/visitors by picked utm_source(s).

Source : SHOPIFY_DATA_URL / SHOPIFY_DATA_ANON_KEY
Target : SUPABASE_DB_URL

Usage
  python sync_sessions_by_utm_page.py             # gap-fill last-known → today (3d overlap)
  python sync_sessions_by_utm_page.py --since 2026-05-13
  python sync_sessions_by_utm_page.py --full      # 2024-01-01 → today
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
    sys.exit('[fatal] need SHOPIFY_DATA_URL + SHOPIFY_DATA_ANON_KEY + SUPABASE_DB_URL')


def http_get(path, extra=None, timeout=45):
    hdrs = {'apikey': SRC_KEY, 'Authorization': f'Bearer {SRC_KEY}',
            'Accept': 'application/json'}
    if extra: hdrs.update(extra)
    req = urllib.request.Request(f'{SRC_URL}{path}', headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def fetch_day(day_iso):
    """Pull raw rows for a single day (session_date=eq.day). One day at
    a time keeps memory bounded — full 90d has ~1M raw rows, per-day = ~10k."""
    rows = []
    page = 0
    PAGE = 5000
    cols = ('session_date,landing_page_path,utm_source,sessions,'
            'online_store_visitors,sessions_with_cart_additions,'
            'sessions_that_reached_checkout,bounces')
    while True:
        lo, hi = page * PAGE, (page + 1) * PAGE - 1
        st, _, body = http_get(
            f'/rest/v1/sessions?select={cols}&session_date=eq.{day_iso}',
            extra={'Range-Unit': 'items', 'Range': f'{lo}-{hi}'},
        )
        if st not in (200, 206):
            raise RuntimeError(f'source fetch failed {st}: {body[:200]}')
        chunk = json.loads(body)
        if not chunk: break
        rows.extend(chunk)
        if len(chunk) < PAGE: break
        page += 1
    return rows


def aggregate(rows):
    """Fold (session_date, landing_page_path, utm_source) → sessions, visitors,
    atc_sessions, checkout_sessions, bounces. Path fallback '(none)', utm '(direct)'."""
    agg = {}
    for r in rows:
        d   = r.get('session_date')
        p   = (r.get('landing_page_path') or '').strip() or '(none)'
        src = (r.get('utm_source') or '').strip() or '(direct)'
        key = (d, p, src)
        bin = agg.get(key)
        if not bin:
            bin = agg[key] = [0, 0, 0, 0, 0]   # sess, vis, atc, chk, bnc
        bin[0] += int(r.get('sessions') or 0)
        bin[1] += int(r.get('online_store_visitors') or 0)
        bin[2] += int(r.get('sessions_with_cart_additions') or 0)
        bin[3] += int(r.get('sessions_that_reached_checkout') or 0)
        bin[4] += int(r.get('bounces') or 0)
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
        since_iso = '2024-01-01'
    elif args.since:
        since_iso = args.since
    else:
        cur.execute('SELECT MAX(session_date) FROM public.sessions_by_utm_page_daily')
        row = cur.fetchone()
        since_iso = ((row[0] - timedelta(days=3)) if row and row[0]
                     else (date.today() - timedelta(days=89))).isoformat()
    until_iso = args.until or date.today().isoformat()
    d_from = date.fromisoformat(since_iso)
    d_to   = date.fromisoformat(until_iso)
    total_days = (d_to - d_from).days + 1
    print(f'[*] window {since_iso} → {until_iso}  ({total_days} days)')

    total_rows_up = 0
    d = d_from
    while d <= d_to:
        raw = fetch_day(d.isoformat())
        agg = aggregate(raw)
        if agg:
            payload = [(k[0], k[1], k[2], v[0], v[1], v[2], v[3], v[4]) for k, v in agg.items()]
            execute_values(
                cur,
                """insert into public.sessions_by_utm_page_daily
                     (session_date, landing_page_path, utm_source,
                      sessions, online_store_visitors,
                      sessions_with_cart_additions, sessions_that_reached_checkout,
                      bounces, synced_at)
                   values %s
                   on conflict (session_date, landing_page_path, utm_source)
                     do update set
                       sessions                       = excluded.sessions,
                       online_store_visitors          = excluded.online_store_visitors,
                       sessions_with_cart_additions   = excluded.sessions_with_cart_additions,
                       sessions_that_reached_checkout = excluded.sessions_that_reached_checkout,
                       bounces                        = excluded.bounces,
                       synced_at                      = now()""",
                payload,
                template='(%s, %s, %s, %s, %s, %s, %s, %s, now())',
                page_size=1000,
            )
            conn.commit()
            total_rows_up += len(payload)
        print(f'  {d}  raw={len(raw):>6,}  agg={len(agg):>5,}   total_up={total_rows_up:,}', flush=True)
        d += timedelta(days=1)

    cur.close(); conn.close()
    print(f'\n[done] {total_rows_up:,} rows upserted across {total_days} days')


if __name__ == '__main__':
    main()
