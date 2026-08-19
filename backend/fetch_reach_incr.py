"""fetch_reach_incr.py — populate {account,campaign,adset,ad}_incr tables
with Meta's deduped cumulative reach, anchored at a fixed ORIGIN_DATE.

For each entity of the chosen --level, iterate dates from SNAPSHOT_START
through YESTERDAY. For each date D, call Meta's /act_{id}/insights endpoint
ONCE per (account, level) with time_range={since: ORIGIN_DATE, until: D}.
Meta returns one row per entity with its deduped reach for [ORIGIN, D] —
upsert into the level-specific *_incr table.

Semantics:
    reach_cumulative(D) = Meta reach for time_range [ORIGIN_DATE, D]
    incremental_reach(D) = reach_cumulative(D) - reach_cumulative(D-1)

So the very first snapshot row (SNAPSHOT_START) carries the ENTIRE
historical cumulative reach up to that date — not zero. Each subsequent
day adds the newly-reached unique users, deduped against all history.

Rationale (2026-08-17): the old ireach_cumulative_daily used a rolling
36-month ORIGIN that drifted every night, invalidating prior values.
Here ORIGIN_DATE is fixed at 2023-08-01 (safely inside Meta's 37-month
time_range.since limit — has ~1 year of headroom before we advance).

Volume: SNAPSHOT_START → today spans ~17 days initially. Meta returns ALL
entities for a level in one paginated call, so total work is
17 dates × 3 accounts × 4 levels ≈ 200 calls per full run.

USAGE
  python fetch_reach_incr.py --level account   # ~15 min for all 4 accounts
  python fetch_reach_incr.py --level campaign  # ~15 min
  python fetch_reach_incr.py --level adset     # ~15-30 min
  python fetch_reach_incr.py --level ad        # ~30-60 min (pagination)
  python fetch_reach_incr.py --level all       # runs all 4 in sequence
  python fetch_reach_incr.py --account 1136644150469466 --level campaign
  python fetch_reach_incr.py --dry-run

CONSUMED BY
  Later: get_reach_incr_by_window(level, from, to) RPC — replaces the
  stale get_ireach_incremental_analysis + get_ireach_saturation_curve.
"""
from __future__ import annotations
import os, sys, io, json, re, time, argparse, requests, psycopg2
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from psycopg2.extras import execute_values

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='backslashreplace')

# Keep Windows awake for long runs
if os.name == 'nt':
    try:
        import ctypes
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001 | 0x00000040)
    except Exception: pass

load_dotenv(override=True)
TOK = (os.environ.get('META_ACCESS_TOKEN') or '').strip()
if not TOK: sys.exit('Missing META_ACCESS_TOKEN in .env')
VER = os.environ.get('META_API_VERSION', 'v22.0').strip()
API = f'https://graph.facebook.com/{VER}'
DB_URL = (os.environ.get('SUPABASE_DB_URL') or '').strip()
if not DB_URL: sys.exit('Missing SUPABASE_DB_URL in .env')

# ── Fixed origin for cumulative windows ────────────────────────────
# All reach_cumulative values in *_incr tables represent Meta's deduped
# reach for time_range [ORIGIN_DATE, this_row's_date]. Meta caps
# time_range.since at ~37 months back — 2023-08-01 is safely inside that.
# Change this only if you rebuild all 4 tables — mixing origins breaks
# the delta math.
ORIGIN_DATE = date(2023, 8, 1)

# The first `until` date we actually store rows for. Prior history is
# still baked into ORIGIN → SNAPSHOT_START on this row (so the first
# row carries the ENTIRE historical cumulative reach, not zero).
SNAPSHOT_START = date(2026, 8, 1)

# Ad accounts we sync (id → display name)
ACCOUNTS = [
    ('1136644150469466', 'Raho Saadaa'),
    ('1349767139294217', 'Fourth Ad Account - SD'),
    ('264868699479122',  'Third Ad Account - SD'),
]

_TOK = re.compile(r'(?:EAA[A-Za-z0-9]{30,}|IGQ[\w\-]{20,}|eyJ[\w\-.]{40,})')
def scrub(s):
    s = _TOK.sub('<REDACTED>', str(s or ''))
    return re.sub(r'(access_token=)[^&\s"]+', r'\1<REDACTED>', s)

LOG_FILE = 'logs/reach_incr.log'
PROGRESS = '.reach_incr.progress.json'

def log(*a):
    msg = ' '.join(scrub(str(x)) for x in a)
    print(msg, flush=True)
    os.makedirs('logs', exist_ok=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f: f.write(msg + '\n')

def _sleep_if_throttled(headers, success):
    max_pct = 0
    try:
        for k in ('x-app-usage', 'x-business-use-case-usage'):
            v = headers.get(k)
            if not v: continue
            j = json.loads(v) if isinstance(v, str) else v
            def walk(x):
                if isinstance(x, dict):
                    for _, vv in x.items(): yield from walk(vv)
                elif isinstance(x, list):
                    for vv in x: yield from walk(vv)
                elif isinstance(x, (int, float)) and 0 <= x <= 100:
                    yield x
            for n in walk(j):
                if n > max_pct: max_pct = n
    except Exception: pass
    if max_pct >= 95:
        log(f"  [throttle {max_pct}%] sleep 300s"); time.sleep(300)
    elif max_pct >= 90:
        log(f"  [throttle {max_pct}%] sleep 60s");  time.sleep(60)
    elif max_pct >= 80:
        log(f"  [throttle {max_pct}%] sleep 15s");  time.sleep(15)
    return max_pct

def _get(url, params, retries=6):
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=45)
        except requests.RequestException as e:
            log(f"    [net {attempt}] {type(e).__name__}: {str(e)[:120]} — sleep {delay}s")
            time.sleep(delay); delay = min(delay*2, 120); continue
        if r.status_code == 200:
            data = r.json()
            _sleep_if_throttled(r.headers, success=True)
            return data
        _sleep_if_throttled(r.headers, success=False)
        try: j = r.json()
        except Exception: j = {}
        err = (j.get('error') or {})
        emsg = err.get('message') or r.text[:200]
        code = err.get('code')
        if r.status_code in (429, 500, 502, 503, 504) or code in (4, 17, 32) \
           or 'too many calls' in str(emsg).lower():
            log(f"    [throttle {attempt}] {scrub(emsg)[:120]} — sleep {delay}s")
            time.sleep(delay); delay = min(delay*2, 300); continue
        log(f"    [!] HTTP {r.status_code} {scrub(emsg)[:200]}")
        return {'__error__': emsg}
    return {'__error__': 'exhausted retries'}

# Level → (fields, table, id-column, extra-cols for INSERT)
LEVEL_CFG = {
    'account': {
        'fields': 'reach,spend,impressions',
        'table':  'account_incr',
        'cols':   ('account_id', 'account_name', 'date',
                   'reach_cumulative', 'spend_cumulative', 'impressions_cumulative'),
        'row_key': None,  # one row per account per date (no per-entity data field)
    },
    'campaign': {
        'fields': 'campaign_id,campaign_name,reach,spend,impressions',
        'table':  'campaign_incr',
        'cols':   ('campaign_id', 'campaign_name', 'account_id', 'account_name', 'date',
                   'reach_cumulative', 'spend_cumulative', 'impressions_cumulative'),
        'row_key': ('campaign_id', 'campaign_name'),
    },
    'adset': {
        'fields': 'adset_id,adset_name,campaign_id,campaign_name,reach,spend,impressions',
        'table':  'adset_incr',
        'cols':   ('adset_id', 'adset_name', 'campaign_id', 'campaign_name',
                   'account_id', 'account_name', 'date',
                   'reach_cumulative', 'spend_cumulative', 'impressions_cumulative'),
        'row_key': ('adset_id', 'adset_name', 'campaign_id', 'campaign_name'),
    },
    'ad': {
        'fields': 'ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,reach,spend,impressions',
        'table':  'ad_incr',
        'cols':   ('ad_id', 'ad_name', 'adset_id', 'adset_name',
                   'campaign_id', 'campaign_name', 'account_id', 'account_name', 'date',
                   'reach_cumulative', 'spend_cumulative', 'impressions_cumulative'),
        'row_key': ('ad_id', 'ad_name', 'adset_id', 'adset_name',
                    'campaign_id', 'campaign_name'),
    },
}

def fetch_level_for_date(account_id, level, until_date):
    """One Meta paginated call — returns list of dicts, each with reach/spend
    for [ORIGIN_DATE, until_date] at the given level."""
    cfg = LEVEL_CFG[level]
    url = f'{API}/act_{account_id}/insights'
    params = {
        'level': level,
        'fields': cfg['fields'],
        'time_range': json.dumps({'since': ORIGIN_DATE.isoformat(), 'until': until_date.isoformat()}),
        'limit': 500,
        'access_token': TOK,
    }
    out = []
    while True:
        j = _get(url, params)
        if not j or '__error__' in j:
            return out, (j or {}).get('__error__', 'no response')
        for row in j.get('data', []) or []:
            out.append(row)
        nxt = (j.get('paging') or {}).get('next')
        if not nxt: return out, None
        url, params = nxt, None
        time.sleep(0.15)

def _load_prog():
    if not os.path.exists(PROGRESS): return {}
    try:
        with open(PROGRESS, encoding='utf-8') as f: return json.load(f)
    except Exception: return {}

def _save_prog(p):
    with open(PROGRESS, 'w', encoding='utf-8') as f: json.dump(p, f, indent=2)

def run_level(conn, level, account_filter=None, reset=False, dry_run=False):
    cfg = LEVEL_CFG[level]
    table = cfg['table']
    upsert_cols = cfg['cols']

    end = date.today() - timedelta(days=1)          # yesterday (today's reach is unstable)
    if end < SNAPSHOT_START:
        log(f"[{level}] end < BASELINE, nothing to do"); return
    n_days = (end - SNAPSHOT_START).days + 1
    log(f"[{level}] window {SNAPSHOT_START} → {end} ({n_days} days), table={table}")

    prog = {} if reset else _load_prog()
    done_key = f"{level}"
    done = set(prog.get(done_key, []))

    accts = [a for a in ACCOUNTS if not account_filter or a[0] == account_filter]
    if not accts:
        log(f"[{level}] no matching account for {account_filter}"); return

    for account_id, account_name in accts:
        log(f"[{level}] account: {account_name} ({account_id})")

        d = SNAPSHOT_START
        while d <= end:
            marker = f"{account_id}|{d.isoformat()}"
            if marker in done:
                d += timedelta(days=1); continue

            log(f"  {d.isoformat()} …")
            rows, err = fetch_level_for_date(account_id, level, d)
            if err:
                log(f"    [!] error: {scrub(err)[:200]} — will retry next run")
                d += timedelta(days=1); continue

            if not rows:
                log("    (no data)")
                done.add(marker); prog[done_key] = sorted(done)
                if not dry_run: _save_prog(prog)
                d += timedelta(days=1); continue

            values = []
            for r in rows:
                reach = int(float(r.get('reach') or 0))
                spend = float(r.get('spend') or 0)
                impressions = int(float(r.get('impressions') or 0))
                if level == 'account':
                    values.append((account_id, account_name, d, reach, spend, impressions))
                elif level == 'campaign':
                    values.append((r.get('campaign_id'), r.get('campaign_name'),
                                   account_id, account_name, d, reach, spend, impressions))
                elif level == 'adset':
                    values.append((r.get('adset_id'), r.get('adset_name'),
                                   r.get('campaign_id'), r.get('campaign_name'),
                                   account_id, account_name, d, reach, spend, impressions))
                elif level == 'ad':
                    values.append((r.get('ad_id'), r.get('ad_name'),
                                   r.get('adset_id'), r.get('adset_name'),
                                   r.get('campaign_id'), r.get('campaign_name'),
                                   account_id, account_name, d, reach, spend, impressions))

            if dry_run:
                log(f"    [dry-run] would upsert {len(values)} rows")
            else:
                cols_sql = ', '.join(upsert_cols)
                pk = 'account_id, date' if level == 'account' else \
                     f'{level}_id, date'
                update_cols = [c for c in upsert_cols if c not in (pk.split(', '))]
                update_sql = ', '.join(f"{c} = EXCLUDED.{c}" for c in update_cols) + \
                             ", fetched_at = now()"
                sql = f"""INSERT INTO public.{table} ({cols_sql})
                          VALUES %s
                          ON CONFLICT ({pk}) DO UPDATE SET {update_sql}"""
                with conn.cursor() as cur:
                    execute_values(cur, sql, values, page_size=500)
                conn.commit()
                log(f"    upserted {len(values)} rows")

            done.add(marker); prog[done_key] = sorted(done)
            if not dry_run: _save_prog(prog)
            d += timedelta(days=1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--level', choices=['account','campaign','adset','ad','all'], default='all')
    ap.add_argument('--account', help='Restrict to one ad account id')
    ap.add_argument('--reset', action='store_true', help='Ignore progress file, restart')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    conn = None if args.dry_run else psycopg2.connect(DB_URL, connect_timeout=30, keepalives=1)

    t0 = time.time()
    levels = ['account','campaign','adset','ad'] if args.level == 'all' else [args.level]
    for lvl in levels:
        run_level(conn, lvl, args.account, args.reset, args.dry_run)
    log(f"\n[DONE] total wall time {(time.time()-t0)/60:.1f} min")

    if conn: conn.close()

if __name__ == '__main__':
    main()
