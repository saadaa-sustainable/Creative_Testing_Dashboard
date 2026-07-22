"""
fetch_ireach_cumulative.py — populate ireach_cumulative_daily with Meta's
CUMULATIVE unique reach at account / campaign / adset level.

For each (account, date) pair, calls Meta's /act_{id}/insights endpoint
three times (level=account/campaign/adset) with a growing time_range
[ORIGIN, date]. Meta returns unique reach across that whole range —
CANNOT be derived by summing daily reaches (users overlap).

Sheet-style incremental reach then falls out as:
    incr_reach(D)  =  cumulative(D)  -  cumulative(D-1)
    cost/1k_incr   =  daily_spend × 1000 / incr_reach

Where daily_spend + daily_purchases live in primary_camp_table /
primary_adset_table (already fetched by fetch_adset_camp_reach.py).

USAGE
  python fetch_ireach_cumulative.py                   # full incremental
  python fetch_ireach_cumulative.py --level account   # account only (fast)
  python fetch_ireach_cumulative.py --level campaign  # + campaigns
  python fetch_ireach_cumulative.py --level adset     # + adsets
  python fetch_ireach_cumulative.py --account 1136644150469466
  python fetch_ireach_cumulative.py --from 2025-06-01 --to 2025-07-22
  python fetch_ireach_cumulative.py --dry-run         # probe token + count only
  python fetch_ireach_cumulative.py --reset           # ignore progress file

RATE LIMITS
  • 0.25s throttle between requests
  • X-App-Usage ≥ 75% → sleep 300s
  • HTTP 400 "too many calls" → exponential backoff 90/180/270s
  • Resumable via .ireach_cum.progress.json (per account × date × level)

SAFETY
  • Read-only on primary_ad_ae / primary_camp_table (used only for lookups)
  • Windows keep-awake (SetThreadExecutionState) so long runs survive lid-close
  • Token never logged; regex-scrub on every write
"""
from __future__ import annotations
import os, sys, io, json, re, time, argparse, requests, psycopg2
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from psycopg2.extras import execute_values

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='backslashreplace')

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

# Anchor for the growing time_range — dashboard's HISTORIC_CUTOFF.
# Anything before this date isn't shown in the frontend anyway.
ORIGIN = '2025-01-01'

# 3 Meta ad accounts (id → display name). Ids match ireach_campaign_daily.
ACCOUNTS = [
    ('1136644150469466', 'Raho Saadaa'),
    ('1349767139294217', 'Fourth Ad Account - SD'),
    ('264868699479122',  'Third Ad Account - SD'),
]

_TOK = re.compile(r'(?:EAA[A-Za-z0-9]{30,}|IGQ[\w\-]{20,}|eyJ[\w\-.]{40,})')
def scrub(s):
    s = _TOK.sub('<REDACTED>', str(s or ''))
    return re.sub(r'(access_token=)[^&\s\"]+', r'\1<REDACTED>', s)

LOG_FILE = 'logs/ireach_cumulative.log'
PROGRESS = '.ireach_cum.progress.json'
def log(*a):
    msg = ' '.join(scrub(str(x)) for x in a)
    print(msg, flush=True)
    os.makedirs('logs', exist_ok=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f: f.write(msg + '\n')

def _sleep_if_throttled(headers, success):
    """Cooperative back-off based on Meta's X-App-Usage header.

    Sleep tiers (only kick in when the current response was NOT successful,
    otherwise we'd throw away progress). Success responses always let the
    data land; back-off only applies to the NEXT call:
       ≥ 95% → sleep 300s  (very close to hard limit, back way off)
       ≥ 90% → sleep 60s   (approaching limit, brief pause)
       ≥ 80% → sleep 15s   (elevated, mild pause)
       < 80% → no pause
    """
    max_pct = 0
    try:
        for k in ('x-app-usage', 'x-business-use-case-usage'):
            v = headers.get(k)
            if not v: continue
            j = json.loads(v) if isinstance(v, str) else v
            def walk(x):
                if isinstance(x, dict):
                    for kk, vv in x.items(): yield from walk(vv)
                elif isinstance(x, list):
                    for vv in x: yield from walk(vv)
                elif isinstance(x, (int, float)) and 0 <= x <= 100:
                    yield x
            for n in walk(j):
                if n > max_pct: max_pct = n
    except Exception: pass
    # Only pause AFTER we've handled the response (data has been extracted)
    if max_pct >= 95:
        log(f"  [throttle {max_pct}%] sleeping 300s (critical)")
        time.sleep(300)
    elif max_pct >= 90:
        log(f"  [throttle {max_pct}%] sleeping 60s")
        time.sleep(60)
    elif max_pct >= 80:
        log(f"  [throttle {max_pct}%] sleeping 15s")
        time.sleep(15)
    return max_pct

def _get(url, params, retries=6):
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=45)
        except requests.RequestException as e:
            log(f"    [net {attempt}] {type(e).__name__}: {str(e)[:120]} — sleeping {delay}s")
            time.sleep(delay); delay = min(delay*2, 120); continue
        if r.status_code == 200:
            # extract the payload FIRST so a subsequent throttle-sleep
            # doesn't cost us the data we just paid for
            data = r.json()
            _sleep_if_throttled(r.headers, success=True)
            return data
        _sleep_if_throttled(r.headers, success=False)
        try: j = r.json()
        except Exception: j = {}
        err = (j.get('error') or {})
        emsg = err.get('message') or r.text[:200]
        code = err.get('code')
        # transient / rate-limit
        if r.status_code in (429, 500, 502, 503, 504) or code in (4, 17, 32) or 'too many calls' in str(emsg).lower():
            log(f"    [throttle {attempt}] {scrub(emsg)[:120]} — sleeping {delay}s")
            time.sleep(delay); delay = min(delay*2, 300); continue
        log(f"    [!] HTTP {r.status_code} {scrub(emsg)[:200]}")
        return {'__error__': emsg}
    return {'__error__': 'exhausted retries'}

def fetch_level_cumulative(account_id, level, until_date):
    """One Meta call — returns cumulative reach per entity at the given level
    over [ORIGIN, until_date]. Paginates until all entities are returned."""
    url = f'{API}/act_{account_id}/insights'
    if level == 'account':
        fields = 'reach'
    elif level == 'campaign':
        fields = 'campaign_id,campaign_name,reach'
    elif level == 'adset':
        fields = 'campaign_id,campaign_name,adset_id,adset_name,reach'
    elif level == 'ad':
        fields = 'ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name,reach'
    else:
        raise ValueError(f'Unknown level {level}')
    params = {
        'level': level,
        'fields': fields,
        'time_range': json.dumps({'since': ORIGIN, 'until': until_date}),
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

UPSERT_SQL = """
INSERT INTO public.ireach_cumulative_daily
    (level, entity_id, entity_name, account_id, account_name, date, cumulative_reach)
VALUES %s
ON CONFLICT (level, entity_id, date) DO UPDATE SET
    entity_name      = EXCLUDED.entity_name,
    account_name     = EXCLUDED.account_name,
    cumulative_reach = EXCLUDED.cumulative_reach,
    fetched_at       = NOW()
"""

def _save_prog(state):
    with open(PROGRESS, 'w', encoding='utf-8') as f: json.dump(state, f)
def _load_prog():
    if not os.path.exists(PROGRESS): return {}
    try:
        with open(PROGRESS, encoding='utf-8') as f: return json.load(f)
    except Exception: return {}

def daterange(d_from: date, d_to: date):
    cur = d_from
    while cur <= d_to:
        yield cur
        cur += timedelta(days=1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--level', choices=['account','campaign','adset','ad','all'], default='all')
    ap.add_argument('--account', help='Restrict to one account_id')
    ap.add_argument('--from', dest='date_from', default=ORIGIN)
    ap.add_argument('--to',   dest='date_to',   default=date.today().isoformat())
    ap.add_argument('--reset', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--batch',  type=int, default=10)
    args = ap.parse_args()

    d_from = datetime.strptime(args.date_from, '%Y-%m-%d').date()
    d_to   = datetime.strptime(args.date_to,   '%Y-%m-%d').date()
    if d_from < datetime.strptime(ORIGIN, '%Y-%m-%d').date():
        log(f"  [note] clamping --from to ORIGIN {ORIGIN}")
        d_from = datetime.strptime(ORIGIN, '%Y-%m-%d').date()

    levels = ['account','campaign','adset','ad'] if args.level == 'all' else [args.level]
    accts = [(i,n) for (i,n) in ACCOUNTS if not args.account or i == args.account]
    if not accts: sys.exit(f'No matching account for {args.account}')

    n_dates = (d_to - d_from).days + 1
    total_calls = len(accts) * n_dates * len(levels)
    log(f"[*] token tail …{TOK[-6:]}   accounts={len(accts)}   levels={levels}")
    log(f"[*] date range {d_from} → {d_to} ({n_dates} dates)")
    log(f"[*] estimated Meta API calls: {total_calls:,}   (@ ~1 call/sec ≈ {total_calls/3600:.1f}h)")
    if args.dry_run:
        log("[dry-run] not fetching; run without --dry-run to start.")
        return

    prog = {} if args.reset else _load_prog()

    def _connect():
        """Robust connect — Supabase pooler drops long-idle conns during Meta
        throttle sleeps. Exponential backoff up to ~35 min so a multi-hour
        fetch survives intermittent pooler outages."""
        delay = 2
        for attempt in range(1, 12):
            try:
                c = psycopg2.connect(DB_URL, connect_timeout=15)
                c.autocommit = False
                return c
            except psycopg2.OperationalError as e:
                log(f"  [db retry {attempt}] {type(e).__name__}: {str(e)[:80]} — sleeping {delay}s")
                time.sleep(delay); delay = min(delay*2, 300)
        raise psycopg2.OperationalError('gave up after 11 reconnect attempts')

    conn = _connect()
    t0 = time.time()
    total_upserts = 0
    total_call_i  = 0

    def _flush(batch_rows):
        """Upsert with retry-on-disconnect. Reopens conn if the pooler dropped
        us during a long Meta throttle sleep."""
        nonlocal conn
        if not batch_rows: return
        for attempt in range(3):
            try:
                with conn.cursor() as cur:
                    execute_values(cur, UPSERT_SQL, batch_rows, page_size=200)
                conn.commit()
                return
            except psycopg2.Error as e:
                log(f"  [db retry] flush {attempt+1}: {type(e).__name__}: {str(e)[:80]} — reconnecting")
                try: conn.close()
                except Exception: pass
                conn = _connect()
        log(f"  [!] flush failed 3× — dropping {len(batch_rows)} rows")

    for (acct_id, acct_name) in accts:
        for level in levels:
            key = f"{acct_id}|{level}"
            done_dates = set(prog.get(key, []))
            batch = []
            log(f"\n══ {acct_name}  ({acct_id})  level={level} ══")
            for d in daterange(d_from, d_to):
                d_str = d.isoformat()
                if d_str in done_dates: continue
                rows, err = fetch_level_cumulative(acct_id, level, d_str)
                total_call_i += 1
                if err:
                    log(f"  [err] {d_str} — {scrub(err)[:120]}")
                    time.sleep(0.25)
                    continue
                for row in rows:
                    reach = int(row.get('reach') or 0)
                    if level == 'account':
                        entity_id = acct_id
                        entity_name = acct_name
                    elif level == 'campaign':
                        entity_id = row.get('campaign_id') or ''
                        entity_name = row.get('campaign_name') or ''
                    elif level == 'adset':
                        entity_id = row.get('adset_id') or ''
                        entity_name = row.get('adset_name') or ''
                    else:  # ad
                        entity_id = row.get('ad_id') or ''
                        entity_name = row.get('ad_name') or ''
                    if not entity_id: continue
                    batch.append((level, entity_id, entity_name, acct_id, acct_name, d_str, reach))
                done_dates.add(d_str)
                if total_call_i % 25 == 0:
                    elapsed = time.time() - t0
                    rate = total_call_i / max(elapsed, 1)
                    eta_s = (total_calls - total_call_i) / max(rate, 0.001)
                    log(f"  [{total_call_i:>5}/{total_calls:>5}]  {d_str}  "
                        f"rows={len(rows):>3}  rate={rate:.2f}/s  eta={eta_s/60:.0f}m")
                if len(batch) >= args.batch:
                    _flush(batch)
                    total_upserts += len(batch); batch.clear()
                    prog[key] = list(done_dates); _save_prog(prog)
                time.sleep(0.25)
            if batch:
                _flush(batch)
                total_upserts += len(batch); batch.clear()
            prog[key] = list(done_dates); _save_prog(prog)

    conn.close()
    log('\n' + '─' * 70)
    log(f"[ok] complete · upserts={total_upserts:,} · calls={total_call_i:,} · "
        f"elapsed={(time.time()-t0)/60:.1f} min")

if __name__ == '__main__':
    main()
