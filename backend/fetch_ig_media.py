"""
fetch_ig_media.py — walk /media for every saadaa IG Business Account,
pull per-post insights + media_product_type + boost_ads_list, upsert
into public.ig_media.

Accounts (hardcoded from the Business Discovery probe):
    saadaadesigns   17841412619002528
    saadaa_women    17841475090589771
    saadaa_men      17841469757332608

Rate-limit hygiene:
    - Meta's X-App-Usage / X-Business-Use-Case-Usage headers are parsed
      after every response; if any bucket ≥ 75 % we sleep 5 min.
    - Small (0.25 s) inter-request throttle keeps steady-state well
      below the limits.

Resumable via .ig_media_progress.json — the fetcher checkpoints after
every media so an interrupted run picks up where it left off.

USAGE
    python fetch_ig_media.py                 # full walk, all 3 accounts
    python fetch_ig_media.py --account saadaa_women --limit 50
    python fetch_ig_media.py --reset         # start fresh (drop progress file)
    python fetch_ig_media.py --dry-run       # fetch + summarise, no DB write
"""
from __future__ import annotations
import os, sys, io, json, re, time, argparse, requests, psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='backslashreplace')

# Windows keep-awake so long fetches survive lid-close / idle
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

_TOK = re.compile(r'(?:EAA[A-Za-z0-9]{30,}|IGQ[\w\-]{20,}|eyJ[\w\-.]{40,})')
def scrub(s): return _TOK.sub('<REDACTED>', str(s or ''))
LOG_FILE = 'logs/ig_media.log'
PROGRESS = '.ig_media_v2.progress.json'
def log(*a):
    msg = ' '.join(scrub(str(x)) for x in a)
    print(msg, flush=True)
    os.makedirs('logs', exist_ok=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f: f.write(msg + '\n')

IG_ACCOUNTS = [
    {'username': 'saadaadesigns', 'ig_id': '17841412619002528'},
    {'username': 'saadaa_women',  'ig_id': '17841475090589771'},
    {'username': 'saadaa_men',    'ig_id': '17841469757332608'},
]

# Metrics safe to request per media_product_type.  Meta rejects requests
# that include unsupported metrics for the given media type, so we tailor.
METRICS_BY_TYPE = {
    'REELS':           'reach,views,likes,saved,comments,shares,total_interactions',
    'FEED':            'reach,views,likes,saved,comments,shares,total_interactions',
    'IMAGE':           'reach,views,likes,saved,comments,shares,total_interactions',
    'VIDEO':           'reach,views,likes,saved,comments,shares,total_interactions',
    'CAROUSEL_ALBUM':  'reach,views,likes,saved,comments,shares,total_interactions',
    'STORY':           'reach,views,total_interactions',
    'AD':              'reach,impressions',
}
FALLBACK_METRIC_SET = 'reach,views'

# ── HTTP with retry + rate-limit backoff ───────────────────────────────
def _sleep_if_throttled(headers):
    """Look at X-App-Usage / X-Business-Use-Case-Usage; sleep 5 min if any
    bucket is above 75 %."""
    max_pct = 0
    try:
        for k in ('x-app-usage', 'x-business-use-case-usage'):
            v = headers.get(k)
            if not v: continue
            j = json.loads(v) if isinstance(v, str) else v
            if isinstance(j, dict):
                for entry in j.values() if not isinstance(list(j.values())[0], dict) else [j]:
                    if isinstance(entry, list):
                        for e in entry:
                            max_pct = max(max_pct, e.get('call_count', 0),
                                          e.get('total_cputime', 0), e.get('total_time', 0))
                    elif isinstance(entry, dict):
                        max_pct = max(max_pct, entry.get('call_count', 0),
                                      entry.get('total_cputime', 0), entry.get('total_time', 0))
    except Exception: pass
    if max_pct >= 75:
        log(f"  [throttle {max_pct}%] sleeping 300 s")
        time.sleep(300)

def _get(url, params, retries=6):
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=45)
        except requests.RequestException as e:
            log(f"    [net {attempt}] {type(e).__name__}: {str(e)[:120]} — sleeping {delay}s")
            time.sleep(delay); delay = min(delay*2, 120); continue
        _sleep_if_throttled(r.headers)
        if r.status_code == 200:
            return r.json()
        try: j = r.json()
        except Exception: j = {}
        err = (j.get('error') or {})
        code = err.get('code'); sub = err.get('error_subcode')
        emsg = err.get('message') or r.text[:200]
        # transient
        if r.status_code in (429, 500, 502, 503, 504) or code in (4, 17, 32) or 'throttle' in str(emsg).lower():
            log(f"    [throttle {attempt}] {scrub(emsg)[:120]} — sleeping {delay}s")
            time.sleep(delay); delay = min(delay*2, 300); continue
        # non-retriable — surface partial for the row builder to decide
        log(f"    [!] HTTP {r.status_code} {scrub(emsg)[:200]}")
        return {'__error__': emsg, '__code__': code, '__subcode__': sub}
    return {'__error__': 'exhausted retries'}

# ── Data shaping ───────────────────────────────────────────────────────
BASE_FIELDS = ('id,timestamp,permalink,media_url,thumbnail_url,'
               'media_type,media_product_type,caption,'
               'is_shared_to_feed')

def _walk_media(ig_id):
    """Yield every media_id (with base fields) for one IG business account."""
    url = f'{API}/{ig_id}/media'
    params = {'fields': BASE_FIELDS, 'limit': 100, 'access_token': TOK}
    while url:
        j = _get(url, params)
        if not j or '__error__' in j:
            log(f"  [walk] account {ig_id} failed: {scrub(j.get('__error__','')[:120]) if j else 'no response'}")
            return
        for m in j.get('data', []) or []:
            yield m
        nxt = (j.get('paging') or {}).get('next')
        if not nxt: return
        url, params = nxt, None
        time.sleep(0.25)

def _fetch_insights(media_id, media_product_type):
    metrics = METRICS_BY_TYPE.get(media_product_type or '', FALLBACK_METRIC_SET)
    j = _get(f'{API}/{media_id}/insights',
             {'metric': metrics, 'access_token': TOK})
    out = {}
    if j and '__error__' not in j:
        for m in j.get('data', []) or []:
            name = m.get('name')
            vals = m.get('values') or []
            if not vals: continue
            v = vals[0].get('value')
            if isinstance(v, dict):
                # Some metrics return a breakdown dict; sum
                v = sum(int(x) for x in v.values() if isinstance(x, (int, float)))
            out[name] = v
    elif j and j.get('__error__'):
        # Retry with a minimal metric set if the tailored one failed
        j2 = _get(f'{API}/{media_id}/insights',
                  {'metric': FALLBACK_METRIC_SET, 'access_token': TOK})
        if j2 and '__error__' not in j2:
            for m in j2.get('data', []) or []:
                name = m.get('name')
                vals = m.get('values') or []
                if vals: out[name] = vals[0].get('value')
    return out

def _fetch_boost_state(media_id):
    """Return (status, boost_ads_count).  status = 'boosted' if boost_ads_list
    is non-empty OR boost_eligibility_info reports an active promotion; else
    'organic'.  This runs off the IG side — cross-check with ad_creative_link
    from primary_table gives the fuller picture later."""
    # 1) boost_ads_list on the media (edge)
    j = _get(f'{API}/{media_id}/boost_ads_list', {'access_token': TOK})
    if j and '__error__' not in j:
        ads = j.get('data', []) or []
        if ads:
            return 'boosted', len(ads)
    # 2) boost_eligibility_info as a soft signal (rarely reports active status)
    j = _get(f'{API}/{media_id}',
             {'fields': 'boost_eligibility_info', 'access_token': TOK})
    if j and '__error__' not in j:
        bei = j.get('boost_eligibility_info') or {}
        # Some responses expose a promotion_status
        ps = str(bei.get('promotion_status') or '').upper()
        if ps in ('ACTIVE', 'RUNNING', 'IN_REVIEW', 'SUBMITTED'):
            return 'boosted', 1
    return 'organic', 0

def _to_row(m, ig_username, ig_id, ins, status, boost_count):
    return (
        m.get('id'),
        ig_username, ig_id,
        m.get('timestamp'),
        m.get('permalink'),
        m.get('media_url'),
        m.get('thumbnail_url'),
        m.get('media_type'),
        m.get('media_product_type'),
        m.get('caption'),
        int(ins.get('reach') or 0),
        int(ins.get('impressions') or 0),
        int(ins.get('views') or 0),
        int(ins.get('likes') or 0),
        int(ins.get('saved') or 0),
        int(ins.get('comments') or 0),
        int(ins.get('shares') or 0),
        int(ins.get('total_interactions') or 0),
        status,
        boost_count,
    )

UPSERT_COLS = [
    'media_id','ig_username','ig_business_id',
    'publish_date','permalink','media_url','thumbnail_url',
    'media_type','media_product_type','caption',
    'reach','impressions','views','likes','saved','comments','shares','total_interactions',
    'status','boost_ads_count',
]
_update = ','.join(f'{c}=EXCLUDED.{c}' for c in UPSERT_COLS if c != 'media_id')
# synced_at column has a DEFAULT NOW() on the table, so we let the DB
# supply it on INSERT and override it explicitly on UPDATE.
UPSERT_SQL = f"""
INSERT INTO public.ig_media ({','.join(UPSERT_COLS)})
VALUES %s
ON CONFLICT (media_id) DO UPDATE SET
  {_update},
  synced_at = NOW()
"""

def _save_prog(state):
    with open(PROGRESS, 'w', encoding='utf-8') as f: json.dump(state, f)
def _load_prog():
    if not os.path.exists(PROGRESS): return {'done': {}}
    try:
        with open(PROGRESS, encoding='utf-8') as f: return json.load(f)
    except Exception: return {'done': {}}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--account', help='Restrict to one IG username')
    ap.add_argument('--limit', type=int, default=0, help='Cap per account (0 = all)')
    ap.add_argument('--reset', action='store_true', help='Ignore progress file, start fresh')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--batch', type=int, default=100)
    args = ap.parse_args()

    accts = [a for a in IG_ACCOUNTS if not args.account or a['username'] == args.account]
    if not accts: sys.exit(f"No matching account for --account={args.account}")

    prog = {'done': {}} if args.reset else _load_prog()
    done_by_acct = prog.get('done', {})

    conn = None if args.dry_run else psycopg2.connect(DB_URL, connect_timeout=30)
    t0 = time.time()
    total_upserts = 0

    for acct in accts:
        uname, ig_id = acct['username'], acct['ig_id']
        log(f"\n══ IG account: {uname}   business_id={ig_id} ══")
        seen_this_acct = set(done_by_acct.get(uname, []))
        batch = []
        count = 0

        for m in _walk_media(ig_id):
            mid = m.get('id')
            if not mid: continue
            if mid in seen_this_acct:
                continue
            count += 1
            if args.limit and count > args.limit:
                log(f"  reached --limit {args.limit}, stopping account")
                break

            mpt = m.get('media_product_type')
            ins = _fetch_insights(mid, mpt)
            status, boost_count = _fetch_boost_state(mid)
            row = _to_row(m, uname, ig_id, ins, status, boost_count)
            batch.append(row)
            seen_this_acct.add(mid)

            if count % 20 == 0:
                log(f"  scanned {count} media · sample: {mid} · {status} · reach={ins.get('reach','?')}")

            # Flush every batch_size rows
            if len(batch) >= args.batch:
                if not args.dry_run:
                    with conn.cursor() as cur:
                        execute_values(cur, UPSERT_SQL, batch, page_size=200)
                    conn.commit()
                total_upserts += len(batch)
                batch.clear()
                done_by_acct[uname] = list(seen_this_acct)
                prog['done'] = done_by_acct
                _save_prog(prog)

            time.sleep(0.1)  # tiny inter-request cushion

        if batch:
            if not args.dry_run:
                with conn.cursor() as cur:
                    execute_values(cur, UPSERT_SQL, batch, page_size=200)
                conn.commit()
            total_upserts += len(batch)
        done_by_acct[uname] = list(seen_this_acct)
        prog['done'] = done_by_acct
        _save_prog(prog)
        log(f"  done account {uname}: {count} media processed")

    if conn: conn.close()
    log('\n' + '─' * 70)
    log(f"[ok] fetch_ig_media complete · upserts={total_upserts:,} · "
        f"elapsed={(time.time()-t0)/60:.1f} min"
        + (" (dry-run)" if args.dry_run else ''))

if __name__ == '__main__':
    main()
