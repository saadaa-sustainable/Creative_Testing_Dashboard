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

# Full metric universe per Meta v22 docs:
#   https://developers.facebook.com/docs/instagram-platform/reference/instagram-media/insights
# Metrics differ by media_product_type; Meta rejects requests that include
# unsupported metrics for the given type, so we tailor per type. Any metric
# rejected at runtime is dropped (see _parse_meta_err) and the response is
# retried — so this list is the WIDEST plausible bundle.
_METRICS_FEED = [
    'reach','views','likes','saved','comments','shares','total_interactions',
    'follows','profile_visits','profile_activity','impressions',
    'reposts','total_comments','total_likes','total_views',
    'facebook_views',
]
_METRICS_REELS = [
    'reach','views','likes','saved','comments','shares','total_interactions',
    'ig_reels_avg_watch_time','ig_reels_video_view_total_time',
    'crossposted_views','reels_skip_rate',
    'reposts','total_comments','total_likes','total_views',
    'facebook_views',
]
_METRICS_STORY = [
    'reach','views','total_interactions','replies','shares',
    'follows','profile_visits','profile_activity','impressions',
    'navigation','link_clicks','reposts','total_views','facebook_views',
]
_METRICS_AD = ['reach','impressions']

METRICS_BY_TYPE = {
    'REELS':          ','.join(_METRICS_REELS),
    'FEED':           ','.join(_METRICS_FEED),
    'IMAGE':          ','.join(_METRICS_FEED),
    'VIDEO':          ','.join(_METRICS_FEED),
    'CAROUSEL_ALBUM': ','.join(_METRICS_FEED),
    'STORY':          ','.join(_METRICS_STORY),
    'AD':             ','.join(_METRICS_AD),
}
FALLBACK_METRIC_SET = 'reach,views'

# Metrics that Meta returns as {breakdown_key: value} rather than a scalar.
# We store the sum as the primary numeric AND keep the raw dict in *_breakdown.
_BREAKDOWN_METRICS = {'navigation', 'profile_activity'}

# Regex helpers for parsing metric-name errors (Meta returns text like
# "(#100) metric[reels_skip_rate] must be one of ..." or "does not support
# the X metric" or "the impressions metric is no longer supported").
_RE_MUST_BE_ONE_OF    = re.compile(r'must be one of the following values:\s*([\w_, ]+?)(?:\.|$)', re.IGNORECASE)
_RE_DOES_NOT_SUPPORT  = re.compile(r'does not support (?:the )?(.+?) metric', re.IGNORECASE)
_RE_NO_LONGER_SUPPORT = re.compile(r'the\s+([\w_]+)\s+metric\s+is\s+no\s+longer\s+supported', re.IGNORECASE)
_RE_INVALID_METRIC    = re.compile(r'metric\[([\w_]+)\]', re.IGNORECASE)

def _parse_meta_metric_err(msg):
    """Return list of unsupported/invalid metric names to drop from the request."""
    if not msg: return []
    drop = []
    m = _RE_DOES_NOT_SUPPORT.search(msg)
    if m: drop += [t.strip() for t in m.group(1).split(',') if t.strip()]
    m = _RE_NO_LONGER_SUPPORT.search(msg)
    if m: drop.append(m.group(1).strip())
    m = _RE_INVALID_METRIC.search(msg)
    if m: drop.append(m.group(1).strip())
    return drop

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
# /media walk: scalar fields only. Nested edges (owner, children, collaborators,
# copyright_check_information, boost_eligibility_info) blow up the response
# size when × 100 media/page and cause "reduce the amount of data" 400s.
# Those fields are fetched per-media in _fetch_extra_fields() below.
BASE_FIELDS_FULL = (
    'id,timestamp,permalink,media_url,thumbnail_url,shortcode,username,'
    'media_type,media_product_type,caption,alt_text,'
    'is_ai_generated,is_comment_enabled,is_shared_to_feed,'
    'media_audio_type,legacy_instagram_media_id,'
    'like_count,comments_count,reposts_count,saved_count,shares_count,'
    'total_comments_count,total_like_count,total_views_count'
)
# view_count omitted — permission-gated ("outside Business Discovery API")
# for this token. `views` (from /insights) and `total_views_count` cover
# the same numbers.
# Fallback if even the scalar set is rejected (e.g., a newly-added Meta field
# starts 400'ing the whole request).
BASE_FIELDS_SAFE = (
    'id,timestamp,permalink,media_url,thumbnail_url,shortcode,username,'
    'media_type,media_product_type,caption,is_shared_to_feed'
)
# Per-media follow-up for the nested edges. Cheaper as a per-item call.
EXTRA_FIELDS = 'owner{id},children{id},collaborators{id,username},copyright_check_information,boost_eligibility_info'

def _fetch_extra_fields(media_id):
    """Fetch nested-edge fields for a single media. Returns dict or {}."""
    j = _get(f'{API}/{media_id}', {'fields': EXTRA_FIELDS, 'access_token': TOK})
    if not j or '__error__' in j: return {}
    return j

# Per-account cache of the converged field list — probes down on first
# failure, then re-uses for all subsequent pages.
_BASE_FIELDS_CACHE = {}   # ig_id -> str

def _walk_media(ig_id):
    """Yield every media (with base fields) for one IG business account.
    Uses the widest field set first; on 400 caches the safe fallback for
    the remainder of the walk."""
    fields = _BASE_FIELDS_CACHE.get(ig_id) or BASE_FIELDS_FULL
    url = f'{API}/{ig_id}/media'
    params = {'fields': fields, 'limit': 100, 'access_token': TOK}
    while url:
        j = _get(url, params)
        if not j or '__error__' in j:
            err = (j or {}).get('__error__','')
            # If the wide field set was rejected, retry once with the safe set
            if fields is BASE_FIELDS_FULL:
                log(f"  [walk] wide fields rejected — falling back to safe set  "
                    f"({scrub(err)[:100]})")
                fields = BASE_FIELDS_SAFE
                _BASE_FIELDS_CACHE[ig_id] = fields
                params = {'fields': fields, 'limit': 100, 'access_token': TOK}
                continue
            log(f"  [walk] account {ig_id} failed: {scrub(err)[:120] if err else 'no response'}")
            return
        _BASE_FIELDS_CACHE[ig_id] = fields
        for m in j.get('data', []) or []:
            yield m
        nxt = (j.get('paging') or {}).get('next')
        if not nxt: return
        url, params = nxt, None
        time.sleep(0.25)

# Per-type cache of the converged metric list — first media of a given kind
# probes / drops unsupported metrics, subsequent media reuse the working set.
_ALLOWED_CACHE = {}   # media_product_type -> list[str]

def _fetch_insights(media_id, media_product_type):
    """Fetch insights adaptively — start with the widest per-type bundle;
    on '(#100) metric[X] must be…' or 'does not support X' errors, drop the
    offending metric(s) and retry. Cache the converged set per media_product_type.

    Returns dict with:
      - scalar metrics mapped to name -> number
      - breakdown metrics (navigation, profile_activity) mapped to:
          name          -> summed total (int)
          name + "_bd"  -> raw dict for JSONB storage
    """
    key = (media_product_type or '').upper() or 'FEED'
    default_metrics = (METRICS_BY_TYPE.get(key) or FALLBACK_METRIC_SET).split(',')
    cached = _ALLOWED_CACHE.get(key)
    metrics = list(cached) if cached else list(default_metrics)

    out = {}
    cache_safe = True   # flip to False on unknown-shape errors so a per-media
                        # emergency shrink doesn't poison the shared cache
    for attempt in range(5):
        if not metrics:
            return out
        j = _get(f'{API}/{media_id}/insights',
                 {'metric': ','.join(metrics), 'access_token': TOK})
        if j and '__error__' not in j:
            # Only update cache if the converged set was reached via structured
            # metric-drop errors (i.e., Meta explicitly named unsupported
            # metrics). If we got here after an unknown-shape fallback, the
            # reduced set is probably only right for THIS media.
            if cache_safe:
                _ALLOWED_CACHE[key] = list(metrics)
            for m in j.get('data', []) or []:
                name = m.get('name')
                vals = m.get('values') or []
                if not vals: continue
                v = vals[0].get('value')
                if isinstance(v, dict):
                    total = sum(int(x) for x in v.values() if isinstance(x, (int, float)))
                    out[name] = total
                    out[name + '_bd'] = v
                else:
                    out[name] = v
            return out
        # error path — parse and drop unsupported metrics
        emsg = (j or {}).get('__error__', '')
        drop = _parse_meta_metric_err(emsg)
        if drop:
            before = len(metrics)
            metrics = [m for m in metrics if m not in drop]
            if len(metrics) == before:
                # Meta named something we don't have; strip breakdown metrics
                metrics = [m for m in metrics if m not in _BREAKDOWN_METRICS]
        else:
            # Unknown shape — try emergency minimum, but flag not to cache it
            cache_safe = False
            metrics = ['reach','views','total_interactions']

    # Last-ditch: fallback set
    j2 = _get(f'{API}/{media_id}/insights',
              {'metric': FALLBACK_METRIC_SET, 'access_token': TOK})
    if j2 and '__error__' not in j2:
        for m in j2.get('data', []) or []:
            name = m.get('name')
            vals = m.get('values') or []
            if vals: out[name] = vals[0].get('value')
    return out

def _fetch_boost_state(media_id, bei=None):
    """Return (status, boost_ads_count).  status = 'boosted' if boost_ads_list
    is non-empty OR boost_eligibility_info reports an active promotion; else
    'organic'.  Pass the media's boost_eligibility_info from the walk to skip
    the extra roundtrip (that field is already in BASE_FIELDS_FULL)."""
    # 1) boost_ads_list on the media (edge)
    j = _get(f'{API}/{media_id}/boost_ads_list', {'access_token': TOK})
    if j and '__error__' not in j:
        ads = j.get('data', []) or []
        if ads:
            return 'boosted', len(ads)
    # 2) boost_eligibility_info as a soft signal
    if bei is None:
        j = _get(f'{API}/{media_id}',
                 {'fields': 'boost_eligibility_info', 'access_token': TOK})
        if j and '__error__' not in j:
            bei = j.get('boost_eligibility_info') or {}
    bei = bei or {}
    ps = str(bei.get('promotion_status') or '').upper()
    if ps in ('ACTIVE', 'RUNNING', 'IN_REVIEW', 'SUBMITTED'):
        return 'boosted', 1
    return 'organic', 0

def _i(v):
    """Coerce to int-or-None (never 0-substitutes NULL — 0 = 'we asked, got 0';
    None = 'metric not supported for this media type')."""
    if v is None or v == '': return None
    try: return int(v)
    except (TypeError, ValueError):
        try: return int(float(v))
        except Exception: return None

def _f(v):
    if v is None or v == '': return None
    try: return float(v)
    except Exception: return None

def _bd(v):
    """Serialize breakdown dict to JSON string for Postgres jsonb insertion."""
    if not v or not isinstance(v, dict): return None
    return json.dumps(v)

def _b(v):
    """Coerce truthy/falsy to boolean-or-None."""
    if v is None or v == '': return None
    if isinstance(v, bool): return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ('true','t','1','yes','y'): return True
        if s in ('false','f','0','no','n'): return False
        return None
    return bool(v)

def _s(v):
    """Coerce to non-empty str-or-None."""
    if v is None: return None
    s = str(v).strip()
    return s if s else None

def _to_row(m, ig_username, ig_id, ins, status, boost_count):
    owner  = m.get('owner')  or {}
    kids   = m.get('children') or {}
    collab = m.get('collaborators') or {}
    cci    = m.get('copyright_check_information') or {}
    bei    = m.get('boost_eligibility_info')
    children_ids = None
    if isinstance(kids, dict) and kids.get('data'):
        children_ids = json.dumps([c.get('id') for c in kids['data'] if c.get('id')])
    collab_json = None
    if isinstance(collab, dict) and collab.get('data'):
        collab_json = json.dumps(collab['data'])
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
        # insight baseline scalars
        _i(ins.get('reach')),
        _i(ins.get('impressions')),
        _i(ins.get('views')),
        _i(ins.get('likes')),
        _i(ins.get('saved')),
        _i(ins.get('comments')),
        _i(ins.get('shares')),
        _i(ins.get('total_interactions')),
        status,
        boost_count,
        # insight v22 additions
        _i(ins.get('crossposted_views')),
        _i(ins.get('facebook_views')),
        _i(ins.get('follows')),
        _i(ins.get('ig_reels_avg_watch_time')),
        _i(ins.get('ig_reels_video_view_total_time')),
        _i(ins.get('link_clicks')),
        _i(ins.get('navigation')),
        _bd(ins.get('navigation_bd')),
        _i(ins.get('profile_activity')),
        _bd(ins.get('profile_activity_bd')),
        _i(ins.get('profile_visits')),
        _f(ins.get('reels_skip_rate')),
        _i(ins.get('replies')),
        _i(ins.get('reposts')),
        _i(ins.get('total_comments')),
        _i(ins.get('total_likes')),
        _i(ins.get('total_views')),
        # base /media node additions
        _s(m.get('alt_text')),
        _b(m.get('is_ai_generated')),
        _b(m.get('is_comment_enabled')),
        _s(m.get('media_audio_type')),
        _s(owner.get('id') if isinstance(owner, dict) else owner),
        _s(m.get('shortcode')),
        _s(m.get('username')),
        _s(m.get('legacy_instagram_media_id')),
        _b(m.get('is_shared_to_feed')),
        _i(m.get('like_count')),
        _i(m.get('comments_count')),
        _i(m.get('view_count')),
        _i(m.get('reposts_count')),
        _i(m.get('saved_count')),
        _i(m.get('shares_count')),
        _i(m.get('total_comments_count')),
        _i(m.get('total_like_count')),
        _i(m.get('total_views_count')),
        _s(cci.get('status') if isinstance(cci, dict) else None),
        json.dumps(cci) if isinstance(cci, dict) and cci else None,
        json.dumps(bei) if isinstance(bei, dict) and bei else None,
        children_ids,
        collab_json,
    )

UPSERT_COLS = [
    'media_id','ig_username','ig_business_id',
    'publish_date','permalink','media_url','thumbnail_url',
    'media_type','media_product_type','caption',
    'reach','impressions','views','likes','saved','comments','shares','total_interactions',
    'status','boost_ads_count',
    # insight v22 additions — order MUST match _to_row above
    'crossposted_views','facebook_views','follows',
    'ig_reels_avg_watch_time','ig_reels_video_view_total_time',
    'link_clicks','navigation','navigation_breakdown',
    'profile_activity','profile_activity_breakdown',
    'profile_visits','reels_skip_rate','replies','reposts',
    'total_comments','total_likes','total_views',
    # base /media node additions
    'alt_text','is_ai_generated','is_comment_enabled','media_audio_type',
    'owner_id','shortcode','username','legacy_instagram_media_id','is_shared_to_feed',
    'like_count','comments_count','view_count','reposts_count','saved_count','shares_count',
    'total_comments_count','total_like_count','total_views_count',
    'copyright_check_status','copyright_check_info','boost_eligibility_info',
    'children_ids','collaborators',
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
            # Pull nested edges (owner/children/collaborators/copyright/boost_eligibility)
            # per-media; walking the account with them inline blows past Meta's
            # response-size limit.
            extras = _fetch_extra_fields(mid)
            if extras: m.update(extras)
            status, boost_count = _fetch_boost_state(mid, m.get('boost_eligibility_info'))
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
