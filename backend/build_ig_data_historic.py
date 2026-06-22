"""
build_ig_data_historic.py — for each unique username in
saadaa-creatorhub.cleaned_data, call Meta Business Discovery and upsert
the result into ig_data_historic.

Logic:
  - canonical_existing_pid = most-frequent profile_id for that username in cleaned_data
  - meta_pid               = id Meta returns for the handle
  - if existing is NULL   →  profile_id = meta_pid,  not_matched_profile_id = NULL
  - if existing == meta   →  profile_id = existing,  not_matched_profile_id = NULL
  - if existing != meta   →  profile_id = existing,  not_matched_profile_id = meta_pid
  - if Meta fails (403/personal/etc): row written with status + error;
    profile_id stays whatever cleaned_data had.

USAGE
  # Dry-run 100 handles, NO writes (just print + time)
  python build_ig_data_historic.py --limit 100 --no-write

  # Dry-run 100 with writes (real data, small scope)
  python build_ig_data_historic.py --limit 100

  # Full run, resumes by skipping handles already in ig_data_historic
  python build_ig_data_historic.py

  # Refetch even handles already present
  python build_ig_data_historic.py --refresh
"""
import os, sys, io, re, json, time, argparse, datetime as dt
from pathlib import Path
import requests
from dotenv import load_dotenv

try: sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="backslashreplace")
except Exception: pass

# Keep Windows awake for the lifetime of this process so a closed laptop /
# idle session does not suspend mid-batch. SetThreadExecutionState with
# ES_CONTINUOUS | ES_SYSTEM_REQUIRED tells the OS "do not enter sleep
# while this thread is alive." Clears automatically when the process exits.
if os.name == "nt":
    try:
        import ctypes
        ES_CONTINUOUS       = 0x80000000
        ES_SYSTEM_REQUIRED  = 0x00000001
        ES_AWAYMODE_REQUIRED= 0x00000040
        ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED
        )
    except Exception: pass

load_dotenv()
META_TOKEN = os.environ["META_ACCESS_TOKEN"].strip()
META_VER   = os.getenv("META_API_VERSION", "v22.0").strip()
META_SRC   = os.getenv("IG_BUSINESS_ID", "").strip()
CH_URL     = os.environ["CREATOR_HUB_URL"].strip().rstrip("/")
CH_KEY     = os.environ["CREATOR_HUB_ACCESS"].strip()

LOG_FILE = "build_ig_data_historic.log"
PROGRESS = ".ig_historic_progress.json"
TOK_RE = re.compile(r"(?:EAA[a-zA-Z0-9]{30,}|shpa_[a-zA-Z0-9]{20,}|IGQ[\w\-]{20,}|eyJ[\w\-\.]{40,})")
def scrub(s):
    if s is None: return s
    try: return TOK_RE.sub("<REDACTED>", str(s))
    except Exception: return "<scrub_err>"
def log(*a):
    msg = " ".join(scrub(x) for x in a)
    try: print(msg, flush=True)
    except UnicodeEncodeError:
        enc = sys.stdout.encoding or "ascii"
        print(msg.encode(enc, errors="replace").decode(enc, errors="replace"), flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f: f.write(msg + "\n")

# REST headers for the creator-hub Supabase project
CH = {"apikey": CH_KEY, "Authorization": f"Bearer {CH_KEY}", "Content-Type": "application/json"}

# ── 1. discover IG_BUSINESS_ID if not in env ─────────────────────────────────
def discover_src_ig():
    r = requests.get(f"https://graph.facebook.com/{META_VER}/debug_token",
                     params={"input_token": META_TOKEN, "access_token": META_TOKEN}, timeout=60)
    if r.status_code != 200: return None
    ig_ids = []
    for gs in (r.json().get("data") or {}).get("granular_scopes", []):
        if gs.get("scope") == "instagram_basic": ig_ids = gs.get("target_ids") or []
    best, best_f = None, -1
    for ig in ig_ids:
        rr = requests.get(f"https://graph.facebook.com/{META_VER}/{ig}",
                          params={"fields": "id,followers_count", "access_token": META_TOKEN}, timeout=30)
        if rr.status_code != 200: continue
        f = int(rr.json().get("followers_count") or 0)
        if f > best_f: best, best_f = rr.json().get("id"), f
    return best

# ── 2. read deduped (username, canonical existing pid) from cleaned_data ─────
def load_handle_list():
    """Page through cleaned_data (1000 rows/page — PostgREST default cap) and
    reduce to one row per LOWER(username), taking the most-frequent non-null
    profile_id."""
    rows = []; offset = 0; PAGE = 1000
    while True:
        r = requests.get(f"{CH_URL}/rest/v1/cleaned_data",
                         headers=CH,
                         params={"select": "username,profile_id",
                                 "limit": PAGE, "offset": offset,
                                 "order": "id.asc"},
                         timeout=120)
        if r.status_code not in (200, 206):
            log(f"[fatal] cleaned_data read failed HTTP {r.status_code}: {scrub(r.text[:200])}"); sys.exit(1)
        batch = r.json()
        rows.extend(batch)
        if len(batch) < PAGE: break
        offset += PAGE
    log(f"[ok] read {len(rows):,} rows from cleaned_data")
    # Dedupe: per LOWER(username), pick mode(profile_id) ignoring blanks
    from collections import defaultdict, Counter
    bucket = defaultdict(Counter)
    raw_username = {}   # keep original-case display
    for row in rows:
        u_raw = (row.get("username") or "").strip().lstrip("@").strip()
        if not u_raw: continue
        u = u_raw.lower()
        raw_username.setdefault(u, u_raw)
        pid = (row.get("profile_id") or "").strip()
        if pid: bucket[u][pid] += 1
    handles = []
    for u in sorted(raw_username.keys()):
        c = bucket.get(u)
        canonical_pid = c.most_common(1)[0][0] if c else None
        handles.append({"username": raw_username[u], "existing_pid": canonical_pid})
    log(f"[ok] deduped to {len(handles):,} unique usernames")
    return handles

# ── 3. read existing ig_data_historic usernames where status='ok' ────────────
# (We skip already-successful rows on resume; error / rate_limited rows are
#  re-fetched so transient failures heal on the next pass.)
def load_existing_done():
    done = set(); offset = 0; PAGE = 1000
    while True:
        r = requests.get(f"{CH_URL}/rest/v1/ig_data_historic",
                         headers=CH,
                         params={"select": "username", "status": "eq.ok",
                                 "limit": PAGE, "offset": offset,
                                 "order": "id.asc"},
                         timeout=60)
        if r.status_code not in (200, 206): break
        b = r.json()
        for row in b: done.add((row.get("username") or "").strip().lower())
        if len(b) < PAGE: break
        offset += PAGE
    return done

# ── 4. Meta Batch API call ───────────────────────────────────────────────────
# `ig_id` is the LEGACY IG user id (matches cleaned_data.profile_id).
# `id` is the IGBA (Instagram Business Account id, starts with 1784141...).
PROF = "id,ig_id,username,followers_count,profile_picture_url"
MED  = "like_count,comments_count"

# Meta caps a batch at 50 sub-requests per HTTP call
BATCH_MAX = 50
# media.limit(12) -- 1/4 the CPU cost vs limit(50), statistically comparable ER
MEDIA_LIMIT = 12

def _parse_one(body_json):
    """Map a single Business Discovery body to (info_dict, status, error)."""
    if body_json is None:
        return None, "error", "empty sub-response"
    if "error" in body_json:
        msg = (body_json["error"] or {}).get("message", "unknown")
        code = (body_json["error"] or {}).get("code")
        if code == 4 or "request limit" in msg.lower():
            return None, "rate_limited", scrub(msg)
        return None, "error", scrub(msg)
    bd_obj = body_json.get("business_discovery")
    if not bd_obj: return None, "no_business_discovery", "personal account or non-business profile"
    return bd_obj, "ok", None

def fetch_meta_batch(src_ig, handles):
    """POST one Meta Batch API call with up to 50 BD sub-requests.
    Returns (list of (info, status, error) parallel to handles, x_app_usage dict)."""
    import urllib.parse
    sub = []
    for h in handles:
        clean = h.lstrip("@").strip()
        bd = f"business_discovery.username({clean}){{{PROF},media.limit({MEDIA_LIMIT}){{{MED}}}}}"
        rel = f"{META_VER}/{src_ig}?fields={urllib.parse.quote(bd, safe='')}"
        sub.append({"method": "GET", "relative_url": rel})
    payload = {"access_token": META_TOKEN, "batch": json.dumps(sub)}
    # Survive transient network failures (DNS hiccup, wifi power-save,
    # brief disconnect): up to 5 retries with exponential backoff,
    # then return an all-error batch instead of crashing the run.
    r = None; last_exc = None
    for attempt in range(5):
        try:
            r = requests.post(f"https://graph.facebook.com/{META_VER}/", data=payload, timeout=180)
            break
        except requests.exceptions.RequestException as e:
            last_exc = e
            log(f"  [net-retry {attempt+1}/5] {type(e).__name__}: {scrub(str(e))[:120]}")
            time.sleep(min(60, 5 * (2 ** attempt)))   # 5,10,20,40,60s
    if r is None:
        return [(None, "error", f"network: {type(last_exc).__name__}: {scrub(str(last_exc))[:120]}")
                for _ in handles], {}
    # Aggregate app-usage from headers (drives adaptive throttle)
    try: usage = json.loads(r.headers.get("X-App-Usage", "{}"))
    except Exception: usage = {}
    if r.status_code != 200:
        # Whole-batch failure (auth, overloaded, etc.)
        try:    msg = r.json().get("error", {}).get("message", r.text[:200])
        except Exception: msg = r.text[:200]
        return [(None, "rate_limited" if r.status_code == 429 else "error",
                 f"HTTP {r.status_code}: {scrub(msg)}") for _ in handles], usage
    try:
        arr = r.json()
    except Exception:
        return [(None, "error", "batch JSON decode failed") for _ in handles], usage
    results = []
    for sub_resp in arr:
        if sub_resp is None:
            # Meta sometimes returns null when a sub-request was throttled internally
            results.append((None, "rate_limited", "sub-response null (internally throttled)"))
            continue
        code = sub_resp.get("code")
        try:    body = json.loads(sub_resp.get("body") or "{}")
        except Exception: body = None
        info, status, error = _parse_one(body)
        if code and code != 200 and status == "ok":
            status = "error"; error = error or f"sub HTTP {code}"
        results.append((info, status, error))
    return results, usage

def derive_row(handle, existing_pid, info, status, error):
    if status != "ok" or info is None:
        return {
            "username": handle,
            "profile_id": existing_pid,
            "not_matched_profile_id": None,
            "followers": None, "avg_likes": None, "engagement_rate": None,
            "image_url": None, "posts_sampled": None,
            "status": status, "error": error,
        }
    # Match against the LEGACY ig_id (same id-space as cleaned_data.profile_id).
    # Meta's `id` is the IGBA — different id-space, never directly compared here.
    meta_legacy_pid = str(info.get("ig_id") or "").strip() or None
    if not existing_pid:                              p_id, nm = meta_legacy_pid, None
    elif meta_legacy_pid and meta_legacy_pid == existing_pid: p_id, nm = existing_pid,       None
    else:                                             p_id, nm = existing_pid,       meta_legacy_pid
    items = ((info.get("media") or {}).get("data")) or []
    n = len(items)
    likes = sum(int(it.get("like_count") or 0) for it in items)
    comm  = sum(int(it.get("comments_count") or 0) for it in items)
    followers = int(info.get("followers_count") or 0)
    avg_likes = round(likes/n, 2) if n else 0
    er = round(100.0 * ((likes+comm)/n) / followers, 4) if (n and followers) else 0
    return {
        "username": info.get("username") or handle,
        "profile_id": p_id,
        "not_matched_profile_id": nm,
        "followers": followers,
        "avg_likes": avg_likes,
        "engagement_rate": er,
        "image_url": info.get("profile_picture_url"),
        "posts_sampled": n,
        "status": "ok",
        "error": None,
    }

def upsert_row(row):
    h = dict(CH); h["Prefer"] = "resolution=merge-duplicates,return=minimal"
    r = requests.post(f"{CH_URL}/rest/v1/ig_data_historic?on_conflict=username",
                      headers=h, data=json.dumps(row), timeout=30)
    if r.status_code not in (200, 201, 204):
        return False, f"HTTP {r.status_code}: {scrub(r.text[:200])}"
    return True, None

def upsert_batch(rows):
    """Bulk upsert a list of rows in one POST."""
    if not rows: return 0
    h = dict(CH); h["Prefer"] = "resolution=merge-duplicates,return=minimal"
    r = requests.post(f"{CH_URL}/rest/v1/ig_data_historic?on_conflict=username",
                      headers=h, data=json.dumps(rows), timeout=60)
    if r.status_code not in (200, 201, 204):
        log(f"  [DB-FAIL] HTTP {r.status_code}: {scrub(r.text[:200])}")
        return 0
    return len(rows)

def usage_max_pct(usage):
    """Return max % from X-App-Usage dict (call_count/total_cputime/total_time)."""
    if not usage: return 0
    return max([int(v) for v in usage.values() if isinstance(v, (int, float))] or [0])

def probe_app_usage():
    """Cheap /me call to read true X-App-Usage (batch outer response omits it)."""
    try:
        r = requests.get(f"https://graph.facebook.com/{META_VER}/me",
                         params={"access_token": META_TOKEN, "fields": "id"}, timeout=15)
        try:    return json.loads(r.headers.get("X-App-Usage", "{}"))
        except: return {}
    except Exception:
        return {}

# ── 5. main ──────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit",    type=int, default=0,         help="process at most N handles (0=all)")
    ap.add_argument("--batch",    type=int, default=BATCH_MAX, help="sub-requests per Meta batch call (max 50)")
    ap.add_argument("--sleep",    type=float, default=3.0,     help="seconds between batches")
    ap.add_argument("--cool-pct", type=int, default=70,        help="if X-App-Usage > this, pause to cool down")
    ap.add_argument("--cool-sec", type=int, default=300,       help="seconds to sleep when cooling (default 5min)")
    ap.add_argument("--refresh",  action="store_true",         help="re-fetch handles already in ig_data_historic")
    ap.add_argument("--no-write", action="store_true",         help="skip DB upsert, just hit Meta and print")
    args = ap.parse_args()
    open(LOG_FILE, "w").close()

    if args.batch > BATCH_MAX:
        log(f"[warn] --batch {args.batch} > Meta max {BATCH_MAX}; clamping")
        args.batch = BATCH_MAX

    src = META_SRC or discover_src_ig()
    if not src: log("[fatal] no IG source"); sys.exit(1)
    log(f"[ok] Business Discovery source IG: {src}")
    log(f"[ok] batch_size={args.batch}  sleep_between_batches={args.sleep}s  cool>={args.cool_pct}%")

    handles = load_handle_list()
    if not args.refresh:
        done = load_existing_done()
        before = len(handles)
        handles = [h for h in handles if h["username"].lower() not in done]
        log(f"[ok] resume mode: {before - len(handles):,} already done, {len(handles):,} remaining")
    if args.limit and len(handles) > args.limit:
        handles = handles[:args.limit]
        log(f"[ok] --limit {args.limit} applied -> {len(handles):,} handles will be processed")

    total = len(handles)
    if not total: log("[done] nothing to do"); return

    t_start = time.time()
    cnt_ok = cnt_no_bd = cnt_err = cnt_rate = 0
    cnt_no_pid = cnt_match = cnt_mismatch = 0
    processed = 0

    for bstart in range(0, total, args.batch):
        chunk = handles[bstart : bstart + args.batch]
        t0 = time.time()
        results, usage = fetch_meta_batch(src, [h["username"] for h in chunk])
        dt_ms = (time.time() - t0) * 1000

        rows_to_upsert = []
        for h, (info, status, error) in zip(chunk, results):
            uname = h["username"]; existing = h["existing_pid"]
            row = derive_row(uname, existing, info, status, error)
            rows_to_upsert.append(row)
            if status == "ok":                          cnt_ok += 1
            elif status == "no_business_discovery":     cnt_no_bd += 1
            elif status == "rate_limited":              cnt_rate += 1
            else:                                       cnt_err += 1
            if row["status"] == "ok":
                if not existing:                        cnt_no_pid += 1
                elif row["not_matched_profile_id"]:     cnt_mismatch += 1
                else:                                   cnt_match += 1

        if not args.no_write:
            upsert_batch(rows_to_upsert)
        processed += len(chunk)

        # Side-channel probe for true X-App-Usage (batch outer response omits it)
        probed = probe_app_usage()
        usage_pct = max(usage_max_pct(usage), usage_max_pct(probed))
        elapsed = time.time() - t_start
        rate = processed / elapsed if elapsed > 0 else 0
        eta_s = (total - processed) / rate if rate > 0 else 0
        full_eta_s = 7951 / rate if rate > 0 else 0
        log(f"  [batch {bstart//args.batch + 1:>3}] handles {bstart+1}-{bstart+len(chunk)}  "
            f"({dt_ms:>5.0f}ms; ok={cnt_ok} no_bd={cnt_no_bd} rate={cnt_rate} err={cnt_err})  "
            f"app_usage={usage_pct}%  "
            f"rate={rate*60:.0f}/min  ETA {eta_s/60:.1f}m  full7951 {full_eta_s/60:.1f}m")

        # Adaptive cool-down BEFORE next batch
        if usage_pct >= args.cool_pct and bstart + args.batch < total:
            log(f"  [throttle] X-App-Usage {usage_pct}% >= {args.cool_pct}% -> sleeping {args.cool_sec}s")
            time.sleep(args.cool_sec)
        elif bstart + args.batch < total:
            time.sleep(args.sleep)

    elapsed = time.time() - t_start
    rate = total / elapsed if elapsed > 0 else 0
    full_full = 7951 / rate if rate > 0 else 0
    log("")
    log(f"[summary] processed {total} handles in {elapsed:.1f}s  ({rate:.1f}/s)")
    log(f"[summary] meta: ok={cnt_ok}  no_bd={cnt_no_bd}  rate_limited={cnt_rate}  error={cnt_err}")
    log(f"[summary] pid:  filled_blank={cnt_no_pid}  matched={cnt_match}  mismatched={cnt_mismatch}")
    log(f"[extrapolation] full 7,951 handles ETA at this rate = {full_full/60:.1f} min ({full_full/3600:.2f} h)")

if __name__ == "__main__":
    main()
