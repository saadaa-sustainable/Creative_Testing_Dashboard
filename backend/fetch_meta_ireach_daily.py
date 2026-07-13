"""
fetch_meta_ireach_daily.py — pull daily *unique* reach + spend at
campaign and adset level from Meta's /insights endpoint and upsert into
public.ireach_campaign_daily / public.ireach_adset_daily.

Why not sum from primary_table?
    primary_table.reach is the ad-level unique reach. Summing it up to
    a campaign OR adset overcounts, because the same user seeing two
    ads in the same campaign is counted twice. Meta tracks unique
    audience at every level of the hierarchy — that's the number we
    want here, and only /insights?level=campaign / adset returns it.

Grain
    One row per (campaign_id / adset_id, date). Idempotent — safe to
    re-run at any cadence.

USAGE
    python fetch_meta_ireach_daily.py                     # daily incremental
                                                          # (last 15 days)
    python fetch_meta_ireach_daily.py --since 2025-01-01  # backfill
    python fetch_meta_ireach_daily.py --level campaign    # only campaigns
    python fetch_meta_ireach_daily.py --level adset       # only adsets
    python fetch_meta_ireach_daily.py --dry-run           # fetch, no write

Env
    META_ACCESS_TOKEN, META_API_VERSION
    ACCOUNT_1..3_ID / ACCOUNT_1..3_NAME
    SUPABASE_DB_URL
"""
from __future__ import annotations
import os, sys, time, json, argparse, requests
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

# Keep Windows awake during long backfills.
try:
    import ctypes
    _FLAGS = 0x80000000 | 0x00000001 | 0x00000040
    ctypes.windll.kernel32.SetThreadExecutionState(_FLAGS)
except Exception:
    pass

TOKEN   = (os.environ.get("META_ACCESS_TOKEN") or "").strip()
API_VER = (os.environ.get("META_API_VERSION") or "v21.0").strip()
DB_URL  = (os.environ.get("SUPABASE_DB_URL") or "").strip()

if not TOKEN:  sys.exit("Missing META_ACCESS_TOKEN in .env")
if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")

BASE = f"https://graph.facebook.com/{API_VER}"

ACCOUNTS = [
    {"name": os.environ.get("ACCOUNT_1_NAME","Raho Saadaa"),
     "id":   os.environ.get("ACCOUNT_1_ID","1136644150469466")},
    {"name": os.environ.get("ACCOUNT_2_NAME","Fourth Ad Account - SD"),
     "id":   os.environ.get("ACCOUNT_2_ID","1349767139294217")},
    {"name": os.environ.get("ACCOUNT_3_NAME","Third Ad Account - SD"),
     "id":   os.environ.get("ACCOUNT_3_ID","264868699479122")},
]
CHUNK_DAYS = 15
PAGE_LIMIT = 500

def _scrub(s):
    """Redact the access token in log lines."""
    return str(s or "").replace(TOKEN, "<REDACTED>") if TOKEN else str(s or "")
def _say(m): print(_scrub(str(m)), flush=True)

def _get(url, params=None):
    """One HTTP GET with retry + throttle handling. Returns parsed JSON."""
    delay = 5
    for attempt in range(1, 6):
        try:
            r = requests.get(url, params=params, timeout=90)
        except requests.RequestException as e:
            _say(f"    [net {attempt}] {type(e).__name__}: {str(e)[:120]} — sleeping {delay}s")
            time.sleep(delay); delay = min(delay*2, 120); continue
        # Handle throttling
        try:
            j = r.json()
        except Exception:
            j = {"error":{"message":r.text[:300]}}
        if r.status_code == 200 and "error" not in j:
            return j
        emsg = ((j.get("error") or {}).get("message") or "")
        code = ((j.get("error") or {}).get("code") or 0)
        # Rate-limit / retriable
        if r.status_code == 429 or code in (17, 4, 32) or "throttle" in emsg.lower():
            _say(f"    [throttled {attempt}] {emsg[:120]} — sleeping {delay}s")
            time.sleep(delay); delay = min(delay*2, 120); continue
        # Non-retriable
        raise RuntimeError(f"Meta API HTTP {r.status_code}: {_scrub(emsg)[:200]}")
    raise RuntimeError("Meta API exhausted retries")

def _fetch_insights(account_id, level, since, until):
    """Yield rows for one account at `level` (campaign|adset) across the
    [since, until] window. Handles pagination + chunking. Per-chunk
    failures (400 Invalid parameter, timeouts, etc.) skip the chunk
    with a log line instead of blowing up the whole backfill — this
    protects against edge dates where Meta rejects a specific window
    for one account level (e.g. an account that didn't exist yet)."""
    fields = ("account_id,account_name," +
              ("campaign_id,campaign_name," if level == "campaign" else
               "campaign_id,campaign_name,adset_id,adset_name,") +
              "reach,spend,impressions,frequency")
    d_since = date.fromisoformat(since)
    d_until = date.fromisoformat(until)
    cursor  = d_since
    while cursor <= d_until:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS - 1), d_until)
        url = f"{BASE}/act_{account_id}/insights"
        params = {
            "level":         level,
            "fields":        fields,
            "time_increment":"1",
            "time_range":    json.dumps({"since": cursor.isoformat(),
                                          "until": chunk_end.isoformat()}),
            "access_token":  TOKEN,
            "limit":         PAGE_LIMIT,
        }
        next_url, next_params = url, params
        try:
            while next_url:
                j = _get(next_url, next_params)
                for row in j.get("data", []) or []:
                    yield row
                paging = (j.get("paging") or {}).get("next")
                next_url, next_params = paging, None
                if not paging: break
        except RuntimeError as e:
            # Meta returned a non-retriable 4xx for THIS chunk.
            # Skip and move on so the rest of the backfill still lands.
            _say(f"    [!] chunk {cursor} → {chunk_end}  skipped: {str(e)[:200]}")
        cursor = chunk_end + timedelta(days=1)

def _to_row_camp(r, account):
    return (
        account["id"], account["name"],
        r.get("campaign_id"), r.get("campaign_name"),
        r.get("date_start"),
        int(r.get("reach") or 0),
        float(r.get("spend") or 0),
        int(r.get("impressions") or 0),
        float(r.get("frequency") or 0),
    )

def _to_row_adset(r, account):
    return (
        account["id"], account["name"],
        r.get("campaign_id"), r.get("campaign_name"),
        r.get("adset_id"),    r.get("adset_name"),
        r.get("date_start"),
        int(r.get("reach") or 0),
        float(r.get("spend") or 0),
        int(r.get("impressions") or 0),
        float(r.get("frequency") or 0),
    )

CAMP_UPSERT = """
INSERT INTO public.ireach_campaign_daily
  (account_id, account_name, campaign_id, campaign_name, date,
   reach_daily, spend_daily, impressions, frequency, synced_at)
VALUES %s
ON CONFLICT (campaign_id, date) DO UPDATE SET
  account_id    = EXCLUDED.account_id,
  account_name  = EXCLUDED.account_name,
  campaign_name = EXCLUDED.campaign_name,
  reach_daily   = EXCLUDED.reach_daily,
  spend_daily   = EXCLUDED.spend_daily,
  impressions   = EXCLUDED.impressions,
  frequency     = EXCLUDED.frequency,
  synced_at     = NOW();
"""
ADSET_UPSERT = """
INSERT INTO public.ireach_adset_daily
  (account_id, account_name, campaign_id, campaign_name,
   adset_id, adset_name, date,
   reach_daily, spend_daily, impressions, frequency, synced_at)
VALUES %s
ON CONFLICT (adset_id, date) DO UPDATE SET
  account_id    = EXCLUDED.account_id,
  account_name  = EXCLUDED.account_name,
  campaign_id   = EXCLUDED.campaign_id,
  campaign_name = EXCLUDED.campaign_name,
  adset_name    = EXCLUDED.adset_name,
  reach_daily   = EXCLUDED.reach_daily,
  spend_daily   = EXCLUDED.spend_daily,
  impressions   = EXCLUDED.impressions,
  frequency     = EXCLUDED.frequency,
  synced_at     = NOW();
"""

def _upsert(sql, tuples, dry_run):
    if dry_run or not tuples: return
    import psycopg2
    from psycopg2.extras import execute_values
    conn = psycopg2.connect(DB_URL, connect_timeout=15)
    try:
        with conn.cursor() as cur:
            # Pad each tuple with the synced_at (NOW()) — DB default handles it.
            padded = [t + (datetime.now(timezone.utc),) for t in tuples]
            execute_values(cur, sql, padded, page_size=500)
        conn.commit()
    finally:
        conn.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="YYYY-MM-DD  (default = today - 14d)")
    ap.add_argument("--until", help="YYYY-MM-DD  (default = today)")
    ap.add_argument("--level", choices=["campaign","adset","both"], default="both")
    ap.add_argument("--account", help="Limit to a single account_id")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    today = datetime.now(timezone.utc).date()
    until = date.fromisoformat(args.until) if args.until else today
    since = date.fromisoformat(args.since) if args.since else (today - timedelta(days=14))
    if since > until: sys.exit(f"since ({since}) > until ({until})")

    accts = [a for a in ACCOUNTS if not args.account or a["id"] == args.account]
    _say(f"[*] window {since} → {until}   accounts={len(accts)}   level={args.level}   dry_run={args.dry_run}")
    _say(f"[*] token tail …{TOKEN[-6:]}   api={API_VER}")

    t0 = time.time()
    total_camp = total_adset = 0
    for a in accts:
        if args.level in ("campaign","both"):
            _say(f"\n  ── {a['name']} ({a['id']})  ·  level=campaign  ──")
            rows_camp = list(_fetch_insights(a["id"], "campaign",
                                              since.isoformat(), until.isoformat()))
            tuples = [_to_row_camp(r, a) for r in rows_camp]
            _upsert(CAMP_UPSERT, tuples, args.dry_run)
            _say(f"    campaigns  fetched {len(rows_camp):,}  upserted {len(tuples):,}")
            total_camp += len(tuples)
        if args.level in ("adset","both"):
            _say(f"\n  ── {a['name']} ({a['id']})  ·  level=adset     ──")
            rows_adset = list(_fetch_insights(a["id"], "adset",
                                                since.isoformat(), until.isoformat()))
            tuples = [_to_row_adset(r, a) for r in rows_adset]
            _upsert(ADSET_UPSERT, tuples, args.dry_run)
            _say(f"    adsets     fetched {len(rows_adset):,}  upserted {len(tuples):,}")
            total_adset += len(tuples)

    _say("\n" + "─"*70)
    _say(f"  campaign rows upserted : {total_camp:,}" + (" (dry-run)" if args.dry_run else ""))
    _say(f"  adset rows upserted    : {total_adset:,}" + (" (dry-run)" if args.dry_run else ""))
    _say(f"  elapsed                : {(time.time()-t0)/60:.1f} min")

if __name__ == "__main__":
    main()
