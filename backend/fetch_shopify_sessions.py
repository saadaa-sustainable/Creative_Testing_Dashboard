"""
fetch_shopify_sessions.py — pull daily session-per-landing-page data from
Shopify Analytics (ShopifyQL) and upsert into Saada_Shopify_Data.sessions.

The existing sessions table stores rows at the grain of
    (session_date, landing_page_path, utm_source, utm_medium,
     utm_campaign, utm_content, utm_term)
which matches ShopifyQL's supported dimensions on the `sessions` table.
For every day in the requested window we run one ShopifyQL query with
all dims + all supported measures and upsert every row.

ShopifyQL cost is ~1 per query and the daily row cap is well under
10 000 rows at full grain, so a 49-day gap-fill completes in seconds.

USAGE
  python fetch_shopify_sessions.py                    # gap-fill: max(session_date)+1 → today
  python fetch_shopify_sessions.py --since 2025-01-01 # explicit start
  python fetch_shopify_sessions.py --since 2025-01-01 --until 2025-12-31
  python fetch_shopify_sessions.py --dry-run          # fetch but skip DB writes
  python fetch_shopify_sessions.py --overlap 3        # gap-fill re-pulls last 3d
                                                       (default) to catch late data

Env (read from backend/.env):
  ADMIN_ACCESS_TOKEN       Shopify Admin API token
  SHOP_DOMAIN              saadaa-design.myshopify.com
  SHOPIFY_API_VERSION      2025-10
  SHOPIFY_DATA_DB_URL      the pooler URL for Saada_Shopify_Data (same var
                           the orders sync uses)
"""
import os, sys, time, argparse, json, requests, psycopg2
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

# Keep Windows awake during long backfills (~seconds normally, but --since 2025-01-01
# over 550 days spawns hundreds of queries; the OS shouldn't sleep mid-run).
try:
    import ctypes
    _FLAGS = 0x80000000 | 0x00000001 | 0x00000040
    ctypes.windll.kernel32.SetThreadExecutionState(_FLAGS)
    def keep_awake(): ctypes.windll.kernel32.SetThreadExecutionState(_FLAGS)
except Exception:
    def keep_awake(): pass

load_dotenv()

TOK  = (os.environ.get("ADMIN_ACCESS_TOKEN")
        or os.environ.get("SHOPIFY_ADMIN_ACCESS_TOKEN")
        or os.environ.get("SHOPIFY_ACCESS_TOKEN") or "").strip()
SHOP = (os.environ.get("SHOP_DOMAIN") or "saadaa-design.myshopify.com").strip()
SHOP = SHOP.replace("https://","").replace("http://","").rstrip("/")
VER  = os.environ.get("SHOPIFY_API_VERSION", "2025-10").strip()
URL  = f"https://{SHOP}/admin/api/{VER}/graphql.json"
HDRS = {"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"}
DB_URL = (os.environ.get("SHOPIFY_DATA_DB_URL") or "").strip()

# PostgREST endpoint for Saada_Shopify_Data — used both to look up
# max(session_date) when direct DB is unavailable, AND to upsert rows
# when --write-rest is set. Same anon-key fallback that
# sync_orders_via_rest.py uses (public.sessions has RLS disabled per
# the Supabase advisory, so anon can INSERT).
SD_URL = (os.environ.get("SHOPIFY_DATA_URL")
          or "https://siymyhhrpzzbowfqtauf.supabase.co").rstrip("/")
SD_KEY = (os.environ.get("SHOPIFY_DATA_ANON") or "").strip() or (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNpeW15aGhy"
    "cHp6Ym93ZnF0YXVmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzgzMTQwODEsImV4cCI6MjA5Mzg5MDA4M"
    "X0.ocx4jlY3KeXdF_-5JI3_SDcLekmk8hrfWXba7EXEDgo"
)
REST_HDRS = {"apikey": SD_KEY, "Authorization": f"Bearer {SD_KEY}",
             "Content-Type": "application/json",
             "Prefer": "resolution=merge-duplicates,return=minimal"}
ON_CONFLICT = ("session_date,landing_page_path,utm_source,utm_medium,"
               "utm_campaign,utm_content,utm_term")

if not TOK: sys.exit("Missing ADMIN_ACCESS_TOKEN in .env")
# DB_URL is only required when writing directly. Without it the fetcher
# will upsert via PostgREST (--write-rest, default when DB_URL is empty).

# ─── ShopifyQL query template ──────────────────────────────────────────
# LIMIT 10 000 comfortably clears the observed ~8 700 rows/day peak.
# If a day ever exceeds 10 000 rows we paginate with OFFSET.
GQL = """
query Q($q: String!) {
  shopifyqlQuery(query: $q) {
    parseErrors
    tableData { columns { name dataType } rows }
  }
}
"""

DIMS = ("day, landing_page_path, landing_page_type, "
        "utm_source, utm_medium, utm_campaign, utm_content, utm_term")
MEASURES = ("sessions, online_store_visitors, sessions_with_cart_additions, "
            "added_to_cart_rate, bounces, bounce_rate, average_session_duration, "
            "pageviews_per_session, sessions_that_reached_checkout")

def sql_for_day(d, offset=0, limit=10000):
    # Stable ORDER BY so LIMIT/OFFSET pagination doesn't overlap between
    # pages (which would produce duplicate rows and break the ON CONFLICT
    # DO UPDATE upsert on the composite unique key downstream).
    return (f"FROM sessions SHOW {MEASURES} GROUP BY {DIMS} "
            f"SINCE {d} UNTIL {d} "
            f"ORDER BY sessions DESC "
            f"LIMIT {limit} OFFSET {offset}")

def scrub(s):
    """Redact the access token if it ever leaks into a log line."""
    if not TOK: return str(s or "")
    return str(s or "").replace(TOK, "<REDACTED>")

def _is_throttled(j):
    """True if the response indicates cost/rate throttling. Shopify's
    string is 'Rate limited. Please retry later.' with extensions.code
    = 'THROTTLED' — match on code (canonical) so message-text changes
    don't slip past us."""
    for e in (j.get("errors") or []):
        code = ((e.get("extensions") or {}).get("code") or "").upper()
        msg  = (e.get("message") or "").lower()
        if code in ("THROTTLED", "MAX_COST_EXCEEDED") or "rate limited" in msg or "throttled" in msg:
            return True
    return False

def run_gql(q):
    """POST one ShopifyQL query. Long exponential backoff on throttle —
    Shopify's ShopifyQL bucket recovers at 200/s but a 90-second cooldown
    is what actually clears heavy-cost queries after repeated 429s."""
    delay = 5
    for attempt in range(1, 8):
        r = requests.post(URL, headers=HDRS, json={"query": GQL, "variables": {"q": q}}, timeout=90)
        try: j = r.json()
        except Exception:
            j = {"errors": [{"message": f"HTTP {r.status_code}: {r.text[:200]}"}]}
        if _is_throttled(j):
            cost = ((j.get("extensions") or {}).get("cost") or {})
            avail = ((cost.get("throttleStatus") or {}).get("currentlyAvailable") or "?")
            print(f"  [throttled attempt {attempt}] sleeping {delay}s  (bucket avail={avail})", flush=True)
            time.sleep(delay); delay = min(delay * 2, 120)
            continue
        return j
    return j

def fetch_day(d):
    """One day's worth of rows across all pages. Returns list[dict]."""
    out = []; offset = 0
    while True:
        j = run_gql(sql_for_day(d, offset=offset))
        errs = j.get("errors")
        if errs:
            raise RuntimeError(f"gql errors on {d}: {scrub(json.dumps(errs))[:400]}")
        node = (j.get("data") or {}).get("shopifyqlQuery") or {}
        pe = node.get("parseErrors") or []
        if pe:
            raise RuntimeError(f"shopifyql parse errors on {d}: {pe}")
        rows = ((node.get("tableData") or {}).get("rows") or [])
        out.extend(rows)
        if len(rows) < 10000: break
        offset += 10000
        # Cheap heartbeat between pages so the user sees progress on days
        # that need multiple pages.
        print(f"  [{d}] paginating → offset {offset:,}", flush=True)
    return out

# ─── DB helpers ────────────────────────────────────────────────────────
def _connect():
    """Same 11-attempt exponential-backoff pattern the thumbnails scraper
    uses — the Supabase pooler can drop long-idle conns and a naive
    reconnect isn't enough during a several-hundred-day backfill."""
    delay = 2
    for attempt in range(1, 12):
        try:
            return psycopg2.connect(DB_URL, connect_timeout=15)
        except psycopg2.OperationalError as e:
            print(f"  [db retry {attempt}] {type(e).__name__}: {str(e)[:80]} — sleeping {delay}s", flush=True)
            time.sleep(delay); delay = min(delay * 2, 300)
    raise psycopg2.OperationalError("gave up after 11 reconnect attempts")

def _last_session_date(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(session_date) FROM sessions")
        return cur.fetchone()[0]

def _last_session_date_rest():
    """Fetch MAX(session_date) via PostgREST — used when SHOPIFY_DATA_DB_URL
    is empty. Returns a date object or None."""
    try:
        r = requests.get(
            f"{SD_URL}/rest/v1/sessions?select=session_date&order=session_date.desc&limit=1",
            headers={"apikey": SD_KEY, "Authorization": f"Bearer {SD_KEY}"},
            timeout=30)
        j = r.json()
        if isinstance(j, list) and j and j[0].get("session_date"):
            return date.fromisoformat(j[0]["session_date"])
    except Exception as e:
        print(f"  [!] REST max-date lookup failed: {type(e).__name__}: {str(e)[:120]}", flush=True)
    return None

def _dedupe_rest(rows):
    """PostgREST/Postgres will 21000 if two rows in one INSERT share the
    ON CONFLICT unique key. Keep last-wins for each 7-tuple."""
    keyed = {}
    for r in rows:
        k = (r.get("session_date"), r.get("landing_page_path"),
             r.get("utm_source") or None, r.get("utm_medium") or None,
             r.get("utm_campaign") or None, r.get("utm_content") or None,
             r.get("utm_term") or None)
        keyed[k] = r
    return list(keyed.values())

def _upsert_rest(rows, batch=1000):
    """Bulk-upsert via PostgREST. Rows come in as ShopifyQL-shape dicts
    (with 'day' → session_date already normalised by the caller)."""
    if not rows: return 0
    def _i(v):
        try: return int(v) if v not in ("", None) else None
        except: return None
    def _f(v):
        try: return float(v) if v not in ("", None) else None
        except: return None
    def _s(v): return v if v not in ("", None) else None
    payload = [{
        "session_date":                   _s(r.get("session_date")),
        "landing_page_type":              _s(r.get("landing_page_type")),
        "landing_page_path":              _s(r.get("landing_page_path")),
        "utm_source":                     _s(r.get("utm_source")),
        "utm_medium":                     _s(r.get("utm_medium")),
        "utm_campaign":                   _s(r.get("utm_campaign")),
        "utm_content":                    _s(r.get("utm_content")),
        "utm_term":                       _s(r.get("utm_term")),
        "online_store_visitors":          _i(r.get("online_store_visitors")),
        "sessions":                       _i(r.get("sessions")),
        "sessions_with_cart_additions":   _i(r.get("sessions_with_cart_additions")),
        "added_to_cart_rate":             _f(r.get("added_to_cart_rate")),
        "bounces":                        _i(r.get("bounces")),
        "bounce_rate":                    _f(r.get("bounce_rate")),
        "average_session_duration":       _f(r.get("average_session_duration")),
        "pageviews_per_session":          _f(r.get("pageviews_per_session")),
        "sessions_that_reached_checkout": _i(r.get("sessions_that_reached_checkout")),
    } for r in rows]
    payload = _dedupe_rest(payload)
    total = 0
    for i in range(0, len(payload), batch):
        chunk = payload[i:i+batch]
        delay = 3
        for attempt in range(1, 6):
            r = requests.post(f"{SD_URL}/rest/v1/sessions?on_conflict={ON_CONFLICT}",
                              headers=REST_HDRS, data=json.dumps(chunk), timeout=90)
            if r.status_code in (200, 201, 204):
                total += len(chunk); break
            if r.status_code >= 500 or r.status_code in (408, 429):
                time.sleep(delay); delay = min(delay * 2, 60); continue
            raise RuntimeError(f"REST upsert HTTP {r.status_code}: {r.text[:300]}")
        else:
            raise RuntimeError("REST upsert exhausted retries")
    return total

def _upsert(conn, rows):
    """Bulk-upsert to sessions.  Empty-string utm_* values are stored as
    NULL so ShopifyQL's None matches Postgres NULL in the unique key."""
    if not rows: return 0
    def _s(v): return v if (v not in ("", None)) else None
    def _i(v):
        if v in ("", None): return None
        try: return int(v)
        except: return None
    def _f(v):
        if v in ("", None): return None
        try: return float(v)
        except: return None
    tuples = [(
        _s(r.get("day")),                    # session_date
        _s(r.get("landing_page_type")),
        _s(r.get("landing_page_path")),
        _s(r.get("utm_source")),
        _s(r.get("utm_medium")),
        _s(r.get("utm_campaign")),
        _s(r.get("utm_content")),
        _s(r.get("utm_term")),
        _i(r.get("online_store_visitors")),
        _i(r.get("sessions")),
        _i(r.get("sessions_with_cart_additions")),
        _f(r.get("added_to_cart_rate")),
        _i(r.get("bounces")),
        _f(r.get("bounce_rate")),
        _f(r.get("average_session_duration")),
        _f(r.get("pageviews_per_session")),
        _i(r.get("sessions_that_reached_checkout")),
    ) for r in rows]

    sql = """
    INSERT INTO sessions (
      session_date, landing_page_type, landing_page_path,
      utm_source, utm_medium, utm_campaign, utm_content, utm_term,
      online_store_visitors, sessions, sessions_with_cart_additions,
      added_to_cart_rate, bounces, bounce_rate,
      average_session_duration, pageviews_per_session,
      sessions_that_reached_checkout, synced_at
    ) VALUES %s
    ON CONFLICT (session_date, landing_page_path, utm_source, utm_medium,
                 utm_campaign, utm_content, utm_term) DO UPDATE SET
      landing_page_type              = EXCLUDED.landing_page_type,
      online_store_visitors          = EXCLUDED.online_store_visitors,
      sessions                       = EXCLUDED.sessions,
      sessions_with_cart_additions   = EXCLUDED.sessions_with_cart_additions,
      added_to_cart_rate             = EXCLUDED.added_to_cart_rate,
      bounces                        = EXCLUDED.bounces,
      bounce_rate                    = EXCLUDED.bounce_rate,
      average_session_duration       = EXCLUDED.average_session_duration,
      pageviews_per_session          = EXCLUDED.pageviews_per_session,
      sessions_that_reached_checkout = EXCLUDED.sessions_that_reached_checkout,
      synced_at                      = NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql,
            [(t[0],t[1],t[2],t[3],t[4],t[5],t[6],t[7],t[8],t[9],t[10],
              t[11],t[12],t[13],t[14],t[15],t[16], datetime.now(timezone.utc)) for t in tuples],
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            page_size=1000)
    conn.commit()
    return len(tuples)

# ─── Main ──────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="YYYY-MM-DD (default: max(session_date) - overlap)")
    ap.add_argument("--until", help="YYYY-MM-DD (default: today, UTC)")
    ap.add_argument("--overlap", type=int, default=3,
                    help="Days to re-pull before max(session_date) for late-arriving data (default 3)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json-out", help="Path to write NDJSON (one row per line) "
                    "when the DB URL isn't available; skips direct DB writes.")
    args = ap.parse_args()

    json_out_fp = None
    if args.json_out:
        json_out_fp = open(args.json_out, "w", encoding="utf-8")

    conn = None
    use_rest = False
    if not args.dry_run and not json_out_fp:
        if DB_URL:
            conn = _connect()
        else:
            # No direct DB → fall back to PostgREST upserts. Same path
            # used by _load_shopify_sessions_via_rest.py.
            use_rest = True
            print("[*] SHOPIFY_DATA_DB_URL missing — falling back to PostgREST for writes")

    today = datetime.now(timezone.utc).date()
    until = date.fromisoformat(args.until) if args.until else today

    if args.since:
        since = date.fromisoformat(args.since)
    else:
        last = _last_session_date(conn) if conn else _last_session_date_rest()
        if last is None:
            since = date(2025, 1, 1)
            print(f"[*] sessions table is empty (or lookup failed) — defaulting since = {since}")
        else:
            since = last - timedelta(days=args.overlap) + timedelta(days=1)
            since = max(since, date(2022, 1, 1))
            print(f"[*] last session_date = {last} · overlap {args.overlap}d → since = {since}")

    if since > until:
        print(f"[!] since ({since}) > until ({until}) — nothing to do")
        return

    days = [since + timedelta(days=i) for i in range((until - since).days + 1)]
    print(f"[*] fetching {len(days):,} day(s)  ({since} → {until})  · shop={SHOP} · ver={VER}")
    print(f"[*] token tail …{TOK[-6:]}   · dry_run={args.dry_run}\n")

    total_rows = 0; t0 = time.time()
    for i, d in enumerate(days, 1):
        keep_awake()
        t_d = time.time()
        try:
            rows = fetch_day(d)
        except Exception as e:
            print(f"  [!] {d}: fetch failed — {scrub(str(e))[:200]}", flush=True)
            continue
        # Normalise the ShopifyQL 'day' key → 'session_date' so downstream
        # writers see the same column name the DB uses.
        for r in rows:
            r["session_date"] = r.pop("day", None) if "day" in r else r.get("session_date")

        if json_out_fp:
            for r in rows:
                json_out_fp.write(json.dumps(r, ensure_ascii=False) + "\n")
            json_out_fp.flush()
        elif use_rest and not args.dry_run:
            try:
                _upsert_rest(rows)
            except Exception as e:
                print(f"  [!] {d}: REST upsert failed — {scrub(str(e))[:200]}", flush=True)
        elif not args.dry_run:
            try:
                _upsert(conn, rows)
            except psycopg2.Error as e:
                print(f"  [!] {d}: db upsert failed — {type(e).__name__} · reconnecting", flush=True)
                try: conn.close()
                except Exception: pass
                conn = _connect()
                _upsert(conn, rows)
        total_rows += len(rows)
        dt = time.time() - t_d
        print(f"  [{i:>4}/{len(days):>4}] {d}  rows={len(rows):>6,}  ({dt:.1f}s)", flush=True)
        # Proactive cooldown between days — one shopifyqlQuery on this shop's
        # dataset costs enough that firing back-to-back without a breather
        # drains the bucket fast and triggers 429s that eat many days.
        time.sleep(2)

    print()
    print("─" * 70)
    print(f"  days processed : {len(days):,}")
    print(f"  rows upserted  : {total_rows:,}" + (" (dry-run)" if args.dry_run else ""))
    print(f"  elapsed        : {(time.time()-t0)/60:.1f} min")
    if json_out_fp: json_out_fp.close()
    if conn: conn.close()

if __name__ == "__main__":
    main()
