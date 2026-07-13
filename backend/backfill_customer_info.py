"""backfill_customer_info.py — patch existing shopify_ad_attribution rows
with customer_id, customer_num_orders, contact_email.

Why this exists:
    The attribution rebuild table was built before we started requesting
    customer info from Shopify.  Re-running the full attribution rebuild
    to add those three columns would take 2-3 hours and re-do the whole
    utm-matching engine for nothing.  This script only pulls the missing
    fields for existing rows, using GraphQL's batch `nodes(ids: [...])`
    endpoint so ~26k orders finish in ~30-60 minutes at 50 orders/query.

Usage:
    python backfill_customer_info.py               # backfill all NULL rows
    python backfill_customer_info.py --limit 500   # test with 500 rows
    python backfill_customer_info.py --dry-run     # fetch, don't UPDATE

Idempotent — the WHERE clause skips rows that already have customer_id.

Env:
    SUPABASE_DB_URL, ADMIN_ACCESS_TOKEN, SHOP_DOMAIN
"""
from __future__ import annotations
import os, sys, io, time, argparse, json, requests, psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
load_dotenv(override=True)

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")

SHOPIFY_TOKEN = (os.environ.get("ADMIN_ACCESS_TOKEN")
                 or os.environ.get("SHOPIFY_ADMIN_ACCESS_TOKEN")
                 or os.environ.get("SHOPIFY_ACCESS_TOKEN") or "").strip()
if not SHOPIFY_TOKEN: sys.exit("Missing ADMIN_ACCESS_TOKEN in .env")

SHOP = (os.environ.get("SHOP_DOMAIN") or "saadaa-design.myshopify.com")
SHOP = SHOP.replace("https://","").replace("http://","").rstrip("/")
VER  = os.environ.get("SHOPIFY_API_VERSION", "2025-10").strip()
URL  = f"https://{SHOP}/admin/api/{VER}/graphql.json"
HDRS = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}

BATCH = 100           # nodes(ids:100) = ~200 pts, well within the 4000-pt budget
LOG   = "logs/backfill_customer_info.log"
PROG  = "backfill_customer_info.progress.json"

def _scrub(s):
    return str(s or "").replace(SHOPIFY_TOKEN, "<REDACTED>") if SHOPIFY_TOKEN else str(s or "")

def log(*a):
    msg = " ".join(_scrub(str(x)) for x in a)
    print(msg, flush=True)
    os.makedirs("logs", exist_ok=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(msg + "\n")

QUERY = """
query BackfillCust($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Order {
      id
      customer { id numberOfOrders email }
    }
  }
}
"""

def _fetch_batch(ids, retries=8):
    """Return {order_gid: (cust_id_num_str_or_None, num_orders_int_or_None, email_or_None)}"""
    payload = {"query": QUERY, "variables": {"ids": ids}}
    delay = 5
    for i in range(retries):
        try:
            r = requests.post(URL, headers=HDRS, json=payload, timeout=120)
        except requests.exceptions.RequestException as e:
            log(f"  [net {i+1}] {type(e).__name__}: {str(e)[:120]} — sleeping {delay}s")
            time.sleep(delay); delay = min(delay*2, 120); continue
        if r.status_code != 200:
            log(f"  HTTP {r.status_code}: {r.text[:200]}")
            if r.status_code in (429, 502, 503, 504):
                time.sleep(delay); delay = min(delay*2, 120); continue
            return {}
        try:
            j = r.json()
        except Exception:
            log(f"  bad JSON: {r.text[:200]}"); return {}
        if "errors" in j:
            emsg = str(j["errors"])[:200]
            if "THROTTLED" in emsg or "Throttled" in emsg:
                log(f"  [throttled {i+1}] sleeping {delay}s"); time.sleep(delay)
                delay = min(delay*2, 120); continue
            log(f"  graphql errors: {emsg}"); return {}
        out = {}
        for n in j.get("data", {}).get("nodes", []) or []:
            if not n: continue
            gid = n.get("id"); cust = n.get("customer") or {}
            cid_gid = cust.get("id") or ""
            cid = cid_gid.rsplit("/", 1)[-1] if cid_gid else None
            nn  = cust.get("numberOfOrders")
            out[gid] = (
                cid,
                int(nn) if nn is not None else None,
                cust.get("email"),
            )
        return out
    log("  exhausted retries")
    return {}

UPDATE_SQL = """
UPDATE public.shopify_ad_attribution
   SET customer_id         = %s,
       customer_num_orders = %s,
       contact_email       = %s
 WHERE order_id            = %s
   AND customer_id         IS NULL
"""

def _apply(conn, rows):
    """rows = [(cid, nn, email, order_id), ...]"""
    with conn.cursor() as cur:
        execute_batch(cur, UPDATE_SQL, rows, page_size=500)
    conn.commit()

def _save_prog(state):
    with open(PROG, "w", encoding="utf-8") as f: json.dump(state, f)
def _load_prog():
    if not os.path.exists(PROG): return {}
    try:
        with open(PROG, encoding="utf-8") as f: return json.load(f)
    except Exception: return {}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=0, help="cap to first N orders (0 = all)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--batch",  type=int, default=BATCH)
    args = p.parse_args()

    log(f"\n========== backfill_customer_info @ {time.strftime('%Y-%m-%d %H:%M:%S')} ==========")
    log(f"dry_run={args.dry_run}  limit={args.limit}  batch={args.batch}")

    conn = psycopg2.connect(DB_URL, connect_timeout=30); conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '30min'")
        sql = ("SELECT order_id FROM public.shopify_ad_attribution "
               "WHERE customer_id IS NULL ORDER BY order_created_at DESC")
        if args.limit > 0: sql += f" LIMIT {args.limit}"
        cur.execute(sql)
        order_ids = [r[0] for r in cur.fetchall()]
    log(f"[*] {len(order_ids):,} rows need customer info")

    if not order_ids:
        log("[ok] nothing to backfill"); conn.close(); return

    t0 = time.time()
    total_patched = 0
    total_seen    = 0
    total_missing = 0
    prog = _load_prog()
    last_done = int(prog.get("done", 0))
    if last_done:
        log(f"[*] resuming from index {last_done:,} (progress file)")
        order_ids = order_ids[last_done:]

    for i in range(0, len(order_ids), args.batch):
        chunk = order_ids[i:i + args.batch]
        result = _fetch_batch(chunk)
        total_seen += len(chunk)
        rows_to_update = []
        for oid in chunk:
            if oid in result:
                cid, nn, email = result[oid]
                if cid is None and nn is None and email is None:
                    total_missing += 1
                rows_to_update.append((cid, nn, email, oid))
            else:
                total_missing += 1
        if rows_to_update and not args.dry_run:
            _apply(conn, rows_to_update)
            total_patched += len(rows_to_update)
        _save_prog({"done": last_done + i + len(chunk),
                    "patched": total_patched,
                    "missing": total_missing,
                    "at": time.strftime("%Y-%m-%d %H:%M:%S")})
        # brief throttle — Shopify GraphQL cost budget is 100 pts / query,
        # nodes(ids:50) usually ~2 pts each = 100 pts.  Buffer to be safe.
        if (i // args.batch) % 20 == 0 or i + args.batch >= len(order_ids):
            rate = total_seen / max(time.time() - t0, 0.01)
            eta  = (len(order_ids) - total_seen) / max(rate, 0.01) / 60
            log(f"    seen={total_seen:>6,}  patched={total_patched:>6,}  "
                f"missing={total_missing:>4,}  "
                f"rate={rate:>4.1f}/s  eta={eta:>5.1f} min")
        # batch=100 costs ~200 pts; Shopify restores 200 pts/sec so we
        # need >= 1s between calls to stay clear of throttling.
        time.sleep(1.05)

    log("─" * 70)
    log(f"[ok] backfill complete "
        f"seen={total_seen:,} patched={total_patched:,} "
        f"missing={total_missing:,} elapsed={(time.time()-t0)/60:.1f} min"
        + (" (dry-run)" if args.dry_run else ""))
    conn.close()

if __name__ == "__main__":
    main()
