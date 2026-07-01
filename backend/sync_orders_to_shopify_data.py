"""
sync_orders_to_shopify_data.py — incremental Shopify → Supabase orders sync

Pulls every Shopify order created since the last row in
saadaa_shopify_data.public.orders (with a 24-hour safety overlap) and upserts
into the same table.

ENV VARS (added to backend/.env — see .env.example for the full set)
  ADMIN_ACCESS_TOKEN      Shopify Admin API token (already present)
  SHOP_DOMAIN             saadaa-design.myshopify.com (default)
  SHOPIFY_API_VERSION     2025-10 (default)
  SHOPIFY_DATA_DB_URL     postgresql://postgres.siymyhhrpzzbowfqtauf:<PASSWORD>
                          @aws-1-ap-northeast-1.pooler.supabase.com:5432/postgres
                          (copy from Supabase dashboard → Saada_Shopify_Data
                           → Project Settings → Database → Connection string
                           → Connection pooling, "Transaction" mode)

USAGE
  python sync_orders_to_shopify_data.py                 # incremental (default)
  python sync_orders_to_shopify_data.py --since 2026-05-20
  python sync_orders_to_shopify_data.py --dry-run       # GraphQL only, no DB write
  python sync_orders_to_shopify_data.py --batch 100     # upsert batch size

Safe: every write is an INSERT … ON CONFLICT (id) DO UPDATE, so re-running
is idempotent. Progress is committed every batch.
"""
import os, sys, json, time, argparse, requests, psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

# Keep Windows awake during long runs (same pattern as fetch_shopify_orders_6m.py)
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

if not TOK:
    raise SystemExit("Missing ADMIN_ACCESS_TOKEN in .env")
if not DB_URL:
    raise SystemExit(
        "Missing SHOPIFY_DATA_DB_URL in .env\n\n"
        "Copy it from the Supabase dashboard:\n"
        "  Project: Saada_Shopify_Data → Project Settings → Database\n"
        "  → Connection string → URI (Connection pooling, Transaction mode)\n"
        "  Append to backend/.env as:\n"
        "    SHOPIFY_DATA_DB_URL=postgresql://postgres.siymyhhrpzzbowfqtauf:"
        "<PASSWORD>@aws-1-ap-northeast-1.pooler.supabase.com:5432/postgres"
    )

def log(*a): print(" ".join(str(x) for x in a), flush=True)

# ── GraphQL: every column we need to populate ─────────────────────────────
QUERY = """
query AllOrdersPage($after: String, $q: String!) {
  orders(first: 50, after: $after, query: $q, sortKey: CREATED_AT, reverse: false) {
    edges {
      cursor
      node {
        id
        name
        createdAt
        cancelledAt
        currencyCode
        displayFinancialStatus
        displayFulfillmentStatus
        currentTotalPriceSet      { shopMoney { amount } }
        currentSubtotalPriceSet   { shopMoney { amount } }
        currentTotalTaxSet        { shopMoney { amount } }
        currentTotalDiscountsSet  { shopMoney { amount } }
        totalShippingPriceSet     { shopMoney { amount } }
        totalRefundedSet          { shopMoney { amount } }
        discountCodes
        note
        tags
        sourceName
        channelInformation {
          channelDefinition { channelName handle }
          app { title handle }
        }
        paymentGatewayNames
        customAttributes { key value }
        shippingAddress {
          firstName lastName phone
          city province country zip
        }
        clientIp
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

def gql_page(after, q, retries=8):
    payload = {"query": QUERY, "variables": {"after": after, "q": q}}
    delay = 5
    for i in range(retries):
        try:
            r = requests.post(URL, headers=HDRS, json=payload, timeout=180)
            if r.status_code == 200:
                j = r.json()
                ext = j.get("extensions", {}).get("cost") or {}
                tput = ext.get("throttleStatus") or {}
                avail = tput.get("currentlyAvailable", 1000)
                restore = tput.get("restoreRate", 50)
                if avail < 200:
                    nap = max(1.0, (250 - avail) / max(restore, 1))
                    log(f"  [throttle] avail={avail} restore={restore}/s — sleep {nap:.1f}s")
                    time.sleep(nap)
                if "errors" in j:
                    if any("THROTTLED" in str(e) for e in j["errors"]):
                        time.sleep(delay); delay = min(delay * 2, 120); continue
                    log(f"  graphql errors: {str(j['errors'])[:200]}"); return None
                return j["data"]["orders"]
            log(f"  HTTP {r.status_code}: {r.text[:200]}")
            if r.status_code in (429, 502, 503, 504):
                time.sleep(delay); delay = min(delay * 2, 120); continue
            return None
        except Exception as e:
            log(f"  retry {i+1}/{retries} {type(e).__name__}: {e}")
            time.sleep(delay); delay = min(delay * 2, 120)
    return None

def money(set_obj):
    try: return float((set_obj or {}).get("shopMoney", {}).get("amount") or 0) or None
    except Exception: return None

def row_from(node):
    ca_list = node.get("customAttributes") or []
    ca = {a["key"]: a["value"] for a in ca_list}
    ship = node.get("shippingAddress") or {}
    payment_gateways = node.get("paymentGatewayNames") or []
    discount_codes  = node.get("discountCodes") or []
    ch      = node.get("channelInformation") or {}
    ch_def  = ch.get("channelDefinition") or {}
    ch_app  = ch.get("app") or {}
    sales_channel = (ch_def.get("channelName")
                     or ch_app.get("title")
                     or node.get("sourceName")
                     or None)
    return {
        "id":                       node.get("id") or "",
        "name":                     node.get("name") or None,
        "created_at":               node.get("createdAt"),
        "cancelled_at":             node.get("cancelledAt"),
        "financial_status":         node.get("displayFinancialStatus"),
        "fulfillment_status":       node.get("displayFulfillmentStatus"),
        "total_price":              money(node.get("currentTotalPriceSet")),
        "currency":                 node.get("currencyCode"),
        "subtotal":                 money(node.get("currentSubtotalPriceSet")),
        "total_discounts":          money(node.get("currentTotalDiscountsSet")),
        "total_shipping":           money(node.get("totalShippingPriceSet")),
        "total_tax":                money(node.get("currentTotalTaxSet")),
        "total_refunded":           money(node.get("totalRefundedSet")),
        "discount_codes":           ", ".join(discount_codes) if discount_codes else None,
        "note":                     node.get("note"),
        "tags":                     node.get("tags"),
        "source_name":              node.get("sourceName"),
        "sales_channel":            sales_channel,
        "payment_gateway":          ", ".join(payment_gateways) if payment_gateways else None,
        "custom_attributes":        json.dumps(ca, ensure_ascii=False) if ca else None,
        "shipping_first_name":      ship.get("firstName"),
        "shipping_last_name":       ship.get("lastName"),
        "shipping_phone":           ship.get("phone"),
        "shipping_city":            ship.get("city"),
        "shipping_province":        ship.get("province"),
        "shipping_country":         ship.get("country"),
        "shipping_zip":             ship.get("zip"),
        "synced_at":                datetime.now(timezone.utc).isoformat(),
        "customer_ip":              node.get("clientIp"),
        "utm_campaign":             ca.get("utm_campaign"),
        "utm_content":              ca.get("utm_content"),
        "utm_medium":               ca.get("utm_medium"),
        "utm_source":               ca.get("utm_source"),
        "utm_term":                 ca.get("utm_term"),
    }

UPSERT_COLS = [
    "id","name","created_at","cancelled_at","financial_status","fulfillment_status",
    "total_price","currency","subtotal","total_discounts","total_shipping","total_tax",
    "total_refunded","discount_codes","note","tags","source_name","sales_channel",
    "payment_gateway",
    "custom_attributes","shipping_first_name","shipping_last_name","shipping_phone",
    "shipping_city","shipping_province","shipping_country","shipping_zip","synced_at",
    "customer_ip","utm_campaign","utm_content","utm_medium","utm_source","utm_term"
]
def upsert_batch(cur, rows):
    if not rows: return 0
    placeholders = "(" + ",".join(["%s"] * len(UPSERT_COLS)) + ")"
    values = ",".join([placeholders] * len(rows))
    cols   = ",".join(UPSERT_COLS)
    update = ",".join(f"{c}=EXCLUDED.{c}" for c in UPSERT_COLS if c != "id")
    sql = f"INSERT INTO public.orders ({cols}) VALUES {values} ON CONFLICT (id) DO UPDATE SET {update}"
    flat = []
    for r in rows:
        for c in UPSERT_COLS:
            flat.append(r.get(c))
    cur.execute(sql, flat)
    return len(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since",    help="YYYY-MM-DD override (default: MAX(created_at)-1d in DB)")
    ap.add_argument("--until",    help="YYYY-MM-DD override (default: today)")
    ap.add_argument("--dry-run",  action="store_true", help="GraphQL only, no DB write")
    ap.add_argument("--batch",    type=int, default=100, help="upsert batch size")
    args = ap.parse_args()

    log(f"[*] connecting to Saada_Shopify_Data …")
    conn = psycopg2.connect(DB_URL, connect_timeout=15)
    conn.autocommit = False
    cur = conn.cursor()

    if args.since:
        since_dt = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        cur.execute("SELECT MAX(created_at) FROM public.orders")
        latest = cur.fetchone()[0]
        if latest is None:
            since_dt = datetime.now(timezone.utc) - timedelta(days=183)
            log(f"[*] orders table is empty — defaulting to last 6 months")
        else:
            # 24h safety overlap so any order that arrived late gets refreshed
            since_dt = latest - timedelta(hours=24)
            log(f"[*] latest order in DB: {latest.isoformat()}")
    until_dt = datetime.strptime(args.until, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.until \
               else datetime.now(timezone.utc)

    since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%S%z").replace("+0000","Z")
    until_str = until_dt.strftime("%Y-%m-%dT%H:%M:%S%z").replace("+0000","Z")
    q = f"created_at:>='{since_str}' AND created_at:<='{until_str}'"
    log(f"[*] window:  {since_str}  →  {until_str}")
    log(f"[*] shop:    {SHOP}   api={VER}   token tail …{TOK[-6:]}")
    log(f"[*] dry-run: {args.dry_run}   batch={args.batch}")

    cursor = None
    t0 = time.time()
    pages = inserted = 0
    pending = []
    while True:
        keep_awake()
        page = gql_page(cursor, q)
        if page is None:
            log("[!] page returned None — aborting (no changes committed yet)")
            conn.rollback(); break
        edges = page.get("edges") or []
        for e in edges:
            pending.append(row_from(e["node"]))
        pages += 1
        pi = page.get("pageInfo") or {}
        cursor = pi.get("endCursor")
        if not args.dry_run and len(pending) >= args.batch:
            n = upsert_batch(cur, pending); inserted += n; conn.commit()
            log(f"  [page {pages:>3}] upserted {n}  total {inserted}  cursor=…{(cursor or '')[-12:]}")
            pending = []
        else:
            log(f"  [page {pages:>3}] +{len(edges)}  total queued {len(pending)+inserted}  cursor=…{(cursor or '')[-12:]}")
        if not pi.get("hasNextPage"): break
        time.sleep(0.25)

    if pending and not args.dry_run:
        n = upsert_batch(cur, pending); inserted += n; conn.commit()
        log(f"  [final ] upserted {n}  total {inserted}")

    dt = time.time() - t0
    log("─" * 70)
    log(f"  pages fetched   : {pages}")
    log(f"  rows upserted   : {inserted}  {'(dry-run, skipped)' if args.dry_run else ''}")
    log(f"  elapsed         : {dt/60:.1f} min")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()
