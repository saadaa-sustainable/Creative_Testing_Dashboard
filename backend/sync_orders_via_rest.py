"""
sync_orders_via_rest.py — Shopify → Saada_Shopify_Data via Supabase REST

Self-contained sync that doesn't need the DB password. Uses the Saada_Shopify_Data
project's anon key with Supabase REST API + Prefer: resolution=merge-duplicates
(works because public.orders has RLS disabled, so anon has full write access).

If RLS is later enabled, this will start failing — switch to
sync_orders_to_shopify_data.py (direct postgres connection) instead.

USAGE
  python sync_orders_via_rest.py                 # incremental from 2026-05-19
  python sync_orders_via_rest.py --since 2026-04-01
  python sync_orders_via_rest.py --dry-run
  python sync_orders_via_rest.py --batch 200
"""
import os, sys, json, time, argparse, requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
try:
    import ctypes
    _F = 0x80000000 | 0x00000001 | 0x00000040
    ctypes.windll.kernel32.SetThreadExecutionState(_F)
    def keep_awake(): ctypes.windll.kernel32.SetThreadExecutionState(_F)
except Exception:
    def keep_awake(): pass

load_dotenv()

# ── Shopify side ──────────────────────────────────────────────────────────
TOK  = (os.environ.get("ADMIN_ACCESS_TOKEN")
        or os.environ.get("SHOPIFY_ADMIN_ACCESS_TOKEN")
        or os.environ.get("SHOPIFY_ACCESS_TOKEN") or "").strip()
SHOP = (os.environ.get("SHOP_DOMAIN") or "saadaa-design.myshopify.com").strip()
SHOP = SHOP.replace("https://","").replace("http://","").rstrip("/")
VER  = os.environ.get("SHOPIFY_API_VERSION", "2025-10").strip()
URL  = f"https://{SHOP}/admin/api/{VER}/graphql.json"
SHOP_HDRS = {"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"}

if not TOK:
    raise SystemExit("Missing ADMIN_ACCESS_TOKEN in .env")

# ── Saada_Shopify_Data REST endpoint ──────────────────────────────────────
SD_URL = (os.environ.get("SHOPIFY_DATA_URL")
          or "https://siymyhhrpzzbowfqtauf.supabase.co").rstrip("/")
SD_KEY = (os.environ.get("SHOPIFY_DATA_ANON") or "").strip()
if not SD_KEY:
    # Fall back to the anon key retrieved via MCP for one-off sync convenience.
    # Move to .env (SHOPIFY_DATA_ANON=...) for production cron.
    SD_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNpeW15aGhycHp6Ym93ZnF0YXVmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzgzMTQwODEsImV4cCI6MjA5Mzg5MDA4MX0.ocx4jlY3KeXdF_-5JI3_SDcLekmk8hrfWXba7EXEDgo"
SD_HDRS = {
    "apikey":         SD_KEY,
    "Authorization":  f"Bearer {SD_KEY}",
    "Content-Type":   "application/json",
    "Prefer":         "resolution=merge-duplicates,return=minimal",
}

def log(*a): print(" ".join(str(x) for x in a), flush=True)

# ── GraphQL ───────────────────────────────────────────────────────────────
QUERY = """
query AllOrdersPage($after: String, $q: String!) {
  orders(first: 50, after: $after, query: $q, sortKey: CREATED_AT, reverse: false) {
    edges {
      cursor
      node {
        id name createdAt cancelledAt currencyCode
        displayFinancialStatus displayFulfillmentStatus
        currentTotalPriceSet      { shopMoney { amount } }
        currentSubtotalPriceSet   { shopMoney { amount } }
        currentTotalTaxSet        { shopMoney { amount } }
        currentTotalDiscountsSet  { shopMoney { amount } }
        totalShippingPriceSet     { shopMoney { amount } }
        totalRefundedSet          { shopMoney { amount } }
        discountCodes note tags sourceName
        paymentGatewayNames
        customAttributes { key value }
        shippingAddress { firstName lastName phone city province country zip }
        clientIp
        # sales channel — friendly name (e.g. "gokwik", "Online Store")
        channelInformation {
          channelDefinition { channelName handle }
          app { title handle }
        }
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
            r = requests.post(URL, headers=SHOP_HDRS, json=payload, timeout=180)
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
    try: v = (set_obj or {}).get("shopMoney", {}).get("amount")
    except Exception: v = None
    if v in (None, ""): return None
    try: return float(v)
    except Exception: return None

def row_from(node):
    ca_list = node.get("customAttributes") or []
    ca = {a["key"]: a["value"] for a in ca_list}
    ship = node.get("shippingAddress") or {}
    payment_gateways = node.get("paymentGatewayNames") or []
    discount_codes  = node.get("discountCodes") or []
    tags_v = node.get("tags")
    tags_s = (", ".join(tags_v) if isinstance(tags_v, list) else tags_v) or None
    # Sales channel — prefer the Shopify-resolved friendly name
    # (channelInformation.channelDefinition.channelName), fall back to
    # the owning app's name, then to sourceName as a last resort.
    ch      = node.get("channelInformation") or {}
    ch_def  = ch.get("channelDefinition") or {}
    ch_app  = ch.get("app") or {}
    sales_channel = (ch_def.get("channelName")
                     or ch_app.get("title")
                     or node.get("sourceName")
                     or None)
    return {
        "id":                       node.get("id") or "",
        "name":                     node.get("name"),
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
        "tags":                     tags_s,
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

def upsert(rows, retries=4):
    """POST a batch to /rest/v1/orders with merge-duplicates."""
    url = SD_URL + "/rest/v1/orders"
    delay = 2
    for attempt in range(retries):
        try:
            r = requests.post(url, headers=SD_HDRS, json=rows, timeout=120)
            if r.status_code in (200, 201, 204): return True, ""
            if r.status_code in (429, 500, 502, 503, 504):
                log(f"    [retry {attempt+1}/{retries}] HTTP {r.status_code}: {r.text[:160]}")
                time.sleep(delay); delay = min(delay * 2, 60); continue
            return False, f"HTTP {r.status_code}: {r.text[:300]}"
        except Exception as e:
            log(f"    [retry {attempt+1}/{retries}] {type(e).__name__}: {e}")
            time.sleep(delay); delay = min(delay * 2, 60)
    return False, "exhausted retries"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since",   default="2026-05-19", help="YYYY-MM-DD")
    ap.add_argument("--until",   default=None, help="YYYY-MM-DD (default today UTC)")
    ap.add_argument("--batch",   type=int, default=200, help="orders per REST upsert")
    ap.add_argument("--dry-run", action="store_true", help="GraphQL only, skip REST writes")
    args = ap.parse_args()

    since = args.since
    until = args.until or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    q = f"created_at:>={since} created_at:<={until}"
    log(f"[ok] window: {since} → {until}   shop={SHOP}   api={VER}")
    log(f"     shopify token …{TOK[-6:]}   batch={args.batch}   dry-run={args.dry_run}")
    log(f"     supabase: {SD_URL}/rest/v1/orders")

    cursor = None
    pages = rows_fetched = rows_upserted = 0
    pending = []
    t0 = time.time()
    while True:
        keep_awake()
        page = gql_page(cursor, q)
        if page is None:
            log("[!] page returned None — aborting"); break
        edges = page.get("edges") or []
        for e in edges:
            pending.append(row_from(e["node"]))
        pages += 1
        rows_fetched += len(edges)
        pi = page.get("pageInfo") or {}
        cursor = pi.get("endCursor")
        # Flush
        while len(pending) >= args.batch:
            chunk, pending = pending[:args.batch], pending[args.batch:]
            if args.dry_run:
                log(f"  [page {pages:>3}] DRY {len(chunk)} rows  total fetched={rows_fetched:,}")
            else:
                ok, err = upsert(chunk)
                if not ok:
                    log(f"  ❌ upsert failed: {err}")
                    log("     stopping — re-run script to resume")
                    return
                rows_upserted += len(chunk)
                log(f"  [page {pages:>3}] ↑ {len(chunk):>3}  total upserted={rows_upserted:,}  fetched={rows_fetched:,}")
        if pages % 20 == 0 and not args.dry_run:
            elapsed = time.time() - t0
            log(f"  [progress] pages={pages} fetched={rows_fetched:,} upserted={rows_upserted:,} "
                f"({rows_fetched/max(1,elapsed):.1f}/s)")
        if not pi.get("hasNextPage"): break
        time.sleep(0.25)

    # Final flush
    if pending and not args.dry_run:
        ok, err = upsert(pending)
        if not ok: log(f"  ❌ final upsert failed: {err}")
        else:
            rows_upserted += len(pending)
            log(f"  [final ] ↑ {len(pending)}  total upserted={rows_upserted:,}")

    dt = time.time() - t0
    log("─" * 70)
    log(f"  pages fetched   : {pages}")
    log(f"  rows fetched    : {rows_fetched:,}")
    log(f"  rows upserted   : {rows_upserted:,}")
    log(f"  elapsed         : {dt/60:.1f} min")

if __name__ == "__main__":
    main()
