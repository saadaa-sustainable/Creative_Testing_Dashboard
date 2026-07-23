"""
fetch_men_cotton_pants.py — count total orders + units + colour breakdown
for every product matching "men cotton pants" in the Shopify catalog.

USAGE
  python fetch_men_cotton_pants.py --dry-run     # list matching products only
  python fetch_men_cotton_pants.py                # dry-run + aggregate orders
  python fetch_men_cotton_pants.py --since 2024-01-01

STAGES
  1. DISCOVER — Admin GraphQL `products(query: ...)` search for keywords
     "cotton" + "pant"/"pants"/"trouser"/"chinos" + "men"/"male". Emits a
     table of matching products with variant options + current inventory.
  2. AGGREGATE (only when NOT --dry-run) — walk `orders(query: ...)` for
     each matching product, sum line-item quantity grouped by variant
     option (colour when present, else variant title).

CREDENTIALS  (backend/.env)
  SHOP_DOMAIN           e.g. saadaa.myshopify.com
  ADMIN_ACCESS_TOKEN    Admin API access token (shpat_...)
  SHOPIFY_API_VERSION   optional; default 2025-10

SAFETY
  * Dry-run makes only 1-3 product-search calls; no order traversal.
  * Token is regex-scrubbed on every write.
"""
from __future__ import annotations
import os, sys, io, re, json, argparse, time, requests
from collections import defaultdict, Counter
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='backslashreplace')

load_dotenv(override=True)

SHOP_DOMAIN = (os.environ.get("SHOP_DOMAIN") or "").strip()
SHOP_TOKEN  = (os.environ.get("ADMIN_ACCESS_TOKEN") or "").strip()
API_VER     = (os.environ.get("SHOPIFY_API_VERSION") or "2025-10").strip()

if not SHOP_DOMAIN: sys.exit("Missing SHOP_DOMAIN in .env")
if not SHOP_TOKEN:  sys.exit("Missing ADMIN_ACCESS_TOKEN in .env")

GRAPHQL = f"https://{SHOP_DOMAIN}/admin/api/{API_VER}/graphql.json"
HDRS    = {"X-Shopify-Access-Token": SHOP_TOKEN, "Content-Type": "application/json"}

TOK_RE = re.compile(r"(?:shpat_[A-Za-z0-9]{20,}|shpca_[A-Za-z0-9]{20,}|eyJ[\w\-.]{40,})")
def scrub(s): return TOK_RE.sub("<REDACTED>", str(s or ""))
def say(m):   print(scrub(str(m)), flush=True)

# SKU-based discovery — user's convention:
#   Base product SKU prefix    = SMCP  (Saadaa Men Cotton Pant)
#   Colour code                = 2 letters after SMCP  → SMCPGR, SMCPKH, ...
#   Full variant SKU           = SMCPGR-<size>   e.g. SMCPGR-M
SKU_PREFIX = "SMCP"
COLOUR_RE  = re.compile(r"^" + re.escape(SKU_PREFIX) + r"([A-Z]{2})", re.IGNORECASE)

QUERIES = [
    # Any product with a variant whose SKU starts with SMCP
    f'sku:{SKU_PREFIX}*',
]

PRODUCTS_Q = """
query FindProducts($q: String!, $after: String) {
  products(first: 50, query: $q, after: $after) {
    edges {
      node {
        id
        title
        handle
        productType
        vendor
        status
        tags
        totalInventory
        options { name values }
        variants(first: 50) {
          edges { node {
            id title sku inventoryQuantity price
            selectedOptions { name value }
          } }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

# For AGGREGATE stage: paginate orders that include our target products.
# Shopify `orders(query: "line_items.product_id:X")` isn't broadly supported;
# we filter client-side after paginating the order stream by created_at.
ORDERS_Q = """
query Orders($q: String!, $after: String) {
  orders(first: 100, query: $q, after: $after, sortKey: PROCESSED_AT) {
    edges {
      node {
        id name processedAt cancelledAt
        lineItems(first: 60) {
          edges { node {
            quantity title
            sku                          # ← preserved on line item even if variant was later deleted
            product { id title }
            variant {
              id title sku
              selectedOptions { name value }
            }
          } }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

def gql(q, variables=None, tries=3):
    for i in range(tries):
        r = requests.post(GRAPHQL, headers=HDRS,
                          json={"query": q, "variables": variables or {}}, timeout=45)
        if r.status_code == 429 or (r.status_code >= 500):
            time.sleep(2 * (i + 1)); continue
        r.raise_for_status()
        d = r.json()
        if "errors" in d:
            # Throttled? backoff and retry
            errs = d["errors"]
            if any("Throttled" in str(e) for e in errs) and i + 1 < tries:
                time.sleep(4 * (i + 1)); continue
            raise RuntimeError(scrub(json.dumps(errs)))
        # Warn on cost throttle but proceed
        cost = d.get("extensions", {}).get("cost", {})
        remaining = cost.get("throttleStatus", {}).get("currentlyAvailable")
        if remaining and remaining < 200:
            time.sleep(1.5)
        return d["data"]
    raise RuntimeError("giving up after retries")

def discover_products():
    found = {}  # id -> product dict
    for q in QUERIES:
        cursor = None
        while True:
            data = gql(PRODUCTS_Q, {"q": q, "after": cursor})
            page = data["products"]
            for edge in page["edges"]:
                p = edge["node"]
                if p["status"] != "ACTIVE":  # skip drafts/archived
                    continue
                found[p["id"]] = p
            if not page["pageInfo"]["hasNextPage"]:
                break
            cursor = page["pageInfo"]["endCursor"]
    return list(found.values())

def print_discovery(products):
    say(f"[*] discovered {len(products)} matching product(s)\n")
    for p in products:
        opts = ", ".join([o["name"] for o in p["options"]])
        say(f"  • {p['title']}")
        say(f"      id={p['id']}   type={p['productType']}   opts=[{opts}]")
        # Colour values (if a Color option exists) — else fall through
        col_opt = next((o for o in p["options"] if o["name"].lower() in ("color","colour","shade")), None)
        if col_opt:
            say(f"      colours: {', '.join(col_opt['values'])}")
        say(f"      current inventory (all variants): {p['totalInventory']}")
        say("")

def _sku_colour(sku):
    """Extract the 6-char SKU colour code (e.g. 'SMCPGR' from 'SMCPGR-M')."""
    if not sku: return None
    m = COLOUR_RE.match(sku.strip().upper())
    return (SKU_PREFIX + m.group(1).upper()) if m else None

def _build_sku_colour_map(products):
    """product_id → { colour_code -> product_title } and colour_code -> product_title
    based on the actual variants returned by discover_products()."""
    code_to_title = {}
    for p in products:
        for ve in p["variants"]["edges"]:
            v = ve["node"]
            code = _sku_colour(v.get("sku"))
            if not code: continue
            code_to_title.setdefault(code, p["title"])
    return code_to_title

def aggregate_orders(code_to_title, since=None):
    """Walk order stream sorted by processed_at; for every line item whose
    variant SKU starts with SMCP, bucket by the 6-char colour code."""
    # Bucket: colour_code -> {orders: set, units: int}
    stats = defaultdict(lambda: {"orders": set(), "units": 0})
    order_totals = {"orders": set(), "units": 0}
    unknown_codes = Counter()   # SMCP codes we didn't see in the product catalogue

    # Server-side SKU filter — Shopify supports `sku:SMCP*` on orders. This
    # returns ONLY orders that contain at least one line item whose SKU starts
    # with SMCP.  Cuts the walk from ~all-store-orders to only the ~SMCP orders.
    q_parts = [f"sku:{SKU_PREFIX}*", "-status:cancelled"]
    if since: q_parts.append(f"processed_at:>={since}")
    q = " ".join(q_parts)
    say(f"[*] paging orders  query={q!r}")

    cursor = None; page_i = 0; matched = 0
    t0 = time.time()
    while True:
        data = gql(ORDERS_Q, {"q": q, "after": cursor})
        page = data["orders"]
        for edge in page["edges"]:
            o = edge["node"]
            if o.get("cancelledAt"):
                continue
            for le in o["lineItems"]["edges"]:
                li = le["node"]
                # Prefer line-item sku (preserved even when the variant record was
                # later deleted); fall back to variant.sku for older exports.
                sku_seen = li.get("sku") or ((li.get("variant") or {}).get("sku"))
                code = _sku_colour(sku_seen)
                if not code:
                    continue
                if code not in code_to_title:
                    unknown_codes[code] += int(li.get("quantity") or 0)
                matched += 1
                stats[code]["orders"].add(o["id"])
                stats[code]["units"] += int(li.get("quantity") or 0)
                order_totals["orders"].add(o["id"])
                order_totals["units"] += int(li.get("quantity") or 0)
        page_i += 1
        if page_i % 5 == 0:
            say(f"  ... pages={page_i}  matched_lines={matched}  distinct_orders={len(order_totals['orders']):,}  elapsed={time.time()-t0:.0f}s")
        if not page["pageInfo"]["hasNextPage"]:
            break
        cursor = page["pageInfo"]["endCursor"]
    return stats, order_totals, unknown_codes

def print_agg(stats, order_totals, code_to_title, unknown_codes):
    say("")
    say("=== TOTALS — Men Cotton Pant (SKU prefix SMCP) ===")
    say(f"    distinct orders : {len(order_totals['orders']):,}")
    say(f"    total units sold: {order_totals['units']:,}")
    say("")
    say("=== colour breakdown ===")
    say(f"    {'SKU code':<10} {'colour (product title)':<30} {'orders':>8} {'units':>8}")
    say(f"    {'-'*10} {'-'*30} {'-'*8} {'-'*8}")
    rows = [(code, code_to_title.get(code, '(unknown SKU)'),
             len(v['orders']), v['units']) for code, v in stats.items()]
    rows.sort(key=lambda r: -r[3])   # by units desc
    for code, title, n_orders, units in rows:
        say(f"    {code:<10} {title[:30]:<30} {n_orders:>8,} {units:>8,}")
    if unknown_codes:
        say("")
        say("  ⚠ unknown SMCP SKU codes seen in orders but not in the current"
            "  Shopify product catalogue (deleted/renamed products):")
        for code, units in unknown_codes.most_common():
            say(f"       {code:<10}  units={units:,}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="only list matching products; skip order traversal")
    ap.add_argument("--since", help="YYYY-MM-DD earliest order date")
    args = ap.parse_args()

    say(f"[*] SHOP_DOMAIN = {SHOP_DOMAIN}   api_ver={API_VER}")
    say(f"[*] token tail  = …{SHOP_TOKEN[-6:]}\n")

    products = discover_products()
    print_discovery(products)
    code_to_title = _build_sku_colour_map(products)
    say(f"[*] SKU→colour map:  {len(code_to_title)} distinct SMCP codes")
    for code, t in sorted(code_to_title.items()):
        say(f"      {code} -> {t}")
    say("")

    if args.dry_run or not products:
        say("[dry-run] not aggregating orders. Re-run without --dry-run to count units.")
        return

    stats, order_totals, unknown = aggregate_orders(code_to_title, since=args.since)
    print_agg(stats, order_totals, code_to_title, unknown)

if __name__ == "__main__":
    main()
