"""fetch_cogs_by_sku.py — precompute per-master-SKU sales × inventory × Meta
cost, mirrored into Meta_ads_data.public.cogs_by_sku.

Master SKU = variant SKU with the last 2 chars stripped (color code).
For example: SDCPGR / SDCPBL → SDCP.

Sources
-------
* ShopifyQL `sales` model (via Admin GraphQL `shopifyqlQuery`):
    FROM sales SHOW net_items_sold, gross_sales, discounts, sales_reversals,
                    net_sales, taxes, total_sales
    GROUP BY product_title, product_vendor, product_type
* Shopify product variants + inventory (Admin GraphQL `productVariants`
  paginated, plus the parent product's totalInventory).
* Meta ads spend joined by matching master_sku SUBSTRING inside
  primary_table.ad_name  (Python-side; same rule as fetch_content_asset_register).

Result row grain: one row per (window_key, master_sku).
window_key ∈ {'1d','7d','30d'} — precomputed daily by the pipeline; the
frontend switches between them via a toggle.

USAGE
  python fetch_cogs_by_sku.py                       # runs all 3 windows
  python fetch_cogs_by_sku.py --window 7d           # single window
  python fetch_cogs_by_sku.py --dry-run             # fetch + summarise
"""
from __future__ import annotations
import os, sys, json, time, argparse, requests, psycopg2
from datetime import datetime, timezone, date, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from pathlib import Path
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(Path(__file__).parent / ".env", override=True)

DB_URL   = os.environ.get("SUPABASE_DB_URL", "").strip()
SHOP     = (os.environ.get("SHOP_DOMAIN") or "saadaa-design.myshopify.com").strip()
TOKEN    = (os.environ.get("ADMIN_ACCESS_TOKEN")
            or os.environ.get("SHOPIFY_ADMIN_ACCESS_TOKEN")
            or os.environ.get("SHOPIFY_ACCESS_TOKEN") or "").strip()
API_VER  = os.environ.get("SHOPIFY_API_VERSION", "2025-10").strip()
GQL_URL  = f"https://{SHOP.replace('https://','').rstrip('/')}/admin/api/{API_VER}/graphql.json"

if not (DB_URL and TOKEN):
    sys.exit("[fatal] need SUPABASE_DB_URL + ADMIN_ACCESS_TOKEN in .env")

TABLE = "public.cogs_by_sku"

WINDOWS = {
    "1d":  1,
    "7d":  7,
    "30d": 30,
}

DDL = f"""
create table if not exists {TABLE} (
    window_key       text not null,        -- '1d' | '7d' | '30d' | custom label
    master_sku       text not null,
    variant_skus     text[],               -- ['SDCPGR','SDCPBL',…]
    product_title    text,                 -- most common / first-seen title
    product_vendor   text,
    product_type     text,
    net_items_sold   numeric,
    gross_sales      numeric,
    discounts        numeric,
    sales_reversals  numeric,
    net_sales        numeric,
    taxes            numeric,
    total_sales      numeric,
    days_in_window   integer,
    doq              numeric,              -- net_items_sold / days_in_window
    inventory_total  bigint,               -- sum of variantsInventoryQuantity
    variants_count   integer,
    ad_spend         numeric,
    ad_ncp           integer,
    cost_per_ncp     numeric,              -- ad_spend / ad_ncp
    matched_ad_count integer,
    cogs             numeric,              -- placeholder (formula pending)
    computed_at      timestamptz not null default now(),
    primary key (window_key, master_sku)
);
create index if not exists cogs_by_sku_window_idx on {TABLE} (window_key);
"""

def gql(query, variables=None, timeout=60):
    r = requests.post(GQL_URL,
        headers={"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"},
        json={"query": query, "variables": variables or {}}, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if j.get("errors"):
        raise RuntimeError(f"GraphQL errors: {json.dumps(j['errors'])[:400]}")
    if not j.get("data"):
        raise RuntimeError(f"GraphQL empty data: {json.dumps(j)[:400]}")
    return j["data"]

# ────────────────────────────── ShopifyQL ────────────────────────────────
# ShopifyqlQueryResponse.tableData.rows is a JSON scalar — list of dicts
# already keyed by column name. parseErrors is a list of scalars.
SHOPIFYQL_SALES = """
query SalesByProduct($q: String!) {
  shopifyqlQuery(query: $q) {
    tableData { columns { name dataType } rows }
    parseErrors
  }
}
"""

def _to_num(v):
    if v is None: return 0.0
    try: return float(str(v).replace(",", ""))
    except Exception: return 0.0

def fetch_sales(days):
    """Runs the sales ShopifyQL over the last N days grouped by product."""
    since = (date.today() - timedelta(days=days)).isoformat()
    until = date.today().isoformat()
    # Note: 'sales_reversals' isn't exposed in this shop's ShopifyQL schema;
    # 'returns' is the closest analogue. Same for absence of a 'total_sales'
    # column in some versions — we compute it as net_sales + taxes if missing.
    q = (f"FROM sales SHOW net_items_sold, gross_sales, discounts, returns, "
         f"net_sales, taxes, total_sales "
         f"WHERE product_title IS NOT NULL "
         f"SINCE {since} UNTIL {until} "
         f"GROUP BY product_title, product_vendor, product_type "
         f"ORDER BY total_sales DESC LIMIT 1000")
    data = gql(SHOPIFYQL_SALES, {"q": q})
    resp = data["shopifyqlQuery"]
    if resp.get("parseErrors"):
        raise RuntimeError(f"ShopifyQL parse errors: {resp['parseErrors']}")
    td = resp.get("tableData") or {}
    rows = td.get("rows") or []  # already list-of-dicts
    # Coerce money/int strings to numbers + alias 'returns' → 'sales_reversals'
    # so downstream code doesn't need to care which name Shopify used.
    for r in rows:
        if "returns" in r and "sales_reversals" not in r:
            r["sales_reversals"] = r.pop("returns")
        for k in ("net_items_sold","gross_sales","discounts","sales_reversals",
                  "net_sales","taxes","total_sales"):
            if k in r: r[k] = _to_num(r.get(k))
    print(f"  [shopifyql] {len(rows)} rows for {days}d window ({since}→{until})")
    return rows, since, until

# ─────────────────────────── Product variants ────────────────────────────
VARIANTS_QUERY = """
query VariantsPage($after: String) {
  productVariants(first: 250, after: $after) {
    edges {
      node {
        sku
        inventoryQuantity
        product {
          id title vendor productType status
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

def fetch_all_variants():
    """Paginate all product variants — SKU, inventory, and parent product info."""
    variants = []
    after = None
    while True:
        data = gql(VARIANTS_QUERY, {"after": after})
        pv = data["productVariants"]
        for e in pv["edges"]:
            variants.append(e["node"])
        pi = pv["pageInfo"]
        if not pi["hasNextPage"]:
            break
        after = pi["endCursor"]
        print(f"    [variants] fetched {len(variants)} so far …", flush=True)
    print(f"  [variants] total {len(variants)}")
    return variants

# ────────────────────────── master SKU derivation ────────────────────────
def master_of(sku):
    """Master SKU = variant SKU with last 2 chars stripped. Guard against
    SKUs shorter than 3 chars (return None so we drop them)."""
    if not sku or not isinstance(sku, str): return None
    s = sku.strip()
    if len(s) < 4: return None
    return s[:-2]

# ──────────────────────── ad-spend match (Python) ────────────────────────
def compute_ad_spend(cur, master_skus):
    """For each master_sku, scan primary_table.ad_name for a substring match
    (case-insensitive) and sum spend + ncp across matched ads. Also counts
    the distinct matched ads. Returns dict master_sku → dict of aggregates.
    primary_table's spend column is amount_spent_inr; NCP is ncp_count."""
    if not master_skus: return {}
    cur.execute("select ad_id::text, ad_name, amount_spent_inr, ncp_count "
                "from public.primary_table where ad_name is not null")
    ads = cur.fetchall()
    print(f"  [match] scanning {len(ads):,} primary_table rows")
    lc_ads = [(aid, name.lower(), spend, ncp) for aid, name, spend, ncp in ads]
    out = {}
    for sku in master_skus:
        needle = sku.lower()
        if len(needle) < 4: continue     # guard: too-short prefixes hit false positives
        spend, ncp, cnt = 0.0, 0, 0
        for aid, name_lc, sp, np_ in lc_ads:
            if needle in name_lc:
                spend += float(sp or 0)
                ncp   += int(np_ or 0)
                cnt   += 1
        if cnt:
            out[sku] = {"spend": spend, "ncp": ncp, "matched_ad_count": cnt}
    print(f"  [match] {len(out)} master SKUs had ≥1 matching ad")
    return out

# ─────────────────────────────── main flow ───────────────────────────────
def build_window(cur, window_key, days):
    print(f"\n=== window {window_key} ({days}d) ===")
    sales, since, until = fetch_sales(days)
    variants = fetch_all_variants()

    # 1. Group variant SKUs by master SKU
    var_by_master = defaultdict(list)
    inv_by_master = defaultdict(int)
    product_by_master = {}      # master_sku → (title, vendor, productType) - first-seen
    for v in variants:
        sku = (v.get("sku") or "").strip()
        m = master_of(sku)
        if not m: continue
        var_by_master[m].append(sku)
        inv_by_master[m] += int(v.get("inventoryQuantity") or 0)
        prod = v.get("product") or {}
        if m not in product_by_master:
            product_by_master[m] = (prod.get("title"), prod.get("vendor"), prod.get("productType"))

    # 2. Roll sales up to master SKU by matching product_title.
    #    Sales ShopifyQL groups by product_title — no direct SKU. We map
    #    product_title → master_skus via the variants' parent product.title.
    #    A product usually maps to ONE master SKU (all variants share prefix).
    title_to_masters = defaultdict(set)
    for m, (title, _, _) in product_by_master.items():
        if title: title_to_masters[title.strip().lower()].add(m)

    sales_by_master = defaultdict(lambda: defaultdict(float))
    unmatched_titles = 0
    for row in sales:
        title = (row.get("product_title") or "").strip().lower()
        masters = title_to_masters.get(title, set())
        if not masters:
            unmatched_titles += 1
            continue
        # Split evenly if a title maps to multiple masters (rare).
        share = 1.0 / len(masters)
        for m in masters:
            for k in ("net_items_sold","gross_sales","discounts","sales_reversals",
                      "net_sales","taxes","total_sales"):
                sales_by_master[m][k] += float(row.get(k) or 0) * share
    print(f"  [rollup] {len(sales_by_master)} masters had sales; "
          f"{unmatched_titles} titles unmatched")

    # 3. Ad spend / NCP per master SKU (needs primary_table access; skip in dry-run)
    all_masters = set(var_by_master.keys())
    ad_by_master = compute_ad_spend(cur, all_masters) if cur else {}

    # 4. Assemble rows (one per master SKU that had EITHER sales OR ad spend
    #    OR inventory — i.e. anything to report).
    rows = []
    for m in all_masters:
        s = sales_by_master.get(m, {})
        a = ad_by_master.get(m, {"spend": 0.0, "ncp": 0, "matched_ad_count": 0})
        inv = int(inv_by_master.get(m, 0))
        title, vendor, ptype = product_by_master.get(m, (None, None, None))
        net_items = float(s.get("net_items_sold", 0))
        doq = (net_items / days) if days > 0 else None
        cpn = (a["spend"] / a["ncp"]) if a["ncp"] > 0 else None
        rows.append({
            "window_key":       window_key,
            "master_sku":       m,
            "variant_skus":     sorted(set(var_by_master[m])),
            "product_title":    title,
            "product_vendor":   vendor,
            "product_type":     ptype,
            "net_items_sold":   net_items,
            "gross_sales":      float(s.get("gross_sales", 0)),
            "discounts":        float(s.get("discounts", 0)),
            "sales_reversals":  float(s.get("sales_reversals", 0)),
            "net_sales":        float(s.get("net_sales", 0)),
            "taxes":            float(s.get("taxes", 0)),
            "total_sales":      float(s.get("total_sales", 0)),
            "days_in_window":   days,
            "doq":              doq,
            "inventory_total":  inv,
            "variants_count":   len(var_by_master[m]),
            "ad_spend":         a["spend"],
            "ad_ncp":           a["ncp"],
            "cost_per_ncp":     cpn,
            "matched_ad_count": a["matched_ad_count"],
            "cogs":             None,   # formula pending
        })
    return rows

COLS = ["window_key","master_sku","variant_skus","product_title","product_vendor",
        "product_type","net_items_sold","gross_sales","discounts","sales_reversals",
        "net_sales","taxes","total_sales","days_in_window","doq","inventory_total",
        "variants_count","ad_spend","ad_ncp","cost_per_ncp","matched_ad_count","cogs"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", choices=list(WINDOWS.keys())+["all"], default="all")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    t0 = datetime.now(timezone.utc)
    keys = list(WINDOWS.keys()) if args.window == "all" else [args.window]

    conn = psycopg2.connect(DB_URL, connect_timeout=30) if not args.dry_run else None
    if conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(DDL); conn.commit()

    for k in keys:
        cur = conn.cursor() if conn else None
        rows = build_window(cur, k, WINDOWS[k])
        print(f"  [window {k}] {len(rows)} master-SKU rows built")
        if not conn: continue
        cur.execute(f"delete from {TABLE} where window_key = %s", (k,))
        cols_sql = ", ".join(COLS)
        placeholders = "(" + ",".join(["%s"] * len(COLS)) + ")"
        payload = [tuple(r[c] for c in COLS) for r in rows]
        execute_values(cur, f"insert into {TABLE} ({cols_sql}) values %s",
                       payload, template=placeholders, page_size=500)
        conn.commit()
        print(f"  [upsert] window {k} = {len(payload)} rows")

    if conn: conn.close()
    print(f"\n[done] {(datetime.now(timezone.utc)-t0).total_seconds():.1f}s")

if __name__ == "__main__":
    main()
