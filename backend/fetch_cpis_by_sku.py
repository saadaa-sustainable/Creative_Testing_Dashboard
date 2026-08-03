"""fetch_cogs_by_sku.py — precompute per-SKU sales × inventory × Meta cost
at TWO levels (master + color variant), mirrored into public.cpis_by_sku.

SKU hierarchy (per user's naming convention):
  variant SKU (finest)   e.g. SMCPBL_L    = master + color + _size
  color variant SKU      e.g. SMCPBL      = master + color
  master SKU (product)   e.g. SMCP        = product code

Derivation rules:
  color_of(sku)  = strip '_<size>' suffix if present     (SMCPBL_L → SMCPBL)
  master_of(sku) = color_of(sku) with last 2 chars stripped (SMCPBL → SMCP)

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

TABLE = "public.cpis_by_sku"

WINDOWS = {
    "1d":  1,
    "7d":  7,
    "30d": 30,
}

DDL = f"""
create table if not exists {TABLE} (
    window_key       text not null,        -- '1d' | '7d' | '30d' | custom label
    level            text not null,        -- 'master' | 'color'
    sku              text not null,        -- master SKU or color-variant SKU
    parent_sku       text,                 -- master for color rows; null for master rows
    color_code       text,                 -- last 2 chars for color rows (GR/BL/…)
    variant_skus     text[],               -- child SKUs rolled up into this row
    product_title    text,
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
    ad_spend         numeric,              -- populated at MASTER level only
    ad_ncp           integer,              -- populated at MASTER level only
    cost_per_ncp     numeric,              -- ad_spend / ad_ncp
    matched_ad_count integer,              -- distinct ad_ids matching this master
    doh              numeric,              -- inventory / doq = days of holding
    roas             numeric,              -- total_sales / ad_spend
    cpis             numeric,              -- Cost Per Item Sold placeholder (formula pending)
    computed_at      timestamptz not null default now(),
    primary key (window_key, level, sku)
);
create index if not exists cpis_by_sku_window_idx on {TABLE} (window_key);
create index if not exists cpis_by_sku_level_idx  on {TABLE} (level);
create index if not exists cpis_by_sku_parent_idx on {TABLE} (parent_sku);
-- New columns added after initial ship — safe on re-runs.
alter table {TABLE}
    add column if not exists doh  numeric,
    add column if not exists roas numeric;
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
  productVariants(first: 250, after: $after, query: "product_status:active") {
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
    """Paginate all product variants — SKU, inventory, and parent product info.
    Server-side filter to product_status:active — archived / draft products
    carry stale inventory that used to inflate the master rollup (a single
    master could sum to 30k+ units otherwise). Belt-and-suspenders check
    inside Python in case the query filter is loosened in a future API."""
    variants = []
    after = None
    while True:
        data = gql(VARIANTS_QUERY, {"after": after})
        pv = data["productVariants"]
        for e in pv["edges"]:
            n = e["node"]
            prod = n.get("product") or {}
            if str(prod.get("status", "")).upper() != "ACTIVE": continue
            variants.append(n)
        pi = pv["pageInfo"]
        if not pi["hasNextPage"]:
            break
        after = pi["endCursor"]
        print(f"    [variants] fetched {len(variants)} so far …", flush=True)
    print(f"  [variants] total {len(variants)}  (ACTIVE products only)")
    return variants

# ─────────────────────────── canonical title ─────────────────────────────
# Words we strip when computing a master product name from the colour-variant
# titles — colour names + a few packaging / gender / model modifiers that
# creep into individual product titles.
_COLOR_WORDS = set("""
black white red blue green yellow pink purple orange brown grey gray navy
olive maroon beige tan cream ivory gold silver khaki wine burgundy teal mint
coral lavender mustard rust peach salmon lilac plum turquoise indigo magenta
fuchsia sky sage charcoal aqua lemon lime royal midnight rose ruby emerald
sapphire onyx pearl bronze copper cobalt cyan taupe mauve saffron scarlet
tangerine mocha nude walnut cocoa amber jade forest slate stone off-white
""".split())

def canonical_master_title(titles):
    """Given a list of colour-variant product titles, return a single
    master product name by keeping only words that appear in ALL titles
    AND are not obvious colour words. Falls back to the shortest title.

    Examples
      ["Men Navy Blue Cotton Pant","Men Green Cotton Pant","Men Black Cotton Pant"]
        → "Men Cotton Pant"
      ["Women White Cotton Pant"]
        → "Women Cotton Pant"
    """
    clean = [t for t in titles if t]
    if not clean: return None
    if len(clean) == 1:
        toks = [w for w in clean[0].split()
                if w.lower().strip(",.-") not in _COLOR_WORDS]
        return " ".join(toks) or clean[0]
    # Intersection of words across every title (case-insensitive).
    common = None
    for t in clean:
        toks = {w.lower().strip(",.-") for w in t.split()}
        common = toks if common is None else (common & toks)
    if not common: return min(clean, key=len)
    # Reconstruct in the order of the FIRST title, dropping colour words + dupes.
    seen, out = set(), []
    for w in clean[0].split():
        key = w.lower().strip(",.-")
        if key in common and key not in _COLOR_WORDS and key not in seen:
            out.append(w); seen.add(key)
    return " ".join(out) or min(clean, key=len)

# ────────────────────────── SKU-level derivation ─────────────────────────
def color_of(sku):
    """Color-variant SKU = variant SKU with the '_<size>' suffix stripped.
    SMCPBL_L → SMCPBL   ·   SMCPBL → SMCPBL   ·   SMCP → SMCP."""
    if not sku or not isinstance(sku, str): return None
    s = sku.strip()
    if not s: return None
    # Split once on first underscore. Anything after '_' is treated as size.
    return s.split("_", 1)[0]

def master_of(sku):
    """Master SKU = color_variant with last 2 chars (colour code) stripped.
    SMCPBL_L → SMCP   ·   SMCPBL → SMCP   ·   SMCP → None (too short)."""
    cv = color_of(sku)
    if not cv or len(cv) < 4: return None
    return cv[:-2]

def color_code_of(sku):
    """The 2-char colour suffix. SMCPBL_L → 'BL'; None if not derivable."""
    cv = color_of(sku)
    if not cv or len(cv) < 4: return None
    return cv[-2:]

# ──────────────────────── ad-spend match (Python) ────────────────────────
def compute_ad_spend(cur, master_skus, days=None):
    """For each master_sku, scan primary_table.ad_name for a substring match
    (case-insensitive) and sum spend + ncp across matched ads WITHIN THE
    SAME DATE WINDOW as the sales rollup. Counts DISTINCT matching ad_ids.

    primary_table has one row per (ad_id, date). Without the date filter
    we were summing lifetime spend, inflating a single master from the
    real ~₹2L/week to ~₹1.4Cr."""
    if not master_skus: return {}
    if days:
        cur.execute(
            "select ad_id::text, ad_name, amount_spent_inr, ncp_count "
            "from public.primary_table "
            "where ad_name is not null "
            "and date >= current_date - (%s || ' days')::interval",
            (str(days),)
        )
    else:
        cur.execute("select ad_id::text, ad_name, amount_spent_inr, ncp_count "
                    "from public.primary_table where ad_name is not null")
    ads = cur.fetchall()
    print(f"  [match] scanning {len(ads):,} primary_table rows (window={days}d)")
    lc_ads = [(aid, name.lower(), spend, ncp) for aid, name, spend, ncp in ads]
    out = {}
    for sku in master_skus:
        needle = sku.lower()
        if len(needle) < 4: continue     # guard: too-short prefixes hit false positives
        spend, ncp = 0.0, 0
        ad_ids = set()
        for aid, name_lc, sp, np_ in lc_ads:
            if needle in name_lc:
                spend += float(sp or 0)
                ncp   += int(np_ or 0)
                ad_ids.add(aid)
        if ad_ids:
            out[sku] = {"spend": spend, "ncp": ncp,
                        "matched_ad_count": len(ad_ids)}   # DISTINCT ad_ids
    print(f"  [match] {len(out)} master SKUs had ≥1 matching ad")
    return out

# ─────────────────────────────── main flow ───────────────────────────────
def build_window(cur, window_key, days):
    print(f"\n=== window {window_key} ({days}d) ===")
    sales, since, until = fetch_sales(days)
    variants = fetch_all_variants()

    # 1. Group variant SKUs by BOTH master and color-variant.
    #    variant SKU (finest) → color variant (SMCPBL) → master (SMCP)
    #    inv only counted for non-negative quantities (drops returns/holds).
    size_skus_by_color = defaultdict(list)     # color → [SMCPBL_L, SMCPBL_XL, …]
    inv_by_color       = defaultdict(int)
    color_by_master    = defaultdict(set)      # master → {SMCPBL, SMCPGR, …}
    titles_by_master   = defaultdict(list)     # master → [color-variant product titles]
    vendor_by_master   = {}
    ptype_by_master    = {}
    product_by_color   = {}                    # color → (title, vendor, type, parent_master, code)
    for v in variants:
        raw = (v.get("sku") or "").strip()
        m   = master_of(raw)
        c   = color_of(raw)
        if not m: continue
        inv = max(0, int(v.get("inventoryQuantity") or 0))
        prod = v.get("product") or {}
        title = prod.get("title")
        if m not in vendor_by_master:
            vendor_by_master[m] = prod.get("vendor")
            ptype_by_master[m]  = prod.get("productType")
        if c and c != m:
            size_skus_by_color[c].append(raw)
            inv_by_color[c] += inv
            color_by_master[m].add(c)
            if c not in product_by_color:
                product_by_color[c] = (title, prod.get("vendor"),
                                       prod.get("productType"), m, color_code_of(raw))
                if title and title not in titles_by_master[m]:
                    titles_by_master[m].append(title)
    # Aggregate master inventory from its color variants (avoids double
    # counting the raw variant sizes since those were already summed into
    # inv_by_color per colour).
    inv_by_master = {m: sum(inv_by_color[c] for c in colors)
                     for m, colors in color_by_master.items()}
    # variant_skus at master level = the colour-variant SKUs, sorted.
    var_by_master = {m: sorted(colors) for m, colors in color_by_master.items()}

    # 2. Roll ShopifyQL sales up by matching product_title. Sales come only
    #    at product_title grain (no direct SKU), so we split the row's total
    #    proportionally between all COLOR variants of that product (weighted
    #    by inventory as a proxy; falls back to even split).
    title_to_colors = defaultdict(list)          # title → [color_variants]
    for c, (title, _, _, _, _) in product_by_color.items():
        if title: title_to_colors[title.strip().lower()].append(c)

    sales_by_master = defaultdict(lambda: defaultdict(float))
    sales_by_color  = defaultdict(lambda: defaultdict(float))
    unmatched_titles = 0
    KEYS = ("net_items_sold","gross_sales","discounts","sales_reversals",
            "net_sales","taxes","total_sales")
    for row in sales:
        title  = (row.get("product_title") or "").strip().lower()
        colors = title_to_colors.get(title, [])
        if not colors:
            unmatched_titles += 1
            continue
        # Weight = inventory (fallback to 1 if all-zero). Skips negative inventory.
        weights = [max(1, inv_by_color.get(c, 0)) for c in colors]
        total_w = float(sum(weights)) or 1.0
        for c, w in zip(colors, weights):
            share = w / total_w
            parent = product_by_color[c][3]      # master
            for k in KEYS:
                v = float(row.get(k) or 0) * share
                sales_by_color[c][k]  += v
                sales_by_master[parent][k] += v
    print(f"  [rollup] {sum(1 for s in sales_by_master.values() if s)} masters had sales · "
          f"{sum(1 for s in sales_by_color.values() if s)} color variants had sales · "
          f"{unmatched_titles} titles unmatched")

    # 3. Ad spend / NCP — MASTER LEVEL ONLY (per user rule: "only map master
    #    sku names with ad's name, not color variants"). Scoped to the SAME
    #    date window as the sales — without this, one master pulled its
    #    lifetime spend (~₹1.4Cr) instead of the week's ~₹2L.
    all_masters = set(var_by_master.keys())
    ad_by_master = compute_ad_spend(cur, all_masters, days=days) if cur else {}

    # 4. Assemble rows. Emit one row per (level, sku).
    def _assemble(level, sku, parent, code, kids, inv, sales_map, ad_map, title):
        s = sales_map.get(sku, {})
        net_items = float(s.get("net_items_sold", 0))
        doq = (net_items / days) if days > 0 else None
        if level == "master":
            a = ad_map.get(sku, {"spend": 0.0, "ncp": 0, "matched_ad_count": 0})
            spend, ncp, macnt = a["spend"], a["ncp"], a["matched_ad_count"]
        else:
            spend, ncp, macnt = None, None, None
        cpn = (spend / ncp) if (spend is not None and ncp and ncp > 0) else None
        inv_int = int(inv)
        doh = (inv_int / doq) if (doq and doq > 0) else None
        roas = (float(s.get("total_sales", 0)) / spend) if (spend and spend > 0) else None
        vendor = vendor_by_master.get(parent or sku)
        ptype  = ptype_by_master.get(parent or sku)
        return {
            "window_key":       window_key,
            "level":            level,
            "sku":              sku,
            "parent_sku":       parent,
            "color_code":       code,
            "variant_skus":     sorted(set(kids)),
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
            "inventory_total":  int(inv),
            "variants_count":   len(kids),
            "ad_spend":         spend,
            "ad_ncp":           ncp,
            "cost_per_ncp":     cpn,
            "matched_ad_count": macnt,
            "doh":              doh,
            "roas":             roas,
            "cpis":             None,
        }

    rows = []
    for m in all_masters:
        master_title = canonical_master_title(titles_by_master.get(m, []))
        rows.append(_assemble("master", m, None, None,
                              var_by_master.get(m, []), inv_by_master.get(m, 0),
                              sales_by_master, ad_by_master, master_title))
    for c, (title, _, _, parent, code) in product_by_color.items():
        rows.append(_assemble("color", c, parent, code,
                              sorted(set(size_skus_by_color.get(c, []))),
                              inv_by_color.get(c, 0),
                              sales_by_color, {}, title))
    return rows

COLS = ["window_key","level","sku","parent_sku","color_code","variant_skus",
        "product_title","product_vendor","product_type",
        "net_items_sold","gross_sales","discounts","sales_reversals",
        "net_sales","taxes","total_sales","days_in_window","doq","inventory_total",
        "variants_count","ad_spend","ad_ncp","cost_per_ncp","matched_ad_count",
        "doh","roas","cpis"]

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
