"""
sync_shopify_products.py — pull the Shopify product catalogue via Admin
GraphQL and upsert into public.shopify_products.

Solves the CORS block that hits the dashboard when it tries to call
Shopify Admin directly. The dashboard now reads public.shopify_products
via Supabase REST (anon), no cross-origin request needed.

Credentials via .env (dotenv-loaded, never printed):
  SHOP_DOMAIN            e.g. saadaa.myshopify.com
  ADMIN_ACCESS_TOKEN     shpat_...
  SHOPIFY_API_VERSION    e.g. 2025-10 (default)
  SUPABASE_DB_URL        Postgres URL for upsert

Run manually or wire into full_run.py as a pipeline step.
"""
from __future__ import annotations
import os, sys, re, json, time, requests, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv(override=True)

SHOP_DOMAIN = (os.environ.get("SHOP_DOMAIN") or "").strip()
SHOP_TOKEN  = (os.environ.get("ADMIN_ACCESS_TOKEN") or "").strip()
API_VER     = (os.environ.get("SHOPIFY_API_VERSION") or "2025-10").strip()
DB_URL      = (os.environ.get("SUPABASE_DB_URL") or "").strip()

if not (SHOP_DOMAIN and SHOP_TOKEN and DB_URL):
    raise SystemExit("Missing SHOP_DOMAIN / ADMIN_ACCESS_TOKEN / SUPABASE_DB_URL in .env")

TOK_RE = re.compile(r"(?:shpat_[A-Za-z0-9]{20,}|shpca_[A-Za-z0-9]{20,}|eyJ[\w\-.]{40,})")
def scrub(s): return TOK_RE.sub("<REDACTED>", str(s or ""))

def say(msg): print(scrub(str(msg)))

GRAPHQL = f"https://{SHOP_DOMAIN}/admin/api/{API_VER}/graphql.json"
HDRS    = {"X-Shopify-Access-Token": SHOP_TOKEN, "Content-Type": "application/json"}

# Matches the fields the dashboard needs. Kept small so a full-catalogue
# scan is quick and the response fits in Shopify's 1 MB cap per page.
QUERY = """
query InvPage($after: String) {
  products(first: 100, after: $after) {
    edges {
      cursor
      node {
        id
        title
        status
        vendor
        productType
        handle
        tags
        createdAt
        updatedAt
        totalInventory
        featuredImage { url }
        priceRangeV2 {
          minVariantPrice { amount }
          maxVariantPrice { amount }
        }
        variants(first: 1) { edges { node { id } } }
        variantsCount { count }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

def _fetch_all():
    products = []
    cursor = None
    page   = 0
    started = time.time()
    while True:
        page += 1
        r = requests.post(GRAPHQL, headers=HDRS,
                          json={"query": QUERY, "variables": {"after": cursor}},
                          timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify {r.status_code}: {scrub(r.text)[:200]}")
        j = r.json()
        errs = j.get("errors")
        if errs:
            raise RuntimeError(f"GraphQL errors: {scrub(json.dumps(errs))[:300]}")
        pd = j.get("data", {}).get("products", {}) or {}
        edges = pd.get("edges", []) or []
        for e in edges:
            n = e.get("node") or {}
            price = n.get("priceRangeV2") or {}
            pmin = (price.get("minVariantPrice") or {}).get("amount")
            pmax = (price.get("maxVariantPrice") or {}).get("amount")
            img  = (n.get("featuredImage") or {}).get("url") or None
            vc   = (n.get("variantsCount") or {}).get("count")
            products.append({
                "id":              n.get("id"),
                "title":           n.get("title"),
                "status":          n.get("status"),
                "vendor":          n.get("vendor") or None,
                "product_type":    n.get("productType") or None,
                "handle":          n.get("handle"),
                "tags":            n.get("tags") or [],
                "image_url":       img,
                "variant_count":   int(vc) if vc is not None else None,
                "total_inventory": n.get("totalInventory"),
                "price_min":       float(pmin) if pmin else None,
                "price_max":       float(pmax) if pmax else None,
                "created_at_shop": n.get("createdAt"),
                "updated_at_shop": n.get("updatedAt"),
            })
        pi = pd.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        say(f"    page {page:3d}  cursor={cursor[:20] if cursor else '-'}…  running total={len(products):,}")
    say(f"[ok] fetched {len(products):,} products in {time.time()-started:.1f}s ({page} pages)")
    return products

def _upsert(products):
    if not products:
        say("[!] nothing to upsert"); return
    sql = """
    INSERT INTO public.shopify_products
      (id, title, status, vendor, product_type, handle, tags, image_url,
       variant_count, total_inventory, price_min, price_max,
       created_at_shop, updated_at_shop)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
      title            = EXCLUDED.title,
      status           = EXCLUDED.status,
      vendor           = EXCLUDED.vendor,
      product_type     = EXCLUDED.product_type,
      handle           = EXCLUDED.handle,
      tags             = EXCLUDED.tags,
      image_url        = EXCLUDED.image_url,
      variant_count    = EXCLUDED.variant_count,
      total_inventory  = EXCLUDED.total_inventory,
      price_min        = EXCLUDED.price_min,
      price_max        = EXCLUDED.price_max,
      created_at_shop  = EXCLUDED.created_at_shop,
      updated_at_shop  = EXCLUDED.updated_at_shop,
      synced_at        = NOW()
    """
    rows = [(
        p["id"], p["title"], p["status"], p["vendor"], p["product_type"],
        p["handle"], p["tags"], p["image_url"],
        p["variant_count"], p["total_inventory"], p["price_min"], p["price_max"],
        p["created_at_shop"], p["updated_at_shop"],
    ) for p in products]

    conn = psycopg2.connect(DB_URL)
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, template=None, page_size=200)
        conn.commit()
        say(f"[ok] upserted {len(rows):,} rows into public.shopify_products")
    finally:
        conn.close()

def _delete_stale(seen_ids):
    """Remove cached products that were deleted upstream."""
    if not seen_ids:
        say("[!] skipping stale delete — no seen ids")
        return
    conn = psycopg2.connect(DB_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM public.shopify_products
                 WHERE id NOT IN %s
            """, (tuple(seen_ids),))
            deleted = cur.rowcount
        conn.commit()
        if deleted:
            say(f"[ok] deleted {deleted:,} stale rows (products removed from Shopify)")
        else:
            say(f"[ok] no stale rows to delete")
    finally:
        conn.close()

if __name__ == "__main__":
    say(f"[*] syncing Shopify products from https://{SHOP_DOMAIN}/admin/api/{API_VER}")
    products = _fetch_all()
    _upsert(products)
    _delete_stale({p["id"] for p in products})
