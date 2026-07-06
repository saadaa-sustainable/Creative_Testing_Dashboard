"""
refresh_product_doq.py — build public.product_doq_daily by joining
BigQuery-sourced variant-SKU inventory rows to Shopify products via
the SKU-prefix tag pattern.

Join logic
----------
Shopify products carry SKU prefixes as free-form tags (e.g. 'SDFLS',
'SDCCT', 'SDLP', 'SMFHK'). Every BQ variant SKU begins with one of
these prefixes (e.g. 'SDFLSGGM' → SDFLS product, 'SDCCTWHS' → SDCCT
product). The join is:

    bq_inventory_daily.sku LIKE unnest(shopify_products.tags) || '%'

We filter tags to the SKU-shaped subset ('^S[A-Z]{2,}[A-Z0-9]*$', 4+
chars, all-uppercase letters/digits) so casual tags like 'Pants' or
'2025' don't produce spurious matches.

Aggregation
-----------
For each (product_id, date_day) we sum:
  - current_stock, total_inprogress
  - daily_quantity          (today's ordered qty)
  - t7_quantity, t45_quantity, t730_quantity
  - doq_7, doq_15, doq_30, doq_45, doq_90, doq_365
    (avg across the product's variants — mean, not sum, since DoQ is
     a per-variant rate)
  - oos_days_7 / 15 / 30 / 45 / 90 / 365   (max across variants —
     worst-case OOS days for the product)

Idempotent: full rebuild every run (TRUNCATE + INSERT).
"""
from __future__ import annotations
import os, sys, time, psycopg2
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: raise SystemExit("Missing SUPABASE_DB_URL in .env")

DDL = """
CREATE TABLE IF NOT EXISTS public.product_doq_daily (
    product_id     text        NOT NULL,
    date_day       date        NOT NULL,
    matched_tag    text        NOT NULL,   -- the tag that produced the match
    variant_ct     integer,                -- # of BQ variant SKUs rolled up
    current_stock  bigint,
    total_inprogress bigint,
    daily_quantity numeric,
    t7_quantity    numeric,
    t45_quantity   numeric,
    t730_quantity  numeric,
    doq_7          numeric,
    doq_15         numeric,
    doq_30         numeric,
    doq_45         numeric,
    doq_90         numeric,
    doq_365        numeric,
    oos_days_7     integer,
    oos_days_15    integer,
    oos_days_30    integer,
    oos_days_45    integer,
    oos_days_90    integer,
    oos_days_365   integer,
    synced_at      timestamptz DEFAULT NOW(),
    PRIMARY KEY (product_id, date_day, matched_tag)
);
CREATE INDEX IF NOT EXISTS ix_pdoq_date   ON public.product_doq_daily(date_day);
CREATE INDEX IF NOT EXISTS ix_pdoq_prod   ON public.product_doq_daily(product_id);
"""

REBUILD_SQL = """
WITH sku_tags AS (
    -- Expand product.tags → one row per (product_id, SKU-shaped tag)
    SELECT p.id                       AS product_id,
           UPPER(TRIM(t))             AS tag,
           LENGTH(TRIM(t))            AS tag_len
    FROM public.shopify_products p,
         UNNEST(p.tags) AS t
    WHERE TRIM(t) ~ '^S[A-Z]{2,}[A-Z0-9]*$'
      AND LENGTH(TRIM(t)) BETWEEN 4 AND 8
),
matches AS (
    -- Equi-join on LEFT(sku, tag_len) per tag length (4..8).
    -- Each SELECT is index-backed on ix_bqinv_sku → fast.
    SELECT st.product_id, st.tag, st.tag_len, b.sku, b.date_day,
           b.current_stock, b.total_inprogress,
           b.daily_quantity, b.t7_quantity, b.t45_quantity, b.t730_quantity,
           b.doq_7, b.doq_15, b.doq_30, b.doq_45, b.doq_90, b.doq_365,
           b.oos_days_7, b.oos_days_15, b.oos_days_30,
           b.oos_days_45, b.oos_days_90, b.oos_days_365
    FROM sku_tags st
    JOIN public.bq_inventory_daily b
      ON UPPER(LEFT(b.sku, st.tag_len)) = st.tag
    WHERE b.sku IS NOT NULL
),
best AS (
    -- If a SKU matches multiple tags (e.g. 'SDLS' and 'SDLSP' both match
    -- SDLSPGGM), keep only the LONGEST tag — the more specific product.
    SELECT DISTINCT ON (sku, date_day, product_id)
           product_id, tag AS matched_tag, sku, date_day,
           current_stock, total_inprogress,
           daily_quantity, t7_quantity, t45_quantity, t730_quantity,
           doq_7, doq_15, doq_30, doq_45, doq_90, doq_365,
           oos_days_7, oos_days_15, oos_days_30,
           oos_days_45, oos_days_90, oos_days_365
    FROM matches
    ORDER BY sku, date_day, product_id, tag_len DESC
)
INSERT INTO public.product_doq_daily (
    product_id, date_day, matched_tag, variant_ct,
    current_stock, total_inprogress,
    daily_quantity, t7_quantity, t45_quantity, t730_quantity,
    doq_7, doq_15, doq_30, doq_45, doq_90, doq_365,
    oos_days_7, oos_days_15, oos_days_30,
    oos_days_45, oos_days_90, oos_days_365
)
SELECT
    product_id,
    date_day,
    matched_tag,
    COUNT(DISTINCT sku)              AS variant_ct,
    SUM(current_stock)               AS current_stock,
    SUM(total_inprogress)            AS total_inprogress,
    SUM(daily_quantity)              AS daily_quantity,
    SUM(t7_quantity)                 AS t7_quantity,
    SUM(t45_quantity)                AS t45_quantity,
    SUM(t730_quantity)               AS t730_quantity,
    AVG(doq_7)                       AS doq_7,
    AVG(doq_15)                      AS doq_15,
    AVG(doq_30)                      AS doq_30,
    AVG(doq_45)                      AS doq_45,
    AVG(doq_90)                      AS doq_90,
    AVG(doq_365)                     AS doq_365,
    MAX(oos_days_7)                  AS oos_days_7,
    MAX(oos_days_15)                 AS oos_days_15,
    MAX(oos_days_30)                 AS oos_days_30,
    MAX(oos_days_45)                 AS oos_days_45,
    MAX(oos_days_90)                 AS oos_days_90,
    MAX(oos_days_365)                AS oos_days_365
FROM best
GROUP BY product_id, date_day, matched_tag
"""

def main():
    t0 = time.time()
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            # Direct DB session (not PostgREST) — safe to lift the 3s cap
            cur.execute("SET LOCAL statement_timeout = '600s'")
            cur.execute(DDL)
            cur.execute("TRUNCATE TABLE public.product_doq_daily")
            cur.execute(REBUILD_SQL)
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT product_id), COUNT(DISTINCT date_day) FROM public.product_doq_daily")
            n_rows, n_prods, n_days = cur.fetchone()
        conn.commit()

    print(f"[✓] product_doq_daily rebuilt in {time.time()-t0:.1f}s")
    print(f"    rows      : {n_rows:,}")
    print(f"    products  : {n_prods:,} (of ~1,185 in shopify_products)")
    print(f"    day slots : {n_days}")

if __name__ == "__main__":
    main()
