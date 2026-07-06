"""
ingest_bq_inventory.py — pull the latest daily snapshot of
`saadaa-wh.MAPLEMONK.saadaa_inventory_planning` from BigQuery and
truncate-load it into public.bq_inventory_daily in Supabase.

Auth
----
Uses Application Default Credentials (ADC) — no service account key
file, no API key. Set it up once with:

    gcloud auth application-default login
    gcloud config set project saadaa-wh

The google-cloud-bigquery client picks the ADC token up automatically.

Semantics
---------
The BigQuery table is a *daily* rolling snapshot — one row per (sku,
warehouse) for every day. For the dashboard we only care about the
freshest day, so we take WHERE date_day = MAX(date_day) and TRUNCATE +
INSERT into Supabase. Re-running is idempotent.
"""
from __future__ import annotations
import os, sys, time, warnings, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Silence the cosmetic ADC quota-project warning — job billing goes to the
# resource-owner project (saadaa-wh) automatically.
warnings.filterwarnings("ignore", message=".*without a quota project.*")

from google.cloud import bigquery

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: raise SystemExit("Missing SUPABASE_DB_URL in .env")

BQ_PROJECT = "saadaa-wh"
BQ_TABLE   = "`saadaa-wh.MAPLEMONK.saadaa_inventory_planning`"

# Column list matches the BQ schema.  Order here defines the INSERT order.
COLS = [
    ("date_day",                              "date"),
    ("sku",                                   "text"),
    ("warehouse",                             "text"),
    ("rm_code",                               "text"),   # RM_code
    ("dyed_fabric_sku",                       "text"),   # Dyed_Fabric_SKU
    ("product_name",                          "text"),
    ("product_variant",                       "text"),   # Product_Variant
    ("color",                                 "text"),   # Color
    ("size",                                  "text"),   # Size
    ("category",                              "text"),
    ("sub_category",                          "text"),   # Sub_Category
    ("fabric_consumption_avg",                "numeric"),# Fabric_Consumption_Average
    ("category_type",                         "text"),   # CategoryType
    ("cost",                                  "numeric"),
    ("item_category",                         "text"),
    ("gender",                                "text"),
    ("fit_type",                              "text"),   # FitType
    ("age_group",                             "text"),   # Age_Group
    ("demographic_price_range",               "text"),
    ("weave_type",                            "text"),   # WEAVE_TYPE
    ("fabric_name",                           "text"),   # FABRIC_NAME
    ("fabric_composition",                    "text"),   # FABRIC_COMPOSITION
    ("fabric_gsm",                            "text"),   # FABRIC_GSM
    ("garment_length_type",                   "text"),
    ("sleeve_type",                           "text"),
    ("neck_collar_type",                      "text"),   # Neck_Collar_Type
    ("replenishment_type",                    "text"),
    ("washcare_sku",                          "text"),   # Washcare_SKU
    ("season",                                "text"),
    ("gst",                                   "text"),   # GST
    ("related_ongoing_product",               "text"),
    ("qty_in_metres",                         "text"),
    ("product_state",                         "text"),
    ("daily_quantity",                        "numeric"),
    ("has_inventory_today",                   "integer"),
    ("t7_quantity",                           "numeric"),
    ("t730_quantity",                         "numeric"),
    ("t73015_quantity",                       "numeric"),
    ("t45_quantity",                          "numeric"),
    ("doq_7",                                 "numeric"),
    ("doq_15",                                "numeric"),
    ("doq_7_30",                              "numeric"),
    ("doq_30_45",                             "numeric"),
    ("doq_90",                                "numeric"),
    ("doq_30",                                "numeric"),
    ("doq_45",                                "numeric"),
    ("doq_365",                               "numeric"),
    ("oos_days_7",                            "integer"),
    ("oos_days_15",                           "integer"),
    ("oos_days_30",                           "integer"),
    ("oos_days_45",                           "integer"),
    ("oos_days_90",                           "integer"),
    ("oos_days_365",                          "integer"),
    ("total_sales_last_45_inv_days",          "numeric"),# total_sales_in_last_45_inventory_days
    ("weighted_doq_45",                       "numeric"),
    ("weightage_doq",                         "numeric"),
    ("monthly_doq",                           "numeric"),
    ("yearly_doq",                            "numeric"),
    ("lead_time",                             "integer"),# Lead_Time
    ("buffer_days",                           "integer"),# Buffer_Days
    ("current_stock",                         "integer"),
    ("total_inprogress",                      "integer"),
    ("shopify_sp",                            "numeric"),
    ("v_doq",                                 "numeric"),
]

# BigQuery column names (case-sensitive — schema uses mixed case)
BQ_SELECT_COLS = [
    "date_day", "sku", "warehouse", "RM_code", "Dyed_Fabric_SKU",
    "product_name", "Product_Variant", "Color", "Size", "Category",
    "Sub_Category", "Fabric_Consumption_Average", "CategoryType", "cost",
    "item_category", "Gender", "FitType", "Age_Group",
    "demographic_price_range", "WEAVE_TYPE", "FABRIC_NAME",
    "FABRIC_COMPOSITION", "FABRIC_GSM", "Garment_Length_Type",
    "Sleeve_Type", "Neck_Collar_Type", "Replenishment_Type",
    "Washcare_SKU", "Season", "GST", "Related_Ongoing_Product",
    "Qty_in_metres", "product_state", "daily_quantity",
    "has_inventory_today", "t7_quantity", "t730_quantity",
    "t73015_quantity", "t45_quantity", "doq_7", "doq_15", "doq_7_30",
    "doq_30_45", "doq_90", "doq_30", "doq_45", "doq_365", "oos_days_7",
    "oos_days_15", "oos_days_30", "oos_days_45", "oos_days_90",
    "oos_days_365", "total_sales_in_last_45_inventory_days",
    "weighted_doq_45", "weightage_doq", "monthly_doq", "yearly_doq",
    "Lead_Time", "Buffer_Days", "current_stock", "total_inprogress",
    "shopify_sp", "v_doq",
]

DDL = f"""
CREATE TABLE IF NOT EXISTS public.bq_inventory_daily (
    { ',\n    '.join(f'{n} {t}' for n, t in COLS) },
    synced_at timestamptz DEFAULT NOW()
);
-- Truncate-and-load semantics — no PK needed, warehouse can be NULL
-- for raw-material rows.  Indexes cover the dashboard query patterns.
CREATE INDEX IF NOT EXISTS ix_bqinv_sku         ON public.bq_inventory_daily(sku);
CREATE INDEX IF NOT EXISTS ix_bqinv_category    ON public.bq_inventory_daily(category);
CREATE INDEX IF NOT EXISTS ix_bqinv_gender      ON public.bq_inventory_daily(gender);
CREATE INDEX IF NOT EXISTS ix_bqinv_prodstate   ON public.bq_inventory_daily(product_state);
CREATE INDEX IF NOT EXISTS ix_bqinv_doq45       ON public.bq_inventory_daily(doq_45);
"""

def _v(x):
    """Coerce BQ Row value into a psycopg2-safe scalar (None on empty)."""
    if x is None: return None
    if isinstance(x, str):
        s = x.strip()
        return s if s else None
    return x

def main():
    t0 = time.time()
    print(f"[*] BigQuery client bootstrap — project={BQ_PROJECT}")
    try:
        client = bigquery.Client(project=BQ_PROJECT)
    except Exception as e:
        raise SystemExit(
            f"BQ client init failed: {type(e).__name__}: {e}\n"
            "Did you run `gcloud auth application-default login`?"
        )

    # 1. peek at max date so we can report the window we're pulling
    q_max = f"SELECT MAX(date_day) AS d FROM {BQ_TABLE}"
    max_day = list(client.query(q_max).result())[0].d
    print(f"[*] latest snapshot in BQ : {max_day}  → pulling last 7 days")

    # 2. pull the last 7 days worth of snapshots — enough recency to
    # power DoQ trends without ballooning the row count.
    sel = ",\n    ".join(f"`{c}`" for c in BQ_SELECT_COLS)
    q = f"""
        SELECT {sel}
        FROM {BQ_TABLE}
        WHERE date_day BETWEEN
              DATE_SUB((SELECT MAX(date_day) FROM {BQ_TABLE}), INTERVAL 6 DAY)
          AND (SELECT MAX(date_day) FROM {BQ_TABLE})
    """
    print("[*] querying last-7-days snapshots ...")
    rows = list(client.query(q).result())
    print(f"[ok] fetched {len(rows):,} rows from BigQuery in {time.time()-t0:.1f}s")
    if not rows:
        raise SystemExit("BQ query returned 0 rows — nothing to load")

    # 3. Supabase target
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("TRUNCATE TABLE public.bq_inventory_daily")

            insert_cols = ", ".join(n for n, _ in COLS)
            sql = f"INSERT INTO public.bq_inventory_daily ({insert_cols}) VALUES %s"

            # Bulk insert with execute_values — 50-100× faster than
            # executemany for 11k+ rows because it issues a single
            # multi-row INSERT statement per batch.
            all_tuples = [tuple(_v(getattr(r, c)) for c in BQ_SELECT_COLS) for r in rows]
            execute_values(cur, sql, all_tuples, page_size=1000)
            n_written = len(all_tuples)
        conn.commit()

    print(f"[✓] loaded {n_written:,} rows into public.bq_inventory_daily "
          f"(date_day={max_day})  ·  {time.time()-t0:.1f}s total")

if __name__ == "__main__":
    main()
