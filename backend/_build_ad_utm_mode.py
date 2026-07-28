"""Pre-aggregate the mode (most common) UTM values + matched_value/tier
per ad_id into a small helper table. Speeds up the Apps Script UTM fetch
from ~63 batches of raw-order pages to ONE paginated fetch of ~15 pages.

Idempotent — safe to re-run. RLS enabled (service_role bypasses).
"""
import os, sys, io, psycopg2
from dotenv import load_dotenv
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
load_dotenv(override=True)
conn = psycopg2.connect(os.environ['SUPABASE_DB_URL'], connect_timeout=30, keepalives=1)
conn.autocommit = False

DDL = """
DROP TABLE IF EXISTS public.ad_utm_mode;
CREATE TABLE public.ad_utm_mode (
    ad_id             text PRIMARY KEY,
    utm_content_top   text,
    utm_term_top      text,
    utm_campaign_top  text,
    matched_value_top text,
    matched_tier_top  text,
    n_orders          integer NOT NULL,
    refreshed_at      timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.ad_utm_mode ENABLE ROW LEVEL SECURITY;
"""

INSERT_SQL = """
INSERT INTO public.ad_utm_mode (
    ad_id, utm_content_top, utm_term_top, utm_campaign_top,
    matched_value_top, matched_tier_top, n_orders
)
SELECT ad_id,
       MODE() WITHIN GROUP (ORDER BY utm_content)   FILTER (WHERE utm_content   IS NOT NULL AND utm_content   <> '') AS utm_content_top,
       MODE() WITHIN GROUP (ORDER BY utm_term)      FILTER (WHERE utm_term      IS NOT NULL AND utm_term      <> '') AS utm_term_top,
       MODE() WITHIN GROUP (ORDER BY utm_campaign)  FILTER (WHERE utm_campaign  IS NOT NULL AND utm_campaign  <> '') AS utm_campaign_top,
       MODE() WITHIN GROUP (ORDER BY matched_value) FILTER (WHERE matched_value IS NOT NULL AND matched_value <> '') AS matched_value_top,
       MODE() WITHIN GROUP (ORDER BY matched_tier)  FILTER (WHERE matched_tier  IS NOT NULL) AS matched_tier_top,
       COUNT(*)::int AS n_orders
  FROM public.shopify_ad_attribution
 WHERE ad_id IS NOT NULL AND ad_id <> ''
 GROUP BY ad_id;
"""

try:
    with conn.cursor() as cur:
        print("[1/2] Drop + create public.ad_utm_mode ...")
        cur.execute(DDL)
        print("[2/2] Populating (MODE per ad_id from shopify_ad_attribution) ...")
        cur.execute(INSERT_SQL)
        print(f"      rows inserted: {cur.rowcount:,}")
        cur.execute("SELECT COUNT(*), MIN(n_orders), MAX(n_orders), AVG(n_orders)::int FROM public.ad_utm_mode;")
        r = cur.fetchone()
        print(f"      distinct ad_ids: {r[0]:,}   orders per ad: min={r[1]} max={r[2]} avg={r[3]}")
        cur.execute("SELECT * FROM public.ad_utm_mode WHERE ad_id IN ('120222271965800422','120212815408130422','120210879750940422') ORDER BY ad_id;")
        print("\n      spot check on ads we touched today:")
        for row in cur.fetchall():
            print(f"        {row[0]}  content={(row[1] or '')[:40]:<42} term={(row[2] or '')[:20]:<22} tier={row[5]}  n={row[6]}")
    conn.commit()
    print("\n[COMMIT] ad_utm_mode built.")
except Exception as e:
    conn.rollback()
    print(f"\n[ROLLBACK] {type(e).__name__}: {e}")
    raise
finally:
    conn.close()
