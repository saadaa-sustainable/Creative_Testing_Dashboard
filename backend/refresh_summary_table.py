"""Rebuild summary_table from backfill_table.

One row per unique ad_id with lifetime aggregates and lifecycle status.
Run anytime backfill_table changes (e.g., after run_backfill.py).

Status rules (6 categories — mirrors dashboard getAdCategory):
    Incremental Winner = F1 AND (F2 OR F3) AND F4
    Winner             = F1 AND (F2 OR F3)
    P0 analysis        = F1 AND F4
    P1 analysis        = F1 only
    P2 analysis        = F2 only
    Discarded          = everything else

Thresholds:
    F1 = impressions ≥ 50,000
    F2 = conv_value / spend ≥ 3.2
    F3 = spend / ncp     ≤ 525  (positive)
    F4 = spend / ftewv   ≤ 25   (positive)
"""

import os
import sys
import time
import logging

import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("summary_table")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS summary_table (
    ad_id              TEXT PRIMARY KEY,
    ad_name            TEXT,
    created_date       DATE,
    last_seen          DATE,
    days_active        INT,
    total_impressions  BIGINT,
    total_spend        NUMERIC(14,2),
    total_conv_value   NUMERIC(14,2),
    total_ncp          INT,
    total_ftewv        INT,
    status             TEXT,
    ad_status          TEXT,
    f1_pass            BOOLEAN,
    f2_pass            BOOLEAN,
    f3_pass            BOOLEAN,
    f4_pass            BOOLEAN,
    -- Shopify-attribution lifetime aggregates (NULL = no matched orders)
    shopify_orders         BIGINT,
    shopify_sales          NUMERIC(14,2),
    shopify_aov            NUMERIC(12,2),
    shopify_roas           NUMERIC(14,3),   -- shopify_sales / total_spend
    shopify_first_order    DATE,
    shopify_last_order     DATE,
    shopify_top_tier       TEXT,           -- dominant match tier (most $-volume)
    refreshed_at       TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS refreshed_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS ad_status TEXT;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS f1_pass BOOLEAN;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS f2_pass BOOLEAN;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS f3_pass BOOLEAN;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS f4_pass BOOLEAN;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS shopify_orders BIGINT;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS shopify_sales NUMERIC(14,2);
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS shopify_aov NUMERIC(12,2);
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS shopify_roas NUMERIC(14,3);
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS shopify_first_order DATE;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS shopify_last_order DATE;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS shopify_top_tier TEXT;
-- Live preview URLs (Meta Graph scrape → ad_thumbnails).  Instagram permalink
-- is the base URL; the dashboard appends /embed/captioned/ at render time to
-- build the iframe src.  Facebook permalink drives the FB plugin iframe.
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS instagram_permalink TEXT;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS fb_permalink        TEXT;
-- Verdict-history: status_at is when the CURRENT status was set (i.e. when
-- the ad last transitioned INTO its present verdict). prev_status is the
-- verdict it held BEFORE the current one, and prev_status_at is when that
-- verdict was first applied. Together they let the 14-day buffer resolver
-- show "Winner (5 d) — was P0 analysis (12 d)" without any audit table.
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS status_at       TIMESTAMPTZ;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS prev_status     TEXT;
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS prev_status_at  TIMESTAMPTZ;
"""

REFRESH_SQL = """
-- UPSERT (not TRUNCATE+INSERT) so verdict-history columns can carry
-- forward across rebuilds. Old logic destroyed all state every run,
-- which meant we could never tell WHEN an ad's verdict actually
-- changed — every row's status_at was NOW() regardless of drift.
INSERT INTO summary_table (
    ad_id, ad_name, created_date, last_seen, days_active,
    total_impressions, total_spend, total_conv_value, total_ncp, total_ftewv,
    status, ad_status, f1_pass, f2_pass, f3_pass, f4_pass,
    shopify_orders, shopify_sales, shopify_aov, shopify_roas,
    shopify_first_order, shopify_last_order, shopify_top_tier,
    instagram_permalink, fb_permalink,
    status_at,
    refreshed_at
)
WITH agg AS (
    SELECT
        ad_id,
        (ARRAY_AGG(ad_name ORDER BY date DESC))[1]            AS ad_name,
        MIN(ad_created_date)::date                            AS created_date,
        MAX(date)                                             AS last_seen,
        COUNT(DISTINCT date)                                  AS days_active,
        COALESCE(SUM(impressions), 0)                         AS total_impressions,
        ROUND(COALESCE(SUM(amount_spent_inr), 0)::numeric, 2) AS total_spend,
        ROUND(COALESCE(SUM(conversion_value), 0)::numeric, 2) AS total_conv_value,
        COALESCE(SUM(ncp_count), 0)                           AS total_ncp,
        COALESCE(SUM(ftewv_count), 0)                         AS total_ftewv
    FROM backfill_table
    WHERE ad_id IS NOT NULL
    GROUP BY ad_id
),
-- Shopify-attribution lifetime aggregates per ad_id.
-- shopify_top_tier = the matched_tier that contributed the highest $ volume
-- (a "primary attribution path" indicator for the dashboard).
shopify_agg AS (
    SELECT ad_id,
           COUNT(*)                          AS shopify_orders,
           ROUND(SUM(total_price), 2)        AS shopify_sales,
           ROUND(AVG(total_price), 2)        AS shopify_aov,
           MIN(order_created_at)::date       AS shopify_first_order,
           MAX(order_created_at)::date       AS shopify_last_order
    FROM shopify_ad_attribution
    WHERE has_match AND ad_id IS NOT NULL AND ad_id <> ''
    GROUP BY ad_id
),
shopify_top_tier AS (
    SELECT DISTINCT ON (ad_id) ad_id, matched_tier AS shopify_top_tier
    FROM (
        SELECT ad_id, matched_tier, SUM(total_price) AS tier_sales
        FROM shopify_ad_attribution
        WHERE has_match AND ad_id IS NOT NULL AND ad_id <> ''
        GROUP BY ad_id, matched_tier
    ) t
    ORDER BY ad_id, tier_sales DESC
),
-- Current Meta status per ad: from primary_table (last 15-day rolling window).
-- NULL means the ad hasn't delivered impressions in the recent sync window,
-- so it is effectively not in active rotation regardless of what its last
-- backfill row shows. Use primary-only here to avoid the stale-ACTIVE trap
-- where backfill_table records the status on the *last delivery day*, not now.
latest_status AS (
    SELECT a.ad_id, p.ad_status
    FROM agg a
    LEFT JOIN LATERAL (
        SELECT ad_status FROM primary_table p2
        WHERE p2.ad_id = a.ad_id AND p2.ad_status IS NOT NULL
        ORDER BY p2.date DESC LIMIT 1
    ) p ON TRUE
),
-- Instagram / Facebook permalinks — one row per ad_id from ad_thumbnails
-- (Meta Graph scrape).  Instagram is the URL used for the drawer's live
-- iframe embed; the dashboard appends /embed/captioned/ at render time.
permalinks AS (
    SELECT ad_id, instagram_permalink, fb_permalink
    FROM public.ad_thumbnails
)
SELECT
    a.ad_id, a.ad_name, a.created_date, a.last_seen, a.days_active,
    a.total_impressions, a.total_spend, a.total_conv_value, a.total_ncp, a.total_ftewv,
    CASE
        -- Incremental Winner = F1 AND (F2 OR F3) AND F4
        WHEN a.total_impressions >= 50000
         AND ((a.total_spend > 0 AND a.total_conv_value / a.total_spend >= 3.2)
              OR (a.total_ncp > 0 AND a.total_spend / a.total_ncp <= 525))
         AND a.total_ftewv > 0 AND a.total_spend / a.total_ftewv <= 25
            THEN 'Incremental Winner'
        -- Winner = F1 AND (F2 OR F3)
        WHEN a.total_impressions >= 50000
         AND ((a.total_spend > 0 AND a.total_conv_value / a.total_spend >= 3.2)
              OR (a.total_ncp > 0 AND a.total_spend / a.total_ncp <= 525))
            THEN 'Winner'
        -- P0 analysis (was Priority) = F1 AND F4
        WHEN a.total_impressions >= 50000
         AND a.total_ftewv > 0 AND a.total_spend / a.total_ftewv <= 25
            THEN 'P0 analysis'
        -- P1 analysis (was Analyze 1) = F1 only
        WHEN a.total_impressions >= 50000
            THEN 'P1 analysis'
        -- P2 analysis (was Analyze 2) = F2 only
        WHEN a.total_spend > 0 AND a.total_conv_value / a.total_spend >= 3.2
            THEN 'P2 analysis'
        ELSE 'Discarded'
    END  AS status,
    ls.ad_status,
    (a.total_impressions >= 50000)                                                                AS f1_pass,
    (a.total_spend > 0 AND a.total_conv_value / a.total_spend >= 3.2)                             AS f2_pass,
    (a.total_ncp   > 0 AND a.total_spend / a.total_ncp   <= 525)                                  AS f3_pass,
    (a.total_ftewv > 0 AND a.total_spend / a.total_ftewv <= 25)                                   AS f4_pass,
    sa.shopify_orders,
    sa.shopify_sales,
    sa.shopify_aov,
    CASE WHEN a.total_spend > 0 AND sa.shopify_sales IS NOT NULL
         THEN ROUND((sa.shopify_sales / a.total_spend)::numeric, 3)
         ELSE NULL END                                                                            AS shopify_roas,
    sa.shopify_first_order,
    sa.shopify_last_order,
    st.shopify_top_tier,
    pl.instagram_permalink,
    pl.fb_permalink,
    NOW() AS status_at,     -- overridden by the ON CONFLICT branch below when status is unchanged
    NOW() AS refreshed_at
FROM agg a
LEFT JOIN latest_status ls USING (ad_id)
LEFT JOIN shopify_agg sa USING (ad_id)
LEFT JOIN shopify_top_tier st USING (ad_id)
LEFT JOIN permalinks pl USING (ad_id)
ON CONFLICT (ad_id) DO UPDATE SET
    ad_name             = EXCLUDED.ad_name,
    created_date        = EXCLUDED.created_date,
    last_seen           = EXCLUDED.last_seen,
    days_active         = EXCLUDED.days_active,
    total_impressions   = EXCLUDED.total_impressions,
    total_spend         = EXCLUDED.total_spend,
    total_conv_value    = EXCLUDED.total_conv_value,
    total_ncp           = EXCLUDED.total_ncp,
    total_ftewv         = EXCLUDED.total_ftewv,
    ad_status           = EXCLUDED.ad_status,
    f1_pass             = EXCLUDED.f1_pass,
    f2_pass             = EXCLUDED.f2_pass,
    f3_pass             = EXCLUDED.f3_pass,
    f4_pass             = EXCLUDED.f4_pass,
    shopify_orders      = EXCLUDED.shopify_orders,
    shopify_sales       = EXCLUDED.shopify_sales,
    shopify_aov         = EXCLUDED.shopify_aov,
    shopify_roas        = EXCLUDED.shopify_roas,
    shopify_first_order = EXCLUDED.shopify_first_order,
    shopify_last_order  = EXCLUDED.shopify_last_order,
    shopify_top_tier    = EXCLUDED.shopify_top_tier,
    instagram_permalink = EXCLUDED.instagram_permalink,
    fb_permalink        = EXCLUDED.fb_permalink,
    refreshed_at        = EXCLUDED.refreshed_at,
    -- Verdict-history bookkeeping: only rewrite status_at/prev_status when
    -- the status *actually* changes. Same-status rebuilds keep the old
    -- status_at so users see "Winner (12 d ago)" not "Winner (just now)".
    status              = EXCLUDED.status,
    status_at           = CASE
        WHEN summary_table.status IS DISTINCT FROM EXCLUDED.status
        THEN EXCLUDED.status_at
        ELSE summary_table.status_at
    END,
    prev_status         = CASE
        WHEN summary_table.status IS DISTINCT FROM EXCLUDED.status
        THEN summary_table.status
        ELSE summary_table.prev_status
    END,
    prev_status_at      = CASE
        WHEN summary_table.status IS DISTINCT FROM EXCLUDED.status
        THEN summary_table.status_at
        ELSE summary_table.prev_status_at
    END;
"""


def main():
    log.info("Connecting to Supabase…")
    t0 = time.time()
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '30min'")

    log.info("Ensuring summary_table schema exists…")
    cur.execute(SCHEMA_SQL)
    conn.commit()

    log.info("Running UPSERT from backfill_table (verdict history preserved)…")
    cur.execute(REFRESH_SQL)
    conn.commit()

    elapsed = time.time() - t0
    log.info(f"Done in {elapsed:.1f}s")

    # Report counts
    cur.execute("SELECT COUNT(*) FROM summary_table")
    total = cur.fetchone()[0]
    log.info(f"summary_table: {total:,} rows")

    cur.execute("SELECT status, COUNT(*) FROM summary_table GROUP BY status ORDER BY COUNT(*) DESC")
    log.info("Per-status breakdown (F-filter):")
    for status, n in cur.fetchall():
        log.info(f"  {status:<20} {n:>6,}")

    cur.execute("""SELECT COALESCE(ad_status,'(unknown)'), COUNT(*)
                   FROM summary_table GROUP BY 1 ORDER BY 2 DESC""")
    log.info("Per-ad_status breakdown (Meta):")
    for s, n in cur.fetchall():
        log.info(f"  {s:<20} {n:>6,}")

    cur.execute("""SELECT
        COUNT(*) FILTER (WHERE shopify_orders > 0),
        ROUND(SUM(shopify_orders),0),
        ROUND(SUM(shopify_sales),0),
        ROUND(AVG(shopify_roas) FILTER (WHERE shopify_roas IS NOT NULL),3)
        FROM summary_table""")
    n_with, o_sum, s_sum, roas_avg = cur.fetchone()
    log.info("Shopify attribution rolled into summary_table:")
    log.info(f"  ads with >=1 attributed order : {n_with:,}")
    log.info(f"  total attributed orders       : {o_sum or 0:,.0f}")
    log.info(f"  total attributed sales (Rs)   : {s_sum or 0:,.0f}")
    log.info(f"  avg shopify_roas (per ad)     : {roas_avg or 0}")

    # ── Per-ad analytical helper views ─────────────────────────────────
    log.info("Creating helper views: ad_order_detail, ad_daily_sales ...")
    cur.execute("""
        CREATE OR REPLACE VIEW ad_order_detail AS
        SELECT
            ad_id, order_id, order_created_at, ordered_item, total_price,
            utm_source, utm_medium, utm_campaign, utm_content, utm_term,
            matched_tier, matched_value
        FROM shopify_ad_attribution
        WHERE has_match AND ad_id IS NOT NULL AND ad_id <> '';

        CREATE OR REPLACE VIEW ad_daily_sales AS
        SELECT ad_id,
               order_created_at::date AS day,
               COUNT(*)                          AS orders,
               ROUND(SUM(total_price), 2)        AS sales,
               ROUND(AVG(total_price), 2)        AS aov
        FROM shopify_ad_attribution
        WHERE has_match AND ad_id IS NOT NULL AND ad_id <> ''
        GROUP BY ad_id, order_created_at::date;
    """)
    conn.commit()
    log.info("  views ready")

    cur.execute("""
        SELECT MIN(last_seen)::text, MAX(last_seen)::text,
               MIN(created_date)::text, MAX(created_date)::text
        FROM summary_table
    """)
    min_ls, max_ls, min_cd, max_cd = cur.fetchone()
    log.info(f"Last seen range : {min_ls} -> {max_ls}")
    log.info(f"Created date range: {min_cd} -> {max_cd}")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
