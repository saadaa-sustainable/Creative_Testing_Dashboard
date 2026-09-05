"""
build_ad_result_snapshots.py — historical verdict checkpoints per ad.

The Ads Analyse "Category" column is computed from an ad's LIFETIME totals,
so a creative that won in week one and decayed afterwards reads as whatever
it is today. This script freezes the metric inputs at the two moments a test
is actually judged by, and writes them to public.ad_result_snapshots:

  d14_*   totals over the ad's first 14 days
          (ad_created_date .. ad_created_date + 14 inclusive — the same
           window result_classifier.py uses)
  k50_*   totals up to and including the day the ad's cumulative impressions
          first crossed 50,000

Only the METRICS are stored, never a verdict label: the dashboard runs them
through the same F1-F4 threshold inputs as the live Category column, so the
two historical columns stay consistent when someone edits a threshold.

Two deliberate differences from result_classifier.py:
  * the 50k scan covers the ad's WHOLE life, not just its first 14 days, so
    an ad that crosses on day 40 still gets a checkpoint;
  * because Meta reports per day, the totals at the crossing include the
    whole of the crossing day, so k50_impressions overshoots 50,000. That is
    the real state of the ad at the first moment anyone could have seen it
    cross.

Metrics come from backfill_table UNION primary_table, deduped per (ad_id,
date) with MAX — the same pairing ae_table_view reads, so a verdict here and
a verdict in the Category column are computed off the same numbers.
primary_table alone only reaches back to 2026-01-01 and would blank out the
history of every older ad.

The whole computation runs as ONE server-side statement; the two stores are
~2.2M rows between them, which is not something to stream into Python.

Usage:
    python build_ad_result_snapshots.py           # recompute + upsert all
    python build_ad_result_snapshots.py status    # row counts, no writes
"""

import os
import sys
import time
import logging

import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

# The 14-day window and the 50,000-impression bar are the two checkpoints the
# team judges a creative test by; they mirror result_classifier.WINDOW_DAYS /
# IMPRESSION_THRESHOLD and AE_DEFAULTS.f1 in the dashboard.
BUILD_SQL = """
INSERT INTO public.ad_result_snapshots (
    ad_id, account_name, ad_name, ad_created_date,
    d14_end_date, d14_complete, d14_impressions, d14_spend, d14_conv_value,
    d14_ncp_count, d14_ftewv_count, d14_roas, d14_cost_per_ncp, d14_cost_per_ftewv,
    k50_crossed_at, k50_days_to_cross, k50_impressions, k50_spend, k50_conv_value,
    k50_ncp_count, k50_ftewv_count, k50_roas, k50_cost_per_ncp, k50_cost_per_ftewv,
    last_computed_at)
-- One deduped daily series per ad across BOTH stores, exactly as the f1_hit
-- CTE in refresh_ae_table.py does it. backfill_table is the lifetime store
-- (2022 onwards, kept current by propagate_primary_to_backfill.py) and is
-- where ae_table_view reads its F1-F4 metrics from; primary_table only holds
-- 2026-01-01 onwards but can be fresher for the last few days. MAX per
-- (ad_id, date) takes whichever store has the fuller figure without ever
-- double-counting a day present in both.
WITH daily AS (
    SELECT ad_id, date,
           MAX(impressions)      AS imp,
           MAX(amount_spent_inr) AS sp,
           MAX(conversion_value) AS cv,
           MAX(ncp_count)        AS ncp,
           MAX(ftewv_count)      AS ft
      FROM (
            SELECT ad_id, date, COALESCE(impressions,0) impressions,
                   COALESCE(amount_spent_inr,0) amount_spent_inr,
                   COALESCE(conversion_value,0) conversion_value,
                   COALESCE(ncp_count,0) ncp_count, COALESCE(ftewv_count,0) ftewv_count
              FROM public.backfill_table WHERE ad_id IS NOT NULL
            UNION ALL
            SELECT ad_id, date, COALESCE(impressions,0),
                   COALESCE(amount_spent_inr,0), COALESCE(conversion_value,0),
                   COALESCE(ncp_count,0), COALESCE(ftewv_count,0)
              FROM public.primary_table  WHERE ad_id IS NOT NULL
           ) u
     GROUP BY ad_id, date
), meta AS (
    -- Keyed on ad_id alone to match ae_table_view's grain; account_name and
    -- ad_name come from the most recent row that carries them.
    SELECT ad_id,
           min(ad_created_date)                            AS ad_created_date,
           (array_agg(account_name ORDER BY date DESC))[1] AS account_name,
           (array_agg(ad_name      ORDER BY date DESC))[1] AS ad_name
      FROM (
            SELECT ad_id, date, ad_created_date, account_name, ad_name
              FROM public.backfill_table
             WHERE ad_id IS NOT NULL AND ad_created_date IS NOT NULL
            UNION ALL
            SELECT ad_id, date, ad_created_date, account_name, ad_name
              FROM public.primary_table
             WHERE ad_id IS NOT NULL AND ad_created_date IS NOT NULL
           ) m
     GROUP BY ad_id
), base AS (
    SELECT d.ad_id, d.date, m.ad_created_date, d.imp, d.sp, d.cv, d.ncp, d.ft
      FROM daily d
      JOIN meta m USING (ad_id)
     WHERE d.date >= m.ad_created_date
), d14 AS (
    SELECT ad_id, sum(imp) AS imp, sum(sp) AS sp, sum(cv) AS cv,
           sum(ncp) AS ncp, sum(ft) AS ft
      FROM base
     WHERE date <= ad_created_date + 14
     GROUP BY 1
), cum AS (
    SELECT ad_id, date, ad_created_date,
           sum(imp) OVER w AS c_imp, sum(sp)  OVER w AS c_sp,
           sum(cv)  OVER w AS c_cv,  sum(ncp) OVER w AS c_ncp,
           sum(ft)  OVER w AS c_ft
      FROM base
    WINDOW w AS (PARTITION BY ad_id ORDER BY date
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), k50 AS (
    SELECT DISTINCT ON (ad_id)
           ad_id, date AS crossed_at, (date - ad_created_date) AS days_to_cross,
           c_imp, c_sp, c_cv, c_ncp, c_ft
      FROM cum
     WHERE c_imp >= 50000
     ORDER BY ad_id, date
)
SELECT m.ad_id, COALESCE(m.account_name, ''), COALESCE(m.ad_name, ''), m.ad_created_date,
       m.ad_created_date + 14,
       CURRENT_DATE > m.ad_created_date + 14,
       COALESCE(d.imp, 0), COALESCE(d.sp, 0), COALESCE(d.cv, 0),
       COALESCE(d.ncp, 0), COALESCE(d.ft, 0),
       CASE WHEN d.sp  > 0 THEN round(d.cv / d.sp, 4) END,
       CASE WHEN d.ncp > 0 THEN round(d.sp / d.ncp, 2) END,
       CASE WHEN d.ft  > 0 THEN round(d.sp / d.ft,  2) END,
       k.crossed_at, k.days_to_cross,
       k.c_imp, k.c_sp, k.c_cv, k.c_ncp, k.c_ft,
       CASE WHEN k.c_sp  > 0 THEN round(k.c_cv / k.c_sp,  4) END,
       CASE WHEN k.c_ncp > 0 THEN round(k.c_sp / k.c_ncp, 2) END,
       CASE WHEN k.c_ft  > 0 THEN round(k.c_sp / k.c_ft,  2) END,
       NOW()
  FROM meta m
  LEFT JOIN d14 d USING (ad_id)
  LEFT JOIN k50 k USING (ad_id)
ON CONFLICT (ad_id) DO UPDATE SET
    account_name       = EXCLUDED.account_name,
    ad_name            = EXCLUDED.ad_name,
    ad_created_date    = EXCLUDED.ad_created_date,
    d14_end_date       = EXCLUDED.d14_end_date,
    d14_complete       = EXCLUDED.d14_complete,
    d14_impressions    = EXCLUDED.d14_impressions,
    d14_spend          = EXCLUDED.d14_spend,
    d14_conv_value     = EXCLUDED.d14_conv_value,
    d14_ncp_count      = EXCLUDED.d14_ncp_count,
    d14_ftewv_count    = EXCLUDED.d14_ftewv_count,
    d14_roas           = EXCLUDED.d14_roas,
    d14_cost_per_ncp   = EXCLUDED.d14_cost_per_ncp,
    d14_cost_per_ftewv = EXCLUDED.d14_cost_per_ftewv,
    k50_crossed_at     = EXCLUDED.k50_crossed_at,
    k50_days_to_cross  = EXCLUDED.k50_days_to_cross,
    k50_impressions    = EXCLUDED.k50_impressions,
    k50_spend          = EXCLUDED.k50_spend,
    k50_conv_value     = EXCLUDED.k50_conv_value,
    k50_ncp_count      = EXCLUDED.k50_ncp_count,
    k50_ftewv_count    = EXCLUDED.k50_ftewv_count,
    k50_roas           = EXCLUDED.k50_roas,
    k50_cost_per_ncp   = EXCLUDED.k50_cost_per_ncp,
    k50_cost_per_ftewv = EXCLUDED.k50_cost_per_ftewv,
    last_computed_at   = NOW()
"""


def _connect():
    conn = psycopg2.connect(SUPABASE_DB_URL, connect_timeout=30)
    conn.autocommit = False
    # The build scans ~1M primary_table rows and sorts them per ad for the
    # running impression total — minutes, not seconds. The pooler's default
    # statement_timeout would kill it partway.
    with conn.cursor() as cur:
        try:
            cur.execute("SET statement_timeout = '1800s'")
        except Exception:
            log.warning("could not raise statement_timeout — pooler may cap the build")
    return conn


def build() -> int:
    t0 = time.time()
    log.info("Building ad_result_snapshots (single server-side statement)…")
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(BUILD_SQL)
            n = cur.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    log.info(f"  upserted {n:,} ads in {time.time() - t0:.0f}s")
    return n


def show_status() -> None:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*),
                       count(*) FILTER (WHERE d14_complete),
                       count(*) FILTER (WHERE k50_crossed_at IS NOT NULL),
                       max(last_computed_at)
                  FROM ad_result_snapshots
            """)
            rows, d14_done, k50, last_run = cur.fetchone()
        log.info(f"  rows          : {rows:,}")
        log.info(f"  14d complete  : {d14_done:,}")
        log.info(f"  crossed 50k   : {k50:,}")
        log.info(f"  last computed : {last_run}")
    finally:
        conn.close()


def main() -> int:
    if not SUPABASE_DB_URL:
        log.error("SUPABASE_DB_URL is not set")
        return 1
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        show_status()
        return 0
    build()
    show_status()
    return 0


if __name__ == "__main__":
    sys.exit(main())
