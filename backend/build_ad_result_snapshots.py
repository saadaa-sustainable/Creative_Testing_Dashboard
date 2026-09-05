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

The whole computation runs as ONE server-side statement. primary_table is
~1M rows; streaming that into Python (as result_classifier.py does) is both
slower and fragile through the Supabase pooler.

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
    account_name, ad_id, ad_name, ad_created_date,
    d14_end_date, d14_complete, d14_impressions, d14_spend, d14_conv_value,
    d14_ncp_count, d14_ftewv_count, d14_roas, d14_cost_per_ncp, d14_cost_per_ftewv,
    k50_crossed_at, k50_days_to_cross, k50_impressions, k50_spend, k50_conv_value,
    k50_ncp_count, k50_ftewv_count, k50_roas, k50_cost_per_ncp, k50_cost_per_ftewv,
    last_computed_at)
WITH meta AS (
    SELECT account_name, ad_id,
           min(ad_created_date)                        AS ad_created_date,
           (array_agg(ad_name ORDER BY date DESC))[1]  AS ad_name
      FROM public.primary_table
     WHERE ad_created_date IS NOT NULL AND ad_id IS NOT NULL
     GROUP BY 1, 2
), base AS (
    SELECT p.account_name, p.ad_id, p.date, m.ad_created_date,
           COALESCE(p.impressions, 0)       AS imp,
           COALESCE(p.amount_spent_inr, 0)  AS sp,
           COALESCE(p.conversion_value, 0)  AS cv,
           COALESCE(p.ncp_count, 0)         AS ncp,
           COALESCE(p.ftewv_count, 0)       AS ft
      FROM public.primary_table p
      JOIN meta m USING (account_name, ad_id)
     WHERE p.date >= m.ad_created_date
), d14 AS (
    SELECT account_name, ad_id,
           sum(imp) AS imp, sum(sp) AS sp, sum(cv) AS cv,
           sum(ncp) AS ncp, sum(ft) AS ft
      FROM base
     WHERE date <= ad_created_date + 14
     GROUP BY 1, 2
), cum AS (
    SELECT account_name, ad_id, date, ad_created_date,
           sum(imp) OVER w AS c_imp, sum(sp)  OVER w AS c_sp,
           sum(cv)  OVER w AS c_cv,  sum(ncp) OVER w AS c_ncp,
           sum(ft)  OVER w AS c_ft
      FROM base
    WINDOW w AS (PARTITION BY account_name, ad_id ORDER BY date
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), k50 AS (
    SELECT DISTINCT ON (account_name, ad_id)
           account_name, ad_id,
           date                       AS crossed_at,
           (date - ad_created_date)   AS days_to_cross,
           c_imp, c_sp, c_cv, c_ncp, c_ft
      FROM cum
     WHERE c_imp >= 50000
     ORDER BY account_name, ad_id, date
)
SELECT m.account_name, m.ad_id, COALESCE(m.ad_name, ''), m.ad_created_date,
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
  LEFT JOIN d14 d USING (account_name, ad_id)
  LEFT JOIN k50 k USING (account_name, ad_id)
ON CONFLICT (account_name, ad_id) DO UPDATE SET
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
