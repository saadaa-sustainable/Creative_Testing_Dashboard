"""
test_08_pipeline_health.py — data pipeline recency + basic monotonicity.

Confirms that:
  * primary_table and backfill_table both have data through yesterday
  * synced_at / updated_at are recent
  * daily row counts don't collapse to zero mid-history (indicates a gap)
  * results_table has one row per account + 'All Accounts' rollup
"""
from __future__ import annotations
import psycopg2
from datetime import date, timedelta
from ._common import DB_URL

EXPECTED_ACCOUNTS = {"Raho Saadaa", "Fourth Ad Account - SD",
                     "Third Ad Account - SD", "All Accounts"}

def run(suite):
    with psycopg2.connect(DB_URL) as c:
        c.set_session(readonly=True)
        with c.cursor() as cur:
            # 1. primary_table max date
            cur.execute("SELECT MAX(date) FROM public.primary_table")
            max_dt = cur.fetchone()[0]
            today = date.today()
            lag = (today - max_dt).days if max_dt else 999
            if lag <= 1:
                suite.pass_("primary_table.date current",
                            f"max={max_dt} ({lag}d lag)")
            elif lag <= 2:
                suite.warn("primary_table.date current",
                           f"max={max_dt} ({lag}d lag)")
            else:
                suite.fail("primary_table.date current",
                           f"max={max_dt} ({lag}d lag)")

            # 2. backfill_table max date
            cur.execute("SELECT MAX(date) FROM public.backfill_table")
            max_dt_b = cur.fetchone()[0]
            lag = (today - max_dt_b).days if max_dt_b else 999
            if lag <= 1:
                suite.pass_("backfill_table.date current",
                            f"max={max_dt_b} ({lag}d lag)")
            else:
                suite.warn("backfill_table.date current",
                           f"max={max_dt_b} ({lag}d lag)")

            # 3. No zero-row day in the last 14 days
            cur.execute("""
                WITH days AS (
                    SELECT generate_series(CURRENT_DATE - INTERVAL '14 days',
                                           CURRENT_DATE - INTERVAL '1 day',
                                           '1 day')::date AS d
                ),
                cnt AS (
                    SELECT date, COUNT(*) AS n
                      FROM public.primary_table
                     WHERE date >= CURRENT_DATE - INTERVAL '14 days'
                     GROUP BY date
                )
                SELECT d, COALESCE(n,0) FROM days
                  LEFT JOIN cnt ON cnt.date = d
                 ORDER BY d
            """)
            gaps = [(d, n) for d, n in cur.fetchall() if n == 0]
            if not gaps:
                suite.pass_("14-day primary_table coverage", "no zero-row days")
            else:
                suite.warn("14-day primary_table coverage",
                           f"{len(gaps)} gap days: {gaps[:3]}")

            # 4. results_table has one row per expected account
            cur.execute("""
                SELECT DISTINCT account_name FROM public.results_table
            """)
            accts = {row[0] for row in cur.fetchall()}
            missing = EXPECTED_ACCOUNTS - accts
            if not missing:
                suite.pass_("results_table covers all 4 accounts",
                            f"{len(accts)} accounts")
            else:
                suite.warn("results_table covers all 4 accounts",
                           f"missing: {missing}")

            # 5. shopify_ad_agg populated
            cur.execute("SELECT COUNT(*) FROM public.shopify_ad_agg")
            n = cur.fetchone()[0]
            if n > 0:
                suite.pass_("shopify_ad_agg populated", f"{n:,} ad-level rows")
            else:
                suite.fail("shopify_ad_agg populated", "0 rows")

            # 6. Impressions monotonically non-negative across all daily tables
            for tbl in ("primary_table", "backfill_table"):
                cur.execute(f"""
                    SELECT COUNT(*) FROM public.{tbl}
                     WHERE impressions < 0 OR reach < 0 OR amount_spent_inr < 0
                """)
                n = cur.fetchone()[0]
                if n == 0:
                    suite.pass_(f"{tbl} sanity (no negatives)", "clean")
                else:
                    suite.fail(f"{tbl} sanity (no negatives)",
                               f"{n:,} rows with negative values")

            # 7. Impressions per day monotonic-ish — grand total should be within
            #    3× of the 7-day median (a 3×+ spike is possible during launches
            #    but worth flagging).
            # PERCENTILE_CONT can't be used as a window function — compute
            # median separately, then compare recent days against it.
            cur.execute("""
                WITH d AS (
                    SELECT date, SUM(impressions)::bigint AS imp
                      FROM public.primary_table
                     WHERE date >= CURRENT_DATE - INTERVAL '14 days'
                     GROUP BY date
                )
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY imp) FROM d
            """)
            med = float(cur.fetchone()[0] or 1)
            cur.execute("""
                SELECT date, SUM(impressions)::bigint
                  FROM public.primary_table
                 WHERE date >= CURRENT_DATE - INTERVAL '3 days'
                 GROUP BY date ORDER BY date DESC
            """)
            outliers = 0
            for dt, imp in cur.fetchall():
                imp = int(imp or 0)
                ratio = imp / max(med, 1)
                if not (0.3 <= ratio <= 3.0):
                    outliers += 1
                    suite.warn(f"impressions on {dt} vs 14d median",
                               f"{imp:,} vs median {int(med):,} (×{ratio:.2f})")
            if outliers == 0:
                suite.pass_("14-day impressions stability",
                            f"latest 3 days within 0.3–3× of median {int(med):,}")

if __name__ == "__main__":
    from ._common import run_suite
    s = run_suite(__name__, run)
    print(s.counts)
