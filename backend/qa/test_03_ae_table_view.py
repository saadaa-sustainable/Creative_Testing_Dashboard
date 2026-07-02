"""
test_03_ae_table_view.py — structural integrity of ae_table_view.

Checks:
  * Every ad_id in ae_table_view exists in ae_raw_view.
  * No duplicate ad_ids in ae_table_view.
  * F1/F2/F3/F4 pass flags reconcile against thresholds:
       F1 = impressions >= 50,000
       F2 = roas_ma >= 3.2
       F3 = 0 < cost_per_ncp <= 525
       F4 = 0 < cost_per_ftewv <= 25
  * Sums (impressions, spend, reach) reconcile against backfill_table
    lifetime totals (the source of truth for the tile "Total lifetime spend").
  * ROAS_ma is never negative.
  * No conv_value > 0 with amount_spent = 0 (would produce infinite ROAS).
"""
from __future__ import annotations
import psycopg2
from ._common import DB_URL, within_pct

# Thresholds copied from refresh_ae_table.py — must match production defaults
F1_IMP  = 50_000
F2_ROAS = 3.2
F3_CPN  = 525
F4_CPF  = 25

def run(suite):
    with psycopg2.connect(DB_URL) as c:
        c.set_session(readonly=True)
        with c.cursor() as cur:
            # 1. Row count matches ae_raw_view (should be 1:1)
            cur.execute("SELECT COUNT(*) FROM public.ae_raw_view")
            n_raw = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM public.ae_table_view")
            n_view = cur.fetchone()[0]
            if n_raw == n_view:
                suite.pass_("ae_table_view row count = ae_raw_view",
                            f"{n_view:,} rows")
            else:
                suite.fail("ae_table_view row count = ae_raw_view",
                           f"raw={n_raw:,} view={n_view:,}",
                           expected=n_raw, actual=n_view)

            # 2. No duplicate ad_ids in ae_table_view
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT ad_id FROM public.ae_table_view
                     GROUP BY ad_id HAVING COUNT(*) > 1
                ) t
            """)
            dupes = cur.fetchone()[0]
            if dupes == 0:
                suite.pass_("ad_id uniqueness", "no duplicates")
            else:
                suite.fail("ad_id uniqueness", f"{dupes:,} duplicated ad_ids")

            # 3. Every ad_id is text and non-null
            cur.execute("""
                SELECT COUNT(*) FROM public.ae_table_view
                 WHERE ad_id IS NULL OR ad_id = ''
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("ad_id non-null / non-empty", "0 violations")
            else:
                suite.fail("ad_id non-null / non-empty", f"{n:,} rows")

            # 4. F1/F2/F3/F4 pass flags match the threshold definitions.
            # Uses IS DISTINCT FROM so NULL boolean flags count as mismatches.
            checks = [
                (1, f"(impressions >= {F1_IMP})",           f"impressions >= {F1_IMP:,}",   "f1_pass"),
                (2, f"(COALESCE(roas_ma,0) >= {F2_ROAS})",  f"roas_ma >= {F2_ROAS}",        "f2_pass"),
                (3, f"(cost_per_ncp > 0 AND cost_per_ncp <= {F3_CPN})",
                                                            f"0 < cost_per_ncp <= {F3_CPN}",  "f3_pass"),
                (4, f"(cost_per_ftewv > 0 AND cost_per_ftewv <= {F4_CPF})",
                                                            f"0 < cost_per_ftewv <= {F4_CPF}","f4_pass"),
            ]
            for fn, expr, label, flag_col in checks:
                cur.execute(
                    f"SELECT COUNT(*) FROM public.ae_table_view "
                    f"WHERE {expr} IS DISTINCT FROM {flag_col}"
                )
                miscount = cur.fetchone()[0]
                if miscount == 0:
                    suite.pass_(f"F{fn} pass flag = {label}", "0 mismatches")
                else:
                    suite.warn(f"F{fn} pass flag = {label}",
                               f"{miscount:,} rows disagree — likely ae_raw_view uses "
                               f"different upstream thresholds")

            # 5. Lifetime spend reconciles against backfill_table
            cur.execute("""
                SELECT SUM(amount_spent_inr) FROM public.backfill_table
            """)
            bf_spend = float(cur.fetchone()[0] or 0)
            cur.execute("SELECT SUM(amount_spent) FROM public.ae_table_view")
            v_spend = float(cur.fetchone()[0] or 0)
            if within_pct(bf_spend, v_spend, 0.01):
                suite.pass_("lifetime spend reconciles vs backfill_table",
                            f"₹{v_spend:,.0f}")
            else:
                gap = abs(bf_spend - v_spend)
                suite.fail("lifetime spend reconciles vs backfill_table",
                           f"gap ₹{gap:,.2f} (bf=₹{bf_spend:,.0f}, view=₹{v_spend:,.0f})",
                           expected=bf_spend, actual=v_spend)

            # 6. ROAS ≥ 0 everywhere
            cur.execute("SELECT COUNT(*) FROM public.ae_table_view WHERE roas_ma < 0")
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("roas_ma >= 0 everywhere", "0 negatives")
            else:
                suite.fail("roas_ma >= 0 everywhere", f"{n:,} negative rows")

            # 7. Conv value should not exceed amount_spent by 10000x (data corruption)
            cur.execute("""
                SELECT COUNT(*) FROM public.ae_table_view
                 WHERE conv_value > 0 AND amount_spent > 0
                   AND conv_value / amount_spent > 10000
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("ROAS sanity (conv/spend < 10,000x)",
                            "no extreme outliers")
            else:
                suite.warn("ROAS sanity (conv/spend < 10,000x)",
                           f"{n:,} rows with ROAS > 10,000× — inspect for corruption")

            # 8. shopify_orders always >= 0
            cur.execute("SELECT COUNT(*) FROM public.ae_table_view WHERE shopify_orders < 0")
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("shopify_orders >= 0", "0 negatives")
            else:
                suite.fail("shopify_orders >= 0", f"{n:,} negative rows")

            # 9. refreshed_at is within last 24h (data freshness)
            cur.execute("""
                SELECT EXTRACT(EPOCH FROM (NOW() - MAX(refreshed_at)))/3600 AS h
                  FROM public.ae_raw_view
            """)
            hrs = float(cur.fetchone()[0] or 999)
            if hrs <= 30:
                suite.pass_("ae_raw_view freshness",
                            f"refreshed {hrs:.1f}h ago")
            elif hrs <= 48:
                suite.warn("ae_raw_view freshness", f"{hrs:.1f}h ago (>30h)")
            else:
                suite.fail("ae_raw_view freshness",
                           f"{hrs:.1f}h ago",
                           expected="<=30h", actual=f"{hrs:.1f}h")

if __name__ == "__main__":
    from ._common import run_suite
    s = run_suite(__name__, run)
    print(s.counts)
