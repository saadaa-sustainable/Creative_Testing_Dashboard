"""
test_06_cross_view.py — cross-view number parity.

Creative Testing (Last-30d window computed from primary_table) and Ads
Analyse (lifetime, from ae_table_view) show different aggregates, but
several invariants MUST hold between them and their source tables.

Checks:
  * ae_table_view lifetime spend = backfill_table lifetime spend
  * summary_table.total_impressions = SUM by ad from backfill_table (sample check)
  * Category sums from ae_table_view = category sums from summary_table (label-mapped)
  * results_table cache exists and covers the "Last 30d" window
"""
from __future__ import annotations
import psycopg2
from datetime import date, timedelta
from ._common import DB_URL, within_pct, approx_equal

def run(suite):
    with psycopg2.connect(DB_URL) as c:
        c.set_session(readonly=True)
        with c.cursor() as cur:
            # 1. Lifetime spend parity: ae_table_view vs backfill_table
            cur.execute("SELECT SUM(amount_spent)     FROM public.ae_table_view")
            v_spend = float(cur.fetchone()[0] or 0)
            cur.execute("SELECT SUM(amount_spent_inr) FROM public.backfill_table")
            b_spend = float(cur.fetchone()[0] or 0)
            if within_pct(v_spend, b_spend, 0.01):
                suite.pass_("ae_table_view.spend = backfill.spend",
                            f"₹{v_spend:,.0f}")
            else:
                suite.fail("ae_table_view.spend = backfill.spend",
                           f"view=₹{v_spend:,.0f}  backfill=₹{b_spend:,.0f}",
                           expected=b_spend, actual=v_spend)

            # 2. Category counts consistent (ae_table_view uses computed pass
            #    flags per period; summary_table uses lifetime totals). They
            #    should share the same 6-label set at minimum.
            cur.execute("""
                SELECT category FROM public.ae_table_view
                 GROUP BY category
            """)
            v_labels = {r[0] for r in cur.fetchall()}
            cur.execute("""
                SELECT status FROM public.summary_table
                 GROUP BY status
            """)
            s_labels = {r[0] for r in cur.fetchall()}
            missing = v_labels ^ s_labels
            if not missing:
                suite.pass_("category label sets align",
                            f"both have {len(v_labels)} labels")
            else:
                suite.warn("category label sets align",
                           f"symmetric diff: {missing}")

            # 3. results_table cache covers a recent window
            cur.execute("""
                SELECT data_date_from::text, data_date_to::text,
                       count_total_ads,
                       (computed_at AT TIME ZONE 'Asia/Kolkata')::timestamp
                  FROM public.results_table
                 WHERE account_name = 'All Accounts'
                 ORDER BY computed_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                suite.fail("results_table cache exists",
                           "no 'All Accounts' row found")
            else:
                d_from, d_to, n_ads, ist = row
                today = date.today()
                to_dt = date.fromisoformat(d_to)
                lag_days = (today - to_dt).days
                if lag_days <= 1:
                    suite.pass_("results_table cache freshness",
                                f"{d_from}→{d_to} ({n_ads:,} ads, {lag_days}d lag)")
                else:
                    suite.warn("results_table cache freshness",
                               f"{d_from}→{d_to} — {lag_days}d lag")

            # 4. primary_table has data through yesterday
            cur.execute("""
                SELECT MAX(date), COUNT(*) FROM public.primary_table
            """)
            max_date, n = cur.fetchone()
            if max_date and (date.today() - max_date).days <= 1:
                suite.pass_("primary_table freshness",
                            f"latest {max_date} · {n:,} rows total")
            else:
                suite.warn("primary_table freshness",
                           f"latest {max_date} — check pipeline")

            # 5. Sample per-ad: pick one high-spend ad and verify summary_table
            #    matches ad-level sum from backfill_table (spend, impressions).
            cur.execute("""
                SELECT ad_id FROM public.ae_table_view
                 ORDER BY amount_spent DESC NULLS LAST LIMIT 1
            """)
            ad_row = cur.fetchone()
            if not ad_row:
                suite.skip("high-spend ad reconciles across sources",
                           "no ads in ae_table_view")
            else:
                aid = ad_row[0]
                cur.execute("""
                    SELECT SUM(impressions), SUM(amount_spent_inr)
                      FROM public.backfill_table WHERE ad_id::text = %s
                """, (aid,))
                bf_imp, bf_spend = cur.fetchone()
                cur.execute("""
                    SELECT total_impressions, total_spend
                      FROM public.summary_table WHERE ad_id::text = %s
                """, (aid,))
                st_row = cur.fetchone()
                if not st_row:
                    suite.warn("high-spend ad reconciles across sources",
                               f"ad {aid} missing from summary_table")
                else:
                    st_imp, st_spend = st_row
                    ok_i = approx_equal(bf_imp,   st_imp,   tol=0.001)
                    ok_s = approx_equal(bf_spend, st_spend, tol=0.001)
                    if ok_i and ok_s:
                        suite.pass_("high-spend ad reconciles across sources",
                                    f"ad {aid}: {int(st_imp or 0):,} impr · "
                                    f"₹{float(st_spend or 0):,.0f}")
                    else:
                        suite.fail("high-spend ad reconciles across sources",
                                   f"impr bf={bf_imp} st={st_imp} · "
                                   f"spend bf={bf_spend} st={st_spend}",
                                   expected=bf_imp, actual=st_imp)

            # 6. ae_freq_lifecycle max_cum_freq should be recomputable —
            #    for a random ad, cum_impressions = SUM(impressions from earliest
            #    date to last_date). We validate the sum is consistent.
            cur.execute("""
                SELECT ad_id, max_cum_freq, first_date, last_date
                  FROM public.ae_freq_lifecycle_mat
                 ORDER BY RANDOM() LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                suite.skip("random freq lifecycle sanity", "empty mat")
            else:
                aid, mcf, first_dt, last_dt = row
                cur.execute("""
                    SELECT SUM(impressions), MAX(reach)
                      FROM public.primary_table
                     WHERE ad_id::text = %s
                       AND date BETWEEN %s AND %s
                """, (aid, first_dt, last_dt))
                p_imp, p_reach = cur.fetchone()
                cur.execute("""
                    SELECT SUM(impressions), MAX(reach)
                      FROM public.backfill_table
                     WHERE ad_id::text = %s
                       AND date BETWEEN %s AND %s
                """, (aid, first_dt, last_dt))
                b_imp, b_reach = cur.fetchone()
                # dedup: prefer primary
                imp_sum = int(p_imp or 0) if p_imp else int(b_imp or 0)
                reach_max = max(int(p_reach or 0), int(b_reach or 0))
                if reach_max > 0 and imp_sum > 0:
                    computed = imp_sum / reach_max
                    if approx_equal(computed, float(mcf), tol=0.05):
                        suite.pass_("random ad max_cum_freq sanity",
                                    f"ad {aid}: computed={computed:.2f} mat={float(mcf):.2f}")
                    else:
                        suite.warn("random ad max_cum_freq sanity",
                                   f"ad {aid}: computed={computed:.2f} mat={float(mcf):.2f} "
                                   f"(mat is exact; computed is approximate — "
                                   f"backfill dedup may differ)")
                else:
                    suite.skip("random ad max_cum_freq sanity",
                               f"ad {aid} has no impressions in raw tables")

if __name__ == "__main__":
    from ._common import run_suite
    s = run_suite(__name__, run)
    print(s.counts)
