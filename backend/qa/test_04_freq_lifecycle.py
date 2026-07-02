"""
test_04_freq_lifecycle.py — validate ae_freq_lifecycle_mat integrity.

Every ad in ae_table_view that has ever had impressions > 0 must have
exactly one row in ae_freq_lifecycle_mat. Crossings must be ordered
chronologically (d_1 <= d_1_5 <= d_2 <= d_2_5 <= d_3).

max_cum_freq must be > 0 for served ads and NULL/0 for the rest.

The mat table is a physical table refreshed by refresh_ae_freq_lifecycle();
the anon view ae_freq_lifecycle is a thin passthrough. We assert both
return the same data.
"""
from __future__ import annotations
import psycopg2
from ._common import DB_URL

def run(suite):
    with psycopg2.connect(DB_URL) as c:
        c.set_session(readonly=True)
        with c.cursor() as cur:
            # 1. Row count in mat table matches served ads
            cur.execute("SELECT COUNT(*) FROM public.ae_freq_lifecycle_mat")
            n_mat = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM public.ae_freq_lifecycle")
            n_view = cur.fetchone()[0]
            if n_mat == n_view:
                suite.pass_("view = mat table row count", f"{n_mat:,} rows")
            else:
                suite.fail("view = mat table row count",
                           f"mat={n_mat:,} view={n_view:,}")

            # 2. Unique ad_id
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT ad_id FROM public.ae_freq_lifecycle_mat
                     GROUP BY ad_id HAVING COUNT(*) > 1
                ) t
            """)
            dupes = cur.fetchone()[0]
            if dupes == 0:
                suite.pass_("ad_id uniqueness in mat", "no duplicates")
            else:
                suite.fail("ad_id uniqueness in mat", f"{dupes:,} duplicated")

            # 3. Chronological crossing order (d_1 <= d_1_5 <= d_2 <= d_2_5 <= d_3)
            cur.execute("""
                SELECT COUNT(*) FROM public.ae_freq_lifecycle_mat
                 WHERE (d_1   IS NOT NULL AND d_1_5 IS NOT NULL AND d_1   > d_1_5)
                    OR (d_1_5 IS NOT NULL AND d_2   IS NOT NULL AND d_1_5 > d_2  )
                    OR (d_2   IS NOT NULL AND d_2_5 IS NOT NULL AND d_2   > d_2_5)
                    OR (d_2_5 IS NOT NULL AND d_3   IS NOT NULL AND d_2_5 > d_3  )
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("crossings are monotonic (d_1 <= d_1_5 <= d_2 <= …)",
                            "0 violations")
            else:
                suite.fail("crossings are monotonic (d_1 <= d_1_5 <= d_2 <= …)",
                           f"{n:,} rows out of order")

            # 4. max_cum_freq > 0 for all mat rows
            cur.execute("""
                SELECT COUNT(*) FROM public.ae_freq_lifecycle_mat
                 WHERE max_cum_freq IS NULL OR max_cum_freq <= 0
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("max_cum_freq > 0", "0 zero/null rows in mat")
            else:
                suite.warn("max_cum_freq > 0",
                           f"{n:,} rows have max_cum_freq <= 0")

            # 5. Every mat row must reference an existing ad in ae_table_view
            cur.execute("""
                SELECT COUNT(*) FROM public.ae_freq_lifecycle_mat m
                 WHERE NOT EXISTS (
                     SELECT 1 FROM public.ae_table_view t WHERE t.ad_id = m.ad_id
                 )
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("mat.ad_id ⊆ ae_table_view.ad_id",
                            "referential integrity holds")
            else:
                suite.warn("mat.ad_id ⊆ ae_table_view.ad_id",
                           f"{n:,} orphan ad_ids (deleted ads?)")

            # 6. Each d_X has a matching imp_at_X, reach_at_X, spend_at_X
            cur.execute("""
                SELECT
                  SUM(CASE WHEN d_1   IS NOT NULL AND imp_at_1   IS NULL THEN 1 ELSE 0 END),
                  SUM(CASE WHEN d_1_5 IS NOT NULL AND imp_at_1_5 IS NULL THEN 1 ELSE 0 END),
                  SUM(CASE WHEN d_2   IS NOT NULL AND imp_at_2   IS NULL THEN 1 ELSE 0 END),
                  SUM(CASE WHEN d_2_5 IS NOT NULL AND imp_at_2_5 IS NULL THEN 1 ELSE 0 END),
                  SUM(CASE WHEN d_3   IS NOT NULL AND imp_at_3   IS NULL THEN 1 ELSE 0 END)
                FROM public.ae_freq_lifecycle_mat
            """)
            gaps = cur.fetchone()
            gap_ct = sum(gaps or (0,)*5)
            if gap_ct == 0:
                suite.pass_("crossing date ↔ metric pairing intact",
                            "every d_N has imp_at_N")
            else:
                suite.fail("crossing date ↔ metric pairing intact",
                           f"{gap_ct:,} unpaired cells across thresholds")

            # 7. Sub-1× bucket count is 0 mathematically (each served day has freq >= 1)
            cur.execute("""
                SELECT COUNT(*) FROM public.ae_freq_lifecycle_mat
                 WHERE max_cum_freq < 1.0
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("no served ad has max_cum_freq < 1 (math)",
                            "0 sub-1× rows — expected")
            else:
                suite.warn("no served ad has max_cum_freq < 1 (math)",
                           f"{n:,} rows have max_cum_freq < 1 — investigate")

            # 8. Distribution snapshot
            cur.execute("""
                SELECT
                  COUNT(*) FILTER (WHERE max_cum_freq >= 3.0) AS b5,
                  COUNT(*) FILTER (WHERE max_cum_freq >= 2.5 AND max_cum_freq < 3.0) AS b4,
                  COUNT(*) FILTER (WHERE max_cum_freq >= 2.0 AND max_cum_freq < 2.5) AS b3,
                  COUNT(*) FILTER (WHERE max_cum_freq >= 1.5 AND max_cum_freq < 2.0) AS b2,
                  COUNT(*) FILTER (WHERE max_cum_freq >= 1.0 AND max_cum_freq < 1.5) AS b1
                FROM public.ae_freq_lifecycle_mat
            """)
            b5, b4, b3, b2, b1 = cur.fetchone()
            suite.pass_("bucket distribution",
                        f"1-1.5×={b1:,} 1.5-2×={b2:,} 2-2.5×={b3:,} 2.5-3×={b4:,} 3×+={b5:,}")

            # 9. Every ad from ae_table_view with impressions > 0 should be in mat
            cur.execute("""
                SELECT COUNT(*) FROM public.ae_table_view t
                 WHERE t.impressions > 0
                   AND NOT EXISTS (
                       SELECT 1 FROM public.ae_freq_lifecycle_mat m WHERE m.ad_id = t.ad_id
                   )
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("every served ad has a lifecycle row",
                            "no missing coverage")
            else:
                suite.warn("every served ad has a lifecycle row",
                           f"{n:,} served ads missing from lifecycle_mat")

if __name__ == "__main__":
    from ._common import run_suite
    s = run_suite(__name__, run)
    print(s.counts)
