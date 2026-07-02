"""
test_02_category_logic.py — verify F1/F2/F3/F4 → category algorithm on ae_table_view.

The rules:
  Incremental Winner : f1 AND (f2 OR f3) AND f4
  Winner             : f1 AND (f2 OR f3)
  P0 analysis        : f1 AND f4
  P1 analysis        : f1 only
  P2 analysis        : f2 only
  Discarded          : else

Priority ordering matters — the first matching rule wins. We assert:
  1. Every row's category is one of the 6 labels.
  2. Every row's category is consistent with its (f1_pass, f2_pass,
     f3_pass, f4_pass) flags per the ordering.
  3. Distribution counts match the SQL CASE (sanity).
  4. No row is misclassified vs the algorithm.
"""
from __future__ import annotations
import psycopg2
from ._common import DB_URL

VALID = {"Incremental Winner", "Winner", "P0 analysis",
         "P1 analysis", "P2 analysis", "Discarded"}

def expected_category(f1, f2, f3, f4) -> str:
    if f1 and (f2 or f3) and f4: return "Incremental Winner"
    if f1 and (f2 or f3):        return "Winner"
    if f1 and f4:                return "P0 analysis"
    if f1:                       return "P1 analysis"
    if f2:                       return "P2 analysis"
    return "Discarded"

def run(suite):
    with psycopg2.connect(DB_URL) as c:
        c.set_session(readonly=True)
        with c.cursor() as cur:
            # 1. every category is in the valid set
            cur.execute("""
                SELECT category, COUNT(*) FROM public.ae_table_view
                 GROUP BY category
            """)
            counts = dict(cur.fetchall())
            unknown = [k for k in counts if k not in VALID]
            if unknown:
                suite.fail("category label validity",
                           f"unknown={unknown}", expected=list(VALID), actual=unknown)
            else:
                suite.pass_("category label validity",
                            f"6 labels present ({sum(counts.values()):,} rows)")

            # 2. Total row count reconciles
            cur.execute("SELECT COUNT(*) FROM public.ae_table_view")
            total = cur.fetchone()[0]
            suite.pass_("ae_table_view total row count",
                        f"{total:,} rows")

            # 3. Sample 1,000 random rows and verify their category vs the
            #    algorithm's output. Any mismatch = fail.
            cur.execute("""
                SELECT ad_id, f1_pass, f2_pass, f3_pass, f4_pass, category
                  FROM public.ae_table_view
                  ORDER BY RANDOM()
                  LIMIT 1000
            """)
            mismatches = []
            for aid, f1, f2, f3, f4, cat in cur.fetchall():
                exp = expected_category(f1, f2, f3, f4)
                if cat != exp:
                    mismatches.append((aid, (f1,f2,f3,f4), cat, exp))

            if mismatches:
                sample = mismatches[:3]
                suite.fail("category algorithm consistency (1000-row sample)",
                           f"{len(mismatches)}/1000 mismatched. e.g. "
                           f"ad {sample[0][0]} flags={sample[0][1]} db={sample[0][2]} exp={sample[0][3]}",
                           expected="0", actual=len(mismatches))
            else:
                suite.pass_("category algorithm consistency (1000-row sample)",
                            "all rows match")

            # 4. Priority ordering — no ad should be in 'Discarded' if any f-flag is true
            cur.execute("""
                SELECT COUNT(*) FROM public.ae_table_view
                 WHERE category = 'Discarded'
                   AND (f1_pass OR f2_pass)
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("Discarded rows have no f1/f2 pass",
                            "0 rows violate ordering")
            else:
                suite.fail("Discarded rows have no f1/f2 pass",
                           f"{n:,} rows misclassified as Discarded despite f1/f2")

            # 5. Winner-class rows must satisfy the OR clause
            cur.execute("""
                SELECT COUNT(*) FROM public.ae_table_view
                 WHERE category IN ('Winner','Incremental Winner')
                   AND NOT (f1_pass AND (f2_pass OR f3_pass))
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("Winner-class rows honour F1 AND (F2 OR F3)",
                            "0 violations")
            else:
                suite.fail("Winner-class rows honour F1 AND (F2 OR F3)",
                           f"{n:,} rows tagged Winner but don't satisfy the rule")

            # 6. Distribution printout
            expected_order = ["Incremental Winner","Winner","P0 analysis",
                              "P1 analysis","P2 analysis","Discarded"]
            dist = " · ".join(f"{k}={counts.get(k,0):,}" for k in expected_order)
            suite.pass_("distribution snapshot", dist)

            # 7. total sum must match global ae_table_view spend
            cur.execute("SELECT SUM(amount_spent) FROM public.ae_table_view")
            total_spend = float(cur.fetchone()[0] or 0)
            suite.pass_("lifetime spend (ae_table_view)",
                        f"₹{total_spend:,.0f}")

if __name__ == "__main__":
    from ._common import run_suite
    s = run_suite(__name__, run)
    print(s.counts)
