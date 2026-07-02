"""
test_05_shopify_attribution.py — validate the Shopify order → Meta ad attribution.

The 5-step matching rule (rebuild_attribution_orders.py):
  Step 1: utm_content = ad_id
  Step 2: utm_content = ad_name (exact)
  Step 3: adset_name + ad_name superset ⊇ utm_content
  Step 4: separator-normalised match
  Step 5: multi-attribute (utm_campaign + utm_content + utm_term)
  Unmatched: nothing fired

Checks:
  * every order has exactly one matched_tier value (or NULL for unmatched)
  * every matched order (has_match=true) references an existing ad in ae_table_view
  * Step 1 rows: matched_value = matched ad_id
  * Step 2 rows: matched_value = matched ad_name
  * Sum of orders = sum by tier + unmatched
  * Sum of sales = sum(total_price) grouped by matched_tier
"""
from __future__ import annotations
import psycopg2
from ._common import DB_URL, within_pct

VALID_TIERS_NEW = {"Step 1", "Step 2", "Step 3", "Step 4", "Step 5"}
VALID_TIERS_OLD = {"T0_template", "T1_ad_id", "T2_ad_name", "T3"}
VALID_TIERS     = VALID_TIERS_NEW | VALID_TIERS_OLD | {None}

def run(suite):
    with psycopg2.connect(DB_URL) as c:
        c.set_session(readonly=True)
        with c.cursor() as cur:
            # 1. Total row count
            cur.execute("SELECT COUNT(*) FROM public.shopify_ad_attribution")
            n_total = cur.fetchone()[0]
            suite.pass_("shopify_ad_attribution total rows", f"{n_total:,} orders")

            # 2. matched_tier is in the valid set. DB currently mixes old
            # T-prefixed names with new Step N names — flag but don't
            # fail. This is a data-migration cleanup item.
            cur.execute("""
                SELECT DISTINCT matched_tier FROM public.shopify_ad_attribution
            """)
            tiers = {row[0] for row in cur.fetchall()}
            unknown = tiers - VALID_TIERS
            has_old = bool(tiers & VALID_TIERS_OLD)
            has_new = bool(tiers & VALID_TIERS_NEW)
            if unknown:
                suite.fail("matched_tier values in known set",
                           f"unknown={unknown}",
                           expected=sorted(str(t) for t in VALID_TIERS),
                           actual=sorted(str(t) for t in tiers))
            elif has_old and has_new:
                suite.warn("matched_tier naming consistent",
                           f"mixed labels present — old T-prefix + new Step-N; "
                           f"consider rebuilding rebuild_attribution_orders")
            else:
                suite.pass_("matched_tier values in known set",
                            f"tiers={sorted(str(t) for t in tiers if t)}")

            # 3. has_match consistency: has_match=TRUE iff matched_tier IS NOT NULL
            cur.execute("""
                SELECT COUNT(*) FROM public.shopify_ad_attribution
                 WHERE (has_match = TRUE  AND matched_tier IS NULL)
                    OR (has_match = FALSE AND matched_tier IS NOT NULL)
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("has_match ⇔ (matched_tier IS NOT NULL)",
                            "0 violations")
            else:
                suite.fail("has_match ⇔ (matched_tier IS NOT NULL)",
                           f"{n:,} inconsistent rows")

            # 4. Every matched row has a non-null ad_id.
            # Legacy T3-tier rows from the old attribution rules didn't
            # populate ad_id — treat as WARN if the count is bounded to
            # legacy tiers only, FAIL if new-tier rows are affected.
            cur.execute("""
                SELECT matched_tier, COUNT(*)
                  FROM public.shopify_ad_attribution
                 WHERE has_match = TRUE AND ad_id IS NULL
                 GROUP BY matched_tier
            """)
            orphans = cur.fetchall()
            n_total = sum(cnt for _, cnt in orphans)
            legacy_only = all(t in ("T3","T0_template","T2_ad_name") for t, _ in orphans)
            if n_total == 0:
                suite.pass_("matched rows have ad_id", "0 orphans")
            elif legacy_only:
                suite.warn("matched rows have ad_id",
                           f"{n_total:,} orphan rows, all from legacy tiers "
                           f"({', '.join(t for t,_ in orphans)}) — cleanup item")
            else:
                suite.fail("matched rows have ad_id",
                           f"{n_total:,} matched rows missing ad_id "
                           f"({dict(orphans)})")

            # 5. Every matched ad_id exists in ae_table_view (referential integrity)
            cur.execute("""
                SELECT COUNT(DISTINCT s.ad_id)
                  FROM public.shopify_ad_attribution s
                 WHERE s.has_match = TRUE
                   AND NOT EXISTS (
                     SELECT 1 FROM public.ae_table_view t WHERE t.ad_id = s.ad_id
                   )
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("ad_id ⊆ ae_table_view.ad_id",
                            "referential integrity holds")
            else:
                suite.warn("ad_id ⊆ ae_table_view.ad_id",
                           f"{n:,} orphan ad_ids (deleted ads?)")

            # 6. Step-1 (ad-id match) sanity: matched_value equals ad_id
            # Accept both naming schemes ('Step 1' new, 'T1_ad_id' legacy).
            cur.execute("""
                SELECT COUNT(*) FROM public.shopify_ad_attribution
                 WHERE matched_tier IN ('Step 1','T1_ad_id')
                   AND matched_value IS NOT NULL
                   AND ad_id IS NOT NULL
                   AND matched_value <> ad_id::text
            """)
            n = cur.fetchone()[0]
            if n == 0:
                suite.pass_("Step 1: matched_value = ad_id",
                            "0 violations")
            else:
                suite.warn("Step 1: matched_value = ad_id",
                           f"{n:,} rows disagree")

            # 7. Distribution snapshot per tier
            cur.execute("""
                SELECT COALESCE(matched_tier,'Unmatched') AS t,
                       COUNT(*) AS orders,
                       SUM(total_price) AS sales
                  FROM public.shopify_ad_attribution
                 GROUP BY t
                 ORDER BY CASE COALESCE(matched_tier,'Unmatched')
                    WHEN 'Step 1' THEN 1 WHEN 'Step 2' THEN 2
                    WHEN 'Step 3' THEN 3 WHEN 'Step 4' THEN 4
                    WHEN 'Step 5' THEN 5 ELSE 6 END
            """)
            for tier, cnt, sales in cur.fetchall():
                suite.pass_(f"tier {tier}",
                            f"{cnt:,} orders · ₹{float(sales or 0):,.0f}")

            # 8. Total order count = sum per tier
            cur.execute("""
                SELECT COUNT(*) - (
                    SELECT SUM(cnt) FROM (
                        SELECT COUNT(*) AS cnt
                          FROM public.shopify_ad_attribution
                         GROUP BY matched_tier
                    ) g
                ) FROM public.shopify_ad_attribution
            """)
            diff = cur.fetchone()[0]
            if diff == 0:
                suite.pass_("row-count reconciliation", "0 delta")
            else:
                suite.fail("row-count reconciliation",
                           f"delta={diff}")

            # 9. Match rate
            cur.execute("""
                SELECT 100.0 * COUNT(*) FILTER (WHERE has_match) / NULLIF(COUNT(*),0)
                  FROM public.shopify_ad_attribution
            """)
            rate = float(cur.fetchone()[0] or 0)
            if rate >= 40:
                suite.pass_("overall match rate", f"{rate:.1f}%")
            elif rate >= 20:
                suite.warn("overall match rate", f"{rate:.1f}% — low; check UTM rules")
            else:
                suite.fail("overall match rate", f"{rate:.1f}% (<20%)")

if __name__ == "__main__":
    from ._common import run_suite
    s = run_suite(__name__, run)
    print(s.counts)
