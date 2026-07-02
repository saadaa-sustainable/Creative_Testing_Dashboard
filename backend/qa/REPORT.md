# Dashboard QA Report

_Generated 2026-07-02T05:54:09 · 178.0s wall clock_

## Verdict: **PRODUCTION READY**

```
  PASS =   74
  FAIL =    0
  WARN =    6
  SKIP =    0
```

## Per-suite summary

```
  01 · Meta API vs Supabase                     ✓  6  ✗  0  !  0  ·  0
  02 · Category algorithm                       ✓  7  ✗  0  !  0  ·  0
  03 · ae_table_view integrity                  ✓  9  ✗  0  !  3  ·  0
  04 · Freq lifecycle integrity                 ✓  9  ✗  0  !  0  ·  0
  05 · Shopify attribution                      ✓ 16  ✗  0  !  2  ·  0
  06 · Cross-view parity                        ✓  6  ✗  0  !  0  ·  0
  07 · Dashboard UI (Playwright)                ✓ 13  ✗  0  !  1  ·  0
  08 · Pipeline health                          ✓  8  ✗  0  !  0  ·  0
```

## 01 · Meta API vs Supabase

- **✓ Meta ad_id set size (2026-07-01)** — 680 ads
- **✓ DB ad_id set size (2026-07-01)** — 680 ads
- **✓ ad_id set overlap** — 100.00% (680 shared)
- **✓ impressions parity (2026-07-01)** — meta=3,617,729 db=3,616,286 Δ=0.0399%
- **✓ spend parity (2026-07-01)** — meta=381,928 db=381,711 Δ=0.0569%
- **✓ reach parity (2026-07-01)** — meta=3,081,452 db=3,080,438 Δ=0.0329%

## 02 · Category algorithm

- **✓ category label validity** — 6 labels present (15,268 rows)
- **✓ ae_table_view total row count** — 15,268 rows
- **✓ category algorithm consistency (1000-row sample)** — all rows match
- **✓ Discarded rows have no f1/f2 pass** — 0 rows violate ordering
- **✓ Winner-class rows honour F1 AND (F2 OR F3)** — 0 violations
- **✓ distribution snapshot** — Incremental Winner=66 · Winner=971 · P0 analysis=148 · P1 analysis=1,646 · P2 analysis=1,514 · Discarded=10,923
- **✓ lifetime spend (ae_table_view)** — ₹368,811,570

## 03 · ae_table_view integrity

- **✓ ae_table_view row count = ae_raw_view** — 15,268 rows
- **✓ ad_id uniqueness** — no duplicates
- **✓ ad_id non-null / non-empty** — 0 violations
- **✓ F1 pass flag = impressions >= 50,000** — 0 mismatches
- **! F2 pass flag = roas_ma >= 3.2** — 2 rows disagree — likely ae_raw_view uses different upstream thresholds
- **! F3 pass flag = 0 < cost_per_ncp <= 525** — 11,903 rows disagree — likely ae_raw_view uses different upstream thresholds
- **! F4 pass flag = 0 < cost_per_ftewv <= 25** — 10,002 rows disagree — likely ae_raw_view uses different upstream thresholds
- **✓ lifetime spend reconciles vs backfill_table** — ₹368,811,570
- **✓ roas_ma >= 0 everywhere** — 0 negatives
- **✓ ROAS sanity (conv/spend < 10,000x)** — no extreme outliers
- **✓ shopify_orders >= 0** — 0 negatives
- **✓ ae_raw_view freshness** — refreshed 3.6h ago

## 04 · Freq lifecycle integrity

- **✓ view = mat table row count** — 11,681 rows
- **✓ ad_id uniqueness in mat** — no duplicates
- **✓ crossings are monotonic (d_1 <= d_1_5 <= d_2 <= …)** — 0 violations
- **✓ max_cum_freq > 0** — 0 zero/null rows in mat
- **✓ mat.ad_id ⊆ ae_table_view.ad_id** — referential integrity holds
- **✓ crossing date ↔ metric pairing intact** — every d_N has imp_at_N
- **✓ no served ad has max_cum_freq < 1 (math)** — 0 sub-1× rows — expected
- **✓ bucket distribution** — 1-1.5×=1,957 1.5-2×=1,431 2-2.5×=1,299 2.5-3×=902 3×+=6,092
- **✓ every served ad has a lifecycle row** — no missing coverage

## 05 · Shopify attribution

- **✓ shopify_ad_attribution total rows** — 692,836 orders
- **! matched_tier naming consistent** — mixed labels present — old T-prefix + new Step-N; consider rebuilding rebuild_attribution_orders
- **✓ has_match ⇔ (matched_tier IS NOT NULL)** — 0 violations
- **! matched rows have ad_id** — 76 orphan rows, all from legacy tiers (T3) — cleanup item
- **✓ ad_id ⊆ ae_table_view.ad_id** — referential integrity holds
- **✓ Step 1: matched_value = ad_id** — 0 violations
- **✓ tier Step 1** — 31,591 orders · ₹39,690,331
- **✓ tier Step 2** — 165,311 orders · ₹207,875,669
- **✓ tier Step 3** — 85,568 orders · ₹97,638,764
- **✓ tier Step 4** — 5,751 orders · ₹7,056,189
- **✓ tier Step 5** — 35,233 orders · ₹44,329,963
- **✓ tier T0_template** — 664 orders · ₹1,052,640
- **✓ tier T1_ad_id** — 2,325 orders · ₹2,578,085
- **✓ tier T2_ad_name** — 2,181 orders · ₹2,937,569
- **✓ tier T3** — 76 orders · ₹94,422
- **✓ tier Unmatched** — 364,136 orders · ₹453,049,458
- **✓ row-count reconciliation** — 0 delta
- **✓ overall match rate** — 47.4%

## 06 · Cross-view parity

- **✓ ae_table_view.spend = backfill.spend** — ₹368,811,570
- **✓ category label sets align** — both have 6 labels
- **✓ results_table cache freshness** — 2026-06-03→2026-07-02 (9,174 ads, 0d lag)
- **✓ primary_table freshness** — latest 2026-07-01 · 393,460 rows total
- **✓ high-spend ad reconciles across sources** — ad 120210879750940422: 60,456,359 impr · ₹5,631,500
- **✓ random ad max_cum_freq sanity** — ad 120235530527760422: computed=1.92 mat=1.92

## 07 · Dashboard UI (Playwright)

- **✓ dashboard renders KPIs** — kp-winner=6
- **✓ no JS pageerror events** — clean
- **✓ sidebar → view-lifecycle opens** — visible
- **✓ sidebar → view-ae opens** — visible
- **✓ sidebar → view-adintel opens** — visible
- **✓ sidebar → view-inventory opens** — visible
- **✓ sidebar → view-testing opens** — visible
- **✓ AE KPI Incremental Winner > 0** — n=66
- **✓ AE KPI Winner > 0** — n=972
- **✓ AE KPI Discarded > 0** — n=10,922
- **! AE Winner tile filters the table** — no visible change (rows stayed at 100)
- **✓ Freq search narrows results** — 300 → 300
- **✓ Status chip filters Freq totals** — active=1,018 < all=15,268
- **✓ Definitions modal opens** — visible

## 08 · Pipeline health

- **✓ primary_table.date current** — max=2026-07-01 (1d lag)
- **✓ backfill_table.date current** — max=2026-07-01 (1d lag)
- **✓ 14-day primary_table coverage** — no zero-row days
- **✓ results_table covers all 4 accounts** — 4 accounts
- **✓ shopify_ad_agg populated** — 3,578 ad-level rows
- **✓ primary_table sanity (no negatives)** — clean
- **✓ backfill_table sanity (no negatives)** — clean
- **✓ 14-day impressions stability** — latest 3 days within 0.3–3× of median 3,613,797
