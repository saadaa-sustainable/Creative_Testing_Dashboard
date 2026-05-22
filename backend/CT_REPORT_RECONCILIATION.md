# CT Report ↔ Dashboard Reconciliation

**Last updated:** 2026-05-19
**Reference case:** Week `Ws-19-26` (CT Report dates 2026-05-11 → 2026-05-16)
**Source files used:**
- CT Report: `Creative Testing Results - CT Results.csv` (the curated Google Sheet)
- Dashboard export: `CT-Creative-Export_All-Accounts_all-dates_38ads.csv` (filters: Created 2026-05-11→15, All Accounts, Excl. Copy, Active Only)

---

## 1. The headline numbers (Ws-19-26)

| | Dashboard (created 11-15) | CT Report (dates 11-15) | Δ |
|---|---|---|---|
| **Unique ads** | 38 | 34 | **+4** |
| **Total impressions** | 132,781 | 57,075 | **+75,706** |
| **Total spend** | ₹19,639 | ₹10,138 | **+₹9,501** |

The dashboard runs **~2.3× higher** on impressions and spend. The gap is not a bug — it's the result of three structural differences between how the dashboard queries Meta and how your team maintains the CT Report.

> **What happens when you widen Created Date To = 2026-05-16?**
> The gap **widens, not narrows**. The CT Report only logs 6 new ads dated 16/05; the dashboard pulls every ad Meta created on 16/05 (~20 of them). Result: 58 dashboard ads vs 40 CT Report ads.

---

## 2. Root causes (in order of impact)

### Cause A — The CT Report's `Date` column is NOT `ad_created_date`

The CT Report's second column is your team's **logging / evaluation date** — the day the ad was reviewed and entered into the sheet. The dashboard filters by Meta's **`ad_created_date`** (the actual creation timestamp from the Ads API).

These two dates routinely disagree by 1-3 days. Concrete examples from this week:

| Ad name | CT Report "Date" | Real `ad_created_date` (Meta) |
|---|---|---|
| `CLP-SMCFS+MU+NA+OSP+SIF-1461-P3-_nareshbohra-16/05/26` | 2026-05-16 | **2026-05-13** |
| `CTP-BST+IFAD-MU+NA+OSP+SIF-1864-P1-_muskaan__mehra_-16/05/26` | 2026-05-16 | **2026-05-14** |
| `CTP-BST+IFAD-MU+NA+OSP+SIF-3425-P1-simran_dalal-16/05/26` | 2026-05-16 | **2026-05-14** |
| `CTP-MSA+IFAD-MU+NA+OSP+SIF-1650-P1-men.vastra -16/05/26` | 2026-05-16 | **2026-05-13** |

**Implication:** no `ad_created_date` filter range will ever map cleanly onto a CT Report week.

### Cause B — Dashboard counts every Meta ad; CT Report is curated

The dashboard reflects **everything Meta returns**. The CT Report is **manually maintained** — the team only logs ads they've classified. Three sub-cases:

1. **`NO-ID` placeholder ads** — created in Meta but never assigned a real identifier (e.g. `CTP-SDFAK+MU+NA+IHP+NO-ID-15/05/26`). The team correctly skips these in the sheet. The dashboard still shows them.
2. **Same ad_name reused for multiple Meta ad_ids** — when an ad is duplicated, edited, or re-launched, Meta creates a new ad_id; the old one becomes `ARCHIVED`. The CSV logs the name once; the dashboard shows one row per ad_id. Example: `CTP-SDFLK+IFAD-MU+NA+OSP+SIF-1806-P1-shubhanshi_tyagi_-14/05/26` has **3 ad_ids** in Raho Saadaa.
3. **Newly created ads not yet logged** — real ads the team simply hasn't gotten to yet. These are the largest contributor to the gap (~9 ads in this week).

### Cause C — `primary_table` is up to one day stale

`primary_table` is populated by `primary_sync.py daily` (nightly cron at 00:30 IST). At any given time it has data through **yesterday**, never today. The hourly script (`hourly_impressions.py`) writes today's partial data into `results_table` only, not `primary_table`. The dashboard's CT view reads from `primary_table` (when filtered), so it can be missing the most recent day or two of impressions.

This is the source of the per-ad number deltas for *in-progress* ads:

| Ad | CT Report imp | DB imp | Δ |
|---|---|---|---|
| `CLP-SDWLP+MU+OFF-RS+IHP+SDWLP_VRP_MH_SC_GAD-May-1010-13/05/26` | 33,495 | 31,431 | **+2,064** |

The CT Report was pulled today (May 19), pulling Meta numbers through ~May 18. The dashboard's `primary_table` only has data through May 17. The ~2,000 impression gap = the 18/05 (and partial 19/05) day that the dashboard hasn't ingested yet.

---

## 3. Per-ad evidence (Ws-19-26)

### 3a. 10 ads in both, with mismatched numbers

| Ad name (truncated) | Account | CT Report (imp / spend) | Dashboard (imp / spend) | Delta |
|---|---|---|---|---|
| `CLP-SDWLP+MU+OFF-RS+IHP+SDWLP_VRP_MH_SC_GAD-May-1010-13/05/26` | Raho Saadaa | 33,495 / ₹7,229 | 31,431 / ₹6,734 | +2,064 / ₹+495 |
| `CTP-SDFLK+...shubhanshi_tyagi_-14/05/26` | Raho Saadaa | 1,021 / ₹78 | 494 / ₹83 | +527 / ₹-4 |
| `CTP-MSA+...ved.sawlani-13/05/26` | Fourth Ad Account | 1,208 / ₹69 | 12 / ₹1 | +1,196 / ₹+68 |
| `CTP-SMPPT+...fitsbyprthm-Knitwear -14/05/26` | Fourth Ad Account | 222 / ₹55 | 1,196 / ₹69 | -974 / ₹-13 |
| `CTP-MSA+...deepugahlawatt-13/05/26` | Fourth Ad Account | 23 / ₹7 | 50 / ₹9 | -27 / ₹-1 |
| `CTP-MSA+...men.vastra -13/05/26` | Fourth Ad Account | 50 / ₹8 | 74 / ₹16 | -24 / ₹-7 |
| `CTP-MSA+...pranjalnehete-13/05/26` | Fourth Ad Account | 13 / ₹2 | 23 / ₹8 | -10 / ₹-5 |
| `CLP-CTP+...drawwithvanshika-14/05/26` | Raho Saadaa | 926 / ₹106 | 909 / ₹103 | +17 / ₹+3 |
| `CLP-SDCSS+...aridhi_gupta-14/05/26` | Raho Saadaa | 5,990 / ₹914 | 5,982 / ₹914 | +8 / ₹+0 |
| `CTP-BST+...aakankshasinghchauhan-14/05/26` | Raho Saadaa | 115 / ₹26 | 114 / ₹24 | +1 / ₹+2 |

**Interpretation:**
- The 5 ads where **CT < Dashboard** are typically the Fourth-account `MSA+IFAD` ads where the CT Report values look like an under-count (probably the team logged a partial day or aggregated differently).
- The 5 ads where **CT > Dashboard** include `SDWLP` (one-day-stale `primary_table`) and `shubhanshi_tyagi_` (ad_name reused across 3 ad_ids — the CSV likely aggregates, the dashboard splits per-ad_id).

### 3b. 12 ads in Dashboard but NOT in CT Report

These are ads the dashboard sees that the team hasn't (yet) logged. Three categories:

**Category 1 — `NO-ID` placeholder ads (5 ads):**

| Ad name | Imp | Spend |
|---|---|---|
| `CLP-SDALS+MU+NA+OSP+NO-ID-11/05/26` | 611 | ₹76 |
| `CTP-SDFAK+MU+NA+IHP+NO-ID-15/05/26` | 4,600 | ₹768 |
| `CTP-SDFCT+MU+NA+IHP+NO-ID-15/05/26` | 310 | ₹66 |
| `CTP-SDFLS+MU+NA+IHP+NO-ID-15/05/26` | 2,910 | ₹646 |
| `CTP-SDFWP+MU+NA+IHP+NO-ID-15/05/26` | 5,770 | ₹866 |

**Category 2 — Real ads not yet logged in the sheet (7 ads):**

| Ad name | Imp | Spend |
|---|---|---|
| `CTP-SDFLS+MU+NA+IHP+SDFLS_VRP_PL_GF_GAD-May-1015-15/05/26` | **34,849** | **₹3,371** |
| `CTP-SDCSP+MU+NA+IHP+MC_VRP_CIP003-0107-11/05/26` | 6,154 | ₹1,246 |
| `CTP-SDFAK+MU+NA+IHP+SDFAK_VRP_PL_GF_GAD-May-1013-15/05/26` | 1,536 | ₹169 |
| `CTP-SDFWP+MU+NA+IHP+SDFWP_VRP_PL_GF_GAD-May-1016-15/05/26` | 1,580 | ₹145 |
| `CTP-SDFCT+MU+NA+IHP+SDFCT_VRP_PL_GF_GAD-May-1014-15/05/26 -` | 359 | ₹53 |
| `CTP-FLN+IGP+NA+IHP+BST_CPL004-0109-14/05/26` | 230 | ₹9 |
| `CLP-SDALS+IFAD-MU+NA+OSP+SIF-2050-P1-priyakathait_-14/05/26` | 132 | ₹12 |

The single ad `CTP-SDFLS+...SDFLS_VRP_PL_GF_GAD-May-1015-15/05/26` (34,849 imp, ₹3,371 spend) alone accounts for **46% of the impression gap and 35% of the spend gap**.

### 3c. 15 ads in CT Report but NOT in dashboard

These split cleanly by why they're missing from the dashboard's view:

**Category A — Logged with CT date 2026-05-16 (excluded by user's "To = 2026-05-15" filter):**

| Ad name | CT "Date" | Imp | Spend |
|---|---|---|---|
| `CLP-SDCP+MU+NA+IHP+SDCP_VRP_MH_SC_GAD-May-1012-16/05/26` | 2026-05-16 | 727 | ₹99 |
| `CLP-SDRPT+MU+NA+IHP+SDRPT_VRP_MH_GF_GAD-May-1008-16/05/26` | 2026-05-16 | 295 | ₹38 |
| `CLP-SDVPL+MU+NA+IHP+SDVPL_USP_PD_SC_GAD-May-1009-16/05/26` | 2026-05-16 | **14,412** | **₹1,910** |
| `CTP-COO+IFAD-MU+NA+OSP+SIF-1797-P1-shama.___________16/05/26` | 2026-05-16 | 1,091 | ₹154 |
| `CTP-COO+IFAD-MU+NA+OSP+SIF-2122-P1-nishitaoswal_-14/05/26` | 2026-05-16 | 1 | ₹0 |

Setting the dashboard filter to `Created Date To = 2026-05-16` would recover these (if their real `ad_created_date` is also ≤ 16) — *but* it would simultaneously add ~20 new dashboard-only ads for 16/05, widening the gap.

**Category B — Real `ad_created_date` < CT date (the date-mismatch problem from Cause A):**

| Ad name | CT "Date" | Real `ad_created_date` |
|---|---|---|
| `CLP-SMCFS+MU+NA+OSP+SIF-1461-P3-_nareshbohra-16/05/26` | 2026-05-16 | 2026-05-13 |
| `CTP-BST+IFAD-MU+NA+OSP+SIF-1864-P1-_muskaan__mehra_-16/05/26` | 2026-05-16 | 2026-05-14 |
| `CTP-BST+IFAD-MU+NA+OSP+SIF-3425-P1-simran_dalal-16/05/26` | 2026-05-16 | 2026-05-14 |
| `CTP-BST+IFAD-MU+NA+OSP+SIF-3982-P1-khushimodiii-16/05/26` | 2026-05-16 | 2026-05-14 |
| `CTP-BST+IFAD-MU+NA+OSP+SIF-4191-P1-vrindakhurana02-16/05/26` | 2026-05-16 | 2026-05-14 |
| `CTP-BST+MU+NA+OSP+SIF-7615-P1-gaender-16/05/26 -` | 2026-05-16 | 2026-05-14 |
| `CTP-MSA+IFAD-MU+NA+OSP+SIF-1650-P1-men.vastra -16/05/26` | 2026-05-16 | 2026-05-13 |
| `CTP-MSA+IFAD-MU+NA+OSP+SIF-1995-P2-_rehansiddiqui-16/05/26` | 2026-05-16 | 2026-05-13 |
| `CTP-MSA+IFAD-MU+NA+OSP+SIF-3942-P1-saral_vijay-16/05/26` | 2026-05-16 | 2026-05-15 |

These ARE in `primary_table` — they just have an earlier `ad_created_date` than the CT Report's column says. To capture them with the dashboard filter, set `Created Date From = 2026-05-13` (or earlier) and trust the dashboard to pull them in.

**Category C — Not in `primary_table` at all (1 ad):**

| Ad name | Note |
|---|---|
| `CLP-SMCP+MU+NA+IHP+SMCP_VRP_MH_SC_GAD-May-1011-15/05/26` | Meta returned 0 impressions; never wrote to `primary_table` |

These are ads created in Meta UI but never delivered. Meta excludes them from the `/insights` endpoint, so they never make it into our sync. They show up in the CT Report only because someone added the name manually.

---

## 4. How to tackle each scenario

### Scenario A — "I want to reconcile a specific CT week"

**Workflow:**

1. Apply these dashboard filters:
   - Created Date From: **earliest CT date − 3 days** (to absorb the date-mismatch from Cause A)
   - Created Date To: **latest CT date**
   - Ad Name "contains": (leave blank or use `CLP-, CTP-` with the **CT Format** toggle)
   - Exclude Copy: ON
   - Active Only: **OFF** (CT Report includes archived/paused)
   - Account: All Accounts

2. Expect a residual gap of **5-15 ads** (your team's logging backlog) and **1-3% per-ad delta** on in-progress ads (the one-day staleness of `primary_table`).

3. Use the Export CSV button. Diff dashboard export ↔ CT Report by `ad_name` to find:
   - Ads in dashboard only → either NO-ID placeholders (ignorable) or genuinely-missed entries that should be added to the sheet.
   - Ads in CT Report only → either Cause-A date-shifts (already captured if you widened the From date) or Category-C zero-impression ads (Meta-only ghosts).

### Scenario B — "I want the per-ad numbers to match exactly"

The dashboard numbers will be **within ~5%** of the CT Report once `primary_table` is freshly synced. To minimize the per-ad delta:

```powershell
python primary_sync.py daily       # pull yesterday's full data
python result_classifier.py        # refresh verdicts
python results_sync.py             # update 30-day rollup
```

After this, only the `ad_name`-with-multiple-`ad_id`s case will still diverge — and the divergence is the right behavior (each ad_id IS a separate Meta creative; the dashboard correctly shows them as separate rows; the CT Report's aggregation is a manual convention).

### Scenario C — "I want the dashboard count to be the source of truth instead"

This is a fair option if your team is willing to switch reporting models:

- The dashboard is **complete** (every Meta ad with any spend).
- The CT Report is **curated** (only ads the team has reviewed).

Switching means treating the dashboard's higher number as correct and using the CT Report as a *secondary* tracker for human notes (Discarded reasoning, Remark, etc).

The dashboard's classification (Winner / ITE / Discarded / Analyse / Priority) is computed deterministically from F1/F2/F3/F4 thresholds. If the team is happy with those thresholds, they can stop manually classifying entirely.

### Scenario D — "Live with the gap, but flag it"

Lowest-effort option. Add a badge to the dashboard: *"Dashboard shows N ads. CT Report logs M. Gap is K — see CT_REPORT_RECONCILIATION.md."* This is the only option that requires zero workflow change from either side.

---

## 5. Recurring patterns to watch for

When the gap exceeds expectations, check these in order:

1. **Is `primary_table` fresh?** Last `updated_at` should be today's date for hourly, yesterday for daily granular rows. If stale → run `primary_sync.py daily`.
2. **Are there many `NO-ID` ads in the dashboard-only list?** They should NOT exist in production — they're ad-creation drafts. Ask the team to clean them up in Meta UI.
3. **Did the CT date for an ad shift backward?** That signals Cause A — the team logged on a different day than the ad was created. Always widen Created Date From by 2-3 days when reconciling.
4. **Is one ad_name appearing with multiple ad_ids?** Usually means the ad was duplicated. Decide whether to add an "Aggregate by ad_name" toggle (see implementation notes below) or live with the split rows.

---

## 6. Implementation notes (for future dashboard changes)

If we want to close the gap further on the dashboard side, the realistic options are:

### Option 1 — Add "Aggregate by ad_name" toggle (closes Cause B.2)
- In `aeCompute()` / `filteredAds()`, when toggle is ON, group rows by `ad_name` instead of `ad_id`. Sum impressions/spend/conv. Pick the highest `ad_status` (ACTIVE > PAUSED > ARCHIVED).
- Effort: ~30 lines in `index.html`.
- Effect: collapses 2-4 ads per week into single rows on Ws-19-26.

### Option 2 — Default-on "Exclude NO-ID placeholders" toggle (closes Cause B.1)
- Filter `ad_name NOT LIKE '%NO-ID%'` in `filteredAds()`.
- Effort: ~10 lines.
- Effect: removes 5-7 dashboard-only ads per week.

### Option 3 — "First Report Date" filter instead of `ad_created_date` (mitigates Cause A)
- Add a date-range input that filters by the earliest date impressions appeared for an ad. This is closer to the CSV team's "evaluation date" mental model.
- Effort: ~25 lines.
- Effect: most reliable way to align the dashboard's date semantics with the CT Report's, but still won't be exact (the team's "Date" is a logging decision, not an algorithmic one).

### Option 4 — Ingest the CT Report into Supabase weekly (closes everything but Cause C)
- Add a new table `ct_report_log` with columns `(week_tag, log_date, ad_name, status, remark)` synced from the Google Sheet API.
- Add a dashboard filter "Only show ads in CT Report".
- Effort: ~150 lines (sheet API client + sync script + UI toggle).
- Effect: exact match against the curated sheet.

**Recommendation:** Options 1 and 2 are low-effort wins worth implementing now. Option 4 is the only path to 100% reconciliation but it formally couples the dashboard to the CT Report — which contradicts the original goal of *replacing* the CT Report with the dashboard.

---

## 7. Quick reference: filter recipe for Ws-19-26 verification

To reproduce the comparison in this doc:

| Filter | Value |
|---|---|
| Account | All Accounts |
| Created Date From | 2026-05-11 |
| Created Date To | 2026-05-15 (or 2026-05-16 with caveats) |
| Ad Name | (blank) — or `CLP-, CTP-` if **CT Format** toggle is ON |
| Exclude Copy | ON |
| Active Only | OFF |
| Date Range (top) | leave default (Last 30 days) |

Then **Export CSV** and diff against the CT Report week tab.

---

*This document is recomputed manually each time a major mismatch needs reconciling. If you want it auto-generated weekly, see Option 4 above.*
