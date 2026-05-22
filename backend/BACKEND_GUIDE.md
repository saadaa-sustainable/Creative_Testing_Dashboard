# Creative Testing Dashboard — Backend Guide

This document explains how data moves between Meta Ads, the Python scripts, the Supabase tables, and the dashboard UI. It is written as a workflow reference — every section either describes a flow or shows a diagram of it.

---

## 1. End-to-end system map

```
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│   ┌────────────┐                                                         │
│   │  META ADS  │  ← source of truth                                      │
│   │    API     │                                                         │
│   └─────┬──────┘                                                         │
│         │                                                                │
│         │  HTTPS requests (Graph API v21.0)                              │
│         │                                                                │
│   ┌─────▼──────────────────────────────────────────────┐                 │
│   │  PYTHON SCRIPTS  (running on the local machine)    │                 │
│   ├────────────────────────────────────────────────────┤                 │
│   │  primary_sync.py        — nightly full daily fetch │                 │
│   │  hourly_impressions.py  — hourly today-only fetch  │                 │
│   │  result_classifier.py   — 14-day buffer evaluator  │                 │
│   │  results_sync.py        — 30-day rollup writer     │                 │
│   │  cron.py                — scheduler (always-on)    │                 │
│   └─────┬──────────────────────────────────────────────┘                 │
│         │                                                                │
│         │  psycopg2 INSERT / UPSERT / SELECT                             │
│         │                                                                │
│   ┌─────▼──────────────────────────────────────────────┐                 │
│   │  SUPABASE  (managed PostgreSQL)                    │                 │
│   ├────────────────────────────────────────────────────┤                 │
│   │  primary_table   — raw daily rows (one per ad-day) │                 │
│   │  results_table   — 30-day rollups (per account)    │                 │
│   │  ad_results      — 14-day buffer verdicts (per ad) │                 │
│   └─────┬──────────────────────────────────────────────┘                 │
│         │                                                                │
│         │  HTTPS GET via PostgREST                                       │
│         │  (apikey = supabaseAnon URL param)                             │
│         │                                                                │
│   ┌─────▼─────────────────────────────────────────────┐                  │
│   │  DASHBOARD  (static index.html on Vercel)         │                  │
│   ├───────────────────────────────────────────────────┤                  │
│   │  Creative Testing tab    → results_table         │                  │
│   │  Analysis Engine tab     → results_table         │                  │
│   │  Lifecycle (Fatigue) tab → results_table         │                  │
│   └───────────────────────────────────────────────────┘                  │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 2. The three Supabase tables

### `primary_table`
Granular daily rows. One physical row per (account_name, ad_id, date).

```
Columns (key ones):
  account_name        TEXT       — 'Raho Saadaa' / 'Fourth Ad Account - SD' / 'Third Ad Account - SD'
  ad_id               TEXT       — Meta's ad ID
  ad_name             TEXT
  campaign_name       TEXT
  ad_status           TEXT       — ACTIVE / PAUSED / CAMPAIGN_PAUSED / WITH_ISSUES / ...
  ad_created_date     DATE
  date                DATE       — the delivery day this row represents
  impressions         BIGINT
  reach               BIGINT     — daily unique reach
  amount_spent_inr    NUMERIC
  conversion_value    NUMERIC
  outbound_clicks     INT
  thruplays           INT
  three_sec_video_plays INT
  post_engagements    INT
  ftewv_count         INT        — first-time engaged-video-view conversions
  ncp_count           INT        — new customer purchases
  ltv_reach           BIGINT     — lifetime unique reach for this ad (per-row, max = true value)
  ltv_frequency       NUMERIC    — lifetime frequency for this ad
  preview_link        TEXT
  ad_link             TEXT
```

Volume: ~100,000 rows total at present. Grows ~3,000 rows per day.

### `results_table`
Pre-aggregated rolling 30-day snapshots. One physical row per `account_name` (4 total: 3 accounts + "All Accounts" combined).

```
Columns (key ones):
  account_name        TEXT       — same 4 values as above
  computed_at         TIMESTAMP  — when this row was generated
  data_date_from      DATE       — start of the 30-day window
  data_date_to        DATE       — end of the 30-day window
  total_raw_rows      INT
  ads_json            JSONB      — array of per-ad objects (see below)
  total_spend         NUMERIC
  total_impr          BIGINT
  total_reach         BIGINT
  total_conv_value    NUMERIC
  count_total_ads     INT
  ct_roas, hook_rate, hold_rate, ...  — derived rates
```

Shape of `ads_json[]`:
```json
{
  "adId": "120246453983040422",
  "adName": "CTP-BCO+IGP+NA+OSP+NO-ID-5/05/26",
  "acct": "Raho Saadaa",
  "campName": "NCP+NCA+...",
  "adStatus": "ACTIVE",
  "adCreated": "2026-05-04",
  "ctype": "VID",
  "product": "BCO",
  "impr": 41662,
  "spend": 7720.94,
  "reach": 38378,
  "ltvReach": 38378,
  "ltvFreq": 1.0856,
  "convV": 4295,
  ...
}
```

Volume: always exactly 4 rows (purged after each write).

### `ad_results`
14-day-buffer verdict per ad. One row per `(account_name, ad_id)`.

```
Columns:
  account_name        TEXT
  ad_id               TEXT
  ad_name             TEXT
  campaign_name       TEXT
  ad_status           TEXT
  ad_created_date     DATE
  evaluation_end_date DATE       — ad_created_date + 14 days
  impressions_14d     INT        — sum of impressions during the 14-day window
  crossed_threshold_at DATE      — first day cumulative impr >= 50000 (NULL if never)
  result_status       TEXT       — 'Winner' / 'Result Pending' / 'Failed'
  last_computed_at    TIMESTAMP
```

Volume: ~3,650 rows (one per ad ever seen). Grows slowly.

---

## 3. The scripts at a glance

| Script | When it runs | Reads from | Writes to | Duration |
|---|---|---|---|---|
| `primary_sync.py` | Nightly 00:30 IST | Meta API (last 15 days) | `primary_table` | 10–30 min |
| `hourly_impressions.py` | Every hour at :30 (skips 00:30) | Meta API (today) + `primary_table` | `results_table` | 30 sec – 2 min |
| `result_classifier.py` | Nightly after `primary_sync` | `primary_table` | `ad_results` | 1–3 min |
| `results_sync.py` | Nightly after `result_classifier` | `primary_table` (last 30 days) | `results_table` | 30 sec |
| `cron.py` | Continuously | clock | logs (triggers other scripts via subprocess) | — |

---

## 4. Nightly cycle (one-day refresh)

```
00:30:00 IST  cron.py detects "should_run_primary" is true

                  │
                  ▼
        ┌─────────────────────────────────┐
        │   subprocess.run(primary_sync.py daily) │
        └─────────────────────────────────┘
                  │
                  ▼
      ┌───────────────────────────────────────────────────┐
      │  primary_sync.py — for each of 3 accounts:        │
      │                                                   │
      │    1. GET /act_<id>?fields=…  → custom conv IDs   │
      │    2. GET /act_<id>/insights  → daily rows for    │
      │       last 15 days, level=ad, time_increment=1    │
      │    3. GET /<ad_id>            → ad metadata       │
      │       (status, created_time, campaign)            │
      │    4. GET /act_<id>/insights date_preset=maximum  │
      │       → ltv_reach + ltv_frequency per ad          │
      │    5. UPSERT into primary_table                   │
      │       ON CONFLICT (account_name, ad_id, date)     │
      │       DO UPDATE SET impressions=…, reach=…, …     │
      │                                                   │
      │    Output: ~11,000 rows upserted across 3 accts   │
      └───────────────────────────────────────────────────┘
                  │
                  ▼ (only if primary_sync exit code == 0)
        ┌─────────────────────────────────┐
        │ subprocess.run(result_classifier.py)│
        └─────────────────────────────────┘
                  │
                  ▼
      ┌───────────────────────────────────────────────────┐
      │  result_classifier.py:                            │
      │                                                   │
      │    1. SELECT * FROM primary_table                 │
      │       WHERE ad_created_date IS NOT NULL           │
      │       (returns ~100k rows)                        │
      │    2. For each unique (account_name, ad_id):      │
      │         - eval_end_date = ad_created_date + 14d   │
      │         - sum impressions where                   │
      │             ad_created_date <= date <= eval_end   │
      │         - decide: Winner / Pending / Failed       │
      │    3. UPSERT into ad_results                      │
      │       ON CONFLICT (account_name, ad_id)           │
      │       DO UPDATE SET impressions_14d=…             │
      │                                                   │
      │    Output: ~3,650 rows upserted                   │
      └───────────────────────────────────────────────────┘
                  │
                  ▼
        ┌─────────────────────────────────┐
        │  subprocess.run(results_sync.py) │
        └─────────────────────────────────┘
                  │
                  ▼
      ┌───────────────────────────────────────────────────┐
      │  results_sync.py:                                 │
      │                                                   │
      │    today = today's date                           │
      │    since = today - 29 days                        │
      │                                                   │
      │    For each of 3 accounts:                        │
      │      1. SELECT * FROM primary_table               │
      │         WHERE account_name = ? AND date >= since  │
      │      2. Group rows by ad_id, sum metrics          │
      │         (impr, reach, spend, conv, ftewv, ncp,    │
      │          ltv_reach, ltv_frequency)                │
      │      3. Build ads_json[]                          │
      │      4. INSERT into results_table                 │
      │         (account_name='Raho Saadaa', ads_json=…)  │
      │                                                   │
      │    Then: aggregate all 3 → "All Accounts" row     │
      │                                                   │
      │    Then: purge_old_results — keep 1 per account   │
      │                                                   │
      │    Output: 4 rows in results_table                │
      └───────────────────────────────────────────────────┘
                  │
                  ▼
              ALL DATA FRESH
              (until tomorrow's run)
```

---

## 5. Hourly cycle (intra-day refresh)

```
XX:30 IST   cron.py detects "should_run_hourly" is true
            (every hour except 00:30, which is the nightly slot)

                  │
                  ▼
        ┌──────────────────────────────────────┐
        │ subprocess.run(hourly_impressions.py) │
        └──────────────────────────────────────┘
                  │
                  ▼
      ┌────────────────────────────────────────────────────┐
      │  hourly_impressions.py — per account:              │
      │                                                    │
      │    today = today's date                            │
      │    since = today - 29 days                         │
      │                                                    │
      │    STEP 1: fetch today's data from Meta            │
      │    ─────────────────────────────────                │
      │    GET /act_<id>/insights                          │
      │      ?level=ad                                     │
      │      &date_preset=today                            │
      │      &fields=ad_id,impressions,reach,spend,…       │
      │    →  { ad_id → today's metrics }                  │
      │                                                    │
      │    STEP 2: read historical primary_table           │
      │    ─────────────────────────────────                │
      │    SELECT * FROM primary_table                     │
      │      WHERE account_name = ?                        │
      │        AND date >= since                           │
      │    (read-only — never writes to primary_table)     │
      │                                                    │
      │    STEP 3: merge in memory                         │
      │    ─────────────────                                │
      │    For each ad_id:                                 │
      │      base = sum of primary_table rows              │
      │             (EXCLUDING today, since today is stale)│
      │      total = base + today's Meta data              │
      │                                                    │
      │    STEP 4: write to results_table                  │
      │    ────────────────────                             │
      │    INSERT into results_table                       │
      │      (account_name = …,                            │
      │       computed_at = now(),                         │
      │       ads_json = [...per-ad totals...],            │
      │       total_spend = sum, … )                       │
      │                                                    │
      │    STEP 5: combined "All Accounts" row              │
      │    ──────────────────────────                       │
      │    Same as above but with ads from all 3 accts     │
      │    deduplicated by ad_id                           │
      │                                                    │
      │    STEP 6: purge                                   │
      │    ─────────                                        │
      │    DELETE FROM results_table                       │
      │    WHERE id NOT IN (latest 1 per account_name)     │
      └────────────────────────────────────────────────────┘
                  │
                  ▼
        results_table now reflects current hour
        primary_table is unchanged
        (next hourly run repeats; nightly does a full refresh)
```

---

## 6. Dashboard read paths

The dashboard never writes anything. Each tab triggers a specific Supabase fetch.

```
┌───────────────────────────────────────────────────────────────────┐
│   USER OPENS URL  ?supabaseUrl=…&supabaseAnon=…                   │
└─────────────────────┬─────────────────────────────────────────────┘
                      │
                      │  index.html loads, JS reads URL params
                      │
                      ▼
        ┌──────────────────────────────────┐
        │   selectAccount(0) on init       │
        │   → fetchSheet()                 │
        └─────────────────────┬────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────────┐
        │ fetchSheet():                               │
        │   1. fetchFromResultsTable(account_name)    │
        │      → GET /rest/v1/results_table           │
        │           ?account_name=eq.<name>           │
        │           &order=computed_at.desc&limit=1   │
        │      → response.ads_json  → ALL[]           │
        │                                             │
        │   2. If no row found → fallback:            │
        │      GET /rest/v1/primary_table             │
        │           ?account_name=eq.<name>           │
        │           &limit=1000&offset=…              │
        │      (paged) → mapSupabaseRows() → ALL[]    │
        └─────────────────────┬───────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────────┐
        │  ALL[] = list of per-ad objects             │
        │  applyFilters() → filteredAds() → renders   │
        └─────────────────────────────────────────────┘


Tab-specific reads:

┌──────────────────────────────┬────────────────────────────────────┐
│  Tab                         │  HTTP call                         │
├──────────────────────────────┼────────────────────────────────────┤
│  Creative Testing            │  results_table (full ads_json)     │
│   (custom date range picked) │  → fetchFromPrimaryForDateRange()  │
│                              │    GET primary_table?date=gte.…    │
├──────────────────────────────┼────────────────────────────────────┤
│  Analysis Engine             │  results_table (fast path)         │
│                              │  → falls back to primary_table     │
│                              │    only if range is outside cache  │
├──────────────────────────────┼────────────────────────────────────┤
│  Lifecycle (Fatigue)         │  results_table only                │
│                              │  GET results_table                 │
│                              │    ?account_name=eq.<acct>         │
│                              │    &limit=1&order=computed_at.desc │
│                              │  → JS parses ads_json              │
│                              │  → filters by ad_created_date      │
│                              │  → buckets by frequency            │
└──────────────────────────────┴────────────────────────────────────┘
```

---

## 7. How an ad becomes a Winner (single-ad lifecycle)

```
Day 0 — Ad created in Meta Ads Manager
        ad_created_date = 2026-05-01

Day 0 → Day 14 — Evaluation window (the "buffer")

  Day 0:  Meta starts delivering. Some impressions today.
              ↓
          Nightly: primary_sync.py records this day as one row.
                   primary_table now has 1 row for this ad.
              ↓
          Nightly: result_classifier.py computes
                   impressions_14d = 1,200 (so far)
                   result_status = 'Result Pending'
                   ad_results updated.

  Day 1:  more impressions. Another primary_table row.
          Classifier re-runs. impressions_14d = 4,500. Still Pending.

  Day 2:  more. impressions_14d = 18,000. Still Pending.
  …
  Day 5:  impressions_14d hits 51,300. Crosses 50,000 threshold.
              ↓
          Classifier next morning:
              result_status = 'Winner'
              crossed_threshold_at = day 5's date
              ad_results updated.

  Day 6 → 14: more impressions accumulate, but verdict is locked.
              The ad keeps appearing as 'Winner' in ad_results.

After Day 14 — evaluation window closes.

  If impressions_14d crossed 50,000 → 'Winner' (locked).
  If never crossed                 → 'Failed'.
```

The classifier is idempotent and runs nightly — re-runs don't change anything for ads whose windows are already closed.

---

## 8. The frequency buckets (Lifecycle tab logic)

```
Inputs (per ad, from results_table.ads_json):
  - total_impressions  =  ad.impr
  - lifetime_reach     =  ad.ltvReach   (Meta's true unique reach)

Compute:
  - frequency  =  total_impressions / lifetime_reach
                  (if lifetime_reach > 0; else fall back to max-daily-reach)

Bucket assignment:
  ┌──────────────────────┬──────────────┬─────────────┐
  │  Frequency           │  Bucket      │  Color      │
  ├──────────────────────┼──────────────┼─────────────┤
  │  0  ≤  freq  ≤  2    │  Low         │  sage       │
  │  2  <  freq  ≤  2.5  │  Medium      │  gold       │
  │  2.5 < freq  ≤  3    │  High        │  terra      │
  │       freq  >  3     │  (excluded)  │  —          │
  └──────────────────────┴──────────────┴─────────────┘

Date filter (Lifecycle tab):
  - User picks a date range with the two date inputs.
  - This filter is applied to ad_created_date — NOT delivery date.
  - Only ads whose ad_created_date falls in the range count.
```

---

## 9. The cron clock

```
00:30 IST  │ primary_sync   ─┐
00:30 IST  │ result_classifier─┼─ nightly chain (~10-30 min total)
00:30 IST  │ results_sync   ─┘
           │
01:30 IST  │ hourly_impressions
02:30 IST  │ hourly_impressions
03:30 IST  │ hourly_impressions
04:30 IST  │ hourly_impressions
05:30 IST  │ hourly_impressions
   ⋮
23:30 IST  │ hourly_impressions
           │
00:30 IST  │ (cycle repeats)
```

Total per day: **23 hourly runs + 1 nightly chain = 24 sync events**.

---

## 10. Network call counts per cycle

```
Per nightly run (primary_sync.py for 3 accounts):
  - 3   × /act_<id>?fields=…
  - ~6  × /act_<id>/insights (paginated, multiple chunks of 7 days)
  - ~100 × /<ad_id>?fields=…  (batched 25 ads per call)
  - 3   × /act_<id>/insights date_preset=maximum
  ───────────────────────
  Total: ~110-150 Meta API calls

Per hourly run (hourly_impressions.py for 3 accounts):
  - 3 × /act_<id>/insights date_preset=today
  - 3 × SELECT primary_table (1 query each)
  ───────────────────────
  Total: 3 Meta API calls + 3 DB reads + 4 DB writes
```

---

## 11. Failure modes and what happens

```
┌─────────────────────────────────┬─────────────────────────────────────┐
│  Failure                        │  Behavior                           │
├─────────────────────────────────┼─────────────────────────────────────┤
│  Meta API 500 / 503             │  primary_sync retries up to 4 times │
│                                 │  with 5/15/30/60s backoff           │
│                                 │  hourly_impressions retries silently│
├─────────────────────────────────┼─────────────────────────────────────┤
│  Supabase statement_timeout     │  result_classifier sets             │
│  (large SELECT > 8s default)    │  "SET statement_timeout = 600s"     │
│                                 │  hourly_impressions sets 300s       │
├─────────────────────────────────┼─────────────────────────────────────┤
│  Laptop sleeps through 00:30    │  cron.py is a Python while-loop;    │
│                                 │  OS suspends it during sleep.       │
│                                 │  On wake, the loop resumes but the  │
│                                 │  fixed-minute check has passed →    │
│                                 │  trigger missed entirely.           │
│                                 │  Manual fix: re-run the chain.      │
├─────────────────────────────────┼─────────────────────────────────────┤
│  Meta /ads endpoint times out   │  primary_sync skips the metadata    │
│                                 │  enrichment step and continues.     │
│                                 │  Ad statuses may be stale.          │
├─────────────────────────────────┼─────────────────────────────────────┤
│  Dashboard hits 401 from        │  No anon key or wrong project.      │
│  Supabase REST                  │  Tables also require RLS policies   │
│                                 │  allowing SELECT for role 'anon'.   │
└─────────────────────────────────┴─────────────────────────────────────┘
```

---

## 12. File map

```
backend/
├── primary_sync.py           ← nightly full daily fetch (15-day window)
├── hourly_impressions.py     ← hourly today-only fetch
├── result_classifier.py      ← 14-day buffer evaluator
├── results_sync.py           ← 30-day rollup writer
├── cron.py                   ← scheduler daemon
├── db.py                     ← psycopg2 connection helper
├── run_backfill.py           ← one-off historical backfill tool
├── logs/
│   ├── cron.log
│   ├── full_backfill.log
│   └── hourly_impressions.log
├── primary_sync.log          ← (root) sync output
├── result_classifier.log     ← (root) classifier output
└── BACKEND_GUIDE.md          ← this file
```

---

## 13. Quick reference — manual commands

```powershell
# Run a single primary sync (15-day window from GMT today)
python primary_sync.py daily

# Run the classifier
python result_classifier.py

# Run results_sync once
python results_sync.py

# Run hourly impressions once
python hourly_impressions.py

# Show cron schedule status
python cron.py --status

# Run a single job through cron's wrapper
python cron.py --now primary
python cron.py --now hourly
python cron.py --now results
python cron.py --now lifecycle
python cron.py --now both

# Start the cron daemon (detached, survives terminal close)
Start-Process pythonw -ArgumentList "cron.py" -WorkingDirectory "backend\"
```

---

## 14. Data freshness guarantees

```
For "Creative Testing" tab metrics (read from results_table):
  Last refreshed = max(last nightly, last hourly)
  → typically less than 60 minutes stale

For "Lifecycle" tab metrics (read from results_table):
  Same as above.

For per-day breakdowns drilled via custom date range
(read from primary_table directly):
  As fresh as the last nightly primary_sync.
  → can be up to 24 hours stale

For "Winner / Pending / Failed" verdicts (ad_results):
  Refreshed nightly after primary_sync completes.
  → typically less than 24 hours stale
```

---

## 15. The data path for one rendered number

Example: the **"Total Spend"** displayed in the Low Fatigue card.

```
1. Meta API stores: today's spend per ad
2. At XX:30  hourly_impressions.py fires
3. GET /act_<id>/insights date_preset=today
   → today's spend per ad
4. SELECT * FROM primary_table WHERE date >= today-29
   → historical 29 days of spend per ad
5. In memory, for each ad:
     spend_total = spend_history + spend_today
6. Filter: keep only ads created in the user's date range
7. For each ad: compute frequency = impr / ltv_reach
8. If frequency in [0, 2]: add to Low bucket
9. INSERT INTO results_table (ads_json = [...])

10. User opens dashboard
11. GET /rest/v1/results_table?account_name=eq.<X>&limit=1
12. Parse ads_json
13. Filter ads by ad_created_date
14. Re-bucket by frequency
15. Sum spend across Low bucket ads
16. Render the number on screen
```

---

## 16. Filter logic — how ads are shown

Every dashboard tab applies a chain of filters to the in-memory `ALL[]` array
each time the user interacts with the filter bar. The order matters because each
step removes rows from the working set.

### 16.1 Creative Testing tab — `filteredAds()` filter chain

```
ALL[]   (every ad row currently loaded for the selected account)
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  1. Account filter                                              │
│     drop if  a.accountName ≠ selected account                   │
│     (skipped when "All Accounts" is selected, ACTIVE_ACCT===0)  │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. Excl. Copy toggle                                           │
│     drop if  a.isCopy === true   AND  EXCLUDE_COPY_TOGGLE = ON  │
│     (a.isCopy is computed from ad name via /copy/i regex)       │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. Active Only toggle                                          │
│     drop if  ad_status ∉ {ACTIVE, ADSET_PAUSED}                 │
│              AND  ACTIVE_ONLY_TOGGLE = ON                       │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. Ad Name filter  (mode = contains | doesn't contain)         │
│     terms = input text split by " or " (case-insensitive)       │
│                                                                 │
│     if mode === 'contains':                                     │
│        drop if NO term matches a.name (case-insensitive)        │
│     if mode === 'not_contains':                                 │
│        drop if ANY term matches a.name                          │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  5. Report Date filter (Date Range From → To)                   │
│     drop if  a.sheetDateObj < dateFrom                          │
│     drop if  a.sheetDateObj > dateTo  (23:59:59 of dateTo)      │
│     SKIPPED when window._fromResultsTable=true AND no explicit  │
│     date set — the results_table row is already pre-aggregated  │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  6. Created Date filter (Created From → To)                     │
│     drop if  a.adCreated < createdFrom                          │
│     drop if  a.adCreated > createdTo                            │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
filteredAds[]
  → feeds renderOverviewPerf(),
         renderCreatives(),
         renderResults(),
         ctExportCSV()
```

### 16.2 Lifecycle (Fatigue) tab — `ftFetchAndRender()` filter chain

```
results_table.ads_json[]   (per-ad aggregates for selected account)
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  1. Account selection                                           │
│     Account chip → query results_table with                     │
│         account_name = eq.<chosen account>                      │
│     (server-side filter, runs at fetch time)                    │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. ad_created_date date-range filter                           │
│     drop if  ad.adCreated < ftDateFrom                          │
│     drop if  ad.adCreated > ftDateTo                            │
│     NOTE: this filter is on CREATED date, not delivery date     │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. Zero-impression drop                                        │
│     drop if  ad.impr ≤ 0                                        │
│     (ad has no measurable delivery, can't compute frequency)    │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. Frequency bucketing                                         │
│     denom = ad.ltvReach > 0 ? ad.ltvReach : ad.maxDailyReach    │
│     freq  = ad.impr / denom                                     │
│                                                                 │
│     if freq > 3       → bucketIdx = -1  (excluded from display) │
│     if freq ≤ 2       → bucketIdx = 0   (Low)                   │
│     if freq ≤ 2.5     → bucketIdx = 1   (Medium)                │
│     if freq ≤ 3       → bucketIdx = 2   (High)                  │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
ftAds[]
  → bucket counts shown on cards
  → click a bucket → drill-down table (filtered by bucketIdx)
```

### 16.3 Analysis Engine — multi-rule filter

The Analysis Engine has its own filter builder with arbitrary "contains all of /
any of / doesn't contain" rules joined as AND. Each rule applies to a column
selected from the dropdown (ad name, campaign name, ID, category, ad status).

```
ae-filtered[]  =  aeRaw.filter(ad =>
    rules.every(rule =>
        match(ad[rule.column], rule.condition, rule.terms)
    )
)
```

Match logic per condition:
```
'contains all of'   →  every term must appear in the column value
'contains any of'   →  at least one term must appear
'doesn't contain'   →  no term may appear
```

The Analysis Engine also applies the Category chip filter (Winner / Priority /
Analyse / Discarded) on top, plus its own Status filter and Date Range picker.

---

## 17. Counting rules — how the numbers shown are derived

### 17.1 Total Ads (badge at top right of CT tab)

```
Set:        filteredAds[]
Operation:  uniqueAdIds = new Set(filteredAds.map(a => a.adId || a.name))
Display:    uniqueAdIds.size + ' ads'
```

A single ad with 10 daily delivery rows counts as **1** ad — duplicates by
`ad_id` are collapsed.

### 17.2 Totals (Overview - Performance KPI cards)

```
Total Amount Spent       =  Σ filteredAds.spend
Total Impressions        =  Σ filteredAds.impr
Total Reach              =  Σ filteredAds.reach
Total Conversion Value   =  Σ filteredAds.convValue
CT ROAS                  =  Σ convValue / Σ spend
Avg Hook Rate            =  weighted average of per-ad hook rates
                            weighted by impressions
Avg Outbound CTR         =  Σ outClicks / Σ impr × 100
Avg Engagement Rate      =  Σ postEng / Σ impr × 100
Avg ThruPlay Rate        =  Σ thruPlays / Σ impr × 100
Avg Hold Rate            =  Σ thruPlays / Σ videoPlays × 100
```

When data is from `results_table.ads_json`, each entry is already a per-ad
aggregate — summing across entries gives the totals directly. When data is
from `primary_table`, the dashboard first groups rows by `ad_id` and sums
metrics within each group before computing the totals above.

### 17.3 Creative Type breakdown (Overview - Creatives)

```
For each ad in filteredAds:

  ctype = detect_ctype(ad.name):
      "IFAD" in name.upper()                  → "IFAD"
      "GAD" in name.upper()                   → "Graphic AD"
      any of {VRP, NNC, VIDEO, IGP} in name   → "VID"
      any of {STATIC, _ST_, +ST+} in name     → "STATIC"
      else                                    → "VID" (default)

  OVERRIDE (post-detection):
      if ctype === "VID"
         AND no video signal (videoPlays=0 AND thruPlays=0)
         AND no explicit VID keyword (VRP/NNC/VIDEO/IGP/VID-AD)
      → reclassify as "Graphic AD"

      if ctype not in {IFAD, Graphic AD}
         AND video signal present
      → reclassify as "VID"

  Count grouped by detected ctype.
```

### 17.4 Product breakdown (Overview - Creatives)

```
For each ad in filteredAds:

  product = detect_product(ad.name):
      tokens = ad.name.upper().split by [+, -, _, space, .]
      if any token is in PRODUCTS list           → that product
      else if any PRODUCTS string is in name     → that product
      else                                       → "Other"

PRODUCTS list (41 entries):
  BST, BCO, COO, SDAFD, CL, TTB, TW, GE, SDCPT, SDELKB, SDELPT,
  SDFLK, SDFSK, SDCSS, SDLS, SDRPT, SDRST, SDVPL, SMCP, SMELMS,
  SMFLK, SMFSK, SMLS, BR, SDASP, SDCP, SDWLP, GP, SUZNS, WW,
  STL, LE, WLP, SDLWC, SDTTB, SDECT, SDALS, WBS, SD5, SUPFH,
  SDAWP, SDCSP
```

### 17.5 Category breakdown (Overview - Performance result chips)

```
For each ad in filteredAds:
  cat = getAdCategory(...)
  counts[cat] += 1

Display:
  Incremental Winner chip:  counts['Incremental Winner']
  Winner chip:              counts['Winner']
  Priority chip:            counts['Priority']
  Analyse chip:             counts['Analyse']
  Discarded chip:           counts['Discarded']
```

### 17.6 Frequency-bucket counts (Lifecycle tab)

```
For each ad in ftAds  (after filters from §16.2):
  if ad.bucketIdx === 0  →  Low.n   += 1; Low.spend   += ad.spend; Low.reach   += ad.reach
  if ad.bucketIdx === 1  →  Med.n   += 1; Med.spend   += ad.spend; Med.reach   += ad.reach
  if ad.bucketIdx === 2  →  High.n  += 1; High.spend  += ad.spend; High.reach  += ad.reach
```

Each card shows `n` ads, total spend (₹), and total reach for ads in that bucket.

### 17.7 14-day lifecycle status counts (Lifecycle tab chips, if used)

```
SELECT result_status, COUNT(*)
FROM ad_results
WHERE  account_name = <selected>
   AND ad_created_date BETWEEN <from> AND <to>
GROUP BY result_status
```

Three chips:
```
Winner            count of rows with result_status = 'Winner'
Result Pending    count with result_status = 'Result Pending'
Failed            count with result_status = 'Failed'
```

---

## 18. Value definitions (glossary)

### 18.1 Raw Meta-API metrics

| Term | Formula / Source | Notes |
|---|---|---|
| `impressions` | `/insights` field | Number of times the ad was shown |
| `reach` | `/insights` field | Unique people who saw the ad **on that day**. Daily reaches overlap across days (same user counted each day). |
| `frequency` | `impressions / reach` (Meta-computed) | Average times each person saw the ad in that window |
| `ltv_reach` | `/insights date_preset=maximum` | Lifetime unique reach for the ad. Same value across all rows of the same ad — `MAX(ltv_reach)` recovers the true value if some rows are null |
| `ltv_frequency` | `/insights date_preset=maximum` | Lifetime average frequency = lifetime impressions / `ltv_reach` |
| `amount_spent_inr` (spend) | `/insights spend` | Amount in INR billed for that day's delivery |
| `conversion_value` | `/insights action_values[purchase]` | Total purchase value attributed to the ad that day |
| `outbound_clicks` | `/insights outbound_clicks[outbound_click]` | Clicks that left Meta to the destination URL |
| `thruplays` | `/insights video_thruplay_watched_actions` | Number of full / near-full video views (≥ 95% or 15s, per Meta) |
| `three_sec_video_plays` | `/insights actions[video_view]` | 3-second video views |
| `post_engagements` | `/insights actions[post_engagement]` | Sum of all post engagements (likes, comments, shares, clicks…) |
| `ftewv_count` | `/insights actions[offsite_conversion.custom.1133449967928420]` | First-Time Engaged Video View — custom conversion |
| `ncp_count` | `/insights actions[offsite_conversion.custom.1109740267306786]` | New Customer Purchase — custom conversion |
| `ad_status` | `/<ad_id>?fields=effective_status` | ACTIVE / PAUSED / CAMPAIGN_PAUSED / WITH_ISSUES / ADSET_PAUSED / ARCHIVED / DELETED |
| `ad_created_date` | `/<ad_id>?fields=created_time` | Date the ad was first created in Meta |

### 18.2 Derived metrics (computed by the dashboard or sync)

| Term | Formula | Threshold (default) |
|---|---|---|
| **CT ROAS** | `conv_value / spend` | F2 ≥ 3.2 |
| **Cost / NCP** | `spend / ncp_count` | F3 ≤ ₹525 |
| **Cost / FTEWV** | `spend / ftewv_count` | F4 ≤ ₹12 |
| **Hook Rate** | `three_sec_video_plays / impressions × 100` | % who watched ≥ 3 seconds |
| **Hold Rate** | `thruplays / three_sec_video_plays × 100` | of starters, % who finished |
| **ThruPlay Rate** | `thruplays / impressions × 100` | end-to-end view % |
| **Outbound CTR** | `outbound_clicks / impressions × 100` | click-through to destination |
| **Engagement Rate** | `post_engagements / impressions × 100` | engagement intensity |
| **Frequency (window)** | `Σ impressions / ltv_reach` <br/>fallback: `Σ impressions / max(daily reach)` | used for Lifecycle bucketing |
| **VPT (avg video play time)** | `Σ (vpt × impr) / Σ impr` | impression-weighted average |
| **Profit Eff** | `conv_value − spend` | absolute profit |

### 18.3 The four Winner filters (F1 – F4)

| Filter | What it checks | Default threshold | Tunable in UI |
|---|---|---|---|
| **F1** | `impressions ≥ targetImp` | 50,000 | Yes (`#targetImp`) |
| **F2** | `CT ROAS ≥ targetRoas` | 3.2 | Yes (`#targetRoas`) |
| **F3** | `Cost/NCP ≤ targetCpn` AND `Cost/NCP > 0` | ₹525 | Yes (`#targetCpn`) |
| **F4** | `Cost/FTEWV ≤ targetFtewv` AND `Cost/FTEWV > 0` | ₹12 | Yes (`#targetFtewv`) |

**Note**: F2 and F3 are treated as a **combined gate** in the current logic —
an ad passes the combined gate if either F2 ✓ or F3 ✓.

### 18.4 Category outcomes (`getAdCategory()`)

| Category | Rule | Action recommendation |
|---|---|---|
| **Incremental Winner** | F1 ✓ AND (F2 ✓ OR F3 ✓) AND F4 ✓ | Scale aggressively |
| **Winner** | F1 ✓ AND (F2 ✓ OR F3 ✓) | Scale budget, maintain format |
| **Priority** | F1 ✓ AND F2 ✗ AND F3 ✗ | Iterate — impressions met but no efficiency signal |
| **Analyse** | NOT F1 AND (has conversions OR F3 ✓) | Study pattern — sparse data with signal |
| **Discarded** | NOT F1 AND no conv AND F3 ✗ | Do not replicate, review creative |

### 18.5 14-day lifecycle status (`ad_results.result_status`)

These are **distinct from Category** above. The classifier evaluates each ad
against a single binary test: did it accumulate 50,000 impressions in its
first 14 days?

| Status | Rule |
|---|---|
| **Winner** | `impressions_14d ≥ 50,000` (locked once crossed) |
| **Result Pending** | Today is still inside the 14-day window AND `impressions_14d < 50,000` |
| **Failed** | Today is past the 14-day window AND `impressions_14d < 50,000` |

### 18.6 Ad-name parsing rules

The dashboard infers two facts from each ad's name string:

```
detect_ctype(name):
  return 'IFAD'        if "IFAD" in upper(name)
  return 'Graphic AD'  if "GAD" in upper(name)
  return 'VID'         if any of {VRP, NNC, VIDEO, IGP} in upper(name)
  return 'STATIC'      if any of {STATIC, _ST_, +ST+} in upper(name)
  return 'VID'         otherwise (default)

detect_product(name):
  tokens = upper(name).split([+, -, _, space, .])
  return  first token in PRODUCTS list (exact match)
       OR first PRODUCTS string found as substring in upper(name)
       OR 'Other'

is_copy(name):
  return  /copy/i .test(name)
```

These rules are mirrored in `results_sync.py`, `hourly_impressions.py`, and
`index.html` so the same name always yields the same labels regardless of
which layer parsed it.
