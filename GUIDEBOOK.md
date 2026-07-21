# Saadaa Creative Testing Dashboard — Guidebook

A field-by-field reference for the v2 dashboard (`index_v2.html`). Covers every view, every filter, every column. Drop screenshots into `docs/screenshots/` and reference them under each section — the placeholder image tags below already point there.

---

## Table of contents

- [Getting started](#getting-started)
- [Global concepts](#global-concepts)
- [Sidebar navigation](#sidebar-navigation)
- [Section 1 — Creative Testing](#section-1--creative-testing)
- [Section 2 — Creative Lifecycle](#section-2--creative-lifecycle)
- [Section 3 — Ads Analyse](#section-3--ads-analyse)
- [Section 4 — Ad Intelligence](#section-4--ad-intelligence)
- [Section 5 — Inventory](#section-5--inventory)
- [Modals and drawers](#modals-and-drawers)
- [Data pipeline — where the numbers come from](#data-pipeline--where-the-numbers-come-from)
- [FAQ and troubleshooting](#faq-and-troubleshooting)

---

## Getting started

### Opening the dashboard

The dashboard is a single HTML file that reads Supabase over the browser's REST API. Two query-string parameters are required:

```
index_v2.html?supabaseUrl=https://<project>.supabase.co&supabaseAnon=<anon-key>
```

Without those params the top-status strip shows `Missing ?supabaseUrl & ?supabaseAnon` and nothing loads.

### First-load behaviour

On first open the dashboard fires four parallel HTTP calls and waits for all of them:

```
ae_table_view          15,268 ads    lifetime aggregates + F1/F2/F3/F4 pass flags
ae_reach_recent           varies     incremental daily reach snapshot
ae_freq_lifecycle_mat  11,681 rows   per-ad frequency crossing dates
results_table (cache)  30-day set    fast-path aggregate for Creative Testing
```

The **top-right status strip** shows which source is loaded and the date window it covers. `Loaded 3,289 ads (results_table cache · 2026-06-03 → 2026-07-02)` is normal.

`![Getting Started — top strip](docs/screenshots/getting-started-top-strip.png)`

---

## Global concepts

### The four filters (F1–F4)

Every ad in the system is scored against four cost-and-signal filters. Every category (Winner, P0, etc.) is a rule on top of these:

```
F1   Impressions     >=   50,000       (delivery — has this ad served enough to judge)
F2   ROAS            >=      3         (return on ad spend — Meta-attributed; editable in toolbar)
F3   Cost per NCP    <=     ₹525       (new customer purchase — retention proxy)
F4   Cost per FTEWV  <=      ₹25       (first-time engaged video-viewer — creative quality)
```

Thresholds are editable in the toolbar of both Creative Testing and Ads Analyse (fields `#ctF1..#ctF4` / `#aeF1..#aeF4`). Resetting reverts to the defaults above.

### The six categories

Every ad falls into exactly one bucket. Priority order matters — the first matching rule wins:

```
Incremental Winner    F1 AND (F2 OR F3) AND F4    scale aggressively; top-tier proven
Winner                F1 AND (F2 OR F3)           scale; F4 close-second, iterate on quality
P0 analysis           F1 AND F4                   impressions + cheap FTEWV; iteration target
P1 analysis           F1 only                     hit impressions but no signal on ROAS/NCP/FTEWV
P2 analysis           F2 only                     strong ROAS but still building impressions
Discarded             else                        no signal — cut spend or rework creative
```

Colours across every KPI tile and badge follow this same scheme.

### The unified date-range picker

Every view uses the same picker. Preset shortcuts (`.drp-preset`):

```
Today               Last 7 Days        This Month           Last 90 Days
Yesterday           Last 30 Days       Last Month           Lifetime            Custom Range
```

Ad Intelligence adds a `Last 15 Days` between `Last 7 Days` and `This Month`.

The picker has dual month calendars, arrow navigation, and an `Apply` button that only commits after a valid `from / to` range is set. Cancel discards.

`![Date-range picker](docs/screenshots/date-range-picker.png)`

### The multi-filter builder

Both Creative Testing (`#ctMfRows`) and Ads Analyse (`#aeMfRows`) have a chainable rule builder. Each row is `field | operator | value` and rules combine with **AND**.

```
Field         Any of: ad_name, campaign, adset, account, category, status, spend, ROAS,
              impressions, reach, cost_per_ftewv, cost_per_ncp
Operator      contains, does_not_contain, equals, not_equals, > , >=, <, <=, is_empty
Value         free text or numeric (auto-parses)
```

Buttons:

```
Add Rule      appends a fresh field/operator/value row
Apply         commits all rules; KPIs, funnel, charts, and table re-compute
Clear         removes all rules and reapplies
```

---

## Sidebar navigation

The left rail (`#sidebar`) groups five destinations:

```
CREATIVE TESTING
  Creative testing        →  view-testing       broad performance dashboard
  Creative Lifecycle      →  view-lifecycle     frequency saturation + 14-day buffer

ANALYSIS
  Ads Analyse             →  view-ae            row-level table (60+ columns)
  Ad Intelligence         →  view-adintel       Shopify orders / UTM attribution

CATALOG
  Inventory               →  view-inventory     Shopify product + stock
```

The active item highlights yellow. Switching views doesn't re-fetch — the data was loaded once at boot and each view reads whichever slice it needs. Below the nav is a live **DB status** line (`#dbStat`) showing which source is powering the current numbers.

---

## Section 1 — Creative Testing

The default landing view. Mirrors the layout of the legacy v1 dashboard.

`![Creative Testing — overview](docs/screenshots/ct-overview.png)`

### Toolbar

```
Definitions  (#btnDef)      opens the metric-definitions modal
Refresh      (#btnRefresh)  re-fetches ae_table_view + primary aggregation
Export CSV   (#btnExport)   exports the currently-filtered ad list
```

### Date-preset row

Chip row above the toolbar. Default is `Last 30d`. Custom presets show `#fDateFrom` and `#fDateTo` date inputs.

```
Last 7d      Last 30d      Last 90d      This Month      Custom
```

Changing the preset re-runs `fetchPrimaryAggregated()` against `primary_table` with the new window.

### Filter grid

Six controls arranged left-to-right, then a second row of thresholds:

```
Campaign         (#fCampaign)      dropdown — list is derived from primary_table
Content type     (#fContent)       IFAD / Graphic AD / Video / Static
Tier             (#fTier)          the 6 categories
Status           (#fStatus)        Active / Paused / Archived
Account          (#fAccount)       one of 3 Meta ad accounts
Created from-to  (#fCreatedFrom)   ad creation-date window (independent of delivery window)
Excl. copy       (#fExclCopy)      toggle — hides ads with "copy" in the name
CT Format        (#fCtFormat)      toggle — restrict to CLP-/CTP- creative-testing names
Reset Filters    (#fCreatedClear)  clears every filter and preset back to Last 30d
```

Thresholds (second row, editable numeric):

```
F1 — Impressions       (#ctF1)     default 50,000
F2 — ROAS              (#ctF2)     default 3
F3 — Cost / NCP        (#ctF3)     default 525
F4 — Cost / FTEWV      (#ctF4)     default 12
Reset (#ctResetThresh)             defaults
```

Changing a threshold live-recategorises every ad in the browser (no HTTP refetch) and updates every KPI tile, the Funnel, and the table.

`![Creative Testing — filter grid](docs/screenshots/ct-filter-grid.png)`

### KPI tiles

Six clickable tiles across the top (`#kpiRow`). Each shows count + total spend for its category. Clicking any tile opens the **Category Performers** modal (see the modals section).

```
Winner                (#kp-winner)   green
Incremental Winner    (#kp-incr)     dark green
P0 analysis           (#kp-pri)      amber
P1 analysis           (#kp-a1)       muted amber
P2 analysis           (#kp-a2)       terracotta
Discarded             (#kp-dis)      red
```

Clicking the same tile again closes the modal.

### Creative Type Funnel

A 6×6 matrix (`#funnelBody`): rows are creative types (IFAD / GAD / VID / UGC / ADB / BR / BST / Other), columns are the 6 categories plus a "grand total" column. Each cell shows count + a percentage bar.

```
row header    IFAD, Graphic AD, Video, UGC, ADB, Brand, BST, Other
column header Incremental Winner, Winner, P0, P1, P2, Discarded
```

Click any cell with a non-zero count to open the drill modal pre-filtered to that intersection. `data-fnl-ct` and `data-fnl-cat` on the cell carry the exact filter.

`![Creative Type Funnel](docs/screenshots/ct-funnel.png)`

### Focus panels

Two pill strips below the funnel:

**Product Focus** (`#prodStrip`) — how ad spend distributes across landing-page types:

```
Collection page   /collections/... URLs
Product page      /products/... URLs
Custom page       /pages/... URLs
Other             everything else (blog, homepage, etc.)
```

**Creative Focus** (`#creativeStrip`) — same but for creative type buckets:

```
IFAD   GAD   VID   UGC   ADB   BR   BST   Other
```

Every pill shows count + spend and opens the drill modal for that bucket on click.

### Charts strip

Three canvases side-by-side (`#chProd`, `#chTrend`, `#chCreative`):

```
Product mix           donut     spend by product bucket
Creatives by week     bar       creatives added per week (creation date)
Creative mix          donut     spend by creative-type bucket
```

Each has a small `i` icon (`data-ci`) that shows a tooltip explaining what the metric measures.

### Landing Page Focus

Grouped table (`#landingPageFocus`) sorted by ad count or spend. One row per unique landing URL.

```
Column         Meaning
Product code   friendly name derived from URL (collection slug, product slug)
Landing URL    full ad_link
Count          number of ads pointing at this URL
Spend          sum of amount_spent for those ads
```

Click any row to open the drill modal filtered to that URL.

---

## Section 2 — Creative Lifecycle

Two sub-sections toggle at the top of the view (`#lifeSectionToggle`):

```
Frequency distribution   (default)  frequency saturation across ad lifecycles
14-Day buffer                       ads at or past day-14 needing verdict
```

`![Creative Lifecycle — overview](docs/screenshots/lifecycle-overview.png)`

### Date-range control

Lifetime by default. The picker filters the view to ads created within the chosen range (`ad_created` window, not delivery). All KPIs and bucket counts respect it.

```
Refresh    (#lifeRefresh)   re-fetches allAds + freqLifecycleByAdId
Export CSV (#lifeExport)    exports the visible ad list
```

### Frequency Distribution sub-view

#### Status filter + search bar

Chip row above the KPI tiles (`#lifeFreqStatusRow`). Applies to totals, bucket counts, and drill:

```
All statuses      Active      Paused      Archived / Other
```

Adjacent search input (`#lifeFreqSearch`) supports **AND-tokenised** search across ad_name, campaign, ad_id, account, adset, category. Type `shubh active winner` to require all three tokens. The counter to the right shows `N / M matched` live.

#### Lifetime totals strip

Four tiles (`#freqTotals`) — always reflect the full filtered set:

```
Total ads                  (#freqTotAds)
Total lifetime spend       (#freqTotSpend)
Total lifetime reach       (#freqTotReach)
Total lifetime impressions (#freqTotImpr)
```

#### Frequency buckets

Six clickable cards (`#lifeFreqBuckets`) keyed by `max_cum_freq` (peak cumulative frequency the ad has ever reached):

```
< 1×          never served — no impressions
1 – 1.5×      crossed 1× but not 1.5×
1.5 – 2×      crossed 1.5× but not 2×
2 – 2.5×      crossed 2× but not 2.5×
2.5 – 3×     crossed 2.5× but not 3×
3×+           saturated reach — crossed 3×
```

Each card shows ad count + total spend + total reach for that bucket. Colour progresses grey → green → amber → red as frequency grows. Click any bucket to open the drill table.

#### Drill table (`#freqDrillTable`)

Appears below the buckets when a card is active. Sortable headers with visible arrows (↕ neutral, ↑ asc, ↓ desc). Sensible defaults per column family:

```
Column            Sort key         First-click direction
Ad name           ad_name          desc (Z-A)
Status            ad_status        desc
Max freq          max_cum_freq     desc (highest first)
Entered N×        depends on b     asc  (oldest crossing first)
Crossed 1×        d_1              asc  (chronological)
Crossed 1.5×      d_1_5            asc
Crossed 2×        d_2              asc
Crossed 2.5×      d_2_5            asc
Crossed 3×        d_3              asc
Lifetime spend    amount_spent     desc (top spend first)
Lifetime reach    reach            desc
```

Each `Crossed N×` cell shows `impressions / reach / spend on that day`. Cells for thresholds the ad never reached render as em-dashes and sort to the bottom.

The **Entered** column shows the date the ad entered its current bucket. Header re-labels dynamically (`Entered 1×`, `Entered 1.5×`, etc.) as you click through buckets.

`![Frequency drill](docs/screenshots/lifecycle-freq-drill.png)`

### 14-Day Buffer sub-view

Every ad at day-14 or later since creation. Meant for the daily "should we cut / keep / scale" review.

#### Verdict filter presets (`#lifeBufferStatusRow`)

```
All          Winners       Inc. Winners      P0 analysis
P1 analysis  P2 analysis   Discarded
```

Adjacent search input (`#lifeBufferSearch`).

#### Table (`#lifeBufferTbl`)

```
Column         Meaning
Ad name        + campaign_name underneath
Status         Meta ad_status pill
Created        ad_created (mono, YYYY-MM-DD)
Days since     integer days from ad_created to today
Verdict        category badge
F1..F4         individual pass ticks (✓ / ✗)
Spend          amount_spent (lifetime)
ROAS           roas_ma
FTEWV          ftewv_count
NCP            ncp_count
```

Sorted by spend desc. Top 500 shown.

---

## Section 3 — Ads Analyse

Row-level table with 60+ columns pulled from `ae_table_view`. Use this when you need a specific column, an ad-level export, or want to cross-check the KPI tiles.

`![Ads Analyse — overview](docs/screenshots/ae-overview.png)`

### Toolbar

```
Cross-check   (#aeBtnCrosscheck)   diff DB category vs current-threshold category
Refresh       (#aeBtnRefresh)      re-fetches ae_table_view (all 15k ads)
Export CSV    (#aeBtnExport)       exports current filtered slice
```

### Filter grid — row 1

Six controls:

```
Account       (#aeAcct)       ad-account dropdown (or All)
Group By      (#aeGroupBy)    Ad / Ad name / Campaign — aggregation level
Category      (#aeCategory)   the 6 tiers (or All)
Ad Status     (#aeStatus)     Active / Paused / Campaign paused / Archived / With issues
Date Field    (#aeDateField)  which date column the range filter applies to
Date Range    (#ae-drp-btn)   9 preset picker
```

**`Date Field` options** — crucial to understand, this is the source of most "why do I see 79 ads" confusion:

```
Delivery       (__delivery__)     ads that had impressions in the window
                                  (looked up via aeDeliverySet from primary_table)
Ad created     ad_created          when the ad object was created in Meta
First seen     first_seen_date     first day the ad reported ANY row
Result date    date_of_result      when Meta computed the tier verdict
Reporting end  reporting_ends      last day the ad had reporting data
```

Default is **Delivery** — this matches "ads that actually ran on those days" (622 for June 29, 2026, for example). `Reporting end` filters to ads whose last reporting day was that specific day (much narrower — only 79 for June 29).

### Filter grid — row 2

Thresholds and clear buttons:

```
F1 Imp             (#aeF1)             default 50,000
F2 ROAS            (#aeF2)             default 3
F3 C/NCP           (#aeF3)             default 525
F4 C/FTEWV         (#aeF4)             default 12
Clear Dates        (#aeClearDates)     resets both date inputs
Clear Filters      (#aeClearFilters)   resets everything to defaults + Delivery date field
```

### Multi-filter builder

Same pattern as Creative Testing (`#aeMfRows`). Chainable rules with AND semantics.

### Category KPI cards (`#aeCats`)

Six horizontal tiles, each with count + total spend. Clicking a tile pins that category as an active filter (highlight state). Click again to unpin.

```
Incremental Winner   Winner   P0 analysis   P1 analysis   P2 analysis   Discarded
```

An "Options" row underneath includes:

```
Show Discarded    (#aeShowDiscarded)  toggle — Discarded is hidden by default
Cross-check       (#aeCrossPanel)     panel — DB-category vs recomputed-category diff
```

### Main table (`#aeMain`)

Every column is sortable via the header (arrows visible). Pagination strip above and below shows row info + page-size picker (25 / 50 / 100 / 250 / 500 / 1000) + prev/next.

#### Identity columns

```
Preview        thumbnail from Meta ad-preview API (thumbsByAdId cache)
Campaign       campaign_name
Adset ID       adset_id
Ad ID          ad_id (17-digit Meta ID)
Ad Name        ad_name (bold)
Attribution    attribution_setting
Account        account_name
```

#### Lifecycle columns

```
Created           ad_created
First Seen        first_seen_date
F1 Hit Date       date_target_imp_achieved (first day cumulative impressions ≥ F1)
Result Date       date_of_result
Days to Result    result_date - first_seen_date
Days to F1        F1_hit_date - first_seen_date
Category          the 6-tier badge
F1 / F2 / F3 / F4 individual pass ticks
Ad Status         Meta ad_status pill
```

#### Delivery columns

```
Impressions        impressions
Reach              reach
Incr. Reach        latest daily reach - previous daily reach
Cost/Incr Reach    latest daily spend / incremental_reach
Reach Weight %     reach / global_reach * 100
Frequency          impressions / reach
Spend              amount_spent
Cost/1k            cost_per_1000 (CPM)
CPC Link           cpc_link
CTR %              ctr_pct
Link Clicks        link_clicks_raw
```

#### Funnel columns

```
ATC                add_to_cart events
ATC/LC %           add_to_cart / link_clicks
CI                 checkout_initiations
CI/ATC %           checkout_initiations / add_to_cart
Checkout %         checkout_completion_pct
CR/LC %            checkout / link_clicks
Purchases          purchases
Conv Value         conv_value (Meta-attributed)
ROAS               conv_value / spend
```

#### Shopify columns

```
Shopify Orders     from ae_shopify_enriched + shopify_ad_agg joins on ad_id
Shopify Sales      sum of matched order totals
```

#### Cost/quality columns

```
Cost/FTEWV         cost_per_ftewv
FTEWV              ftewv_count (first-time engaged video views)
% Reach FTEWV      ftewv_count / reach
Cost/NCP           cost_per_ncp
NCP                ncp_count (new customer purchases)
```

#### Efficiency columns (all normalised to global averages)

```
Profit Eff              conv_value - amount_spent
Contrib Margin %        (1 - spend/conv_value) * 100
Blended Eff             weighted composite: 10% CPR + 25% FTV Contrib + 15% FTEV Vol +
                        20% NCP Cost + 20% ROAS + 10% Profit Vol
Delivery Eff            CPR + FTV Contrib + Cost/FTEWV (delivery quality slice)
Sales-Spend Eff         NCP Cost + ROAS (return slice)
CPR Eff                 cost per reach vs global average (higher = cheaper reach)
FTV Contrib Eff         FTEWV rate on reach vs global
FTEV Volume             absolute FTEWV vs global median
NCP Cost Eff            cost per NCP vs global average
ROAS Eff                ROAS vs global average
Profit Vol Eff          profit vs global median
```

Every efficiency column is 1.00 = "same as global average". Higher is better across every metric.

#### Meta metadata

```
LTV Reach       ltv_reach
LTV Freq        ltv_frequency
Engagement      engagement_count
Preview link    Meta ad preview URL
Ad link         destination URL (landing page)
```

---

## Section 4 — Ad Intelligence

Shopify-order-level view. Every row is one Shopify order and shows how it was attributed back to a Meta ad via UTM matching.

`![Ad Intelligence — overview](docs/screenshots/adintel-overview.png)`

### Toolbar

```
Date Range       (#ai-drp-btn)      9 presets (default Last 30 Days)
Display toggle   (#aiDisplayToggle) Count (#) or Percentage (%) — swaps KPI display
Refresh          (#aiRefresh)       re-fetches shopify_ad_attribution
Export CSV       (#aiExport)        exports current filtered slice
```

### Channel KPI row (`#aiChannelRow`)

Five tiles:

```
Total Orders     everything in the window
Meta             utm_source in (meta / facebook / instagram / ig)
Google           utm_source in (google / adwords)
Retention        utm_source in (email / sms / whatsapp / klaviyo)
Other            everything else + no utm_source
```

Each tile shows count + percentage + sales. Click a tile to open the **channel drilldown** panel below, which breaks it down by exact `utm_source` value.

### Tier KPI row (`#aiTierCards`)

Six tiles showing which of the 5-step matching rules attributed the order:

```
Step 1  utm = ad_id                    strongest match
Step 2  utm = ad_name                  strong
Step 3  adset_name + ad_name superset  medium
Step 4  separator-normalised           weak
Step 5  multi-attribute fuzzy          weakest
Unmatched                              no rule fired
```

Each shows count + percentage + sales.

### Filter grid

Eight controls:

```
utm_source        (#aiUtmSourceBtn)  multi-select with search, All, None
utm_medium        (#aiUtmMedium)     dropdown (single)
matched_tier      (#aiTierSel)       Step 1-5, Any match, Unmatched
Ad status         (#aiAdStatus)      All / Active only / Inactive only
utm_campaign      (#aiUtmCampaign)   substring search
utm_content       (#aiUtmContent)    substring search
utm_term          (#aiUtmTerm)       substring search
matched_value     (#aiMatchedValue)  substring search
```

### Main table (`#aiTbl`)

Every column sortable via `data-aisort`:

```
Order Date       order_created_at (mono, DD-MMM)
Order ID         Shopify order name (e.g. #10025)
Total ₹          order total
Tier             matched_tier badge (Step 1..5 / Unmatched)
utm_source       raw UTM
utm_medium       raw UTM
utm_campaign     raw UTM
utm_content      raw UTM
utm_term         raw UTM
matched_value    the value the rule matched on (ad_id / ad_name / etc.)
Ad ID            matched Meta ad_id (empty if Unmatched)
Ad Name          matched ad_name
Campaign         matched campaign_name
Ad Status        matched Meta ad_status
```

Pagination strip (`#aiPager`): row info + page size (50 / 100 / 250 / 500 / 1000) + prev/next.

---

## Section 5 — Inventory

Shopify product catalogue with stock levels.

`![Inventory — overview](docs/screenshots/inventory-overview.png)`

### Toolbar

```
Search       (#invSearch)      substring — title, SKU, vendor
Refresh      (#invRefresh)     re-fetches Shopify product catalogue
Export CSV   (#invExport)      exports current filtered list
```

### Status filter chips (`#invStatusRow`)

```
All   Active   Draft   Archived
```

### Exclusion toggles

```
Exclude out-of-stock   (#invExcludeOOS)
Exclude no image       (#invExcludeNoImg)
```

### KPI tiles (`#invKpis`)

```
Total products    Active   Draft   Archived   Total variants   Out of stock
```

### Product table (`#invTbl`)

```
Column           Meaning
Image            48×48 product thumbnail from Shopify CDN
Title            product title
Status           ACTIVE / DRAFT / ARCHIVED pill
Vendor           product.vendor
Type             product.productType
Variants         count of variants
Total inventory  sum of variant inventory quantities
Price range      min - max variant price in ₹
Created          product.createdAt
```

Sticky header, hover highlight per row.

---

## Modals and drawers

### Definitions modal (`#defsModal`)

Opens from the `Definitions` toolbar button in Creative Testing. Explains F1–F4 thresholds and the category algorithm with a worked example table.

```
Header               category name
Threshold grid       F1/F2/F3/F4 with default target values
Algorithm prose      how the ordering works
Decision table       each category: condition, action
```

### Bucket drill modal (`#bucketModal`)

Opens from:
- Any funnel cell (Creative Type × Category intersection)
- Any Product Focus pill
- Any Creative Focus pill
- Any Landing Page Focus row

Contents:

```
Title             "Product in Focus — Collection page" or similar
Sort chips        Spend / ROAS / FTEWV / Newest
Limit chips       Top 15 / 50 / All
Trend chart       Creatives added per week
Spend chart       Top-15 spend + ROAS combo
Detail table      one row per ad in the selection
```

#### Detail table columns

```
Preview           thumbnail
Ad name + URL     bold ad_name, muted destination URL beneath (host + path)
Created           ad_created
Spend             amount_spent
ROAS              roas_ma
FTEWV             ftewv_count
NCP               ncp_count
Shop. orders      shopify_orders (looked up from ae_table_view by ad_id)
Status            category badge
Links             ↗ Landing (destination) + ◈ Preview (Meta preview)
```

Row click opens the **ad drawer**. Link chips open the raw URL in a new tab (click doesn't propagate).

`![Bucket drill modal](docs/screenshots/bucket-drill.png)`

### Category performers modal (`#modal`)

Opens by clicking any KPI tile in Creative Testing. Shows the top N ads for that category:

```
Top-N chips       10 / 20 / 50
Metric chips      Spend / ROAS / FTEWV / NCP  (any combination)
Bar chart         top-N by selected metric
Detail table      Preview, Ad, Spend, ROAS, FTEWV, NCP, Shopify orders, Status
```

### Ad drawer (`#drawer`)

Right-slide panel. Opens from a row click in any drill table.

```
Header            ad_name + campaign_name subtitle
Metrics grid      2-column dense grid: impressions, reach, freq, spend, ROAS,
                  cost per FTEWV, cost per NCP, category, etc.
Preview section   Meta ad preview URL button + rendered thumbnail;
                  fallback to Ads Library link if no thumbnail is cached
```

---

## Data pipeline — where the numbers come from

Every number in the dashboard resolves back to one of these views/tables:

```
Table / View                Purpose
primary_table               daily raw Meta insights, last 6-9 months
backfill_table              daily raw Meta insights, from launch (full history)
ae_raw_view                 union of primary + backfill + computed pass flags
ae_table_view               ae_raw_view + Shopify joins + efficiency columns + category
ae_reach_recent             per-ad latest daily reach + incremental delta
ae_freq_lifecycle_mat       per-ad crossing dates for 1×/1.5×/2×/2.5×/3×
ae_freq_lifecycle           thin view pointing at the mat table (frontend URL)
summary_table               per-ad lifetime aggregates, refreshed by refresh_summary_table.py
results_table               per-window aggregate cache (results_sync.py)
shopify_ad_attribution      one row per Shopify order + matched Meta ad (5-step rule)
shopify_ad_agg              per-ad aggregate of matched Shopify orders
ae_shopify_enriched         per-ad Shopify metrics + tier
```

Pipeline order (`backend/full_run.py`):

```
1. primary_sync.py daily       fetch yesterday's Meta insights
2. propagate_primary_to_backfill mirror primary into backfill
3. apply_ctp_unique_ids        deduplicate CTP variant ad IDs
4. refresh_ae_table            rebuild ae_raw_view, ae_table_view, ae_reach_recent
5. refresh_summary_table       rebuild summary_table
6. result_classifier           re-tag verdicts on new ads
7. results_sync                rebuild results_table cache
8. rebuild_attribution_orders  rebuild shopify_ad_attribution (5-step matching)
```

Typical full run is ~45 min end-to-end.

---

## FAQ and troubleshooting

**Q: I see a completely blank/frozen dashboard, no sections open.**
Almost always a JavaScript parse error. Open browser DevTools console. Look for a `SyntaxError` — usually a duplicate `const` in the same scope. Fix, hard-refresh (Ctrl+F5).

**Q: A section shows zero data even though the KPI tiles look right.**
That section's data source failed silently. Common cause: PostgREST statement timeout on a slow view. Check DevTools → Network tab for a 504 response. Solution: materialise the view (see `ae_freq_lifecycle_mat` for the pattern).

**Q: Why do I see 79 ads instead of 622 for a specific date?**
Your `Date Field` in Ads Analyse is set to `Reporting End`. That filters ads whose last reporting day was that specific day — a much narrower set. Change to `Delivery` to see "every ad that ran that day".

**Q: The `Total lifetime spend` tile says ₹36,88,11,570. Is this correct?**
Yes — this is `SUM(amount_spent)` from `ae_table_view` and matches the raw `backfill_table` lifetime sum to the paise. Every rupee ever billed on Meta across the three ad accounts since day one.

**Q: The Shopify orders column shows `—` in a drill table.**
Fixed as of `c02c1d9` — the drill now looks up `shopify_orders` from `ae_table_view` by `ad_id`. If it still shows `—`, hard-refresh to bypass the browser cache.

**Q: Meta ad status changed, but the dashboard still shows the old one.**
Meta status is baked into `ae_table_view` at the last pipeline run. Hit the toolbar `Refresh` button; the whole ad list re-fetches. If it's still stale, the pipeline itself hasn't run — check `backend/full_run.log` for the last run timestamp.

**Q: I want to change the category thresholds temporarily.**
Edit the F1/F2/F3/F4 fields in the toolbar. Ads recategorise live in the browser — no DB writes. To make it permanent, edit `aeCategorise()` in `index_v2.html`, and the CASE statement in `ae_table_view` (via a migration) so the DB-side categorisation matches.

**Q: I want to see how an ad's category was computed.**
Open the ad drawer (row click). The F1..F4 pass flags are shown individually. In Ads Analyse, the `F1 / F2 / F3 / F4` columns show individual ticks. The `Category` column is the ordered rule result.

---

_Last updated 2026-07-02 — matches commit `22e3d1e` (URL preview under ad name)._
