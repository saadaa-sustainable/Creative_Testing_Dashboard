# UTM Last-Click Attribution — Matching Logic

## What we're doing

Every Shopify order carries a "click history" in its URL parameters (UTMs) — the ad the customer last clicked before buying. We use those UTMs to point each order back to the Meta ad that earned it. **Last click, one order → one ad.**

Each order carries three URL fields we care about:

| Field         | What it should hold                              |
| ------------- | ------------------------------------------------ |
| `utm_campaign`| The campaign name or campaign ID                 |
| `utm_term`    | The adset ID (Meta usually puts a numeric ID here) |
| `utm_content` | The ad — either its numeric `ad_id` or `ad_name`  |

---

## The Cascade — 5 steps, first hit wins

Every order is walked through these five steps in order. As soon as a step matches, attribution is locked and we stop.

### **Step 0 — Manual Overrides (highest priority)**
If the utm_content contains a phrase we've mapped by hand (e.g. `DIVYAYRIAC` → this specific active ad), we route the order to that ad immediately. Used for creative-rename fixes we can't recover through pattern matching. **~13,000 orders currently carry a manual marker.**

### **Step 1 — Direct Ad ID Match (most trustworthy)**
`utm_content` is a numeric ad_id (like `120233708339810431`) that matches an ad exactly. This means the Meta URL template used `{{ad.id}}` — the gold standard. Unambiguous, one ad, one match.

### **Step 2 — Global Ad Name Match**
`utm_content` isn't a numeric ID but matches an ad's name globally (exact, fuzzy, or substring). This happens when the URL template used `{{ad.name}}` instead of `{{ad.id}}`. If exactly one ad in the whole account has this name → win. If multiple share the name (clones), we prefer the ad with more lifetime spend.

### **Step 3 — Adset Scope + Ad Identifier (any of three methods)**
`utm_term` names a known adset. Within that adset, we identify the specific ad by one of:
  1. **Asset ID substring** — utm_content contains a user-managed creative code (e.g. `Sep-682`, `SIF-4442-P1`, `glam.khush`). Asset ID is unique per creative and rename-stable, so this is reliable even after ad_name changes.
  2. **Historical name match** — utm_content matches any name the ad has EVER had (renames handled).
  3. **Fuzzy / substring / token match** — utm_content roughly matches an ad_name within the adset.

If exactly one candidate emerges → attribute. If multiple → we split by spend weight (see "Special Cases" below).

### **Step 4 — Campaign Scope Only**
`utm_term` doesn't match any adset, but `utm_campaign` matches a campaign. We can identify the campaign but not the specific ad. Weakest match — used sparingly.

### **Step 5 — Unmatched**
Order has UTMs but none resolve to a known Meta ad. Typically: direct traffic, organic search, non-Meta channels, or orders whose Meta URL macro failed to expand (`{{campaign.name}}` literal in the URL).

---

## Sign Convention You'll See in Reports

| Column                | Meaning                                                                            |
| --------------------- | ---------------------------------------------------------------------------------- |
| `shopify_sales`       | ₹ from orders our engine attributed to this ad                                     |
| `conv_value`          | ₹ Meta's own pixel attributed to this ad (usually higher — includes view-through) |
| `shop_minus_meta`     | `shopify − conv`. **Positive** = Shopify sees more. **Negative** = Meta over-reports. |
| `shop_vs_meta_pct`    | Same as above as a percentage of Meta's conv value                                 |
| `matched_tier`        | Which of Steps 1–5 fired for the order (or the ad's most-common tier)              |
| `matched_value`       | The exact string that fired the match (ad_id, ad_name, or asset code)              |

---

## Special Cases We Handle

**Same-creative clones (2+ ads with identical name and asset_id):**
Instead of a coin flip, we split the orders across the clones proportional to their lifetime Meta spend. The higher-spending clone gets the larger share of orders. Marked internally as `spend-weight-hashed`.

**Renamed creatives (e.g. MEGHA → DIVYAYRIAC on 22-Apr-2025):**
Orders placed after the rename date are re-routed to the new ad's name; orders before the rename stay with the original. Historical names are stored per ad so future re-attribution runs pick them up automatically.

**URL macro out of sync with ad name:**
Sometimes the URL template emits a short creative slug (e.g. `FoundersAd_131024`) but the ad's actual name is longer (`HP+IGP+OFF-RS+IHP+FOU_131024_W`). We link the slug → the correct ad via manual override, and re-attribute the orphaned orders.

**Missing utm_term (null adset):**
If utm_content is a known ad_id → attribute via Step 1 anyway. If utm_content is a name-only string with a known unique match → Step 2. Otherwise we route via manual override or leave unmatched.

---

## What Makes This Reliable

- **Step 1 is unbeatable** — direct numeric ID match. Any ad whose URL template uses `{{ad.id}}` never suffers ambiguity.
- **Step 3 asset method** covers the gap for ads whose URL template uses `{{ad.name}}` and ends up carrying a slug — as long as we've registered the asset code.
- **Manual markers** protect ~13,000 orders across 7 fix categories from being clobbered by re-attribution runs.
- **The `ON CONFLICT DO NOTHING` insert semantics** in the fetcher means new order data can only ADD attribution, never overwrite existing decisions.

---

## What Weakens It

- **Meta URL templates that use `{{ad.name}}` instead of `{{ad.id}}`** — every rename of the ad breaks the last-click link. Currently 127 ads in the last 30 days are emitting names/slugs instead of IDs. Fixing these at the source (Meta Ads Manager) removes most attribution ambiguity.
- **Multi-clone creatives** — when 2+ ads share a name AND asset ID, we can only best-effort split by spend. The true click-owner is unrecoverable without a `{{ad.id}}` macro.
- **`utm_term` missing on the order** — kills the adset-scoped Step 3 fast path; we fall back to global name lookup or leave unmatched.

---

*One-pager · UTM Last-Click Attribution · Saadaa Creative Testing Dashboard · 2026-07-28*
