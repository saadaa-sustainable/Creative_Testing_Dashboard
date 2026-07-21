# Last Click UTM Analysis — How Attribution Works

**Audience:** non-technical stakeholders (marketing team, founders, analysts).
**Purpose:** explain in plain language how the dashboard decides which ad every Shopify order came from.

---

## The problem

When someone buys on your Shopify store, they arrive with a UTM tag in the URL:

```
utm_source   = google | meta | ig | facebook | ...
utm_medium   = paid | social | email | ...
utm_campaign = the campaign name or ID
utm_term     = usually the adset / adgroup ID
utm_content  = usually the ad ID or the ad name
```

Meta and Google build these tags automatically when your ad runs, but the format varies by:
- who set it up (some templates put the ad ID, others put the ad name)
- whether the ad was renamed later
- whether the platform is Meta or Google (each uses different naming conventions)
- whether the campaign is a special type (Advantage+ shopping, Performance Max)

The dashboard's job is to **look at each order's UTM tags, hand them to a matching engine, and figure out which specific ad the customer clicked**.

If we can identify the exact ad → order is "matched" and rolls up into that ad's ROAS.
If we can only identify the adset / campaign → order is "partially matched" and rolls up at that group level.
If we can't identify anything → order is "unmatched".

---

## The 5-step cascade — Meta orders

For every Meta / Facebook / Instagram order, the engine tries these steps in order. The **first step that succeeds** wins:

### Step 1 — Direct ID match
> "The `utm_content` is a numeric ad ID that exists in our ad library."

Cleanest possible match. Order is attributed to that exact ad with 100% confidence.
Example: `utm_content = 120238454531840431` → find that ad, done.

### Step 2 — Ad name match (global)
> "The `utm_content` matches the name of an ad we know."

Tries the exact ad name first, then a "fuzzy" match (ignores case, spacing, `_h0` / `_copy` suffixes), then a substring / separator-tolerant match if there's exactly one clear winner. When multiple ads share the same name fragment, the engine picks the one with **higher lifetime spend** (Meta actually delivered it).

Example: `utm_content = CLP-SMFLK+IFAD+NA+IHP+-1PA-SIF-1419-P1-vinitsikariya_16/12/25_H0`
        → matches exactly one ad in the whole library → matched.

### Step 3 — Adset-scoped match
> "The `utm_term` is an adset ID we know. Within that adset, narrow down to the specific ad by name."

Meta puts the adset ID in `utm_term`. Once we know the adset, the search space shrinks from 15,000 ads to 3–10 ads. Within that adset, the engine tries:
- Exact ad-name match
- Fuzzy match (suffix + case tolerant)
- Substring match (case-insensitive)
- Separator-tolerant substring (treats `+`, `-`, `_`, `.`, `/`, space as interchangeable)
- **Token-subset match** (all utm tokens must appear in the ad name — enables cases where the utm has fewer tokens than the full ad name)
- **Ratio-tiebreak** (when multiple ads pass the token check, pick the ad whose historical name most tightly matches — highest overlap ratio with a clear margin over the runner-up)

Example: `utm_term = 120238454413510431` + `utm_content = SMFLK_IFAD-vinitsikariya_16/12/25`
        → 3 ads in the adset, only one has "vinitsikariya" → matched to that ad.

If we can identify the adset but not a specific ad, the order is recorded as "Step 3 (adset only)" — attributed at the adset level, not to any one creative.

### Step 4 — Campaign-scoped match
> "The `utm_campaign` is a campaign ID we know. Within that campaign, narrow down to the specific ad by name."

Same idea as Step 3 but at the campaign level (wider scope, more ads to search). Uses the same name-matching cascade.

If we can identify the campaign but not a specific ad, the order is "Step 4 (campaign only)" — attributed at the campaign level.

### Step 5 — Unmatched
> "None of the above worked."

Nothing in the UTMs matched any known Meta ad. Order is left unmatched — visible as "Unmatched" on the dashboard, no ad ROAS credit given.

Common reasons:
- UTM tags are missing or malformed
- The customer came via WhatsApp click-to-message ads (no destination URL)
- The ad was deleted long ago (> 37 months) and Meta doesn't retain it any more
- The utm_source isn't Meta at all (Google, direct, email, retention tools go through separate engines)

---

## The 5-step cascade — Google orders

Google Ads uses a different URL template. Their default puts:

```
utm_campaign = {campaignid}    (numeric)
utm_term     = {adgroupid}     (numeric)
utm_content  = {creative}      (numeric ad ID OR the ad's short name)
```

Same principle — try each step in priority order:

### Step 1 — Ad ID match
> "The `utm_content` is a numeric Google ad ID we know."

Direct match. Attributed to that exact ad.

### Step 2 — Campaign ID match
> "The `utm_campaign` is a numeric Google campaign ID we know."

Google's auto-tagging often sends the campaign ID but not the ad ID (Performance Max, some Search formats). Matched at campaign scope.

### Step 3 — Adgroup name match
> "The `utm_term` matches an adgroup ID we know AND the utm_content narrows within it."

Similar to Meta's Step 3 but at Google's adgroup level.

### Step 4 — Campaign name match
> "The `utm_campaign` string matches a Google campaign name (not ID) we know."

Fallback for orders that didn't come through auto-tagging.

### Step 5 — Unmatched
> "None of the above worked."

Nothing matched. Common causes:
- Organic Google (utm_source=google but no paid campaign attached)
- Sagemailer / other Google-branded tools that piggyback on the source name
- Old / deleted campaigns not in our current fetch

---

## Priority — matched wins over specific

If Step 1 succeeds, we don't try Step 2. If Step 3 succeeds at the specific-ad level, we don't fall back to campaign-scope. **Earlier / more specific matches always win.**

If Step 3 succeeds only at the adset scope (no specific ad narrowed), the order still counts as "Step 3" — but ad-level ROAS won't credit any specific creative. The KPI cascade shows it in Step 3's count so the drop-off between "matched to a specific ad" (Step 1 + Step 2) and "matched to a group" (Step 3 + Step 4) is visible.

---

## What "unmatched" actually costs you

Unmatched orders aren't lost revenue — they're just orders whose ad-of-origin we couldn't prove. Common reality checks:

| Unmatched cause | Typical rate | Action |
|---|---|---|
| Blank UTMs (direct / bookmark / mobile app) | 15–20% | Nothing — customer typed the URL directly |
| Old ad long-since deleted | 1–3% | Fetch archived-ads regularly (already automated) |
| WhatsApp click-to-message ad | 1–2% | Meta doesn't provide a destination URL for these |
| UTM template misconfigured on Google side | varies | Check Google Ads URL template — should use `{campaignid}` |
| Novel utm_source we haven't classified | small | Update classification rules |

The unmatched pool is monitored — a sudden spike usually means a template broke or a new tool started sending traffic with a new utm_source.

---

## Cascade summary — one glance

```
                    ORDER ARRIVES
                    with UTM tags
                          │
                          ▼
              ┌─────────────────────────┐
              │  Is utm_source Meta,    │
              │  Google, or something   │
              │  else?                  │
              └─────┬───────┬───────────┘
                    │       │
              Meta  ▼       ▼  Google
      ┌─────────────────┐   ┌─────────────────┐
      │ Step 1: ad_id   │   │ Step 1: ad_id   │
      │ Step 2: ad_name │   │ Step 2: camp_id │
      │ Step 3: adset   │   │ Step 3: adgroup │
      │ Step 4: campgn  │   │ Step 4: camp_nm │
      │ Step 5: Unmatch │   │ Step 5: Unmatch │
      └─────────────────┘   └─────────────────┘
                    │       │
                    └───┬───┘
                        ▼
                  ATTRIBUTED
                  (or unmatched)
                        │
                        ▼
              Shows up in dashboard's
              Last Click UTM view
              (matched_tier column)
```

---

## Where you see this on the dashboard

**Last Click UTM Analysis** page (in the sidebar under Analysis)

- **Meta / Google toggle** at the top switches between the two cascades. Cards show `Step 1`, `Step 2`, `Step 3`, `Step 4`, `Unmatched` — counts and sales for each.
- **Channel row above the tier cards** breaks orders by channel: Meta, Google, Retention, Organic, etc. Click a channel to filter down.
- **Orders table below** shows every order with its `matched_tier`, the exact ad / adset / campaign that was resolved, and the underlying UTM tags for debugging.
- **CSV export** dumps the currently-filtered order list — useful for spot-checking a specific creator, campaign, or tier.

If you spot an order that should have matched but shows up as unmatched — the token-subset + ratio-tiebreak rules in Step 3/4 are the ones that most often need refinement. Share the order ID + expected ad ID and the engine can be tuned to catch that pattern in future runs.
