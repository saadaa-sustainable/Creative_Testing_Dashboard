# Meta Ads Direct — Google Sheet setup

Fresh Google Sheet that pulls Meta Ads data directly via Meta Graph API,
bypassing our Supabase pipeline entirely. Four tabs:

- `Active Ads 30d` — ACTIVE/paused ads with last-30d totals
- `Active Ads 90d` — same, last-90d
- `Daily Breakdown 30d` — one row per (ad × date), last 30 days
- `Daily Breakdown 90d` — same, last 90 days

Each tab auto-refreshes hourly via Google's server-side trigger (works
even when your laptop is off).

---

## Step 1 — Create the Google Sheet

1. https://sheets.new — creates an empty sheet.
2. Rename it (e.g. "Meta Ads Direct").
3. Menu bar → **Extensions → Apps Script** — opens the editor.
4. Delete the default `function myFunction()` boilerplate.
5. **File → Project settings** — copy the **Script ID** (long random string).

## Step 2 — Install clasp (one-time, per machine)

```
npm install -g @google/clasp
```

If Node isn't installed: https://nodejs.org (LTS).

## Step 3 — Authenticate clasp with your Google account

```
clasp login
```

Opens a browser tab. Log in with the same Google account that owns the sheet.
This writes `~/.clasprc.json` — you only do this once.

## Step 4 — Point clasp at your new sheet

Rename the template and paste your Script ID:

```
cd D:/Creative_Testing_Dashboard/backend/apps_script_meta_direct
cp .clasp.json.template .clasp.json
```

Open `.clasp.json` and replace `PASTE_YOUR_APPS_SCRIPT_ID_HERE` with the
Script ID you copied in Step 1.

## Step 5 — Push the code

```
clasp push
```

The `Code.gs` and `appsscript.json` files land in your Apps Script project.

## Step 6 — Add Script Properties (Meta token + account IDs)

In the Apps Script editor:

1. **Project Settings** (⚙ gear icon) → **Script Properties** → **Add script property**.
2. Add these keys (values from your `backend/.env`):

```
META_ACCESS_TOKEN       <the fresh long-lived Meta token>
META_API_VERSION        v22.0
ACCOUNT_1_ID            1136644150469466
ACCOUNT_1_NAME          Raho Saadaa
ACCOUNT_2_ID            1349767139294217
ACCOUNT_2_NAME          Fourth Ad Account - SD
ACCOUNT_3_ID            264868699479122
ACCOUNT_3_NAME          Third Ad Account - SD
```

### Optional — last-click UTM attribution overlay (Shopify sales)

If you also want `shopify_orders` + `shopify_sales` columns on each row
(fetched from Supabase's `shopify_ad_attribution` table), add these two
extra properties. Without them, the two shopify columns stay blank and
everything else still works.

```
SUPABASE_URL            https://<project-ref>.supabase.co
SUPABASE_ANON           <anon key from Supabase dashboard>
```

## Step 7 — Run the first refresh

1. Reload the sheet (Ctrl+R) — menu bar now shows **"Meta Direct"**.
2. **Meta Direct → Refresh ALL four tabs** — takes 1-3 min.
3. Grant the OAuth consent when prompted (Apps Script needs permission to
   call external URLs — the Meta Graph API).

Sheet tabs get populated with data. If you see "No rows returned", the
Meta token is either invalid or missing — check Script Properties.

## Step 8 — Install hourly auto-refresh

**Meta Direct → Install hourly triggers** — schedules all four refreshes
to fire hourly on Google's servers. Sheet stays fresh forever without
your laptop being on.

To stop: **Meta Direct → Uninstall triggers**.

---

## Pushing code updates

Any time you edit `Code.gs` locally, just:

```
cd D:/Creative_Testing_Dashboard/backend/apps_script_meta_direct
clasp push
```

That syncs your local file to the sheet's Apps Script project.

## Troubleshooting

- **"Cannot read property token"** → META_ACCESS_TOKEN missing from Script Properties.
- **Meta HTTP 400** → token expired OR insufficient permissions on the ad account.
- **Meta HTTP 500 / rate limit** → Meta app quota hit; wait an hour and retry.
- **Sheet shows old data after hourly trigger fires** → check View → Executions in the editor for error logs.
