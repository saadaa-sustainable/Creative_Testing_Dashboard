# Deploying the Creative Testing Dashboard

> **Goal**: single URL like `https://dashboard.saadaa.in` serves both the frontend and the backend. No `?apiBase=` or `?supabaseUrl=` query params needed. Nightly refresh keeps running on your workstation (or wherever the orchestrator lives).

## Architecture after deploy

```
        Your browser
              │
              ▼
   https://dashboard.saadaa.in
              │
              ▼  serves both static files + API
     ┌────────────────────┐
     │  FastAPI backend   │  (Render / Fly / Cloud Run / VPS)
     │  api_ae.py         │
     └────────┬───────────┘
              │  psycopg2 pool
              ▼
     ┌────────────────────┐
     │  Supabase Postgres │
     │  (already in prod) │
     └────────────────────┘

 Separately, on your workstation:
   nightly cron → _refresh_all_dashboard_data.py
                    → POST /api/cache/invalidate after refresh
```

The nightly orchestrator does NOT need to run on the same box as the web server. It just needs a Postgres connection to Supabase and an HTTP hop to the web server's cache-invalidate endpoint.

## Recommended hosting: Render.com

Best for this workload: free tier for testing, ~$7/mo for prod-quality, has Singapore region (~40 ms latency from India), auto-deploys on git push, no DevOps knowledge required.

**Alternatives worth considering** (skip if Render works):
- **Fly.io** — cheaper ($3-5/mo), needs `flyctl` CLI, more regions
- **Google Cloud Run** — scales to zero (cheap when idle), Mumbai region, containerized
- **Hetzner CX11 VPS** — €3.79/mo, most control, requires nginx/systemd setup

## Prerequisites

1. Your code is pushed to a GitHub repo (already true — `saadaa-sustainable/Creative_Testing_Dashboard`)
2. Supabase project is running (already true)
3. `.env` file has `SUPABASE_DB_URL` set (already true locally)

## Step-by-step: Render deploy

### 1. Sign in to Render

Go to https://render.com → sign in with GitHub → authorize access to `saadaa-sustainable/Creative_Testing_Dashboard`.

### 2. Create a new Web Service

- Dashboard → **New +** → **Web Service** → pick the repo
- **Name**: `ctd-backend` (this becomes `ctd-backend.onrender.com`)
- **Region**: **Singapore** (closest to India)
- **Branch**: `master`
- **Root Directory**: leave blank
- **Runtime**: **Docker** — Render sees `backend/Dockerfile` and uses it
- **Dockerfile path**: `backend/Dockerfile`
- **Docker Context**: leave blank (uses repo root — matches `COPY backend/...` paths)

### 3. Environment variables

Under the **Environment** tab, add:

```
SUPABASE_DB_URL       postgresql://postgres.<project>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres
META_ACCESS_TOKEN     (optional — only needed if you want ad-hoc Meta calls from the deployed backend)
SHOPIFY_ACCESS_TOKEN  (optional — same)
```

You **must** use the **Supabase Session Pooler** URL (`aws-0-...:5432`), not the direct-connection URL (`db.<project>.supabase.co:5432`) — Render's outbound IPs aren't allowlisted for direct connections on the free tier.

Find the pooler URL: Supabase dashboard → Project Settings → Database → **Connection string** → **Session pooler**.

### 4. Plan

- **Free** — spins down after 15 min idle, ~30s cold start. Fine for a dashboard used a few times a day. **Recommended to start.**
- **Starter ($7/mo)** — always on, 512 MB RAM, no cold starts. Upgrade once the team relies on it daily.

### 5. Deploy

Click **Create Web Service** — Render pulls, builds (~5 min first time, ~2 min after), starts container, runs healthcheck.

Once green: open `https://ctd-backend.onrender.com` → dashboard should load immediately, no URL params needed. Section URLs like `https://ctd-backend.onrender.com/ads-analyse` work out of the box.

### 6. (Optional) Custom domain

Under **Settings → Custom Domains**:

- Add `dashboard.saadaa.in`
- Render shows a CNAME record to add to your DNS provider — usually `dashboard.saadaa.in CNAME ctd-backend.onrender.com`
- Wait 5-30 min for DNS propagation
- Render auto-provisions a Let's Encrypt SSL cert

Now users visit `https://dashboard.saadaa.in/ads-analyse` — clean.

## Point the nightly orchestrator at the deployed backend

Once deployed, add one line to your local `.env` on the workstation running the orchestrator:

```
API_BASE=https://dashboard.saadaa.in
```

(Or `https://ctd-backend.onrender.com` if you skipped the custom domain.)

The orchestrator's cache-invalidate call already reads `API_BASE` from env (see `_refresh_all_dashboard_data.py` — the block that POSTs to `/api/cache/invalidate` after refresh_meta_direct_views). Once set, every nightly run flushes the deployed cache so morning users see fresh data.

## Verifying deployment

After deploy completes, run this from any machine:

```bash
curl -s https://dashboard.saadaa.in/api/health
# → {"ok":true,"db":true,"ts":1234567890}

curl -sI https://dashboard.saadaa.in/api/ads | grep -iE "x-cache|cache-control"
# → x-cache: MISS
# → cache-control: public, max-age=300, stale-while-revalidate=60

curl -s https://dashboard.saadaa.in/api/cache/stats
# → {"entries":1,"hits":0,"misses":1,...}

curl -sI https://dashboard.saadaa.in/ads-analyse | grep -i "content-type"
# → content-type: text/html; charset=utf-8   ← same HTML for every section
```

If any of these fail, check the Render logs (dashboard → Logs tab).

## Common gotchas

### 1. Supabase blocks the Render IP

Symptom: `/api/health` shows `{"ok":true,"db":false,...}` or the browser dashboard shows empty tables.

Fix: use the **Session Pooler** URL (`aws-0-...`) not the direct-connect URL. The pooler is public and doesn't check IPs. If you truly need direct connect, Supabase → Project Settings → Database → Connection pooler → add Render's outbound IPs (they're documented per plan; free tier has ~4 rotating IPs).

### 2. Build fails with "psycopg2 not found"

Symptom: build log ends with `pg_config not found` or similar.

Fix: `requirements.web.txt` uses `psycopg2-binary` (not `psycopg2`), which is prebuilt for Linux. This should already work — if you see this error, verify the file has the `-binary` suffix.

### 3. Frontend loads but data doesn't

Open browser devtools → Network tab → click one of the failing requests.
- **CORS error** — shouldn't happen post-deploy since everything is same-origin. If you see this, something is off with `window.location.origin` detection. Force refresh (Ctrl+Shift+R) to bypass browser cache of the old dashboard.js.
- **404 on `/rest/v1/…`** — the PostgREST proxy in FastAPI is guarded by an `_ALLOWED_TABLES` set. If you added a new table locally and forgot to add it to that set, requests will 404. Check `backend/api_ae.py:_ALLOWED_TABLES`.
- **500 with statement_timeout** — Supabase pooler's role-level timeout. This won't affect the dashboard (queries are short), but heavy admin queries fail. Not urgent.

### 4. Cache is stale for hours after nightly refresh

Symptom: morning users see Aug 24 data even though the orchestrator ran overnight and Aug 25 is in Supabase.

Fix: check that `API_BASE` is set on the workstation running the orchestrator. The cache-invalidate call fails silently if the env var is missing — check the run log for `-- cache invalidate skipped`. Set the env var and re-run the orchestrator (or wait for the 15-min TTL to expire naturally).

## Cutover — what changes for users

**Before deploy**, the shared bookmark was something like:
```
file:///D:/Creative_Testing_Dashboard/index_v2.html?apiBase=http://localhost:8000
```
or
```
https://saadaa-dashboard.netlify.app/?supabaseUrl=https://xxx.supabase.co&supabaseAnon=eyJ…
```

**After deploy**, everyone just uses:
```
https://dashboard.saadaa.in                    (home)
https://dashboard.saadaa.in/ads-analyse       (jump straight to a section)
https://dashboard.saadaa.in/landing-page
https://dashboard.saadaa.in/incremental-analysis
```

Send the team the new URL. Old URLs still work if anyone has them bookmarked — the `?apiBase=` and `?supabaseUrl=` code paths are preserved for backwards compat.

## When to upgrade to a paid plan

- **Free tier is fine** for: personal use, 1-3 team members, dashboard opened a few times a day
- **Upgrade to Starter ($7/mo)** when: cold-start delay bothers users (dashboard is opened rarely enough that Render spins the container down), OR you have >5 daily users
- **Upgrade to Pro ($25/mo)** when: you need multi-region deploy, more RAM for larger datasets, or SLA guarantees

## Undeploy / rollback

- **Rollback to previous build** — Render dashboard → Deploys tab → click old deploy → **Redeploy**. Takes ~30s.
- **Emergency stop** — Settings → Suspend Service. Costs nothing while suspended.
- **Full teardown** — Settings → Delete Service. Frees the name so you can re-create later.

## What we didn't cover

- **CI/CD via GitHub Actions** — Render auto-deploys on `git push`, so this is mostly not needed. Only add Actions if you want pre-deploy test runs.
- **Multi-region failover** — not applicable at this scale. If Singapore Render region goes down, cutover manually.
- **Rate limiting** — no need with team-only access. Add if you expose the API publicly.
- **Auth / login screen** — currently the dashboard is open to anyone with the URL. Add a Google OAuth wall (via FastAPI + `authlib`) if you're worried about external eyes on the data.
