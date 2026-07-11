# Google Ads API — setup (gcloud + developer token)

You don't need to share OAuth client credentials. The pipeline uses:
- your **developer token** from `.env`
- **your own Google account** via gcloud Application Default Credentials

That's it. No client_id/secret/refresh_token to manage.

## One-time setup

1. Add just this line to `backend/.env`:

   ```
   GOOGLE_ADS_DEVELOPER_TOKEN=<the token you were shared>
   ```

   Optional (only if you have MCC access):

   ```
   GOOGLE_ADS_LOGIN_CUSTOMER_ID=<10-digit MCC id, no dashes>
   ```

2. Grant Google Ads scope to your gcloud login. This opens a browser once:

   ```
   gcloud auth application-default login --scopes=https://www.googleapis.com/auth/adwords,https://www.googleapis.com/auth/cloud-platform,openid,https://www.googleapis.com/auth/userinfo.email
   ```

   > `cloud-platform` is required by gcloud even though the Ads API
   > only reads via the `adwords` scope. Without it the command
   > rejects with "cloud-platform scope is required".
   >
   > Google is deprecating the `adwords` scope on gcloud's built-in
   > OAuth client. If a future run of this command fails with
   > "scopes will be blocked", create a client ID for yourself under
   > **Google Cloud Console → APIs & Services → Credentials → Create
   > OAuth client ID (Desktop app)**, download the JSON, and point
   > gcloud at it once via `gcloud auth application-default login
   > --client-id-file=<path>`. Nothing changes downstream — the ADC
   > file will still hold the resulting refresh token in the same
   > location the scripts read from.

3. Verify the credentials reach the API:

   ```
   python backend/_probe_google_ads_connect.py
   ```

   It prints your accessible customer(s) and exits. Nothing is written to
   any database.

## Daily fetch

Once the probe passes:

```
python backend/fetch_google_ads_daily.py --dry-run          # fetch summary, no DB writes
python backend/fetch_google_ads_daily.py --since 2025-01-01 # initial backfill
python backend/fetch_google_ads_daily.py                    # daily incremental (default)
```

Grain: one row per (customer_id, ad_id, date). Metrics use Google Ads'
native last-click attribution (the `conversions` / `conversions_value`
columns). Table `public.google_ads_daily` is auto-created on first run.

## Refreshing OAuth

gcloud refresh tokens are long-lived but not forever. If a run fails with
`invalid_grant` or `insufficient authentication scopes`, redo step 2 in the
one-time setup above.
