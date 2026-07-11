# Google Ads API — MCC account setup

Add these lines to `backend/.env`. Every value comes from your Google Ads
Manager (MCC) console; nothing here is invented.

```
# Google Ads API v18+
# Get this from  https://developers.google.com/google-ads/api/docs/first-call/dev-token
GOOGLE_ADS_DEVELOPER_TOKEN=

# OAuth2 client credentials — Google Cloud Console → APIs & Services →
# Credentials → OAuth 2.0 Client IDs → download JSON, copy client_id + secret.
GOOGLE_ADS_CLIENT_ID=
GOOGLE_ADS_CLIENT_SECRET=

# Long-lived refresh token — one-time flow, easiest via the google-ads-python
# generate_user_credentials.py helper, or `oauth2l fetch --scope adwords`.
GOOGLE_ADS_REFRESH_TOKEN=

# MCC account ID (10 digits, no dashes) — the manager that owns the sub-accounts
# you want to query. Every request will be routed through this account.
GOOGLE_ADS_LOGIN_CUSTOMER_ID=

# Optional — a specific customer_id to query first. Leave blank to auto-list
# every child account under the MCC.
GOOGLE_ADS_CUSTOMER_ID=
```

## Verifying

Once the env vars are in place, run:

```
python backend/_probe_google_ads_connect.py
```

It prints the MCC's child accounts. No data is written to Supabase.

## Pipeline integration

After the probe passes, `fetch_google_ads_daily.py` will pull one day of
ad-level performance data (impressions, clicks, cost, conversions,
conversion value) for every child account and upsert into
`public.google_ads_daily` in the Meta Ads Supabase project. Attribution
is Google Ads' native last-click model (the "conversions" column with
default attribution).

The fetcher will be added to `_run_full_update.py` after the initial
run confirms the schema is what we want.
