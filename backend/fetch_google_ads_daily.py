"""
fetch_google_ads_daily.py — pull daily ad-level performance from every
child account under the MCC, upsert into public.google_ads_daily.

Grain per row: (customer_id, ad_id, date). Metrics use Google Ads'
native last-click attribution (the "conversions" column with default
attribution settings).

USAGE
  python fetch_google_ads_daily.py                 # incremental (max(date)+1 → today, 3-day overlap)
  python fetch_google_ads_daily.py --since 2025-01-01
  python fetch_google_ads_daily.py --until 2026-07-10
  python fetch_google_ads_daily.py --customer 1234567890   # single sub-account
  python fetch_google_ads_daily.py --dry-run       # fetch, print summary, skip DB write

ENV (backend/.env — see GOOGLE_ADS_SETUP.md)
  GOOGLE_ADS_DEVELOPER_TOKEN
  GOOGLE_ADS_CLIENT_ID / CLIENT_SECRET / REFRESH_TOKEN
  GOOGLE_ADS_LOGIN_CUSTOMER_ID     (the MCC)
  SUPABASE_DB_URL                  (the Meta Ads project — reused)
"""
import os, sys, time, argparse
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DEV_TOKEN   = (os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip()
CLIENT_ID   = (os.environ.get("GOOGLE_ADS_CLIENT_ID") or "").strip()
CLIENT_SEC  = (os.environ.get("GOOGLE_ADS_CLIENT_SECRET") or "").strip()
REFRESH_TOK = (os.environ.get("GOOGLE_ADS_REFRESH_TOKEN") or "").strip()
MCC_ID      = (os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or "").strip()
DB_URL      = (os.environ.get("SUPABASE_DB_URL") or "").strip()

for name, v in [("GOOGLE_ADS_DEVELOPER_TOKEN", DEV_TOKEN),
                ("GOOGLE_ADS_CLIENT_ID", CLIENT_ID),
                ("GOOGLE_ADS_CLIENT_SECRET", CLIENT_SEC),
                ("GOOGLE_ADS_REFRESH_TOKEN", REFRESH_TOK),
                ("GOOGLE_ADS_LOGIN_CUSTOMER_ID", MCC_ID),
                ("SUPABASE_DB_URL", DB_URL)]:
    if not v: sys.exit(f"Missing {name} in .env — see backend/GOOGLE_ADS_SETUP.md")

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError:
    sys.exit("google-ads not installed. Run: pip install google-ads")

import psycopg2
from psycopg2.extras import execute_values

# ── Config ────────────────────────────────────────────────────────────
GAQL_ACCOUNTS = """
  SELECT customer_client.id, customer_client.descriptive_name,
         customer_client.currency_code, customer_client.manager,
         customer_client.status
  FROM customer_client
  WHERE customer_client.status = 'ENABLED'
    AND customer_client.manager = FALSE
"""

# Ad-level performance query. Google Ads GAQL — "ad_group_ad" is the
# ad-serving entity. All numeric metrics come from the default
# last-click attribution model.
GAQL_AD_PERF = """
  SELECT
    segments.date,
    ad_group_ad.ad.id,
    ad_group_ad.ad.name,
    ad_group.id,
    ad_group.name,
    campaign.id,
    campaign.name,
    ad_group_ad.status,
    ad_group_ad.ad.type,
    metrics.impressions,
    metrics.clicks,
    metrics.cost_micros,
    metrics.conversions,
    metrics.conversions_value,
    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.video_views,
    metrics.engagements
  FROM ad_group_ad
  WHERE segments.date BETWEEN '{from_d}' AND '{until_d}'
"""

# ── DB helpers ────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS public.google_ads_daily (
  customer_id       BIGINT      NOT NULL,
  customer_name     TEXT,
  currency_code     TEXT,
  date              DATE        NOT NULL,
  campaign_id       BIGINT,
  campaign_name     TEXT,
  ad_group_id       BIGINT,
  ad_group_name     TEXT,
  ad_id             BIGINT      NOT NULL,
  ad_name           TEXT,
  ad_status         TEXT,
  ad_type           TEXT,
  impressions       BIGINT,
  clicks            BIGINT,
  cost              NUMERIC(14,2),
  conversions       NUMERIC(14,3),
  conversion_value  NUMERIC(14,2),
  all_conversions       NUMERIC(14,3),
  all_conversions_value NUMERIC(14,2),
  video_views       BIGINT,
  engagements       BIGINT,
  synced_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (customer_id, ad_id, date)
);
CREATE INDEX IF NOT EXISTS idx_google_ads_daily_date ON public.google_ads_daily(date);
CREATE INDEX IF NOT EXISTS idx_google_ads_daily_campaign ON public.google_ads_daily(campaign_id);
"""

UPSERT = """
INSERT INTO public.google_ads_daily (
  customer_id, customer_name, currency_code, date,
  campaign_id, campaign_name, ad_group_id, ad_group_name,
  ad_id, ad_name, ad_status, ad_type,
  impressions, clicks, cost, conversions, conversion_value,
  all_conversions, all_conversions_value, video_views, engagements,
  synced_at
) VALUES %s
ON CONFLICT (customer_id, ad_id, date) DO UPDATE SET
  customer_name         = EXCLUDED.customer_name,
  currency_code         = EXCLUDED.currency_code,
  campaign_id           = EXCLUDED.campaign_id,
  campaign_name         = EXCLUDED.campaign_name,
  ad_group_id           = EXCLUDED.ad_group_id,
  ad_group_name         = EXCLUDED.ad_group_name,
  ad_name               = EXCLUDED.ad_name,
  ad_status             = EXCLUDED.ad_status,
  ad_type               = EXCLUDED.ad_type,
  impressions           = EXCLUDED.impressions,
  clicks                = EXCLUDED.clicks,
  cost                  = EXCLUDED.cost,
  conversions           = EXCLUDED.conversions,
  conversion_value      = EXCLUDED.conversion_value,
  all_conversions       = EXCLUDED.all_conversions,
  all_conversions_value = EXCLUDED.all_conversions_value,
  video_views           = EXCLUDED.video_views,
  engagements           = EXCLUDED.engagements,
  synced_at             = NOW()
"""

def get_conn():
    return psycopg2.connect(DB_URL, connect_timeout=15)

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

def latest_synced_date(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date) FROM public.google_ads_daily")
        return cur.fetchone()[0]

# ── Main ──────────────────────────────────────────────────────────────
def list_child_accounts(client, mcc_id):
    """Return [(customer_id, name, currency)] for every ENABLED non-manager
    child account under the MCC. Uses customer_client via searchStream."""
    svc = client.get_service("GoogleAdsService")
    out = []
    try:
        stream = svc.search_stream(customer_id=mcc_id, query=GAQL_ACCOUNTS)
        for batch in stream:
            for row in batch.results:
                cc = row.customer_client
                out.append((str(cc.id), cc.descriptive_name, cc.currency_code))
    except GoogleAdsException as e:
        print(f"  [!] list_child_accounts failed: {e.error.code().name} — {str(e)[:200]}")
    return out

def fetch_customer(client, customer_id, since_d, until_d):
    """Yield rows for one customer over the window."""
    svc = client.get_service("GoogleAdsService")
    q = GAQL_AD_PERF.format(from_d=since_d.isoformat(), until_d=until_d.isoformat())
    stream = svc.search_stream(customer_id=customer_id, query=q)
    for batch in stream:
        for row in batch.results:
            yield row

def to_row(customer_id, customer_name, currency, r):
    ad = r.ad_group_ad
    return (
        int(customer_id), customer_name, currency,
        r.segments.date,
        int(r.campaign.id) if r.campaign.id else None,
        r.campaign.name or None,
        int(r.ad_group.id) if r.ad_group.id else None,
        r.ad_group.name or None,
        int(ad.ad.id),
        ad.ad.name or None,
        ad.status.name if hasattr(ad.status, "name") else str(ad.status),
        ad.ad.type_.name if hasattr(ad.ad.type_, "name") else str(ad.ad.type_),
        int(r.metrics.impressions),
        int(r.metrics.clicks),
        (r.metrics.cost_micros or 0) / 1_000_000,   # micros → currency units
        float(r.metrics.conversions),
        float(r.metrics.conversions_value),
        float(r.metrics.all_conversions),
        float(r.metrics.all_conversions_value),
        int(r.metrics.video_views or 0),
        int(r.metrics.engagements or 0),
        datetime.now(timezone.utc),
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since",     help="YYYY-MM-DD  (default: last synced date - 3d overlap)")
    ap.add_argument("--until",     help="YYYY-MM-DD  (default: today, UTC)")
    ap.add_argument("--customer",  help="Query only this child customer_id (10 digits)")
    ap.add_argument("--overlap",   type=int, default=3,
                    help="Days to re-pull before max(date) — default 3")
    ap.add_argument("--dry-run",   action="store_true", help="Fetch + summarise, don't write to DB")
    args = ap.parse_args()

    client = GoogleAdsClient.load_from_dict({
        "developer_token":     DEV_TOKEN,
        "client_id":           CLIENT_ID,
        "client_secret":       CLIENT_SEC,
        "refresh_token":       REFRESH_TOK,
        "login_customer_id":   MCC_ID,
        "use_proto_plus":      True,
    })

    conn = get_conn()
    ensure_schema(conn)

    today = datetime.now(timezone.utc).date()
    until_d = date.fromisoformat(args.until) if args.until else today
    if args.since:
        since_d = date.fromisoformat(args.since)
    else:
        last = latest_synced_date(conn)
        if last is None:
            since_d = date(2025, 1, 1)
            print(f"[*] google_ads_daily is empty — defaulting since = {since_d}")
        else:
            since_d = last - timedelta(days=args.overlap) + timedelta(days=1)
            print(f"[*] last date = {last}  overlap={args.overlap}d → since = {since_d}")

    if since_d > until_d:
        print(f"[!] since ({since_d}) > until ({until_d}) — nothing to do")
        return

    if args.customer:
        accounts = [(args.customer, args.customer, "INR")]
    else:
        print(f"[*] listing MCC child accounts …")
        accounts = list_child_accounts(client, MCC_ID)
    print(f"[*] window {since_d} → {until_d}   accounts={len(accounts)}   dry_run={args.dry_run}")

    total_rows = 0; total_upserts = 0
    t0 = time.time()
    for (cid, cname, ccy) in accounts:
        print(f"\n  ── customer {cid}  ({cname or '—'})  ─────")
        try:
            batch = []
            for r in fetch_customer(client, cid, since_d, until_d):
                batch.append(to_row(cid, cname, ccy, r))
                if len(batch) >= 500:
                    total_rows += len(batch)
                    if not args.dry_run:
                        with conn.cursor() as cur:
                            execute_values(cur, UPSERT, batch, page_size=500)
                        conn.commit()
                        total_upserts += len(batch)
                    batch.clear()
            if batch:
                total_rows += len(batch)
                if not args.dry_run:
                    with conn.cursor() as cur:
                        execute_values(cur, UPSERT, batch, page_size=500)
                    conn.commit()
                    total_upserts += len(batch)
        except GoogleAdsException as e:
            print(f"    [!] GoogleAdsException: {e.error.code().name} — {str(e)[:220]}")
            continue
        except Exception as e:
            print(f"    [!] {type(e).__name__}: {str(e)[:220]}")
            continue

    print("\n" + "─" * 70)
    print(f"  accounts processed : {len(accounts)}")
    print(f"  rows fetched       : {total_rows:,}")
    print(f"  rows upserted      : {total_upserts:,}" + (" (dry-run)" if args.dry_run else ""))
    print(f"  elapsed            : {(time.time()-t0)/60:.1f} min")
    conn.close()

if __name__ == "__main__":
    main()
