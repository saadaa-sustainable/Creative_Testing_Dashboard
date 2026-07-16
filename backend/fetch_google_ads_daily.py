"""
fetch_google_ads_daily.py — pull daily ad-level performance from every
child account under the MCC (or direct-access accounts) and upsert into
public.google_ads_primary.

Grain per row: (customer_id, ad_id, date). Mirrors Meta's primary_table
shape — one truthful daily row per ad. See refresh_google_ads_summary.py
for the aggregated one-row-per-ad view.

USAGE
  python fetch_google_ads_daily.py                 # incremental (last synced date + 3d overlap → today)
  python fetch_google_ads_daily.py --since 2025-01-01
  python fetch_google_ads_daily.py --until 2026-07-15
  python fetch_google_ads_daily.py --customer 1234567890   # single sub-account
  python fetch_google_ads_daily.py --dry-run       # fetch, print summary, skip DB write

ENV (backend/.env — see GOOGLE_ADS_SETUP.md)
  GOOGLE_ADS_DEVELOPER_TOKEN
  GOOGLE_ADS_LOGIN_CUSTOMER_ID     (the MCC, optional)
  SUPABASE_DB_URL                  (the Meta Ads project — reused)
Auth via gcloud ADC — application_default_credentials.json is read directly.
"""
import os, sys, time, argparse
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

import json, pathlib
DEV_TOKEN = (os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip()
MCC_ID    = (os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or "").strip()
DB_URL    = (os.environ.get("SUPABASE_DB_URL") or "").strip()

for name, v in [("GOOGLE_ADS_DEVELOPER_TOKEN", DEV_TOKEN),
                ("SUPABASE_DB_URL", DB_URL)]:
    if not v: sys.exit(f"Missing {name} in .env — see backend/GOOGLE_ADS_SETUP.md")

def _adc_path():
    if os.name == "nt":
        appdata = os.environ.get("APPDATA")
        return pathlib.Path(appdata) / "gcloud" / "application_default_credentials.json" if appdata else None
    return pathlib.Path.home() / ".config" / "gcloud" / "application_default_credentials.json"

_adc_p = _adc_path()
if not _adc_p or not _adc_p.is_file():
    sys.exit(f"gcloud ADC not found at {_adc_p}. Run the OAuth flow first (see _gads_web_auth.py or GOOGLE_ADS_SETUP.md)")
try:
    _adc = json.loads(_adc_p.read_text(encoding="utf-8"))
except Exception as e:
    sys.exit(f"Failed to parse ADC: {e}")
if not _adc.get("refresh_token"):
    sys.exit("ADC has no refresh_token — re-run OAuth flow with the adwords scope.")

CLIENT_ID   = _adc.get("client_id")
CLIENT_SEC  = _adc.get("client_secret")
REFRESH_TOK = _adc.get("refresh_token")

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError:
    sys.exit("google-ads not installed. Run: pip install google-ads")

import psycopg2
from psycopg2.extras import execute_values

# ── GAQL ─────────────────────────────────────────────────────────────
# Enumerate MCC child accounts (only enabled non-manager).
GAQL_ACCOUNTS = """
  SELECT customer_client.id, customer_client.descriptive_name,
         customer_client.currency_code, customer_client.manager,
         customer_client.status
  FROM customer_client
  WHERE customer_client.status = 'ENABLED'
    AND customer_client.manager = FALSE
"""

# Ad-level performance query — every valid ad_group_ad metric in v24
# that doesn't require a breakdown segment.  Metrics that are only
# valid on VIDEO campaigns still parse for other campaign types (Meta
# returns 0/null gracefully).  Fields added over time can be appended
# here; schema in google_ads_primary_and_summary migration covers all
# of them.
GAQL_AD_PERF = """
  SELECT
    segments.date,

    ad_group_ad.ad.id,
    ad_group_ad.ad.name,
    ad_group_ad.ad.type,
    ad_group_ad.ad.final_urls,
    ad_group_ad.ad.display_url,
    ad_group_ad.status,
    ad_group_ad.ad_strength,

    ad_group.id,
    ad_group.name,
    ad_group.status,

    campaign.id,
    campaign.name,
    campaign.status,
    campaign.advertising_channel_type,
    campaign.advertising_channel_sub_type,
    campaign.bidding_strategy_type,

    metrics.impressions,
    metrics.clicks,
    metrics.cost_micros,
    metrics.average_cpc,
    metrics.average_cpm,
    metrics.ctr,

    metrics.interactions,
    metrics.interaction_rate,
    metrics.engagements,
    metrics.engagement_rate,

    metrics.conversions,
    metrics.conversions_value,
    metrics.conversions_from_interactions_rate,
    metrics.cost_per_conversion,
    metrics.value_per_conversion,

    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.all_conversions_from_interactions_rate,
    metrics.cost_per_all_conversions,

    metrics.view_through_conversions,
    metrics.video_quartile_p25_rate,
    metrics.video_quartile_p50_rate,
    metrics.video_quartile_p75_rate,
    metrics.video_quartile_p100_rate,

    metrics.absolute_top_impression_percentage,
    metrics.top_impression_percentage,
    metrics.active_view_impressions,
    metrics.active_view_measurability,
    metrics.active_view_viewability
  FROM ad_group_ad
  WHERE segments.date BETWEEN '{from_d}' AND '{until_d}'
"""

# ── DB helpers ────────────────────────────────────────────────────────
UPSERT_COLS = [
    "customer_id","customer_name","currency_code","date",
    "campaign_id","campaign_name","campaign_status",
    "advertising_channel_type","advertising_channel_sub_type","bidding_strategy_type",
    "ad_group_id","ad_group_name","ad_group_status",
    "ad_id","ad_name","ad_status","ad_type","ad_strength","final_urls","display_url",
    "impressions","clicks","cost","average_cpc","average_cpm","average_cpv","ctr",
    "interactions","interaction_rate","engagements","engagement_rate",
    "conversions","conversion_value","conversions_from_interactions_rate",
    "cost_per_conversion","value_per_conversion",
    "all_conversions","all_conversions_value",
    "all_conversions_from_interactions_rate","cost_per_all_conversions","value_per_all_conversion",
    "view_through_conversions","video_view_rate",
    "video_quartile_p25_rate","video_quartile_p50_rate",
    "video_quartile_p75_rate","video_quartile_p100_rate",
    "absolute_top_impression_percentage","top_impression_percentage",
    "active_view_impressions","active_view_measurability","active_view_viewability",
    "synced_at",
]
_update_cols = ",".join(f"{c} = EXCLUDED.{c}" for c in UPSERT_COLS
                        if c not in ("customer_id","ad_id","date"))
UPSERT_SQL = f"""
INSERT INTO public.google_ads_primary ({",".join(UPSERT_COLS)})
VALUES %s
ON CONFLICT (customer_id, ad_id, date) DO UPDATE SET
  {_update_cols}
"""

def get_conn():
    return psycopg2.connect(DB_URL, connect_timeout=15)

def latest_synced_date(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date) FROM public.google_ads_primary")
        return cur.fetchone()[0]

# ── Main ──────────────────────────────────────────────────────────────
def list_child_accounts(client, mcc_id):
    """Return [(customer_id, name, currency)] for every ENABLED non-manager
    child under the MCC."""
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

def _enum(x):
    """Google Ads SDK returns enum objects — extract the .name string."""
    if x is None: return None
    return x.name if hasattr(x, "name") else str(x)

def to_row(customer_id, customer_name, currency, r):
    ad     = r.ad_group_ad
    m      = r.metrics
    urls   = list(ad.ad.final_urls) if ad.ad.final_urls else []
    return (
        int(customer_id), customer_name, currency,
        r.segments.date,
        int(r.campaign.id)      if r.campaign.id      else None,
        r.campaign.name or None,
        _enum(r.campaign.status),
        _enum(r.campaign.advertising_channel_type),
        _enum(r.campaign.advertising_channel_sub_type),
        _enum(r.campaign.bidding_strategy_type),
        int(r.ad_group.id)      if r.ad_group.id      else None,
        r.ad_group.name or None,
        _enum(r.ad_group.status),
        int(ad.ad.id),
        ad.ad.name or None,
        _enum(ad.status),
        _enum(ad.ad.type_),
        _enum(getattr(ad, "ad_strength", None)),
        ",".join(urls) if urls else None,
        ad.ad.display_url or None,
        int(m.impressions), int(m.clicks),
        (m.cost_micros or 0) / 1_000_000,
        (m.average_cpc  or 0) / 1_000_000,
        (m.average_cpm  or 0) / 1_000_000,
        None,                                     # average_cpv retired on ad_group_ad in v24
        float(m.ctr),
        int(m.interactions or 0), float(m.interaction_rate),
        int(m.engagements  or 0), float(m.engagement_rate),
        float(m.conversions), float(m.conversions_value),
        float(m.conversions_from_interactions_rate),
        (m.cost_per_conversion or 0) / 1_000_000,
        float(m.value_per_conversion or 0),
        float(m.all_conversions), float(m.all_conversions_value),
        float(m.all_conversions_from_interactions_rate),
        (m.cost_per_all_conversions or 0) / 1_000_000,
        None,                                     # value_per_all_conversion retired in v24
        int(m.view_through_conversions or 0),
        None,                                     # video_view_rate retired in v24
        float(m.video_quartile_p25_rate),
        float(m.video_quartile_p50_rate),
        float(m.video_quartile_p75_rate),
        float(m.video_quartile_p100_rate),
        float(m.absolute_top_impression_percentage),
        float(m.top_impression_percentage),
        int(m.active_view_impressions or 0),
        float(m.active_view_measurability),
        float(m.active_view_viewability),
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

    _cfg = {
        "developer_token":  DEV_TOKEN,
        "client_id":        CLIENT_ID,
        "client_secret":    CLIENT_SEC,
        "refresh_token":    REFRESH_TOK,
        "use_proto_plus":   True,
    }
    if MCC_ID:
        _cfg["login_customer_id"] = MCC_ID
    client = GoogleAdsClient.load_from_dict(_cfg)

    conn = get_conn()

    today = datetime.now(timezone.utc).date()
    until_d = date.fromisoformat(args.until) if args.until else today
    if args.since:
        since_d = date.fromisoformat(args.since)
    else:
        last = latest_synced_date(conn)
        if last is None:
            since_d = date(2025, 1, 1)
            print(f"[*] google_ads_primary is empty — defaulting since = {since_d}")
        else:
            since_d = last - timedelta(days=args.overlap) + timedelta(days=1)
            print(f"[*] last date = {last}  overlap={args.overlap}d → since = {since_d}")

    if since_d > until_d:
        print(f"[!] since ({since_d}) > until ({until_d}) — nothing to do")
        return

    if args.customer:
        accounts = [(args.customer, args.customer, "INR")]
    elif MCC_ID:
        print(f"[*] listing MCC {MCC_ID} child accounts …")
        accounts = list_child_accounts(client, MCC_ID)
    else:
        print(f"[*] no MCC set — listing direct-access customers …")
        svc = client.get_service("CustomerService")
        rns = svc.list_accessible_customers().resource_names
        accounts = []
        ga = client.get_service("GoogleAdsService")
        for rn in rns:
            cid = rn.split("/")[-1]
            try:
                rows = ga.search(customer_id=cid, query=
                    "SELECT customer.id, customer.descriptive_name, customer.currency_code, customer.manager FROM customer LIMIT 1")
                for row in rows:
                    if row.customer.manager: continue
                    accounts.append((str(row.customer.id), row.customer.descriptive_name, row.customer.currency_code))
            except GoogleAdsException:
                continue
    print(f"[*] window {since_d} → {until_d}   accounts={len(accounts)}   dry_run={args.dry_run}")

    total_rows = 0; total_upserts = 0
    t0 = time.time()
    for (cid, cname, ccy) in accounts:
        print(f"\n  ── customer {cid}  ({cname or '—'})  ─────", flush=True)
        try:
            batch = []
            for r in fetch_customer(client, cid, since_d, until_d):
                batch.append(to_row(cid, cname, ccy, r))
                if len(batch) >= 500:
                    total_rows += len(batch)
                    if not args.dry_run:
                        with conn.cursor() as cur:
                            execute_values(cur, UPSERT_SQL, batch, page_size=500)
                        conn.commit()
                        total_upserts += len(batch)
                    batch.clear()
            if batch:
                total_rows += len(batch)
                if not args.dry_run:
                    with conn.cursor() as cur:
                        execute_values(cur, UPSERT_SQL, batch, page_size=500)
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
