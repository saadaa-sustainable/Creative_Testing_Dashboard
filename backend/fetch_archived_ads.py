"""Fetch ARCHIVED ads from Meta Ads Manager (aka 'Deleted' in Meta UI) and
insert lightweight placeholder rows into backfill_table.

Meta's default /act_{id}/ads endpoint excludes ARCHIVED (and truly DELETED
ads can't be queried at all — "Requesting for deleted objects is not supported"
per Meta's error). ARCHIVED = still-queryable ads hidden from Ads Manager's
default view; those are the ones we need for attribution — orders continue
to reference them via utm_content/utm_term for months after Meta archives
the ad.

Placeholder rows use:
  date            = created_date (chronological ordering)
  ad_status       = 'ARCHIVED'
  metrics         = 0
  ad_link/tags    = empty
Once inserted, load_ad_universe() in rebuild_attribution_orders.py picks
these ads up in by_id / by_name / adset_ads / camp_id_ads. Follow this
with `python reattribute_all.py` to re-resolve unmatched orders.

One-shot script — safe to run repeatedly (idempotent upsert on
(account_name, ad_id, date))."""
import os, sys, time, json, logging
from datetime import date, datetime
from dotenv import load_dotenv
import requests
import psycopg2
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv(override=True)

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

TOK = os.getenv("META_ACCESS_TOKEN")
DB  = os.getenv("SUPABASE_DB_URL")
VER = os.getenv("META_API_VERSION", "v22.0")
BASE = f"https://graph.facebook.com/{VER}"

ACCOUNTS = [
    {"name": os.getenv("ACCOUNT_1_NAME", "Raho Saadaa"),            "id": os.getenv("ACCOUNT_1_ID", "1136644150469466")},
    {"name": os.getenv("ACCOUNT_2_NAME", "Fourth Ad Account - SD"), "id": os.getenv("ACCOUNT_2_ID", "1349767139294217")},
    {"name": os.getenv("ACCOUNT_3_NAME", "Third Ad Account - SD"),  "id": os.getenv("ACCOUNT_3_ID", "264868699479122")},
]

PAGE_LIMIT = 50    # cross-account safe (Fourth 500s at 100 for nested fields)
MAX_RETRIES = 3

def _get(url: str, params: dict = None, attempt: int = 1) -> dict:
    try:
        r = requests.get(url, params=params or {}, timeout=90)
        if r.status_code == 200: return r.json()
        # 400 rate-limit sniff (same as primary_sync)
        if r.status_code == 400:
            body = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
            msg = ((body.get("error") or {}).get("message") or "").lower()
            if any(x in msg for x in ("too many calls","rate","throttl")):
                if attempt < MAX_RETRIES:
                    wait = 90 * attempt
                    log.warning(f"  Meta 400 rate limit — waiting {wait}s ({attempt}/{MAX_RETRIES})")
                    time.sleep(wait); return _get(url, params, attempt+1)
            log.warning(f"  Meta 400: {msg[:200]}")
            return {"data": [], "paging": {}}
        if r.status_code in (403, 500):
            if attempt < MAX_RETRIES:
                wait = 30 * attempt
                log.warning(f"  Meta {r.status_code} — waiting {wait}s ({attempt}/{MAX_RETRIES})")
                time.sleep(wait); return _get(url, params, attempt+1)
            log.warning(f"  Meta {r.status_code} persists — skipping page")
            return {"data": [], "paging": {}}
        r.raise_for_status()
        return r.json()
    except Exception as e:
        if attempt < MAX_RETRIES:
            log.warning(f"  retry {attempt}/{MAX_RETRIES}: {type(e).__name__}")
            time.sleep(10 * attempt); return _get(url, params, attempt+1)
        raise


def fetch_archived(account_id: str) -> list:
    url = f"{BASE}/act_{account_id}/ads"
    params = {
        "fields": "id,name,effective_status,created_time,updated_time,"
                  "adset{id,name},campaign{id,name}",
        "filtering": json.dumps([{
            "field": "effective_status", "operator": "IN",
            "value": ["ARCHIVED"]
        }]),
        "limit": PAGE_LIMIT,
        "access_token": TOK,
    }
    ads = []
    page = 0
    while url:
        page += 1
        data = _get(url, params if page == 1 else None)
        batch = data.get("data") or []
        ads.extend(batch)
        url = (data.get("paging") or {}).get("next")
        params = None
        if page % 25 == 0:
            log.info(f"    …page {page}: {len(ads)} archived ads so far")
    return ads


UPSERT_SQL = """
INSERT INTO public.backfill_table
  (account_name, date, ad_id, ad_name, adset_id, adset_name,
   campaign_id, campaign_name, ad_status, ad_created_date,
   impressions, reach, amount_spent_inr, synced_at)
VALUES %s
ON CONFLICT (account_name, ad_id, date) DO UPDATE
   SET ad_name         = EXCLUDED.ad_name,
       adset_id        = EXCLUDED.adset_id,
       adset_name      = EXCLUDED.adset_name,
       campaign_id     = EXCLUDED.campaign_id,
       campaign_name   = EXCLUDED.campaign_name,
       ad_status       = EXCLUDED.ad_status,
       ad_created_date = EXCLUDED.ad_created_date,
       synced_at       = NOW()
"""

def upsert(acc_name: str, ads: list) -> int:
    if not ads: return 0
    rows = []
    for a in ads:
        ct = a.get("created_time", "")
        created_d = None
        if ct:
            try: created_d = datetime.fromisoformat(ct.replace("Z","+00:00")).date()
            except Exception: pass
        placeholder_date = created_d or date.today()
        adset = a.get("adset") or {}
        camp  = a.get("campaign") or {}
        rows.append((
            acc_name, placeholder_date,
            a.get("id") or "", a.get("name") or "",
            adset.get("id") or "", adset.get("name") or "",
            camp.get("id") or "",  camp.get("name") or "",
            a.get("effective_status") or "ARCHIVED",
            created_d,
            0, 0, 0,   # impressions, reach, amount_spent_inr
        ))
    conn = psycopg2.connect(DB, connect_timeout=30)
    try:
        with conn.cursor() as cur:
            # execute_values with NOW() sentinel for synced_at
            template = "(" + ",".join(["%s"]*13) + ", NOW())"
            execute_values(cur, UPSERT_SQL, rows, template=template, page_size=500)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def main():
    t0 = datetime.now()
    grand = 0
    log.info(f"\n{'='*60}\nARCHIVED-ADS FETCH — Meta v{VER}\n{'='*60}")
    for acct in ACCOUNTS:
        log.info(f"\n>>  {acct['name']}  (act_{acct['id']})")
        try:
            ads = fetch_archived(acct["id"])
        except Exception as e:
            log.error(f"  fetch failed: {e}")
            continue
        log.info(f"    fetched {len(ads):,} archived ads")
        n = upsert(acct["name"], ads)
        log.info(f"    upserted {n:,} placeholder rows into backfill_table")
        grand += n

    elapsed = (datetime.now()-t0).total_seconds()
    log.info(f"\n{'='*60}\nDONE — {grand:,} archived ads landed  ·  {elapsed:.0f}s\n{'='*60}\n")
    log.info("Next step: `python reattribute_all.py` to re-resolve orders against the enriched universe.")


if __name__ == "__main__":
    main()
