"""
primary_sync.py — Full Meta API → primary_table pipeline

Fetches EVERYTHING the Apps Script fetches, including:
  - All insight metrics (impressions, spend, ROAS, CTR, etc.)
  - Custom conversions: FTEWV (First-time EWV) and NCP
  - LTV stats: lifetime reach + frequency per ad (date_preset=maximum)
  - Ad metadata: effective_status + created_time
  - All computed rates: CPM, checkout completion %, ATC rate, etc.

One row per (account_name, ad_id, date) — same as Apps Script.

Usage:
  python primary_sync.py backfill   # Jan 1 2026 → today, all accounts
  python primary_sync.py daily      # last 15 days → primary_table
  python primary_sync.py hourly     # last 3 days  → primary_table
  python primary_sync.py status     # row count + latest dates
"""

import os
import sys
import time
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import date, timedelta, datetime, timezone
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("primary_sync.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
API_VERSION = os.getenv("META_API_VERSION", "v21.0")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

ACCOUNTS = [
    {
        "name": os.getenv("ACCOUNT_1_NAME", "Raho Saadaa"),
        "id": os.getenv("ACCOUNT_1_ID", "1136644150469466"),
    },
    {
        "name": os.getenv("ACCOUNT_2_NAME", "Fourth Ad Account - SD"),
        "id": os.getenv("ACCOUNT_2_ID", "1349767139294217"),
    },
    {
        "name": os.getenv("ACCOUNT_3_NAME", "Third Ad Account - SD"),
        "id": os.getenv("ACCOUNT_3_ID", "264868699479122"),
    },
]

# Custom conversion names — must match exactly what's in Meta Business Manager
CUSTOM_METRIC_FTEWV = os.getenv("CUSTOM_METRIC_FTEWV", "First-time EWV")
CUSTOM_METRIC_NCP = os.getenv("CUSTOM_METRIC_NCP", "NCP")

TABLE = "primary_table"
CHUNK_DAYS = 15  # 7 days per API chunk (prevents Meta 500 on large accounts)
PAGE_LIMIT = 500  # rows per Meta API page
BATCH_SIZE = 500  # rows per Supabase upsert
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Core insight fields — matches Apps Script exactly
META_FIELDS = ",".join(
    [
        "date_start",
        "date_stop",
        "ad_name",
        "ad_id",
        "campaign_name",
        "campaign_id",
        "impressions",
        "reach",
        "frequency",
        "spend",
        "purchase_roas",
        "actions",
        "action_values",
        "outbound_clicks",
        "inline_link_clicks",
        "inline_link_click_ctr",
        "cost_per_inline_link_click",
        "video_thruplay_watched_actions",
        "inline_post_engagement",
        "video_avg_time_watched_actions",
    ]
)

# All columns in primary_table (insertion order)
COLUMNS = [
    "account_name",
    "date",
    "date_stop",
    "ad_created_date",
    "ad_name",
    "ad_id",
    "campaign_name",
    "campaign_id",
    "ad_status",
    "impressions",
    "reach",
    "frequency",
    "amount_spent_inr",
    "outbound_clicks",
    "inline_link_clicks",
    "ctr",
    "cpc",
    "cpm",
    "thruplays",
    "three_sec_video_plays",
    "video_play_time",
    "purchase_roas",
    "purchases",
    "cost_per_purchase",
    "conversion_value",
    "initiate_checkout",
    "add_to_cart",
    "post_engagements",
    "checkout_completion",
    "atc_rate",
    "ci_atc_rate",
    "purchase_rate",
    "ftewv_count",
    "cost_per_ftewv",
    "ncp_count",
    "cost_per_ncp",
    "ltv_reach",
    "ltv_frequency",
]
UPDATE_COLS = [c for c in COLUMNS if c not in ("account_name", "ad_id", "date")]


# ── Date helpers ──────────────────────────────────────────────
def gmt_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def gmt_days_ago(n: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=n)).strftime("%Y-%m-%d")


def date_chunks(since: str, until: str, chunk_days: int = CHUNK_DAYS):
    start = date.fromisoformat(since)
    end = date.fromisoformat(until)
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current.isoformat(), chunk_end.isoformat()
        current = chunk_end + timedelta(days=1)


# ── Meta API helpers ──────────────────────────────────────────
def _get(url: str, params: dict = None, attempt: int = 1) -> dict:
    try:
        resp = requests.get(url, params=params or {}, timeout=90)

        if resp.status_code == 403:
            if attempt < MAX_RETRIES:
                log.warning(
                    f"  Meta 403 rate limit — waiting 60s (attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(60)
                return _get(url, params, attempt + 1)
            log.warning("  Meta 403 persists — skipping page")
            return {"data": [], "paging": {}}

        if resp.status_code == 500:
            if attempt < MAX_RETRIES:
                wait = RETRY_DELAY * attempt * 3
                log.warning(
                    f"  Meta 500 server error — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                return _get(url, params, attempt + 1)
            log.warning("  Meta 500 persists — skipping page")
            return {"data": [], "paging": {}}

        resp.raise_for_status()
        return resp.json()

    except Exception as e:
        if attempt < MAX_RETRIES:
            log.warning(f"  Retry {attempt}/{MAX_RETRIES}: {e}")
            time.sleep(RETRY_DELAY * attempt)
            return _get(url, params, attempt + 1)
        raise


def get_custom_conversion_ids(account_id: str) -> dict:
    """
    Returns {name_lower: id} for all custom conversions in this account.
    Matches Apps Script's getCustomConversionIdByName().
    """
    ids = {}
    try:
        url = f"{BASE_URL}/act_{account_id}/customconversions"
        params = {"fields": "name,id", "access_token": ACCESS_TOKEN, "limit": 200}
        data = _get(url, params)
        for c in data.get("data", []):
            ids[c["name"].lower().strip()] = c["id"]
        log.info(f"  Custom conversions found: {list(ids.keys())}")
    except Exception as e:
        log.error(f"  Custom conversion fetch error: {e}")
    return ids


def fetch_all_ads(account_id: str) -> list:
    """
    Fetches the full ad list for an account from /act_<id>/ads — every ad regardless
    of delivery state. Returned as a list of dicts with id, name, status, created_time,
    campaign_id, campaign_name.

    Why: Meta's /insights endpoint only returns rows for date×ad combinations that
    actually delivered impressions. Active-but-not-delivering ads never show up there,
    so the dashboard's count is lower than what Meta Ads Manager UI displays. We use
    this list to fill in metadata-only placeholder rows so the counts reconcile.
    """
    ads = []
    url = f"{BASE_URL}/act_{account_id}/ads"
    params = {
        "fields": "id,name,effective_status,created_time,campaign{id,name}",
        "limit": 500,
        "access_token": ACCESS_TOKEN,
    }
    page = 0
    while url:
        page += 1
        data = _get(url, params if page == 1 else None)
        batch = data.get("data", [])
        for a in batch:
            camp = a.get("campaign") or {}
            ct = a.get("created_time", "")
            ads.append(
                {
                    "id": a.get("id", ""),
                    "name": a.get("name", ""),
                    "effective_status": a.get("effective_status", ""),
                    "created_date": ct[:10] if ct else None,
                    "campaign_id": camp.get("id", ""),
                    "campaign_name": camp.get("name", ""),
                }
            )
        url = (data.get("paging") or {}).get("next")
        params = None
        # Also handle params from "next" URL — paging.next already encodes them
        if url and "?" not in url:
            params = {"access_token": ACCESS_TOKEN}
    log.info(f"  /ads endpoint returned {len(ads)} ads for account {account_id}")
    return ads


def get_ad_metadata(ad_ids: list) -> dict:
    """
    Returns {ad_id: {status, created_date}} for all given IDs.
    Batches 25 per request. Matches Apps Script's getAdStatusMap + getTargetedAdCreationMap.
    """
    metadata = {}
    unique_ids = list(set(ad_ids))
    for i in range(0, len(unique_ids), 25):
        chunk = unique_ids[i : i + 25]
        try:
            data = _get(
                f"{BASE_URL}/",
                {
                    "ids": ",".join(chunk),
                    "fields": "effective_status,created_time",
                    "access_token": ACCESS_TOKEN,
                },
            )
            for ad_id, info in data.items():
                if isinstance(info, dict):
                    ct = info.get("created_time", "")
                    metadata[ad_id] = {
                        "status": info.get("effective_status", ""),
                        "created_date": ct[:10] if ct else None,
                    }
        except Exception as e:
            log.error(f"  Ad metadata error (chunk {i}): {e}")
    return metadata


def get_ltv_stats(ad_ids: list, account_id: str) -> dict:
    """
    Fetches lifetime reach + frequency per ad using date_preset=maximum.
    Matches Apps Script's getTargetedLifetimeStatsMap().
    Batches 50 ad IDs per request with filtering.
    Returns {ad_id: {reach, frequency}}
    """
    ltv = {}
    unique_ids = list(set(ad_ids))

    for i in range(0, len(unique_ids), 50):
        chunk = unique_ids[i : i + 50]
        try:
            import json

            filtering = json.dumps(
                [{"field": "ad.id", "operator": "IN", "value": chunk}]
            )
            data = _get(
                f"{BASE_URL}/act_{account_id}/insights",
                {
                    "level": "ad",
                    "fields": "ad_id,reach,frequency",
                    "date_preset": "maximum",
                    "filtering": filtering,
                    "access_token": ACCESS_TOKEN,
                    "limit": 500,
                },
            )
            for r in data.get("data", []):
                if r.get("ad_id"):
                    ltv[r["ad_id"]] = {
                        "reach": float(r.get("reach", 0) or 0),
                        "frequency": float(r.get("frequency", 0) or 0),
                    }
        except Exception as e:
            log.error(f"  LTV stats error (chunk {i}): {e}")

    return ltv


def _action_val(lst: list, *types) -> float:
    if not lst:
        return 0.0
    for item in lst:
        if item.get("action_type") in types:
            return float(item.get("value", 0) or 0)
    return 0.0


def parse_row(
    row: dict, account_name: str, metadata: dict, ltv: dict, ftewv_id: str, ncp_id: str
) -> dict:
    """
    Converts a raw Meta API row into a primary_table-ready dict.
    Computes all metrics exactly as the Apps Script does.
    """
    ad_id = row.get("ad_id", "")
    ad_meta = metadata.get(ad_id, {})
    ltv_ad = ltv.get(ad_id, {"reach": 0, "frequency": 0})

    spend = float(row.get("spend", 0) or 0)
    impressions = float(row.get("impressions", 0) or 0)
    reach = float(row.get("reach", 0) or 0)
    frequency = float(row.get("frequency", 0) or 0)
    link_clicks = float(row.get("inline_link_clicks", 0) or 0)
    ctr = float(row.get("inline_link_click_ctr", 0) or 0)
    cpc = float(row.get("cost_per_inline_link_click", 0) or 0)

    # ROAS
    purchase_roas = 0.0
    for r in row.get("purchase_roas") or []:
        if r.get("action_type") in ("omni_purchase", "purchase"):
            purchase_roas = float(r.get("value", 0) or 0)
            break

    # Actions
    actions = row.get("actions") or []
    purchases = _action_val(actions, "omni_purchase", "purchase")
    checkout_ini = _action_val(actions, "omni_initiated_checkout", "initiate_checkout")
    add_to_cart = _action_val(actions, "omni_add_to_cart", "add_to_cart")
    three_sec = _action_val(actions, "video_view")

    # Custom conversions — matches Apps Script logic exactly
    ftewv_count = 0.0
    ncp_count = 0.0
    if ftewv_id:
        ftewv_count = _action_val(actions, f"offsite_conversion.custom.{ftewv_id}")
    if ncp_id:
        ncp_count = _action_val(actions, f"offsite_conversion.custom.{ncp_id}")

    # Cost per FTEWV and NCP — derived from spend/count (avoids cost_per_action_type
    # which causes Meta 400/500 errors on large accounts)
    cost_per_ftewv = round(spend / ftewv_count, 2) if ftewv_count > 0 else 0.0
    cost_per_ncp = round(spend / ncp_count, 2) if ncp_count > 0 else 0.0

    # Conversion value
    conv_value = _action_val(
        row.get("action_values") or [], "omni_purchase", "purchase"
    )

    # ThruPlays
    tp_list = row.get("video_thruplay_watched_actions") or []
    thruplays = float(tp_list[0].get("value", 0)) if tp_list else 0.0

    # Outbound clicks
    oc_list = row.get("outbound_clicks") or []
    outbound = float(oc_list[0].get("value", 0)) if oc_list else 0.0

    # Video play time
    vpt_list = row.get("video_avg_time_watched_actions") or []
    video_time = float(vpt_list[0].get("value", 0)) if vpt_list else 0.0

    # Post engagements
    post_eng = float(row.get("inline_post_engagement", 0) or 0)

    # Computed rates — identical to Apps Script formulas
    cpm = (spend / impressions * 1000) if impressions > 0 else 0.0
    cost_per_purchase = (spend / purchases) if purchases > 0 else 0.0
    checkout_compl = (purchases / checkout_ini * 100) if checkout_ini > 0 else 0.0
    atc_rate = (add_to_cart / link_clicks * 100) if link_clicks > 0 else 0.0
    ci_atc_rate = (checkout_ini / add_to_cart * 100) if add_to_cart > 0 else 0.0
    purchase_rate = (purchases / link_clicks * 100) if link_clicks > 0 else 0.0

    return {
        "account_name": account_name,
        "date": row.get("date_start"),
        "date_stop": row.get("date_stop"),
        "ad_created_date": ad_meta.get("created_date"),
        "ad_name": row.get("ad_name"),
        "ad_id": ad_id,
        "campaign_name": row.get("campaign_name"),
        "campaign_id": row.get("campaign_id"),
        "ad_status": ad_meta.get("status"),
        "impressions": int(impressions),
        "reach": int(reach),
        "frequency": round(frequency, 4),
        "amount_spent_inr": round(spend, 2),
        "outbound_clicks": int(outbound),
        "inline_link_clicks": int(link_clicks),
        "ctr": round(ctr, 4),
        "cpc": round(cpc, 4),
        "cpm": round(cpm, 4),
        "thruplays": int(thruplays),
        "three_sec_video_plays": int(three_sec),
        "video_play_time": round(video_time, 4),
        "purchase_roas": round(purchase_roas, 4),
        "purchases": round(purchases, 2),
        "cost_per_purchase": round(cost_per_purchase, 2),
        "conversion_value": round(conv_value, 2),
        "initiate_checkout": round(checkout_ini, 2),
        "add_to_cart": round(add_to_cart, 2),
        "post_engagements": int(post_eng),
        "checkout_completion": round(checkout_compl, 4),
        "atc_rate": round(atc_rate, 4),
        "ci_atc_rate": round(ci_atc_rate, 4),
        "purchase_rate": round(purchase_rate, 4),
        "ftewv_count": round(ftewv_count, 2),
        "cost_per_ftewv": round(cost_per_ftewv, 2),
        "ncp_count": round(ncp_count, 2),
        "cost_per_ncp": round(cost_per_ncp, 2),
        "ltv_reach": round(ltv_ad["reach"], 2),
        "ltv_frequency": round(ltv_ad["frequency"], 4),
    }


# ── Supabase ──────────────────────────────────────────────────
@contextmanager
def get_conn():
    conn = psycopg2.connect(
        SUPABASE_DB_URL,
        connect_timeout=30,
        options="-c search_path=public -c statement_timeout=60000",
    )
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# UPSERT — same pattern as Apps Script rowMap:
# if (account_name, ad_id, date) exists → UPDATE all metrics
# if not → INSERT new row
# This means daily syncs always reflect the latest Meta numbers
UPSERT_SQL = f"""
    INSERT INTO {TABLE} ({", ".join(COLUMNS)})
    VALUES ({", ".join([f"%({c})s" for c in COLUMNS])})
    ON CONFLICT (account_name, ad_id, date)
    DO UPDATE SET
        {", ".join([f"{c} = EXCLUDED.{c}" for c in UPDATE_COLS])},
        updated_at = NOW()
"""


def upsert_rows(rows: list) -> int:
    """
    UPSERT — mirrors Apps Script rowMap pattern exactly:
    UPDATE existing (account_name, ad_id, date) rows with latest Meta numbers,
    INSERT new rows for new ads or new dates.
    """
    if not rows:
        return 0
    total = 0
    n_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        batch_no = i // BATCH_SIZE + 1
        clean = [{c: r.get(c) for c in COLUMNS} for r in batch]

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Fresh connection per batch
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_batch(
                            cur, UPSERT_SQL, clean, page_size=100
                        )
                total += len(batch)
                log.debug(f"  Batch {batch_no}/{n_batches}: {len(batch)} ✓")
                break
            except Exception as e:
                if attempt < MAX_RETRIES:
                    log.warning(f"  Batch {batch_no} retry {attempt}: {e}")
                    time.sleep(RETRY_DELAY * attempt)
                else:
                    log.error(f"  Batch {batch_no} FAILED: {e}")
                    raise
    return total


def get_row_count() -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE};")
            return cur.fetchone()[0]


def get_latest_dates() -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT account_name, MAX(date)::text FROM {TABLE} GROUP BY account_name;"
            )
            return {row[0]: row[1] for row in cur.fetchall()}


# ── Core fetch engine ─────────────────────────────────────────
def fetch_and_upsert(
    account_id: str,
    account_name: str,
    since: str,
    until: str,
    ftewv_id: str,
    ncp_id: str,
) -> tuple:
    """
    Fetches all insights for a date range, plus LTV stats + metadata,
    and upserts into primary_table.
    Returns (fetched_count, upserted_count).
    """
    log.info(f"  Fetching {account_name} | {since} → {until}")

    url = f"{BASE_URL}/act_{account_id}/insights"
    params = {
        "level": "ad",
        "fields": META_FIELDS,
        "time_increment": "1",
        "time_range": f'{{"since":"{since}","until":"{until}"}}',
        "access_token": ACCESS_TOKEN,
        "limit": 500,
    }

    raw_rows = []
    page = 0

    while url:
        page += 1
        data = _get(url, params if page == 1 else None)
        batch = data.get("data", [])
        raw_rows.extend(batch)
        log.debug(f"    page {page}: {len(batch)} rows")
        url = (data.get("paging") or {}).get("next")
        params = None

    if not raw_rows:
        log.info(f"  No data returned")
        return 0, 0, set()

    # Collect unique ad IDs
    ad_ids = list({r["ad_id"] for r in raw_rows if r.get("ad_id")})
    log.info(
        f"  {len(raw_rows)} rows to insert | {len(ad_ids)} unique ad IDs for metadata + LTV lookup..."
    )

    # Fetch metadata and LTV in parallel batches
    metadata = get_ad_metadata(ad_ids)
    ltv = get_ltv_stats(ad_ids, account_id)

    # Parse all rows
    rows = [
        parse_row(r, account_name, metadata, ltv, ftewv_id, ncp_id) for r in raw_rows
    ]

    # Upsert
    n = upsert_rows(rows)
    log.info(
        f"  ✅ {len(rows)} fetched, {n} upserted (updated if existed, inserted if new)"
    )
    return len(rows), n, set(ad_ids)


# ── Sync functions ────────────────────────────────────────────
# Statuses Meta returns for ads that are "alive" (eligible to deliver, even if not delivering today).
# We insert placeholder rows for these so the dashboard can display them even when /insights
# returned no impressions in the date range.
ACTIVE_LIKE_STATUSES = {
    "ACTIVE",
    "WITH_ISSUES",
    "PENDING_REVIEW",
    "PREAPPROVED",
    "IN_PROCESS",
    "PENDING_BILLING_INFO",
}


def upsert_placeholders(
    account_name: str,
    active_ads: list,
    delivered_keys: set,
    since: str,
    until: str,
) -> int:
    """
    For every active ad that didn't appear in the /insights pull for [since, until],
    upsert one zero-metrics row per date in the range so the dashboard reflects the
    same ad list as Meta Ads Manager.

    `delivered_keys` is the set of ad_ids that did appear in /insights.
    `active_ads` is the list returned by fetch_all_ads().
    """
    candidates = [
        a
        for a in active_ads
        if a["id"]
        and a["id"] not in delivered_keys
        and (a.get("effective_status") or "").upper() in ACTIVE_LIKE_STATUSES
    ]
    if not candidates:
        log.info("  No placeholder rows needed (all active ads delivered)")
        return 0

    # Only fill placeholders for ads that have ever delivered impressions —
    # matches Meta Ads Manager's CSV behavior, which excludes never-delivered
    # ads from filtered exports even when their status is active.
    candidate_ids = [a["id"] for a in candidates]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ad_id
                  FROM primary_table
                 WHERE account_name = %s
                   AND ad_id = ANY(%s)
                   AND impressions > 0
                """,
                (account_name, candidate_ids),
            )
            ever_delivered = {r[0] for r in cur.fetchall()}

    needs_placeholder = [a for a in candidates if a["id"] in ever_delivered]
    skipped = len(candidates) - len(needs_placeholder)
    if skipped:
        log.info(
            f"  Skipping {skipped} active ads that never delivered (excluded by Meta CSV behavior)"
        )
    if not needs_placeholder:
        log.info("  No placeholder rows needed after past-delivery filter")
        return 0

    # One row per (ad, date) across the sync range so dashboard date filters always match
    start_d = date.fromisoformat(since)
    end_d = date.fromisoformat(until)
    days = []
    cur = start_d
    while cur <= end_d:
        days.append(cur.isoformat())
        cur += timedelta(days=1)

    rows = []
    for ad in needs_placeholder:
        for d in days:
            rows.append(
                {
                    "account_name": account_name,
                    "date": d,
                    "date_stop": d,
                    "ad_created_date": ad.get("created_date"),
                    "ad_name": ad.get("name", ""),
                    "ad_id": ad["id"],
                    "campaign_name": ad.get("campaign_name", ""),
                    "campaign_id": ad.get("campaign_id", ""),
                    "ad_status": ad.get("effective_status", ""),
                    "impressions": 0,
                    "reach": 0,
                    "frequency": 0,
                    "amount_spent_inr": 0,
                    "outbound_clicks": 0,
                    "inline_link_clicks": 0,
                    "ctr": 0,
                    "cpc": 0,
                    "cpm": 0,
                    "thruplays": 0,
                    "three_sec_video_plays": 0,
                    "video_play_time": 0,
                    "purchase_roas": 0,
                    "purchases": 0,
                    "cost_per_purchase": 0,
                    "conversion_value": 0,
                    "initiate_checkout": 0,
                    "add_to_cart": 0,
                    "post_engagements": 0,
                    "checkout_completion": 0,
                    "atc_rate": 0,
                    "ci_atc_rate": 0,
                    "purchase_rate": 0,
                    "ftewv_count": 0,
                    "cost_per_ftewv": 0,
                    "ncp_count": 0,
                    "cost_per_ncp": 0,
                    "ltv_reach": 0,
                    "ltv_frequency": 0,
                }
            )

    log.info(
        f"  Inserting placeholders: {len(needs_placeholder)} non-delivering active ads × {len(days)} days = {len(rows)} rows"
    )
    return upsert_rows(rows)


def sync(since: str, until: str, label: str):
    log.info(f"\n{'='*60}")
    log.info(f"{label.upper()} | {since} → {until} (GMT)")
    log.info(f"{'='*60}")

    total_f = total_u = 0
    total_p = 0
    start = datetime.now()

    for acct in ACCOUNTS:
        log.info(f"\n▶  {acct['name']}")

        # Get custom conversion IDs for this account
        conv_ids = get_custom_conversion_ids(acct["id"])
        ftewv_id = conv_ids.get(CUSTOM_METRIC_FTEWV.lower().strip())
        ncp_id = conv_ids.get(CUSTOM_METRIC_NCP.lower().strip())

        if ftewv_id:
            log.info(f"  FTEWV ID: {ftewv_id}")
        else:
            log.warning(f"  FTEWV custom conversion not found — will store 0")

        if ncp_id:
            log.info(f"  NCP ID: {ncp_id}")
        else:
            log.warning(f"  NCP custom conversion not found — will store 0")

        af = au = 0
        delivered_ad_ids: set = set()
        for chunk_since, chunk_until in date_chunks(since, until):
            f, u, ids = fetch_and_upsert(
                acct["id"], acct["name"], chunk_since, chunk_until, ftewv_id, ncp_id
            )
            af += f
            au += u
            delivered_ad_ids.update(ids)

        # Pull every ad in the account, then placeholder-fill ones that didn't deliver
        try:
            active_ads = fetch_all_ads(acct["id"])
            placeholder_n = upsert_placeholders(
                acct["name"], active_ads, delivered_ad_ids, since, until
            )
            total_p += placeholder_n
        except Exception as e:
            log.error(f"  Placeholder fill failed: {e}")
            placeholder_n = 0

        log.info(
            f"  ── {acct['name']}: {af:,} fetched, {au:,} upserted, {placeholder_n:,} placeholders"
        )
        total_f += af
        total_u += au

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"\n{'='*60}")
    log.info(
        f"DONE | {total_f:,} fetched | {total_u:,} upserted | {total_p:,} placeholders | {elapsed:.0f}s"
    )
    log.info(f"{'='*60}\n")


def backfill():
    """One-time backfill: Jan 1 2026 → today into primary_table."""
    since = "2026-01-01"
    until = gmt_today()
    total_days = (date.fromisoformat(until) - date.fromisoformat(since)).days
    n_chunks = (total_days // CHUNK_DAYS + 1) * len(ACCOUNTS)
    log.info(f"\nPRIMARY TABLE BACKFILL")
    log.info(f"Range:     {since} → {until} ({total_days} days)")
    log.info(f"Accounts:  {len(ACCOUNTS)}")
    log.info(f"API calls: ~{n_chunks} chunks × 3 accounts")
    log.info(f"Note: LTV stats fetched per chunk (lifetime per ad)")
    log.info(f"Tip: tail -f primary_sync.log to watch\n")
    sync(since, until, "primary backfill")
    log.info(f"Total rows in {TABLE}: {get_row_count():,}")


def daily():
    """Last 15 GMT days → primary_table."""
    sync(gmt_days_ago(15), gmt_today(), "daily sync")


def hourly():
    """Last 3 GMT days → primary_table."""
    sync(gmt_days_ago(3), gmt_today(), "hourly sync")


def status():
    today = gmt_today()
    count = get_row_count()
    dates = get_latest_dates()
    log.info(f"\nSTATUS | {TABLE} | GMT today: {today}")
    log.info(f"Total rows: {count:,}")
    for acct in ACCOUNTS:
        latest = dates.get(acct["name"], "no data")
        lag = ""
        if latest and latest != "no data":
            days = (date.fromisoformat(today) - date.fromisoformat(latest)).days
            lag = f"  ({days}d behind)" if days > 0 else "  ✅ up to date"
        log.info(f"  {acct['name']}: {latest}{lag}")


# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"

    # Test connection
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {TABLE};")
                n = cur.fetchone()[0]
                log.info(f"Connected. {TABLE} has {n:,} rows.")
    except Exception as e:
        log.error(f"Cannot connect: {e}")
        sys.exit(1)

    if cmd == "backfill":
        backfill()
    elif cmd == "daily":
        daily()
    elif cmd == "hourly":
        hourly()
    elif cmd == "status":
        status()
    else:
        print(f"Unknown: {cmd}")
        print("Usage: python primary_sync.py [backfill|daily|hourly|status]")
        sys.exit(1)
