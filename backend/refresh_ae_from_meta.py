"""
Refresh ae_table_view directly from the Meta Graph API.

Designed to run as an independent cron job (separate from primary_sync / run_backfill).
Fetches lifetime insights per account, computes Book1 metrics + lifecycle category,
and populates ae_table_view via TRUNCATE + INSERT.

Idempotent: re-running just refreshes the snapshot.

Usage:
    python refresh_ae_from_meta.py

Exit codes:
    0  success
    1  Meta API failure or DB write failure
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timezone, date, timedelta
from collections import defaultdict
from urllib.parse import quote_plus

import requests
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()
ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]
DB_URL = os.environ["SUPABASE_DB_URL"]

ACCOUNTS = [
    {"name": "Raho Saadaa",             "id": "1136644150469466"},
    {"name": "Fourth Ad Account - SD",  "id": "1349767139294217"},
    {"name": "Third Ad Account - SD",   "id": "264868699479122"},
]

# F1-F4 thresholds (same as dashboard defaults)
T_IMP, T_ROAS, T_CPN, T_CPFT = 50000, 3.2, 525, 12

# Chunked-fetch configuration (avoid Meta dev-tier rate limit)
# Meta's /insights endpoint rejects time_range start beyond 37 months ago.
# We use 36 months for safety; computed at runtime.
EARLIEST_DELIVERY = (date.today() - timedelta(days=36 * 30))
CHUNK_DAYS_PRIMARY = 5                 # First-pass chunk size — 5-day windows only
CHUNK_DAYS_FALLBACK = 5                # No sub-fallback when primary == fallback

# Blended Eff weights
W = {"X": 0.10, "Y": 0.25, "Z": 0.15, "AA": 0.20, "AB": 0.20, "AC": 0.10}

INSIGHT_FIELDS = ",".join([
    "ad_id", "ad_name", "campaign_name", "campaign_id", "adset_id", "adset_name",
    "date_start", "date_stop",
    "impressions", "reach", "frequency", "spend",
    "inline_link_clicks", "inline_link_click_ctr", "cost_per_inline_link_click",
    "purchase_roas", "actions", "cost_per_action_type",
])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("ae_meta")


def meta_get(url, retries=3):
    """GET with retry on transient errors. Max 3 attempts total.

    Retryable:
      - 429 (rate-limited)
      - 5xx (server-side)
      - 403 with error code 4 (app-level rate limit — Meta marks as transient)
      - Network exceptions
    """
    for i in range(retries):
        try:
            r = requests.get(url, timeout=180)
        except requests.RequestException as e:
            log.warning(f"Network error: {e}; retry {i+1}/{retries}")
            time.sleep(20 * (i + 1))
            continue
        if r.status_code == 200:
            return r.json()
        # Decide retryability
        retryable = r.status_code in (429, 500, 502, 503, 504)
        if r.status_code == 403:
            try:
                body = r.json()
                code = body.get("error", {}).get("code")
                if code == 4:                            # Application request limit reached
                    retryable = True
            except Exception:
                pass
        if retryable and i < retries - 1:
            # Longer backoff for app-level rate limit (Meta needs minutes, not seconds)
            sleep_s = 60 * (i + 1) if r.status_code == 403 else 30 * (i + 1)
            log.warning(f"HTTP {r.status_code} (retry {i+1}/{retries}); sleeping {sleep_s}s")
            time.sleep(sleep_s)
            continue
        log.error(f"HTTP {r.status_code}: {r.text[:300]}")
        r.raise_for_status()
    raise RuntimeError(f"Meta API failed after {retries} retries")


def get_custom_conversion_id(account_id, name):
    name_n = name.lower().strip()
    url = (
        f"https://graph.facebook.com/v21.0/act_{account_id}/customconversions"
        f"?fields=name,id&limit=200&access_token={ACCESS_TOKEN}"
    )
    try:
        j = meta_get(url)
    except Exception:
        return None
    for c in j.get("data", []):
        if c.get("name", "").lower().strip() == name_n:
            return c["id"]
    return None


def action_val(actions, *types):
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") in types:
            return float(a.get("value", 0) or 0)
    return 0.0


def _date_chunks(since, until, days):
    """Yield (chunk_since, chunk_until) date pairs covering [since, until] in `days`-day windows."""
    cur = since
    while cur <= until:
        end = min(cur + timedelta(days=days - 1), until)
        yield cur, end
        cur = end + timedelta(days=1)


def _fetch_chunk(account_id, fields, since, until, time_increment=None, limit=500):
    """Single chunked insights call with pagination. Returns flat row list. Raises on hard failure."""
    tr = json.dumps({"since": str(since), "until": str(until)})
    qs = (
        f"?level=ad&fields={fields}&time_range={quote_plus(tr)}"
        f"&limit={limit}&access_token={ACCESS_TOKEN}"
    )
    if time_increment:
        qs += f"&time_increment={time_increment}"
    url = f"https://graph.facebook.com/v21.0/act_{account_id}/insights{qs}"
    out = []
    while url:
        j = meta_get(url)
        out.extend(j.get("data", []))
        url = j.get("paging", {}).get("next")
    return out


def fetch_chunked_insights(account_id, account_name, fields, time_increment=None, label=""):
    """Fetch insights for full lifetime in 100-day chunks. Falls back to 10-day chunks on 403."""
    today = date.today()
    all_rows = []
    chunks = list(_date_chunks(EARLIEST_DELIVERY, today, CHUNK_DAYS_PRIMARY))
    log.info(f"  {account_name} {label}: {len(chunks)} primary chunks ({CHUNK_DAYS_PRIMARY}-day windows)")

    for i, (since, until) in enumerate(chunks, 1):
        try:
            rows = _fetch_chunk(account_id, fields, since, until, time_increment)
            all_rows.extend(rows)
            log.info(f"    [{i}/{len(chunks)}] {since} → {until}: {len(rows)} rows (total {len(all_rows)})")
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            # Fall back to smaller windows on:
            #  - 403 (rate limit) → spreading calls in time helps
            #  - 500 (Meta "reduce the amount of data" error code 1) → smaller window has less data
            should_fallback = status in (403, 500) and CHUNK_DAYS_PRIMARY > CHUNK_DAYS_FALLBACK
            if should_fallback:
                log.warning(
                    f"    [{i}/{len(chunks)}] {since} → {until} failed HTTP {status}; "
                    f"retrying with {CHUNK_DAYS_FALLBACK}-day sub-chunks"
                )
                sub_chunks = list(_date_chunks(since, until, CHUNK_DAYS_FALLBACK))
                for j, (s_since, s_until) in enumerate(sub_chunks, 1):
                    try:
                        sub_rows = _fetch_chunk(account_id, fields, s_since, s_until, time_increment)
                        all_rows.extend(sub_rows)
                        log.info(f"      sub [{j}/{len(sub_chunks)}] {s_since} → {s_until}: {len(sub_rows)} rows")
                    except Exception as e2:
                        log.error(f"      sub {s_since} → {s_until} failed: {e2}; skipping")
            else:
                log.error(f"    [{i}/{len(chunks)}] {since} → {until} unrecoverable: {e}; skipping")
    return all_rows


def aggregate_chunks_to_lifetime(chunk_rows):
    """Merge multiple chunk rows per ad into single lifetime row.

    Sum: impressions, reach (NOT deduped across chunks — limitation), spend, link_clicks
    Sum per action_type: actions array
    Recompute purchase_roas: Σ(roas × chunk_spend) / Σ(chunk_spend)
    """
    by_ad = {}
    for row in chunk_rows:
        ad_id = row.get("ad_id")
        if not ad_id:
            continue
        if ad_id not in by_ad:
            by_ad[ad_id] = {
                "ad_id": ad_id,
                "ad_name": row.get("ad_name"),
                "campaign_name": row.get("campaign_name"),
                "campaign_id": row.get("campaign_id"),
                "adset_id": row.get("adset_id"),
                "adset_name": row.get("adset_name"),
                "date_start": row.get("date_start"),
                "date_stop": row.get("date_stop"),
                "impressions": 0.0,
                "reach": 0.0,
                "spend": 0.0,
                "inline_link_clicks": 0.0,
                "_actions": defaultdict(float),
                "_proas_conv_sum": 0.0,
                "_proas_spend_sum": 0.0,
            }
        ad = by_ad[ad_id]
        # keep latest non-null metadata
        for k in ("ad_name", "campaign_name", "campaign_id", "adset_id", "adset_name"):
            v = row.get(k)
            if v:
                ad[k] = v
        # date_start = MIN, date_stop = MAX
        ds = row.get("date_start")
        if ds and (not ad["date_start"] or ds < ad["date_start"]):
            ad["date_start"] = ds
        de = row.get("date_stop")
        if de and (not ad["date_stop"] or de > ad["date_stop"]):
            ad["date_stop"] = de
        # sum metrics
        chunk_spend = float(row.get("spend") or 0)
        ad["impressions"] += float(row.get("impressions") or 0)
        ad["reach"] += float(row.get("reach") or 0)
        ad["spend"] += chunk_spend
        ad["inline_link_clicks"] += float(row.get("inline_link_clicks") or 0)
        # sum action counts
        for a in row.get("actions") or []:
            at = a.get("action_type")
            if at:
                ad["_actions"][at] += float(a.get("value") or 0)
        # weighted ROAS
        for r in row.get("purchase_roas") or []:
            if r.get("action_type") in ("omni_purchase", "purchase"):
                roas_val = float(r.get("value") or 0)
                ad["_proas_conv_sum"] += roas_val * chunk_spend
                ad["_proas_spend_sum"] += chunk_spend
                break

    # Convert back to flat row format compatible with parse_insight_row
    out = []
    for ad in by_ad.values():
        weighted_roas = (ad["_proas_conv_sum"] / ad["_proas_spend_sum"]) if ad["_proas_spend_sum"] > 0 else 0.0
        out.append({
            "ad_id": ad["ad_id"],
            "ad_name": ad["ad_name"],
            "campaign_name": ad["campaign_name"],
            "campaign_id": ad["campaign_id"],
            "adset_id": ad["adset_id"],
            "adset_name": ad["adset_name"],
            "date_start": ad["date_start"],
            "date_stop": ad["date_stop"],
            "impressions": ad["impressions"],
            "reach": ad["reach"],
            "spend": ad["spend"],
            "inline_link_clicks": ad["inline_link_clicks"],
            "actions": [{"action_type": at, "value": str(v)} for at, v in ad["_actions"].items()],
            "purchase_roas": [{"action_type": "omni_purchase", "value": str(weighted_roas)}] if weighted_roas > 0 else [],
            "cost_per_action_type": [],   # downstream recomputes from spend/count
        })
    return out


def fetch_lifetime_insights(account_id, account_name):
    """Chunked lifetime aggregated insights — one row per ad after aggregation."""
    chunk_rows = fetch_chunked_insights(
        account_id, account_name, INSIGHT_FIELDS, time_increment=None, label="lifetime"
    )
    aggregated = aggregate_chunks_to_lifetime(chunk_rows)
    log.info(f"  {account_name}: aggregated {len(chunk_rows)} chunk rows → {len(aggregated)} unique ads")
    return aggregated


def fetch_daily_insights(account_id, account_name):
    """Chunked daily granular insights (one row per ad per day) — used for F1-hit dates."""
    return fetch_chunked_insights(
        account_id, account_name,
        fields="ad_id,date_start,impressions",
        time_increment=1, label="daily"
    )


def compute_f1_hit_dates(daily_rows, target_imp=T_IMP):
    """Returns {ad_id: f1_hit_date_str (YYYY-MM-DD) or None}."""
    from collections import defaultdict
    by_ad = defaultdict(list)
    for row in daily_rows:
        ad_id = row.get("ad_id")
        if not ad_id:
            continue
        by_ad[ad_id].append((row.get("date_start"), float(row.get("impressions", 0) or 0)))
    hits = {}
    for ad_id, days in by_ad.items():
        days.sort(key=lambda x: x[0] or "")
        cum = 0.0
        hits[ad_id] = None
        for date, imp in days:
            cum += imp
            if cum >= target_imp:
                hits[ad_id] = date
                break
    return hits


def fetch_ad_metadata(ad_ids):
    """Returns {ad_id: {created_time, effective_status, preview_shareable_link, ad_link}}."""
    out = {}
    for i in range(0, len(ad_ids), 50):
        chunk = ad_ids[i:i + 50]
        url = (
            f"https://graph.facebook.com/v21.0/?ids={','.join(chunk)}"
            f"&fields=created_time,effective_status,preview_shareable_link"
            f"&access_token={ACCESS_TOKEN}"
        )
        try:
            j = meta_get(url)
        except Exception as e:
            log.warning(f"  metadata batch {i//50} failed: {e}")
            continue
        for ad_id, info in j.items():
            if isinstance(info, dict):
                out[ad_id] = {
                    "created_time": (info.get("created_time") or "")[:10] or None,
                    "effective_status": info.get("effective_status", ""),
                    "preview_link": info.get("preview_shareable_link", ""),
                }
    return out


def parse_insight_row(row, account_name, ftewv_id, ncp_id, ad_meta_map, f1_hit_map):
    spend = float(row.get("spend") or 0)
    reach = float(row.get("reach") or 0)
    imp = float(row.get("impressions") or 0)
    link_clicks = float(row.get("inline_link_clicks") or 0)

    actions = row.get("actions") or []
    purchases = action_val(actions, "omni_purchase", "purchase")
    ci = action_val(actions, "omni_initiated_checkout", "initiate_checkout")
    atc = action_val(actions, "omni_add_to_cart", "add_to_cart")
    ftewv = action_val(actions, f"offsite_conversion.custom.{ftewv_id}") if ftewv_id else 0
    ncp = action_val(actions, f"offsite_conversion.custom.{ncp_id}") if ncp_id else 0

    # ROAS direct from Meta
    purchase_roas = 0.0
    for r in row.get("purchase_roas") or []:
        if r.get("action_type") in ("omni_purchase", "purchase"):
            purchase_roas = float(r.get("value", 0) or 0)
            break
    conv_value = purchase_roas * spend

    ad_id = row.get("ad_id", "")
    meta = ad_meta_map.get(ad_id, {})

    # Result-timing fields — derived from F1-hit date + ad_created/first_seen
    first_seen = row.get("date_start")
    ad_created = meta.get("created_time")
    f1_hit = f1_hit_map.get(ad_id)  # 'YYYY-MM-DD' or None
    date_target_imp = f1_hit
    if f1_hit:
        date_of_result = f1_hit
    else:
        base = ad_created or first_seen
        if base:
            from datetime import datetime, timedelta
            try:
                base_dt = datetime.strptime(base, "%Y-%m-%d")
                date_of_result = (base_dt + timedelta(days=14)).strftime("%Y-%m-%d")
            except Exception:
                date_of_result = None
        else:
            date_of_result = None

    def days_between(a, b):
        if not a or not b:
            return None
        from datetime import datetime
        try:
            return max(0, (datetime.strptime(a, "%Y-%m-%d") - datetime.strptime(b, "%Y-%m-%d")).days)
        except Exception:
            return None

    return {
        "ad_id": ad_id,
        "account_name": account_name,
        "ad_name": row.get("ad_name") or "",
        "campaign_name": row.get("campaign_name") or "",
        "adset_id": row.get("adset_id") or "",
        "adset_name": row.get("adset_name") or "",
        "ad_status": meta.get("effective_status", ""),
        "ad_created": ad_created,
        "reporting_starts": first_seen,
        "reporting_ends": row.get("date_stop"),
        "date_target_imp_achieved": date_target_imp,
        "date_of_result": date_of_result,
        "days_to_result": days_between(date_of_result, first_seen),
        "days_to_target_f1": days_between(f1_hit, first_seen) if f1_hit else None,
        "preview_link": meta.get("preview_link", ""),
        "impressions": int(imp),
        "reach": int(reach),
        "amount_spent": round(spend, 2),
        "link_clicks": int(link_clicks),
        "purchases": round(purchases, 2),
        "ci": round(ci, 2),
        "atc": round(atc, 2),
        "conv_value": round(conv_value, 2),
        "ftewv_count": int(ftewv),
        "ncp_count": int(ncp),
    }


def median(values):
    if not values:
        return 0
    s = sorted(values)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def safe_div(num, den):
    return num / den if den else 0


def compute_row(ad, anchors=None, medians=None):
    """Build a raw row for ae_raw_view. All derived columns (reach_weight, eff scores,
    category, etc.) are computed by the ae_table_view VIEW on read — not stored.
    `anchors` and `medians` kept as args for backward-compat but unused.
    """
    reach = ad["reach"]
    spend = ad["amount_spent"]
    conv = ad["conv_value"]
    imp = ad["impressions"]
    ftewv = ad["ftewv_count"]
    ncp = ad["ncp_count"]
    link_clicks = ad["link_clicks"]
    purchases = ad["purchases"]
    ci = ad["ci"]
    atc = ad["atc"]

    # Raw base metrics from Meta (these ARE stored — needed as inputs to the VIEW)
    frequency = safe_div(imp, reach)
    cpr_1000 = safe_div(spend, reach) * 1000
    cpc_link = safe_div(spend, link_clicks)
    ctr_pct = safe_div(link_clicks, imp) * 100
    chk_compl = safe_div(purchases, ci) * 100
    cr_lc = safe_div(purchases, link_clicks) * 100
    atc_lc = safe_div(atc, link_clicks) * 100
    ci_atc = safe_div(ci, atc) * 100
    roas_ma = safe_div(conv, spend)
    cost_per_ftewv = safe_div(spend, ftewv)
    cost_per_ncp = safe_div(spend, ncp)

    # F-flags (booleans — stored since they're inputs to the VIEW's category logic)
    f1 = imp >= T_IMP
    f2 = spend > 0 and roas_ma >= T_ROAS
    f3 = ncp > 0 and 0 < cost_per_ncp <= T_CPN
    f4 = ftewv > 0 and 0 < cost_per_ftewv <= T_CPFT

    return (
        ad["account_name"], ad["campaign_name"], ad["adset_id"], ad["adset_name"],
        ad["ad_id"], ad["ad_name"], ad["ad_created"], ad["reporting_starts"], ad["reporting_ends"],
        ad.get("date_target_imp_achieved"), ad.get("date_of_result"),
        ad.get("days_to_result"), ad.get("days_to_target_f1"),
        ad["ad_status"], f1, f2, f3, f4,
        imp, reach,
        round(cpr_1000, 2) if cpr_1000 else None,
        round(frequency, 2) if frequency else None,
        round(spend, 2),
        round(cpc_link, 2) if cpc_link else None,
        round(ctr_pct, 2) if ctr_pct else None,
        round(chk_compl, 2) if chk_compl else None,
        round(cr_lc, 2) if cr_lc else None,
        round(atc_lc, 2) if atc_lc else None,
        round(ci_atc, 2) if ci_atc else None,
        round(roas_ma, 3),
        round(cost_per_ftewv, 2) if cost_per_ftewv else None,
        ftewv,
        round(cost_per_ncp, 2) if cost_per_ncp else None,
        ncp, None, None, 0,  # ltv_reach, ltv_frequency, engagement_count
        ad["preview_link"], "",
        round(conv, 2),
        round(purchases, 2),
        link_clicks,
        round(ci, 2),
        round(atc, 2),
        "meta_api",
    )


INSERT_SQL = """
    INSERT INTO ae_raw_view (
        account_name, campaign_name, adset_id, adset_name, ad_id, ad_name,
        ad_created, reporting_starts, reporting_ends,
        date_target_imp_achieved, date_of_result, days_to_result, days_to_target_f1,
        ad_status, f1_pass, f2_pass, f3_pass, f4_pass,
        impressions, reach, cost_per_1000, frequency,
        amount_spent, cpc_link, ctr_pct, checkout_compl_pct, cr_link_clicks_pct,
        atc_lc_pct, ci_atc_pct, roas_ma, cost_per_ftewv, ftewv_count,
        cost_per_ncp,
        ncp_count, ltv_reach, ltv_frequency, engagement_count, preview_link, ad_link,
        conv_value, purchases, link_clicks_raw, ci_count, atc_count, source
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (ad_id) DO UPDATE SET
        account_name=EXCLUDED.account_name, campaign_name=EXCLUDED.campaign_name,
        adset_id=EXCLUDED.adset_id, adset_name=EXCLUDED.adset_name, ad_name=EXCLUDED.ad_name,
        ad_created=EXCLUDED.ad_created, reporting_starts=EXCLUDED.reporting_starts,
        reporting_ends=EXCLUDED.reporting_ends,
        date_target_imp_achieved=EXCLUDED.date_target_imp_achieved,
        date_of_result=EXCLUDED.date_of_result,
        days_to_result=EXCLUDED.days_to_result,
        days_to_target_f1=EXCLUDED.days_to_target_f1,
        ad_status=EXCLUDED.ad_status,
        f1_pass=EXCLUDED.f1_pass, f2_pass=EXCLUDED.f2_pass, f3_pass=EXCLUDED.f3_pass,
        f4_pass=EXCLUDED.f4_pass,
        impressions=EXCLUDED.impressions, reach=EXCLUDED.reach,
        cost_per_1000=EXCLUDED.cost_per_1000,
        frequency=EXCLUDED.frequency, amount_spent=EXCLUDED.amount_spent,
        cpc_link=EXCLUDED.cpc_link, ctr_pct=EXCLUDED.ctr_pct,
        checkout_compl_pct=EXCLUDED.checkout_compl_pct, cr_link_clicks_pct=EXCLUDED.cr_link_clicks_pct,
        atc_lc_pct=EXCLUDED.atc_lc_pct, ci_atc_pct=EXCLUDED.ci_atc_pct,
        roas_ma=EXCLUDED.roas_ma, cost_per_ftewv=EXCLUDED.cost_per_ftewv,
        ftewv_count=EXCLUDED.ftewv_count, cost_per_ncp=EXCLUDED.cost_per_ncp,
        ncp_count=EXCLUDED.ncp_count, preview_link=EXCLUDED.preview_link,
        conv_value=EXCLUDED.conv_value, purchases=EXCLUDED.purchases,
        link_clicks_raw=EXCLUDED.link_clicks_raw, ci_count=EXCLUDED.ci_count,
        atc_count=EXCLUDED.atc_count, source='meta_api',
        refreshed_at=NOW()
"""


def main():
    log.info("Starting ae_table_view refresh from Meta API")

    # Step 1: Fetch insights + metadata per account
    all_ads = []
    for acc in ACCOUNTS:
        log.info(f"Account: {acc['name']}")
        ftewv_id = get_custom_conversion_id(acc["id"], "First-time EWV")
        ncp_id = get_custom_conversion_id(acc["id"], "NCP")
        log.info(f"  FTEWV custom conversion id: {ftewv_id}")
        log.info(f"  NCP   custom conversion id: {ncp_id}")

        rows = fetch_lifetime_insights(acc["id"], acc["name"])
        log.info(f"  Fetched {len(rows)} ad insights (lifetime)")
        if not rows:
            continue
        ad_ids = [r["ad_id"] for r in rows if r.get("ad_id")]
        log.info(f"  Fetching metadata for {len(ad_ids)} ads (batched 50/req)…")
        ad_meta = fetch_ad_metadata(ad_ids)
        log.info(f"  Got metadata for {len(ad_meta)} ads")
        log.info(f"  Fetching daily insights for F1-hit date computation…")
        daily_rows = fetch_daily_insights(acc["id"], acc["name"])
        f1_hit_map = compute_f1_hit_dates(daily_rows, target_imp=T_IMP)
        log.info(f"  Computed F1-hit dates for {sum(1 for v in f1_hit_map.values() if v)} / {len(f1_hit_map)} ads")
        for row in rows:
            all_ads.append(parse_insight_row(row, acc["name"], ftewv_id, ncp_id, ad_meta, f1_hit_map))

    log.info(f"Total ads collected: {len(all_ads)}")
    if not all_ads:
        log.error("No ads to insert; exiting")
        return 1

    # Step 2: Global anchors + medians
    g_reach = sum(a["reach"] for a in all_ads)
    g_spend = sum(a["amount_spent"] for a in all_ads)
    g_ftewv = sum(a["ftewv_count"] for a in all_ads)
    g_ncp = sum(a["ncp_count"] for a in all_ads)
    g_conv = sum(a["conv_value"] for a in all_ads)
    anchors = {
        "g_reach": g_reach, "g_spend": g_spend, "g_ftewv": g_ftewv, "g_ncp": g_ncp,
        "anchor_cpr": (g_spend / g_reach * 1000) if g_reach > 0 else 0,
        "anchor_roas": (g_conv / g_spend) if g_spend > 0 else 0,
        "anchor_ftewv_pct": (g_ftewv / g_reach) if g_reach > 0 else 0,
        "anchor_ncp": (g_spend / g_ncp) if g_ncp > 0 else 0,
        "anchor_cpftewv": (g_spend / g_ftewv) if g_ftewv > 0 else 0,
    }
    medians = {
        "med_ftewv": median([a["ftewv_count"] for a in all_ads]),
        "med_profit": median([a["conv_value"] - a["amount_spent"] for a in all_ads]),
    }
    log.info(
        f"Anchors: gR={g_reach:,} gS={g_spend:,.0f} gF={g_ftewv:,} gN={g_ncp:,} "
        f"E1={anchors['anchor_cpr']:.2f} N1={anchors['anchor_roas']:.4f} "
        f"R1={anchors['anchor_ftewv_pct']:.6f} O3={anchors['anchor_cpftewv']:.2f} "
        f"Q1={anchors['anchor_ncp']:.2f}"
    )
    log.info(f"Medians: med_FT={medians['med_ftewv']:.0f}  med_PF={medians['med_profit']:.0f}")

    # Step 3: Build rows for INSERT (dedupe by ad_id, keep highest-reach)
    by_ad = {}
    for ad in all_ads:
        existing = by_ad.get(ad["ad_id"])
        if not existing or ad["reach"] > existing["reach"]:
            by_ad[ad["ad_id"]] = ad
    rows_to_insert = [compute_row(ad, anchors, medians) for ad in by_ad.values()]
    log.info(f"Rows prepared for INSERT: {len(rows_to_insert)} (deduplicated by ad_id)")

    # Step 4: Write to ae_table_view
    log.info("Connecting to Supabase…")
    conn = psycopg2.connect(DB_URL)
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '180s'")
        log.info("TRUNCATE ae_raw_view…")
        cur.execute("TRUNCATE ae_raw_view")
        log.info("Inserting rows (batched)…")
        execute_batch(cur, INSERT_SQL, rows_to_insert, page_size=500)
        conn.commit()
        # Categories come from the ae_table_view VIEW (derived from F-flags in ae_raw_view)
        cur.execute("SELECT COUNT(*), category FROM ae_table_view GROUP BY category ORDER BY COUNT(*) DESC")
        log.info("Per-category counts:")
        for row in cur.fetchall():
            log.info(f"  {row[1]:<25} {row[0]:>6,}")
        cur.close()
    finally:
        conn.close()

    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
