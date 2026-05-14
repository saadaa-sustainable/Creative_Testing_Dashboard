"""
hourly_impressions.py — Refreshes results_table with today's Meta data combined
with the last 30 days of primary_table history (READ-ONLY on primary_table).

Triggered hourly by cron.py. Keeps the dashboard's fatigue/frequency aggregates
fresh without the cost of a full primary_sync.

Flow:
  1. For each account, fetch today's Meta /insights (date_preset=today, level=ad).
  2. SELECT last-30-day rows from primary_table for that account.
  3. Merge: prior 29 days from primary_table + today's fresh Meta data
     (we drop primary_table's today rows since they are stale by definition).
  4. Aggregate per ad_id → write one results_table row per account
     + one combined "All Accounts" row.
  5. Purge older results_table rows (keep 1 per account).

Usage:
    python hourly_impressions.py
"""

import os
import sys
import json
import logging
import math
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────
META_TOKEN      = os.getenv("META_ACCESS_TOKEN")
META_API_VER    = os.getenv("META_API_VERSION", "v21.0")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

ACCOUNTS = [
    {"name": "Raho Saadaa",            "id": os.getenv("ACCOUNT_1_ID", "1136644150469466")},
    {"name": "Fourth Ad Account - SD", "id": os.getenv("ACCOUNT_2_ID", "1349767139294217")},
    {"name": "Third Ad Account - SD",  "id": os.getenv("ACCOUNT_3_ID", "264868699479122")},
]

DAYS_WINDOW = 30  # rolling window for aggregates (must match results_sync.py)

# ── Logging ─────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "hourly_impressions.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("hourly_impressions")


# ── DB helpers ─────────────────────────────────────────────────────
def get_conn():
    """Connection with extended statement timeout (Supabase pooler default is too tight)."""
    conn = psycopg2.connect(SUPABASE_DB_URL, connect_timeout=30)
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '300s'")
    except Exception:
        pass
    return conn


def fetch_primary_window(conn, account_name, since_date):
    """READ-ONLY pull of primary_table rows for an account, from since_date onwards."""
    cols = (
        "ad_id, ad_name, ad_status, ad_created_date, campaign_name, "
        "date, impressions, reach, amount_spent_inr, conversion_value, "
        "outbound_clicks, thruplays, three_sec_video_plays, post_engagements, "
        "video_play_time, ftewv_count, ncp_count, "
        "ltv_reach, ltv_frequency, preview_link, ad_link"
    )
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT {cols} FROM primary_table WHERE account_name = %s AND date >= %s ORDER BY date DESC",
            (account_name, since_date),
        )
        return cur.fetchall()


# ── Meta API ───────────────────────────────────────────────────────
def _action_val(actions, action_type):
    """Extract value for a specific action_type from Meta actions array."""
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == action_type:
            return float(a.get("value", 0) or 0)
    return 0.0


def fetch_today_from_meta(account_id, account_name):
    """Fetch today's ad-level insights from Meta for one account.
    Returns dict: ad_id → {impressions, reach, spend, conversion_value, outbound_clicks, ...}.
    """
    today_data = {}
    if not META_TOKEN:
        log.warning("  META_ACCESS_TOKEN not set — skipping Meta fetch")
        return today_data

    url = f"https://graph.facebook.com/{META_API_VER}/act_{account_id}/insights"
    params = {
        "level": "ad",
        "date_preset": "today",
        "fields": ",".join([
            "ad_id", "ad_name", "campaign_name",
            "impressions", "reach", "spend", "frequency",
            "actions", "action_values", "outbound_clicks",
            "video_thruplay_watched_actions", "inline_post_engagement",
        ]),
        "limit": 500,
        "access_token": META_TOKEN,
    }

    try:
        page_num = 0
        while True:
            page_num += 1
            r = requests.get(url, params=params, timeout=60)
            if not r.ok:
                log.warning(f"  Meta {r.status_code} on page {page_num}: {r.text[:160]}")
                break
            body = r.json()
            if "error" in body:
                log.warning(f"  Meta API error: {body['error']}")
                break

            for row in body.get("data", []):
                ad_id = str(row.get("ad_id", "")).strip()
                if not ad_id:
                    continue
                actions = row.get("actions", []) or []
                action_vals = row.get("action_values", []) or []
                thruplay = row.get("video_thruplay_watched_actions", []) or []

                today_data[ad_id] = {
                    "ad_name": row.get("ad_name") or "",
                    "campaign_name": row.get("campaign_name") or "",
                    "impr":   int(float(row.get("impressions", 0) or 0)),
                    "reach":  int(float(row.get("reach", 0) or 0)),
                    "spend":  float(row.get("spend", 0) or 0),
                    "conv_v": _action_val(action_vals, "purchase"),
                    "clicks": int(_action_val(row.get("outbound_clicks", []), "outbound_click")
                                  if isinstance(row.get("outbound_clicks"), list)
                                  else (row.get("outbound_clicks", 0) or 0)),
                    "thru":   int(_action_val(thruplay, "video_thruplay_watched")),
                    "vid3":   int(_action_val(actions, "video_view")),
                    "eng":    int(_action_val(actions, "post_engagement")
                                  or (row.get("inline_post_engagement", 0) or 0)),
                    "ftewv":  int(_action_val(actions, "offsite_conversion.custom.1133449967928420")),
                    "ncp":    int(_action_val(actions, "offsite_conversion.custom.1109740267306786")),
                }

            paging = body.get("paging", {}) or {}
            next_url = paging.get("next")
            if not next_url:
                break
            url = next_url
            params = {}  # next URL already has params embedded
    except Exception as e:
        log.warning(f"  [{account_name}] Meta fetch error: {e}")

    return today_data


# ── Aggregation ────────────────────────────────────────────────────
def compute_aggregates(primary_rows, today_data, account_name):
    """Combine historical primary_table rows + today's Meta data → list of per-ad dicts.
    Drops primary_table's today rows since the Meta call gives a fresher value."""
    today_str = date.today().isoformat()
    ad_map = {}

    # ── Pass 1: history (everything except today) ──
    for r in primary_rows:
        ad_id = str(r["ad_id"] or "").strip()
        if not ad_id:
            continue
        if str(r["date"]) == today_str:
            continue  # skip stale today row — Meta data below has fresh values

        if ad_id not in ad_map:
            ad_map[ad_id] = {
                "ad_id": ad_id,
                "ad_name": r["ad_name"] or "",
                "campaign_name": r["campaign_name"] or "",
                "ad_status": r["ad_status"] or "",
                "ad_created_date": str(r["ad_created_date"]) if r["ad_created_date"] else "",
                "preview_link": r["preview_link"] or "",
                "ad_link": r["ad_link"] or "",
                "impr": 0, "spend": 0.0, "reach": 0, "conv_v": 0.0,
                "clicks": 0, "thru": 0, "vid3": 0, "eng": 0,
                "ftewv": 0, "ncp": 0, "vpt_sum": 0.0,
                "ltv_reach": 0, "ltv_frequency": 0.0,
            }

        a = ad_map[ad_id]
        a["impr"]   += int(r["impressions"] or 0)
        a["spend"]  += float(r["amount_spent_inr"] or 0)
        a["reach"]  += int(r["reach"] or 0)
        a["conv_v"] += float(r["conversion_value"] or 0)
        a["clicks"] += int(r["outbound_clicks"] or 0)
        a["thru"]   += int(r["thruplays"] or 0)
        a["vid3"]   += int(r["three_sec_video_plays"] or 0)
        a["eng"]    += int(r["post_engagements"] or 0)
        a["ftewv"]  += int(r["ftewv_count"] or 0)
        a["ncp"]    += int(r["ncp_count"] or 0)
        a["vpt_sum"] += float(r["video_play_time"] or 0) * int(r["impressions"] or 0)

        lr = float(r["ltv_reach"] or 0)
        lf = float(r["ltv_frequency"] or 0)
        if lr > a["ltv_reach"]:     a["ltv_reach"] = lr
        if lf > a["ltv_frequency"]: a["ltv_frequency"] = lf

    # ── Pass 2: merge today's Meta data ──
    for ad_id, t in today_data.items():
        if ad_id not in ad_map:
            # Brand new ad (first day, no primary_table history yet)
            ad_map[ad_id] = {
                "ad_id": ad_id,
                "ad_name": t["ad_name"],
                "campaign_name": t["campaign_name"],
                "ad_status": "ACTIVE",  # safe default; refined nightly by primary_sync
                "ad_created_date": today_str,
                "preview_link": "", "ad_link": "",
                "impr": 0, "spend": 0.0, "reach": 0, "conv_v": 0.0,
                "clicks": 0, "thru": 0, "vid3": 0, "eng": 0,
                "ftewv": 0, "ncp": 0, "vpt_sum": 0.0,
                "ltv_reach": 0, "ltv_frequency": 0.0,
            }
        a = ad_map[ad_id]
        a["impr"]   += t["impr"]
        a["spend"]  += t["spend"]
        a["reach"]  += t["reach"]
        a["conv_v"] += t["conv_v"]
        a["clicks"] += t["clicks"]
        a["thru"]   += t["thru"]
        a["vid3"]   += t["vid3"]
        a["eng"]    += t["eng"]
        a["ftewv"]  += t["ftewv"]
        a["ncp"]    += t["ncp"]

    return list(ad_map.values())


# ── Writer ─────────────────────────────────────────────────────────
def sdv(n, d):
    return 0.0 if not d or d == 0 else n / d


# Reuse detection helpers from results_sync
PRODUCTS = [
    "BST","BCO","COO","SDAFD","CL","TTB","TW","GE","SDCPT","SDELKB","SDELPT",
    "SDFLK","SDFSK","SDCSS","SDLS","SDRPT","SDRST","SDVPL","SMCP","SMELMS",
    "SMFLK","SMFSK","SMLS","BR","SDASP","SDCP","SDWLP","GP","SUZNS","WW",
    "STL","LE","WLP","SDLWC","SDTTB","SDECT","SDALS","WBS","SD5","SUPFH",
    "SDAWP","SDCSP",
]

def detect_product(name):
    import re
    n = (name or "").upper()
    for t in re.split(r"[\+\-\_\s\.]+", n):
        if t in PRODUCTS: return t
    for p in PRODUCTS:
        if len(p) > 2 and p in n: return p
    return "Other"

def detect_ctype(name):
    n = (name or "").upper()
    if "IFAD" in n: return "IFAD"
    if "GAD" in n:  return "Graphic AD"
    if any(k in n for k in ("VRP","NNC","VIDEO","IGP")): return "VID"
    if any(k in n for k in ("STATIC","_ST_","+ST+")):    return "STATIC"
    return "VID"

def is_copy(name):
    import re
    return bool(re.search(r"copy", name or "", re.IGNORECASE))


def write_results_row(conn, account_name, ads, since_date, today):
    """Insert a fresh results_table row for the account."""
    total_spend = sum(a["spend"] for a in ads)
    total_impr  = sum(a["impr"] for a in ads)
    total_reach = sum(a["reach"] for a in ads)
    total_conv  = sum(a["conv_v"] for a in ads)
    total_thru  = sum(a["thru"] for a in ads)
    total_vid3  = sum(a["vid3"] for a in ads)
    total_clicks= sum(a["clicks"] for a in ads)
    total_eng   = sum(a["eng"] for a in ads)

    ct_roas    = sdv(total_conv, total_spend)
    hook_rate  = sdv(total_vid3, total_impr) * 100
    hold_rate  = sdv(total_thru, total_vid3) * 100
    thru_rate  = sdv(total_thru, total_impr) * 100
    ob_ctr     = sdv(total_clicks, total_impr) * 100
    eng_rate   = sdv(total_eng,   total_impr) * 100

    ads_json = [{
        "adId": a["ad_id"],
        "adName": a["ad_name"],
        "acct": account_name,
        "campName": a["campaign_name"],
        "adStatus": a["ad_status"],
        "adCreated": a["ad_created_date"],
        "ctype": detect_ctype(a["ad_name"]),
        "product": detect_product(a["ad_name"]),
        "isCopy": is_copy(a["ad_name"]),
        "impr":  int(a["impr"]),
        "spend": round(a["spend"], 2),
        "reach": int(a["reach"]),
        "clicks": int(a["clicks"]),
        "thru":  int(a["thru"]),
        "vid3":  int(a["vid3"]),
        "eng":   int(a["eng"]),
        "convV": round(a["conv_v"], 2),
        "vpt":   round(sdv(a.get("vpt_sum", 0), a["impr"]), 3),
        "ftewv": int(a["ftewv"]),
        "cpf":   round(sdv(a["spend"], a["ftewv"]), 2),
        "ncp":   int(a["ncp"]),
        "cpn":   round(sdv(a["spend"], a["ncp"]), 2),
        "ltvReach": int(a["ltv_reach"]),
        "ltvFreq":  round(a["ltv_frequency"], 4),
        "preview": a["preview_link"],
        "adLink":  a["ad_link"],
    } for a in ads]

    cols = (
        "account_name, computed_at, data_date_from, data_date_to, total_raw_rows, "
        "ads_json, total_spend, total_impr, total_reach, total_conv_value, "
        "total_thru_plays, total_video_plays, total_out_clicks, total_post_eng, "
        "ct_roas, hook_rate, hold_rate, thruplay_rate, outbound_ctr, engagement_rate, "
        "count_total_ads"
    )
    placeholders = ", ".join(["%s"] * 21)
    sql = f"INSERT INTO results_table ({cols}) VALUES ({placeholders})"
    with conn.cursor() as cur:
        cur.execute(sql, (
            account_name,
            datetime.now(timezone.utc).isoformat(),
            since_date.isoformat(),
            today.isoformat(),
            len(ads),
            json.dumps(ads_json),
            round(total_spend, 2),
            int(total_impr),
            int(total_reach),
            round(total_conv, 2),
            int(total_thru),
            int(total_vid3),
            int(total_clicks),
            int(total_eng),
            round(ct_roas, 4),
            round(hook_rate, 4),
            round(hold_rate, 4),
            round(thru_rate, 4),
            round(ob_ctr, 4),
            round(eng_rate, 4),
            len(ads),
        ))
    conn.commit()


def purge_old_results(conn, keep_per_account=1):
    """Keep only the latest N rows per account in results_table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM results_table
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY account_name
                               ORDER BY computed_at DESC
                           ) AS rn
                    FROM results_table
                ) ranked
                WHERE rn <= %s
            )
            """,
            (keep_per_account,),
        )
    conn.commit()


# ── Main ───────────────────────────────────────────────────────────
def main():
    today = date.today()
    since_date = today - timedelta(days=DAYS_WINDOW - 1)

    log.info("=" * 60)
    log.info("  HOURLY IMPRESSIONS REFRESH")
    log.info(f"  Window: {since_date.isoformat()} → {today.isoformat()}")
    log.info("=" * 60)

    if not META_TOKEN:
        log.error("META_ACCESS_TOKEN missing — aborting")
        sys.exit(1)
    if not SUPABASE_DB_URL:
        log.error("SUPABASE_DB_URL missing — aborting")
        sys.exit(1)

    conn = get_conn()
    try:
        all_ads_combined = {}

        for acct in ACCOUNTS:
            log.info(f"▶ {acct['name']}")

            log.info("  Fetching today's Meta data...")
            today_data = fetch_today_from_meta(acct["id"], acct["name"])
            log.info(f"  → {len(today_data)} ads from Meta")

            log.info(f"  Reading primary_table from {since_date}...")
            primary_rows = fetch_primary_window(conn, acct["name"], since_date)
            log.info(f"  → {len(primary_rows)} historical rows")

            ads = compute_aggregates(primary_rows, today_data, acct["name"])
            log.info(f"  → {len(ads)} unique ads aggregated")

            write_results_row(conn, acct["name"], ads, since_date, today)
            log.info(f"  ✓ results_table row written")

            # Add to combined map (dedup by ad_id)
            for a in ads:
                all_ads_combined[a["ad_id"]] = a

        log.info(f"▶ All Accounts (combined)")
        combined_list = list(all_ads_combined.values())
        write_results_row(conn, "All Accounts", combined_list, since_date, today)
        log.info(f"  ✓ Combined row written — {len(combined_list)} unique ads")

        purge_old_results(conn, keep_per_account=1)
        log.info("Purged old rows — keeping latest 1 per account")

    finally:
        conn.close()

    log.info(f"DONE at {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()
