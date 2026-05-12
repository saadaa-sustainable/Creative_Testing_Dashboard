"""
results_sync.py
═══════════════════════════════════════════════════════════════════════════
Runs once nightly (after primary sync via cron.py).
Reads the LAST 30 DAYS of primary_table → aggregates dashboard metrics →
writes ONE compact row per account (+ one for All Accounts) into results_table.

results_table now holds a rolling-30-day snapshot per account. The dashboard
defaults to displaying this window. If the user picks a date range outside
the 30-day window, the frontend hits primary_table directly.

Usage:
    python results_sync.py              # all accounts, default 30-day window
    python results_sync.py --account 1  # single account only (for testing)
    python results_sync.py --days 60    # custom window length (override default)
"""

import os, json, math
from datetime import datetime, timezone, date, timedelta
from collections import defaultdict

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

# Rolling window length for the snapshot. 30 days = today inclusive minus 29.
DEFAULT_DAYS_WINDOW = 30

# ── Account map ────────────────────────────────────────────────────────
ACCOUNTS = [
    {"name": "Raho Saadaa"},
    {"name": "Fourth Ad Account - SD"},
    {"name": "Third Ad Account - SD"},
]

# ── Detection helpers (mirror of dashboard JS logic) ──────────────────
PRODUCTS = [
    "BST",
    "BCO",
    "COO",
    "SDAFD",
    "CL",
    "TTB",
    "TW",
    "GE",
    "SDCPT",
    "SDELKB",
    "SDELPT",
    "SDFLK",
    "SDFSK",
    "SDCSS",
    "SDLS",
    "SDRPT",
    "SDRST",
    "SDVPL",
    "SMCP",
    "SMELMS",
    "SMFLK",
    "SMFSK",
    "SMLS",
    "BR",
    "SDASP",
    "SDCP",
    "SDWLP",
    "GP",
    "SUZNS",
    "WW",
    "STL",
    "LE",
    "WLP",
    "SDLWC",
    "SDTTB",
    "SDECT",
    "SDALS",
    "WBS",
    "SD5",
    "SUPFH",
    "SDAWP",
    "SDCSP",
]


def detect_product(name: str) -> str:
    n = name.upper()
    import re

    tokens = re.split(r"[\+\-\_\s\.]+", n)
    for t in tokens:
        if t in PRODUCTS:
            return t
    for p in PRODUCTS:
        if len(p) > 2 and p in n:
            return p
    return "Other"


def detect_ctype(name: str) -> str:
    n = name.upper()
    if "IFAD" in n:
        return "IFAD"
    if "GAD" in n:
        return "Graphic AD"
    if any(k in n for k in ("VRP", "NNC", "VIDEO", "IGP")):
        return "VID"
    if any(k in n for k in ("STATIC", "_ST_", "+ST+")):
        return "STATIC"
    return "VID"  # default


def is_copy(name: str) -> bool:
    import re

    return bool(re.search(r"copy", name, re.IGNORECASE))


def sdv(n, d):
    return 0.0 if not d or math.isnan(d) else n / d


# ── DB helpers ────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(SUPABASE_DB_URL)


def fetch_primary(conn, account_name: str | None = None, since_date: date | None = None):
    """
    Pull rows from primary_table for one account (or all accounts), restricted
    to rows with date >= since_date when provided.
    Returns list of dicts.
    """
    cols = [
        "account_name",
        "date",
        "ad_name",
        "ad_id",
        "campaign_name",
        "ad_status",
        "ad_created_date",
        "impressions",
        "amount_spent_inr",
        "reach",
        "outbound_clicks",
        "thruplays",
        "three_sec_video_plays",
        "post_engagements",
        "conversion_value",
        "video_play_time",
        "purchase_roas",
        "ftewv_count",
        "ncp_count",
        "preview_link",
        "ad_link",
    ]
    col_str = ", ".join(cols)

    where = []
    params: list = []
    if account_name:
        where.append("account_name = %s")
        params.append(account_name)
    if since_date:
        where.append("date >= %s")
        params.append(since_date)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT {col_str} FROM primary_table{where_sql} ORDER BY date DESC",
            params,
        )
        return cur.fetchall()


# ── Core computation ──────────────────────────────────────────────────
def compute_results(
    rows: list, account_name: str, target_imp=50000, target_roas=3.0
) -> dict:
    """
    Takes raw primary_table rows, mirrors dashboard JS logic exactly,
    returns a dict ready to INSERT into results_table.
    """
    if not rows:
        return None

    # ── Date range of data used ────────────────────────────────────────
    dates = [r["date"] for r in rows if r.get("date")]
    date_from = min(dates) if dates else None
    date_to = max(dates) if dates else None

    # ── Aggregate per unique ad (same as JS renderResults) ────────────
    # Key: ad_id — sum metrics across all dates
    ad_map: dict[str, dict] = {}

    for r in rows:
        ad_id = str(r.get("ad_id") or "").strip()
        if not ad_id:
            continue

        ad_name = str(r.get("ad_name") or "")
        impr = float(r.get("impressions") or 0)
        spend = float(r.get("amount_spent_inr") or 0)
        reach = float(r.get("reach") or 0)
        clicks = float(r.get("outbound_clicks") or 0)
        thru = float(r.get("thruplays") or 0)
        vid3 = float(r.get("three_sec_video_plays") or 0)
        eng = float(r.get("post_engagements") or 0)
        conv_v = float(r.get("conversion_value") or 0)
        vpt = float(r.get("video_play_time") or 0)
        ftewv = float(r.get("ftewv_count") or 0)
        ncp = float(r.get("ncp_count") or 0)

        if ad_id not in ad_map:
            ad_map[ad_id] = {
                "adId": ad_id,
                "adName": ad_name,
                "acct": str(r.get("account_name") or account_name),
                "campName": str(r.get("campaign_name") or ""),
                "adStatus": str(r.get("ad_status") or ""),
                "adCreated": str(r.get("ad_created_date") or ""),
                "ctype": detect_ctype(ad_name),
                "product": detect_product(ad_name),
                "isCopy": is_copy(ad_name),
                "previewLink": str(r.get("preview_link") or ""),
                "adLink": str(r.get("ad_link") or ""),
                "impr": 0,
                "spend": 0,
                "reach": 0,
                "clicks": 0,
                "thru": 0,
                "vid3": 0,
                "eng": 0,
                "convV": 0,
                "vpt_sum": 0,
                "ftewv": 0,
                "ncp": 0,
            }
        else:
            # Keep the most recent non-empty link values seen
            if (r.get("preview_link") or "") and not ad_map[ad_id].get("previewLink"):
                ad_map[ad_id]["previewLink"] = str(r.get("preview_link") or "")
            if (r.get("ad_link") or "") and not ad_map[ad_id].get("adLink"):
                ad_map[ad_id]["adLink"] = str(r.get("ad_link") or "")

        ad = ad_map[ad_id]
        ad["impr"] += impr
        ad["spend"] += spend
        ad["reach"] += reach
        ad["clicks"] += clicks
        ad["thru"] += thru
        ad["vid3"] += vid3
        ad["eng"] += eng
        ad["convV"] += conv_v
        ad["vpt_sum"] += vpt * impr  # weighted for later avg
        ad["ftewv"] += ftewv
        ad["ncp"] += ncp

    # ── Overview totals (all rows, not deduplicated) ───────────────────
    total_spend = sum(float(r.get("amount_spent_inr") or 0) for r in rows)
    total_impr = sum(float(r.get("impressions") or 0) for r in rows)
    total_reach = sum(float(r.get("reach") or 0) for r in rows)
    total_conv = sum(float(r.get("conversion_value") or 0) for r in rows)
    total_thru = sum(float(r.get("thruplays") or 0) for r in rows)
    total_vid3 = sum(float(r.get("three_sec_video_plays") or 0) for r in rows)
    total_clicks = sum(float(r.get("outbound_clicks") or 0) for r in rows)
    total_eng = sum(float(r.get("post_engagements") or 0) for r in rows)

    # ── Derived rates ─────────────────────────────────────────────────
    ct_roas = sdv(total_conv, total_spend)
    hook_rate = sdv(total_vid3, total_impr) * 100
    hold_rate = sdv(total_thru, total_vid3) * 100
    thruplay_rate = sdv(total_thru, total_impr) * 100
    outbound_ctr = sdv(total_clicks, total_impr) * 100
    engagement_r = sdv(total_eng, total_impr) * 100

    # ── Categorise each unique ad ──────────────────────────────────────
    CTYPE_ORDER = ["IFAD", "Graphic AD", "VID", "STATIC"]
    counts = {"Winner": 0, "ITE": 0, "Analyse": 0, "Discarded": 0}
    ctype_b = {
        ct: {"Winner": 0, "ITE": 0, "Analyse": 0, "Discarded": 0, "total": 0}
        for ct in CTYPE_ORDER
    }
    product_b: dict[str, int] = defaultdict(int)

    # Only non-copy ads for categorisation (mirrors JS EXCLUDE_COPY_TOGGLE=true)
    ads_no_copy = [ad for ad in ad_map.values() if not ad["isCopy"]]

    for ad in ads_no_copy:
        ct_roas_ad = sdv(ad["convV"], ad["spend"])
        has_conv = ad["convV"] > 0

        if ad["impr"] >= target_imp and ct_roas_ad >= target_roas:
            cat = "Winner"
        elif ad["impr"] >= target_imp and ct_roas_ad < target_roas:
            cat = "ITE"
        elif ad["impr"] < target_imp and has_conv:
            cat = "Analyse"
        else:
            cat = "Discarded"

        counts[cat] += 1

        ct = ad["ctype"] if ad["ctype"] in CTYPE_ORDER else "VID"
        ctype_b[ct][cat] += 1
        ctype_b[ct]["total"] += 1

        product_b[ad["product"]] += 1

    # ── Build compact ads_json for frontend ───────────────────────────
    # Only store what the frontend needs to render the table
    ads_json = [
        {
            "adId": ad["adId"],
            "adName": ad["adName"],
            "acct": ad.get("acct", account_name),
            "campName": ad["campName"],
            "adStatus": ad["adStatus"],
            "adCreated": ad["adCreated"],
            "ctype": ad["ctype"],
            "product": ad["product"],
            "isCopy": ad["isCopy"],
            "impr": round(ad["impr"]),
            "spend": round(ad["spend"], 2),
            "reach": round(ad["reach"]),
            "clicks": round(ad["clicks"]),
            "thru": round(ad["thru"]),
            "vid3": round(ad["vid3"]),
            "eng": round(ad["eng"]),
            "convV": round(ad["convV"], 2),
            "vpt": round(sdv(ad["vpt_sum"], ad["impr"]), 3),
            "ftewv": round(ad.get("ftewv", 0)),
            "cpf": round(sdv(ad["spend"], ad.get("ftewv", 0)), 2),
            "ncp": round(ad.get("ncp", 0)),
            "cpn": round(sdv(ad["spend"], ad.get("ncp", 0)), 2),
            "preview": ad.get("previewLink", ""),
            "adLink": ad.get("adLink", ""),
        }
        for ad in ad_map.values()
    ]

    return {
        "account_name": account_name,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "data_date_from": str(date_from),
        "data_date_to": str(date_to),
        "total_raw_rows": len(rows),
        "ads_json": json.dumps(ads_json),
        "total_spend": round(total_spend, 2),
        "total_impr": int(total_impr),
        "total_reach": int(total_reach),
        "total_conv_value": round(total_conv, 2),
        "total_thru_plays": int(total_thru),
        "total_video_plays": int(total_vid3),
        "total_out_clicks": int(total_clicks),
        "total_post_eng": int(total_eng),
        "ct_roas": round(ct_roas, 4),
        "hook_rate": round(hook_rate, 4),
        "hold_rate": round(hold_rate, 4),
        "thruplay_rate": round(thruplay_rate, 4),
        "outbound_ctr": round(outbound_ctr, 4),
        "engagement_rate": round(engagement_r, 4),
        "target_imp": target_imp,
        "target_roas": target_roas,
        "count_winner": counts["Winner"],
        "count_ite": counts["ITE"],
        "count_analyse": counts["Analyse"],
        "count_discarded": counts["Discarded"],
        "count_total_ads": len(ads_no_copy),
        "ctype_breakdown": json.dumps(ctype_b),
        "product_breakdown": json.dumps(dict(product_b)),
    }


def write_result(conn, result: dict):
    """Insert computed result row into results_table."""
    cols = list(result.keys())
    vals = list(result.values())
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    sql = f"INSERT INTO results_table ({col_names}) VALUES ({placeholders})"
    with conn.cursor() as cur:
        cur.execute(sql, vals)
    conn.commit()


def purge_old_results(conn, keep_per_account: int = 1):
    """Keep only the N most recent rows per account — prevent table bloat.
    Default is 1 (the current 30-day snapshot is the only one needed)."""
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
    print(f"  Old rows purged — keeping latest {keep_per_account} per account")


# ── Main ─────────────────────────────────────────────────────────────
def run(account_name: str | None = None, days_window: int = DEFAULT_DAYS_WINDOW):
    today = date.today()
    since_date = today - timedelta(days=days_window - 1)
    print(f"\n[results_sync] Starting — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Window: {since_date.isoformat()} → {today.isoformat()} ({days_window} days)")

    conn = get_conn()
    try:
        targets = [a["name"] for a in ACCOUNTS] if not account_name else [account_name]

        all_rows_combined = []

        for acct in targets:
            print(f"  Fetching primary_table for: {acct}...")
            rows = fetch_primary(conn, acct, since_date=since_date)
            print(f"    {len(rows):,} rows fetched ({since_date.isoformat()} onwards)")

            if not rows:
                print(f"    No data — skipping")
                continue

            result = compute_results(rows, acct)
            if result:
                # Override data range to the requested window — keeps the frontend's
                # date filter stable even if there are gap days with no delivery.
                result["data_date_from"] = since_date.isoformat()
                result["data_date_to"] = today.isoformat()
                write_result(conn, result)
                print(
                    f"    ✓ Wrote results_table row — "
                    f"{result['count_total_ads']} ads | "
                    f"Winner:{result['count_winner']} "
                    f"ITE:{result['count_ite']} "
                    f"Analyse:{result['count_analyse']} "
                    f"Discarded:{result['count_discarded']}"
                )

            all_rows_combined.extend(rows)

        # ── Write combined "All Accounts" row ─────────────────────────
        if len(targets) > 1 and all_rows_combined:
            print("  Computing All Accounts combined...")
            combined = compute_results(all_rows_combined, "All Accounts")
            if combined:
                combined["data_date_from"] = since_date.isoformat()
                combined["data_date_to"] = today.isoformat()
                write_result(conn, combined)
                print(
                    f"    ✓ All Accounts row written — {combined['count_total_ads']} unique ads"
                )

        purge_old_results(conn, keep_per_account=1)
        print(f"[results_sync] Done — {datetime.now().strftime('%H:%M:%S')}\n")

    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    acct = None
    days = DEFAULT_DAYS_WINDOW
    if "--account" in sys.argv:
        idx = sys.argv.index("--account")
        if idx + 1 < len(sys.argv):
            num = int(sys.argv[idx + 1])
            acct = ACCOUNTS[num - 1]["name"]
    if "--days" in sys.argv:
        idx = sys.argv.index("--days")
        if idx + 1 < len(sys.argv):
            days = int(sys.argv[idx + 1])
    run(acct, days_window=days)
