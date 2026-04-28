"""
full_backfill.py — SAADAA Meta Ads Complete Historical Backfill
═══════════════════════════════════════════════════════════════════════
Fetches ALL data from Meta API from account creation date → today
and writes everything into backfill_table in Supabase.

backfill_table is separate from primary_table:
  primary_table  → live sync (daily/hourly via primary_sync.py)
  backfill_table → full lifetime history (this script, run once)

Designed to run as a one-time PowerShell script:
    python full_backfill.py                  # all 3 accounts, auto-detect start → today
    python full_backfill.py --from 2025-01-01          # custom start date (skip auto-detect)
    python full_backfill.py --account "Raho Saadaa"    # single account
    python full_backfill.py --dry-run                  # show plan, no DB writes

Progress is saved to backfill_progress.json after each chunk so the
script can be safely interrupted and resumed without re-fetching data.
"""

import os
import sys
import json
import time
import logging
import traceback
import requests
import psycopg2
import psycopg2.extras
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ════════════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════════════

META_TOKEN = os.getenv("META_ACCESS_TOKEN")
META_API_VER = os.getenv("META_API_VERSION", "v21.0")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

ACCOUNTS = [
    {"name": "Raho Saadaa", "id": os.getenv("ACCOUNT_1_ID", "1136644150469466")},
    {
        "name": "Fourth Ad Account - SD",
        "id": os.getenv("ACCOUNT_2_ID", "1349767139294217"),
    },
    {
        "name": "Third Ad Account - SD",
        "id": os.getenv("ACCOUNT_3_ID", "264868699479122"),
    },
]

# Default start date — 'auto' means detect from Meta API per account
DEFAULT_START_DATE = "auto"

# Chunk size: 7 days per API request (prevents Meta HTTP 500 on pagination)
CHUNK_DAYS = 7
API_PAGE_LIMIT = 500
BATCH_SIZE = 500  # rows per Supabase upsert batch
MAX_RETRIES = 4
RETRY_DELAYS = [5, 15, 30, 60]  # seconds between retries

PROGRESS_FILE = Path(__file__).parent / "backfill_progress.json"

# Meta API fields — 19 insight fields (cost_per_action_type removed — causes HTTP 500)
META_FIELDS = ",".join(
    [
        "date_start",
        "ad_name",
        "ad_id",
        "campaign_name",
        "campaign_id",
        "impressions",
        "reach",
        "frequency",
        "spend",
        "purchase_roas",
        "actions",  # contains video_view (3-sec plays), purchases etc
        "action_values",  # purchase conversion value
        "outbound_clicks",
        "inline_post_engagement",
        "video_thruplay_watched_actions",
        "video_avg_time_watched_actions",
    ]
)

# ════════════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════════════

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "full_backfill.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("full_backfill")

# ════════════════════════════════════════════════════════════════════════
# DATE HELPERS
# ════════════════════════════════════════════════════════════════════════


def today_str():
    return date.today().isoformat()


def date_chunks(since: str, until: str, chunk_days: int = CHUNK_DAYS):
    """Yield (chunk_since, chunk_until) pairs covering since→until."""
    start = date.fromisoformat(since)
    end = date.fromisoformat(until)
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        yield cur.isoformat(), chunk_end.isoformat()
        cur = chunk_end + timedelta(days=1)


# ════════════════════════════════════════════════════════════════════════
# PROGRESS TRACKER — resume from interruption
# ════════════════════════════════════════════════════════════════════════


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text())
        except Exception:
            pass
    return {}


def save_progress(progress: dict):
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2))


def chunk_key(account_name: str, chunk_since: str) -> str:
    return f"{account_name}|{chunk_since}"


def is_done(progress: dict, account_name: str, chunk_since: str) -> bool:
    return progress.get(chunk_key(account_name, chunk_since)) == "done"


def mark_done(progress: dict, account_name: str, chunk_since: str):
    progress[chunk_key(account_name, chunk_since)] = "done"
    save_progress(progress)


# ════════════════════════════════════════════════════════════════════════
# META API FETCH
# ════════════════════════════════════════════════════════════════════════


def _action_val(actions: list, action_type: str) -> float:
    """Extract value for a specific action_type from Meta actions array."""
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == action_type:
            return float(a.get("value", 0))
    return 0.0


def fetch_ad_metadata(account_id: str, ad_ids: list) -> dict:
    """
    Batch-fetch ad status and created_time for a list of ad IDs.
    Returns {ad_id: {'status': str, 'created_time': str}}
    """
    meta = {}
    if not ad_ids:
        return meta

    # Fetch in batches of 25 (Meta API limit for batch)
    for i in range(0, len(ad_ids), 25):
        batch = ad_ids[i : i + 25]
        ids_str = ",".join(batch)
        url = f"https://graph.facebook.com/{META_API_VER}/"
        params = {
            "ids": ids_str,
            "fields": "id,status,created_time,adset_id",
            "access_token": META_TOKEN,
        }
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.ok:
                data = r.json()
                for ad_id, ad_data in data.items():
                    if isinstance(ad_data, dict):
                        ct = ad_data.get("created_time", "")
                        # Parse ISO to date only: "2026-01-15T10:30:00+0000" → "2026-01-15"
                        created_date = ct[:10] if ct else None
                        meta[ad_id] = {
                            "status": ad_data.get("status", ""),
                            "created_date": created_date,
                        }
        except Exception as e:
            log.warning(f"    Ad metadata fetch error: {e}")
    return meta


def get_account_start_date(account_id: str, account_name: str) -> str:
    """
    Ask Meta API for the earliest date this ad account has any data.
    Uses the account's created_time as the lower bound, then verifies
    by checking the earliest ad insights available.

    Falls back to 2024-01-01 if the API does not return a usable date.
    """
    fallback = "2024-01-01"

    # ── Step 1: Get account created_time ──────────────────────────
    try:
        url = f"https://graph.facebook.com/{META_API_VER}/act_{account_id}"
        r = requests.get(
            url,
            params={
                "fields": "created_time,name",
                "access_token": META_TOKEN,
            },
            timeout=30,
        )
        if r.ok:
            data = r.json()
            created_time = data.get("created_time", "")
            if created_time:
                # "2023-08-14T09:22:11+0000" → "2023-08-14"
                account_start = created_time[:10]
                log.info(f"    Account created : {account_start}")
            else:
                account_start = fallback
        else:
            log.warning(f"    Could not get account info: {r.status_code}")
            account_start = fallback
    except Exception as e:
        log.warning(f"    Account info error: {e}")
        account_start = fallback

    # ── Step 2: Find earliest date that actually has ad data ───────
    # Binary search would be ideal but is complex — instead we probe
    # by fetching insights for a 7-day window starting at account_start.
    # Meta returns date_start of the first row with actual impressions.
    try:
        # Probe the first 30 days from account creation
        probe_end = (date.fromisoformat(account_start) + timedelta(days=30)).isoformat()

        url = f"https://graph.facebook.com/{META_API_VER}/act_{account_id}/insights"
        params = {
            "level": "account",
            "time_increment": 1,
            "time_range": json.dumps({"since": account_start, "until": probe_end}),
            "fields": "date_start,impressions",
            "limit": 1,
            "access_token": META_TOKEN,
        }
        r = requests.get(url, params=params, timeout=30)
        if r.ok:
            rows = r.json().get("data", [])
            if rows:
                first_data_date = rows[0].get("date_start", account_start)
                log.info(f"    First data date : {first_data_date}")
                # Use whichever is earlier — account_start or first_data_date
                earliest = min(account_start, first_data_date)
                return earliest
    except Exception as e:
        log.warning(f"    Probe error: {e}")

    log.info(f"    Using account created date: {account_start}")
    return account_start


def fetch_insights(account_id: str, account_name: str, since: str, until: str) -> list:
    """
    Fetch all ad-level daily insights from Meta API for one date range.
    Returns list of row dicts ready for Supabase INSERT.
    """
    url = f"https://graph.facebook.com/{META_API_VER}/act_{account_id}/insights"
    params = {
        "level": "ad",
        "time_increment": 1,  # 1 row per ad per day
        "time_range": json.dumps({"since": since, "until": until}),
        "fields": META_FIELDS,
        "limit": API_PAGE_LIMIT,
        "access_token": META_TOKEN,
    }

    all_data = []
    page_num = 0

    while True:
        page_num += 1
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(url, params=params, timeout=60)
                if r.status_code == 200:
                    break
                elif r.status_code in (500, 503):
                    wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                    log.warning(
                        f"      Meta {r.status_code} on page {page_num} — retry {attempt+1}/{MAX_RETRIES} in {wait}s"
                    )
                    time.sleep(wait)
                    continue
                else:
                    log.error(f"      Meta API error {r.status_code}: {r.text[:200]}")
                    return all_data  # return what we have
            except requests.RequestException as e:
                wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
                log.warning(f"      Request error: {e} — retry {attempt+1} in {wait}s")
                time.sleep(wait)
        else:
            log.error(
                f"      Max retries reached for page {page_num} — skipping rest of chunk"
            )
            return all_data

        body = r.json()
        if "error" in body:
            log.error(
                f'      Meta error: {body["error"].get("message", body["error"])}'
            )
            return all_data

        rows = body.get("data", [])
        all_data.extend(rows)

        # Next page
        paging = body.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break
        url = next_url
        params = {}  # params are already embedded in the next URL

    if not all_data:
        return []

    # ── Fetch ad metadata (status + created_date) ──────────────────
    ad_ids = list({r.get("ad_id") for r in all_data if r.get("ad_id")})
    ad_meta = fetch_ad_metadata(account_id, ad_ids)

    # ── Parse rows into Supabase schema ───────────────────────────
    parsed = []
    for r in all_data:
        ad_id = str(r.get("ad_id", ""))
        ad_meta_r = ad_meta.get(ad_id, {})
        actions = r.get("actions", [])
        act_vals = r.get("action_values", [])
        thruplay = r.get("video_thruplay_watched_actions", [])
        vpt_list = r.get("video_avg_time_watched_actions", [])

        parsed.append(
            {
                "account_name": account_name,
                "date": r.get("date_start"),
                "ad_name": r.get("ad_name", ""),
                "ad_id": ad_id,
                "campaign_name": r.get("campaign_name", ""),
                "campaign_id": str(r.get("campaign_id", "")),
                "ad_status": ad_meta_r.get("status", ""),
                "ad_created_date": ad_meta_r.get("created_date"),
                "impressions": int(r.get("impressions", 0) or 0),
                "reach": int(r.get("reach", 0) or 0),
                "amount_spent_inr": float(r.get("spend", 0) or 0),
                "purchase_roas": float(
                    r["purchase_roas"][0]["value"] if r.get("purchase_roas") else 0
                ),
                "outbound_clicks": int(
                    _action_val(r.get("outbound_clicks", []), "outbound_click")
                    if isinstance(r.get("outbound_clicks"), list)
                    else r.get("outbound_clicks", 0) or 0
                ),
                "thruplays": int(_action_val(thruplay, "video_thruplay_watched")),
                "three_sec_video_plays": int(_action_val(actions, "video_view")),
                "post_engagements": int(
                    _action_val(actions, "post_engagement")
                    or int(r.get("inline_post_engagement", 0) or 0)
                ),
                "conversion_value": float(_action_val(act_vals, "purchase")),
                "video_play_time": float(vpt_list[0]["value"] if vpt_list else 0),
                # FTEWV, NCP, LTV — only available via primary_sync.py's extended fields
                # Set to 0 for backfill; primary_sync.py will overwrite with real values
                "ftewv_count": 0,
                "cost_per_ftewv": 0,
                "ncp_count": 0,
                "cost_per_ncp": 0,
                "ltv_reach": 0,
                "ltv_frequency": 0,
            }
        )

    return parsed


# ════════════════════════════════════════════════════════════════════════
# SUPABASE UPSERT
# ════════════════════════════════════════════════════════════════════════


def get_conn():
    """Fresh connection per call — prevents Supabase pooler cursor errors."""
    return psycopg2.connect(SUPABASE_DB_URL)


UPSERT_SQL = """
INSERT INTO backfill_table (
    account_name, date, ad_name, ad_id, campaign_name, campaign_id,
    ad_status, ad_created_date, impressions, reach, amount_spent_inr,
    purchase_roas, outbound_clicks, thruplays, three_sec_video_plays,
    post_engagements, conversion_value, video_play_time,
    ftewv_count, cost_per_ftewv, ncp_count, cost_per_ncp,
    ltv_reach, ltv_frequency
)
VALUES (
    %(account_name)s, %(date)s, %(ad_name)s, %(ad_id)s,
    %(campaign_name)s, %(campaign_id)s, %(ad_status)s, %(ad_created_date)s,
    %(impressions)s, %(reach)s, %(amount_spent_inr)s, %(purchase_roas)s,
    %(outbound_clicks)s, %(thruplays)s, %(three_sec_video_plays)s,
    %(post_engagements)s, %(conversion_value)s, %(video_play_time)s,
    %(ftewv_count)s, %(cost_per_ftewv)s, %(ncp_count)s, %(cost_per_ncp)s,
    %(ltv_reach)s, %(ltv_frequency)s
)
ON CONFLICT (account_name, ad_id, date) DO NOTHING
"""
# Writes into backfill_table — separate from primary_table which daily sync manages.
# ON CONFLICT DO NOTHING — safe to re-run, never overwrites existing rows.


def upsert_batch(rows: list, dry_run: bool = False) -> int:
    if dry_run or not rows:
        return len(rows)

    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur, UPSERT_SQL, batch, page_size=BATCH_SIZE
                )
            conn.commit()
            inserted += len(batch)
        except Exception as e:
            conn.rollback()
            log.error(f"    DB upsert error: {e}")
            raise
        finally:
            conn.close()
    return inserted


# ════════════════════════════════════════════════════════════════════════
# MAIN BACKFILL
# ════════════════════════════════════════════════════════════════════════


def run_backfill(start_date: str, accounts: list, dry_run: bool = False):
    end_date = today_str()

    log.info("=" * 65)
    log.info("  SAADAA — Meta Ads Full Historical Backfill → backfill_table")
    log.info("=" * 65)
    if start_date == "auto":
        log.info(f"  Date range : Auto-detect from Meta API → {end_date}")
    else:
        log.info(f"  Date range : {start_date} → {end_date}")
    log.info(f"  Accounts   : {len(accounts)}")
    for a in accounts:
        log.info(f'    • {a["name"]} (ID: {a["id"]})')
    log.info(f"  Chunk size : {CHUNK_DAYS} days per API request")
    log.info(f"  Dry run    : {dry_run}")
    log.info(f"  Progress   : {PROGRESS_FILE}")
    log.info("=" * 65)

    if not META_TOKEN:
        log.error("META_ACCESS_TOKEN not set in .env — cannot fetch from Meta API")
        sys.exit(1)

    if not SUPABASE_DB_URL:
        log.error("SUPABASE_DB_URL not set in .env — cannot write to Supabase")
        sys.exit(1)

    # Test DB connection
    if not dry_run:
        try:
            conn = get_conn()
            conn.close()
            log.info("  ✓ Supabase connection OK")
        except Exception as e:
            log.error(f"  ✗ Supabase connection FAILED: {e}")
            sys.exit(1)

    # Load progress (for resume support)
    progress = load_progress()
    already_done = sum(1 for v in progress.values() if v == "done")
    if already_done:
        log.info(
            f"  Resuming — {already_done} chunks already completed (from previous run)"
        )

    log.info("")

    # Count total chunks — for auto mode we estimate using 2024-01-01 as baseline
    # The actual per-account start is detected during the loop
    _est_start = "2024-01-01" if start_date == "auto" else start_date
    total_chunks = sum(
        sum(1 for _ in date_chunks(_est_start, end_date, CHUNK_DAYS)) for _ in accounts
    )
    chunk_num = 0

    grand_fetched = 0
    grand_inserted = 0
    start_time = datetime.now()

    for acct in accounts:
        log.info(f'▶  {acct["name"]}')
        acct_fetched = acct_inserted = 0

        # Auto-detect start date from Meta API
        if start_date == "auto":
            log.info(f"  Detecting earliest date from Meta API...")
            acct_start = get_account_start_date(acct["id"], acct["name"])
            log.info(f"  ✓ Will fetch from: {acct_start}")
        else:
            acct_start = start_date

        chunks = list(date_chunks(acct_start, end_date, CHUNK_DAYS))
        log.info(f"  Total chunks: {len(chunks)} ({CHUNK_DAYS}-day each)")
        for chunk_since, chunk_until in chunks:
            chunk_num += 1
            pct = chunk_num / total_chunks * 100

            # Skip already-completed chunks
            if is_done(progress, acct["name"], chunk_since):
                log.info(
                    f"  [{chunk_num:>3}/{total_chunks}] {chunk_since}→{chunk_until}  ✓ already done (skip)"
                )
                continue

            log.info(
                f"  [{chunk_num:>3}/{total_chunks}] {pct:5.1f}%  {chunk_since} → {chunk_until}  fetching..."
            )

            try:
                rows = fetch_insights(
                    acct["id"], acct["name"], chunk_since, chunk_until
                )

                if rows:
                    n = upsert_batch(rows, dry_run=dry_run)
                    acct_fetched += len(rows)
                    acct_inserted += n
                    log.info(f"             → {len(rows)} rows fetched, {n} inserted")
                else:
                    log.info(f"             → 0 rows (no ads ran in this period)")

                mark_done(progress, acct["name"], chunk_since)

                # Small pause between chunks to be polite to Meta API
                time.sleep(1)

            except KeyboardInterrupt:
                log.info("\n⛔ Interrupted by user — progress saved. Re-run to resume.")
                _print_summary(grand_fetched, grand_inserted, start_time, dry_run)
                sys.exit(0)
            except Exception as e:
                log.error(f"             → ERROR: {e}")
                traceback.print_exc()
                log.warning("             → Skipping chunk and continuing...")
                time.sleep(5)

        grand_fetched += acct_fetched
        grand_inserted += acct_inserted
        log.info(
            f'  ✅ {acct["name"]}: {acct_fetched:,} fetched, {acct_inserted:,} inserted'
        )
        log.info("")

    # Clear progress file on full success
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        log.info("  Progress file cleared (backfill complete)")

    _print_summary(grand_fetched, grand_inserted, start_time, dry_run)


def _print_summary(fetched: int, inserted: int, start_time: datetime, dry_run: bool):
    elapsed = (datetime.now() - start_time).total_seconds()
    hours = int(elapsed // 3600)
    mins = int((elapsed % 3600) // 60)
    secs = int(elapsed % 60)
    elapsed_s = f"{hours}h {mins}m {secs}s" if hours else f"{mins}m {secs}s"

    log.info("=" * 65)
    log.info(f'  BACKFILL {"(DRY RUN) " if dry_run else ""}COMPLETE')
    log.info(f"  Rows fetched  : {fetched:,}")
    log.info(f"  Rows inserted : {inserted:,}")
    log.info(f"  Time elapsed  : {elapsed_s}")
    log.info("=" * 65)


# ════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ════════════════════════════════════════════════════════════════════════


def parse_args():
    args = sys.argv[1:]
    cfg = {
        "start": DEFAULT_START_DATE,
        "account": None,
        "dry_run": False,
    }

    i = 0
    while i < len(args):
        a = args[i]
        if a == "--from" and i + 1 < len(args):
            cfg["start"] = args[i + 1]
            i += 2
        elif a == "--account" and i + 1 < len(args):
            cfg["account"] = args[i + 1]
            i += 2
        elif a == "--dry-run":
            cfg["dry_run"] = True
            i += 1
        elif a == "--help":
            print(__doc__)
            sys.exit(0)
        else:
            log.warning(f"Unknown argument: {a}")
            i += 1

    return cfg


if __name__ == "__main__":
    cfg = parse_args()

    # Filter accounts if --account specified
    accounts = ACCOUNTS
    if cfg["account"]:
        accounts = [a for a in ACCOUNTS if a["name"].lower() == cfg["account"].lower()]
        if not accounts:
            log.error(f'Account not found: {cfg["account"]}')
            log.error(f'Valid accounts: {[a["name"] for a in ACCOUNTS]}')
            sys.exit(1)

    run_backfill(
        start_date=cfg["start"],
        accounts=accounts,
        dry_run=cfg["dry_run"],
    )
