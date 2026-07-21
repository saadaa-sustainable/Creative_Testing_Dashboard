"""Fetch deduped daily reach at ADSET and CAMPAIGN level from Meta,
land into public.primary_adset_table and public.primary_camp_table.

Ad-level reach in primary_table CANNOT be summed to derive adset/camp reach —
two ads reaching the same user get double-counted. Meta's /insights endpoint
returns properly deduped reach when queried with level=adset or level=campaign.
This script fetches those numbers directly.

Modes:
  daily    — refresh last 15 days (called from _run_full_update.py)
  backfill — fetch from Meta's max window (37 months back) to today. One-shot.
  hourly   — refresh last 3 days

Called sequentially: adset first, then campaign. Each account is processed
in date chunks (30d each) to keep Meta happy. Upserts are batched into
Supabase in groups of 500 rows.
"""
import os, sys, time, json, logging
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
import requests
import psycopg2
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv(override=True)

# ── logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── config ──────────────────────────────────────────────────
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
API_VERSION  = os.getenv("META_API_VERSION", "v21.0")
DB_URL       = os.getenv("SUPABASE_DB_URL")
BASE_URL     = f"https://graph.facebook.com/{API_VERSION}"

ACCOUNTS = [
    {"name": os.getenv("ACCOUNT_1_NAME", "Raho Saadaa"),            "id": os.getenv("ACCOUNT_1_ID", "1136644150469466")},
    {"name": os.getenv("ACCOUNT_2_NAME", "Fourth Ad Account - SD"), "id": os.getenv("ACCOUNT_2_ID", "1349767139294217")},
    {"name": os.getenv("ACCOUNT_3_NAME", "Third Ad Account - SD"),  "id": os.getenv("ACCOUNT_3_ID", "264868699479122")},
]

CHUNK_DAYS  = 30
PAGE_LIMIT  = 250
BATCH_SIZE  = 500
MAX_RETRIES = 3
RETRY_DELAY = 5

# ── helpers ─────────────────────────────────────────────────
def gmt_today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

def gmt_days_ago(n: int) -> str:
    return (datetime.utcnow() - timedelta(days=n)).strftime("%Y-%m-%d")

def date_chunks(since: str, until: str, chunk_days: int = CHUNK_DAYS):
    s = date.fromisoformat(since)
    e = date.fromisoformat(until)
    cur = s
    while cur <= e:
        stop = min(cur + timedelta(days=chunk_days - 1), e)
        yield cur.isoformat(), stop.isoformat()
        cur = stop + timedelta(days=1)

def _get(url: str, params: dict = None, attempt: int = 1) -> dict:
    try:
        resp = requests.get(url, params=params or {}, timeout=90)
        if resp.status_code == 403:
            if attempt < MAX_RETRIES:
                log.warning(f"  Meta 403 rate limit — waiting 60s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(60)
                return _get(url, params, attempt + 1)
            log.warning("  Meta 403 persists — skipping page")
            return {"data": [], "paging": {}}
        if resp.status_code == 500:
            if attempt < MAX_RETRIES:
                wait = RETRY_DELAY * attempt * 3
                log.warning(f"  Meta 500 — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                return _get(url, params, attempt + 1)
            log.warning("  Meta 500 persists — skipping page")
            return {"data": [], "paging": {}}
        if resp.status_code == 400:
            body = resp.json() if resp.headers.get("content-type","").startswith("application/json") else {}
            msg  = ((body.get("error") or {}).get("message") or resp.text)[:250]
            log.warning(f"  Meta 400: {msg}")
            return {"data": [], "paging": {}}
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        import re as _re
        safe = _re.sub(r"(\w*token\w*=)[^&\s]+", r"\1<REDACTED>", str(e), flags=_re.I)
        safe = _re.sub(r"EAA[A-Za-z0-9_\-]{20,}", "<REDACTED>", safe)
        if attempt < MAX_RETRIES:
            log.warning(f"  retry {attempt}/{MAX_RETRIES}: {safe}")
            time.sleep(RETRY_DELAY * attempt)
            return _get(url, params, attempt + 1)
        raise type(e)(safe) from None

# ── fetch + upsert ──────────────────────────────────────────
LEVEL_CONFIG = {
    "adset": {
        "fields": "date_start,date_stop,adset_id,adset_name,campaign_id,campaign_name,reach,impressions,frequency,spend",
        "table":  "primary_adset_table",
        "key_cols": ("account_name", "adset_id", "date"),
        "insert_cols": (
            "account_name", "campaign_id", "campaign_name",
            "adset_id", "adset_name", "date",
            "reach", "impressions", "frequency", "amount_spent_inr", "refreshed_at",
        ),
        "row_fn": lambda r, acc_name: (
            acc_name,
            (r.get("campaign_id") or None), (r.get("campaign_name") or None),
            r.get("adset_id"),              (r.get("adset_name") or None),
            r.get("date_start"),
            int(r.get("reach") or 0), int(r.get("impressions") or 0),
            float(r.get("frequency") or 0), float(r.get("spend") or 0),
        ),
        "conflict": "ON CONFLICT (account_name, adset_id, date) DO UPDATE SET "
                    "campaign_id=EXCLUDED.campaign_id, campaign_name=EXCLUDED.campaign_name, "
                    "adset_name=EXCLUDED.adset_name, reach=EXCLUDED.reach, "
                    "impressions=EXCLUDED.impressions, frequency=EXCLUDED.frequency, "
                    "amount_spent_inr=EXCLUDED.amount_spent_inr, refreshed_at=NOW()",
    },
    "campaign": {
        "fields": "date_start,date_stop,campaign_id,campaign_name,reach,impressions,frequency,spend",
        "table":  "primary_camp_table",
        "key_cols": ("account_name", "campaign_id", "date"),
        "insert_cols": (
            "account_name", "campaign_id", "campaign_name", "date",
            "reach", "impressions", "frequency", "amount_spent_inr", "refreshed_at",
        ),
        "row_fn": lambda r, acc_name: (
            acc_name,
            r.get("campaign_id"), (r.get("campaign_name") or None),
            r.get("date_start"),
            int(r.get("reach") or 0), int(r.get("impressions") or 0),
            float(r.get("frequency") or 0), float(r.get("spend") or 0),
        ),
        "conflict": "ON CONFLICT (account_name, campaign_id, date) DO UPDATE SET "
                    "campaign_name=EXCLUDED.campaign_name, reach=EXCLUDED.reach, "
                    "impressions=EXCLUDED.impressions, frequency=EXCLUDED.frequency, "
                    "amount_spent_inr=EXCLUDED.amount_spent_inr, refreshed_at=NOW()",
    },
}

def get_conn():
    return psycopg2.connect(DB_URL, connect_timeout=30)

def upsert_rows(rows: list, table: str, insert_cols: tuple, conflict_sql: str) -> int:
    if not rows: return 0
    conn = get_conn(); n = 0
    try:
        with conn.cursor() as cur:
            # add refreshed_at = NOW() sentinel value slot (we set it via NOW() in insert)
            # execute_values needs the tuple shape matching insert_cols including refreshed_at
            # Simpler: append NOW() as SQL literal via template
            sql = (
                f"INSERT INTO public.{table} ({', '.join(insert_cols)}) VALUES %s "
                f"{conflict_sql}"
            )
            # Build value template with NOW() for the last column (refreshed_at)
            n_data = len(insert_cols) - 1
            template = "(" + ", ".join(["%s"] * n_data) + ", NOW())"
            for i in range(0, len(rows), BATCH_SIZE):
                chunk = rows[i : i + BATCH_SIZE]
                execute_values(cur, sql, chunk, template=template, page_size=BATCH_SIZE)
                n += len(chunk)
        conn.commit()
    finally:
        conn.close()
    return n


def fetch_level(account_id: str, account_name: str, level: str, since: str, until: str) -> int:
    cfg = LEVEL_CONFIG[level]
    log.info(f"  [{level}] {account_name} | {since} -> {until}")
    url = f"{BASE_URL}/act_{account_id}/insights"
    params = {
        "level":         level,
        "fields":        cfg["fields"],
        "time_increment": "1",
        "time_range":    json.dumps({"since": since, "until": until}),
        "access_token":  ACCESS_TOKEN,
        "limit":         PAGE_LIMIT,
    }
    raw = []
    page = 0
    while url:
        page += 1
        data = _get(url, params if page == 1 else None)
        raw.extend(data.get("data", []))
        url = (data.get("paging") or {}).get("next")
        params = None

    if not raw:
        log.info(f"  [{level}] no data returned")
        return 0

    rows = [cfg["row_fn"](r, account_name) for r in raw if r.get(f"{level}_id" if level == "adset" else "campaign_id")]
    n = upsert_rows(rows, cfg["table"], cfg["insert_cols"], cfg["conflict"])
    log.info(f"  [{level}] {len(rows)} rows fetched, {n} upserted")
    return n


def sync(since: str, until: str, label: str):
    log.info(f"\n{'='*60}")
    log.info(f"{label.upper()} | {since} -> {until} (GMT)")
    log.info(f"{'='*60}")
    t0 = datetime.now()

    for lvl in ("adset", "campaign"):
        log.info(f"\n== level={lvl} ==")
        for acct in ACCOUNTS:
            log.info(f"\n>> {acct['name']}  ({lvl})")
            total = 0
            for cs, cu in date_chunks(since, until):
                try:
                    total += fetch_level(acct["id"], acct["name"], lvl, cs, cu)
                except Exception as e:
                    log.error(f"  chunk {cs}..{cu} failed: {e}")
            log.info(f"  == {acct['name']} [{lvl}] total upserted: {total:,}")

    elapsed = (datetime.now() - t0).total_seconds()
    log.info(f"\n{'='*60}")
    log.info(f"DONE  elapsed={elapsed:.0f}s")
    log.info(f"{'='*60}\n")


def daily():    sync(gmt_days_ago(15), gmt_today(), "daily")
def hourly():   sync(gmt_days_ago(3),  gmt_today(), "hourly")

def backfill():
    # Meta caps time_range at 37 months back. Snap to 36 months to leave a
    # safety margin (the 37 boundary can drift by a day between the request
    # firing and the server evaluating it, causing a hard 400).
    start = (datetime.utcnow() - timedelta(days=36 * 30)).strftime("%Y-%m-%d")
    end   = gmt_today()
    log.info(f"[backfill] window: {start} -> {end}")
    sync(start, end, "backfill")


if __name__ == "__main__":
    mode = (sys.argv[1] if len(sys.argv) > 1 else "daily").lower()
    if   mode == "daily":    daily()
    elif mode == "hourly":   hourly()
    elif mode == "backfill": backfill()
    else:
        print(f"unknown mode: {mode}. use daily|hourly|backfill")
        sys.exit(2)
