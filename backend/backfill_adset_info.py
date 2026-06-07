"""
backfill_adset_info.py — patch missing adset_id / adset_name / campaign_name in ae_raw_view.

For every distinct ad_id in ae_raw_view where adset_id is empty/NULL, fetches
adset and campaign info directly from Meta via the lightweight batched IDs
endpoint (/?ids=...&fields=adset{id,name},campaign{id,name}).

Batched 50 ads per call → ~190 API calls for 9.4K ads. Uses the same retry
logic as refresh_ae_from_meta.py (3 retries with 60s/120s/180s backoff on
403 app-rate-limit; 30s/60s/90s on 5xx).

Usage:
    python backfill_adset_info.py

Exit codes:
    0  success (all batches completed)
    1  Meta API or DB failure
"""

import os
import sys
import time
import logging
from collections import defaultdict

import requests
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()
ACCESS_TOKEN = os.environ["META_ACCESS_TOKEN"]
DB_URL = os.environ["SUPABASE_DB_URL"]

BATCH_SIZE = 50               # Meta /?ids= caps at 50
SLEEP_BETWEEN_BATCHES = 0.4   # safe pacing — 2.5 req/sec
RETRIES = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("backfill_adset")


def meta_get(url, retries=RETRIES):
    """Returns parsed JSON. Retries on 429/5xx/403-app-limit with backoff."""
    for i in range(retries):
        try:
            r = requests.get(url, timeout=60)
        except requests.RequestException as e:
            log.warning(f"Network error: {e}; retry {i+1}/{retries}")
            time.sleep(20 * (i + 1))
            continue
        if r.status_code == 200:
            return r.json()
        retryable = r.status_code in (429, 500, 502, 503, 504)
        if r.status_code == 403:
            try:
                if r.json().get("error", {}).get("code") == 4:
                    retryable = True
            except Exception:
                pass
        if retryable and i < retries - 1:
            sleep_s = 60 * (i + 1) if r.status_code == 403 else 30 * (i + 1)
            log.warning(f"HTTP {r.status_code} (retry {i+1}/{retries}); sleeping {sleep_s}s")
            time.sleep(sleep_s)
            continue
        log.error(f"HTTP {r.status_code}: {r.text[:300]}")
        r.raise_for_status()
    raise RuntimeError(f"Meta API failed after {retries} retries")


def fetch_adset_batch(ad_ids):
    """Returns dict { ad_id: (adset_id, adset_name, campaign_name) } for the batch."""
    if not ad_ids:
        return {}
    url = (
        f"https://graph.facebook.com/v21.0/?ids={','.join(ad_ids)}"
        f"&fields=adset{{id,name}},campaign{{id,name}}"
        f"&access_token={ACCESS_TOKEN}"
    )
    j = meta_get(url)
    out = {}
    for ad_id, info in j.items():
        if not isinstance(info, dict):
            continue
        adset = info.get("adset") or {}
        campaign = info.get("campaign") or {}
        out[ad_id] = (
            adset.get("id") or "",
            adset.get("name") or "",
            campaign.get("name") or "",
        )
    return out


UPDATE_SQL = """
    UPDATE ae_raw_view
    SET adset_id      = %s,
        adset_name    = %s,
        campaign_name = COALESCE(NULLIF(%s, ''), campaign_name)
    WHERE ad_id = %s
"""


def main():
    log.info("Connecting to Supabase…")
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    log.info("Fetching ad_ids with missing adset_id from ae_raw_view…")
    cur.execute("""
        SELECT DISTINCT ad_id
        FROM ae_raw_view
        WHERE adset_id IS NULL OR adset_id = ''
        ORDER BY ad_id
    """)
    ad_ids = [r[0] for r in cur.fetchall() if r[0]]
    log.info(f"  {len(ad_ids):,} ads need backfill")
    if not ad_ids:
        log.info("Nothing to do.")
        cur.close(); conn.close()
        return 0

    total_batches = (len(ad_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    log.info(f"  → {total_batches} batches of up to {BATCH_SIZE} ads each")

    all_updates = []
    skipped = 0
    t0 = time.time()
    for i in range(0, len(ad_ids), BATCH_SIZE):
        batch = ad_ids[i : i + BATCH_SIZE]
        batch_idx = i // BATCH_SIZE + 1
        try:
            fetched = fetch_adset_batch(batch)
        except Exception as e:
            log.error(f"Batch {batch_idx}/{total_batches} failed: {e}; skipping batch")
            skipped += len(batch)
            continue

        for ad_id in batch:
            tup = fetched.get(ad_id)
            if tup is None:
                continue
            adset_id, adset_name, campaign_name = tup
            if not adset_id:
                continue   # nothing to update
            all_updates.append((adset_id, adset_name, campaign_name, ad_id))

        if batch_idx % 10 == 0 or batch_idx == total_batches:
            elapsed = time.time() - t0
            rate = batch_idx / elapsed * 60 if elapsed > 0 else 0
            log.info(
                f"  Batch {batch_idx}/{total_batches}  "
                f"updates pending={len(all_updates):,}  skipped={skipped}  "
                f"rate={rate:.1f} batch/min"
            )

        time.sleep(SLEEP_BETWEEN_BATCHES)

    log.info(f"Applying {len(all_updates):,} UPDATEs to ae_raw_view…")
    if all_updates:
        execute_batch(cur, UPDATE_SQL, all_updates, page_size=500)
        conn.commit()

    # Final stats
    cur.execute("SELECT COUNT(*) FROM ae_raw_view WHERE adset_id IS NULL OR adset_id = ''")
    remaining = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM ae_raw_view")
    total = cur.fetchone()[0]
    log.info(f"\nDone. Total: {total:,}  |  Still missing adset_id: {remaining:,}  ({remaining/total*100:.1f}%)")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
