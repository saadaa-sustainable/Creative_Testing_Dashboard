"""sync_ad_metadata.py — keep summary_table.ad_name / ad_status true for EVERY ad.

WHY
---
summary_table is the source of truth for the dashboard, and two of its
columns were quietly wrong for most of the account.

Both are derived from the daily tables, which come from Meta's *insights*
endpoint:

    ad_name    (ARRAY_AGG(ad_name ORDER BY date DESC))[1]  -- refresh_summary_table
    ad_status   latest non-NULL primary_table.ad_status

Insights only returns a row for an ad that DELIVERED that day. An ad that
stops spending therefore stops producing rows, and its name and status
freeze at its last delivering day -- permanently. Renames and pause/archive
events after that day are invisible. Nothing in the pipeline ever corrected
this, because every table downstream is built from the same insights rows.

Measured on 2026-10-15, before this script existed:
  * ads 120246457342780422 and 120246457506960422 were renamed on Meta on
    2026-10-14. summary_table, primary_table and ae_table_view all still
    carried the pre-rename name, last_seen 2026-08-10 -- and both ads were
    stored under the SAME truncated name, so they were indistinguishable.
  * of 25 sampled ads stored ACTIVE with zero 30-day spend, 16 were wrong:
    Meta said ADSET_PAUSED (11), CAMPAIGN_PAUSED (3), ARCHIVED (2).
  * 2,408 rows had ad_status NULL, which only meant "has not delivered
    lately" -- never an actual Meta status.

THE FIX
-------
The /act_{id}/ads edge returns every ad regardless of delivery, so it sees
what insights cannot. A few paginated requests per account, no per-ad calls.
We store Meta's answer in ad_metadata and then push it into summary_table.

!! THE EXPLICIT STATUS FILTER IS NOT OPTIONAL !!
By default the /ads edge HIDES archived and deleted ads. Account 1 returns
13,551 ads unfiltered and 16,736 with the filter below -- 3,185 ads, 23%,
silently missing. Those are exactly the ads most likely to be stale, so a
sync without the filter would leave the worst cases untouched and look like
it had worked. Never drop `filtering` from the request.

effective_status vs configured_status
-------------------------------------
We store both and write effective_status to summary_table.ad_status.
effective_status is what Ads Manager shows and folds in inherited parent
state (ADSET_PAUSED / CAMPAIGN_PAUSED), which is what the existing column
already contained; configured_status is only the ad's own switch and reads
ACTIVE for an ad sitting in a paused adset. Keeping configured_status
alongside makes "live ad, paused parent" answerable without another sync.

ORDERING
--------
refresh_summary_table rebuilds the whole table and its ON CONFLICT clause
overwrites ad_name and ad_status from the insights-derived values, so it
would undo this script's work. Rather than depend on step order, that
script COALESCEs both columns out of ad_metadata, so Meta truth wins even
when the refresh runs alone. This script still writes summary_table
directly, so a sync on its own corrects the table immediately.

USAGE
    python sync_ad_metadata.py              # fetch, store, update summary_table
    python sync_ad_metadata.py --dry-run    # fetch + report drift, write nothing
    python sync_ad_metadata.py --accounts 1 # one account only
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, ".env"))

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
TOKEN  = (os.environ.get("META_ACCESS_TOKEN") or "").strip()
VER    = (os.environ.get("META_API_VERSION") or "v22.0").strip()
API    = f"https://graph.facebook.com/{VER}"


ACCOUNTS = [
    ("1", os.environ.get("ACCOUNT_1_ID", "1136644150469466"), "Raho Saadaa"),
    ("2", os.environ.get("ACCOUNT_2_ID", "1349767139294217"), "Fourth Ad Account"),
    ("3", os.environ.get("ACCOUNT_3_ID", "264868699479122"),  "Third Ad Account"),
]

# Every status Meta can report. Without this the edge omits ARCHIVED and
# DELETED -- see the warning in the docstring.
ALL_STATUSES = [
    "ACTIVE", "PAUSED", "DELETED", "PENDING_REVIEW", "DISAPPROVED",
    "PREAPPROVED", "PENDING_BILLING_INFO", "CAMPAIGN_PAUSED", "ARCHIVED",
    "ADSET_PAUSED", "IN_PROCESS", "WITH_ISSUES",
]
FIELDS = ("id,name,effective_status,configured_status,"
          "updated_time,created_time,adset_id,campaign_id")
PAGE_LIMIT = 500

DDL = """
CREATE TABLE IF NOT EXISTS public.ad_metadata (
    ad_id             TEXT PRIMARY KEY,
    account_id        TEXT,
    account_label     TEXT,
    ad_name           TEXT,
    effective_status  TEXT,
    configured_status TEXT,
    adset_id          TEXT,
    campaign_id       TEXT,
    meta_created_time TIMESTAMPTZ,
    meta_updated_time TIMESTAMPTZ,
    synced_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ad_metadata_eff_status_idx
    ON public.ad_metadata (effective_status);
CREATE INDEX IF NOT EXISTS ad_metadata_account_idx
    ON public.ad_metadata (account_id);
"""

UPSERT = """
INSERT INTO public.ad_metadata (
    ad_id, account_id, account_label, ad_name, effective_status,
    configured_status, adset_id, campaign_id, meta_created_time,
    meta_updated_time, synced_at
) VALUES %s
ON CONFLICT (ad_id) DO UPDATE SET
    account_id        = EXCLUDED.account_id,
    account_label     = EXCLUDED.account_label,
    ad_name           = EXCLUDED.ad_name,
    effective_status  = EXCLUDED.effective_status,
    configured_status = EXCLUDED.configured_status,
    adset_id          = EXCLUDED.adset_id,
    campaign_id       = EXCLUDED.campaign_id,
    meta_created_time = EXCLUDED.meta_created_time,
    meta_updated_time = EXCLUDED.meta_updated_time,
    synced_at         = EXCLUDED.synced_at
"""

# Only touch rows that actually differ, so refreshed_at-style auditing and
# the row count below both mean something.
PUSH_TO_SUMMARY = """
UPDATE public.summary_table s
SET ad_name   = m.ad_name,
    ad_status = m.effective_status
FROM public.ad_metadata m
WHERE s.ad_id = m.ad_id
  AND (s.ad_name   IS DISTINCT FROM m.ad_name
    OR s.ad_status IS DISTINCT FROM m.effective_status)
"""


def scrub(text: str) -> str:
    """Never let the token reach a log line or a traceback."""
    return text.replace(TOKEN, "<REDACTED>") if TOKEN else text


def fetch_account(account_id: str, label: str) -> list[dict]:
    """Walk /act_{id}/ads. Returns one dict per ad, all statuses included."""
    url = f"{API}/act_{account_id}/ads"
    params = {
        "fields": FIELDS,
        "limit": PAGE_LIMIT,
        "filtering": json.dumps(
            [{"field": "ad.effective_status", "operator": "IN", "value": ALL_STATUSES}]),
        "access_token": TOKEN,
    }
    out: list[dict] = []
    page = 0
    while url:
        for attempt in range(5):
            try:
                r = requests.get(url, params=params, timeout=120)
            except requests.RequestException as exc:
                if attempt == 4:
                    raise RuntimeError(scrub(f"{label}: {exc}")) from None
                time.sleep(5 * (attempt + 1)); continue
            if r.status_code == 200:
                break
            err = (r.json().get("error") or {}) if r.headers.get(
                "content-type", "").startswith("application/json") else {}
            # 17 = user request limit, 4 = app limit, 80004 = ad-account limit.
            if r.status_code in (429, 500, 502, 503) or err.get("code") in (4, 17, 80004):
                if attempt == 4:
                    raise RuntimeError(scrub(f"{label}: throttled, gave up: {err}"))
                time.sleep(30 * (attempt + 1)); continue
            raise RuntimeError(scrub(f"{label}: HTTP {r.status_code} {err.get('message','')}"))

        body = r.json()
        out.extend(body.get("data", []))
        page += 1
        url = (body.get("paging") or {}).get("next")
        params = None          # the `next` URL already carries every param
        if url:
            time.sleep(0.2)
    print(f"  {label:20s} {len(out):>6,} ads  ({page} pages)")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and report drift, write nothing")
    ap.add_argument("--accounts", default="1,2,3",
                    help="comma-separated account keys to sync (default all)")
    args = ap.parse_args()

    # Checked here, not at import time: refresh_summary_table.py imports DDL
    # from this module and needs no Meta token, so a module-level exit would
    # take the nightly summary rebuild down with it.
    if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")
    if not TOKEN:  sys.exit("Missing META_ACCESS_TOKEN in .env")

    wanted = {k.strip() for k in args.accounts.split(",") if k.strip()}
    targets = [a for a in ACCOUNTS if a[0] in wanted]
    if not targets:
        sys.exit(f"No accounts matched --accounts {args.accounts!r}")

    print(f"Fetching ad metadata from Meta ({VER}):")
    rows = []
    for _key, acct, label in targets:
        for ad in fetch_account(acct, label):
            rows.append((
                ad.get("id"), acct, label, ad.get("name"),
                ad.get("effective_status"), ad.get("configured_status"),
                ad.get("adset_id"), ad.get("campaign_id"),
                ad.get("created_time"), ad.get("updated_time"),
            ))
    print(f"  {'TOTAL':20s} {len(rows):>6,} ads")
    if not rows:
        sys.exit("Meta returned no ads at all — refusing to touch the database")

    conn = psycopg2.connect(DB_URL, connect_timeout=60)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")
    cur.execute(DDL)

    execute_values(cur, UPSERT, rows,
                   template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                   page_size=1000)
    print(f"\nad_metadata upserted      : {len(rows):,}")

    # Drift report: how wrong was summary_table before this ran?
    cur.execute("""
        SELECT COUNT(*) FILTER (WHERE s.ad_name   IS DISTINCT FROM m.ad_name),
               COUNT(*) FILTER (WHERE s.ad_status IS DISTINCT FROM m.effective_status),
               COUNT(*)
        FROM public.summary_table s
        JOIN public.ad_metadata m USING (ad_id)
    """)
    bad_name, bad_status, matched = cur.fetchone()
    print(f"summary_table rows matched: {matched:,}")
    print(f"  stale ad_name           : {bad_name:,}")
    print(f"  stale ad_status         : {bad_status:,}")

    cur.execute("""SELECT COUNT(*) FROM public.summary_table s
                   WHERE NOT EXISTS (SELECT 1 FROM public.ad_metadata m
                                     WHERE m.ad_id = s.ad_id)""")
    print(f"  in summary, not on Meta : {cur.fetchone()[0]:,}")

    if args.dry_run:
        conn.rollback()
        print("\n--dry-run: rolled back, nothing written")
        return 0

    cur.execute(PUSH_TO_SUMMARY)
    print(f"\nsummary_table rows fixed  : {cur.rowcount:,}")
    conn.commit()

    cur.execute("""SELECT COALESCE(ad_status,'(null)'), COUNT(*)
                   FROM public.summary_table GROUP BY 1 ORDER BY 2 DESC""")
    print(f"\n{'ad_status':22s} {'ads':>8s}")
    for v, n in cur.fetchall():
        print(f"{v:22s} {n:>8,}")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
