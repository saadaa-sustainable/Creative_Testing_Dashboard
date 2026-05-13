"""
result_classifier.py — 14-Day Buffer Lifecycle Resolver

For every unique (account_name, ad_id) in primary_table, computes the ad's
14-day-from-creation impression total and assigns one of three statuses:

  Winner          impressions_14d >= 50_000 (locked in once crossed)
  Result Pending  today <= evaluation_end_date AND impressions_14d < 50_000
  Failed          today >  evaluation_end_date AND impressions_14d < 50_000

Writes the result to ad_results, UPSERTed by (account_name, ad_id).

Usage:
    python result_classifier.py            # full recompute, upsert all
    python result_classifier.py status     # row count + breakdown by status
"""

import os
import sys
import logging
from collections import defaultdict
from datetime import date, timedelta
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("result_classifier.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
WINDOW_DAYS = 14
IMPRESSION_THRESHOLD = 50_000
BATCH_SIZE = 200


@contextmanager
def get_conn():
    conn = psycopg2.connect(SUPABASE_DB_URL, connect_timeout=30)
    conn.autocommit = False
    # Bump statement_timeout to 10 minutes — the unfiltered SELECT on ~100k rows of
    # primary_table can exceed Supabase's pooler default and get cancelled.
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '600s'")
    except Exception:
        pass  # pooler may reject SET; we'll try the SELECT anyway
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


UPSERT_SQL = """
INSERT INTO ad_results (
    account_name, ad_id, ad_name, campaign_name, ad_status,
    ad_created_date, evaluation_end_date,
    impressions_14d, crossed_threshold_at, result_status,
    last_computed_at
) VALUES (
    %(account_name)s, %(ad_id)s, %(ad_name)s, %(campaign_name)s, %(ad_status)s,
    %(ad_created_date)s, %(evaluation_end_date)s,
    %(impressions_14d)s, %(crossed_threshold_at)s, %(result_status)s,
    NOW()
)
ON CONFLICT (account_name, ad_id) DO UPDATE SET
    ad_name              = EXCLUDED.ad_name,
    campaign_name        = EXCLUDED.campaign_name,
    ad_status            = EXCLUDED.ad_status,
    ad_created_date      = EXCLUDED.ad_created_date,
    evaluation_end_date  = EXCLUDED.evaluation_end_date,
    impressions_14d      = EXCLUDED.impressions_14d,
    crossed_threshold_at = EXCLUDED.crossed_threshold_at,
    result_status        = EXCLUDED.result_status,
    last_computed_at     = NOW()
"""


def compute_all() -> dict:
    """Recompute ad_results for every ad in primary_table. Returns counts dict."""
    today = date.today()

    with get_conn() as conn:
        log.info("Pulling primary_table rows for classification…")
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT account_name, ad_id, ad_name, campaign_name, ad_status,
                       ad_created_date, date, impressions
                  FROM primary_table
                 WHERE ad_created_date IS NOT NULL
                """
            )
            rows = cur.fetchall()
        log.info(f"  Loaded {len(rows):,} primary_table rows")

        # Group rows by (account_name, ad_id)
        ads: dict[tuple, dict] = {}
        for r in rows:
            key = (r["account_name"], r["ad_id"])
            if key not in ads:
                ads[key] = {
                    "account_name": r["account_name"],
                    "ad_id": r["ad_id"],
                    "ad_name": r["ad_name"],
                    "campaign_name": r["campaign_name"],
                    "ad_status": r["ad_status"],
                    "ad_created_date": r["ad_created_date"],
                    "days": [],
                }
            else:
                # prefer non-empty meta
                if r["ad_name"] and not ads[key]["ad_name"]:
                    ads[key]["ad_name"] = r["ad_name"]
                if r["campaign_name"] and not ads[key]["campaign_name"]:
                    ads[key]["campaign_name"] = r["campaign_name"]
                # keep latest non-paused status
                rs = (r["ad_status"] or "").upper()
                if rs == "ACTIVE":
                    ads[key]["ad_status"] = r["ad_status"]
            ads[key]["days"].append((r["date"], int(r["impressions"] or 0)))

        log.info(f"  {len(ads):,} unique ads to classify")

        results = []
        counts: dict[str, int] = defaultdict(int)

        for ad in ads.values():
            created = ad["ad_created_date"]
            eval_end = created + timedelta(days=WINDOW_DAYS)

            # Filter to rows inside the 14-day window, sorted by date ascending
            window_rows = sorted(
                [(d, imp) for d, imp in ad["days"] if created <= d <= eval_end],
                key=lambda x: x[0],
            )
            impr_14d = sum(imp for _, imp in window_rows)

            # First date the cumulative total crossed the threshold (if any)
            crossed = None
            cum = 0
            for d, imp in window_rows:
                cum += imp
                if cum >= IMPRESSION_THRESHOLD:
                    crossed = d
                    break

            # Status decision
            if impr_14d >= IMPRESSION_THRESHOLD:
                status = "Winner"
            elif today <= eval_end:
                status = "Result Pending"
            else:
                status = "Failed"

            counts[status] += 1
            results.append(
                {
                    "account_name": ad["account_name"],
                    "ad_id": ad["ad_id"],
                    "ad_name": ad["ad_name"] or "",
                    "campaign_name": ad["campaign_name"] or "",
                    "ad_status": ad["ad_status"] or "",
                    "ad_created_date": created,
                    "evaluation_end_date": eval_end,
                    "impressions_14d": impr_14d,
                    "crossed_threshold_at": crossed,
                    "result_status": status,
                }
            )

        # UPSERT in batches
        log.info(f"Upserting {len(results):,} rows into ad_results…")
        with conn.cursor() as cur:
            for i in range(0, len(results), BATCH_SIZE):
                batch = results[i : i + BATCH_SIZE]
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, batch, page_size=100)

        log.info("Classification complete.")
        log.info(f"  Winner:          {counts.get('Winner', 0):>6,}")
        log.info(f"  Result Pending:  {counts.get('Result Pending', 0):>6,}")
        log.info(f"  Failed:          {counts.get('Failed', 0):>6,}")
        log.info(f"  TOTAL:           {len(results):>6,}")
        return dict(counts)


def status() -> None:
    """Print row counts grouped by result_status."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT result_status, COUNT(*) FROM ad_results
                    GROUP BY result_status ORDER BY result_status"""
            )
            for s, n in cur.fetchall():
                print(f"  {s:<18} {n:>6,}")
            cur.execute("SELECT COUNT(*) FROM ad_results")
            print(f"  {'TOTAL':<18} {cur.fetchone()[0]:>6,}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "compute"
    if cmd == "status":
        status()
    else:
        compute_all()
