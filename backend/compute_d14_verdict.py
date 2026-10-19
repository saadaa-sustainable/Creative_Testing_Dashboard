"""compute_d14_verdict.py — fill summary_table's 14-day-buffer verdict.

WHY
---
summary_table carried only each ad's CURRENT category. The org declares a
result at the end of a 14-day buffer and treats everything before that as
"Result Awaited", so the verdict that actually gets declared was
unrecoverable for every historical ad -- an ad that was an Incremental
Winner at day 14 and drifted to P1 since reads as P1 forever.

This computes the day-14 verdict from the daily tables and stores it, so
both views exist side by side: d14_category (what we declared) and
category (what it is now).

Only the verdict is stored. The underlying d14 metrics (impressions, spend,
roas, cost per NCP / FTEWV) are deliberately NOT columns -- they are fully
reproducible by re-running this script, and summary_table is wide enough.
Re-add them here and in a migration if an audit ever needs them.

THE WINDOW
    [ad_created_date, ad_created_date + 14] inclusive.
Matches result_classifier.py, which is the org's existing definition
(WINDOW_DAYS = 14, evaluation_end_date = ad_created_date + 14). Do not
"fix" this to +13 for a 14-day span -- it would silently disagree with
ad_results.

THE VERDICT
    F1  impressions   >= 50,000
    F2  roas          >= ROAS_MIN          (conv_value / spend)
    F3  cost per NCP  <= 525               (spend / ncp_count)
    F4  cost per FTEWV<= 12                (spend / ftewv_count)

    Incremental Winner  F1 AND (F2 OR F3) AND F4
    Winner              F1 AND (F2 OR F3)
    P0 analysis         F1 AND F4
    P1 analysis         F1
    P2 analysis         F2
    Discarded           otherwise
Priority order matters -- first match wins, mirroring ae_table_view.

    Result Awaited      the 14-day window has not closed yet
    No delivery         no rows at all in the window (ad never served)

THRESHOLDS ARE NOW CANONICAL EVERYWHERE (aligned 2026-10-19)
refresh_summary_table.py, refresh_ae_table.py, results_sync.py,
hourly_impressions.py and assets/dashboard.js all use the same four
numbers, so d14_category and category are computed on identical rules
and drift between them is real drift rather than a threshold artifact.
refresh_summary_table previously used ROAS 3.2 and cost-per-FTEWV 25,
and refresh_ae_table used ROAS 3.2. Change one, change all six.

SOURCE
    backfill_table, which spans launch onward (primary_table only keeps
    ~6-9 months, so it cannot see 2022-2024 ads at all).

USAGE
    python compute_d14_verdict.py              # compute + write
    python compute_d14_verdict.py --dry-run    # distribution only, no write
    python compute_d14_verdict.py --recompute  # redo rows already filled
"""
from __future__ import annotations
import argparse
import os
import sys

import psycopg2
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")

WINDOW_DAYS = 14          # keep in step with result_classifier.WINDOW_DAYS
F1_IMPRESSIONS = 50_000
ROAS_MIN       = 3.0      # canonical; see the note in the docstring
F3_COST_NCP    = 525
F4_COST_FTEWV  = 12

# One pass: aggregate the window, derive the flags, pick the verdict.
# NULLIF guards every denominator; a zero-count ad fails that filter rather
# than dividing by zero.
COMPUTE_SQL = f"""
WITH win AS (
    SELECT b.ad_id,
           MIN(b.ad_created_date)                        AS created,
           MIN(b.ad_created_date) + {WINDOW_DAYS}        AS d14_end,
           COALESCE(SUM(b.impressions), 0)               AS imp,
           COALESCE(SUM(b.amount_spent_inr), 0)          AS spend,
           COALESCE(SUM(b.conversion_value), 0)          AS conv,
           COALESCE(SUM(b.ncp_count), 0)                 AS ncp,
           COALESCE(SUM(b.ftewv_count), 0)               AS ftewv
    FROM public.backfill_table b
    WHERE b.ad_created_date IS NOT NULL
      AND b.date >= b.ad_created_date
      AND b.date <= b.ad_created_date + {WINDOW_DAYS}
    GROUP BY b.ad_id
),
flagged AS (
    SELECT w.*,
           (w.imp >= {F1_IMPRESSIONS})                                        AS f1,
           (w.spend > 0 AND w.conv / NULLIF(w.spend,0) >= {ROAS_MIN})          AS f2,
           (w.ncp   > 0 AND w.spend / NULLIF(w.ncp,0)   <= {F3_COST_NCP})      AS f3,
           (w.ftewv > 0 AND w.spend / NULLIF(w.ftewv,0) <= {F4_COST_FTEWV})    AS f4
    FROM win w
)
UPDATE public.summary_table s
SET d14_category = CASE
        -- Window still open: the org calls this Result Awaited, and it is
        -- NOT the same as failing every filter.
        WHEN f.d14_end > CURRENT_DATE                     THEN 'Result Awaited'
        WHEN f.f1 AND (f.f2 OR f.f3) AND f.f4             THEN 'Incremental Winner'
        WHEN f.f1 AND (f.f2 OR f.f3)                      THEN 'Winner'
        WHEN f.f1 AND f.f4                                THEN 'P0 analysis'
        WHEN f.f1                                         THEN 'P1 analysis'
        WHEN f.f2                                         THEN 'P2 analysis'
        ELSE 'Discarded' END
FROM flagged f
WHERE s.ad_id = f.ad_id
  AND ({{recompute}} OR s.d14_category IS NULL)
"""

# Ads with no row in the window never served inside it. Distinguished from
# 'Discarded' (served, failed every filter) because conflating them makes
# the Discarded bucket look far worse than it is.
NO_DELIVERY_SQL = """
UPDATE public.summary_table s
SET d14_category = 'No delivery'
WHERE s.d14_category IS NULL
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--recompute", action="store_true",
                    help="overwrite rows that already have a verdict")
    args = ap.parse_args()

    conn = psycopg2.connect(DB_URL, connect_timeout=60)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")

    if args.dry_run:
        cur.execute(COMPUTE_SQL.replace("UPDATE public.summary_table s\nSET", "SELECT 1 -- ")
                    .split("UPDATE")[0] + """
            SELECT CASE
                WHEN d14_end > CURRENT_DATE           THEN 'Result Awaited'
                WHEN f1 AND (f2 OR f3) AND f4         THEN 'Incremental Winner'
                WHEN f1 AND (f2 OR f3)                THEN 'Winner'
                WHEN f1 AND f4                        THEN 'P0 analysis'
                WHEN f1                               THEN 'P1 analysis'
                WHEN f2                               THEN 'P2 analysis'
                ELSE 'Discarded' END AS verdict,
                COUNT(*)
            FROM flagged GROUP BY 1 ORDER BY 2 DESC""")
        print(f"{'d14_category':22s} {'ads':>8s}")
        for v, n in cur.fetchall():
            print(f"{v:22s} {n:>8,}")
        conn.rollback()
        return 0

    cur.execute(COMPUTE_SQL.format(recompute="TRUE" if args.recompute else "FALSE"))
    n_computed = cur.rowcount
    cur.execute(NO_DELIVERY_SQL)
    n_nodelivery = cur.rowcount
    conn.commit()

    print(f"computed from daily data : {n_computed:,}")
    print(f"marked 'No delivery'     : {n_nodelivery:,}")

    cur.execute("""SELECT COALESCE(d14_category,'(null)'), COUNT(*)
                   FROM public.summary_table GROUP BY 1 ORDER BY 2 DESC""")
    print(f"\n{'d14_category':22s} {'ads':>8s}")
    for v, n in cur.fetchall():
        print(f"{v:22s} {n:>8,}")
    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
