"""Rebuild summary_table from backfill_table.

One row per unique ad_id with lifetime aggregates and lifecycle status.
Run anytime backfill_table changes (e.g., after run_backfill.py).

Status rules (6 categories — mirrors dashboard getAdCategory):
    Incremental Winner = F1 AND F2 AND F3 AND F4
    Winner             = F1 AND F2 AND F3
    Priority (P0 ITE)  = F1 AND F4
    Analyze 1          = F1 only
    Analyze 2          = F2 only
    Discarded          = everything else

Thresholds:
    F1 = impressions ≥ 50,000
    F2 = conv_value / spend ≥ 3.2
    F3 = spend / ncp     ≤ 525  (positive)
    F4 = spend / ftewv   ≤ 12   (positive)
"""

import os
import sys
import time
import logging

import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("summary_table")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS summary_table (
    ad_id              TEXT PRIMARY KEY,
    ad_name            TEXT,
    created_date       DATE,
    last_seen          DATE,
    days_active        INT,
    total_impressions  BIGINT,
    total_spend        NUMERIC(14,2),
    total_conv_value   NUMERIC(14,2),
    total_ncp          INT,
    total_ftewv        INT,
    status             TEXT,
    refreshed_at       TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE summary_table ADD COLUMN IF NOT EXISTS refreshed_at TIMESTAMPTZ DEFAULT NOW();
"""

REFRESH_SQL = """
TRUNCATE summary_table;

INSERT INTO summary_table (
    ad_id, ad_name, created_date, last_seen, days_active,
    total_impressions, total_spend, total_conv_value, total_ncp, total_ftewv, status, refreshed_at
)
WITH agg AS (
    SELECT
        ad_id,
        (ARRAY_AGG(ad_name ORDER BY date DESC))[1]            AS ad_name,
        MIN(ad_created_date)::date                            AS created_date,
        MAX(date)                                             AS last_seen,
        COUNT(DISTINCT date)                                  AS days_active,
        COALESCE(SUM(impressions), 0)                         AS total_impressions,
        ROUND(COALESCE(SUM(amount_spent_inr), 0)::numeric, 2) AS total_spend,
        ROUND(COALESCE(SUM(conversion_value), 0)::numeric, 2) AS total_conv_value,
        COALESCE(SUM(ncp_count), 0)                           AS total_ncp,
        COALESCE(SUM(ftewv_count), 0)                         AS total_ftewv
    FROM backfill_table
    WHERE ad_id IS NOT NULL
    GROUP BY ad_id
)
SELECT
    ad_id, ad_name, created_date, last_seen, days_active,
    total_impressions, total_spend, total_conv_value, total_ncp, total_ftewv,
    CASE
        -- Incremental Winner = F1 AND F2 AND F3 AND F4
        WHEN total_impressions >= 50000
         AND total_spend > 0 AND total_conv_value / total_spend >= 3.2
         AND total_ncp   > 0 AND total_spend / total_ncp   <= 525
         AND total_ftewv > 0 AND total_spend / total_ftewv <= 12
            THEN 'Incremental Winner'
        -- Winner = F1 AND F2 AND F3
        WHEN total_impressions >= 50000
         AND total_spend > 0 AND total_conv_value / total_spend >= 3.2
         AND total_ncp   > 0 AND total_spend / total_ncp <= 525
            THEN 'Winner'
        -- Priority (P0 ITE) = F1 AND F4
        WHEN total_impressions >= 50000
         AND total_ftewv > 0 AND total_spend / total_ftewv <= 12
            THEN 'Priority'
        -- Analyze 1 = F1 only
        WHEN total_impressions >= 50000
            THEN 'Analyze 1'
        -- Analyze 2 = F2 only
        WHEN total_spend > 0 AND total_conv_value / total_spend >= 3.2
            THEN 'Analyze 2'
        ELSE 'Discarded'
    END  AS status,
    NOW() AS refreshed_at
FROM agg;
"""


def main():
    log.info("Connecting to Supabase…")
    t0 = time.time()
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '180s'")

    log.info("Ensuring summary_table schema exists…")
    cur.execute(SCHEMA_SQL)
    conn.commit()

    log.info("Running TRUNCATE + INSERT from backfill_table…")
    cur.execute(REFRESH_SQL)
    conn.commit()

    elapsed = time.time() - t0
    log.info(f"Done in {elapsed:.1f}s")

    # Report counts
    cur.execute("SELECT COUNT(*) FROM summary_table")
    total = cur.fetchone()[0]
    log.info(f"summary_table: {total:,} rows")

    cur.execute("SELECT status, COUNT(*) FROM summary_table GROUP BY status ORDER BY COUNT(*) DESC")
    log.info("Per-status breakdown:")
    for status, n in cur.fetchall():
        log.info(f"  {status:<20} {n:>6,}")

    cur.execute("""
        SELECT MIN(last_seen)::text, MAX(last_seen)::text,
               MIN(created_date)::text, MAX(created_date)::text
        FROM summary_table
    """)
    min_ls, max_ls, min_cd, max_cd = cur.fetchone()
    log.info(f"Last seen range : {min_ls} → {max_ls}")
    log.info(f"Created date range: {min_cd} → {max_cd}")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
