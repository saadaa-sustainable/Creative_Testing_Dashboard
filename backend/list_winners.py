"""list_winners.py — ads that won, by name token, at day 14 and/or today.

WHY TWO VERDICTS
----------------
summary_table.status is the ad's CURRENT category, recomputed nightly from
lifetime totals. summary_table.d14_category is the verdict as it stood at
the end of the 14-day buffer the org declares results on (see
compute_d14_verdict.py). They disagree often: an ad can win inside its
window and decay afterwards, or build up to a win months later.

This lists both populations and labels which one each ad belongs to:

    d14 only   won in its first 14 days, no longer a winner
    both       won then, still winning
    now only   not a winner at day 14, winning today

THRESHOLDS
All classifiers share one rule (aligned 2026-10-19): F1 impressions
>= 50,000, F2 ROAS >= 3.0 OR cost per NCP <= 525, F3 cost per FTEWV
<= 12. status and d14_category are therefore computed on identical
rules and differ only in the window they cover, which is the whole
point of comparing them. Before that alignment refresh_summary_table
used ROAS 3.2 and cost-per-FTEWV 25, so a third of the "d14 only"
bucket was a threshold artifact rather than real decay.

WINNER means Winner or Incremental Winner. Incremental Winner is the
stricter tier (it additionally passes F4, cost per FTEWV), so both are
reported and the tier is kept in the output rather than flattened.

The day-14 metrics are recomputed here rather than read from a column:
compute_d14_verdict stores only the verdict, by design.

USAGE
    python list_winners.py --match IFAD
    python list_winners.py --match IFAD --out /tmp/ifad_winners
"""
from __future__ import annotations
import argparse
import csv
import os
import sys

import psycopg2
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
WINNERS = ("Winner", "Incremental Winner")

# The token is matched on a boundary, not as a substring: a bare ILIKE
# '%IFAD%' also catches IFADTEST, which is a different (test) token and
# would quietly pad the list.
SQL = """
WITH tagged AS (
    SELECT s.ad_id, s.ad_name, s.created_date, s.ad_status,
           s.status AS now_category, s.d14_category,
           s.total_spend, s.total_impressions, s.total_conv_value,
           s.total_ncp, s.total_ftewv,
           s.shopify_orders, s.shopify_sales
    FROM public.summary_table s
    WHERE s.ad_name ~* ('(^|[-+_ ])' || %(tok)s || '([-+_ ]|$)')
      AND (s.d14_category = ANY(%(win)s) OR s.status = ANY(%(win)s))
),
d14 AS (
    SELECT b.ad_id,
           COALESCE(SUM(b.impressions),0)      AS imp,
           COALESCE(SUM(b.amount_spent_inr),0) AS spend,
           COALESCE(SUM(b.conversion_value),0) AS conv,
           COALESCE(SUM(b.ncp_count),0)        AS ncp,
           COALESCE(SUM(b.ftewv_count),0)      AS ftewv
    FROM public.backfill_table b
    JOIN tagged t USING (ad_id)
    WHERE b.ad_created_date IS NOT NULL
      AND b.date >= b.ad_created_date
      AND b.date <= b.ad_created_date + 14
    GROUP BY 1
),
whereran AS (
    SELECT DISTINCT ON (b.ad_id) b.ad_id, b.campaign_name, b.adset_name
    FROM public.backfill_table b
    JOIN tagged t USING (ad_id)
    WHERE b.impressions > 0          -- skip post-2026-06 filler rows
    ORDER BY b.ad_id, b.date DESC
)
SELECT t.ad_id, t.ad_name, t.created_date,
       CASE WHEN t.d14_category = ANY(%(win)s) AND t.now_category = ANY(%(win)s)
                 THEN 'both'
            WHEN t.d14_category = ANY(%(win)s) THEN 'd14 only'
            ELSE 'now only' END                          AS bucket,
       t.d14_category, t.now_category, t.ad_status,
       w.campaign_name, w.adset_name,
       d.imp, d.spend, ROUND((d.conv / NULLIF(d.spend,0))::numeric, 2) AS d14_roas,
       d.conv, d.ncp, ROUND((d.spend / NULLIF(d.ncp,0))::numeric, 1)   AS d14_cost_ncp,
       d.ftewv, ROUND((d.spend / NULLIF(d.ftewv,0))::numeric, 2)       AS d14_cost_ftewv,
       t.total_impressions, t.total_spend,
       ROUND((t.total_conv_value / NULLIF(t.total_spend,0))::numeric, 2) AS life_roas,
       t.total_conv_value, t.total_ncp,
       ROUND((t.total_spend / NULLIF(t.total_ncp,0))::numeric, 1)        AS life_cost_ncp,
       ROUND((t.total_spend / NULLIF(t.total_ftewv,0))::numeric, 2) AS life_cost_ftewv,
       t.shopify_orders, t.shopify_sales
FROM tagged t
LEFT JOIN d14 d      USING (ad_id)
LEFT JOIN whereran w USING (ad_id)
ORDER BY t.total_spend DESC NULLS LAST
"""

HEADERS = ["ad_id", "ad_name", "created_date", "bucket", "d14_category",
           "now_category", "ad_status_meta", "campaign_name", "adset_name",
           "d14_impressions", "d14_spend", "d14_roas", "d14_conv_value",
           "d14_ncp", "d14_cost_per_ncp", "d14_ftewv", "d14_cost_per_ftewv",
           "lifetime_impressions", "lifetime_spend", "lifetime_roas",
           "lifetime_conv_value", "lifetime_ncp", "lifetime_cost_per_ncp",
           "lifetime_cost_per_ftewv",
           "shopify_orders", "shopify_sales"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--match", required=True, help="name token, e.g. IFAD")
    ap.add_argument("--out", help="output path without extension")
    args = ap.parse_args()
    if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")

    conn = psycopg2.connect(DB_URL, connect_timeout=60)
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")
    cur.execute(SQL, {"tok": args.match, "win": list(WINNERS)})
    rows = cur.fetchall()
    cur.close(); conn.close()

    base = args.out or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    f"{args.match.lower()}_winners")
    with open(base + ".csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh); w.writerow(HEADERS); w.writerows(rows)

    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
        wb = Workbook(); ws = wb.active; ws.title = f"{args.match} winners"
        ws.append(HEADERS)
        for cell in ws[1]: cell.font = Font(bold=True)
        for r in rows: ws.append(list(r))
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        for col, width in (("B", 62), ("H", 38), ("I", 38), ("C", 12)):
            ws.column_dimensions[col].width = width
        wb.save(base + ".xlsx")
        xlsx = base + ".xlsx"
    except Exception as exc:
        xlsx = f"(xlsx skipped: {exc})"

    by = {}
    for r in rows:
        by[r[3]] = by.get(r[3], 0) + 1
    print(f"{args.match} winners: {len(rows):,} ads")
    for k in ("both", "d14 only", "now only"):
        if k in by: print(f"  {k:10s} {by[k]:>5,}")
    print(f"\nwrote {base}.csv")
    print(f"wrote {xlsx}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
