"""analyze_before_after.py — before/after impact for an adset or campaign.

WHAT IT ANSWERS
---------------
"We changed something on this adset/campaign on date D. What happened?"
It aggregates equal-length windows either side of D and reports the delta
on spend, delivery, efficiency and conversion metrics.

    before = [D - W, D - 1]      after = [D, D + W - 1]
D belongs to AFTER: a change made on D affects that day's delivery.

!! THIS IS OBSERVATIONAL, NOT CAUSAL !!
Nothing here isolates the edit from everything else moving at the same
time -- seasonality, sale periods, auction pressure, budget shifts
elsewhere in the account, and Meta's own learning phase all land in the
same window. A 30% ROAS lift after a creative swap is a correlation and
must be reported as one. The honest use is triage: find the changes big
enough to be worth a closer look, not to prove one caused the other.
Two guards are built in for the most common ways this misleads:
  * both windows must have delivery on most days, else the comparison is
    between a live period and a dark one (reported as coverage);
  * new ads entering the entity are counted on each side, because an
    adset that got 20 fresh creatives is not the same adset any more.

PIVOT SELECTION
---------------
--date pins it. Otherwise the pivot is auto-detected as the day the most
new ads first delivered in that entity -- a creative refresh, which is
the change this dashboard exists to measure. The entity's own launch day
is skipped: every ad is "new" then, and there is no before to compare.

LEVELS
------
Campaigns and adsets both come from backfill_table, which carries
campaign_id/campaign_name and adset_id/adset_name on every daily row and
spans the full history. Note the edit log cannot substitute for this:
ad_edit_log holds only ADGROUP (ad-level) events, covers two of the three
accounts, and stops at 2026-09-20.

USAGE
    python analyze_before_after.py --campaign "NCP_CBO_SMCP_EX-EC_7C1E_Oct-2025"
    python analyze_before_after.py --adset "WC_CP_PL-A+_EX-EC_03/10/2025" --window 21
    python analyze_before_after.py --campaign "..." --date 2026-09-01
    python analyze_before_after.py --top 5                  # biggest campaigns
    python analyze_before_after.py --top 5 --level adset
"""
from __future__ import annotations
import argparse
import datetime as dt
import os
import sys
from collections import defaultdict

import psycopg2
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()

LEVELS = {"campaign": "campaign_name", "adset": "adset_name"}


def fetch(cur, level_col: str, names: list[str], lo: dt.date, hi: dt.date):
    """One pass: daily aggregates per entity, plus each ad's first-ever
    delivery date so a creative refresh can be located."""
    cur.execute(f"""
        SELECT {level_col} AS ent, date,
               SUM(impressions)                 AS imp,
               SUM(amount_spent_inr)            AS spend,
               SUM(conversion_value)            AS conv,
               SUM(ncp_count)                   AS ncp,
               SUM(ftewv_count)                 AS ftewv,
               SUM(outbound_clicks)             AS clicks,
               SUM(reach)                       AS reach,
               COUNT(DISTINCT ad_id)            AS ads
        FROM backfill_table
        WHERE {level_col} = ANY(%s) AND date BETWEEN %s AND %s
          AND impressions > 0          -- exclude post-2026-06 filler rows
        GROUP BY 1, 2
    """, (names, lo, hi))
    daily = {r[0]: {} for r in []}
    daily = defaultdict(dict)
    for ent, d, *vals in cur.fetchall():
        daily[ent][d] = [float(v or 0) for v in vals]

    # First DELIVERY date per ad, scoped to the entity it ran in.
    #
    # `impressions > 0` is load-bearing. From 2026-06-01 the pipeline writes
    # a filler row per ad per day with zero spend and zero impressions --
    # 277,801 of 277,955 zero-spend rows in 2026-07 carry no impressions at
    # all. Taking MIN(date) over every row therefore dates an ad from the
    # day filler started, not the day it launched: 3,393 ads appear to
    # "start" on 2026-06-01 and that single fake spike outranks every real
    # creative refresh, so auto-detection picked it for 4 of the 5 largest
    # campaigns. Requiring delivery removes it and the top real date becomes
    # 2026-10-05 with 242 ads.
    cur.execute(f"""
        SELECT ent, first_date, COUNT(*) FROM (
            SELECT {level_col} AS ent, ad_id, MIN(date) AS first_date
            FROM backfill_table
            WHERE {level_col} = ANY(%s) AND impressions > 0
            GROUP BY 1, 2
        ) t GROUP BY 1, 2
    """, (names,))
    new_ads = defaultdict(dict)
    for ent, d, n in cur.fetchall():
        new_ads[ent][d] = n
    return daily, new_ads


def agg(daily_ent: dict, lo: dt.date, hi: dt.date):
    """Sum a window. Returns totals plus how many days actually delivered."""
    tot = [0.0] * 8
    days = active = 0
    d = lo
    while d <= hi:
        days += 1
        row = daily_ent.get(d)
        if row:
            active += 1
            for i, v in enumerate(row):
                tot[i] += v
        d += dt.timedelta(days=1)
    return tot, days, active


def metrics(t: list[float]) -> dict:
    imp, spend, conv, ncp, ftewv, clicks, reach, _ads = t
    div = lambda a, b: (a / b) if b else None
    return {
        "Spend (Rs)":      spend,
        "Impressions":     imp,
        "Reach":           reach,
        "ROAS":            div(conv, spend),
        "Conv value (Rs)": conv,
        "CPM (Rs)":        div(spend * 1000, imp),
        "CTR (%)":         div(clicks * 100, imp),
        "NCP":             ncp,
        "Cost / NCP":      div(spend, ncp),
        "FTEWV":           ftewv,
        "Cost / FTEWV":    div(spend, ftewv),
    }


def fmt(v, key: str) -> str:
    if v is None: return "--"
    if key in ("ROAS", "CTR (%)"):           return f"{v:,.2f}"
    if key.startswith("Cost") or key == "CPM (Rs)": return f"{v:,.1f}"
    return f"{v:,.0f}"


def report(ent: str, pivot: dt.date, W: int, daily_ent: dict, new_ent: dict) -> None:
    b_lo, b_hi = pivot - dt.timedelta(days=W), pivot - dt.timedelta(days=1)
    a_lo, a_hi = pivot, pivot + dt.timedelta(days=W - 1)
    bt, bd, ba = agg(daily_ent, b_lo, b_hi)
    at, ad_, aa = agg(daily_ent, a_lo, a_hi)

    n_before = sum(n for d, n in new_ent.items() if b_lo <= d <= b_hi)
    n_after  = sum(n for d, n in new_ent.items() if a_lo <= d <= a_hi)

    print(f"\n{'='*78}\n{ent}")
    print(f"  pivot {pivot}   window {W}d   "
          f"before {b_lo}..{b_hi}   after {a_lo}..{a_hi}")
    print(f"  delivering days : before {ba}/{bd}   after {aa}/{ad_}")
    print(f"  new ads entering: before {n_before:>3}   after {n_after:>3}")
    if ba == 0 or aa == 0:
        print("  !! one window has no delivery at all -- not comparable")
        return
    if min(ba, aa) / max(ba, aa) < 0.5:
        print("  !! lopsided coverage -- one side ran roughly half as many days")

    mb, ma = metrics(bt), metrics(at)
    print(f"\n  {'metric':17s} {'before':>13} {'after':>13} {'change':>13}")
    print(f"  {'-'*17} {'-'*13} {'-'*13} {'-'*13}")
    for k in mb:
        x, y = mb[k], ma[k]
        if x is None or y is None or x == 0:
            chg = "--"
        else:
            pct = (y - x) / abs(x) * 100
            chg = f"{pct:+.1f}%"
        print(f"  {k:17s} {fmt(x,k):>13} {fmt(y,k):>13} {chg:>13}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--campaign", action="append", default=[])
    ap.add_argument("--adset",    action="append", default=[])
    ap.add_argument("--level", choices=list(LEVELS), default="campaign",
                    help="level used by --top")
    ap.add_argument("--top", type=int, default=0,
                    help="auto-pick the N biggest entities by last-180d spend")
    ap.add_argument("--date", help="pivot date YYYY-MM-DD (default: auto-detect)")
    ap.add_argument("--window", type=int, default=14, help="days each side")
    args = ap.parse_args()

    if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")
    level = "adset" if args.adset else args.level
    names = args.adset or args.campaign
    if not names and not args.top:
        sys.exit("Give --campaign/--adset NAME, or --top N")
    col = LEVELS[level]

    conn = psycopg2.connect(DB_URL, connect_timeout=60)
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")

    if args.top:
        cur.execute(f"""SELECT {col} FROM backfill_table
                        WHERE date > CURRENT_DATE - 181 AND {col} IS NOT NULL
                        GROUP BY 1 ORDER BY SUM(amount_spent_inr) DESC NULLS LAST
                        LIMIT %s""", (args.top,))
        names = [r[0] for r in cur.fetchall()]
        print(f"Top {len(names)} {level}s by last-180d spend")

    pivot_fixed = dt.date.fromisoformat(args.date) if args.date else None
    W = args.window
    # Pull a generous span so any auto-detected pivot has both windows.
    lo = dt.date.today() - dt.timedelta(days=730)
    hi = dt.date.today()
    daily, new_ads = fetch(cur, col, names, lo, hi)

    for ent in names:
        de, ne = daily.get(ent, {}), new_ads.get(ent, {})
        if not de:
            print(f"\n{'='*78}\n{ent}\n  no delivery rows in the last 730 days")
            continue
        if pivot_fixed:
            pivot = pivot_fixed
        else:
            launch = min(de)
            # Need a full before-window of history, so skip the launch ramp.
            cands = {d: n for d, n in ne.items()
                     if d >= launch + dt.timedelta(days=W) and d <= hi - dt.timedelta(days=W - 1)}
            if not cands:
                print(f"\n{'='*78}\n{ent}\n  no creative refresh with a full "
                      f"{W}d window either side (launched {launch})")
                continue
            pivot = max(cands, key=lambda d: (cands[d], d))
        report(ent, pivot, W, de, ne)

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
