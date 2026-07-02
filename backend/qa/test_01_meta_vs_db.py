"""
test_01_meta_vs_db.py — Meta Insights API vs Supabase for the last 7 days.

For each Meta ad account, pull yesterday-only insights (single-day window,
ad-level) and reconcile against the union of primary_table + backfill_table.
Checks:
  * ad_id set parity (>= 99% overlap)
  * impressions parity (within 0.5%)
  * spend parity        (within 0.1%)
  * reach parity        (within 2% — Meta reach fluctuates day-of)
"""
from __future__ import annotations
import os, json, requests, psycopg2
from datetime import date, timedelta
from ._common import (DB_URL, META_TOK, META_ACCOUNTS, META_API_VER,
                       scrub, within_pct, approx_equal)

WINDOW_DATE = (date.today() - timedelta(days=1)).isoformat()

def _fetch_meta_day(account_id, dt):
    """Ad-level insights for a single day; page through /next."""
    url = f"https://graph.facebook.com/{META_API_VER}/act_{account_id}/insights"
    params = {
        "level":       "ad",
        "fields":      "ad_id,impressions,spend,reach",
        "time_range":  json.dumps({"since": dt, "until": dt}),
        "filtering":   json.dumps([{"field":"impressions","operator":"GREATER_THAN","value":0}]),
        "limit":       500,
        "access_token": META_TOK,
    }
    ad_ids = set(); imp = 0; spend = 0.0; reach = 0
    while True:
        r = requests.get(url, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Meta {r.status_code}: {scrub(r.text)[:120]}")
        j = r.json()
        for row in j.get("data", []):
            aid = row.get("ad_id")
            if not aid: continue
            ad_ids.add(str(aid))
            imp   += int(row.get("impressions") or 0)
            spend += float(row.get("spend") or 0)
            reach += int(row.get("reach") or 0)
        nxt = (j.get("paging") or {}).get("next")
        if not nxt: break
        url = nxt; params = None
    return ad_ids, imp, spend, reach

def _fetch_db_day(dt):
    with psycopg2.connect(DB_URL) as c:
        c.set_session(readonly=True)
        with c.cursor() as cur:
            cur.execute("""
                WITH u AS (
                  SELECT ad_id::text AS ad_id, impressions, reach, amount_spent_inr AS spend, 1 AS pri
                    FROM public.primary_table
                   WHERE date = %s AND impressions > 0 AND ad_id IS NOT NULL
                  UNION ALL
                  SELECT ad_id::text AS ad_id, impressions, reach, amount_spent_inr AS spend, 2 AS pri
                    FROM public.backfill_table
                   WHERE date = %s AND impressions > 0 AND ad_id IS NOT NULL
                )
                SELECT DISTINCT ON (ad_id) ad_id, impressions, reach, spend
                  FROM u ORDER BY ad_id, pri
            """, (dt, dt))
            ad_ids = set(); imp = 0; spend = 0.0; reach = 0
            for aid, i, r, s in cur.fetchall():
                ad_ids.add(aid); imp += int(i or 0); reach += int(r or 0)
                spend += float(s or 0)
            return ad_ids, imp, spend, reach

def run(suite):
    if not META_TOK:
        suite.skip("Meta API check", "META_ACCESS_TOKEN not set")
        return

    all_meta_ids = set(); m_imp = m_spend = m_reach = 0
    for name, aid in META_ACCOUNTS:
        try:
            ids, i, s, r = _fetch_meta_day(aid, WINDOW_DATE)
            all_meta_ids |= ids
            m_imp += i; m_spend += s; m_reach += r
        except Exception as e:
            suite.fail(f"Meta insights {name}", scrub(str(e))[:120])
            return
    db_ids, d_imp, d_spend, d_reach = _fetch_db_day(WINDOW_DATE)

    suite.record(f"Meta ad_id set size ({WINDOW_DATE})", "PASS" if all_meta_ids else "WARN",
                 f"{len(all_meta_ids):,} ads")
    suite.record(f"DB ad_id set size ({WINDOW_DATE})", "PASS" if db_ids else "WARN",
                 f"{len(db_ids):,} ads")

    # Overlap
    overlap = all_meta_ids & db_ids
    only_meta = all_meta_ids - db_ids
    only_db   = db_ids - all_meta_ids
    pct = 100.0 * len(overlap) / max(len(all_meta_ids | db_ids), 1)
    if pct >= 99.0:
        suite.pass_("ad_id set overlap", f"{pct:.2f}% ({len(overlap):,} shared)")
    elif pct >= 95.0:
        suite.warn ("ad_id set overlap", f"{pct:.2f}% (only-Meta={len(only_meta)}, only-DB={len(only_db)})")
    else:
        suite.fail ("ad_id set overlap", f"{pct:.2f}%",
                    expected=">=99%", actual=f"{pct:.2f}%")

    # Metric parity
    for label, meta, db, tol in (("impressions", m_imp, d_imp, 0.5),
                                 ("spend",       m_spend, d_spend, 0.1),
                                 ("reach",       m_reach, d_reach, 2.0)):
        gap = abs(meta - db)
        gap_pct = 100.0 * gap / max(meta, 1)
        if within_pct(meta, db, tol):
            suite.pass_(f"{label} parity ({WINDOW_DATE})",
                        f"meta={meta:,.0f} db={db:,.0f} Δ={gap_pct:.4f}%")
        else:
            suite.fail(f"{label} parity ({WINDOW_DATE})",
                       f"meta={meta:,.0f} db={db:,.0f} Δ={gap_pct:.3f}% (>{tol}%)",
                       expected=meta, actual=db)

if __name__ == "__main__":
    from ._common import run_suite
    s = run_suite(__name__, run)
    print(s.counts)
