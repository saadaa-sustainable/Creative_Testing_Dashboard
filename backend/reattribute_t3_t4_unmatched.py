"""
reattribute_t3_t4_unmatched.py — runs the upgraded attribute_order() logic
against existing T3 / T4_* / unmatched rows in shopify_ad_attribution and
UPDATES rows whose match improves.

T0 / T1 / T2 rows are NEVER touched (they remain source of truth).

The script:
  1. Loads the new ad universe (with rich per-adset / per-campaign maps)
  2. Streams the rows to re-attribute, in batches
  3. Calls the upgraded attribute_order() reusing utm_* fields
  4. Writes back: ad_id, ad_name, adset_id, campaign_name, matched_value,
                  matched_tier, has_match
  5. Reports before/after distribution

USAGE
  python reattribute_t3_t4_unmatched.py                        # all candidates
  python reattribute_t3_t4_unmatched.py --since 2026-06-01     # date window
  python reattribute_t3_t4_unmatched.py --dry-run              # log only
  python reattribute_t3_t4_unmatched.py --limit 1000           # sample
"""
import os, sys, time, argparse
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"]

# Reuse helpers from the rebuild script
from rebuild_attribution_orders import (
    load_ad_universe, attribute_order, _parse_iso, log
)

CANDIDATE_TIERS = (
    'T3', 'T3.2',           # adset base + date-only adset attribution
    'T4', 'T4.2',           # campaign base + date-only campaign attribution
)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None, help="only re-attribute orders on/after this date (YYYY-MM-DD)")
    ap.add_argument("--until", default=None, help="only re-attribute orders on/before this date (YYYY-MM-DD)")
    ap.add_argument("--dry-run", action="store_true", help="log only, no DB writes")
    ap.add_argument("--limit",   type=int, default=0, help="cap rows processed (0=all)")
    ap.add_argument("--batch",   type=int, default=2000, help="DB write batch size")
    args = ap.parse_args()

    log("[ok] loading ad universe (rich maps for adset / campaign scope)…")
    maps = load_ad_universe()

    where = [
        "(matched_tier IN ('T2_ad_name','T3','T3.1','T3.2','T3.3',"
        " 'T4','T4.1','T4.2','T4.3') "
        " OR matched_tier IS NULL)"
    ]
    params = []
    if args.since: where.append("order_created_at >= %s"); params.append(args.since)
    if args.until: where.append("order_created_at <= %s"); params.append(args.until + " 23:59:59")
    where_sql = "WHERE " + " AND ".join(where)
    limit_sql = f"LIMIT {args.limit}" if args.limit else ""

    select_sql = f"""
        SELECT order_id, order_created_at,
               utm_campaign, utm_content, utm_medium, utm_source, utm_term,
               matched_tier
        FROM   shopify_ad_attribution
        {where_sql}
        ORDER  BY order_created_at DESC
        {limit_sql}
    """

    log(f"[ok] streaming candidate rows…\n    where: {where_sql}\n    limit: {args.limit or 'none'}")
    conn = psycopg2.connect(DB_URL); conn.set_session(autocommit=False)
    cur  = conn.cursor(name="reattr_cursor"); cur.itersize = 5000
    cur.execute(select_sql, params)

    rows_seen = 0
    transitions = {}    # (old_tier, new_tier) -> count
    upgrades_t3_1 = 0   # adset + signal pinned one ad
    upgrades_t4_1 = 0   # campaign + signal pinned one ad
    no_change = 0
    update_buf = []
    t0 = time.time()

    UPDATE_SQL = """
      UPDATE shopify_ad_attribution
         SET ad_id=%s, ad_name=%s, adset_id=%s, campaign_name=%s,
             matched_value=%s, matched_tier=%s, has_match=%s
       WHERE order_id=%s
    """
    write_conn = psycopg2.connect(DB_URL)

    def flush():
        nonlocal update_buf
        if not update_buf: return
        with write_conn.cursor() as wcur:
            execute_batch(wcur, UPDATE_SQL, update_buf, page_size=500)
        write_conn.commit()
        update_buf = []

    while True:
        chunk = cur.fetchmany(5000)
        if not chunk: break
        for r in chunk:
            (order_id, created_at, utm_camp, utm_content, utm_med,
             utm_source, utm_term, old_tier) = r
            old_tier_s = old_tier or "unmatched"
            ca = {
                "utm_campaign": utm_camp or "",
                "utm_content":  utm_content or "",
                "utm_medium":   utm_med or "",
                "utm_source":   utm_source or "",
                "utm_term":     utm_term or "",
            }
            ad_id, ad_name, adset_id, camp_name, matched_value, new_tier = \
                attribute_order(ca, maps, order_created_at=created_at)
            new_tier_s = new_tier or "unmatched"

            rows_seen += 1
            key = (old_tier_s, new_tier_s)
            transitions[key] = transitions.get(key, 0) + 1
            if new_tier_s == old_tier_s and (ad_id or "") == "" :
                no_change += 1
                continue
            if new_tier in ("T3.1", "T3.3"): upgrades_t3_1 += 1
            if new_tier in ("T4.1", "T4.3"): upgrades_t4_1 += 1

            if not args.dry_run:
                update_buf.append((
                    ad_id or None, ad_name or None, adset_id or None, camp_name or None,
                    matched_value or None, new_tier or None, bool(new_tier),
                    order_id
                ))
                if len(update_buf) >= args.batch:
                    flush()

            if rows_seen % 5000 == 0:
                elapsed = time.time() - t0
                log(f"  [progress] processed {rows_seen:,}  ({rows_seen/elapsed:.0f}/s)  "
                    f"→ T3.x {upgrades_t3_1:,}  T4.x {upgrades_t4_1:,}")

    flush()

    elapsed = time.time() - t0
    log(f"\n[done] processed {rows_seen:,} rows in {elapsed/60:.1f} min")
    log(f"       upgrades to T3.1 / T3.3 (adset + signal):    {upgrades_t3_1:,}")
    log(f"       upgrades to T4.1 / T4.3 (campaign + signal): {upgrades_t4_1:,}")
    log(f"\n[transitions] (old -> new, sorted by count)")
    for (a, b), n in sorted(transitions.items(), key=lambda kv: -kv[1])[:30]:
        log(f"  {n:>8,}   {a:<28} -> {b}")

    cur.close(); conn.close(); write_conn.close()

if __name__ == "__main__":
    main()
