"""reattribute_all.py — re-run attribute_order() over every existing
row in public.shopify_ad_attribution using the new scope-first engine,
and UPDATE rows whose (ad_id, matched_tier) would change.

Preserves customer_id, customer_num_orders, contact_email, total_price,
utm_*, ordered_item, and every other column that came from Shopify —
we only touch attribution-derived fields.

USAGE
    python reattribute_all.py --dry-run           # scan + tally, no writes
    python reattribute_all.py                     # apply the UPDATE diff
    python reattribute_all.py --since 2025-01-01  # narrow window
    python reattribute_all.py --tiers "Step 2,Step 4,Step 5"   # rescope
"""
from __future__ import annotations
import os, sys, io, time, argparse, psycopg2
from collections import defaultdict
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
except Exception: pass
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

load_dotenv(override=True)
DB_URL = (os.environ.get('SUPABASE_DB_URL') or '').strip()
if not DB_URL: sys.exit('Missing SUPABASE_DB_URL')

from rebuild_attribution_orders import load_ad_universe, attribute_order

def log(*a): print(*a, flush=True)

UPDATE_SQL = """
UPDATE public.shopify_ad_attribution
   SET ad_id         = %(ad_id)s,
       ad_name       = %(ad_name)s,
       adset_id      = %(adset_id)s,
       campaign_name = %(campaign_name)s,
       has_match     = %(has_match)s,
       matched_value = %(matched_value)s,
       matched_tier  = %(matched_tier)s,
       last_synced_at = NOW()
 WHERE order_id = %(order_id)s
"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--since', help='order_created_at lower bound (YYYY-MM-DD)')
    ap.add_argument('--tiers', help='Comma-separated matched_tier values to rescope '
                                    '(e.g. "Step 2,Step 4,T2_ad_name"). Default = all rows.')
    ap.add_argument('--batch', type=int, default=500)
    args = ap.parse_args()

    log('[*] loading ad universe from primary+backfill …')
    t0 = time.time()
    maps = load_ad_universe()
    log(f'    built in {time.time()-t0:.1f}s')

    # NEVER touch Google-attributed rows — this engine only knows Meta ads.
    # Google orders are managed by attribute_google_orders.py and their
    # G1/G2/G3 tiers must be preserved.  Also skip channels the current
    # engine can't reason about.
    where = [
        "(matched_tier IS NULL OR matched_tier NOT LIKE 'G%%')",
        "(LOWER(utm_source) IS NULL OR LOWER(utm_source) NOT IN "
        "  ('google','google_ads','gads','yt_ads','youtube'))",
    ]
    params = []
    if args.since:
        where.append("order_created_at >= %s::timestamptz"); params.append(args.since)
    if args.tiers:
        tier_list = [t.strip() for t in args.tiers.split(',') if t.strip()]
        where.append("matched_tier IN %s"); params.append(tuple(tier_list))

    log('\n[*] streaming shopify_ad_attribution rows …')
    conn = psycopg2.connect(DB_URL, connect_timeout=30); conn.autocommit = False
    with conn.cursor(name='reattr_scan') as cur:
        cur.itersize = 5000
        cur.execute(f"""
            SELECT order_id, utm_content, utm_term, utm_campaign,
                   ad_id, ad_name, matched_tier, total_price,
                   order_created_at
              FROM public.shopify_ad_attribution
             WHERE {' AND '.join(where)}
        """, params)

        seen = 0
        counters_before = defaultdict(int)
        counters_after  = defaultdict(int)
        moved_counters  = defaultdict(int)  # (before_tier, after_tier) -> count
        moved_sales     = defaultdict(float)
        updates = []
        for row in cur:
            seen += 1
            (order_id, uc, ut, ua, cur_ad, cur_name, cur_tier, price, order_dt) = row

            ca = {'utm_content': uc, 'utm_term': ut, 'utm_campaign': ua}
            n_ad, n_name, n_adset, n_camp, mv, n_tier = attribute_order(ca, maps, order_created_at=order_dt)

            # Empty tuple → Step 5 (no match)
            if not n_tier: n_tier = 'Step 5'
            counters_before[cur_tier or 'Step 5'] += 1
            counters_after[n_tier] += 1

            n_ad_val   = n_ad   or None
            n_name_val = n_name or None
            n_adset_val= n_adset or None
            n_camp_val = n_camp or None
            has_match  = bool(n_ad_val)

            changed = (
                (cur_ad or None) != n_ad_val or
                (cur_tier or None) != (n_tier if n_tier != 'Step 5' else None)
            )
            if changed:
                moved_counters[(cur_tier or 'Step 5', n_tier)] += 1
                moved_sales[(cur_tier or 'Step 5', n_tier)] += float(price or 0)
                updates.append({
                    'order_id':      order_id,
                    'ad_id':         n_ad_val,
                    'ad_name':       n_name_val,
                    'adset_id':      n_adset_val,
                    'campaign_name': n_camp_val,
                    'has_match':     has_match,
                    'matched_value': mv,
                    'matched_tier':  n_tier if n_tier != 'Step 5' else None,
                })

            if seen % 50_000 == 0:
                log(f'    scanned {seen:,} · updates queued {len(updates):,}')

    log(f'\n[scanned {seen:,} rows]')
    log(f'  updates queued: {len(updates):,}')

    log('\n─── before / after tier distribution ───')
    all_tiers = sorted(set(counters_before) | set(counters_after))
    log(f"  {'tier':<28} {'before':>10} {'after':>10}   Δ")
    for t in all_tiers:
        b, a = counters_before.get(t, 0), counters_after.get(t, 0)
        log(f"  {t:<28} {b:>10,} {a:>10,}   {a-b:+,}")

    log('\n─── top moves (before → after) ───')
    for (bef, aft), n in sorted(moved_counters.items(), key=lambda x: -x[1])[:20]:
        log(f"  {bef:>20} → {aft:<20}  {n:>7,} orders   Rs{moved_sales[(bef,aft)]:>12,.0f}")

    if args.dry_run:
        log('\n[dry-run] not applying updates. Re-run without --dry-run to persist.')
        conn.close(); return

    log(f'\n[*] applying {len(updates):,} UPDATEs in batches of {args.batch} …')
    t1 = time.time()
    with conn.cursor() as w:
        for i in range(0, len(updates), args.batch):
            execute_batch(w, UPDATE_SQL, updates[i:i+args.batch], page_size=500)
            if i % (args.batch * 20) == 0:
                log(f'    applied {i+args.batch:>8,} / {len(updates):,}')
    conn.commit()
    log(f'[ok] applied {len(updates):,} UPDATEs in {(time.time()-t1)/60:.1f} min')
    conn.close()

if __name__ == '__main__':
    main()
