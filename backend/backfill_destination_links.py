"""
backfill_destination_links.py - tier-2 ad_link fallback via the ad-preview HTML.

The creative-side extractor (_extract_ad_link) handles ~95% of natively-created
ads but returns empty for boosted IG posts (their destination lives on the page-
post layer that our ads token cannot read).

The preview API server-renders the ad exactly as it appears on each placement,
and the rendered JSON contains the resolved `link_url`. We parse that and unwrap
Meta's l.facebook.com redirector to get the final destination URL.

Targets ad_ids where ad_link IS NULL/'' in BOTH primary_table and backfill_table.
Writes to whichever table contains the row.

Usage:
    python backfill_destination_links.py                # full pass
    python backfill_destination_links.py --limit 50     # smoke test
    python backfill_destination_links.py --ad <ad_id>   # single ad
"""
import os, sys, io, time, argparse
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

import psycopg2, psycopg2.extras
from dotenv import load_dotenv
from primary_sync import _extract_link_from_preview

load_dotenv()
DB = os.environ["SUPABASE_DB_URL"].strip()


def fetch_empty_ad_ids(cur, table: str) -> list:
    cur.execute(f"""
        SELECT DISTINCT ad_id FROM {table}
        WHERE ad_id IS NOT NULL AND ad_id <> ''
          AND (ad_link IS NULL OR ad_link = '')
    """)
    return [r[0] for r in cur.fetchall()]


def update_links(cur, table: str, ad_id_to_link: dict):
    if not ad_id_to_link:
        return
    payload = [(link, ad_id) for ad_id, link in ad_id_to_link.items()]
    psycopg2.extras.execute_batch(
        cur,
        f"UPDATE {table} SET ad_link = %s WHERE ad_id = %s AND (ad_link IS NULL OR ad_link = '')",
        payload, page_size=500,
    )


def coverage(cur, table: str):
    cur.execute(f"""
        SELECT COUNT(DISTINCT ad_id) FILTER (WHERE ad_link IS NOT NULL AND ad_link <> ''),
               COUNT(DISTINCT ad_id)
        FROM {table} WHERE ad_id IS NOT NULL AND ad_id <> ''
    """)
    return cur.fetchone()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=0, help="Cap on ads to process (0 = all)")
    p.add_argument("--ad", help="Single ad_id (overrides --limit and the DB scan)")
    args = p.parse_args()

    conn = psycopg2.connect(DB, connect_timeout=30); conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")

    if args.ad:
        targets_primary, targets_backfill = [args.ad], [args.ad]
        print(f"[mode] single ad: {args.ad}")
    else:
        print("[1] Loading ad_ids needing destination_link ...")
        targets_primary  = fetch_empty_ad_ids(cur, "primary_table")
        targets_backfill = fetch_empty_ad_ids(cur, "backfill_table")
        union = list({*targets_primary, *targets_backfill})
        print(f"  primary_table  : {len(targets_primary):,} empty")
        print(f"  backfill_table : {len(targets_backfill):,} empty")
        print(f"  union          : {len(union):,} distinct ads to probe")
        if args.limit and len(union) > args.limit:
            union = union[:args.limit]
            print(f"  (capped to --limit {args.limit})")
        # We'll only call the preview API once per ad_id and write to whichever
        # table(s) have a row for it.
        targets_primary  = [a for a in union if a in set(targets_primary)]
        targets_backfill = [a for a in union if a in set(targets_backfill)]
        # Iterate the union
        targets_iter = union

    if not args.ad and not targets_iter:
        print("Nothing to do."); cur.close(); conn.close(); return

    iter_list = [args.ad] if args.ad else targets_iter
    print(f"\n[2] Calling /<ad>/previews for {len(iter_list):,} ads ...")
    primary_set  = set(targets_primary)  if not args.ad else {args.ad}
    backfill_set = set(targets_backfill) if not args.ad else {args.ad}

    resolved = {}
    t0 = time.time()
    for i, ad_id in enumerate(iter_list, 1):
        link = _extract_link_from_preview(ad_id)
        if link:
            resolved[ad_id] = link
        if i % 25 == 0 or i == len(iter_list):
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(iter_list) - i) / rate if rate > 0 else 0
            print(f"  [{i:>5}/{len(iter_list):>5}] resolved={len(resolved):>5}  "
                  f"{rate:5.1f} ads/s  ETA {eta/60:5.1f}m")
        # Flush every 100 to disk so a crash doesn't lose work
        if i % 100 == 0 and resolved:
            p_batch = {a:l for a,l in resolved.items() if a in primary_set}
            b_batch = {a:l for a,l in resolved.items() if a in backfill_set}
            update_links(cur, "primary_table",  p_batch)
            update_links(cur, "backfill_table", b_batch)
            conn.commit()

    print(f"\n[3] Resolved {len(resolved):,}/{len(iter_list):,} via preview "
          f"({len(resolved)/max(len(iter_list),1)*100:.1f}%) in {time.time()-t0:.0f}s")

    if resolved:
        print("\n[4] Final UPDATE ...")
        p_batch = {a:l for a,l in resolved.items() if a in primary_set}
        b_batch = {a:l for a,l in resolved.items() if a in backfill_set}
        update_links(cur, "primary_table",  p_batch)
        update_links(cur, "backfill_table", b_batch)
        conn.commit()
        print(f"  primary_table  updated: {len(p_batch):,}")
        print(f"  backfill_table updated: {len(b_batch):,}")

    print("\n[after] Coverage:")
    for t in ("primary_table", "backfill_table"):
        filled, total = coverage(cur, t)
        print(f"  {t:<16}: {filled:,}/{total:,} ({filled/total*100:.1f}%)")

    cur.close(); conn.close()
    print("\n[done] Re-run refresh_ae_table.py to propagate into ae_raw_view.")


if __name__ == "__main__":
    main()
