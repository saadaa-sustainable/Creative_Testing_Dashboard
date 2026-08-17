"""
recover_t3_global_substr.py
───────────────────────────────────────────────────────────────────
Recovery pass for Meta-related orders stuck at base T3 / unmatched
where utm_content is a name-like string that exists in primary_table
or backfill_table but in a DIFFERENT adset than utm_term points to.

Strategy
  1. Build the ad universe (rich names + ad_created_date) — same
     source as rebuild_attribution_orders.py.
  2. Pull distinct utm_content strings from candidate Meta rows.
  3. For each distinct string:
       - Find ads whose ad_name contains the string OR the string
         contains the ad_name (length safeguard ≥ 10 chars).
       - If exactly one ad matches  → ad_id pinned.
       - If multiple ads match      → pick one by date proximity
                                       (if order_date available).
       - If zero matches            → skip.
  4. Bulk UPDATE the rows to the new tier T2_ad_name (single
     hit) or T2_ad_name (multiple hits resolved by date).
  5. T0 / T1 / T2_exact / T2_fuzzy are NEVER touched (source of truth).

USAGE
  python recover_t3_global_substr.py --dry-run
  python recover_t3_global_substr.py
  python recover_t3_global_substr.py --min-len 12   (default 10)
"""
import os, sys, time, argparse, re
import psycopg2
from psycopg2.extras import execute_batch
from collections import defaultdict
from datetime import date, datetime
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"]

def log(*a): print(*a, flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-len", type=int, default=10,
        help="minimum length of utm_content string to consider for global substring match")
    args = ap.parse_args()

    # ── Pull all distinct ad (id, name, ad_created_date) once ───────────
    log("[ok] loading ad universe…")
    conn = psycopg2.connect(DB_URL); conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600000'")
    cur.execute("""
      SELECT ad_id, ad_name, adset_id, campaign_name, ad_created_date
      FROM   primary_table
      WHERE  ad_id IS NOT NULL AND ad_id <> '' AND ad_name IS NOT NULL AND ad_name <> ''
      UNION ALL
      SELECT ad_id, ad_name, adset_id, campaign_name, ad_created_date
      FROM   backfill_table
      WHERE  ad_id IS NOT NULL AND ad_id <> '' AND ad_name IS NOT NULL AND ad_name <> ''
    """)
    # ad_id -> {ad_name, adset_id, campaign_name, ad_created_date}
    by_ad = {}
    name_to_ads = defaultdict(set)   # name lowercased -> set(ad_id)
    for ad_id, ad_name, adset_id, camp_name, created in cur.fetchall():
        nl = ad_name.strip().lower()
        if not nl: continue
        if ad_id not in by_ad:
            by_ad[ad_id] = {
                "ad_id": ad_id, "ad_name": ad_name.strip(),
                "names": set(), "adset_id": adset_id or "",
                "campaign_name": camp_name or "",
                "ad_created_date": created,
            }
        by_ad[ad_id]["names"].add(ad_name.strip())
        name_to_ads[nl].add(ad_id)
    all_names = list(name_to_ads.keys())
    log(f"[ok] universe: {len(by_ad):,} ads, {len(all_names):,} distinct lower-case names")

    # ── Pull distinct utm_content strings from candidates ───────────────
    log("[ok] pulling candidate utm_content values…")
    cur.execute(r"""
      SELECT utm_content, COUNT(*)
      FROM   shopify_ad_attribution
      WHERE  (matched_tier IN ('T3_adset_id','T4_campaign_name') OR matched_tier IS NULL)
        AND  utm_content IS NOT NULL AND TRIM(utm_content) <> ''
        AND  utm_content !~ '^[0-9]{15,}$'
        AND  utm_content !~ '\{\{|\}\}'
        AND (LOWER(TRIM(utm_source)) LIKE 'meta%'
             OR LOWER(TRIM(utm_source)) IN ('fb','facebook','ig','igshopping','instagram'))
      GROUP BY utm_content
      ORDER BY COUNT(*) DESC
    """)
    candidates = cur.fetchall()
    log(f"[ok] {len(candidates):,} distinct utm_content values  "
        f"({sum(n for _,n in candidates):,} total orders)")

    # ── For each distinct utm_content, find global substring matches ────
    # Build resolution dict: utm_content -> {ad_id, ad_name, adset_id, camp_name, hit_count}
    resolution = {}
    n_seen = 0
    t0 = time.time()
    for utm_content, n in candidates:
        n_seen += 1
        s = utm_content.strip()
        if len(s) < args.min_len: continue
        sl = s.lower()
        # Find ads whose ad_name is a substring of utm OR utm is a substring of ad_name
        # (both checks; min_len safeguard avoids tiny matches)
        hits = []
        for nl, ad_ids in name_to_ads.items():
            if len(nl) < 6: continue
            if (nl in sl) or (sl in nl):
                for aid in ad_ids:
                    hits.append(by_ad[aid])
        # Dedup by ad_id
        seen_ids = set(); uniq = []
        for h in hits:
            if h["ad_id"] in seen_ids: continue
            seen_ids.add(h["ad_id"]); uniq.append(h)
        if not uniq: continue
        if len(uniq) == 1:
            resolution[utm_content] = (uniq[0], "T2_ad_name", 1)
        else:
            # Pick by exact name match preference, then longest matching substring
            exact_hits = [h for h in uniq if any(nm.strip().lower() == sl for nm in h["names"])]
            if len(exact_hits) == 1:
                resolution[utm_content] = (exact_hits[0], "T2_ad_name", 1)
            else:
                # Multiple ambiguous candidates — store all, resolve per-row by date
                resolution[utm_content] = (None, "T2_ad_name", uniq)
        if n_seen % 500 == 0:
            log(f"  [scan] {n_seen:,}/{len(candidates):,}  matched_keys={len(resolution):,}  ({(time.time()-t0):.1f}s)")
    log(f"[ok] scan complete in {(time.time()-t0):.1f}s  "
        f"{len(resolution):,}/{len(candidates):,} utm_content values have a global match")

    # Estimate row impact
    impact_rows = 0
    for uc, n in candidates:
        if uc in resolution: impact_rows += n
    log(f"[ok] orders that will be re-attributed: {impact_rows:,}")

    if args.dry_run:
        # Show top resolutions
        log("\n[top resolutions, by order count]")
        cur2 = conn.cursor()
        for uc, n in candidates[:25]:
            if uc not in resolution: continue
            r = resolution[uc]
            if isinstance(r[2], int):
                m = r[0]
                log(f"  {n:>5}  '{uc[:55]}'  ->  ad {m['ad_id']}  ad_name={m['ad_name'][:50]}")
            else:
                log(f"  {n:>5}  '{uc[:55]}'  ->  {len(r[2])} candidates (date pick)")
        return

    # ── Apply UPDATEs in batches ────────────────────────────────────────
    log("[ok] applying UPDATEs…")
    wconn = psycopg2.connect(DB_URL); wconn.autocommit = False
    wcur  = wconn.cursor()
    wcur.execute("SET statement_timeout = '600000'")
    UPDATE_SQL = """
      UPDATE shopify_ad_attribution
         SET ad_id=%s, ad_name=%s, adset_id=%s, campaign_name=%s,
             matched_value=%s, matched_tier=%s, has_match=TRUE
       WHERE utm_content=%s
         AND (matched_tier IN ('T3_adset_id','T4_campaign_name') OR matched_tier IS NULL)
         AND (LOWER(TRIM(utm_source)) LIKE 'meta%%'
              OR LOWER(TRIM(utm_source)) IN ('fb','facebook','ig','igshopping','instagram'))
    """
    UPDATE_DATE_SQL = """
      UPDATE shopify_ad_attribution
         SET ad_id=%s, ad_name=%s, adset_id=%s, campaign_name=%s,
             matched_value=%s, matched_tier=%s, has_match=TRUE
       WHERE order_id=%s
    """

    # Bucket simple single-hit resolutions for bulk UPDATE by utm_content
    simple = []
    multi_keys = []   # for date-resolved, do per-order
    for uc, (m, tier, payload) in resolution.items():
        if isinstance(payload, int):   # single hit
            simple.append((m["ad_id"], m["ad_name"], m["adset_id"], m["campaign_name"], uc, tier, uc))
        else:
            multi_keys.append((uc, payload))
    log(f"  - {len(simple):,} simple (single-hit) keys → bulk UPDATE by utm_content")
    log(f"  - {len(multi_keys):,} multi-hit keys → resolve per-order by date")

    if simple:
        execute_batch(wcur, UPDATE_SQL, simple, page_size=200)
        wconn.commit()
        log(f"  [applied] {len(simple):,} bulk updates")

    # For multi-hit, fetch each candidate order's date and pick by proximity
    if multi_keys:
        per_order = []
        for uc, ads in multi_keys:
            cur.execute("""
                SELECT order_id, order_created_at FROM shopify_ad_attribution
                WHERE utm_content=%s
                  AND (matched_tier IN ('T3_adset_id','T4_campaign_name') OR matched_tier IS NULL)
                  AND (LOWER(TRIM(utm_source)) LIKE 'meta%%'
                       OR LOWER(TRIM(utm_source)) IN ('fb','facebook','ig','igshopping','instagram'))
            """, (uc,))
            for order_id, order_ts in cur.fetchall():
                od = order_ts.date() if order_ts and hasattr(order_ts,'date') else None
                valid = [a for a in ads if a.get("ad_created_date")]
                if valid and od:
                    valid.sort(key=lambda a: (
                        0 if a["ad_created_date"] <= od else 1,
                        abs((od - a["ad_created_date"]).days)
                    ))
                    best = valid[0]
                else:
                    best = ads[0]
                per_order.append((best["ad_id"], best["ad_name"], best["adset_id"],
                                  best["campaign_name"], uc, "T2_ad_name", order_id))
        if per_order:
            execute_batch(wcur, UPDATE_DATE_SQL, per_order, page_size=500)
            wconn.commit()
            log(f"  [applied] {len(per_order):,} date-resolved updates")

    log("[done]")
    cur.close(); conn.close(); wcur.close(); wconn.close()

if __name__ == "__main__":
    main()
