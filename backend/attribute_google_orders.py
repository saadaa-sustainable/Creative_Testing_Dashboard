"""
attribute_google_orders.py — 5-step UTM matching for Google-source orders,
mirroring the Meta engine (rebuild_attribution_orders.py) but keyed off
google_ads_primary.

Tier ladder (mirrors Meta's T1/T2/T3/T4/Step 5):
    G1_ad_id         utm_content matches an ad_id in google_ads_primary
    G2_ad_name       utm_content matches an ad_name (exact, unique in scope)
    G3_adgroup       utm_term / utm_content resolves to an ad_group, and we
                     can narrow to a unique ad by name / delivery share
    G4_campaign      utm_campaign resolves to a campaign; ad-level narrow
                     if possible
    G5_multi         multi-attribute fallback (utm_source google + utm_campaign
                     token + utm_content hint) → best-guess ad

Source filter: LOWER(utm_source) IN ('google','google_ads','gads','yt_ads')
               OR LOWER(utm_medium) IN ('cpc','ppc')

USAGE
    python attribute_google_orders.py --dry-run          # tally + diff, no writes
    python attribute_google_orders.py                    # apply UPDATEs
    python attribute_google_orders.py --since 2025-01-01 # scope to orders on/after
    python attribute_google_orders.py --only-untagged    # skip rows already G*
"""
from __future__ import annotations
import os, sys, io, re, time, argparse, psycopg2
from collections import defaultdict
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='backslashreplace')

load_dotenv(override=True)
DB_URL = (os.environ.get('SUPABASE_DB_URL') or '').strip()
if not DB_URL: sys.exit('Missing SUPABASE_DB_URL')

GOOGLE_SOURCES = ('google', 'google_ads', 'gads', 'yt_ads', 'youtube')
GOOGLE_MEDIA   = ('cpc', 'ppc')

def log(*a): print(*a, flush=True)

def _norm(s):
    """Lowercase + strip + collapse Meta URL macros ({} braces)."""
    if not s: return ''
    return re.sub(r'[{}]', '', str(s).strip()).lower()

def _digits_only(s):
    m = re.fullmatch(r'\d+', str(s or '').strip())
    return s.strip() if m else ''

# ─────────────────────────────────────────────────────────────
# Build lookup maps from google_ads_primary
# ─────────────────────────────────────────────────────────────
def build_google_maps(conn):
    """Return maps:
       by_ad_id     : ad_id (str)  → dict(ad_id, ad_name, ad_group_id, ad_group_name, campaign_id, campaign_name)
       by_ad_name   : lower(ad_name) → [ad meta ...]
       by_adgroup_id: adgroup_id (str) → [ad_id ...]
       by_adgroup_name: lower(name) → adgroup_id
       by_campaign_id: campaign_id (str) → [ad_id ...]
       by_campaign_name: lower(name) → campaign_id
    """
    by_ad_id, by_ad_name = {}, defaultdict(list)
    by_adgroup_id, by_adgroup_name = defaultdict(list), {}
    by_campaign_id, by_campaign_name = defaultdict(list), {}

    with conn.cursor() as cur:
        # DISTINCT ON (ad_id) — take the latest row per ad for identity;
        # aggregates already live in the summary table so we don't need
        # the whole daily series here.
        cur.execute("""
            SELECT DISTINCT ON (ad_id)
                   ad_id::text, ad_name,
                   ad_group_id::text, ad_group_name,
                   campaign_id::text, campaign_name,
                   ad_status, ad_type
              FROM public.google_ads_primary
             WHERE ad_id IS NOT NULL
             ORDER BY ad_id, date DESC
        """)
        for r in cur.fetchall():
            ad_id, ad_name, agid, ag_name, cid, cname, ad_status, ad_type = r
            meta = {'ad_id': ad_id, 'ad_name': ad_name or '',
                    'adgroup_id': agid, 'adgroup_name': ag_name or '',
                    'campaign_id': cid, 'campaign_name': cname or '',
                    'ad_status': ad_status, 'ad_type': ad_type}
            by_ad_id[ad_id] = meta
            if ad_name: by_ad_name[_norm(ad_name)].append(meta)
            if agid:
                by_adgroup_id[agid].append(ad_id)
                if ag_name: by_adgroup_name[_norm(ag_name)] = agid
            if cid:
                by_campaign_id[cid].append(ad_id)
                if cname: by_campaign_name[_norm(cname)] = cid

    return dict(by_ad_id=by_ad_id, by_ad_name=by_ad_name,
                by_adgroup_id=by_adgroup_id, by_adgroup_name=by_adgroup_name,
                by_campaign_id=by_campaign_id, by_campaign_name=by_campaign_name)

# ─────────────────────────────────────────────────────────────
# 5-step matcher
# ─────────────────────────────────────────────────────────────
def match_google(utm_content, utm_term, utm_campaign, maps):
    """Return (ad_id, ad_name, adgroup_id, adgroup_name, campaign_id,
              campaign_name, matched_value, matched_tier) or None-tuple."""
    uc = (utm_content  or '').strip()
    ut = (utm_term     or '').strip()
    ua = (utm_campaign or '').strip()
    ucn, utn, uan = _norm(uc), _norm(ut), _norm(ua)

    def _ret(m, matched_value, tier):
        return (m['ad_id'], m['ad_name'], m['adgroup_id'], m['adgroup_name'],
                m['campaign_id'], m['campaign_name'], matched_value, tier)

    # ── G1: utm_content is a numeric ad_id in google_ads_primary ──
    candidates_g1 = [_digits_only(uc), _digits_only(ut)]
    for cand in candidates_g1:
        if cand and cand in maps['by_ad_id']:
            return _ret(maps['by_ad_id'][cand], cand, 'G1_ad_id')

    # ── G2: utm_content == ad_name exactly (unique) ──
    if ucn and ucn in maps['by_ad_name']:
        hits = maps['by_ad_name'][ucn]
        if len(hits) == 1:
            return _ret(hits[0], uc, 'G2_ad_name')
        # If multiple, narrow by campaign / adgroup match
        camp_ok = [h for h in hits if h['campaign_id'] == _digits_only(ua)
                   or _norm(h['campaign_name']) == uan]
        if len(camp_ok) == 1:
            return _ret(camp_ok[0], uc, 'G2_ad_name_camp')

    # Key rule (per Google's default URL template {creative}=ad_id):
    # ad-level attribution ONLY when utm_content confidently identifies
    # the ad — either as a numeric ad_id we know (G1 above) or an exact
    # ad_name match (G2 above). Otherwise resolve to group/campaign scope
    # with ad_id NULL — never force-attribute to a "singleton" ad just
    # because it's the only one we have in a campaign.
    uc_is_identifying = bool(uc.strip()) and (
        _digits_only(uc) in maps['by_ad_id']    # known ad_id
        or _norm(uc) in maps['by_ad_name']      # known ad_name
    )

    # ── G3: utm_term / utm_content resolves to an ad_group ──
    adgroup_id = None
    for cand in (_digits_only(ut), _digits_only(uc)):
        if cand and cand in maps['by_adgroup_id']:
            adgroup_id = cand; break
    if not adgroup_id and utn and utn in maps['by_adgroup_name']:
        adgroup_id = maps['by_adgroup_name'][utn]
    if adgroup_id:
        ads = maps['by_adgroup_id'][adgroup_id]
        # Try ad_name substring narrow ONLY when utm_content actually
        # names an ad.
        if uc_is_identifying and ucn:
            nm_hits = [aid for aid in ads
                       if ucn in _norm(maps['by_ad_id'][aid]['ad_name'])]
            if len(nm_hits) == 1:
                return _ret(maps['by_ad_id'][nm_hits[0]], uc, 'G3_adgroup_name')
        # Otherwise resolve to adgroup scope only — leave ad_id NULL so
        # the data honestly reflects "we know the adgroup, not the ad."
        first = maps['by_ad_id'][ads[0]]
        return (None, None, adgroup_id, first['adgroup_name'],
                first['campaign_id'], first['campaign_name'],
                adgroup_id, 'G3_adgroup_only')

    # ── G4: utm_campaign resolves to a campaign ──
    campaign_id = _digits_only(ua)
    if not campaign_id and uan in maps['by_campaign_name']:
        campaign_id = maps['by_campaign_name'][uan]
    if campaign_id and campaign_id in maps['by_campaign_id']:
        ads = maps['by_campaign_id'][campaign_id]
        if uc_is_identifying and ucn:
            nm_hits = [aid for aid in ads
                       if ucn in _norm(maps['by_ad_id'][aid]['ad_name'])]
            if len(nm_hits) == 1:
                return _ret(maps['by_ad_id'][nm_hits[0]], uc, 'G4_campaign_name')
        # Campaign scope only — ad_id NULL.
        first = maps['by_ad_id'][ads[0]]
        return (None, None, None, None,
                campaign_id, first['campaign_name'],
                campaign_id, 'G4_campaign_only')

    # ── G5: multi-attribute — nothing pinned but at least tag as Google ──
    return (None, None, None, None, None, None,
            uc or ua or ut or '', 'G5_google_unpinned')

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
UPDATE_SQL = """
UPDATE public.shopify_ad_attribution
   SET ad_id         = %(ad_id)s,
       ad_name       = %(ad_name)s,
       campaign_name = %(campaign_name)s,
       adset_id      = %(adgroup_id)s,          -- reuse adset_id column for adgroup_id
       has_match     = %(has_match)s,
       matched_value = %(matched_value)s,
       matched_tier  = %(matched_tier)s,
       last_synced_at = NOW()
 WHERE order_id = %(order_id)s
"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--since', help='YYYY-MM-DD lower bound on order_created_at')
    ap.add_argument('--only-untagged', action='store_true',
                    help='Skip orders that already have a G* tier')
    ap.add_argument('--batch', type=int, default=500)
    args = ap.parse_args()

    conn = psycopg2.connect(DB_URL, connect_timeout=30)
    log(f"[*] building lookup maps from google_ads_primary …")
    t0 = time.time()
    maps = build_google_maps(conn)
    log(f"    ads:       {len(maps['by_ad_id']):,}")
    log(f"    ad_names:  {len(maps['by_ad_name']):,}")
    log(f"    adgroups:  {len(maps['by_adgroup_id']):,}")
    log(f"    campaigns: {len(maps['by_campaign_id']):,}")
    log(f"    built in {time.time()-t0:.1f}s")

    where_parts = [
        "(LOWER(utm_source) IN %s OR LOWER(utm_medium) IN %s)",
    ]
    params = [tuple(GOOGLE_SOURCES), tuple(GOOGLE_MEDIA)]
    if args.since:
        where_parts.append("order_created_at >= %s::timestamptz")
        params.append(args.since)
    if args.only_untagged:
        where_parts.append("(matched_tier IS NULL OR matched_tier NOT LIKE 'G%%')")
    where = ' AND '.join(where_parts)

    log(f"\n[*] scanning orders …")
    with conn.cursor(name='google_scan') as cur:
        cur.itersize = 5000
        cur.execute(f"""
            SELECT order_id, utm_source, utm_medium, utm_campaign,
                   utm_content, utm_term, ad_id, matched_tier, total_price
              FROM public.shopify_ad_attribution
             WHERE {where}
        """, params)

        counters = defaultdict(int)
        sums     = defaultdict(float)
        updates  = []
        seen     = 0
        for row in cur:
            seen += 1
            order_id, src, med, ua, uc, ut, cur_ad_id, cur_tier, price = row
            res = match_google(uc, ut, ua, maps)
            (n_ad, n_name, n_agid, n_agname, n_cid, n_cname,
             mv, tier) = res
            counters[tier] += 1
            sums[tier] += float(price or 0)

            # Skip UPDATE if nothing would change
            if cur_ad_id == n_ad and cur_tier == tier:
                counters['(unchanged)'] += 1
                continue
            has_match = tier in ('G1_ad_id','G2_ad_name','G2_ad_name_camp',
                                  'G3_adgroup_singleton','G3_adgroup_name',
                                  'G4_campaign_singleton','G4_campaign_name')
            updates.append({
                'order_id': order_id,
                'ad_id':    n_ad,
                'ad_name':  n_name,
                'campaign_name': n_cname,
                'adgroup_id': n_agid,
                'has_match': has_match,
                'matched_value': mv,
                'matched_tier': tier,
            })

    log(f"\n[scanned {seen:,} Google-side orders]\n")
    log("  Tier breakdown (would-be state):")
    for t in ['G1_ad_id','G2_ad_name','G2_ad_name_camp',
              'G3_adgroup_name','G3_adgroup_only',
              'G4_campaign_name','G4_campaign_only',
              'G5_google_unpinned','(unchanged)']:
        if counters.get(t):
            log(f"    {t:<32} orders={counters[t]:>7,}  sales=Rs{sums[t]:>12,.0f}")

    log(f"\n[*] {len(updates):,} rows would move ({'DRY-RUN' if args.dry_run else 'applying'})")
    if args.dry_run:
        conn.close(); return

    log(f"\n[*] applying UPDATEs in batches of {args.batch} …")
    t1 = time.time()
    with conn.cursor() as cur:
        for i in range(0, len(updates), args.batch):
            execute_batch(cur, UPDATE_SQL, updates[i:i+args.batch], page_size=500)
            if i % (args.batch * 20) == 0:
                log(f"    updated {i+args.batch:>8,} / {len(updates):,}")
    conn.commit()
    log(f"[ok] applied {len(updates):,} UPDATEs in {(time.time()-t1)/60:.1f} min")
    conn.close()

if __name__ == '__main__':
    main()
