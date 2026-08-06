"""Fetch Meta lifetime purchases/CI/ATC per ad and cache in
public.ad_meta_lifetime. Fills the gap left by primary_table's rolling
15-day window: an ad that delivered heavily in the past but is now
paused had purchases=0 in ae_table_view because backfill_table doesn't
carry the purchases column.

Uses Meta Insights /act_XXX/insights?level=ad&date_preset=maximum with
filtering=[ad.id IN [...]] — 250 ids per call, per account. Extracts
`omni_purchase` / `purchase` from actions[].

USAGE
  python fetch_ad_lifetime_purchases.py             # all ads with backfill delivery + no primary purchases
  python fetch_ad_lifetime_purchases.py --ad AD_ID  # single ad probe
  python fetch_ad_lifetime_purchases.py --all       # every ad ever seen
"""
from __future__ import annotations
import os, sys, io, time, argparse, requests, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

TOKEN  = os.environ["META_ACCESS_TOKEN"].strip()
VER    = os.environ.get("META_API_VERSION", "v22.0").strip()
DB_URL = os.environ["SUPABASE_DB_URL"].strip()

# Discover the three Meta accounts from primary_table
def discover_accounts(cur):
    cur.execute("""
      SELECT DISTINCT account_name,
        (regexp_matches(account_name, 'act_[0-9]+|[0-9]{10,}'))[1] AS act_id
      FROM public.primary_table
      WHERE account_name IS NOT NULL
    """)
    return cur.fetchall()

# Simpler: read from primary_table where each row has an act_id embedded in
# meta responses (we don't have it here). Use the same account names/IDs
# primary_sync knows about. Load from env fallback.
ACT_IDS = {
    # Meta ad account IDs — same source of truth as primary_sync.py:52.
    "Raho Saadaa":            os.environ.get("ACCOUNT_1_ID", "1136644150469466"),
    "Fourth Ad Account - SD": os.environ.get("ACCOUNT_2_ID", "1349767139294217"),
    "Third Ad Account - SD":  os.environ.get("ACCOUNT_3_ID", "264868699479122"),
}

DDL = """
CREATE TABLE IF NOT EXISTS public.ad_meta_lifetime (
    ad_id                text        PRIMARY KEY,
    account_name         text,
    lifetime_purchases   numeric(14,2) DEFAULT 0,
    lifetime_ci          numeric(14,2) DEFAULT 0,
    lifetime_atc         numeric(14,2) DEFAULT 0,
    lifetime_link_clicks bigint       DEFAULT 0,
    lifetime_impressions bigint       DEFAULT 0,
    lifetime_spend       numeric(14,2) DEFAULT 0,
    refreshed_at         timestamptz  NOT NULL DEFAULT now()
);
"""

# Match primary_sync.py:601 semantics: take the FIRST matching action_type
# in preference order (omni_purchase, purchase). Meta reports the same
# purchase event under multiple action_types (omni, pixel, custom) — summing
# them 3x-overcounts (user reported 11,605 actual, my sum-all gave 34,815).
PURCHASE_ORDER = ("omni_purchase", "purchase")
CI_ORDER       = ("omni_initiated_checkout", "initiate_checkout")
ATC_ORDER      = ("omni_add_to_cart",        "add_to_cart")

def first_action_val(actions, order):
    """Return value of the first action_type in `order` that appears in
    Meta's actions list. Mirrors primary_sync._action_val."""
    if not actions: return 0
    # Build a lookup so we don't do O(n*m)
    by_type = {a.get("action_type"): a.get("value") for a in actions}
    for t in order:
        if t in by_type:
            try: return float(by_type[t] or 0)
            except Exception: return 0
    return 0

def fetch_ads(act_id: str, ad_ids: list[str], cur=None, account_name: str = "") -> dict:
    """Return {ad_id: {...}} — and, when `cur` is provided, incrementally
    upsert every 20 chunks so a mid-run failure doesn't lose progress."""
    out = {}
    if not act_id or not ad_ids: return out
    # Meta rejected 250-ID batches with error 1487534 "too many rows" even
    # at level=ad — internally the /insights endpoint enumerates all daily
    # rows before aggregating, blowing past its row cap. 25 IDs per call is
    # the practical ceiling under date_preset=maximum for accounts running
    # 500+ days.
    CHUNK = 25
    for i in range(0, len(ad_ids), CHUNK):
        chunk = ad_ids[i:i+CHUNK]
        url = f"https://graph.facebook.com/{VER}/act_{act_id}/insights"
        params = {
            "access_token": TOKEN,
            "level": "ad",
            "date_preset": "maximum",
            "fields": "ad_id,actions,inline_link_clicks,impressions,spend",
            "filtering": '[{"field":"ad.id","operator":"IN","value":' +
                         '[' + ','.join(f'"{x}"' for x in chunk) + ']}]',
            "limit": "500",
        }
        attempt = 0
        while attempt < 3:
            attempt += 1
            r = requests.get(url, params=params, timeout=120)
            if r.status_code == 200: break
            if r.status_code in (500, 502, 503) and attempt < 3:
                print(f"    Meta {r.status_code} — retry {attempt}/3 after 15s")
                time.sleep(15); continue
            raise RuntimeError(f"Meta HTTP {r.status_code}: {r.text[:400]}")
        data = r.json()
        for row in data.get("data", []):
            aid = str(row.get("ad_id") or "")
            if not aid: continue
            actions = row.get("actions") or []
            out[aid] = {
                "purchases":   first_action_val(actions, PURCHASE_ORDER),
                "ci":          first_action_val(actions, CI_ORDER),
                "atc":         first_action_val(actions, ATC_ORDER),
                "link_clicks": int(row.get("inline_link_clicks") or 0),
                "impressions": int(row.get("impressions") or 0),
                "spend":       float(row.get("spend") or 0),
            }
        n_chunk = (i // CHUNK) + 1
        n_total = -(-len(ad_ids) // CHUNK)
        print(f"    chunk {n_chunk}/{n_total}: {len(data.get('data', []))} rows returned")
        time.sleep(1.2)   # throttle
        # Flush progress every 20 chunks — mid-run failure doesn't lose work.
        if cur is not None and n_chunk % 20 == 0 and out:
            upsert(cur, account_name, out)
            out = {}
    return out
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ad", help="Single ad_id to probe")
    ap.add_argument("--all", action="store_true", help="Every ad ever seen (not just missing-purchases)")
    args = ap.parse_args()

    conn = psycopg2.connect(DB_URL, connect_timeout=30); conn.autocommit = True
    cur  = conn.cursor()
    cur.execute("SET statement_timeout = '30min'")
    cur.execute(DDL)

    # Discover which act_id each ad belongs to.
    # We'll query primary_table + backfill_table for each account_name.
    for name, act_id in list(ACT_IDS.items()):
        if not act_id:
            print(f"  [WARN] Missing act_id for {name!r} — set env ACCOUNT_*_ID; skipping.")
            del ACT_IDS[name]

    if args.ad:
        ad_target = [args.ad]
        # Find its account_name from primary/backfill
        cur.execute("""
          SELECT account_name FROM public.primary_table WHERE ad_id=%s LIMIT 1
        """, (args.ad,))
        r = cur.fetchone()
        if not r:
            cur.execute("""
              SELECT account_name FROM public.backfill_table WHERE ad_id=%s LIMIT 1
            """, (args.ad,))
            r = cur.fetchone()
        acct = r[0] if r else None
        act_id = ACT_IDS.get(acct)
        if not act_id:
            print(f"[fatal] can't map ad {args.ad} to an account_name/act_id (acct={acct!r})")
            return 1
        print(f"[*] probing single ad {args.ad} in account {acct!r} (act_{act_id})")
        result = fetch_ads(act_id, ad_target)
        print("Meta returned:", result)
        if result:
            upsert(cur, acct, result)
            print("Upserted.")
        return 0

    # Full run: fetch per-account for the ads that lack primary purchases.
    # Skip ads already fetched in the last 24h so a restart resumes cleanly.
    for name, act_id in ACT_IDS.items():
        if args.all:
            cur.execute("""
              SELECT DISTINCT ad_id FROM public.backfill_table
              WHERE account_name=%s AND ad_id IS NOT NULL AND ad_id <> ''
                AND ad_id NOT IN (
                  SELECT ad_id FROM public.ad_meta_lifetime
                  WHERE refreshed_at > now() - interval '24 hours'
                )
            """, (name,))
        else:
            cur.execute("""
              SELECT DISTINCT b.ad_id FROM public.backfill_table b
              WHERE b.account_name=%s AND b.ad_id IS NOT NULL AND b.ad_id <> ''
                AND b.impressions > 0
                AND b.ad_id NOT IN (
                  SELECT ad_id FROM public.primary_table WHERE purchases > 0
                )
                AND b.ad_id NOT IN (
                  SELECT ad_id FROM public.ad_meta_lifetime
                  WHERE refreshed_at > now() - interval '24 hours'
                )
            """, (name,))
        ids = [r[0] for r in cur.fetchall()]
        print(f"\n[{name}] {len(ids)} ads to fetch (skipping already-cached)")
        if not ids: continue
        result = fetch_ads(act_id, ids, cur=cur, account_name=name)
        print(f"  Meta returned data for {len(result)} of {len(ids)} ads (this batch)")
        if result: upsert(cur, name, result)

    # Sanity: how many rows now?
    cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE lifetime_purchases > 0), SUM(lifetime_purchases) FROM public.ad_meta_lifetime")
    tot, with_p, sum_p = cur.fetchone()
    print(f"\n[DONE] ad_meta_lifetime rows={tot:,} with_purchases={with_p:,} total_purchases={float(sum_p or 0):,.0f}")
    return 0

def upsert(cur, account_name, result):
    rows = [
        (aid, account_name,
         d["purchases"], d["ci"], d["atc"],
         d["link_clicks"], d["impressions"], d["spend"])
        for aid, d in result.items()
    ]
    if not rows: return
    execute_values(cur, """
      INSERT INTO public.ad_meta_lifetime
        (ad_id, account_name, lifetime_purchases, lifetime_ci, lifetime_atc,
         lifetime_link_clicks, lifetime_impressions, lifetime_spend)
      VALUES %s
      ON CONFLICT (ad_id) DO UPDATE SET
        account_name         = EXCLUDED.account_name,
        lifetime_purchases   = EXCLUDED.lifetime_purchases,
        lifetime_ci          = EXCLUDED.lifetime_ci,
        lifetime_atc         = EXCLUDED.lifetime_atc,
        lifetime_link_clicks = EXCLUDED.lifetime_link_clicks,
        lifetime_impressions = EXCLUDED.lifetime_impressions,
        lifetime_spend       = EXCLUDED.lifetime_spend,
        refreshed_at         = now()
    """, rows, page_size=500)
    print(f"    upserted {len(rows)} ads")

if __name__ == "__main__":
    sys.exit(main())
