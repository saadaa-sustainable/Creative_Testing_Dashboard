"""
sync_shopify_ad_attribution.py - build & refresh `shopify_ad_attribution`.

For every ad_id ever seen in our Meta data PLUS every ad_id/adset_id seen
in Shopify utm tags, compute the attributed orders + sales from Shopify.

Pipeline:
  1. CREATE TABLE shopify_ad_attribution (idempotent)
  2. Load ad-lookup maps from backfill_table:
       ads_by_id     : ad_id -> {ad_name, adset_id, adset_name, campaign_name, spend}
       ads_by_name   : ad_name -> [ad_ids]      (exact)
       ads_by_fuzzy  : norm(ad_name) -> [ad_ids] (suffix-stripped)
       adset_ads     : adset_id -> [ad_ids in that adset]
       adset_spend   : adset_id -> total adset spend
  3. Stream Shopify orders (any utm_source=META) from the Supabase mirror's
     custom_attributes JSON column (= Shopify source-of-truth, fast).
     For each Meta order, attribute by cascade:
       Tier 1: utm_content is numeric ad_id and exists -> 1 order, full sales
       Tier 2: utm_content matches ad_name (exact or suffix-stripped) -> 1 order
               (split equally if name maps to multiple ad_ids)
       Tier 3: utm_term matches adset_id -> spend-weighted split across ads in adset
       Else  : record utm_term/utm_content as 'unknown' for the Meta-API fallback step
  4. For unknown adset_ids and ad_ids: call Meta API to fetch metadata,
     INSERT into backfill_table so future runs match.
  5. Re-run Tier 3 attribution using the fresh adset data.
  6. UPSERT one row per ad_id into shopify_ad_attribution.

⚠ IMPORTANT — RUN WITH THE FULL WINDOW EVERY TIME.
The UPSERT replaces each ad's `orders` and `sales` columns on every run.
That means a partial-window run (e.g. last 30 days only) will OVERWRITE the
lifetime numbers for those ads with the 30-day-only numbers, losing history.
For an hourly/daily cron, always pass `2025-01-01 → today`. The full
17-month walk takes ~4 hours on Shopify Admin API at 0.5s pacing — schedule
accordingly. If a faster refresh is needed, partition the table by date.

Usage:
  python sync_shopify_ad_attribution.py                       # FULL: 2025-01-01 -> today (default — SAFE)
  python sync_shopify_ad_attribution.py 2025-01-01 2026-06-08 # FULL with explicit window — SAFE
  python sync_shopify_ad_attribution.py 2026-04-01 2026-05-01 # PARTIAL — DO NOT USE for production sync
"""
import os, sys, json, time, re, requests, psycopg2
from collections import defaultdict
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()
DB_URL       = os.environ["SUPABASE_DB_URL"].strip()
META_TOKEN   = os.environ.get("META_ACCESS_TOKEN", "").strip()
META_API_VER = os.getenv("META_API_VERSION", "v21.0").strip()

# Shopify Admin API direct — token from .env (loaded by dotenv automatically).
# Tries a few common env-var names so the script works whichever you used.
SHOPIFY_TOKEN = (
    os.environ.get("ADMIN_ACCESS_TOKEN")
    or os.environ.get("SHOPIFY_ADMIN_ACCESS_TOKEN")
    or os.environ.get("SHOPIFY_ACCESS_TOKEN")
    or os.environ.get("SHOPIFY_ADMIN_TOKEN")
    or os.environ.get("SHOPIFY_TOKEN")
    or ""
).strip()
SHOPIFY_SHOP = (
    os.environ.get("SHOP_DOMAIN")
    or os.environ.get("SHOPIFY_SHOP")
    or "saadaa-design.myshopify.com"
).strip()
# Strip leading https:// if user pasted full URL
SHOPIFY_SHOP = SHOPIFY_SHOP.replace("https://","").replace("http://","").rstrip("/")
SHOPIFY_API_VER = os.environ.get("SHOPIFY_API_VERSION", "2025-10").strip()
SHOPIFY_GQL_URL = f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VER}/graphql.json"
SHOPIFY_H = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json",
}

SINCE = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
UNTIL = sys.argv[2] if len(sys.argv) > 2 else "2099-12-31"
PAGE  = 50           # Shopify Admin GraphQL caps at 50 orders/page
RATE_SLEEP = 0.5     # seconds between calls — Shopify standard limit is 2/sec

LOG = "sync_shopify_ad_attribution.log"

def log(*a):
    msg = " ".join(str(x) for x in a)
    print(msg, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# ---------------------------------------------------------------------------
# 1) CREATE TABLE
# ---------------------------------------------------------------------------
def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
          CREATE TABLE IF NOT EXISTS shopify_ad_attribution (
            ad_id            TEXT PRIMARY KEY,
            ad_name          TEXT,
            adset_id         TEXT,
            adset_name       TEXT,
            campaign_name    TEXT,
            orders           NUMERIC(14,4) DEFAULT 0,
            sales            NUMERIC(14,2) DEFAULT 0,
            last_synced_at   TIMESTAMPTZ DEFAULT NOW()
          );
          CREATE INDEX IF NOT EXISTS idx_sap_adset   ON shopify_ad_attribution(adset_id);
          CREATE INDEX IF NOT EXISTS idx_sap_ad_name ON shopify_ad_attribution(ad_name);
        """)
    conn.commit()
    log("[ok] table shopify_ad_attribution ensured")

# ---------------------------------------------------------------------------
# 2) Load lookup maps from backfill_table
# ---------------------------------------------------------------------------
# Suffix-strip regex copied from april_adname_match.py
SUFFIX_RE = re.compile(
    r"(?:[\s_-]+(?:copy(?:\s*\d+)?|[hc]\d+))+\s*$",
    re.IGNORECASE,
)
def norm_name(n):
    if not n: return ""
    n = n.strip().replace("�", "").replace("�", "").strip()
    while True:
        new = SUFFIX_RE.sub("", n).strip()
        if new == n: break
        n = new
    n = re.sub(r"\s+", " ", n).strip()
    return n.lower()

def load_lookups(conn):
    cur = conn.cursor()
    cur.execute("""
      SELECT ad_id,
             MIN(ad_name)                                AS ad_name,
             MIN(adset_id)                               AS adset_id,
             MIN(adset_name)                             AS adset_name,
             MIN(campaign_name)                          AS campaign_name,
             COALESCE(SUM(amount_spent_inr),0)::numeric  AS spend
      FROM backfill_table
      WHERE ad_id IS NOT NULL AND ad_id <> ''
      GROUP BY ad_id
    """)
    ads_by_id     = {}
    ads_by_name   = defaultdict(list)
    ads_by_fuzzy  = defaultdict(list)
    adset_ads     = defaultdict(list)
    adset_spend   = defaultdict(float)
    adset_meta    = {}
    for ad_id, ad_name, adset_id, adset_name, campaign, spend in cur.fetchall():
        meta = {
            "ad_id": ad_id, "ad_name": ad_name or "",
            "adset_id": adset_id or "", "adset_name": adset_name or "",
            "campaign_name": campaign or "", "spend": float(spend or 0),
        }
        ads_by_id[ad_id] = meta
        if ad_name:
            ads_by_name[ad_name].append(ad_id)
            ads_by_fuzzy[norm_name(ad_name)].append(ad_id)
        if adset_id:
            adset_ads[adset_id].append(ad_id)
            adset_spend[adset_id] += meta["spend"]
            adset_meta[adset_id] = (adset_name or "", campaign or "")
    log(f"[ok] {len(ads_by_id):,} ads, {len(adset_ads):,} adsets loaded from backfill_table")
    return ads_by_id, ads_by_name, ads_by_fuzzy, adset_ads, adset_spend, adset_meta

# ---------------------------------------------------------------------------
# 3) Stream Shopify orders, attribute
# ---------------------------------------------------------------------------
SHOPIFY_QUERY = """
query AdAttrPage($after: String, $q: String!) {
  orders(first: 50, after: $after, query: $q, sortKey: CREATED_AT, reverse: false) {
    edges {
      cursor
      node {
        name
        createdAt
        currentTotalPriceSet { shopMoney { amount } }
        customAttributes { key value }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

def shopify_page(after_cursor, since, until, retries=8):
    """One page of orders from Shopify Admin API. Auto-retries on 5xx/429."""
    payload = {
        "query": SHOPIFY_QUERY,
        "variables": {
            "after": after_cursor,
            "q": f"created_at:>={since} created_at:<={until}",
        },
    }
    delay = 5
    for i in range(retries):
        try:
            r = requests.post(SHOPIFY_GQL_URL, headers=SHOPIFY_H,
                              json=payload, timeout=180)
            if r.status_code == 200:
                j = r.json()
                if "errors" in j:
                    log(f"  graphql errors: {j['errors']}")
                    # Throttled errors come back as 200 with `THROTTLED` extension
                    if any(("THROTTLED" in str(e)) for e in j["errors"]):
                        time.sleep(delay); delay = min(delay*2, 120); continue
                    return None
                return j["data"]["orders"]
            log(f"  HTTP {r.status_code}: {r.text[:300]}")
            if r.status_code in (502, 503, 504, 429):
                time.sleep(delay); delay = min(delay*2, 120); continue
            return None
        except Exception as e:
            log(f"  retry {i+1}/{retries} {type(e).__name__}: {e}")
            time.sleep(delay); delay = min(delay*2, 120)
    return None

PROG_FILE = "full_backfill.progress.json"

def _attribute_one(node, ads_by_id, ads_by_name, ads_by_fuzzy,
                   adset_ads, adset_spend, per_ad,
                   unknown_adsets, unknown_ad_ids, counters):
    """Attribute one Shopify order node. In-place update of counters/maps."""
    ca = {a["key"]: a["value"] for a in (node.get("customAttributes") or [])}
    utm_source = ca.get("utm_source")
    if not utm_source or utm_source.upper() != "META": return False
    rev = float(node.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount") or 0)
    content = (ca.get("utm_content") or "").strip()
    term    = (ca.get("utm_term")    or "").strip()
    # Tier 1: utm_content is a numeric ad_id we know
    if content and content.isdigit() and content in ads_by_id:
        per_ad[content]["orders"] += 1
        per_ad[content]["sales"]  += rev
        counters["t1_ad_id"] += 1
        return True
    # Tier 2: utm_content matches ad_name (exact or fuzzy)
    if content:
        cand = None
        if content in ads_by_name:           cand = ads_by_name[content]
        else:
            n = norm_name(content)
            if n and n in ads_by_fuzzy:      cand = ads_by_fuzzy[n]
        if cand:
            share = 1.0 / len(cand); share_rev = rev / len(cand)
            for ad_id in cand:
                per_ad[ad_id]["orders"] += share
                per_ad[ad_id]["sales"]  += share_rev
            counters["t2_ad_name"] += 1
            return True
    # Tier 3: utm_term matches adset_id -> spend-weighted split
    if term and term in adset_ads:
        ads = adset_ads[term]
        tot = adset_spend[term]
        if tot > 0:
            for ad_id in ads:
                s = ads_by_id[ad_id]["spend"] / tot
                if s <= 0: continue
                per_ad[ad_id]["orders"] += s
                per_ad[ad_id]["sales"]  += rev * s
        else:
            s = 1.0/len(ads); sr = rev*s
            for ad_id in ads:
                per_ad[ad_id]["orders"] += s
                per_ad[ad_id]["sales"]  += sr
        counters["t3_adset"] += 1
        return True
    # Unknown -> Meta API fallback bucket
    if term:
        unknown_adsets[term]["orders"] += 1
        unknown_adsets[term]["sales"]  += rev
    elif content and content.isdigit():
        unknown_ad_ids[content]["orders"] += 1
        unknown_ad_ids[content]["sales"]  += rev
    else:
        counters["skip_no_term"] += 1
    return True

def _save_progress(state):
    with open(PROG_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, default=str)

def _load_progress():
    if not os.path.exists(PROG_FILE): return None
    try:
        with open(PROG_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception: return None

def stream_and_attribute(ads_by_id, ads_by_name, ads_by_fuzzy,
                         adset_ads, adset_spend, adset_meta):
    if not SHOPIFY_TOKEN:
        log("FATAL: no Shopify token in env (tried SHOPIFY_ADMIN_ACCESS_TOKEN, "
            "SHOPIFY_ACCESS_TOKEN, SHOPIFY_ADMIN_TOKEN, SHOPIFY_TOKEN)")
        sys.exit(1)
    log(f"[shopify] {SHOPIFY_GQL_URL}  range {SINCE} -> {UNTIL}")

    # Resume support
    prev = _load_progress()
    if prev and prev.get("since") == SINCE and prev.get("until") == UNTIL:
        cursor    = prev.get("cursor")
        pages     = prev.get("pages", 0)
        meta_in_window = prev.get("meta", 0)
        counters  = defaultdict(int, prev.get("counters", {}))
        per_ad    = defaultdict(lambda: {"orders":0.0,"sales":0.0},
                                {k: dict(v) for k,v in prev.get("per_ad", {}).items()})
        unknown_adsets = defaultdict(lambda: {"orders":0.0,"sales":0.0},
                                     {k: dict(v) for k,v in prev.get("u_ads", {}).items()})
        unknown_ad_ids = defaultdict(lambda: {"orders":0.0,"sales":0.0},
                                     {k: dict(v) for k,v in prev.get("u_ids", {}).items()})
        log(f"[resume] from cursor={cursor!r} page={pages} meta_so_far={meta_in_window:,}")
    else:
        cursor = None; pages = 0; meta_in_window = 0
        counters       = defaultdict(int)
        per_ad         = defaultdict(lambda: {"orders":0.0, "sales":0.0})
        unknown_adsets = defaultdict(lambda: {"orders":0.0, "sales":0.0})
        unknown_ad_ids = defaultdict(lambda: {"orders":0.0, "sales":0.0})

    while True:
        pages += 1
        orders_obj = shopify_page(cursor, SINCE, UNTIL)
        if orders_obj is None:
            log(f"  fatal at page {pages}; saving progress and exiting")
            break
        edges = orders_obj.get("edges") or []
        if not edges: log(f"  no more orders at page {pages}"); break

        for e in edges:
            node = e["node"]
            if _attribute_one(node, ads_by_id, ads_by_name, ads_by_fuzzy,
                              adset_ads, adset_spend, per_ad,
                              unknown_adsets, unknown_ad_ids, counters):
                meta_in_window += 1

        cursor = orders_obj["pageInfo"]["endCursor"]
        has_next = orders_obj["pageInfo"]["hasNextPage"]

        # progress save every page
        _save_progress({
            "since": SINCE, "until": UNTIL,
            "cursor": cursor, "pages": pages, "meta": meta_in_window,
            "counters": dict(counters),
            "per_ad":   {k: dict(v) for k,v in per_ad.items()},
            "u_ads":    {k: dict(v) for k,v in unknown_adsets.items()},
            "u_ids":    {k: dict(v) for k,v in unknown_ad_ids.items()},
        })

        if pages % 20 == 0 or not has_next:
            last_dt = (edges[-1]["node"].get("createdAt") or "")[:10]
            log(f"  page {pages}  meta={meta_in_window:,}  "
                f"t1={counters['t1_ad_id']:,} t2={counters['t2_ad_name']:,} "
                f"t3={counters['t3_adset']:,} u_ads={len(unknown_adsets)} u_ids={len(unknown_ad_ids)}  "
                f"last_date={last_dt}")
        if not has_next: log("  [end] hasNextPage=false"); break
        time.sleep(RATE_SLEEP)

    log(f"[done streaming] meta_orders={meta_in_window:,}  attributed_ads={len(per_ad):,}")
    return per_ad, unknown_adsets, unknown_ad_ids, counters.get("skip_no_term",0), counters

# ---------------------------------------------------------------------------
# 4) Meta-API fallback for unknown IDs
# ---------------------------------------------------------------------------
def meta_lookup_batch(ids):
    """Returns dict ad_or_adset_id -> dict with adset/campaign/ad info, depending on what Meta returns."""
    if not META_TOKEN:
        log("  WARN: META_ACCESS_TOKEN not set; skipping Meta API fallback")
        return {}
    out = {}
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        url = (f"https://graph.facebook.com/{META_API_VER}/"
               f"?ids={','.join(chunk)}"
               f"&fields=name,adset{{id,name}},campaign{{id,name}}"
               f"&access_token={META_TOKEN}")
        try:
            r = requests.get(url, timeout=60)
            if r.status_code != 200:
                log(f"  Meta API HTTP {r.status_code}: {r.text[:200]}")
                continue
            for k, v in r.json().items():
                if isinstance(v, dict): out[k] = v
        except Exception as e:
            log(f"  Meta API error: {e}")
        time.sleep(0.3)
    return out

def patch_unknown_into_backfill(conn, unknown_adsets, unknown_ad_ids,
                                 ads_by_id, adset_ads, adset_spend, adset_meta):
    """Query Meta API for unknown adset_ids and ad_ids; INSERT minimal rows
    into backfill_table so future runs match. Returns enriched lookup maps."""
    needs = list(unknown_adsets.keys()) + list(unknown_ad_ids.keys())
    if not needs: return
    log(f"[meta-api] resolving {len(needs)} unknown IDs ({len(unknown_adsets)} adsets, {len(unknown_ad_ids)} ad_ids)")
    info = meta_lookup_batch(needs)
    log(f"  Meta API returned info for {len(info)}/{len(needs)} IDs")

    cur = conn.cursor()
    new_rows = 0
    for some_id, meta in info.items():
        # If this is an ad_id: Meta returns name (ad name) and adset/campaign info
        name = meta.get("name")
        adset = meta.get("adset") or {}
        campaign = meta.get("campaign") or {}
        adset_id = adset.get("id") or ""
        adset_name = adset.get("name") or ""
        camp_name = campaign.get("name") or ""

        if some_id in unknown_ad_ids:
            # ad_id case: insert a placeholder row in backfill_table
            cur.execute("""
              INSERT INTO backfill_table (account_name, date, ad_id, ad_name, adset_id, adset_name, campaign_name, impressions, reach, amount_spent_inr)
              VALUES ('(meta-api-fallback)', CURRENT_DATE, %s, %s, %s, %s, %s, 0, 0, 0)
              ON CONFLICT DO NOTHING
            """, (some_id, name or "", adset_id, adset_name, camp_name))
            new_rows += 1
            # Also enrich live lookup
            ads_by_id[some_id] = {"ad_id":some_id, "ad_name":name or "",
                                  "adset_id":adset_id, "adset_name":adset_name,
                                  "campaign_name":camp_name, "spend":0.0}
            if adset_id and some_id not in adset_ads[adset_id]:
                adset_ads[adset_id].append(some_id)
                adset_meta.setdefault(adset_id, (adset_name, camp_name))
        elif some_id in unknown_adsets:
            # adset_id case: Meta returns the adset's name + campaign
            # We don't have ads_in_adset from API, so we record metadata only
            # and these orders stay unmatched-at-ad-level until ads sync later.
            cur.execute("""
              INSERT INTO backfill_table (account_name, date, ad_id, ad_name, adset_id, adset_name, campaign_name, impressions, reach, amount_spent_inr)
              VALUES ('(meta-api-fallback)', CURRENT_DATE, %s, %s, %s, %s, %s, 0, 0, 0)
              ON CONFLICT DO NOTHING
            """, (f"unknown_ad_in_{some_id}", "(unknown ad - via adset)",
                  some_id, name or "", camp_name))
            new_rows += 1
            adset_meta.setdefault(some_id, (name or "", camp_name))
    conn.commit()
    log(f"  inserted {new_rows} placeholder rows into backfill_table for Meta-API-resolved IDs")

# ---------------------------------------------------------------------------
# 5) UPSERT into shopify_ad_attribution
# ---------------------------------------------------------------------------
UPSERT = """
INSERT INTO shopify_ad_attribution
  (ad_id, ad_name, adset_id, adset_name, campaign_name, orders, sales, last_synced_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
ON CONFLICT (ad_id) DO UPDATE SET
  ad_name        = EXCLUDED.ad_name,
  adset_id       = EXCLUDED.adset_id,
  adset_name     = EXCLUDED.adset_name,
  campaign_name  = EXCLUDED.campaign_name,
  orders         = EXCLUDED.orders,
  sales          = EXCLUDED.sales,
  last_synced_at = NOW()
"""

def upsert(conn, per_ad, ads_by_id):
    rows = []
    for ad_id, d in per_ad.items():
        m = ads_by_id.get(ad_id, {})
        rows.append((
            ad_id,
            m.get("ad_name"),
            m.get("adset_id"),
            m.get("adset_name"),
            m.get("campaign_name"),
            round(d["orders"], 4),
            round(d["sales"], 2),
        ))
    with conn.cursor() as cur:
        execute_batch(cur, UPSERT, rows, page_size=500)
    conn.commit()
    log(f"[upsert] {len(rows):,} rows written into shopify_ad_attribution")

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    open(LOG, "w").close()
    conn = psycopg2.connect(DB_URL); conn.autocommit = False
    ensure_table(conn)

    lookups = load_lookups(conn)
    ads_by_id, ads_by_name, ads_by_fuzzy, adset_ads, adset_spend, adset_meta = lookups

    per_ad, unknown_adsets, unknown_ad_ids, unmatched, counters = \
        stream_and_attribute(ads_by_id, ads_by_name, ads_by_fuzzy,
                              adset_ads, adset_spend, adset_meta)

    log("\nAttribution tier counts:")
    for k,v in counters.items(): log(f"  {k:<20} {v:>8,}")

    # Meta API fallback for unknowns
    if unknown_adsets or unknown_ad_ids:
        patch_unknown_into_backfill(conn, unknown_adsets, unknown_ad_ids,
                                    ads_by_id, adset_ads, adset_spend, adset_meta)
        # Re-run a SMALL re-attribution for the previously-unknown buckets only.
        # We just credit the freshly-resolved ad_ids directly (1 order each, full rev).
        for ad_id, d in unknown_ad_ids.items():
            if ad_id in ads_by_id:
                per_ad[ad_id]["orders"] += d["orders"]
                per_ad[ad_id]["sales"]  += d["sales"]
        # For unknown adsets, only resolved adset metadata; ads still unknown,
        # so attribute the order to a synthetic "unknown_ad_in_<adset>" row.
        for adset_id, d in unknown_adsets.items():
            if adset_id in adset_meta:
                pseudo_id = f"unknown_ad_in_{adset_id}"
                ads_by_id.setdefault(pseudo_id, {
                    "ad_id":pseudo_id,
                    "ad_name":"(unknown ad - via adset)",
                    "adset_id":adset_id,
                    "adset_name":adset_meta[adset_id][0],
                    "campaign_name":adset_meta[adset_id][1],
                    "spend":0.0,
                })
                per_ad[pseudo_id]["orders"] += d["orders"]
                per_ad[pseudo_id]["sales"]  += d["sales"]

    upsert(conn, per_ad, ads_by_id)

    # ----- Final summary -----
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), SUM(orders), SUM(sales) FROM shopify_ad_attribution")
    n_rows, tot_orders, tot_sales = cur.fetchone()
    log(f"\n==================== DONE ====================")
    log(f"shopify_ad_attribution rows : {n_rows:,}")
    log(f"sum(orders) attributed       : {tot_orders or 0:,.2f}")
    log(f"sum(sales)  attributed       : Rs.{tot_sales or 0:,.2f}")
    log(f"unmatched (no term/content)  : {unmatched:,}")
    conn.close()
    # Wipe progress on clean finish so the next run starts fresh
    try: os.remove(PROG_FILE)
    except Exception: pass

if __name__ == "__main__":
    main()
