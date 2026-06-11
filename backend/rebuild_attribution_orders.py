"""
rebuild_attribution_orders.py - rebuild shopify_ad_attribution as a per-ORDER
table (replacing the per-AD-aggregated layout).

For every Shopify order since 2025-01-01:
  - Capture all UTM params + line items + total price
  - Run a 4-tier cascade match against our ad universe:
      T1 ad_id        utm_content (numeric) -> primary/backfill_table.ad_id
      T2 ad_name      utm_content / customAttributes.Ad -> ad_name (exact, then fuzzy suffix-strip)
      T3 adset_id     utm_term / customAttributes.adset -> adset_id
      T4 campaign     utm_campaign (numeric -> campaign_id), or Campaign attr -> campaign_name
  - Write one row with: order_id, line items, total, all UTMs, the matched ad's
    ad_id/ad_name/campaign_name/adset_id, has_match=t/f, matched_value=<what hit>

Resumable: progress saved every page, INSERT ON CONFLICT DO NOTHING means
a re-run picks up where we crashed.

Usage:
    python rebuild_attribution_orders.py                  # full: 2025-01-01 -> today
    python rebuild_attribution_orders.py 2025-06-01 2025-07-01   # explicit window
    python rebuild_attribution_orders.py --resume         # continue from progress file
    python rebuild_attribution_orders.py --recreate       # DROP + CREATE table first
"""
import os, sys, io, json, time, re, requests, psycopg2, argparse
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
from collections import defaultdict
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"].strip()
SHOPIFY_TOKEN = (os.environ.get("ADMIN_ACCESS_TOKEN")
                 or os.environ.get("SHOPIFY_ADMIN_ACCESS_TOKEN")
                 or os.environ.get("SHOPIFY_ACCESS_TOKEN") or "").strip()
SHOPIFY_SHOP = (os.environ.get("SHOP_DOMAIN") or "saadaa-design.myshopify.com")
SHOPIFY_SHOP = SHOPIFY_SHOP.replace("https://","").replace("http://","").rstrip("/")
SHOPIFY_API_VER = os.environ.get("SHOPIFY_API_VERSION", "2025-10").strip()
SHOPIFY_GQL_URL = f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VER}/graphql.json"
SHOPIFY_H = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}

PAGE = 50
RATE_SLEEP = 0.5
FLUSH_EVERY = 500
PROG_FILE = "rebuild_attribution.progress.json"
LOG = "logs/rebuild_attribution.log"

# ────────────────────────────────────────────────────────────────
def log(*a):
    msg = " ".join(str(x) for x in a)
    print(msg, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def fresh_conn():
    """Open a fresh DB connection — used at every flush so we never hit
    a stale pooler socket after long Shopify polling stretches."""
    c = psycopg2.connect(DB_URL, connect_timeout=30); c.autocommit = False
    with c.cursor() as cur: cur.execute("SET statement_timeout = '300s'")
    return c


SCHEMA_SQL = """
DROP TABLE IF EXISTS shopify_ad_attribution;
CREATE TABLE shopify_ad_attribution (
  order_id          TEXT PRIMARY KEY,
  order_created_at  TIMESTAMPTZ,
  ordered_item      TEXT,
  total_price       NUMERIC(14,2),
  utm_campaign      TEXT,
  utm_content       TEXT,
  utm_medium        TEXT,
  utm_source        TEXT,
  utm_term          TEXT,
  ad_id             TEXT,
  ad_name           TEXT,
  campaign_name     TEXT,
  adset_id          TEXT,
  has_match         BOOLEAN DEFAULT FALSE,
  matched_value     TEXT,
  matched_tier      TEXT,
  last_synced_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_sao_has_match    ON shopify_ad_attribution (has_match);
CREATE INDEX idx_sao_ad_id        ON shopify_ad_attribution (ad_id) WHERE ad_id IS NOT NULL;
CREATE INDEX idx_sao_created      ON shopify_ad_attribution (order_created_at DESC);
CREATE INDEX idx_sao_utm_source   ON shopify_ad_attribution (utm_source);
"""


def recreate_table():
    conn = fresh_conn()
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit(); conn.close()
    log("[ok] shopify_ad_attribution dropped and recreated with per-order schema")


# ────────────────────────────────────────────────────────────────
# Ad lookup maps from primary_table + backfill_table
SUFFIX_RE = re.compile(r"(?:[\s_-]+(?:copy(?:\s*\d+)?|[hc]\d+))+\s*$", re.IGNORECASE)


def norm_name(n):
    if not n: return ""
    n = n.strip()
    while True:
        new = SUFFIX_RE.sub("", n).strip()
        if new == n: break
        n = new
    return re.sub(r"\s+", " ", n).strip().lower()


def load_ad_universe():
    """Union of primary_table + backfill_table. Returns lookup maps keyed by
    ad_id, ad_name (exact + fuzzy), adset_id, campaign_name, campaign_id."""
    conn = fresh_conn(); cur = conn.cursor()
    cur.execute("""
      SELECT ad_id, MIN(ad_name), MIN(adset_id), MIN(adset_name),
             MIN(campaign_name), MIN(campaign_id)
      FROM (
        SELECT ad_id, ad_name, adset_id, adset_name, campaign_name, campaign_id FROM primary_table
        UNION ALL
        SELECT ad_id, ad_name, adset_id, adset_name, campaign_name, campaign_id FROM backfill_table
      ) u
      WHERE ad_id IS NOT NULL AND ad_id <> ''
      GROUP BY ad_id
    """)
    by_id = {}
    by_name = defaultdict(list)
    by_fuzzy = defaultdict(list)
    by_adset = defaultdict(list)
    by_campaign_name = defaultdict(list)
    by_campaign_id = defaultdict(list)
    for ad_id, ad_name, adset_id, adset_name, camp_name, camp_id in cur.fetchall():
        meta = {"ad_id": ad_id, "ad_name": ad_name or "",
                "adset_id": adset_id or "", "adset_name": adset_name or "",
                "campaign_name": camp_name or "", "campaign_id": camp_id or ""}
        by_id[ad_id] = meta
        if ad_name:
            by_name[ad_name.strip()].append(ad_id)
            f = norm_name(ad_name)
            if f: by_fuzzy[f].append(ad_id)
        if adset_id:        by_adset[adset_id].append(ad_id)
        if camp_name:       by_campaign_name[camp_name.strip()].append(ad_id)
        if camp_id:         by_campaign_id[camp_id].append(ad_id)
    conn.close()
    log(f"[ok] ad universe loaded: {len(by_id):,} ads, "
        f"{len(by_name):,} unique names, {len(by_adset):,} adsets, "
        f"{len(by_campaign_name):,} campaign names")
    return by_id, by_name, by_fuzzy, by_adset, by_campaign_name, by_campaign_id


# ────────────────────────────────────────────────────────────────
SHOPIFY_QUERY = """
query RebuildAttrPage($after: String, $q: String!) {
  orders(first: 50, after: $after, query: $q, sortKey: CREATED_AT, reverse: false) {
    edges {
      cursor
      node {
        id
        name
        createdAt
        currentTotalPriceSet { shopMoney { amount } }
        customAttributes { key value }
        lineItems(first: 25) {
          edges { node { title quantity } }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def shopify_page(after, since, until, retries=8):
    payload = {"query": SHOPIFY_QUERY,
               "variables": {"after": after,
                             "q": f"created_at:>={since} created_at:<={until}"}}
    delay = 5
    for i in range(retries):
        try:
            r = requests.post(SHOPIFY_GQL_URL, headers=SHOPIFY_H,
                              json=payload, timeout=180)
            if r.status_code == 200:
                j = r.json()
                if "errors" in j:
                    if any("THROTTLED" in str(e) for e in j["errors"]):
                        time.sleep(delay); delay = min(delay*2, 120); continue
                    log(f"  graphql errors: {j['errors']}"); return None
                return j["data"]["orders"]
            log(f"  HTTP {r.status_code}: {r.text[:200]}")
            if r.status_code in (502,503,504,429):
                time.sleep(delay); delay = min(delay*2, 120); continue
            return None
        except Exception as e:
            log(f"  retry {i+1}/{retries} {type(e).__name__}: {e}")
            time.sleep(delay); delay = min(delay*2, 120)
    return None


# ────────────────────────────────────────────────────────────────
def attribute_order(ca, maps):
    """Returns (ad_id, ad_name, adset_id, campaign_name, matched_value, matched_tier).
    Empty strings for the metadata fields when no match."""
    by_id, by_name, by_fuzzy, by_adset, by_camp_name, by_camp_id = maps

    utm_content = (ca.get("utm_content")  or "").strip()
    utm_term    = (ca.get("utm_term")     or "").strip()
    utm_camp    = (ca.get("utm_campaign") or "").strip()
    # custom attribute aliases from our destination-URL template
    attr_ad_name   = (ca.get("Ad") or ca.get("ad") or "").strip()
    attr_camp_name = (ca.get("Campaign") or ca.get("campaign") or "").strip()
    attr_adset_id  = (ca.get("AdSetID") or ca.get("adset_id")
                      or ca.get("adset") or "").strip()

    # T1: utm_content is a numeric ad_id in our universe
    if utm_content.isdigit() and utm_content in by_id:
        m = by_id[utm_content]
        return (m["ad_id"], m["ad_name"], m["adset_id"], m["campaign_name"],
                utm_content, "T1_ad_id")

    # T2: utm_content (or the Ad custom attr) matches an ad_name (exact then fuzzy)
    for cand in (attr_ad_name, utm_content):
        if not cand: continue
        if cand in by_name:
            ad_id = by_name[cand][0]; m = by_id[ad_id]
            return (m["ad_id"], m["ad_name"], m["adset_id"], m["campaign_name"],
                    cand, "T2_ad_name_exact")
        f = norm_name(cand)
        if f and f in by_fuzzy:
            ad_id = by_fuzzy[f][0]; m = by_id[ad_id]
            return (m["ad_id"], m["ad_name"], m["adset_id"], m["campaign_name"],
                    cand, "T2_ad_name_fuzzy")

    # T3: utm_term (or AdSetID attr) matches an adset_id
    for cand in (attr_adset_id, utm_term):
        if cand and cand in by_adset:
            ad_id = by_adset[cand][0]; m = by_id[ad_id]
            return ("", m["ad_name"], m["adset_id"], m["campaign_name"],
                    cand, "T3_adset_id")

    # T4: utm_campaign (numeric -> id) or Campaign custom attr (name) matches
    if utm_camp.isdigit() and utm_camp in by_camp_id:
        ad_id = by_camp_id[utm_camp][0]; m = by_id[ad_id]
        return ("", "", m["adset_id"], m["campaign_name"],
                utm_camp, "T4_campaign_id")
    if attr_camp_name and attr_camp_name in by_camp_name:
        ad_id = by_camp_name[attr_camp_name][0]; m = by_id[ad_id]
        return ("", "", "", m["campaign_name"],
                attr_camp_name, "T4_campaign_name")
    if utm_camp and utm_camp in by_camp_name:
        ad_id = by_camp_name[utm_camp][0]; m = by_id[ad_id]
        return ("", "", "", m["campaign_name"],
                utm_camp, "T4_campaign_name")

    return ("", "", "", "", "", "")


def build_row(node, maps):
    ca = {a["key"]: a["value"] for a in (node.get("customAttributes") or [])}
    ad_id, ad_name, adset_id, camp_name, matched_value, matched_tier = \
        attribute_order(ca, maps)

    items = [(e["node"].get("title") or "").replace(",", " ")
             for e in (node.get("lineItems", {}).get("edges") or [])]
    items_str = ", ".join(filter(None, items))[:2000]

    total = node.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount") or 0
    return (
        node["id"],
        node.get("createdAt"),
        items_str,
        float(total),
        ca.get("utm_campaign"), ca.get("utm_content"), ca.get("utm_medium"),
        ca.get("utm_source"),  ca.get("utm_term"),
        ad_id or None, ad_name or None, camp_name or None, adset_id or None,
        bool(matched_tier),
        matched_value or None,
        matched_tier or None,
    )


INSERT_SQL = """
INSERT INTO shopify_ad_attribution
  (order_id, order_created_at, ordered_item, total_price,
   utm_campaign, utm_content, utm_medium, utm_source, utm_term,
   ad_id, ad_name, campaign_name, adset_id,
   has_match, matched_value, matched_tier)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (order_id) DO NOTHING
"""


def flush(buffer, counters):
    if not buffer: return
    conn = fresh_conn()
    with conn.cursor() as cur:
        execute_batch(cur, INSERT_SQL, buffer, page_size=500)
    conn.commit(); conn.close()
    counters["flushed"] += len(buffer)


# ────────────────────────────────────────────────────────────────
def save_prog(state):
    with open(PROG_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, default=str)


def load_prog():
    if not os.path.exists(PROG_FILE): return None
    try:
        with open(PROG_FILE, encoding="utf-8") as f: return json.load(f)
    except Exception: return None


def main():
    os.makedirs("logs", exist_ok=True)
    p = argparse.ArgumentParser()
    p.add_argument("since", nargs="?", default="2025-01-01")
    p.add_argument("until", nargs="?", default="2099-12-31")
    p.add_argument("--resume",   action="store_true", help="Continue from progress file")
    p.add_argument("--recreate", action="store_true", help="DROP + CREATE table first")
    args = p.parse_args()

    open(LOG, "a").close()
    log(f"\n========== rebuild_attribution_orders @ {time.strftime('%Y-%m-%d %H:%M:%S')} ==========")
    log(f"window: {args.since} -> {args.until}  resume={args.resume}  recreate={args.recreate}")

    if args.recreate:
        recreate_table()
        try: os.remove(PROG_FILE)
        except FileNotFoundError: pass

    maps = load_ad_universe()

    cursor = None; pages = 0
    counters = defaultdict(int)
    if args.resume:
        prog = load_prog()
        if prog and prog.get("since") == args.since:
            cursor = prog.get("cursor"); pages = prog.get("pages", 0)
            counters.update(prog.get("counters", {}))
            log(f"[resume] cursor={cursor!r}  pages_done={pages}  rows_so_far={counters.get('flushed',0):,}")

    buffer = []
    while True:
        pages += 1
        data = shopify_page(cursor, args.since, args.until)
        if data is None:
            log(f"[fatal] page {pages} failed; flushing buffer and exiting (resume with --resume)")
            flush(buffer, counters); break
        edges = data.get("edges") or []
        if not edges:
            log(f"[end] no more orders at page {pages}")
            flush(buffer, counters); break

        for e in edges:
            try:
                row = build_row(e["node"], maps)
                buffer.append(row)
                counters["seen"] += 1
                if row[13]: counters[f"hit_{row[15]}"] += 1
                else:       counters["no_match"] += 1
            except Exception as ex:
                log(f"  row build err: {ex}"); counters["row_err"] += 1

        if len(buffer) >= FLUSH_EVERY:
            flush(buffer, counters); buffer = []

        cursor = data["pageInfo"]["endCursor"]
        has_next = data["pageInfo"]["hasNextPage"]
        save_prog({"since": args.since, "until": args.until,
                   "cursor": cursor, "pages": pages,
                   "counters": dict(counters)})

        if pages % 20 == 0 or not has_next:
            last_dt = (edges[-1]["node"].get("createdAt") or "")[:10]
            matched = sum(v for k,v in counters.items() if k.startswith("hit_"))
            log(f"  page {pages:>5}  seen={counters['seen']:,}  matched={matched:,}  "
                f"no_match={counters['no_match']:,}  last_date={last_dt}")

        if not has_next:
            flush(buffer, counters)
            log("[end] hasNextPage=false")
            break
        time.sleep(RATE_SLEEP)

    log("\n========== final tier counts ==========")
    for k in sorted(counters):
        log(f"  {k:<26} {counters[k]:>10,}")

    # final summary from DB
    conn = fresh_conn(); cur = conn.cursor()
    cur.execute("""SELECT COUNT(*), COUNT(*) FILTER (WHERE has_match),
                          ROUND(SUM(total_price),0)
                   FROM shopify_ad_attribution""")
    n, matched, tot = cur.fetchone()
    log(f"\nshopify_ad_attribution: {n:,} rows  matched={matched:,} "
        f"({matched/max(n,1)*100:.1f}%)  sum(total)=Rs {tot or 0:,.0f}")
    conn.close()
    if not args.resume:
        try: os.remove(PROG_FILE)
        except FileNotFoundError: pass


if __name__ == "__main__":
    main()
