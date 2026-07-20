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
from datetime import datetime, date, timezone
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
from collections import defaultdict
from psycopg2.extras import execute_batch

from supabase_config import load_supabase_settings

settings = load_supabase_settings()
DB_URL = settings["db_url"].strip()
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
    with c.cursor() as cur: cur.execute("SET statement_timeout = '30min'")
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

# ── Asset-code detection (same rules as ae_table_view + dashboard) ─────────
_ASSET_PATTERNS = [
    ("IFAD", re.compile(r"IFAD", re.I)),
    ("GAD",  re.compile(r"GAD",  re.I)),
    ("BST",  re.compile(r"BST",  re.I)),
    ("UGC",  re.compile(r"UGC",  re.I)),
    ("ADB",  re.compile(r"ADB",  re.I)),
    ("VID",  re.compile(r"(^|[^A-Za-z])VID([^A-Za-z]|$)")),
    ("BR_",  re.compile(r"BR_",  re.I)),
    ("BI_",  re.compile(r"BI_",  re.I)),
]
def detect_asset_code(s):
    if not s: return None
    for code, pat in _ASSET_PATTERNS:
        if pat.search(s): return code
    return None

# ── Embedded-date extraction (handles many human-typed UTM/ad-name patterns) ─
# Matches: 13/04/26  13-04-26  13.04.26  13_04_26  13/04/2026  13042026  130426
_DATE_PATTERNS = [
    # DD<sep>MM<sep>YYYY  (long year wins — finditer evaluates each pattern)
    re.compile(r"(?<!\d)(\d{1,2})[/_.\-](\d{1,2})[/_.\-](\d{4})(?!\d)"),
    # DD<sep>MM<sep>YY    (YY restricted to 20-35 so we don't grab random 2-digit numbers)
    re.compile(r"(?<!\d)(\d{1,2})[/_.\-](\d{1,2})[/_.\-](2\d|3[0-5])(?!\d)"),
    # DDMMYYYY 8 digits no separator
    re.compile(r"(?<!\d)(\d{2})(\d{2})(20\d{2})(?!\d)"),
    # DDMMYY   6 digits no separator (YY restricted to 20-35)
    re.compile(r"(?<!\d)(\d{2})(\d{2})(2\d|3[0-5])(?!\d)"),
]

def extract_dates_from(s):
    """Return a set of date() objects parsed out of an arbitrary string.
    Accepts D/M/YY, D-M-YY, D.M.YY, D_M_YY, DDMMYY, DDMMYYYY etc.
    Day 1-31, month 1-12, year 2020-2035 (defensive bounds)."""
    if not s: return set()
    out = set()
    for pat in _DATE_PATTERNS:
        for m in pat.finditer(s):
            try:
                d = int(m.group(1)); mo = int(m.group(2))
                yy = m.group(3); y = int(yy) if len(yy) == 4 else 2000 + int(yy)
                if not (1 <= d <= 31 and 1 <= mo <= 12 and 2020 <= y <= 2035):
                    continue
                out.add(date(y, mo, d))
            except (ValueError, TypeError):
                continue
    return out

def _dates_close(set_a, set_b, slack=2):
    """True if any date in set_a is within `slack` days of any date in set_b."""
    if not set_a or not set_b: return False
    for a in set_a:
        for b in set_b:
            if abs((a - b).days) <= slack: return True
    return False

# ── Separator-normalized key for permissive substring matching ────────────────
# Saadaa ad-naming convention is inconsistent: tokens are joined with any of
# "+", "-", "_", ".", or space. utm strings often use a different separator
# than the ad_name (e.g. utm "CLP-SDPL MU OFF-RS" vs name "CLP-SDPL+MU+OFF-RS").
# Collapsing all separators to a single space lets these match.
_SEP_RE = re.compile(r"[+\-_/.\s,]+")
def _sep_key(s):
    if not s: return ""
    return _SEP_RE.sub(" ", s).strip().lower()

def _parse_iso(ts):
    if not ts: return None
    try:
        if hasattr(ts, "date"): return ts.date() if hasattr(ts, "date") else ts
        s = str(ts).replace("Z","+00:00")
        return datetime.fromisoformat(s).date()
    except Exception: return None

def _pick_by_date(candidates, order_date):
    """From a list of ad metas, pick one with ad_created_date closest to (and not after)
    the order date. Returns the best candidate or None."""
    if not candidates or not order_date: return candidates[0] if candidates else None
    valid = [c for c in candidates if c.get("ad_created_date")]
    if not valid: return candidates[0]
    # Prefer ads created on/before the order; tie-break by recency.
    valid.sort(key=lambda c: (
        0 if c["ad_created_date"] <= order_date else 1,
        abs((order_date - c["ad_created_date"]).days)
    ))
    return valid[0]


def load_ad_universe():
    """Union of primary_table + backfill_table. Returns:
      by_id          : ad_id -> canonical meta
      by_name        : ad_name -> [ad_id, ...]   (all historical names)
      by_fuzzy       : norm(ad_name) -> [ad_id, ...]
      by_adset       : adset_id -> [ad_id, ...]
      by_campaign_*  : ditto
      adset_ads      : adset_id -> [rich-meta, ...]   (NEW — adset-scoped)
      camp_name_ads  : campaign_name -> [rich-meta, ...]
      camp_id_ads    : campaign_id   -> [rich-meta, ...]

    Each rich-meta entry has ad_id, ad_name, names(set), ad_created_date,
    asset_code, adset_id, adset_name, campaign_name, campaign_id,
    lifetime_spend (used by T2 substring tiebreaker — prefer the
    high-spend ad when multiple ads share a name fragment).
    """
    conn = fresh_conn(); cur = conn.cursor()
    # Lifetime spend per ad_id (used as substring-match tiebreaker so orders
    # route to ads Meta actually delivered, not to renamed/discarded clones).
    cur.execute("""
      SELECT ad_id, SUM(COALESCE(amount_spent_inr,0))::numeric AS spend
      FROM (
        SELECT ad_id, amount_spent_inr FROM primary_table  WHERE ad_id IS NOT NULL
        UNION ALL
        SELECT ad_id, amount_spent_inr FROM backfill_table WHERE ad_id IS NOT NULL
      ) u
      GROUP BY ad_id
    """)
    lifetime_spend = {ad_id: float(s or 0) for ad_id, s in cur.fetchall()}
    # Distinct (ad_id, ad_name, ...) — keep every historical ad_name variant
    cur.execute("""
      SELECT DISTINCT ad_id, ad_name, adset_id, adset_name,
                      campaign_name, campaign_id, ad_created_date
      FROM (
        SELECT ad_id, ad_name, adset_id, adset_name, campaign_name, campaign_id, ad_created_date FROM primary_table
        UNION ALL
        SELECT ad_id, ad_name, adset_id, adset_name, campaign_name, campaign_id, ad_created_date FROM backfill_table
      ) u
      WHERE ad_id IS NOT NULL AND ad_id <> ''
    """)
    by_id = {}
    by_name  = defaultdict(list)
    by_fuzzy = defaultdict(list)
    by_adset = defaultdict(list)
    by_campaign_name = defaultdict(list)
    by_campaign_id   = defaultdict(list)
    rich = {}   # ad_id -> rich meta (accumulates name variants + earliest ad_created_date)

    for ad_id, ad_name, adset_id, adset_name, camp_name, camp_id, ad_created_date in cur.fetchall():
        ad_name = (ad_name or "").strip()
        adset_id = (adset_id or "").strip()
        camp_name = (camp_name or "").strip()
        camp_id   = (camp_id   or "").strip()
        created   = _parse_iso(ad_created_date)

        if ad_id not in rich:
            rich[ad_id] = {
                "ad_id": ad_id, "ad_name": ad_name,
                "names": set(),
                "name_dates": set(),           # union of dates found in any historical ad_name
                "adset_id": adset_id, "adset_name": (adset_name or "").strip(),
                "campaign_name": camp_name, "campaign_id": camp_id,
                "ad_created_date": created,
                "asset_code": detect_asset_code(ad_name),
            }
        m = rich[ad_id]
        if ad_name:
            m["names"].add(ad_name)
            m["name_dates"].update(extract_dates_from(ad_name))
        if created and (not m["ad_created_date"] or created < m["ad_created_date"]):
            m["ad_created_date"] = created
        # Keep canonical ad_name as the longest seen
        if ad_name and len(ad_name) > len(m["ad_name"]):
            m["ad_name"] = ad_name
            m["asset_code"] = detect_asset_code(ad_name)
        # Upgrade empty metadata fields when a later row carries a non-empty value
        # (some primary_table rows have adset_id='' on the same ad — without this
        # the ad gets dropped from the per-adset index)
        if adset_id  and not m["adset_id"]:      m["adset_id"]      = adset_id
        if adset_name and not m["adset_name"]:   m["adset_name"]    = (adset_name or "").strip()
        if camp_name and not m["campaign_name"]: m["campaign_name"] = camp_name
        if camp_id   and not m["campaign_id"]:   m["campaign_id"]   = camp_id

        # Indices
        if ad_name and ad_id not in by_name[ad_name]:
            by_name[ad_name].append(ad_id)
        f = norm_name(ad_name)
        if f and ad_id not in by_fuzzy[f]:
            by_fuzzy[f].append(ad_id)
        if adset_id  and ad_id not in by_adset[adset_id]: by_adset[adset_id].append(ad_id)
        if camp_name and ad_id not in by_campaign_name[camp_name]: by_campaign_name[camp_name].append(ad_id)
        if camp_id   and ad_id not in by_campaign_id[camp_id]:     by_campaign_id[camp_id].append(ad_id)

    # Promote rich -> by_id, build per-adset / per-campaign rich indexes
    adset_ads     = defaultdict(list)
    camp_name_ads = defaultdict(list)
    camp_id_ads   = defaultdict(list)
    for ad_id, m in rich.items():
        by_id[ad_id] = m
        if m["adset_id"]:      adset_ads[m["adset_id"]].append(m)
        if m["campaign_name"]: camp_name_ads[m["campaign_name"]].append(m)
        if m["campaign_id"]:   camp_id_ads[m["campaign_id"]].append(m)

    # Attach lifetime spend to every rich-meta record.
    for ad_id, m in rich.items():
        m["lifetime_spend"] = lifetime_spend.get(ad_id, 0.0)

    # Global name index for T2 substring + sep-normalized matching.
    # One entry per (ad_id, historical_name) pair, ordered by ad_name length
    # descending so longer (more specific) names are tested first.
    name_index = []   # list of (name_lower, name_sep, name_len, ad_id, spend)
    for ad_id, m in rich.items():
        sp = m.get("lifetime_spend", 0.0)
        for nm in m["names"]:
            nl = nm.lower()
            if not nl: continue
            name_index.append((nl, _sep_key(nm), len(nl), ad_id, sp))
    name_index.sort(key=lambda r: -r[2])  # longest first

    conn.close()
    log(f"[ok] ad universe loaded: {len(by_id):,} ads, "
        f"{len(by_name):,} unique names, {len(by_adset):,} adsets, "
        f"{len(by_campaign_name):,} campaign names, "
        f"{len(name_index):,} (ad,name) pairs for global substring")
    return (by_id, by_name, by_fuzzy, by_adset, by_campaign_name, by_campaign_id,
            adset_ads, camp_name_ads, camp_id_ads, name_index)


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
        # Customer link — surfaces buyer identity + loyalty in Ad
        # Intelligence.  numberOfOrders is Shopify's lifetime counter
        # as of the query, so it doubles as a repeat-buyer signal.
        # email lives on the Customer object (Order.contactEmail was
        # removed in 2025-10).
        customer { id numberOfOrders email }
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
def _scoped_match(ads, name_cand, order_date, scope):
    """Resolve a specific ad within an adset/campaign by NAME ONLY.

    Use case: T2 (global ad_name lookup) failed because the ad was RENAMED
    and the canonical name is no longer the utm string. We fall back to
    matching utm_content against the HISTORICAL names of ads within the
    matched adset / campaign.

    Date-based and asset-code-based guessing was removed (used to emit
    T3.2 / T3.3 / T4.2 / T4.3 — too unreliable). If we can't pin exactly
    one ad via name matching, we return (None, None) and the caller
    falls back to base T3 / T4 (scope known, no specific ad).

    Returns (ad_meta, "T3.1" | "T4.1") or (None, None).
    """
    if not ads: return (None, None)
    n_root = "T3" if scope == "adset" else "T4"
    nc  = (name_cand or "").strip()
    if not nc: return (None, None)
    nc_l = nc.lower()

    # exact (case-sensitive) against any historical name in scope
    for a in ads:
        if nc in a["names"]:
            return (a, f"{n_root}.1")

    # fuzzy: norm_name (suffix-stripped, whitespace-collapsed, lowercased)
    f = norm_name(nc)
    if f:
        fz_hits = [a for a in ads
                   if any(norm_name(n) == f for n in a["names"])]
        if len(fz_hits) == 1:
            return (fz_hits[0], f"{n_root}.1")

    # substring (case-insensitive, separator-strict)
    sub_hits = []
    for a in ads:
        for nm in a["names"]:
            nml = nm.lower()
            if nml and (nml in nc_l or nc_l in nml):
                sub_hits.append(a); break
    if len(sub_hits) == 1:
        return (sub_hits[0], f"{n_root}.1")

    # substring with separator normalization (treats +/-/_/space/./, as equivalent)
    # Catches utm "CLP-SDPL MU OFF-RS IHP" vs ad_name "CLP-SDPL+MU+OFF-RS+IHP+..."
    nc_sep = _sep_key(nc)
    if nc_sep:
        sep_hits = []
        for a in ads:
            for nm in a["names"]:
                nm_sep = _sep_key(nm)
                if nm_sep and (nm_sep in nc_sep or nc_sep in nm_sep):
                    sep_hits.append(a); break
        if len(sep_hits) == 1:
            return (sep_hits[0], f"{n_root}.1")

    # Multi-hit or no-hit cases: fall through to base T3 / T4 (no specific ad)
    return (None, None)


_ATTR_CACHE = {}   # (utm_content, utm_term, utm_campaign, attr_ad, attr_camp, attr_adset) -> result
def attribute_order_cache_clear():
    _ATTR_CACHE.clear()

def attribute_order(ca, maps, order_created_at=None):
    """Returns (ad_id, ad_name, adset_id, campaign_name, matched_value, matched_tier).
    Empty strings for the metadata fields when no match.
    Cascade:
      T1  utm_content is a numeric ad_id
      T2  utm_content matches an ad_name globally (exact / fuzzy / substring / sep-normalized substring)
      T3.1 utm_content matches an ad's HISTORICAL name within the adset (renamed-ad recovery)
      T3   adset_id known, no specific ad pinpointed
      T4.1/T4 same at campaign scope
    Date-based and asset-code guessing was removed.
    """
    (by_id, by_name, by_fuzzy, by_adset, by_camp_name, by_camp_id,
     adset_ads, camp_name_ads, camp_id_ads, name_index) = maps

    utm_content = (ca.get("utm_content")  or "").strip()
    utm_term    = (ca.get("utm_term")     or "").strip()
    utm_camp    = (ca.get("utm_campaign") or "").strip()
    # custom attribute aliases from our destination-URL template
    attr_ad_name   = (ca.get("Ad") or ca.get("ad") or "").strip()
    attr_camp_name = (ca.get("Campaign") or ca.get("campaign") or "").strip()
    attr_adset_id  = (ca.get("AdSetID") or ca.get("adset_id")
                      or ca.get("adset") or "").strip()

    # Result is deterministic from utm fields + custom attrs; cache for batch jobs.
    cache_key = (utm_content, utm_term, utm_camp, attr_ad_name, attr_camp_name, attr_adset_id)
    cached = _ATTR_CACHE.get(cache_key)
    if cached is not None: return cached

    order_date = _parse_iso(order_created_at) if order_created_at else None
    # Best name-like fragment for scoped matching
    name_cand = attr_ad_name or utm_content

    def _ret(r):
        _ATTR_CACHE[cache_key] = r
        return r

    # T1: utm_content is a numeric ad_id in our universe
    if utm_content.isdigit() and utm_content in by_id:
        m = by_id[utm_content]
        return _ret((m["ad_id"], m["ad_name"], m["adset_id"], m["campaign_name"],
                     utm_content, "T1_ad_id"))

    # T2: utm_content matches an ad_name globally — exact -> fuzzy -> substring -> sep-normalized
    for cand in (attr_ad_name, utm_content):
        if not cand: continue
        # exact
        if cand in by_name:
            ad_id = by_name[cand][0]; m = by_id[ad_id]
            return _ret((m["ad_id"], m["ad_name"], m["adset_id"], m["campaign_name"],
                         cand, "T2_ad_name"))
        # fuzzy (suffix-stripped + lowercased)
        f = norm_name(cand)
        if f and f in by_fuzzy:
            ad_id = by_fuzzy[f][0]; m = by_id[ad_id]
            return _ret((m["ad_id"], m["ad_name"], m["adset_id"], m["campaign_name"],
                         cand, "T2_ad_name"))

    # T2 global substring (and sep-normalized substring). The matched
    # substring (whichever side is shorter) must be at least 10 chars long —
    # this prevents generic short names like "ER" from matching by coincidence.
    # Tiebreak: pick the ad with HIGHEST lifetime spend (Meta actually
    # delivered it); name-length proximity is a secondary tiebreaker.
    for cand in (attr_ad_name, utm_content):
        if not cand or len(cand) < 10: continue
        cand_l   = cand.lower()
        cand_sep = _sep_key(cand)
        cand_l_len = len(cand_l)
        cand_sep_len = len(cand_sep)
        # Best so far — tuple (spend DESC, name_length_gap ASC). We track these
        # as separate vars to avoid building a list for non-matching iterations.
        best_aid = None; best_spend = -1.0; best_gap = 10**9
        for nl, nsep, nlen, ad_id, spend in name_index:
            matched = False
            if (cand_l in nl) or (nl in cand_l):
                if min(cand_l_len, nlen) >= 10:
                    matched = True
            if not matched and cand_sep and nsep:
                if (cand_sep in nsep) or (nsep in cand_sep):
                    if min(cand_sep_len, len(nsep)) >= 10:
                        matched = True
            if matched:
                gap = abs(nlen - cand_l_len)
                # Prefer higher spend; on equal spend prefer closer name length
                if (spend > best_spend) or (spend == best_spend and gap < best_gap):
                    best_aid = ad_id; best_spend = spend; best_gap = gap
        if best_aid is not None:
            m = by_id[best_aid]
            return _ret((m["ad_id"], m["ad_name"], m["adset_id"], m["campaign_name"],
                         cand, "T2_ad_name"))

    # ── T3.x : ADSET-SCOPED match  (utm_term -> adset; narrow within) ─────
    for adset_cand in (attr_adset_id, utm_term):
        if not adset_cand: continue
        ads = adset_ads.get(adset_cand)
        if not ads: continue
        a, tier = _scoped_match(ads, name_cand, order_date, "adset")
        if a:
            return _ret((a["ad_id"], a["ad_name"], a["adset_id"], a["campaign_name"],
                         name_cand or adset_cand, tier))
        # No narrowing -> base T3: adset known, no specific ad (spread downstream)
        first = ads[0]
        return _ret(("", first["ad_name"], first["adset_id"], first["campaign_name"],
                     adset_cand, "T3"))

    # ── T4.x : CAMPAIGN-SCOPED match  (utm_camp -> campaign; narrow within) ─
    camp_ads = None; matched_camp = ""; camp_tier_kind = ""
    if utm_camp.isdigit() and utm_camp in camp_id_ads:
        camp_ads = camp_id_ads[utm_camp]; matched_camp = utm_camp; camp_tier_kind = "id"
    elif attr_camp_name and attr_camp_name in camp_name_ads:
        camp_ads = camp_name_ads[attr_camp_name]; matched_camp = attr_camp_name; camp_tier_kind = "name"
    elif utm_camp and utm_camp in camp_name_ads:
        camp_ads = camp_name_ads[utm_camp]; matched_camp = utm_camp; camp_tier_kind = "name"

    if camp_ads:
        a, tier = _scoped_match(camp_ads, name_cand, order_date, "campaign")
        if a:
            return _ret((a["ad_id"], a["ad_name"], a["adset_id"], a["campaign_name"],
                         name_cand or matched_camp, tier))
        first = camp_ads[0]
        return _ret(("", "", first["adset_id"] if camp_tier_kind == "id" else "",
                     first["campaign_name"],
                     matched_camp, "T4"))

    return _ret(("", "", "", "", "", ""))


def build_row(node, maps):
    ca = {a["key"]: a["value"] for a in (node.get("customAttributes") or [])}
    ad_id, ad_name, adset_id, camp_name, matched_value, matched_tier = \
        attribute_order(ca, maps, order_created_at=node.get("createdAt"))

    items = [(e["node"].get("title") or "").replace(",", " ")
             for e in (node.get("lineItems", {}).get("edges") or [])]
    items_str = ", ".join(filter(None, items))[:2000]

    total = node.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount") or 0
    # gid://shopify/Customer/1234 → "1234"; keep as text so we round-trip
    # exactly what Shopify returned (some historical customers use very
    # large numeric IDs that overflow a Postgres int).
    cust = node.get("customer") or {}
    cust_gid = cust.get("id") or ""
    cust_id  = cust_gid.rsplit("/", 1)[-1] if cust_gid else None
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
        cust_id,
        int(cust.get("numberOfOrders")) if cust.get("numberOfOrders") is not None else None,
        cust.get("email"),
    )


INSERT_SQL = """
INSERT INTO shopify_ad_attribution
  (order_id, order_created_at, ordered_item, total_price,
   utm_campaign, utm_content, utm_medium, utm_source, utm_term,
   ad_id, ad_name, campaign_name, adset_id,
   has_match, matched_value, matched_tier,
   customer_id, customer_num_orders, contact_email)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
