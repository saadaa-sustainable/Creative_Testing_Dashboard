"""
sync_shopify_customers.py — mirror the Shopify customer directory into
Saada_Shopify_Data.public.customers.

Pulls every customer via Admin GraphQL (paginated cursor, 100/page) and
upserts each into the target table by numeric id. Idempotent — safe to
re-run at any cadence; every row's synced_at bumps on write.

USAGE
  python sync_shopify_customers.py                # full sync
  python sync_shopify_customers.py --since 2026-06-01
                                                  # only customers whose
                                                  # updated_at >= that date
  python sync_shopify_customers.py --limit 500    # cap for testing
  python sync_shopify_customers.py --dry-run      # fetch, don't write

Env  (backend/.env)
  SHOP_DOMAIN            saadaa-design.myshopify.com
  ADMIN_ACCESS_TOKEN     shpat_...
  SHOPIFY_API_VERSION    2025-10   (default)
  SHOPIFY_DATA_DB_URL    pooler URL for Saada_Shopify_Data
  SHOPIFY_DATA_URL /
  SHOPIFY_DATA_ANON      REST fallback when SHOPIFY_DATA_DB_URL isn't set
                         (same anon key pattern the sessions sync uses)
"""
from __future__ import annotations
import os, sys, re, json, time, argparse, requests
from datetime import datetime, timezone
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

# Keep Windows awake — this can run for 10+ minutes on a large catalogue.
try:
    import ctypes
    _FLAGS = 0x80000000 | 0x00000001 | 0x00000040
    ctypes.windll.kernel32.SetThreadExecutionState(_FLAGS)
except Exception:
    pass

load_dotenv(override=True)

SHOP_DOMAIN = (os.environ.get("SHOP_DOMAIN") or "saadaa-design.myshopify.com").strip()
SHOP_TOKEN  = (os.environ.get("ADMIN_ACCESS_TOKEN") or "").strip()
API_VER     = (os.environ.get("SHOPIFY_API_VERSION") or "2025-10").strip()
DB_URL      = (os.environ.get("SHOPIFY_DATA_DB_URL") or "").strip()
SD_URL      = (os.environ.get("SHOPIFY_DATA_URL")
               or "https://siymyhhrpzzbowfqtauf.supabase.co").rstrip("/")
SD_KEY      = (os.environ.get("SHOPIFY_DATA_ANON") or "").strip() or (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNpeW15aGhy"
    "cHp6Ym93ZnF0YXVmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzgzMTQwODEsImV4cCI6MjA5Mzg5MDA4M"
    "X0.ocx4jlY3KeXdF_-5JI3_SDcLekmk8hrfWXba7EXEDgo"
)

if not SHOP_TOKEN:
    raise SystemExit("Missing ADMIN_ACCESS_TOKEN in .env")

GRAPHQL = f"https://{SHOP_DOMAIN}/admin/api/{API_VER}/graphql.json"
HDRS    = {"X-Shopify-Access-Token": SHOP_TOKEN, "Content-Type": "application/json"}

# Redact secrets that could leak in error text.
_TOK_RE = re.compile(r"(?:shpat_[A-Za-z0-9]{20,}|shpca_[A-Za-z0-9]{20,}|eyJ[\w\-.]{40,})")
def scrub(s): return _TOK_RE.sub("<REDACTED>", str(s or ""))
def say(m):   print(scrub(str(m)), flush=True)

# ─── GraphQL query ────────────────────────────────────────────────────
QUERY = """
query CustomersPage($after: String, $query: String) {
  customers(first: 100, after: $after, query: $query, sortKey: UPDATED_AT) {
    edges {
      cursor
      node {
        id
        email
        phone
        firstName
        lastName
        displayName
        verifiedEmail
        state
        tags
        note
        locale
        taxExempt
        numberOfOrders
        amountSpent { amount currencyCode }
        lastOrder { id name }
        defaultAddress {
          address1 address2 city province provinceCode
          country  countryCodeV2 zip company name phone
        }
        addresses(first: 25) {
          address1 address2 city province provinceCode
          country  countryCodeV2 zip company name phone
        }
        emailMarketingConsent { marketingState marketingOptInLevel consentUpdatedAt }
        smsMarketingConsent   { marketingState marketingOptInLevel consentCollectedFrom consentUpdatedAt }
        createdAt
        updatedAt
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

def _num_id(gid: str) -> int:
    """gid://shopify/Customer/1234567890 → 1234567890"""
    if not gid: return 0
    m = re.search(r"(\d+)$", gid)
    return int(m.group(1)) if m else 0

def _addr(a):
    if not a: return None
    return {k: v for k, v in a.items() if v is not None}

def _fetch(since_updated: str | None, limit: int | None,
            batch_sink=None, batch_size: int = 500):
    """Iterate customers and either return the full list (batch_sink=None)
    or hand them to batch_sink in chunks of batch_size (streaming path).
    Streaming keeps memory bounded on ~400k+ catalogues and surfaces
    progress in the DB as rows land."""
    out = []
    cursor = None
    page = 0
    query = f"updated_at:>={since_updated}" if since_updated else None
    t0 = time.time()
    total_fetched = 0
    total_upserted = 0
    while True:
        page += 1
        r = requests.post(GRAPHQL, headers=HDRS,
                          json={"query": QUERY, "variables":
                                {"after": cursor, "query": query}},
                          timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Shopify HTTP {r.status_code}: {scrub(r.text)[:300]}")
        j = r.json()
        if "errors" in j:
            raise RuntimeError(f"GraphQL errors: {scrub(json.dumps(j['errors']))[:400]}")
        cus = (j.get("data") or {}).get("customers") or {}
        edges = cus.get("edges") or []
        for e in edges:
            n = e.get("node") or {}
            amt = n.get("amountSpent") or {}
            last = n.get("lastOrder") or {}
            row = {
                "id":                _num_id(n.get("id") or ""),
                "email":             n.get("email"),
                "phone":             n.get("phone"),
                "first_name":        n.get("firstName"),
                "last_name":         n.get("lastName"),
                "display_name":      n.get("displayName"),
                "verified_email":    n.get("verifiedEmail"),
                "state":             n.get("state"),
                "tags":              n.get("tags") or [],
                "note":              n.get("note"),
                "locale":            n.get("locale"),
                "tax_exempt":        n.get("taxExempt"),
                "number_of_orders":  int(n.get("numberOfOrders") or 0),
                "amount_spent_inr":  float(amt.get("amount")) if amt.get("amount") else None,
                "currency_code":     amt.get("currencyCode"),
                "last_order_id":     _num_id(last.get("id") or "") or None,
                "last_order_name":   last.get("name"),
                "default_address_json": json.dumps(_addr(n.get("defaultAddress"))) if n.get("defaultAddress") else None,
                "addresses_json":       json.dumps([_addr(a) for a in (n.get("addresses") or [])]),
                "email_marketing_consent": json.dumps(n.get("emailMarketingConsent")) if n.get("emailMarketingConsent") else None,
                "sms_marketing_consent":   json.dumps(n.get("smsMarketingConsent"))   if n.get("smsMarketingConsent")   else None,
                "created_at_shop":   n.get("createdAt"),
                "updated_at_shop":   n.get("updatedAt"),
            }
            if row["id"]:
                out.append(row)
                total_fetched += 1
                if limit and total_fetched >= limit: break
        # Flush every batch_size customers when streaming so the DB sees
        # progress and memory stays bounded.
        if batch_sink and len(out) >= batch_size:
            n = batch_sink(out)
            total_upserted += (n or len(out))
            out = []
        if limit and total_fetched >= limit:
            say(f"[hit --limit {limit}] stopping pagination"); break
        pi = cus.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        if page % 10 == 0 or page < 5:
            say(f"    page {page:4d}  fetched {total_fetched:,}"
                + (f"  upserted {total_upserted:,}" if batch_sink else "")
                + (f"  ({since_updated}+)" if since_updated else ""))
        # Shopify GraphQL is throttled by cost buckets — sleep 0.25s
        # between pages so we stay well under the limit.
        time.sleep(0.25)
    # Flush the final partial batch
    if batch_sink and out:
        n = batch_sink(out)
        total_upserted += (n or len(out))
        out = []
    dt = time.time() - t0
    if batch_sink:
        say(f"[ok] streamed {total_fetched:,} customers in {dt:.1f}s "
            f"({page} pages)  ·  {total_upserted:,} upserted")
        return None
    say(f"[ok] fetched {total_fetched:,} customers in {dt:.1f}s ({page} pages)")
    return out

# ─── DB write paths ───────────────────────────────────────────────────
COLS = [
    "id","email","phone","first_name","last_name","display_name",
    "verified_email","state","tags","note","locale","tax_exempt",
    "number_of_orders","amount_spent_inr","currency_code",
    "last_order_id","last_order_name",
    "default_address_json","addresses_json",
    "email_marketing_consent","sms_marketing_consent",
    "created_at_shop","updated_at_shop",
]

def _upsert_direct(rows):
    """Direct psycopg2 upsert (used when SHOPIFY_DATA_DB_URL is set)."""
    import psycopg2
    from psycopg2.extras import execute_values
    sql = f"""
    INSERT INTO public.customers ({", ".join(COLS)})
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
      email                    = EXCLUDED.email,
      phone                    = EXCLUDED.phone,
      first_name               = EXCLUDED.first_name,
      last_name                = EXCLUDED.last_name,
      display_name             = EXCLUDED.display_name,
      verified_email           = EXCLUDED.verified_email,
      state                    = EXCLUDED.state,
      tags                     = EXCLUDED.tags,
      note                     = EXCLUDED.note,
      locale                   = EXCLUDED.locale,
      tax_exempt               = EXCLUDED.tax_exempt,
      number_of_orders         = EXCLUDED.number_of_orders,
      amount_spent_inr         = EXCLUDED.amount_spent_inr,
      currency_code            = EXCLUDED.currency_code,
      last_order_id            = EXCLUDED.last_order_id,
      last_order_name          = EXCLUDED.last_order_name,
      default_address_json     = EXCLUDED.default_address_json,
      addresses_json           = EXCLUDED.addresses_json,
      email_marketing_consent  = EXCLUDED.email_marketing_consent,
      sms_marketing_consent    = EXCLUDED.sms_marketing_consent,
      created_at_shop          = EXCLUDED.created_at_shop,
      updated_at_shop          = EXCLUDED.updated_at_shop,
      synced_at                = NOW();
    """
    tuples = [tuple(r.get(c) for c in COLS) for r in rows]
    conn = psycopg2.connect(DB_URL, connect_timeout=15)
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, tuples, page_size=500)
        conn.commit()
    finally:
        conn.close()

def _upsert_rest(rows):
    """PostgREST upsert (fallback when SHOPIFY_DATA_DB_URL is missing).
    Same anon-key path used by the sessions + orders REST syncs."""
    hdrs = {"apikey": SD_KEY, "Authorization": f"Bearer {SD_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal"}
    url = f"{SD_URL}/rest/v1/customers?on_conflict=id"
    BATCH = 500
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i+BATCH]
        delay = 3
        for attempt in range(1, 6):
            r = requests.post(url, headers=hdrs, data=json.dumps(chunk), timeout=90)
            if r.status_code in (200, 201, 204): break
            if r.status_code >= 500 or r.status_code in (408, 429):
                say(f"  [retry {attempt}] HTTP {r.status_code} — sleeping {delay}s")
                time.sleep(delay); delay = min(delay * 2, 60); continue
            raise RuntimeError(f"REST upsert HTTP {r.status_code}: {scrub(r.text)[:300]}")
        else:
            raise RuntimeError("REST upsert exhausted retries")

def _upsert(rows):
    if not rows: say("[!] nothing to upsert"); return 0
    if DB_URL:
        _upsert_direct(rows); mode = "direct DB"
    else:
        _upsert_rest(rows);   mode = "PostgREST"
    say(f"[ok] upserted {len(rows):,} customers via {mode}")
    return len(rows)

# ─── Main ─────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="YYYY-MM-DD — only customers updated on/after this date")
    ap.add_argument("--limit", type=int, help="cap the row count (for smoke tests)")
    ap.add_argument("--dry-run", action="store_true", help="fetch but skip DB write")
    args = ap.parse_args()

    say(f"[*] shop={SHOP_DOMAIN}   api={API_VER}   token=…{SHOP_TOKEN[-6:]}")
    say(f"[*] target = public.customers ({'direct DB' if DB_URL else 'PostgREST'})")
    if args.since: say(f"[*] filter: updated_at >= {args.since}")
    if args.limit: say(f"[*] limit:  {args.limit}")

    if args.dry_run:
        # Old behaviour — accumulate into memory + summarise
        rows = _fetch(args.since, args.limit)
        say(f"[dry-run] would upsert {len(rows):,} rows — skipping DB write")
        return
    # Streaming path — upserts every 500 rows so a 400k+ catalogue can
    # complete without buffering the whole thing in memory.
    _fetch(args.since, args.limit, batch_sink=_upsert, batch_size=500)

if __name__ == "__main__":
    main()
