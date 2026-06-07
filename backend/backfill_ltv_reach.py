"""
backfill_ltv_reach.py - patch missing ltv_reach in backfill_table by
fetching lifetime stats from Meta in 50-ad batches per account.

Token-safe: META_ACCESS_TOKEN via dotenv, never echoed, never logged.

For each ad_id with ltv_reach=0 or NULL in backfill_table:
  call act_<account_id>/insights with level=ad, date_preset=maximum,
  filter by ad.id IN (chunk of up to 50). Update backfill_table for
  every (account_name, ad_id) returned.
"""
import os, sys, time, json, re
import requests, psycopg2
from collections import defaultdict
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()
DB_URL       = os.environ["SUPABASE_DB_URL"].strip()
META_TOKEN   = os.environ["META_ACCESS_TOKEN"].strip()
META_API_VER = os.getenv("META_API_VERSION", "v21.0").strip()

ACCOUNTS = {
    os.getenv("ACCOUNT_1_NAME","Raho Saadaa"):            os.getenv("ACCOUNT_1_ID"),
    os.getenv("ACCOUNT_2_NAME","Fourth Ad Account - SD"): os.getenv("ACCOUNT_2_ID"),
    os.getenv("ACCOUNT_3_NAME","Third Ad Account - SD"):  os.getenv("ACCOUNT_3_ID"),
}

LOG = "backfill_ltv_reach.log"
TOKEN_RE = re.compile(r"(?:EAA[a-zA-Z0-9]{30,}|shpa_[a-zA-Z0-9]{20,})")
def _scrub(s):
    if s is None: return s
    try: return TOKEN_RE.sub("<REDACTED>", str(s))
    except: return "<scrub_err>"
def log(*a):
    msg = " ".join(_scrub(x) for x in a)
    print(msg, flush=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(msg + "\n")

# ── Find ads needing backfill ────────────────────────────────────────────────
conn = psycopg2.connect(DB_URL); conn.autocommit = False
cur = conn.cursor()
log("[scan] finding ads with ltv_reach=0 but impressions>0 ...")
cur.execute("""
  SELECT account_name, ad_id
  FROM (
    SELECT account_name, ad_id,
           MAX(COALESCE(ltv_reach,0)) AS ltv,
           SUM(impressions) AS impr
    FROM backfill_table
    WHERE ad_id IS NOT NULL AND ad_id <> ''
    GROUP BY account_name, ad_id
  ) t
  WHERE ltv = 0 AND impr > 0
""")
needs = defaultdict(list)
for acct, ad_id in cur.fetchall():
    needs[acct].append(ad_id)
total = sum(len(v) for v in needs.values())
log(f"  {total:,} ads need ltv_reach across {len(needs)} accounts")
for acct, ids in needs.items():
    log(f"    {acct}: {len(ids):,} ads")

# ── Fetch in 50-ad batches per account ───────────────────────────────────────
updates = []      # (ltv_reach, ltv_frequency, account_name, ad_id)
skipped = 0
t0 = time.time()
for acct_name, ad_ids in needs.items():
    acct_id = ACCOUNTS.get(acct_name)
    if not acct_id:
        log(f"  [skip] no env account_id for {acct_name}"); continue
    log(f"\n[fetch] {acct_name}: {len(ad_ids):,} ads -> {(len(ad_ids)+49)//50} batches")
    for i in range(0, len(ad_ids), 50):
        chunk = ad_ids[i:i+50]
        try:
            r = requests.get(
                f"https://graph.facebook.com/{META_API_VER}/act_{acct_id}/insights",
                params={
                    "level": "ad",
                    "fields": "ad_id,reach,frequency",
                    "date_preset": "maximum",
                    "filtering": json.dumps([{"field":"ad.id","operator":"IN","value":chunk}]),
                    "access_token": META_TOKEN,
                    "limit": 500,
                },
                timeout=120,
            )
        except Exception as e:
            log(f"  [err] batch {i//50+1}: {type(e).__name__}"); skipped += len(chunk); continue
        if r.status_code != 200:
            log(f"  [err] batch {i//50+1}: HTTP {r.status_code}  {_scrub(r.text[:200])}")
            # Backoff on 429/5xx
            if r.status_code in (429, 502, 503, 504): time.sleep(30)
            skipped += len(chunk); continue
        rows = r.json().get("data", [])
        for row in rows:
            aid = row.get("ad_id")
            if not aid: continue
            reach = float(row.get("reach") or 0)
            freq  = float(row.get("frequency") or 0)
            if reach <= 0: continue
            updates.append((reach, freq, acct_name, aid))
        if (i//50+1) % 10 == 0:
            log(f"  batch {i//50+1}/{(len(ad_ids)+49)//50}  updates so far: {len(updates):,}")
        time.sleep(0.4)
    log(f"  done. updates pending: {len(updates):,}")

elapsed = time.time() - t0
log(f"\n[meta] {len(updates):,} updates collected in {elapsed:.1f}s; skipped {skipped}")

# ── Apply UPDATEs ────────────────────────────────────────────────────────────
log(f"\n[update] applying ltv_reach + ltv_frequency to backfill_table ...")
execute_batch(cur, """
  UPDATE backfill_table
  SET ltv_reach = %s,
      ltv_frequency = %s
  WHERE account_name = %s AND ad_id = %s
""", updates, page_size=500)
conn.commit()
log(f"  committed {len(updates):,} updates")

# ── Confirm ──────────────────────────────────────────────────────────────────
cur.execute("""
  SELECT
    COUNT(DISTINCT (account_name, ad_id)) AS total,
    COUNT(DISTINCT (account_name, ad_id)) FILTER (WHERE COALESCE(ltv_reach,0) = 0) AS still_zero
  FROM backfill_table
  WHERE ad_id IS NOT NULL AND ad_id <> ''
""")
t, sz = cur.fetchone()
log(f"\n[after] {t:,} distinct (account, ad_id); still zero ltv_reach: {sz:,}")
conn.close()
log("\n[done] re-run refresh_ae_table.py next to propagate into ae_table_view.")
