"""
refresh_ad_day_from_meta.py - refresh a single ad's daily insights for a
specific date (or date range) directly from Meta Ads Manager, then UPDATE
both primary_table and backfill_table.

Usage:
    python refresh_ad_day_from_meta.py SIF-1466 2026-05-30
    python refresh_ad_day_from_meta.py SIF-1466 2026-05-01 2026-06-05

The ad query is a substring match on ad_name.
"""
import os, sys, time
import requests, psycopg2
from dotenv import load_dotenv

load_dotenv()
META_TOKEN = os.environ["META_ACCESS_TOKEN"].strip()
META_API_VER = os.getenv("META_API_VERSION", "v21.0").strip()
DB_URL = os.environ["SUPABASE_DB_URL"].strip()

FIELDS = "ad_id,ad_name,impressions,reach,frequency,spend,date_start"

def meta_get(url, params, retries=3):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504) and i < retries-1:
            sleep_s = 20 * (i+1)
            print(f"  [retry {i+1}] HTTP {r.status_code} sleeping {sleep_s}s")
            time.sleep(sleep_s); continue
        if r.status_code == 403 and i < retries-1:
            sleep_s = 60 * (i+1)
            print(f"  [retry {i+1}] HTTP 403 sleeping {sleep_s}s")
            time.sleep(sleep_s); continue
        print(f"  [err] HTTP {r.status_code}: {r.text[:300]}")
        r.raise_for_status()
    raise RuntimeError("Meta API failed after retries")

def main():
    if len(sys.argv) < 3:
        print("usage: refresh_ad_day_from_meta.py <ad_name_substr> <from_date> [to_date]")
        return 1
    name_substr = sys.argv[1]
    from_d = sys.argv[2]
    to_d   = sys.argv[3] if len(sys.argv) > 3 else from_d

    conn = psycopg2.connect(DB_URL); conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
      SELECT DISTINCT ad_id, ad_name, account_name
      FROM backfill_table
      WHERE ad_name ILIKE %s
    """, (f"%{name_substr}%",))
    ads = cur.fetchall()
    if not ads:
        print(f"[err] No ad matched '{name_substr}'"); return 1
    print(f"[info] {len(ads)} ad(s) matched:")
    for a in ads: print(f"   - {a[1]} ({a[0]}) :: {a[2]}")

    # Account_id lookup from .env
    acct_id_by_name = {
        os.getenv("ACCOUNT_1_NAME","Raho Saadaa"): os.getenv("ACCOUNT_1_ID"),
        os.getenv("ACCOUNT_2_NAME","Fourth Ad Account - SD"): os.getenv("ACCOUNT_2_ID"),
        os.getenv("ACCOUNT_3_NAME","Third Ad Account - SD"): os.getenv("ACCOUNT_3_ID"),
    }

    upserts = []   # (account_name, ad_id, date, impressions, reach, frequency, spend)
    for ad_id, ad_name, acct_name in ads:
        acct_id = acct_id_by_name.get(acct_name)
        if not acct_id:
            print(f"[warn] No account_id for '{acct_name}', skipping"); continue
        url = f"https://graph.facebook.com/{META_API_VER}/{ad_id}/insights"
        params = {
            "fields": FIELDS,
            "time_range": f'{{"since":"{from_d}","until":"{to_d}"}}',
            "time_increment": 1,
            "access_token": META_TOKEN,
            "limit": 500,
        }
        print(f"\n[fetch] {ad_name}  {from_d} -> {to_d}")
        j = meta_get(url, params)
        data = j.get("data", [])
        print(f"  [ok] {len(data)} daily rows returned by Meta")
        for r in data:
            print(f"    {r.get('date_start')}  impr={r.get('impressions')}  reach={r.get('reach')}  freq={r.get('frequency')}  spend={r.get('spend')}")
            upserts.append((
                acct_name, ad_id, r.get("date_start"),
                int(r.get("impressions", 0) or 0),
                int(r.get("reach", 0) or 0),
                round(float(r.get("frequency", 0) or 0), 4),
                float(r.get("spend", 0) or 0),
            ))

    # UPDATE primary_table where row exists
    print(f"\n[run] Updating primary_table with {len(upserts)} freshly-fetched rows ...")
    cur.executemany("""
      UPDATE primary_table
      SET impressions = %s, reach = %s, frequency = %s, amount_spent_inr = %s
      WHERE account_name = %s AND ad_id = %s AND date = %s
    """, [(u[3], u[4], u[5], u[6], u[0], u[1], u[2]) for u in upserts])
    print(f"  [ok] primary_table updates issued: {cur.rowcount}")

    # UPDATE backfill_table where row exists
    print(f"[run] Updating backfill_table with the same {len(upserts)} rows ...")
    cur.executemany("""
      UPDATE backfill_table
      SET impressions = %s, reach = %s, frequency = %s, amount_spent_inr = %s
      WHERE account_name = %s AND ad_id = %s AND date = %s
    """, [(u[3], u[4], u[5], u[6], u[0], u[1], u[2]) for u in upserts])
    print(f"  [ok] backfill_table updates issued: {cur.rowcount}")

    conn.commit(); print("[commit]")

    # Re-show the updated rows from DB
    for ad_id, ad_name, acct_name in ads:
        cur.execute("""
          SELECT date, impressions, reach, frequency, amount_spent_inr
          FROM backfill_table
          WHERE ad_id = %s AND date BETWEEN %s AND %s
          ORDER BY date
        """, (ad_id, from_d, to_d))
        print(f"\n[verify] backfill_table rows for {ad_name}:")
        for row in cur.fetchall():
            print(f"   {row}")

    cur.close(); conn.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
