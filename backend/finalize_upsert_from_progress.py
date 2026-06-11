"""finalize_upsert_from_progress.py — recover the final UPSERT after the
streaming attribution finished but the Supabase pooler dropped during the
Meta API fallback. Loads the on-disk progress JSON, joins to backfill_table
for ad metadata, upserts shopify_ad_attribution."""
import os, json, psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"].strip()
PROG = r"D:\Creative_Testing_Dashboard\backend\full_backfill.progress.json"

print("[load] progress json …")
j = json.load(open(PROG, encoding="utf-8"))
per_ad = j["per_ad"]
print(f"  {len(per_ad):,} ads with attribution data")

print("[connect] reopening DB …")
conn = psycopg2.connect(DB_URL); conn.autocommit = False
cur = conn.cursor()

print("[lookup] joining to backfill_table for ad metadata …")
ad_ids = list(per_ad.keys())
ads_meta = {}
for i in range(0, len(ad_ids), 1000):
    chunk = ad_ids[i:i+1000]
    cur.execute("""
      SELECT ad_id, MIN(ad_name), MIN(adset_id), MIN(adset_name), MIN(campaign_name)
      FROM backfill_table WHERE ad_id = ANY(%s) GROUP BY ad_id
    """, (chunk,))
    for ad_id, ad_name, adset_id, adset_name, camp in cur.fetchall():
        ads_meta[ad_id] = {"ad_name":ad_name or "", "adset_id":adset_id or "",
                           "adset_name":adset_name or "", "campaign_name":camp or ""}
print(f"  matched {len(ads_meta):,}/{len(ad_ids):,} ad_ids in backfill_table")

print("[upsert] writing into shopify_ad_attribution …")
UPSERT = """
INSERT INTO shopify_ad_attribution
  (ad_id, ad_name, adset_id, adset_name, campaign_name, orders, sales, last_synced_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
ON CONFLICT (ad_id) DO UPDATE SET
  ad_name=EXCLUDED.ad_name, adset_id=EXCLUDED.adset_id, adset_name=EXCLUDED.adset_name,
  campaign_name=EXCLUDED.campaign_name, orders=EXCLUDED.orders, sales=EXCLUDED.sales,
  last_synced_at=NOW()
"""
rows = []
for ad_id, d in per_ad.items():
    m = ads_meta.get(ad_id, {})
    rows.append((ad_id, m.get("ad_name"), m.get("adset_id") or None,
                 m.get("adset_name"), m.get("campaign_name"),
                 round(d["orders"], 4), round(d["sales"], 2)))
execute_batch(cur, UPSERT, rows, page_size=500)
conn.commit()
print(f"  upserted {len(rows):,} rows")

cur.execute("SELECT COUNT(*), ROUND(SUM(orders),0), ROUND(SUM(sales),0), MAX(last_synced_at) FROM shopify_ad_attribution")
n, o, s, ts = cur.fetchone()
print(f"\n==================== DONE ====================")
print(f"shopify_ad_attribution rows : {n:,}")
print(f"sum(orders)                  : {o:,.0f}")
print(f"sum(sales)                   : Rs.{s:,.0f}")
print(f"max(last_synced_at)          : {ts}")
conn.close()

try: os.remove(PROG); print("[cleanup] progress file removed")
except: pass
