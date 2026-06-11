"""
backfill_ad_links_backfill_table.py - same Meta API ad_link fetch, but
targeting backfill_table (the lifetime store). Ensures pre-2026 ads
without a primary_table row also get an ad_link.

1) ALTER TABLE backfill_table ADD COLUMN IF NOT EXISTS ad_link TEXT
2) SELECT DISTINCT ad_id FROM backfill_table WHERE ad_link IS NULL OR ad_link=''
3) Fetch creative.link via Meta /?ids= in 25-ad batches
4) Bulk UPDATE backfill_table

Token safety: META_ACCESS_TOKEN via dotenv only, never echoed/logged.
"""
import os, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

import psycopg2, psycopg2.extras
from dotenv import load_dotenv
from primary_sync import _extract_ad_link, _get, BASE_URL, ACCESS_TOKEN

load_dotenv()
DB = os.environ["SUPABASE_DB_URL"].strip()

FIELDS = (
    "creative{link_url,object_url,asset_feed_spec,"
    "object_story_spec{link_data{link},"
    "video_data{call_to_action},template_data{link}}}"
)

def fetch_links(ad_ids):
    out = {}
    for i in range(0, len(ad_ids), 25):
        chunk = ad_ids[i:i+25]
        try:
            data = _get(f"{BASE_URL}/", {"ids": ",".join(chunk),
                                         "fields": FIELDS, "access_token": ACCESS_TOKEN})
        except Exception as e:
            print(f"  [chunk {i//25}] API error: {type(e).__name__}")
            continue
        for ad_id, info in data.items():
            if not isinstance(info, dict): continue
            link = _extract_ad_link(info.get("creative") or {})
            if link: out[ad_id] = link
        if (i // 25) % 20 == 19:
            print(f"  fetched batch {i//25 + 1}: total resolved so far = {len(out):,}")
    return out

def main():
    conn = psycopg2.connect(DB, connect_timeout=30); conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")

    print("[1] ALTER TABLE backfill_table ADD COLUMN IF NOT EXISTS ad_link TEXT")
    cur.execute("ALTER TABLE backfill_table ADD COLUMN IF NOT EXISTS ad_link TEXT")
    conn.commit()

    print("\n[2] Loading distinct ad_ids needing ad_link …")
    cur.execute("""
        SELECT DISTINCT ad_id FROM backfill_table
        WHERE ad_id IS NOT NULL AND ad_id <> ''
          AND (ad_link IS NULL OR ad_link = '')
    """)
    ad_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT COUNT(DISTINCT ad_id) FROM backfill_table WHERE ad_id IS NOT NULL AND ad_id <> ''")
    total = cur.fetchone()[0]
    print(f"  {len(ad_ids):,} ads need ad_link  (of {total:,} total)")

    if not ad_ids:
        print("Nothing to do."); cur.close(); conn.close(); return

    t0 = time.time()
    links = fetch_links(ad_ids)
    print(f"\n[3] Resolved links for {len(links):,}/{len(ad_ids):,} ads in {time.time()-t0:.0f}s")

    if not links:
        print("Nothing to UPDATE."); cur.close(); conn.close(); return

    print("\n[4] Bulk-UPDATEing backfill_table.ad_link …")
    payload = [(link, ad_id) for ad_id, link in links.items()]
    psycopg2.extras.execute_batch(
        cur,
        "UPDATE backfill_table SET ad_link = %s WHERE ad_id = %s",
        payload, page_size=500,
    )
    conn.commit()

    cur.execute("""
        SELECT COUNT(DISTINCT ad_id) FILTER (WHERE ad_link IS NOT NULL AND ad_link <> ''),
               COUNT(DISTINCT ad_id)
        FROM backfill_table WHERE ad_id IS NOT NULL AND ad_id <> ''
    """)
    filled, total = cur.fetchone()
    print(f"\n[after] backfill_table: {filled:,}/{total:,} distinct ads have ad_link "
          f"({filled/total*100:.1f}%)")

    cur.close(); conn.close()
    print("\n[done] Re-run refresh_ae_table.py next to propagate into ae_raw_view.")

if __name__ == "__main__":
    main()
