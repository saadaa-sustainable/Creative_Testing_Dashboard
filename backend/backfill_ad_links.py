"""
One-shot: refresh ad_link for every ad_id in primary_table using the new
_extract_ad_link() that mirrors Meta's "Link (ad settings)" column.

Reads distinct ad_ids in batches of 25, calls Graph /<ids>?fields=creative{...},
then bulk-UPDATEs primary_table.ad_link. Idempotent — safe to re-run.
"""
import os, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

import psycopg2, psycopg2.extras
from dotenv import load_dotenv

# Re-use the patched extractor and the API helper from primary_sync
from primary_sync import _extract_ad_link, _get, BASE_URL, ACCESS_TOKEN

load_dotenv()
DB = os.environ["SUPABASE_DB_URL"].strip()

FIELDS = (
    "creative{link_url,object_url,asset_feed_spec,"
    "object_story_spec{link_data{link},"
    "video_data{call_to_action},template_data{link}}}"
)


def fetch_links(ad_ids: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for i in range(0, len(ad_ids), 25):
        chunk = ad_ids[i : i + 25]
        try:
            data = _get(
                f"{BASE_URL}/",
                {"ids": ",".join(chunk), "fields": FIELDS, "access_token": ACCESS_TOKEN},
            )
        except Exception as e:
            print(f"  [chunk {i}] API error: {e}")
            continue
        for ad_id, info in data.items():
            if not isinstance(info, dict):
                continue
            link = _extract_ad_link(info.get("creative") or {})
            if link:
                out[ad_id] = link
        if (i // 25) % 10 == 9:
            print(f"  fetched {i + len(chunk):,}/{len(ad_ids):,}")
    return out


def main():
    conn = psycopg2.connect(DB, connect_timeout=30)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")

    print("Loading distinct ad_ids from primary_table…")
    cur.execute("SELECT DISTINCT ad_id FROM primary_table WHERE ad_id IS NOT NULL")
    ad_ids = [r[0] for r in cur.fetchall()]
    print(f"  {len(ad_ids):,} unique ad_ids")

    # Snapshot current state for comparison
    cur.execute(
        "SELECT COUNT(*) FILTER (WHERE ad_link IS NOT NULL AND ad_link != ''), COUNT(*) "
        "FROM primary_table WHERE ad_id IS NOT NULL"
    )
    before_filled, total = cur.fetchone()
    print(f"  before: {before_filled:,}/{total:,} rows have a non-empty ad_link")

    t0 = time.time()
    links = fetch_links(ad_ids)
    print(f"  resolved links for {len(links):,}/{len(ad_ids):,} ads in {time.time()-t0:.0f}s")

    if not links:
        print("Nothing to update. Done.")
        cur.close(); conn.close(); return

    print("Bulk-updating primary_table.ad_link …")
    payload = [(link, ad_id) for ad_id, link in links.items()]
    psycopg2.extras.execute_batch(
        cur,
        "UPDATE primary_table SET ad_link = %s WHERE ad_id = %s",
        payload,
        page_size=500,
    )
    conn.commit()

    cur.execute(
        "SELECT COUNT(*) FILTER (WHERE ad_link IS NOT NULL AND ad_link != ''), COUNT(*) "
        "FROM primary_table WHERE ad_id IS NOT NULL"
    )
    after_filled, total = cur.fetchone()
    print(f"  after:  {after_filled:,}/{total:,} rows have a non-empty ad_link")
    print(f"  delta:  +{after_filled - before_filled:,} rows")

    cur.close(); conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
