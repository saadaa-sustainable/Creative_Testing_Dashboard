"""
fetch_ad_thumbnails.py — populate ad_thumbnails table with Meta Graph thumbnails

For every unique ad_id in ae_table_view, fetch
   GET /{ad_id}?fields=creative{thumbnail_url,image_url,object_type,video_id}
and upsert the URL into ad_thumbnails so the dashboard can render a small
preview image next to each row.

USAGE
  python fetch_ad_thumbnails.py                # incremental — skip ads
                                               # fetched in the last 7 days
  python fetch_ad_thumbnails.py --limit 50     # only process the first 50
                                               # (sorted by descending spend)
  python fetch_ad_thumbnails.py --refresh-all  # re-fetch every ad regardless
  python fetch_ad_thumbnails.py --ad-id 1234   # one-shot single ad probe

SAFETY
  • Read-only on every source table.
  • Token never written to disk; logs are scrubbed.
  • Sleeps 0.25 s between requests and obeys Meta's X-App-Usage header
    (sleeps 300 s when any bucket ≥75 %).
  • Resumable — already-fetched ads are skipped, so interruption is safe.
"""
import os, sys, re, time, argparse, json, requests, psycopg2
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

TOK = (os.environ.get("META_ACCESS_TOKEN")
       or os.environ.get("META_acces_token")
       or os.environ.get("IG_ACCESS_TOKEN") or "").strip()
if not TOK: raise SystemExit("Missing META_ACCESS_TOKEN in .env")

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: raise SystemExit("Missing SUPABASE_DB_URL in .env")

VER   = "v22.0"
GRAPH = f"https://graph.facebook.com/{VER}"
FIELDS = "creative{id,thumbnail_url,image_url,object_type,video_id,object_story_spec}"
SLEEP_BETWEEN = 0.25
REFRESH_DAYS  = 7

_RE = re.compile(r"(?:EAA[A-Za-z0-9]{30,}|IGQ[\w\-]{20,}|eyJ[\w\-.]{40,})")
def scrub(s):
    s = _RE.sub("<REDACTED>", str(s or ""))
    return re.sub(r"(access_token=)[^&\s\"]+", r"\1<REDACTED>", s)

def parse_usage_header(headers):
    """Return the max % across all reported buckets, or 0."""
    raw = headers.get("X-App-Usage") or headers.get("X-Business-Use-Case-Usage") or ""
    if not raw: return 0
    try: obj = json.loads(raw)
    except Exception: return 0
    def walk(v):
        if isinstance(v, dict):
            for k, x in v.items(): yield from walk(x)
        elif isinstance(v, list):
            for x in v: yield from walk(x)
        else:
            if isinstance(v, (int, float)) and 0 <= v <= 100: yield v
    nums = list(walk(obj))
    return max(nums) if nums else 0

def fetch(ad_id):
    """One GET. Returns (record_or_None, error_or_None, usage_pct)."""
    try:
        r = requests.get(f"{GRAPH}/{ad_id}",
                         params={"fields": FIELDS, "access_token": TOK},
                         timeout=30)
    except Exception as e:
        return None, f"NET_{type(e).__name__}", 0
    usage = parse_usage_header(r.headers)
    try: body = r.json()
    except Exception: body = {"_raw": r.text[:300]}
    if r.status_code != 200:
        err = (body.get("error") or {}).get("message", "")
        return None, f"HTTP_{r.status_code}: {scrub(err)[:160]}", usage
    cr = body.get("creative") or {}
    return {
        # Sharper preview source (used in the dashboard's drawer at ~500px wide):
        # 1. creative.image_url             — direct full-res image asset
        # 2. object_story_spec.link_data.image_url   — link-ad sharp image
        # 3. object_story_spec.video_data.image_url  — video-ad poster frame
        # Falls back to the small thumbnail_url everywhere else.
        "thumbnail_url": cr.get("thumbnail_url"),
        "image_url":     cr.get("image_url")
                         or ((cr.get("object_story_spec") or {}).get("link_data")  or {}).get("image_url")
                         or ((cr.get("object_story_spec") or {}).get("video_data") or {}).get("image_url"),
        "creative_id":   cr.get("id"),
        "object_type":   cr.get("object_type"),
        "video_id":      cr.get("video_id"),
    }, None, usage

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit",       type=int, default=0, help="cap the number of ads to fetch")
    ap.add_argument("--refresh-all", action="store_true", help="re-fetch every ad regardless of fetched_at")
    ap.add_argument("--ad-id",       help="single ad_id (debug)")
    args = ap.parse_args()

    conn = psycopg2.connect(DB_URL); cur = conn.cursor()

    if args.ad_id:
        targets = [(args.ad_id, "")]
    else:
        # Order by spend so the most-visible ads are fetched first
        sql = """
        SELECT v.ad_id, v.ad_name
          FROM ae_table_view v
          LEFT JOIN ad_thumbnails t ON t.ad_id = v.ad_id
         WHERE v.ad_id IS NOT NULL
        """
        if not args.refresh_all:
            sql += f" AND (t.ad_id IS NULL OR t.fetched_at < NOW() - INTERVAL '{REFRESH_DAYS} days')"
        sql += " ORDER BY COALESCE(v.amount_spent, 0) DESC"
        if args.limit: sql += f" LIMIT {int(args.limit)}"
        cur.execute(sql)
        targets = cur.fetchall()

    print(f"[*] {len(targets):,} ad(s) to fetch  ·  refresh-window {REFRESH_DAYS}d"
          + (" (refresh-all)" if args.refresh_all else ""))
    print(f"[*] token tail …{TOK[-6:]}")
    print()

    upsert = """
      INSERT INTO ad_thumbnails (ad_id, thumbnail_url, image_url, creative_id,
                                 object_type, video_id, fetched_at, last_error)
      VALUES (%s,%s,%s,%s,%s,%s, NOW(), %s)
      ON CONFLICT (ad_id) DO UPDATE SET
        thumbnail_url = EXCLUDED.thumbnail_url,
        image_url     = EXCLUDED.image_url,
        creative_id   = EXCLUDED.creative_id,
        object_type   = EXCLUDED.object_type,
        video_id      = EXCLUDED.video_id,
        fetched_at    = NOW(),
        last_error    = EXCLUDED.last_error
    """
    ok = err = no_thumb = 0
    t0 = time.time()
    for i, (ad_id, name) in enumerate(targets, 1):
        rec, error, usage = fetch(ad_id)
        if rec is None:
            err += 1
            cur.execute(upsert, (ad_id, None, None, None, None, None, error))
            conn.commit()
            if i % 50 == 0 or i == len(targets):
                print(f"  [{i:>5}/{len(targets):>5}]  err   {ad_id}  {error}")
        else:
            if not rec["thumbnail_url"]: no_thumb += 1
            ok += 1
            cur.execute(upsert, (ad_id, rec["thumbnail_url"], rec["image_url"],
                                  rec["creative_id"], rec["object_type"],
                                  rec["video_id"], None))
            conn.commit()
            if i % 50 == 0 or i == len(targets):
                tt = rec["thumbnail_url"]
                t_short = scrub(tt)[:60] if tt else '—'
                print(f"  [{i:>5}/{len(targets):>5}]  ok    {ad_id}  {t_short}")
        # Throttle
        if usage >= 75:
            print(f"  ⏸ Meta usage at {usage}% — cooling 300 s")
            time.sleep(300)
        else:
            time.sleep(SLEEP_BETWEEN)

    dt = time.time() - t0
    print()
    print("─" * 70)
    print(f"  fetched ok      : {ok:>5}")
    print(f"  no thumbnail    : {no_thumb:>5}   (creative has no thumbnail_url)")
    print(f"  errors          : {err:>5}")
    print(f"  elapsed         : {dt/60:>5.1f} min")
    cur.close(); conn.close()

if __name__ == "__main__":
    main()
