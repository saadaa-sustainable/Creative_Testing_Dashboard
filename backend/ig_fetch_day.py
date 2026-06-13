"""
ig_fetch_day.py - fetch Instagram organic post + insights data for a single day.

Reads IG_ACCESS_TOKEN and IG_USER_ID from .env. Token is never echoed.

Usage:
    python ig_fetch_day.py 2026-06-11   # default: yesterday's posts
"""
import os, sys, io, json, time, re, requests
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="backslashreplace")
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.environ.get("IG_ACCESS_TOKEN", "").strip()
IG_USER = os.environ.get("IG_USER_ID", "").strip()
if not TOKEN or not IG_USER:
    print("FATAL: IG_ACCESS_TOKEN or IG_USER_ID not set in .env")
    sys.exit(1)

API = "https://graph.facebook.com/v21.0"
DAY = sys.argv[1] if len(sys.argv) > 1 else "2026-06-11"
print(f"[ig_fetch_day] window: {DAY} (UTC)")
print(f"               IG user: {IG_USER}")
print(f"               token tail: ...{TOKEN[-6:]}  (full token never echoed)")


def scrub(s):
    return re.sub(r"(access_token=)[^&\s]+", r"\1<REDACTED>", str(s))


def get(path, params=None):
    p = dict(params or {})
    p["access_token"] = TOKEN
    r = requests.get(f"{API}{path}", params=p, timeout=60)
    if r.status_code != 200:
        msg = scrub(r.text)[:400]
        raise RuntimeError(f"HTTP {r.status_code} on {path}: {msg}")
    return r.json()


# ── 1) list media via paginated /media endpoint, since timestamp filter is not server-side
print("\n[1] enumerating /media (paginated)...")
day_start = datetime.fromisoformat(f"{DAY}T00:00:00+00:00")
day_end   = day_start + timedelta(days=1)
all_media = []
next_url = None
fields = "id,caption,media_type,media_product_type,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count,shared_to_feed,username"
page = 0
while True:
    page += 1
    if next_url:
        r = requests.get(next_url, timeout=60)
        if r.status_code != 200:
            print(f"  page {page} HTTP {r.status_code}: {scrub(r.text)[:200]}")
            break
        data = r.json()
    else:
        data = get(f"/{IG_USER}/media", {"fields": fields, "limit": 50})
    items = data.get("data", []) or []
    if not items:
        break
    earliest = datetime.fromisoformat(items[-1]["timestamp"].replace("Z","+00:00"))
    print(f"  page {page}: {len(items)} items, earliest {items[-1]['timestamp']}")
    for it in items:
        ts = datetime.fromisoformat(it["timestamp"].replace("Z","+00:00"))
        if day_start <= ts < day_end:
            all_media.append(it)
    if earliest < day_start:
        print(f"  cursor passed {DAY}; stopping pagination")
        break
    paging = data.get("paging") or {}
    next_url = paging.get("next")
    if not next_url:
        break
    time.sleep(0.2)

print(f"\n[1] media on {DAY}: {len(all_media)}")
for m in all_media:
    cap = (m.get("caption") or "")[:80].replace("\n", " ")
    print(f"  {m['timestamp']}  {m['media_product_type']:<10} id={m['id']}  likes={m.get('like_count','?')} comments={m.get('comments_count','?')}  {cap!r}")


# ── 2) per-media insights ──
# Metric sets vary by media_product_type. Keep them aligned with the spreadsheet
# columns the user shared: Reach, Impressions, Plays, Views, Saved, Shares,
# Total Interactions, Profile Visits, Profile Activity, Follows, Replies, Navigation.
METRICS_FEED  = ["reach","saved","shares","total_interactions","profile_visits","profile_activity","follows","likes","comments","views"]
METRICS_REEL  = ["reach","saved","shares","total_interactions","plays","views","follows","likes","comments","ig_reels_avg_watch_time","ig_reels_video_view_total_time"]
METRICS_STORY = ["reach","impressions","replies","total_interactions","navigation","profile_visits","profile_activity","follows","shares"]

def metrics_for(mpt, mtype):
    if mpt == "REELS" or mtype == "VIDEO" and mpt in ("REELS","CLIPS"):
        return METRICS_REEL
    if mpt == "STORY":
        return METRICS_STORY
    return METRICS_FEED


print("\n[2] fetching /insights per media …")
all_rows = []
for m in all_media:
    mid = m["id"]
    mpt = m.get("media_product_type", "FEED")
    mtype = m.get("media_type", "")
    metrics = metrics_for(mpt, mtype)
    insights = {}
    try:
        d = get(f"/{mid}/insights", {"metric": ",".join(metrics)})
        for entry in (d.get("data") or []):
            name = entry.get("name")
            vals = entry.get("values") or []
            v = vals[0].get("value") if vals else None
            insights[name] = v
    except RuntimeError as e:
        # Some metrics aren't available per media type — try one by one to salvage what works
        for met in metrics:
            try:
                d = get(f"/{mid}/insights", {"metric": met})
                for entry in (d.get("data") or []):
                    name = entry.get("name")
                    vals = entry.get("values") or []
                    insights[name] = vals[0].get("value") if vals else None
            except Exception:
                pass

    row = {
        "username": m.get("username", "saadaadesigns"),
        "media_url": m.get("media_url", ""),
        "media_type": mtype,
        "media_product_type": mpt,
        "id": mid,
        "comments": m.get("comments_count", 0),
        "likes": m.get("like_count", 0),
        "permalink": m.get("permalink", ""),
        "publish_date": m.get("timestamp", ""),
        "media_thumbnail_url": m.get("thumbnail_url", ""),
        "shared_to_feed": m.get("shared_to_feed"),
        "caption": (m.get("caption") or "").replace("\n", " ").strip()[:300],
        "profile_activity": insights.get("profile_activity"),
        "profile_visits": insights.get("profile_visits"),
        "plays": insights.get("plays"),
        "reach": insights.get("reach"),
        "total_interactions": insights.get("total_interactions"),
        "shares": insights.get("shares"),
        "follows": insights.get("follows"),
        "views": insights.get("views"),
        "impressions": insights.get("impressions"),
        "replies": insights.get("replies"),
        "saved": insights.get("saved"),
        "navigation": insights.get("navigation"),
    }
    all_rows.append(row)


print("\n[3] OUTPUT — raw API result for each media on", DAY)
for r in all_rows:
    print("\n" + "─"*78)
    for k, v in r.items():
        if k in ("media_url","media_thumbnail_url","caption","permalink"):
            print(f"  {k:<22} {str(v)[:120]}")
        else:
            print(f"  {k:<22} {v}")

# ── Write CSV matching the user's spreadsheet column order ──
import csv, pathlib
OUT_CSV = pathlib.Path.home() / "Downloads" / f"ig_posts_{DAY}.csv"
HEADER = [
    "Username","Media URL","Media Type","ID","Comments","Likes","Permalink",
    "Publish Date","Media Product Type","Media Thumbnail URL","Shared To Feed","Caption",
    "Profile Activity","Profile Visits","Plays","Reach","Total Interactions","Shares",
    "Follows","Views","Impressions","Replies","Saved","Navigation","Status","ASSET ID",
]
with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(HEADER)
    for r in all_rows:
        w.writerow([
            r["username"], r["media_url"], r["media_type"], r["id"],
            r["comments"], r["likes"], r["permalink"], r["publish_date"],
            r["media_product_type"], r["media_thumbnail_url"], r["shared_to_feed"],
            r["caption"], r["profile_activity"], r["profile_visits"], r["plays"],
            r["reach"], r["total_interactions"], r["shares"], r["follows"],
            r["views"], r["impressions"], r["replies"], r["saved"], r["navigation"],
            "", "",   # Status, ASSET ID — user-managed columns left blank
        ])
print(f"\n[done] {len(all_rows)} media fetched for {DAY}")
print(f"[csv]  saved -> {OUT_CSV}")
