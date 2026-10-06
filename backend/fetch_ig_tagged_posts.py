"""fetch_ig_tagged_posts.py — find recent creator posts that tag our brands,
and capture their permalinks. No permission needed from any creator.

WHY NOT THE OBVIOUS ROUTES
--------------------------
Probed 2026-09-25, all three brand accounts:

  /{ig_id}/tags    -> (#10) Application does not have permission for this
                      action. Fails even for saadaadesigns, which we OWN —
                      the tagged-media edge needs App Review approval this
                      app does not have. Not a permissions grant we can make
                      ourselves; it is an app capability.

  /{ig_id}/media   -> returns only media the account OWNS. An Instagram
                      Collab post belongs to the creator who posted it, so
                      it never appears here for the tagged brand. This is
                      why saadaa_women shows media_count 1196 and returns 0:
                      its grid is largely collab posts owned by creators.

WHAT WORKS
    business_discovery.username(<creator>) evaluated from our own account.
    It reads any public IG Business/Creator profile with no permission from
    them, returns `permalink`, and paginates. We walk the creators we work
    with (from CreatorHub) and keep the posts whose caption names a brand.

TWO GOTCHAS THAT SILENTLY RETURN NOTHING
    1. The API STRIPS '@' FROM CAPTIONS. The post in the 2026-09-25 report
       reads "@saadaa_women @saadaadesigns" on instagram.com but arrives as
       "saadaa_women saadaadesigns". Matching '@handle' finds zero posts and
       looks like "no one tagged us". Match the BARE handle.
    2. The nested media edge returns paging.cursors.after but NO paging.next.
       Waiting for a `next` URL stops after one page.

USAGE
    python fetch_ig_tagged_posts.py                    # creators active last 60d
    python fetch_ig_tagged_posts.py --days 30 --per 12
    python fetch_ig_tagged_posts.py --handles a,b,c
    python fetch_ig_tagged_posts.py --since 2026-09-01 # only posts on/after
"""
from __future__ import annotations
import argparse, csv, json, os, re, sys, time, urllib.parse, urllib.request
from datetime import date, datetime, timedelta

import requests
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

TOK = (os.environ.get("META_ACCESS_TOKEN") or "").strip()
if not TOK: sys.exit("Missing META_ACCESS_TOKEN in .env")
VER = (os.environ.get("META_API_VERSION") or "v22.0").strip()
API = f"https://graph.facebook.com/{VER}"
HOST_IG_ID = os.environ.get("IG_USER_ID_1", "17841412619002528").strip()

CH_URL = (os.environ.get("CREATOR_HUB_URL") or "").rstrip("/")
CH_KEY = (os.environ.get("CREATOR_HUB_ACCESS") or "").strip()

# Bare handles / hashtags — NOT '@'-prefixed, see gotcha 1 above.
BRAND_TERMS = ["saadaa_women", "saadaa_men", "saadaadesigns",
               "rahosaadaa", "pehnosaadaa", "saadaafeels"]

FIELDS = "id,permalink,timestamp,media_type,media_product_type,like_count,comments_count,caption"
PROGRESS = ".ig_tagged.progress.json"

_SECRET = re.compile(r"(?:EAA[A-Za-z0-9]{20,}|IGQ[\w\-]{20,}|eyJ[\w\-.]{30,})")


def scrub(s) -> str:
    s = _SECRET.sub("<REDACTED>", str(s or ""))
    return re.sub(r"(access_token=|apikey=)[^&\s]*", r"\1<REDACTED>", s)


def log(*a): print(" ".join(scrub(x) for x in a), flush=True)


def brands_in(caption: str):
    c = (caption or "").lower()
    return [t for t in BRAND_TERMS if t in c]


def creator_handles(days: int):
    """Handles that posted for us recently, from CreatorHub."""
    since = (date.today() - timedelta(days=days)).isoformat()
    out, offset = set(), 0
    while True:
        qs = urllib.parse.urlencode({
            "select": "username", "post_date": f"gte.{since}",
            "username": "not.is.null", "limit": 1000, "offset": offset})
        req = urllib.request.Request(f"{CH_URL}/rest/v1/posts?{qs}",
              headers={"apikey": CH_KEY, "Authorization": f"Bearer {CH_KEY}",
                       "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            chunk = json.loads(r.read())
        out |= {(x.get("username") or "").strip().lstrip("@") for x in chunk if x.get("username")}
        if len(chunk) < 1000: break
        offset += 1000
    return sorted(h for h in out if h)


def _throttle(headers):
    worst = 0
    try:
        for k in ("x-app-usage", "x-business-use-case-usage"):
            v = headers.get(k)
            if not v: continue
            j = json.loads(v) if isinstance(v, str) else v
            def walk(x):
                if isinstance(x, dict):
                    for _, vv in x.items(): yield from walk(vv)
                elif isinstance(x, list):
                    for vv in x: yield from walk(vv)
                elif isinstance(x, (int, float)) and 0 <= x <= 100: yield x
            for n in walk(j): worst = max(worst, n)
    except Exception: pass
    if   worst >= 95: log(f"  [throttle {worst}%] sleep 300s"); time.sleep(300)
    elif worst >= 90: log(f"  [throttle {worst}%] sleep 60s");  time.sleep(60)
    elif worst >= 80: log(f"  [throttle {worst}%] sleep 15s");  time.sleep(15)
    return worst


def discover(handle: str, per: int, retries: int = 4):
    """Recent media for one creator. Returns (list, error|None)."""
    q = (f"business_discovery.username({handle})"
         f"{{followers_count,media_count,media.limit({per}){{{FIELDS}}}}}")
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(f"{API}/{HOST_IG_ID}",
                             params={"fields": q, "access_token": TOK}, timeout=60)
        except requests.RequestException as e:
            time.sleep(delay); delay = min(delay * 2, 120); continue
        _throttle(r.headers)
        if r.status_code == 200:
            bd = (r.json().get("business_discovery") or {})
            return (bd.get("media") or {}).get("data", []) or [], None
        try: err = (r.json().get("error") or {})
        except Exception: err = {}
        code, msg = err.get("code"), scrub(err.get("message"))[:120]
        if r.status_code in (429, 500, 502, 503, 504) or code in (4, 17, 32, 613):
            log(f"    [throttle {attempt}] {msg[:70]} — sleep {delay}s")
            time.sleep(delay); delay = min(delay * 2, 300); continue
        # code 110/100 = handle not a business account / not found — skip quietly
        return [], f"code={code}: {msg}"
    return [], "exhausted retries"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60, help="creators active in last N days")
    ap.add_argument("--per", type=int, default=12, help="recent posts to check per creator")
    ap.add_argument("--since", help="only keep posts on/after YYYY-MM-DD")
    ap.add_argument("--handles", help="comma-separated handles instead of CreatorHub")
    ap.add_argument("--out", default="ig_tagged_posts.csv")
    ap.add_argument("--limit", type=int, default=0, help="cap creators scanned (0=all)")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    if args.handles:
        handles = [h.strip().lstrip("@") for h in args.handles.split(",") if h.strip()]
    else:
        handles = creator_handles(args.days)
    if args.limit: handles = handles[:args.limit]

    prog = {}
    if not args.reset and os.path.exists(PROGRESS):
        try: prog = json.load(open(PROGRESS))
        except Exception: prog = {}
    done = set(prog.get("done", []))
    rows = prog.get("rows", [])

    log(f"=== scanning {len(handles)} creators, {args.per} recent posts each "
        f"(already done: {len(done)}) ===")
    skipped = 0
    for i, h in enumerate(handles, 1):
        if h in done: continue
        media, err = discover(h, args.per)
        if err:
            skipped += 1
        hits = 0
        for m in media:
            cap = m.get("caption") or ""
            brands = brands_in(cap)
            if not brands: continue
            ts = (m.get("timestamp") or "")[:10]
            if args.since and ts and ts < args.since: continue
            hits += 1
            rows.append({
                "creator": h,
                "post_date": ts,
                "timestamp": m.get("timestamp"),
                "permalink": m.get("permalink"),
                "brands_tagged": "|".join(brands),
                "media_product_type": m.get("media_product_type"),
                "like_count": m.get("like_count"),
                "comments_count": m.get("comments_count"),
                "caption": cap.replace("\n", " ")[:400],
                "media_id": m.get("id"),
            })
        done.add(h)
        if hits: log(f"  [{i}/{len(handles)}] @{h}: {hits} tagged post(s)")
        if i % 20 == 0:
            prog = {"done": sorted(done), "rows": rows}
            json.dump(prog, open(PROGRESS, "w"))
            log(f"  ...{i}/{len(handles)} scanned, {len(rows)} tagged posts so far")
        time.sleep(0.25)

    json.dump({"done": sorted(done), "rows": rows}, open(PROGRESS, "w"))

    rows.sort(key=lambda r: r["timestamp"] or "", reverse=True)
    if rows:
        with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)

    log(f"\n=== {len(rows)} tagged posts from {len(done)} creators "
        f"({skipped} handles unreadable) -> {args.out} ===")
    for r in rows[:10]:
        log(f"  {r['post_date']}  @{r['creator']:22.22s} {r['brands_tagged']:28.28s} {r['permalink']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
