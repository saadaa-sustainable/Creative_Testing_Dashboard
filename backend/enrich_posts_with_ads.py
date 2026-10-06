"""enrich_posts_with_ads.py — add the Ads Manager ad_name(s) to an IG post list.

HOW AN IG POST LINKS TO AN AD
-----------------------------
ig_media.boost_ads_count tells you HOW MANY ads used a post but not WHICH.
The Graph API field `boost_ads_list` on the media node returns the actual
ad ids:

    GET /{ig_media_id}?fields=boost_ads_list
    -> {"data": [{"ad_id": "1202...", "ad_status": "active"}, ...]}

Those ad ids join straight to ae_table_view.ad_id for the name and spend.

TWO THINGS THAT WILL BITE
    1. boost_ads_list REPEATS the same ad_id many times (one entry per
       delivery record, not per ad). One post here returned 190 entries for
       a handful of distinct ads. Dedupe or every count is inflated.
    2. A single post can back dozens of ads, so "the" ad_name is not one
       value. This writes ad_count, top_ad_name (highest lifetime spend) and
       ad_names (all of them, pipe-separated) rather than pretending it is 1:1.

Ads not present in ae_table_view (deleted, or never delivered) keep their
ad_id in ad_names_unmatched so the row is still traceable.

USAGE
    python enrich_posts_with_ads.py in.csv [in2.csv ...]
    python enrich_posts_with_ads.py --suffix _with_ads in.csv
"""
from __future__ import annotations
import argparse, csv, json, os, re, sys, time

import psycopg2
import requests
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

TOK = (os.environ.get("META_ACCESS_TOKEN") or "").strip()
if not TOK: sys.exit("Missing META_ACCESS_TOKEN in .env")
VER = (os.environ.get("META_API_VERSION") or "v22.0").strip()
API = f"https://graph.facebook.com/{VER}"
DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")

CACHE = ".boost_ads.cache.json"


def scrub(s) -> str:
    return re.sub(r"EAA[A-Za-z0-9]{20,}", "<REDACTED>", str(s or ""))


def log(*a): print(" ".join(scrub(x) for x in a), flush=True)


def shortcode(u):
    if not u: return None
    m = re.search(r"instagram\.com/(?:p|reel|tv)/([A-Za-z0-9_-]+)", u.split("?")[0])
    return m.group(1) if m else None


def _throttle(h):
    worst = 0
    try:
        for k in ("x-app-usage", "x-business-use-case-usage"):
            v = h.get(k)
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


def boost_ads(media_id, retries=4):
    """Distinct ad ids that used this media. [] if none, None on hard error."""
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(f"{API}/{media_id}",
                             params={"fields": "boost_ads_list", "access_token": TOK}, timeout=45)
        except requests.RequestException:
            time.sleep(delay); delay = min(delay * 2, 120); continue
        _throttle(r.headers)
        if r.status_code == 200:
            data = ((r.json().get("boost_ads_list") or {}).get("data")) or []
            seen, out = set(), []
            for e in data:                      # gotcha 1: duplicates
                a = e.get("ad_id")
                if a and a not in seen:
                    seen.add(a); out.append(a)
            return out
        try: err = (r.json().get("error") or {})
        except Exception: err = {}
        if r.status_code in (429, 500, 502, 503, 504) or err.get("code") in (4, 17, 32, 613):
            time.sleep(delay); delay = min(delay * 2, 300); continue
        return None
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--suffix", default="_with_ads")
    args = ap.parse_args()

    conn = psycopg2.connect(DB_URL, connect_timeout=30); conn.autocommit = True
    cur = conn.cursor()

    # permalink -> media_id / boost count, for the owned account
    cur.execute("""SELECT permalink, media_id, COALESCE(boost_ads_count,0)
                   FROM public.ig_media
                   WHERE permalink IS NOT NULL AND media_id !~ '^collab:'""")
    by_sc = {}
    for pl, mid, bc in cur.fetchall():
        k = shortcode(pl)
        if k: by_sc[k] = (mid, bc)

    cache = {}
    if os.path.exists(CACHE):
        try: cache = json.load(open(CACHE))
        except Exception: cache = {}

    # collect every media we need, across all input files
    need = []
    parsed = {}
    for f in args.files:
        rows = list(csv.DictReader(open(f, encoding="utf-8-sig")))
        parsed[f] = rows
        for r in rows:
            k = shortcode(r.get("permalink"))
            if not k or k not in by_sc: continue
            mid, bc = by_sc[k]
            if bc > 0 and mid not in cache: need.append(mid)
    need = list(dict.fromkeys(need))
    log(f"media needing boost_ads_list: {len(need)} (cached: {len(cache)})")

    for i, mid in enumerate(need, 1):
        ids = boost_ads(mid)
        cache[mid] = ids if ids is not None else []
        if i % 10 == 0:
            json.dump(cache, open(CACHE, "w"))
            log(f"  {i}/{len(need)} fetched")
        time.sleep(0.2)
    json.dump(cache, open(CACHE, "w"))

    # resolve ad_id -> name + spend
    all_ids = sorted({a for v in cache.values() for a in v})
    meta = {}
    if all_ids:
        cur.execute("""SELECT ad_id, ad_name, COALESCE(amount_spent,0), ad_status
                       FROM public.ae_table_view WHERE ad_id = ANY(%s)""", (all_ids,))
        for aid, nm, sp, st in cur.fetchall():
            meta[aid] = (nm, float(sp or 0), st)
    log(f"distinct ad ids: {len(all_ids)}   matched in ae_table_view: {len(meta)}")

    for f, rows in parsed.items():
        out = []
        for r in rows:
            k = shortcode(r.get("permalink"))
            mid, bc = by_sc.get(k, (None, 0))
            ids = cache.get(mid, []) if mid else []
            known = [(a, *meta[a]) for a in ids if a in meta]
            unknown = [a for a in ids if a not in meta]
            known.sort(key=lambda x: x[2], reverse=True)   # by spend
            r = dict(r)
            r["ad_count"] = len(ids)
            r["top_ad_name"] = known[0][1] if known else ""
            r["top_ad_spend"] = f"{known[0][2]:.2f}" if known else ""
            r["total_ad_spend"] = f"{sum(x[2] for x in known):.2f}" if known else ""
            r["ad_names"] = " | ".join(x[1] or "" for x in known)
            r["ad_ids"] = " | ".join(x[0] for x in known)
            r["ad_ids_unmatched"] = " | ".join(unknown)
            r["was_run_as_ad"] = "yes" if ids else "no"
            out.append(r)
        base, ext = os.path.splitext(f)
        dest = f"{base}{args.suffix}{ext}"
        with open(dest, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.DictWriter(fh, fieldnames=list(out[0].keys()))
            w.writeheader(); w.writerows(out)
        n_ad = sum(1 for r in out if r["was_run_as_ad"] == "yes")
        log(f"{dest}: {len(out)} rows, {n_ad} run as ads "
            f"({sum(int(r['ad_count']) for r in out)} ad links total)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
