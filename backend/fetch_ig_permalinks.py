"""fetch_ig_permalinks.py — pull post permalinks for any IG business account
WITHOUT needing that account to grant us access.

WHY business_discovery INSTEAD OF /media
----------------------------------------
/{ig_id}/media only works for accounts assigned to our app in Business
Manager. business_discovery.username(<handle>) is evaluated against OUR OWN
account (the "host"), so it reads any public IG Business/Creator profile with
no permission from them at all — and it does return `permalink`.

Probed 2026-09-25:
    saadaadesigns  media_count 1462 -> pages cleanly, full history reachable
    saadaa_men     media_count  496 -> returns 13, NO cursor. Hard stop.
    saadaa_women   media_count 1196 -> returns 0 media. Empty edge.

So the ceiling is Meta's, not ours: for saadaa_men and saadaa_women the API
reports a media_count it will not let anyone enumerate. There is no parameter
or permission that changes this — business_discovery already bypasses the
permission question entirely.

PAGINATION GOTCHA
    The nested media object returns paging.cursors.after but NO paging.next.
    Code that waits for a `next` URL (the normal Graph pattern) sees one page
    and concludes the account is exhausted — which is exactly how a 1462-post
    account looks like a 50-post account. Re-request with media.after(<cursor>).

USAGE
    python fetch_ig_permalinks.py                      # all 3, to CSV
    python fetch_ig_permalinks.py --account saadaa_men
    python fetch_ig_permalinks.py --out links.csv --max-pages 30
    python fetch_ig_permalinks.py --store              # also upsert to ig_media
"""
from __future__ import annotations
import argparse, csv, json, os, re, sys, time
from datetime import datetime

import requests
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

TOK = (os.environ.get("META_ACCESS_TOKEN") or "").strip()
if not TOK: sys.exit("Missing META_ACCESS_TOKEN in .env")
VER = (os.environ.get("META_API_VERSION") or "v22.0").strip()
API = f"https://graph.facebook.com/{VER}"

# The account we own — business_discovery is always evaluated from here.
HOST_IG_ID = os.environ.get("IG_USER_ID_1", "17841412619002528").strip()

TARGETS = ["saadaadesigns", "saadaa_men", "saadaa_women"]

FIELDS = "id,permalink,timestamp,media_type,media_product_type,like_count,comments_count,caption"

_TOK_RE = re.compile(r"(?:EAA[A-Za-z0-9]{20,}|IGQ[\w\-]{20,})")


def scrub(s) -> str:
    """Never let a token reach a log line or a traceback."""
    s = _TOK_RE.sub("<REDACTED>", str(s or ""))
    return re.sub(r"(access_token=)[^&\s]*", r"\1<REDACTED>", s)


def log(*a):
    print(" ".join(scrub(x) for x in a), flush=True)


def _sleep_if_throttled(headers) -> int:
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
    except Exception:
        pass
    if   worst >= 95: log(f"  [throttle {worst}%] sleep 300s"); time.sleep(300)
    elif worst >= 90: log(f"  [throttle {worst}%] sleep 60s");  time.sleep(60)
    elif worst >= 80: log(f"  [throttle {worst}%] sleep 15s");  time.sleep(15)
    return worst


def _get(params, retries=5):
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(f"{API}/{HOST_IG_ID}", params=params, timeout=60)
        except requests.RequestException as e:
            log(f"  [net {attempt}] {type(e).__name__}: {scrub(e)[:110]} — sleep {delay}s")
            time.sleep(delay); delay = min(delay * 2, 120); continue
        if r.status_code == 200:
            _sleep_if_throttled(r.headers)
            return r.json(), None
        _sleep_if_throttled(r.headers)
        try: err = (r.json().get("error") or {})
        except Exception: err = {}
        msg, code = scrub(err.get("message") or r.text[:160]), err.get("code")
        if r.status_code in (429, 500, 502, 503, 504) or code in (4, 17, 32, 613):
            log(f"  [throttle {attempt}] {msg[:100]} — sleep {delay}s")
            time.sleep(delay); delay = min(delay * 2, 300); continue
        return None, f"code={code}: {msg[:200]}"
    return None, "exhausted retries"


def fetch_permalinks(target: str, per: int = 100, max_pages: int = 40):
    after, out, pages, media_count = None, [], 0, None
    while pages < max_pages:
        m = f"media.limit({per})" + (f".after({after})" if after else "")
        q = f"business_discovery.username({target}){{followers_count,media_count,{m}{{{FIELDS}}}}}"
        j, err = _get({"fields": q, "access_token": TOK})
        if err:
            log(f"  [!] {target}: {err}")
            return out, media_count, err
        bd = (j or {}).get("business_discovery") or {}
        media_count = bd.get("media_count")
        md = bd.get("media") or {}
        data = md.get("data", []) or []
        out.extend(data); pages += 1
        # cursors.after only — there is no paging.next on this nested edge
        after = ((md.get("paging") or {}).get("cursors") or {}).get("after")
        log(f"  page {pages}: +{len(data):3d}  total {len(out):5d}  cursor={'yes' if after else 'no'}")
        if not data or not after: break
        time.sleep(0.3)
    return out, media_count, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", help="single handle (default: all three)")
    ap.add_argument("--out", default="ig_permalinks.csv")
    ap.add_argument("--per", type=int, default=100)
    ap.add_argument("--max-pages", type=int, default=40)
    args = ap.parse_args()

    targets = [args.account] if args.account else TARGETS
    rows, summary = [], []

    for t in targets:
        log(f"[{t}]")
        got, mc, err = fetch_permalinks(t, args.per, args.max_pages)
        for m in got:
            rows.append({
                "ig_username": t,
                "media_id": m.get("id"),
                "permalink": m.get("permalink"),
                "publish_date": (m.get("timestamp") or "")[:10],
                "timestamp": m.get("timestamp"),
                "media_type": m.get("media_type"),
                "media_product_type": m.get("media_product_type"),
                "like_count": m.get("like_count"),
                "comments_count": m.get("comments_count"),
                "caption": (m.get("caption") or "").replace("\n", " ")[:300],
            })
        ts = sorted(x["timestamp"][:10] for x in got if x.get("timestamp"))
        summary.append((t, len(got), mc, ts[0] if ts else "-", ts[-1] if ts else "-", err or ""))
        log(f"  -> {len(got)} of media_count {mc}"
            f"   range {ts[0] if ts else '-'} .. {ts[-1] if ts else '-'}\n")

    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else
                           ["ig_username","media_id","permalink","publish_date"])
        w.writeheader(); w.writerows(rows)

    log("=" * 74)
    log(f"{'account':16s} {'fetched':>8s} {'media_count':>12s}  {'earliest':10s} {'latest':10s}")
    for t, n, mc, lo, hi, err in summary:
        gap = (mc - n) if isinstance(mc, int) else "?"
        log(f"{t:16s} {n:>8d} {str(mc):>12s}  {lo:10s} {hi:10s}"
            f"{'   UNREACHABLE: ' + str(gap) if gap not in ('?', 0) else ''}")
    log(f"\nwrote {len(rows):,} rows -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
