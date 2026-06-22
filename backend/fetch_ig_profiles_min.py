"""
fetch_ig_profiles_min.py — minimal Instagram profile fetcher.

Fields returned per handle:
  url_handle        → https://www.instagram.com/{username}/
  followers         → followers_count
  image_URL         → profile_picture_url
  engagement_rate   → (sum(likes+comments across recent posts) / posts) / followers * 100
  average_likes     → sum(likes across recent posts) / posts
  is_verified       → NOT_AVAILABLE  (Meta blocks this field via Business Discovery)

Up to the 50 most recent public posts are sampled for likes/comments.
Token is read from .env via dotenv. Never echoed, never logged in plain form.

USAGE
  python fetch_ig_profiles_min.py kajolchugh_ salonyjain08 muskanjain_____ riya_kapooor
  python fetch_ig_profiles_min.py --file handles.txt --out my_profiles.csv
"""
import os, sys, re, csv, time, argparse, datetime as dt, io
from pathlib import Path
import requests
from dotenv import load_dotenv

try: sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv()
META_TOKEN = os.environ["META_ACCESS_TOKEN"].strip()
API_VER    = os.getenv("META_API_VERSION", "v22.0").strip()
IG_BIZ_ID  = os.getenv("IG_BUSINESS_ID", "").strip()
LOG_FILE   = "fetch_ig_profiles_min.log"

TOKEN_RE = re.compile(r"(?:EAA[a-zA-Z0-9]{30,}|shpa_[a-zA-Z0-9]{20,}|IGQ[\w\-]{20,})")
def _scrub(s):
    if s is None: return s
    try: return TOKEN_RE.sub("<REDACTED>", str(s))
    except Exception: return "<scrub_err>"
def log(*a):
    msg = " ".join(_scrub(x) for x in a)
    try: print(msg, flush=True)
    except UnicodeEncodeError:
        enc = sys.stdout.encoding or "ascii"
        print(msg.encode(enc, errors="replace").decode(enc, errors="replace"), flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f: f.write(msg + "\n")

def discover_ig_business_id():
    r = requests.get(f"https://graph.facebook.com/{API_VER}/debug_token",
                     params={"input_token": META_TOKEN, "access_token": META_TOKEN}, timeout=60)
    if r.status_code != 200: return None
    ig_ids = []
    for gs in (r.json().get("data") or {}).get("granular_scopes", []):
        if gs.get("scope") == "instagram_basic":
            ig_ids = gs.get("target_ids") or []
            break
    best, best_f = None, -1
    for ig_id in ig_ids:
        rr = requests.get(f"https://graph.facebook.com/{API_VER}/{ig_id}",
                          params={"fields": "id,followers_count", "access_token": META_TOKEN}, timeout=30)
        if rr.status_code == 200:
            f = int(rr.json().get("followers_count") or 0)
            if f > best_f: best, best_f = rr.json().get("id"), f
    return best

PROFILE_FIELDS = "username,followers_count,profile_picture_url"
MEDIA_FIELDS   = "like_count,comments_count"

def fetch_one(src_ig, handle):
    h = handle.lstrip("@").strip()
    bd = (f"business_discovery.username({h})"
          f"{{{PROFILE_FIELDS},media.limit(50){{{MEDIA_FIELDS}}}}}")
    r = requests.get(f"https://graph.facebook.com/{API_VER}/{src_ig}",
                     params={"fields": bd, "access_token": META_TOKEN}, timeout=60)
    if r.status_code != 200:
        try: err = r.json().get("error", {}).get("message", r.text[:200])
        except Exception: err = r.text[:200]
        return None, f"HTTP {r.status_code}: {_scrub(err)}"
    bd_obj = (r.json() or {}).get("business_discovery")
    if not bd_obj: return None, "no business_discovery (handle may be personal account)"
    return bd_obj, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("handles", nargs="*")
    ap.add_argument("--file")
    ap.add_argument("--out", default="ig_profiles_min.csv")
    ap.add_argument("--ig-business-id")
    ap.add_argument("--sleep", type=float, default=0.4)
    args = ap.parse_args()
    open(LOG_FILE, "w").close()

    src_ig = (args.ig_business_id or IG_BIZ_ID or discover_ig_business_id() or "").strip()
    if not src_ig: log("[fatal] no IG_BUSINESS_ID"); sys.exit(1)
    log(f"[ok] source IG: {src_ig}")

    handles = list(args.handles)
    if args.file:
        with open(args.file, encoding="utf-8") as f:
            for line in f:
                h = line.strip().split("#",1)[0].strip()
                if h: handles.append(h)
    handles = [h for h in (s.strip() for s in handles) if h]
    if not handles: log("[fatal] no handles"); sys.exit(2)
    log(f"[input] {len(handles)} handles")

    cols = ["url_handle","followers","image_URL","engagement_rate","average_likes","is_verified","error"]
    rows = []; ok = err = 0
    for i, h in enumerate(handles, 1):
        info, error = fetch_one(src_ig, h)
        if info is None:
            err += 1
            log(f"  [{i:>3}/{len(handles)}]  @{h.lstrip('@')}  -> ERROR  {error}")
            rows.append({"url_handle": f"https://www.instagram.com/{h.lstrip('@')}/",
                         "followers": "", "image_URL": "", "engagement_rate": "",
                         "average_likes": "", "is_verified": "NOT_AVAILABLE", "error": error})
        else:
            ok += 1
            uname = info.get("username") or h.lstrip("@")
            followers = int(info.get("followers_count") or 0)
            items = ((info.get("media") or {}).get("data")) or []
            n = len(items)
            tot_likes = sum(int(it.get("like_count") or 0) for it in items)
            tot_comments = sum(int(it.get("comments_count") or 0) for it in items)
            avg_likes = (tot_likes / n) if n else 0
            avg_eng_per_post = ((tot_likes + tot_comments) / n) if n else 0
            eng_rate = (100.0 * avg_eng_per_post / followers) if followers else 0
            row = {
                "url_handle":      f"https://www.instagram.com/{uname}/",
                "followers":       followers,
                "image_URL":       info.get("profile_picture_url") or "",
                "engagement_rate": round(eng_rate, 4),
                "average_likes":   round(avg_likes, 2),
                "is_verified":     "NOT_AVAILABLE",   # Meta blocks this field via Business Discovery
                "error":           "",
            }
            log(f"  [{i:>3}/{len(handles)}]  @{uname:<22}  followers={row['followers']:>7}  "
                f"avg_likes={row['average_likes']:>8.0f}  ER={row['engagement_rate']:>5.2f}%  posts_sampled={n}")
            rows.append(row)
        time.sleep(args.sleep)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for r in rows:
            for k in cols: r.setdefault(k, "")
            if r.get("error"): r["error"] = _scrub(r["error"])
            w.writerow(r)
    log(f"\n[done] ok={ok}  err={err}  csv={Path(args.out).resolve()}")
    log("[note] is_verified is hard-blocked by Meta's Business Discovery API for all field-name variants "
        "(is_verified, verified, verification_status). Only available via paid data providers "
        "(Phyllo, Modash, HypeAuditor) or HTML scraping (against Meta ToS).")

if __name__ == "__main__":
    main()
