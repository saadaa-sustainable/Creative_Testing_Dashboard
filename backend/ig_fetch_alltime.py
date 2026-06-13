"""ig_fetch_alltime.py - fetch every Instagram post from oldest to today, no
Apps Script time limit. Writes to CSV by default; optionally upserts directly
into a Google Sheet if a service-account JSON is configured.

Output:
  - C:\\Users\\Saadaa\\Downloads\\ig_alltime_<YYYY-MM-DD>.csv     (always)
  - The configured Sheet tab                                      (if GS_SA_JSON set)

Setup for direct sheet upsert (optional):
  1. Google Cloud Console -> APIs & Services -> Library -> enable
        Google Sheets API + Google Drive API
  2. Credentials -> Create credentials -> Service account
        Download the JSON key
  3. Open the target Sheet -> Share -> add the service account's email
        (format: <name>@<project>.iam.gserviceaccount.com)  with Editor access
  4. Add to .env:
        GS_SA_JSON       = D:\\path\\to\\service-account.json
        GS_SHEET_ID      = 1L6CtdAK3AElFJhMpkVDaBjpwVErzjQkdmiUKQWwUF0A
        GS_SHEET_GID     = 632727315
"""
import os, sys, io, time, csv, pathlib, requests
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="backslashreplace")
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(override=True)
TOKEN  = os.environ["IG_ACCESS_TOKEN"].strip()
IG     = os.environ["IG_USER_ID"].strip()
API    = "https://graph.facebook.com/v21.0"

print(f"[ig_alltime] IG user {IG}  token tail ...{TOKEN[-6:]}")

HEADER = ["Publish Date", "Permalink", "Media URL", "Media Type",
          "Reach", "Impressions", "Views", "Status"]

METRICS_FEED  = ["reach", "impressions", "views"]
METRICS_REEL  = ["reach", "impressions", "views"]
METRICS_STORY = ["reach", "impressions"]


def metrics_for(mpt, mtype):
    if mpt == "STORY": return METRICS_STORY
    if mpt == "REELS" or (mtype == "VIDEO" and mpt == "CLIPS"): return METRICS_REEL
    return METRICS_FEED


def get_with_retry(url, params, attempts=4, base_sleep=2):
    last_exc = None
    for a in range(1, attempts + 1):
        try:
            return requests.get(url, params=params, timeout=60)
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            last_exc = e
            time.sleep(base_sleep * a)
    raise last_exc


def fetch_insights(mid, metrics):
    out = {}
    r = get_with_retry(f"{API}/{mid}/insights",
                       {"metric": ",".join(metrics), "access_token": TOKEN})
    if r.status_code == 200:
        for e in r.json().get("data", []):
            v = (e.get("values") or [{}])[0].get("value")
            out[e["name"]] = v
        return out
    # batch failed (likely impressions deprecated for newer posts) - try each metric
    for met in metrics:
        try:
            rr = get_with_retry(f"{API}/{mid}/insights",
                                {"metric": met, "access_token": TOKEN})
            if rr.status_code == 200:
                for e in rr.json().get("data", []):
                    v = (e.get("values") or [{}])[0].get("value")
                    out[e["name"]] = v
        except Exception as e:
            print(f"    !! skipped metric {met} on {mid}: {type(e).__name__}")
    return out


def get_url_with_retry(url, attempts=5, base_sleep=3):
    last_exc = None
    for a in range(1, attempts + 1):
        try:
            return requests.get(url, timeout=90)
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            last_exc = e
            print(f"    !! /media attempt {a}/{attempts}: {type(e).__name__}; sleeping {base_sleep*a}s")
            time.sleep(base_sleep * a)
    raise last_exc


# ── 1) walk /media all the way back ───────────────────────────────────────
print("\n[1] paginating /media to the oldest post ...")
media = []
next_url = f"{API}/{IG}/media?fields=id,media_type,media_product_type,media_url,permalink,timestamp&limit=50&access_token={TOKEN}"
pages = 0
while next_url:
    pages += 1
    try:
        r = get_url_with_retry(next_url)
    except Exception as e:
        print(f"  page {pages} permanent fail after retries: {e}")
        break
    if r.status_code != 200:
        print(f"  page {pages} HTTP {r.status_code}: {r.text[:200]}")
        break
    j = r.json(); items = j.get("data", [])
    if not items: break
    media.extend(items)
    if pages % 5 == 0 or pages == 1:
        print(f"  page {pages:>3}: {len(media):>4} posts so far, earliest {items[-1]['timestamp']}", flush=True)
    next_url = (j.get("paging") or {}).get("next")
    time.sleep(0.2)

print(f"\n[1] paginated: {len(media)} posts across {pages} pages")
if media:
    print(f"     newest: {media[0]['timestamp']}")
    print(f"     oldest: {media[-1]['timestamp']}")


# ── 2) per-post insights — write each row to disk immediately so a crash
#       mid-run still yields a partial CSV ────────────────────────────────
out_csv = pathlib.Path.home() / "Downloads" / f"ig_alltime_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
print(f"\n[2] writing CSV incrementally: {out_csv}")
f_csv = open(out_csv, "w", newline="", encoding="utf-8")
w_csv = csv.writer(f_csv)
w_csv.writerow(HEADER); f_csv.flush()

print(f"\n[3] fetching insights for {len(media)} posts ...")
rows = []
t0 = time.time()
for i, m in enumerate(media, 1):
    mpt = m.get("media_product_type", "FEED")
    try:
        ins = fetch_insights(m["id"], metrics_for(mpt, m.get("media_type")))
    except Exception as e:
        print(f"    !! insights failed on {m['id']} after retries: {type(e).__name__} — writing row with blanks")
        ins = {}
    imp_for_row = ins.get("impressions")
    if imp_for_row is None and mpt != "STORY":
        imp_for_row = ins.get("views")
    row = [
        m.get("timestamp", ""),
        m.get("permalink", ""),
        m.get("media_url", ""),
        m.get("media_type", ""),
        ins.get("reach") if ins.get("reach") is not None else "",
        imp_for_row if imp_for_row is not None else "",
        ins.get("views") if ins.get("views") is not None else "",
        "",
    ]
    rows.append(row)
    w_csv.writerow(row); f_csv.flush()
    if i % 25 == 0 or i == len(media):
        elapsed = time.time() - t0
        rate = i / elapsed if elapsed else 0
        eta = (len(media) - i) / rate if rate else 0
        print(f"  [{i:>4}/{len(media)}] {m['timestamp'][:10]}  {mpt:<7}  "
              f"reach={str(ins.get('reach','-')):>7}  imp={str(imp_for_row or '-'):>7}  "
              f"views={str(ins.get('views') or '-'):>7}  "
              f"rate={rate:.1f}/s  ETA {eta/60:.1f}m", flush=True)
    time.sleep(0.12)

f_csv.close()
print(f"\n[done CSV] {out_csv}  ({len(rows)} rows)")


# ── 4) optional: direct sheet upsert ──────────────────────────────────────
GS_SA   = os.environ.get("GS_SA_JSON", "").strip()
GS_ID   = os.environ.get("GS_SHEET_ID", "1L6CtdAK3AElFJhMpkVDaBjpwVErzjQkdmiUKQWwUF0A").strip()
GS_GID  = int(os.environ.get("GS_SHEET_GID", "632727315"))

if not GS_SA or not pathlib.Path(GS_SA).exists():
    print("\n[4] skipping direct sheet upsert (GS_SA_JSON not set or file missing)")
    print("    Upload the CSV manually:")
    print(f"      Open the sheet -> File -> Import -> Upload {out_csv}")
    print(f"      Import location: Replace current sheet  (or Append to sheet)")
    sys.exit(0)

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("\n[4] gspread not installed. Install it:")
    print("    pip install gspread google-auth")
    sys.exit(0)

scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(GS_SA, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(GS_ID)

# Find worksheet by gid
target_ws = None
for ws in sh.worksheets():
    if ws.id == GS_GID:
        target_ws = ws; break
if target_ws is None:
    print(f"\n[4] sheet gid={GS_GID} not found. Worksheets:")
    for ws in sh.worksheets(): print(f"    {ws.title}  gid={ws.id}")
    sys.exit(1)

print(f"\n[4] upserting into '{target_ws.title}' (gid={target_ws.id}) ...")
existing = target_ws.get_all_values()
existing_perma = set()
if existing and len(existing) > 1:
    # Permalink is column 2 (index 1)
    for r in existing[1:]:
        if len(r) > 1 and r[1]:
            existing_perma.add(r[1])
print(f"     existing rows: {max(0,len(existing)-1)}   distinct permalinks: {len(existing_perma)}")

# write header if missing
if not existing:
    target_ws.update("A1", [HEADER], value_input_option="USER_ENTERED")
    target_ws.format("A1:H1", {"textFormat": {"bold": True}})
    target_ws.freeze(rows=1)
    next_row = 2
else:
    next_row = len(existing) + 1

# append only NEW permalinks (upsert by permalink)
new_rows = [r for r in rows if r[1] and r[1] not in existing_perma]
if new_rows:
    target_ws.update(f"A{next_row}", new_rows, value_input_option="USER_ENTERED")
    print(f"     appended {len(new_rows)} new rows (skipped {len(rows)-len(new_rows)} already present)")
else:
    print(f"     0 new rows; sheet already has all {len(rows)} permalinks")
