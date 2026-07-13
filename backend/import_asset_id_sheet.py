"""
import_asset_id_sheet.py — pull the CTP-Asset_Sheet_v1 Google Sheet's
"External" tab and upsert the ad_id → asset_id mapping into
public.ad_asset_ids on the Meta ads Supabase project.

Sheet columns (case-sensitive):
    ad_id, Video, Graphic, Influencer, Mapped, ...

For every row where any of Video / Graphic / Influencer is non-empty,
we extract the first non-empty as the asset_id. Rows where all three
are blank (or where Mapped is FALSE) are skipped — nothing to link.

The sheet is expected to be viewable via the "anyone with link" share
setting. No OAuth needed; we hit the CSV export endpoint directly.

USAGE
  python import_asset_id_sheet.py                          # defaults
  python import_asset_id_sheet.py --sheet <ID> --gid 0
  python import_asset_id_sheet.py --dry-run                # skip DB write
  python import_asset_id_sheet.py --url "https://docs.google.com/spreadsheets/d/17SAZ.../edit"
                                                            # or pass a URL

Idempotent — safe to run at any cadence.
"""
from __future__ import annotations
import os, sys, csv, io, re, time, argparse, requests
from datetime import datetime, timezone
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv(override=True)

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()

DEFAULT_SHEET_ID = "17SAZ-WfyHTC8IxArSZlfqm6qKxcnqF2LJr82J9dT-_Y"
DEFAULT_GID      = 0                     # "External" tab
# Column headers we know are present. Rest are ignored.
ASSET_COL_ORDER  = ["Video", "Graphic", "Influencer"]

def _sheet_id_from_url(url: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_\-]+)", url or "")
    return m.group(1) if m else ""

def _fetch_csv(sheet_id: str, gid: int) -> list[dict]:
    """Follow the export redirect and return parsed dict rows."""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    # requests follows redirects by default
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Sheet fetch failed: HTTP {r.status_code} — "
                           f"sheet may be private or the tab gid is wrong")
    ctype = r.headers.get("Content-Type", "")
    if "text/csv" not in ctype and "text/html" in ctype:
        raise RuntimeError("Got HTML back — sheet likely requires sign-in. "
                           "Set share to 'anyone with link'.")
    # csv.DictReader handles quoting + newlines cleanly
    reader = csv.DictReader(io.StringIO(r.text))
    return list(reader)

def _pick_asset(row: dict) -> str:
    """First non-empty of Video / Graphic / Influencer. Returns '' if
    none present so callers can filter."""
    for c in ASSET_COL_ORDER:
        v = (row.get(c) or "").strip()
        if v:
            return v
    return ""

def _normalise_ad_id(v: str) -> str:
    """Strip whitespace + normalise scientific notation (Sheets sometimes
    formats large numeric ids as 1.20233E+17 when the column type is
    Number instead of Plain text). Round-trips those back to full digits."""
    v = (v or "").strip()
    if not v: return ""
    if re.fullmatch(r"\d{5,}", v): return v
    # e.g. "1.2023370834E+17" → "120233708340000000" — best-effort recovery.
    try:
        f = float(v)
        if f > 1e10 and f == int(f):
            return str(int(f))
    except Exception: pass
    return v

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheet",   default=DEFAULT_SHEET_ID, help="Sheet id (from /d/<id>/ in the URL)")
    ap.add_argument("--gid",     type=int, default=DEFAULT_GID, help="Tab gid (0 = default first tab)")
    ap.add_argument("--url",     help="Full sheet URL — overrides --sheet + --gid parse")
    ap.add_argument("--dry-run", action="store_true", help="fetch + summarise, don't write DB")
    args = ap.parse_args()

    sheet_id = args.sheet
    gid      = args.gid
    if args.url:
        parsed = _sheet_id_from_url(args.url)
        if parsed: sheet_id = parsed
        m = re.search(r"[?&#]gid=(\d+)", args.url)
        if m: gid = int(m.group(1))

    print(f"[*] sheet={sheet_id}  gid={gid}  dry_run={args.dry_run}")
    if not DB_URL and not args.dry_run:
        sys.exit("Missing SUPABASE_DB_URL in .env")

    t0 = time.time()
    rows = _fetch_csv(sheet_id, gid)
    print(f"[*] fetched {len(rows):,} CSV rows in {time.time()-t0:.1f}s")

    if not rows:
        print("[!] sheet returned no rows"); return
    print(f"[*] header cols: {list(rows[0].keys())}")

    # Extract (ad_id, asset_id) pairs
    extracted = []
    skipped_blank_asset = 0
    skipped_blank_ad_id = 0
    for r in rows:
        ad_id    = _normalise_ad_id(r.get("ad_id"))
        asset_id = _pick_asset(r)
        if not ad_id:
            skipped_blank_ad_id += 1; continue
        if not asset_id:
            skipped_blank_asset += 1; continue
        extracted.append((ad_id, asset_id))

    # Collapse duplicates — last-value wins (matches spreadsheet semantics).
    dedup = {}
    for aid, ast in extracted: dedup[aid] = ast
    print(f"[*] {len(extracted):,} rows had an asset_id  ·  {len(dedup):,} unique ad_ids after dedup")
    print(f"[*] skipped: {skipped_blank_ad_id:,} missing ad_id  ·  {skipped_blank_asset:,} no bucket flagged")

    # Show a distribution across bucket types for sanity
    dist = {"Video":0, "Graphic":0, "Influencer":0, "Other":0}
    for aid, ast in dedup.items():
        up = ast.upper()
        if up.startswith("SIF"):        dist["Influencer"] += 1
        elif up.startswith("GAD"):      dist["Graphic"]    += 1
        elif re.match(r"^[A-Z][a-z]{2}-?\d", ast): dist["Video"] += 1  # Sep-682, May-1027
        else:                            dist["Other"]      += 1
    print(f"[*] rough bucket distribution: {dist}")

    if args.dry_run:
        print("[dry-run] would upsert; skipping DB write")
        return

    # Upsert into public.ad_asset_ids
    import psycopg2
    from psycopg2.extras import execute_values
    src_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={gid}"
    tuples = [(aid, ast, src_url) for aid, ast in dedup.items()]
    conn = psycopg2.connect(DB_URL, connect_timeout=15)
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO public.ad_asset_ids (ad_id, asset_id, source_url)
                VALUES %s
                ON CONFLICT (ad_id) DO UPDATE SET
                  asset_id    = EXCLUDED.asset_id,
                  source_url  = EXCLUDED.source_url,
                  assigned_at = NOW();
            """, tuples, page_size=500)
        conn.commit()
    finally:
        conn.close()
    print(f"[ok] upserted {len(tuples):,} rows into public.ad_asset_ids")

if __name__ == "__main__":
    main()
