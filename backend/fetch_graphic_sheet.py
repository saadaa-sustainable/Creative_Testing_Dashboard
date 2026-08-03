"""fetch_graphic_sheet.py — mirror the CTP-Asset_Sheet_v1 "Graphic" tab
(gid=655330043) into Meta_ads_data as public.content_graphic_register.

Row grain: one Requisition ID per row (e.g. GAD-Jan-2). Untested = Ad ID
column is blank. Sheet must be shared as "anyone with link (viewer)".

USAGE
  python fetch_graphic_sheet.py                # full mirror (default)
  python fetch_graphic_sheet.py --dry-run      # fetch + summarise, no writes
"""
from __future__ import annotations
import os, sys, csv, io, re, argparse, requests, psycopg2
from datetime import datetime, timezone, date
from dotenv import load_dotenv
from pathlib import Path
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(Path(__file__).parent / ".env", override=True)

SHEET_ID = "17SAZ-WfyHTC8IxArSZlfqm6qKxcnqF2LJr82J9dT-_Y"
GID      = 655330043
DB_URL   = os.environ.get("SUPABASE_DB_URL", "").strip()
if not DB_URL:
    sys.exit("[fatal] SUPABASE_DB_URL missing")

# Sheet column header → table column. The sheet has 2 columns literally named
# "Date" (asset date + ad-launch date) and blank columns between groups; index
# lookups below disambiguate.
COL_MAP = [
    # (header_name_or_None, table_column, sheet_index_override)
    ("Date",                    "asset_date",           0),
    ("Priority",                "priority",             1),
    ("Requisition ID",          "requisition_id",       2),
    ("Creative",                "creative",             3),
    ("Nomenclature",            "nomenclature",         4),
    ("Reference links (if any)","reference_links",      5),
    ("Product",                 "product",              6),
    ("Audience Type",           "audience_type",        7),
    ("Graphic Type",            "graphic_type",         8),
    ("Key Message",             "key_message",          9),
    # index 10 is blank
    ("Things to Note",          "things_to_note",       11),
    ("Objective",               "objective",            12),
    ("DEMOGRAPHIC (Age, Gender)","demographic",         13),
    ("Who is this for",         "who_is_this_for",      14),
    ("Visuals",                 "visuals",              15),
    ("9:16 Count",              "count_9_16",           16),
    ("4:5 Count",               "count_4_5",            17),
    ("16:9 Count",              "count_16_9",           18),
    ("1:1 Count",               "count_1_1",            19),
    ("Total Count",             "total_count",          20),
    ("Platform",                "platform",             21),
    ("Assignee",                "assignee",             22),
    ("Catchphrases (MAIN HEADLINE)", "catchphrase_main",23),
    ("Catchphrases (SUB TEXT/ ICONS)","catchphrase_sub",24),
    ("Due Date",                "due_date",             25),
    ("Status of Completion",    "status_of_completion", 26),
    ("Date of completion",      "date_of_completion",   27),
    ("Status of Testing",       "status_of_testing",    28),
    ("Test Results",            "test_results",         29),
    ("Test Status",             "test_status",          30),
    ("Status",                  "status",               31),
    ("Ad ID",                   "ad_id",                32),
    ("Date",                    "ad_launch_date",       33),
    ("Impressions",             "impressions",          34),
    ("CAC",                     "cac",                  35),
    ("Links 1",                 "link_1",               36),
    ("Links 2",                 "link_2",               37),
    ("Links 3",                 "link_3",               38),
    # 39, 40 blank
    ("Summary Status",          "summary_status",       41),
    ("Summary Result",          "summary_result",       42),
]

TABLE = "public.content_graphic_register"

def _clean(v):
    """Strip, normalise sheet placeholders + scientific-notation ad_ids."""
    v = (v or "").strip()
    if v in ("--", ""): return None
    return v

def _norm_ad_id(v):
    v = _clean(v) or ""
    if re.fullmatch(r"\d{5,}", v): return v
    try:
        f = float(v.replace(",", ""))
        if f > 1e10 and f == int(f): return str(int(f))
    except Exception: pass
    return v or None

def _parse_int(v):
    v = _clean(v)
    if v is None: return None
    v = v.replace(",", "")
    try: return int(float(v))
    except Exception: return None

def _parse_date(v):
    v = _clean(v)
    if v is None: return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d %b %Y"):
        try: return datetime.strptime(v, fmt).date()
        except Exception: continue
    return None

INT_COLS  = {"count_9_16","count_4_5","count_16_9","count_1_1","total_count","impressions"}
DATE_COLS = {"asset_date","due_date","date_of_completion","ad_launch_date"}
NUM_COLS  = {"cac"}

def _coerce(col, v):
    if col == "ad_id":        return _norm_ad_id(v)
    if col in INT_COLS:       return _parse_int(v)
    if col in DATE_COLS:      return _parse_date(v)
    if col in NUM_COLS:
        v = _clean(v);
        if v is None: return None
        try: return float(v.replace(",",""))
        except Exception: return None
    return _clean(v)

DDL = f"""
create table if not exists {TABLE} (
    requisition_id     text primary key,
    asset_date         date,
    priority           text,
    creative           text,
    nomenclature       text,
    reference_links    text,
    product            text,
    audience_type      text,
    graphic_type       text,
    key_message        text,
    things_to_note     text,
    objective          text,
    demographic        text,
    who_is_this_for    text,
    visuals            text,
    count_9_16         integer,
    count_4_5          integer,
    count_16_9         integer,
    count_1_1          integer,
    total_count        integer,
    platform           text,
    assignee           text,
    catchphrase_main   text,
    catchphrase_sub    text,
    due_date           date,
    status_of_completion text,
    date_of_completion date,
    status_of_testing  text,
    test_results       text,
    test_status        text,
    status             text,
    ad_id              text,
    ad_launch_date     date,
    impressions        bigint,
    cac                numeric,
    link_1             text,
    link_2             text,
    link_3             text,
    summary_status     text,
    summary_result     text,
    mirrored_at        timestamptz not null default now()
);
create index if not exists content_graphic_register_ad_id_idx     on {TABLE} (ad_id);
create index if not exists content_graphic_register_status_idx    on {TABLE} (status_of_testing);
create index if not exists content_graphic_register_asset_date_idx on {TABLE} (asset_date desc);
"""

def fetch_csv():
    url = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
           f"/export?format=csv&gid={GID}")
    r = requests.get(url, timeout=60, allow_redirects=True)
    r.raise_for_status()
    if "text/csv" not in r.headers.get("Content-Type", ""):
        raise RuntimeError(f"Not a CSV response: {r.headers.get('Content-Type')}")
    reader = csv.reader(io.StringIO(r.text))
    rows = list(reader)
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    t0 = datetime.now(timezone.utc)
    print(f"[{t0.isoformat()}] fetching Graphic sheet gid={GID}")
    rows = fetch_csv()
    print(f"[csv] {len(rows)} rows (incl. header)")
    if len(rows) < 2:
        sys.exit("[fatal] no data rows")

    # Parse each row into a dict keyed by table_column.
    parsed = []
    skipped_blank = 0
    for r in rows[1:]:  # skip header
        # Pad short rows (last row may have trailing blanks trimmed)
        if len(r) < 43:
            r = r + [""] * (43 - len(r))
        rec = {}
        for header, col, idx in COL_MAP:
            rec[col] = _coerce(col, r[idx] if idx < len(r) else None)
        if not rec["requisition_id"]:
            skipped_blank += 1
            continue
        parsed.append(rec)

    tested   = sum(1 for r in parsed if r.get("ad_id"))
    untested = len(parsed) - tested
    print(f"[parsed] total={len(parsed)}  tested(ad_id set)={tested}  untested={untested}"
          f"  skipped_blank={skipped_blank}")

    if args.dry_run:
        print("[dry-run] skipping DB writes"); return

    conn = psycopg2.connect(DB_URL, connect_timeout=30)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(DDL); conn.commit()
            print("[ddl] applied")
            cols = [c for _, c, _ in COL_MAP]
            cols_sql = ", ".join(cols)
            updates  = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "requisition_id")
            sql = (f"insert into {TABLE} ({cols_sql}) values %s "
                   f"on conflict (requisition_id) do update set {updates}, mirrored_at=now()")
            payload = [tuple(r[c] for c in cols) for r in parsed]
            # Dedup on requisition_id — sheet occasionally has repeated IDs;
            # last one wins (matches "on conflict" semantics but avoids
            # execute_values re-raising on duplicate PK within the same batch).
            seen = {}
            for row in payload:
                seen[row[cols.index("requisition_id")]] = row
            payload = list(seen.values())
            execute_values(cur, sql, payload, page_size=500)
            conn.commit()
            print(f"[upsert] {len(payload)} rows")

            cur.execute(f"select count(*) filter (where ad_id is null) as untested, "
                        f"count(*) filter (where ad_id is not null) as tested, "
                        f"count(*) as total from {TABLE}")
            u, t, tot = cur.fetchone()
            print(f"[verify] total={tot}  tested={t}  untested={u}")
    finally:
        conn.close()

    dt = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"[done] {dt:.1f}s")

if __name__ == "__main__":
    main()
