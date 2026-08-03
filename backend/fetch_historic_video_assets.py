"""fetch_historic_video_assets.py — mirror the 3 DEPRECATED video-side tabs
of CTP-Asset_Sheet_v1 into Meta_ads_data.public.content_historic_video_register.

Sheet: 17SAZ-WfyHTC8IxArSZlfqm6qKxcnqF2LJr82J9dT-_Y
Tabs:
  * Deprecated Unplanned Edited Content   gid 338805839  (16 cols)
      key = Unplanned Unique ID           link = Edited Video Link
  * Deprecated Completed File             gid 1432484794 (23 cols)
      key = Requisition ID                link = Upload File Link
  * Deprecated Planning                   gid 1699216846 (48 cols)
      key = Requisition ID                link = Final Video Link

Rules (matching the live Untested section):
  1. Keep only rows with at least one link URL.
  2. Tested = asset_id / nomenclature appears as substring in some
     primary_table.ad_name (same Python-side scan the live mirror uses).

Idempotent — safe to re-run.

USAGE
  python fetch_historic_video_assets.py                # full mirror
  python fetch_historic_video_assets.py --dry-run      # fetch + parse only
"""
from __future__ import annotations
import os, sys, csv, io, re, time, argparse, requests, psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(Path(__file__).parent / ".env", override=True)

SHEET_ID = "17SAZ-WfyHTC8IxArSZlfqm6qKxcnqF2LJr82J9dT-_Y"
DB_URL   = os.environ.get("SUPABASE_DB_URL", "").strip()
if not DB_URL:
    sys.exit("[fatal] SUPABASE_DB_URL missing")

# Per-tab column index maps → common row shape.
TABS = {
    "unplanned": {
        "gid": 338805839,
        "label": "Deprecated Unplanned Edited Content",
        "cols": {
            "asset_id":       0,    # Unplanned Unique ID
            "timestamp_raw":  1,
            "nomenclature":   2,
            "category":       3,
            "primary_channel":4,
            "channel_page":   5,
            "key_message":    6,
            "reference":      7,
            "production_type":8,
            "core_theme":     9,
            "demographic":   10,
            "audience_type": 11,
            "link":          12,   # Edited Video Link
            "edited_by":     13,
            "summary_status":14,
            "summary_result":15,
        },
    },
    "completed": {
        "gid": 1432484794,
        "label": "Deprecated Completed File",
        "cols": {
            "timestamp_raw": 0,
            "submitter":     1,
            "department":    2,
            "category":      3,   # 'Production (video)' etc.
            "action":        4,
            "file_type":     5,
            "asset_id":      6,   # Requisition ID
            "nomenclature":  7,
            "link":          8,   # Upload File Link
            "shoot_raw":     9,
            "shot_by":      10,
            "shoot_date":   11,
            "remarks":      12,
            "edited_by":    13,
            "summary_status":21,
            "summary_result":22,
        },
    },
    "planning": {
        "gid": 1699216846,
        "label": "Deprecated Planning",
        "cols": {
            "timestamp_raw":  0,   # Planning Date
            "asset_id":       1,   # Requisition ID (e.g. Jan-2)
            "execution_month":2,
            "nomenclature":   3,
            "collection":     4,
            "objective":      5,
            "core_theme":     6,
            "primary_channel":7,
            "channel_page":   9,
            "production_type":13,
            "audience_type": 18,
            "planning_link": 20,
            "planned_by":    22,
            "approval":      23,
            "edited_by":     39,
            "link":          31,   # Final Video Link
            "ad_id_sheet":   34,   # Ad ID column
            "shoot_date":    27,
            "summary_status":46,
            "summary_result":47,
        },
    },
}

TABLE = "public.content_historic_video_register"

DDL = f"""
create table if not exists {TABLE} (
    source_tab          text not null,
    asset_id            text not null,
    nomenclature        text,
    link                text,
    ad_id_sheet         text,
    timestamp_raw       text,
    category            text,
    primary_channel     text,
    channel_page        text,
    production_type     text,
    audience_type       text,
    demographic         text,
    edited_by           text,
    core_theme          text,
    key_message         text,
    summary_status      text,
    summary_result      text,
    computed_is_tested  boolean default false,
    matched_ad_id       text,
    matched_ad_name     text,
    mirrored_at         timestamptz not null default now(),
    primary key (source_tab, asset_id)
);
create index if not exists chvr_source_idx on {TABLE} (source_tab);
create index if not exists chvr_tested_idx on {TABLE} (computed_is_tested);
"""

# Same shared columns for every row; per-tab cols will fill what applies.
SHARED = ["source_tab","asset_id","nomenclature","link","ad_id_sheet","timestamp_raw",
          "category","primary_channel","channel_page","production_type","audience_type",
          "demographic","edited_by","core_theme","key_message",
          "summary_status","summary_result"]

def _clean(v):
    v = (v or "").strip()
    if v in ("", "--"): return None
    return v

def _is_url(v):
    v = (v or "").strip().lower()
    return v.startswith("http://") or v.startswith("https://")

def fetch_csv(gid):
    url = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
           f"/export?format=csv&gid={gid}")
    r = requests.get(url, timeout=60, allow_redirects=True)
    r.raise_for_status()
    if "text/csv" not in r.headers.get("Content-Type", ""):
        raise RuntimeError(f"gid {gid}: not CSV ({r.headers.get('Content-Type')})")
    return list(csv.reader(io.StringIO(r.text)))

def parse_tab(tab_key, spec):
    rows = fetch_csv(spec["gid"])
    n_total = len(rows) - 1 if rows else 0
    parsed, no_key, no_link = [], 0, 0
    cols = spec["cols"]
    max_idx = max(cols.values()) + 1
    for r in rows[1:]:  # skip header
        if len(r) < max_idx:
            r = r + [""] * (max_idx - len(r))
        rec = {"source_tab": tab_key}
        for k, idx in cols.items():
            rec[k] = _clean(r[idx])
        if not rec.get("asset_id"):
            no_key += 1; continue
        if not _is_url(rec.get("link")):
            no_link += 1; continue
        parsed.append(rec)
    print(f"  [{tab_key:9s}] rows={n_total:4d}  kept={len(parsed):4d}  "
          f"no_key={no_key:4d}  no_link_url={no_link:4d}")
    return parsed

def _compute_tested(cur):
    """Match asset_id / nomenclature substring against primary_table.ad_name."""
    cur.execute(f"select source_tab, asset_id, nomenclature from {TABLE} "
                f"where link is not null and link <> ''")
    assets = cur.fetchall()
    print(f"[compute] {len(assets)} link-bearing assets to match")

    cur.execute("select ad_id::text, ad_name from public.primary_table "
                "where ad_name is not null")
    ads = cur.fetchall()
    print(f"[compute] scanning {len(ads):,} ad_names")
    lc_ads = [(aid, name, name.lower()) for aid, name in ads]

    matches = {}
    for src, aid, nomen in assets:
        needles = []
        if aid   and len(aid)   >= 4: needles.append(aid.lower())
        if nomen and len(nomen) >= 4 and nomen.lower() not in needles:
            needles.append(nomen.lower())
        hit = None
        for needle in needles:
            for adid, name, name_lc in lc_ads:
                if needle in name_lc:
                    hit = (adid, name); break
            if hit: break
        if hit: matches[(src, aid)] = hit

    tested = len(matches)
    print(f"[compute] tested={tested}  untested={len(assets)-tested}")

    hits  = [(adid, name, src, aid) for (src, aid), (adid, name) in matches.items()]
    miss  = [(src, aid) for (src, aid, _) in assets if (src, aid) not in matches]
    execute_values(
        cur,
        f"""update {TABLE} c set
              computed_is_tested = true,
              matched_ad_id      = v.ad_id,
              matched_ad_name    = v.ad_name
             from (values %s) as v(ad_id, ad_name, source_tab, asset_id)
            where c.source_tab = v.source_tab and c.asset_id = v.asset_id""",
        hits, page_size=500,
    )
    if miss:
        execute_values(
            cur,
            f"""update {TABLE} c set
                  computed_is_tested = false,
                  matched_ad_id      = null,
                  matched_ad_name    = null
                 from (values %s) as v(source_tab, asset_id)
                where c.source_tab = v.source_tab and c.asset_id = v.asset_id""",
            miss, page_size=500,
        )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    t0 = datetime.now(timezone.utc)
    print(f"[{t0.isoformat()}] fetch_historic_video_assets")

    all_rows = []
    for tab_key, spec in TABS.items():
        print(f"\n[tab] {tab_key} — {spec['label']} (gid={spec['gid']})")
        rows = parse_tab(tab_key, spec)
        all_rows.extend(rows)

    print(f"\n[total] {len(all_rows)} link-bearing assets across all 3 tabs")

    if args.dry_run:
        print("[dry-run] skipping DB writes"); return

    conn = psycopg2.connect(DB_URL, connect_timeout=30)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(DDL); conn.commit()
            print("[ddl] applied")

            # Dedup within batch (same source_tab+asset_id) — last wins.
            seen = {}
            for r in all_rows:
                seen[(r["source_tab"], r["asset_id"])] = r
            payload = [tuple(r.get(c) for c in SHARED) for r in seen.values()]
            cols_sql = ", ".join(SHARED)
            updates  = ", ".join(f"{c}=excluded.{c}" for c in SHARED
                                 if c not in ("source_tab", "asset_id"))
            sql = (f"insert into {TABLE} ({cols_sql}) values %s "
                   f"on conflict (source_tab, asset_id) "
                   f"do update set {updates}, mirrored_at=now()")
            execute_values(cur, sql, payload, page_size=500)
            conn.commit()
            print(f"[upsert] {len(payload)} rows")

            # Purge rows where link is now empty (invariant).
            cur.execute(f"delete from {TABLE} where link is null or link=''")
            n_purged = cur.rowcount; conn.commit()
            print(f"[purge] removed {n_purged} rows without link")

            print("[compute] matching against primary_table.ad_name …")
            t_c = time.time()
            _compute_tested(cur); conn.commit()
            print(f"[compute] done in {time.time()-t_c:.1f}s")

            cur.execute(f"""
              select source_tab,
                     count(*) as total,
                     count(*) filter (where computed_is_tested)     as tested,
                     count(*) filter (where not computed_is_tested) as untested
                from {TABLE}
               group by source_tab order by source_tab
            """)
            for row in cur.fetchall():
                print(f"  {row[0]:9s}  total={row[1]:4d}  tested={row[2]:4d}  untested={row[3]:4d}")
    finally:
        conn.close()

    dt = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"[done] {dt:.1f}s")

if __name__ == "__main__":
    main()
