"""fetch_content_asset_register.py — mirror asset_register from the
content_workflow_optimiser Supabase project into Meta_ads_data as
public.content_asset_register, so the dashboard can render an
"Untested Assets" section without cross-project auth.

Source : CONTENT_WORKFLOW_URL / CONTENT_SERVICE_KEY  (npxpywozmrptzzytzdmg)
Target : SUPABASE_DB_URL                             (rtkohjfzyzhizkebdsuy — Meta_ads_data)

Tested rule (dashboard-side):
    tested   = (ad_id IS NOT NULL)
    untested = (ad_id IS NULL)

Idempotent — table is CREATE IF NOT EXISTS, rows are UPSERTed by asset_id.

USAGE
  python fetch_content_asset_register.py                  # full mirror
  python fetch_content_asset_register.py --dry-run        # fetch, no writes
"""
from __future__ import annotations
import os, sys, json, time, argparse, urllib.request, urllib.error
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(Path(__file__).parent / ".env", override=True)

CT_URL  = os.environ.get("CONTENT_WORKFLOW_URL", "").rstrip("/")
CT_KEY  = os.environ.get("CONTENT_SERVICE_KEY") or os.environ.get("CONTENT_ANON_KEY")
DB_URL  = os.environ.get("SUPABASE_DB_URL", "").strip()

if not (CT_URL and CT_KEY):
    sys.exit("[fatal] set CONTENT_WORKFLOW_URL + CONTENT_SERVICE_KEY in .env")
if not DB_URL:
    sys.exit("[fatal] SUPABASE_DB_URL missing")

# Columns to mirror from asset_register.
COLS = [
    "seq", "asset_id", "source_parent", "asset_type", "category",
    "planning_nomenclature", "link_to_asset", "origin",
    "creative_effort_type", "type_of_content",
    "date_of_production", "date_testing_ads", "date_testing_posting_ig",
    "ad_id", "is_test", "created_at", "source",
    "ads_name", "ads_result", "ads_status", "ads_count",
    "ads_impressions", "ads_spend", "ads_conv_value", "ads_roas",
    "ads_ncp", "ads_cost_per_ncp", "ads_ftewv", "ads_cost_per_ftewv",
    "ads_purchases", "ads_shopify_orders", "ads_shopify_sales",
    "ads_shopify_aov", "ads_shopify_roas", "ads_synced_at",
    "ads_testing_status", "ads_instagram_posted", "ads_instagram_permalink",
    "ads_reach", "ads_ctr", "ads_hook_rate", "ads_hold_rate",
    "ads_thruplay_rate",
]
# Derived brief-side columns — fetched from briefs & joined on asset_id so
# the dashboard can classify Graphic vs Video without a second round-trip.
# shoot_required='N' correlates with graphic assets (no shoot needed).
DERIVED_COLS = ["brief_shoot_required", "brief_aspect_ratio"]

# Postgres column types — text-heavy; numeric where obvious.
TYPES = {
    "seq": "integer",
    "asset_id": "text primary key",
    "source_parent": "text",
    "asset_type": "text",
    "category": "text",
    "planning_nomenclature": "text",
    "link_to_asset": "text",
    "origin": "text",
    "creative_effort_type": "text",
    "type_of_content": "text",
    "date_of_production": "date",
    "date_testing_ads": "date",
    "date_testing_posting_ig": "date",
    "ad_id": "text",
    "is_test": "boolean",
    "created_at": "timestamptz",
    "source": "text",
    "ads_name": "text",
    "ads_result": "text",
    "ads_status": "text",
    "ads_count": "integer",
    "ads_impressions": "bigint",
    "ads_spend": "numeric",
    "ads_conv_value": "numeric",
    "ads_roas": "numeric",
    "ads_ncp": "integer",
    "ads_cost_per_ncp": "numeric",
    "ads_ftewv": "integer",
    "ads_cost_per_ftewv": "numeric",
    "ads_purchases": "integer",
    "ads_shopify_orders": "integer",
    "ads_shopify_sales": "numeric",
    "ads_shopify_aov": "numeric",
    "ads_shopify_roas": "numeric",
    "ads_synced_at": "timestamptz",
    "ads_testing_status": "text",
    "ads_instagram_posted": "text",
    "ads_instagram_permalink": "text",
    "ads_reach": "bigint",
    "ads_ctr": "numeric",
    "ads_hook_rate": "numeric",
    "ads_hold_rate": "numeric",
    "ads_thruplay_rate": "numeric",
    "brief_shoot_required": "text",
    "brief_aspect_ratio":   "text",
}

DDL = f"""
create table if not exists public.content_asset_register (
    {', '.join(f'{c} {TYPES[c]}' for c in COLS + DERIVED_COLS)},
    mirrored_at timestamptz not null default now()
);
create index if not exists content_asset_register_ad_id_idx
    on public.content_asset_register (ad_id);
create index if not exists content_asset_register_source_parent_idx
    on public.content_asset_register (source_parent);
create index if not exists content_asset_register_created_at_idx
    on public.content_asset_register (created_at desc);
alter table public.content_asset_register
    add column if not exists brief_shoot_required text,
    add column if not exists brief_aspect_ratio   text,
    add column if not exists computed_is_tested   boolean default false,
    add column if not exists matched_ad_id        text,
    add column if not exists matched_ad_name      text;
"""

# Tested-status compute runs Python-side, not in SQL: an ILIKE cross-join
# between ~330 assets and ~200k primary_table rows blows past Supabase's
# 30-s statement timeout. Instead we fetch all ad_name rows once, do the
# substring match in memory (fast — Python C-string ops), then batch-UPDATE.
def _compute_tested(cur):
    cur.execute("""
        select asset_id from public.content_asset_register
         where link_to_asset is not null and link_to_asset <> ''
           and length(asset_id) >= 4
    """)
    assets = [r[0] for r in cur.fetchall()]
    print(f"[compute] {len(assets)} assets to match")

    cur.execute("select ad_id::text, ad_name from public.primary_table "
                "where ad_name is not null")
    ads = cur.fetchall()
    print(f"[compute] scanning {len(ads):,} ad_names")

    # Case-insensitive substring match — first hit wins.
    lc_ads = [(aid, name, name.lower()) for aid, name in ads]
    matches = {}
    for asset_id in assets:
        needle = asset_id.lower()
        for aid, name, name_lc in lc_ads:
            if needle in name_lc:
                matches[asset_id] = (aid, name)
                break

    tested = len(matches)
    untested = len(assets) - tested
    print(f"[compute] tested={tested}  untested={untested}")

    # Batch update — one round-trip per 500 assets.
    payload_hit  = [(aid, name, key) for key, (aid, name) in matches.items()]
    payload_miss = [(key,) for key in assets if key not in matches]
    execute_values(
        cur,
        """update public.content_asset_register c set
              computed_is_tested = true,
              matched_ad_id      = v.ad_id,
              matched_ad_name    = v.ad_name
             from (values %s) as v(ad_id, ad_name, asset_id)
            where c.asset_id = v.asset_id""",
        payload_hit, page_size=500,
    )
    execute_values(
        cur,
        """update public.content_asset_register c set
              computed_is_tested = false,
              matched_ad_id      = null,
              matched_ad_name    = null
             from (values %s) as v(asset_id)
            where c.asset_id = v.asset_id""",
        payload_miss, page_size=500,
    )

def http_get(path, extra=None, timeout=30):
    hdrs = {"apikey": CT_KEY, "Authorization": f"Bearer {CT_KEY}",
            "Accept": "application/json"}
    if extra: hdrs.update(extra)
    req = urllib.request.Request(f"{CT_URL}{path}", headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()

def _paginate(path, select_cols):
    """Generic paginator over a PostgREST endpoint."""
    all_rows = []
    page = 0
    PAGE = 1000
    while True:
        lo, hi = page * PAGE, (page + 1) * PAGE - 1
        st, hdrs, b = http_get(
            f"{path}?select={select_cols}",
            extra={"Range-Unit": "items", "Range": f"{lo}-{hi}",
                   "Prefer": "count=exact"},
        )
        if st not in (200, 206):
            raise RuntimeError(f"fetch failed {path} status={st} body={b[:300]}")
        rows = json.loads(b)
        cr = hdrs.get("Content-Range", "")
        all_rows.extend(rows)
        total = None
        if "/" in cr:
            try: total = int(cr.split("/", 1)[1])
            except Exception: pass
        print(f"  [{path} page {page}] fetched={len(rows)}  total_so_far={len(all_rows)}  {cr}", flush=True)
        if len(rows) < PAGE: break
        page += 1
        if total and len(all_rows) >= total: break
    return all_rows

def fetch_all():
    """Fetch asset_register + join briefs.shoot_required / aspect_ratio by asset_id."""
    reg = _paginate("/rest/v1/asset_register", ",".join(COLS) + "&order=seq.asc")
    print(f"[fetched] asset_register: {len(reg)} rows")
    briefs = _paginate("/rest/v1/briefs",
                       "asset_id,shoot_required,shoot_required_2,aspect_ratio")
    print(f"[fetched] briefs: {len(briefs)} rows")
    # Build asset_id → (shoot_required, aspect_ratio) map. shoot_required_2 is
    # a TRUE/FALSE second-pass column; prefer shoot_required, fall back to _2.
    bmap = {}
    for b in briefs:
        aid = b.get("asset_id")
        if not aid: continue
        sr = b.get("shoot_required")
        sr2 = b.get("shoot_required_2")
        if sr in (None, ""):
            if sr2 == "TRUE":  sr = "Y"
            elif sr2 == "FALSE": sr = "N"
        bmap[aid] = (sr, b.get("aspect_ratio"))
    for r in reg:
        sr, ar = bmap.get(r.get("asset_id"), (None, None))
        r["brief_shoot_required"] = sr
        r["brief_aspect_ratio"]   = ar
    return reg

def coerce_row(r):
    """Force column order + None for missing keys."""
    return tuple(r.get(c) for c in COLS + DERIVED_COLS)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="fetch + count only, no DB writes")
    args = ap.parse_args()

    t0 = datetime.now(timezone.utc)
    print(f"[{t0.isoformat()}] mirror start — src={CT_URL} → Meta_ads_data.public.content_asset_register")

    rows_all = fetch_all()
    # Rule: an asset only counts if it has a link_to_asset (Drive URL);
    # rows without a link aren't real assets, they're placeholders.
    rows = [r for r in rows_all if (r.get("link_to_asset") or "").strip()]
    dropped = len(rows_all) - len(rows)
    print(f"[filter] kept {len(rows)} with link_to_asset  ·  dropped {dropped} without link")

    if args.dry_run:
        print("[dry-run] skipping DB writes")
        return

    conn = psycopg2.connect(DB_URL, connect_timeout=30)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            conn.commit()
            print("[ddl] applied")

            cols_sql = ", ".join(COLS + DERIVED_COLS)
            updates  = ", ".join(f"{c}=excluded.{c}" for c in COLS + DERIVED_COLS if c != "asset_id")
            sql = (
                f"insert into public.content_asset_register ({cols_sql}) values %s "
                f"on conflict (asset_id) do update set {updates}, mirrored_at = now()"
            )
            payload = [coerce_row(r) for r in rows]
            execute_values(cur, sql, payload, page_size=500)
            conn.commit()
            print(f"[upsert] {len(payload)} rows")

            # Purge any pre-existing rows that no longer have a link (e.g. a
            # prior mirror stored placeholders). Keeps the table aligned
            # with the "must have link_to_asset" invariant.
            cur.execute("delete from public.content_asset_register "
                        "where link_to_asset is null or link_to_asset = ''")
            n_purged = cur.rowcount
            conn.commit()
            print(f"[purge] removed {n_purged} rows without link_to_asset")

            # Derive computed_is_tested — fetch all ad_names once,
            # substring-match in Python, then batch-UPDATE.
            print("[compute] matching asset_id → primary_table.ad_name …")
            t_compute = time.time()
            _compute_tested(cur)
            conn.commit()
            print(f"[compute] done in {time.time()-t_compute:.1f}s")

            cur.execute("""
              select count(*) as total,
                     count(*) filter (where computed_is_tested)          as tested,
                     count(*) filter (where not computed_is_tested)      as untested,
                     count(*) filter (where matched_ad_id is not null)   as with_matched_ad
                from public.content_asset_register
            """)
            tot, t, u, m = cur.fetchone()
            print(f"[verify] total={tot}  tested={t}  untested={u}  matched_ad_id_populated={m}")
    finally:
        conn.close()

    dt = (datetime.now(timezone.utc) - t0).total_seconds()
    print(f"[done] {dt:.1f}s")

if __name__ == "__main__":
    main()
