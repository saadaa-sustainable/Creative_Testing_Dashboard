"""_snapshot_tier1_drops.py — dump Tier-1 drop candidates to CSV before
the DROP statements run. Rollback insurance: if we ever need one of these
back we can COPY FROM the CSV.

Tier-1 targets (see task #73):
  Tables (9): shopify_ad_attribution_v2, _v2_a3name_map, _v2_adset_ad,
              _v2_all_names, _v2_all_ids, _v2_adset_meta, inventory_snapshot,
              ae_freq_lifecycle_mat, google_ads_daily
  Views (3): order_utm, ae_freq_lifecycle, ig_media_by_account
  RPCs (3):  refresh_ae_freq_lifecycle, get_landing_page_analysis,
             get_reach_by_window

Views/RPCs are dumped as CREATE ... statements (not data) so they can be
recreated verbatim.
"""
import os, io, sys, time, pathlib, psycopg2
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
load_dotenv(override=True)

OUT = pathlib.Path(__file__).parent / "db_snapshots" / "tier1_20260817"
OUT.mkdir(parents=True, exist_ok=True)

TABLES = [
    "shopify_ad_attribution_v2",
    "_v2_a3name_map",
    "_v2_adset_ad",
    "_v2_all_names",
    "_v2_all_ids",
    "_v2_adset_meta",
    "inventory_snapshot",
    "ae_freq_lifecycle_mat",
    "google_ads_daily",
]
VIEWS = ["order_utm", "ae_freq_lifecycle", "ig_media_by_account"]
RPCS  = ["refresh_ae_freq_lifecycle", "get_landing_page_analysis", "get_reach_by_window"]

conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"], connect_timeout=30, keepalives=1)
conn.autocommit = True
cur = conn.cursor()

manifest_lines = [f"# Tier-1 snapshot  {time.strftime('%Y-%m-%d %H:%M:%S')}\n"]

# 1. Tables → CSV
for t in TABLES:
    csv_path = OUT / f"{t}.csv"
    ddl_path = OUT / f"{t}.ddl.sql"
    try:
        cur.execute(f"SELECT count(*) FROM public.{t}")
        n = cur.fetchone()[0]

        # Capture DDL (pg_get_tabledef is not a builtin; assemble from information_schema)
        cur.execute(f"""
            SELECT 'CREATE TABLE public.{t} (' || string_agg(
                column_name || ' ' || data_type
                || CASE WHEN character_maximum_length IS NOT NULL
                        THEN '(' || character_maximum_length || ')' ELSE '' END
                || CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END,
                ', ' ORDER BY ordinal_position
            ) || ');'
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name = %s
        """, (t,))
        row = cur.fetchone()
        ddl_path.write_text(row[0] if row and row[0] else f"-- no DDL for {t}\n", encoding='utf-8')

        with open(csv_path, "wb") as f:
            cur.copy_expert(f"COPY public.{t} TO STDOUT WITH CSV HEADER", f)
        size = csv_path.stat().st_size
        line = f"  TABLE {t:<32} rows={n:>10,}  csv={size:>12,} bytes"
        print(line); manifest_lines.append(line + "\n")
    except Exception as e:
        line = f"  TABLE {t:<32} FAIL {type(e).__name__}: {e}"
        print(line); manifest_lines.append(line + "\n")

# 2. Views → definition
for v in VIEWS:
    p = OUT / f"{v}.view.sql"
    try:
        cur.execute("SELECT pg_get_viewdef(%s::regclass, true)", (f"public.{v}",))
        row = cur.fetchone()
        body = row[0] if row else ""
        p.write_text(f"CREATE OR REPLACE VIEW public.{v} AS\n{body};\n", encoding='utf-8')
        line = f"  VIEW  {v:<32} definition captured ({len(body):>6} chars)"
        print(line); manifest_lines.append(line + "\n")
    except Exception as e:
        line = f"  VIEW  {v:<32} FAIL {type(e).__name__}: {e}"
        print(line); manifest_lines.append(line + "\n")

# 3. RPCs → CREATE OR REPLACE FUNCTION
for r in RPCS:
    p = OUT / f"{r}.func.sql"
    try:
        cur.execute("""
            SELECT pg_get_functiondef(p.oid)
              FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
             WHERE n.nspname='public' AND p.proname=%s
        """, (r,))
        rows = cur.fetchall()
        if not rows:
            line = f"  RPC   {r:<32} NOT FOUND"
        else:
            with open(p, "w", encoding='utf-8') as f:
                for i, (body,) in enumerate(rows, 1):
                    f.write(f"-- overload {i}\n{body}\n\n")
            line = f"  RPC   {r:<32} {len(rows)} overload(s) captured"
        print(line); manifest_lines.append(line + "\n")
    except Exception as e:
        line = f"  RPC   {r:<32} FAIL {type(e).__name__}: {e}"
        print(line); manifest_lines.append(line + "\n")

(OUT / "_manifest.txt").write_text("".join(manifest_lines), encoding='utf-8')
print(f"\n[DONE] snapshot in {OUT}")
cur.close(); conn.close()
