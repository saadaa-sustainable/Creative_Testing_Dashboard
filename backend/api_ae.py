"""Universal FastAPI gateway that replaces the anon Supabase PostgREST layer.

Two service surfaces:

  1. `/rest/v1/*`  — PostgREST-compatible pass-through.
       GET  /rest/v1/{table}?select=…&col=eq.X&order=…&limit=…
       POST /rest/v1/rpc/{name}   body = {arg1: v1, arg2: v2, …}
     Supports the PostgREST filter grammar used by dashboard.js:
       col=eq.X   neq.X   gt.X   gte.X   lt.X   lte.X
       col=like.X ilike.X in.(a,b,c)  is.null   not.is.null
     Plus  select, order (col.asc/desc[.nullslast]), limit, offset,
     Range header, Prefer: count=exact.

  2. `/api/*` — legacy hand-rolled endpoints kept for the AE section
     (pre-aggregated with the fast ae_daily_90d path). No change.

Frontend usage:
  Load with  ?apiBase=http://127.0.0.1:8766  and dashboard.js flips
  its SUPABASE_URL to this gateway — every /rest/v1/* call the
  dashboard already makes gets served here by direct psycopg2 hits.

Why: Supabase's anon PostgREST is subject to per-role statement
timeouts and rate-limits that intermittently blank the dashboard.
This gateway hits Postgres directly with a connection pool.
"""
from __future__ import annotations
import os, sys, io, re, time, json, threading, urllib.parse
from datetime import date, datetime
from decimal import Decimal
from contextlib import contextmanager
from typing import Any

import psycopg2
from psycopg2 import pool as _pool
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DB_URL = os.environ["SUPABASE_DB_URL"]

_POOL = _pool.ThreadedConnectionPool(
    minconn=1, maxconn=8, dsn=DB_URL, connect_timeout=15,
    keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
)

@contextmanager
def cursor():
    conn = _POOL.getconn()
    try:
        conn.autocommit = True
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET LOCAL statement_timeout = '120s'")
            yield cur
    finally:
        _POOL.putconn(conn)

app = FastAPI(title="Creative Testing — PostgREST Gateway", version="2.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET","POST"],
    allow_headers=["*"], expose_headers=["Content-Range","Content-Type"],
)

# ── JSON coercion ────────────────────────────────────────────────────
def _jsonify(v):
    if isinstance(v, dict):  return {k: _jsonify(x) for k, x in v.items()}
    if isinstance(v, list):  return [_jsonify(x) for x in v]
    if isinstance(v, Decimal):        return float(v)
    if isinstance(v, (date, datetime)): return v.isoformat()
    return v

# ── health ───────────────────────────────────────────────────────────
@app.get("/api/health")
def health():
    with cursor() as cur:
        cur.execute("SELECT 1 AS ok")
        return {"ok": True, "db": cur.fetchone()["ok"] == 1, "ts": int(time.time())}

# ─── PostgREST-compatible pass-through ──────────────────────────────────
# Whitelist tables to prevent arbitrary DB access. Everything the dashboard
# reads goes here; add new tables as they come online.
_ALLOWED_TABLES = {
    "ae_table_view", "ae_raw_view", "ae_shopify_enriched", "ae_daily_30d",
    "ae_daily_90d", "ae_reach_recent", "ae_freq_lifecycle",
    "primary_table", "backfill_table",
    "results_table", "summary_table",
    "shopify_ad_attribution", "shopify_ad_agg",
    "ad_thumbnails", "ad_asset_ids", "ad_ctype_overrides",
    "content_asset_register", "content_graphic_register",
    "content_historic_video_register",
    "cpis_by_sku", "cpis_daily_sales", "cpis_daily_ad_stats",
    "ireach_cumulative_daily", "ireach_campaign_daily",
    "new_incr_table", "new_incr_camp_table", "new_incr_adset_table",
    "product_doq_daily",
    "shopify_products",
    "ig_media", "ad_meta_lifetime",
    "ad_utm_mode", "shopify_ad_attribution_l30",
    "landing_page_analysis_30d", "landing_page_ad_breakdown_30d",
    "ad_attribution_overrides", "ad_results",
    "rck_daily_30d", "rck_last30",
}

# PostgREST filter opcode → (SQL op, value coercion)
# value coercion returns the SQL-side value and whether to add a placeholder
_OP = {
    "eq":   ("=",     lambda v: (v, True)),
    "neq":  ("!=",    lambda v: (v, True)),
    "gt":   (">",     lambda v: (v, True)),
    "gte":  (">=",    lambda v: (v, True)),
    "lt":   ("<",     lambda v: (v, True)),
    "lte":  ("<=",    lambda v: (v, True)),
    "like": ("LIKE",  lambda v: (v, True)),
    "ilike":("ILIKE", lambda v: (v, True)),
}

_RESERVED = {"select", "order", "limit", "offset", "on_conflict",
             "and", "or", "not"}

def _parse_in(v: str) -> list:
    # PostgREST in.(a,b,c)  — strip parens, split, strip quotes
    s = v.strip()
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
    out = []
    # Handle quoted strings that may contain commas.
    for part in re.findall(r'"([^"]*)"|([^,]+)', s):
        val = part[0] if part[0] else part[1]
        val = val.strip()
        if val:
            out.append(val.strip('"'))
    return out

def _build_where(qs_pairs, params: list) -> list[str]:
    """Translate query-string filters into SQL WHERE clauses.

    qs_pairs is a list of (key, value) tuples (preserves order + multi-value).
    Appends bind-params to `params`.
    """
    clauses = []
    for key, val in qs_pairs:
        if key in _RESERVED: continue
        if not val: continue
        # value format: `op.arg` or `op(arg)` — split on the first '.'
        if val == "null":
            # PostgREST allows shorthand col=null? actually always via is.null
            continue
        if val.startswith("not."):
            neg_val = val[4:]
            # not.is.null  → IS NOT NULL
            if neg_val == "is.null":
                clauses.append(f'"{key}" IS NOT NULL')
                continue
            # not.in.(…)   → NOT IN (…)
            if neg_val.startswith("in."):
                items = _parse_in(neg_val[3:])
                if items:
                    placeholders = ",".join(["%s"] * len(items))
                    clauses.append(f'"{key}" NOT IN ({placeholders})')
                    params.extend(items)
                continue
            # not.eq.X etc. — invert the op
            if "." in neg_val:
                op, arg = neg_val.split(".", 1)
                if op in _OP:
                    sql_op, coerce = _OP[op]
                    coerced, _ = coerce(arg)
                    clauses.append(f'NOT ("{key}" {sql_op} %s)')
                    params.append(coerced)
                    continue
            continue
        if val == "is.null":
            clauses.append(f'"{key}" IS NULL')
            continue
        if val.startswith("in."):
            items = _parse_in(val[3:])
            if items:
                placeholders = ",".join(["%s"] * len(items))
                clauses.append(f'"{key}" IN ({placeholders})')
                params.extend(items)
            continue
        if "." in val:
            op, arg = val.split(".", 1)
            if op in _OP:
                sql_op, coerce = _OP[op]
                coerced, _ = coerce(arg)
                clauses.append(f'"{key}" {sql_op} %s')
                params.append(coerced)
                continue
    return clauses

def _build_order(order_expr: str | None) -> str:
    """PostgREST order= grammar: col1.asc.nullsfirst,col2.desc.nullslast"""
    if not order_expr: return ""
    parts = []
    for tok in order_expr.split(","):
        bits = tok.strip().split(".")
        if not bits or not bits[0]: continue
        col = bits[0]
        direction = "ASC"
        nulls = ""
        for b in bits[1:]:
            b = b.lower()
            if b in ("asc","desc"): direction = b.upper()
            elif b == "nullsfirst": nulls = " NULLS FIRST"
            elif b == "nullslast":  nulls = " NULLS LAST"
        parts.append(f'"{col}" {direction}{nulls}')
    return " ORDER BY " + ", ".join(parts) if parts else ""

def _parse_range(header: str | None) -> tuple[int, int] | None:
    """PostgREST-style Range: 0-999 → (0, 999). Returns None if unset."""
    if not header: return None
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", header)
    if not m: return None
    return int(m.group(1)), int(m.group(2))

@app.get("/rest/v1/{table}")
def rest_table(
    table: str,
    request: Request,
    response: Response,
):
    if table not in _ALLOWED_TABLES:
        raise HTTPException(404, f"table {table!r} not exposed")

    # Query-string parse (multi-value aware)
    raw_qs = request.url.query
    qs_pairs = urllib.parse.parse_qsl(raw_qs, keep_blank_values=False)
    select    = next((v for k,v in qs_pairs if k == "select"), "*")
    order     = next((v for k,v in qs_pairs if k == "order"), None)
    limit_qs  = next((v for k,v in qs_pairs if k == "limit"), None)
    offset_qs = next((v for k,v in qs_pairs if k == "offset"), None)

    # Range header → (offset, limit)
    range_hdr = request.headers.get("range") or request.headers.get("Range")
    rng = _parse_range(range_hdr)

    # Build WHERE
    where_params: list = []
    where_clauses = _build_where(qs_pairs, where_params)
    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # Column projection
    if select == "*":
        cols_sql = "*"
    else:
        # PostgREST allows `col1,col2` — pass through simple identifiers
        parts = [c.strip() for c in select.split(",") if c.strip()]
        # Guard against injection: only [a-z_][a-z0-9_]*
        for p in parts:
            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", p):
                raise HTTPException(400, f"bad select item {p!r}")
        cols_sql = ", ".join(f'"{c}"' for c in parts)

    order_sql = _build_order(order)

    # Pagination
    limit_val: int | None = None
    offset_val: int = 0
    if rng is not None:
        offset_val = rng[0]
        limit_val  = (rng[1] - rng[0] + 1)
    if limit_qs is not None:
        try: limit_val = int(limit_qs)
        except: pass
    if offset_qs is not None:
        try: offset_val = int(offset_qs)
        except: pass
    limit_sql = f" LIMIT {limit_val}" if limit_val else ""
    offset_sql = f" OFFSET {offset_val}" if offset_val else ""

    sql = f'SELECT {cols_sql} FROM "public"."{table}"{where_sql}{order_sql}{limit_sql}{offset_sql}'

    # Optional count (Prefer: count=exact) — set Content-Range header.
    prefer = (request.headers.get("prefer") or request.headers.get("Prefer") or "").lower()
    want_count = "count=exact" in prefer

    with cursor() as cur:
        cur.execute(sql, tuple(where_params))
        rows = cur.fetchall()
        total_str = "*"
        if want_count:
            cur.execute(f'SELECT COUNT(*)::bigint AS n FROM "public"."{table}"{where_sql}', tuple(where_params))
            total_str = str(cur.fetchone()["n"])

    lo = offset_val
    hi = offset_val + len(rows) - 1 if rows else offset_val
    response.headers["Content-Range"] = f"{lo}-{hi}/{total_str}"
    response.headers["Content-Type"] = "application/json"
    return _jsonify(rows)

@app.post("/rest/v1/rpc/{fn}")
async def rest_rpc(fn: str, request: Request):
    # RPC allow-list — restrict to functions the dashboard actually calls.
    ALLOWED_RPCS = {
        "get_ae_metrics_by_window", "get_delivery_ads",
        "get_ireach_incremental_analysis", "get_ireach_saturation_curve",
        "get_overview_perf_totals", "get_shopify_by_adset",
        "get_shopify_by_campaign", "get_cpis_ad_stats",
        "get_sessions_by_lp",
        # Refresh functions — dashboard's "Recompute" buttons.
        "refresh_landing_page_analysis_30d",
    }
    if fn not in ALLOWED_RPCS:
        raise HTTPException(404, f"rpc {fn!r} not exposed")
    body = {}
    try:
        raw = await request.body()
        if raw:
            body = json.loads(raw)
    except Exception:
        body = {}
    # Build a parameterised call: SELECT * FROM public.fn(k1 => %s, k2 => %s, ...)
    if body:
        keys = list(body.keys())
        placeholders = ", ".join(f'"{k}" => %s' for k in keys)
        params = tuple(body[k] for k in keys)
    else:
        placeholders = ""
        params = ()
    sql = f'SELECT * FROM public."{fn}"({placeholders})'
    with cursor() as cur:
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
        except psycopg2.Error as e:
            raise HTTPException(400, f"rpc {fn} failed: {e.pgerror or str(e)[:300]}")
        return _jsonify(rows)

# ─────────────────────────────────────────────────────────────────────
# Legacy hand-rolled /api/* endpoints kept for the AE section — these
# use the fast pre-aggregated ae_daily_90d path.
# ─────────────────────────────────────────────────────────────────────
_AE_COLS = [
    "account_name","campaign_name","adset_id","adset_name","ad_id","ad_name","ad_created",
    "first_seen_date","reporting_starts","reporting_ends","date_target_imp_achieved",
    "date_of_result","days_to_result","days_to_target_f1",
    "ad_status","category","f1_pass","f2_pass","f3_pass","f4_pass",
    "impressions","reach","reach_weight_pct","frequency","ltv_reach","ltv_frequency",
    "amount_spent","cost_per_1000","cpc_link","ctr_pct","link_clicks_raw",
    "checkout_compl_pct","cr_link_clicks_pct","atc_lc_pct","atc_count",
    "ci_atc_pct","ci_count","roas_ma","ftewv_count","pct_reach_ftewv",
    "cost_per_ftewv","cost_per_ncp","ncp_count","conv_value","purchases",
    "profit_efficiency","contrib_margin_pct","delivery_eff","sales_spend_eff",
    "blended_eff","cpr_eff","ftv_contrib_eff","ftev_volume",
    "ncp_cost_eff","roas_eff","profit_vol_eff","engagement_count",
    "preview_link","ad_link",
    "shopify_orders","shopify_sales","shopify_top_tier","shopify_roas",
]

@app.get("/api/ads")
def ads(status: str | None = None, since: str = "2025-01-01"):
    where = ["reporting_ends >= %s"]
    params: list = [since]
    if status:
        where.append("UPPER(COALESCE(ad_status,'')) = %s")
        params.append(status.upper())
    sql = f"""SELECT {', '.join(_AE_COLS)}
              FROM public.ae_table_view
              WHERE {' AND '.join(where)}
              ORDER BY amount_spent DESC NULLS LAST"""
    with cursor() as cur:
        cur.execute(sql, tuple(params))
        return {"rows": _jsonify(cur.fetchall()), "count": cur.rowcount}

def _valdate(s: str):
    try: date.fromisoformat(s)
    except Exception:
        raise HTTPException(400, f"bad date {s!r} — expected YYYY-MM-DD")

@app.get("/api/delivery")
def delivery(from_: str = Query(..., alias="from"), to: str = Query(...)):
    _valdate(from_); _valdate(to)
    with cursor() as cur:
        cur.execute("SELECT MIN(date) AS md FROM public.ae_daily_90d")
        min_covered = cur.fetchone()["md"]
        if min_covered and date.fromisoformat(from_) >= min_covered:
            cur.execute("""SELECT DISTINCT ad_id::text AS ad_id
                           FROM   public.ae_daily_90d
                           WHERE  date BETWEEN %s AND %s AND impressions > 0""",
                        (from_, to))
        else:
            cur.execute("""SELECT DISTINCT ad_id::text AS ad_id FROM (
                             SELECT ad_id FROM public.primary_table
                              WHERE date BETWEEN %s AND %s AND impressions > 0
                             UNION
                             SELECT ad_id FROM public.backfill_table
                              WHERE date BETWEEN %s AND %s AND impressions > 0
                           ) x WHERE ad_id IS NOT NULL AND ad_id <> ''""",
                        (from_, to, from_, to))
        ids = [r["ad_id"] for r in cur.fetchall()]
        return {"from": from_, "to": to, "ad_ids": ids, "count": len(ids)}

@app.get("/api/window_metrics")
def window_metrics(from_: str = Query(..., alias="from"), to: str = Query(...)):
    _valdate(from_); _valdate(to)
    with cursor() as cur:
        cur.execute("SELECT MIN(date) AS md FROM public.ae_daily_90d")
        min_covered = cur.fetchone()["md"]
        if min_covered and date.fromisoformat(from_) >= min_covered:
            cur.execute("""SELECT
                             ad_id::text                                    AS ad_id,
                             COUNT(*) FILTER (WHERE impressions > 0)::int   AS days_active,
                             COALESCE(SUM(impressions),      0)::bigint     AS impressions,
                             COALESCE(SUM(reach),            0)::bigint     AS reach_sum,
                             COALESCE(MAX(reach),            0)::bigint     AS reach_peak,
                             COALESCE(SUM(amount_spent),     0)::float      AS spend,
                             COALESCE(SUM(purchases),        0)::float      AS purchases,
                             COALESCE(SUM(conversion_value), 0)::float      AS conv_value,
                             COALESCE(SUM(link_clicks_raw),  0)::bigint     AS link_clicks,
                             COALESCE(SUM(ftewv_count),      0)::float      AS ftewv,
                             COALESCE(SUM(ncp_count),        0)::float      AS ncp
                           FROM public.ae_daily_90d
                           WHERE date BETWEEN %s AND %s
                           GROUP BY ad_id
                           HAVING SUM(impressions) > 0""", (from_, to))
        else:
            cur.execute("""WITH u AS (
                 SELECT ad_id::text AS ad_id, date, impressions, reach,
                        amount_spent_inr,
                        purchases::numeric AS purchases,
                        conversion_value::numeric AS conv_value,
                        COALESCE(inline_link_clicks, outbound_clicks)::bigint AS link_clicks,
                        ftewv_count::numeric AS ftewv_count,
                        ncp_count::numeric   AS ncp_count, 1 AS pri
                   FROM public.primary_table
                  WHERE ad_id IS NOT NULL AND impressions IS NOT NULL AND impressions > 0
                    AND date BETWEEN %s AND %s
                 UNION ALL
                 SELECT ad_id::text, date, impressions, reach, amount_spent_inr,
                        NULL::numeric, conversion_value::numeric,
                        outbound_clicks::bigint, ftewv_count::numeric,
                        ncp_count::numeric, 2 AS pri
                   FROM public.backfill_table
                  WHERE ad_id IS NOT NULL AND impressions IS NOT NULL AND impressions > 0
                    AND date BETWEEN %s AND %s
               ), dedup AS (
                 SELECT DISTINCT ON (ad_id, date) *
                   FROM u ORDER BY ad_id, date, pri
               )
               SELECT ad_id, COUNT(*)::int AS days_active,
                      SUM(impressions)::bigint AS impressions,
                      SUM(reach)::bigint AS reach_sum,
                      MAX(reach)::bigint AS reach_peak,
                      SUM(amount_spent_inr)::float AS spend,
                      SUM(purchases)::float AS purchases,
                      SUM(conv_value)::float AS conv_value,
                      SUM(link_clicks)::bigint AS link_clicks,
                      SUM(ftewv_count)::float AS ftewv,
                      SUM(ncp_count)::float AS ncp
                 FROM dedup GROUP BY ad_id""", (from_, to, from_, to))
        return {"from": from_, "to": to, "rows": cur.fetchall(), "count": cur.rowcount}

@app.get("/api/window_shopify")
def window_shopify(from_: str = Query(..., alias="from"), to: str = Query(...)):
    _valdate(from_); _valdate(to)
    with cursor() as cur:
        cur.execute("""SELECT ad_id::text AS ad_id,
                              COUNT(*)::int AS orders,
                              COALESCE(SUM(total_price),0)::float AS sales
                       FROM   public.shopify_ad_attribution
                       WHERE  order_created_at::date BETWEEN %s AND %s
                         AND  has_match = TRUE AND ad_id IS NOT NULL
                       GROUP  BY ad_id""", (from_, to))
        return {"from": from_, "to": to, "rows": cur.fetchall(), "count": cur.rowcount}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8766, log_level="info")
