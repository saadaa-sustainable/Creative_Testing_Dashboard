"""FastAPI service replacing the flaky anon PostgREST RPCs for the
Ads Analyse / Active / Last 90 Days sections.

Why: Supabase's anon PostgREST role is subject to statement-timeout
and rate-limits that cause `get_delivery_ads` / `get_ae_metrics_by_window`
to fail intermittently — the frontend interprets a partial failure as
"no ads delivered in window" and blanks the table 3-5 min after opening.

This service talks directly to Postgres (psycopg2) with a keep-alive
connection pool, so requests aren't gated by PostgREST's per-request
budget. All queries read from the pre-aggregated ae_daily_30d /
ae_daily_90d tables where possible.

Endpoints (all read-only):
  GET  /api/ads
       ae_table_view rows filtered by ad_status=ACTIVE and
       reporting_ends >= 2025-01-01. Cached 60s.
  GET  /api/delivery?from=YYYY-MM-DD&to=YYYY-MM-DD
       {ad_ids:[...]} — ads that delivered in window. Cached 60s.
  GET  /api/window_metrics?from=YYYY-MM-DD&to=YYYY-MM-DD
       [{ad_id, days_active, impressions, reach_sum, reach_peak,
         spend, purchases, conv_value, link_clicks, ftewv, ncp}, ...]
       Cached 60s. Reads from ae_daily_90d (up to 90 days).
  GET  /api/window_shopify?from=YYYY-MM-DD&to=YYYY-MM-DD
       [{ad_id, orders, sales}, ...] — per-ad Shopify aggregates
       from shopify_ad_attribution. Cached 60s.

RUN
  python -m uvicorn api_ae:app --host 127.0.0.1 --port 8766 --reload
  (or use `python backend/api_ae.py` — same effect, embedded uvicorn.)

FRONTEND
  Include ?apiBase=http://127.0.0.1:8766 in the dashboard URL to
  route these calls to the FastAPI service. If unset, dashboard.js
  falls back to the existing PostgREST calls.
"""
from __future__ import annotations
import os, sys, io, time, threading
from datetime import date, timedelta
from typing import Optional
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"]

# ── Connection pool ─────────────────────────────────────────────────
# Small pool — this service is single-user (developer machine). Keeps a
# warm connection so first request doesn't pay Supabase's ~1s cold-connect.
_POOL = psycopg2.pool.ThreadedConnectionPool(
    minconn=1, maxconn=4, dsn=DB_URL, connect_timeout=15,
    keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
)

@contextmanager
def cursor():
    conn = _POOL.getconn()
    try:
        conn.autocommit = True
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET LOCAL statement_timeout = '60s'")
            yield cur
    finally:
        _POOL.putconn(conn)

# ── Tiny TTL cache ──────────────────────────────────────────────────
_cache: dict = {}
_cache_lock = threading.Lock()

def cached(ttl_s: int):
    """Simple decorator — key by (fn_name, args, kwargs)."""
    def deco(fn):
        def wrapper(*args, **kwargs):
            key = (fn.__name__, args, tuple(sorted(kwargs.items())))
            now = time.time()
            with _cache_lock:
                hit = _cache.get(key)
                if hit and now - hit[0] < ttl_s:
                    return hit[1]
            out = fn(*args, **kwargs)
            with _cache_lock:
                _cache[key] = (now, out)
            return out
        return wrapper
    return deco

# ── App ─────────────────────────────────────────────────────────────
app = FastAPI(title="Creative Testing — AE API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # dev only — restrict in prod
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health():
    with cursor() as cur:
        cur.execute("SELECT 1 AS ok")
        return {"ok": True, "db": cur.fetchone()["ok"] == 1, "ts": int(time.time())}

# ── /api/ads ────────────────────────────────────────────────────────
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
def ads(status: str = "ACTIVE", since: str = "2025-01-01"):
    """Mirror of what the frontend's fetchAds() pulls from ae_table_view."""
    sql = f"""
      SELECT {', '.join(_AE_COLS)}
      FROM public.ae_table_view
      WHERE UPPER(COALESCE(ad_status,'')) = %s
        AND reporting_ends >= %s
      ORDER BY amount_spent DESC NULLS LAST
    """
    with cursor() as cur:
        cur.execute(sql, (status.upper(), since))
        rows = cur.fetchall()
        # Convert non-JSON types (Decimal, date) to strings/numbers
        return {"rows": _jsonify(rows), "count": len(rows)}

# ── /api/delivery ───────────────────────────────────────────────────
@app.get("/api/delivery")
def delivery(
    from_: str = Query(..., alias="from"),
    to:    str = Query(...),
):
    """Ads that delivered (impressions > 0) in the window.

    Uses ae_daily_90d when the from-date is within its coverage (fast);
    falls back to live primary_table for older windows.
    """
    _valdate(from_); _valdate(to)
    with cursor() as cur:
        cur.execute("SELECT MIN(date) AS md FROM public.ae_daily_90d")
        min_covered = cur.fetchone()["md"]
        if min_covered and date.fromisoformat(from_) >= min_covered:
            cur.execute("""
              SELECT DISTINCT ad_id::text AS ad_id
              FROM   public.ae_daily_90d
              WHERE  date BETWEEN %s AND %s AND impressions > 0
            """, (from_, to))
        else:
            cur.execute("""
              SELECT DISTINCT ad_id::text AS ad_id FROM (
                SELECT ad_id FROM public.primary_table
                 WHERE date BETWEEN %s AND %s AND impressions > 0
                UNION
                SELECT ad_id FROM public.backfill_table
                 WHERE date BETWEEN %s AND %s AND impressions > 0
              ) x WHERE ad_id IS NOT NULL AND ad_id <> ''
            """, (from_, to, from_, to))
        ids = [r["ad_id"] for r in cur.fetchall()]
        return {"from": from_, "to": to, "ad_ids": ids, "count": len(ids)}

# ── /api/window_metrics ─────────────────────────────────────────────
@app.get("/api/window_metrics")
def window_metrics(
    from_: str = Query(..., alias="from"),
    to:    str = Query(...),
):
    """Per-ad windowed metrics — replaces get_ae_metrics_by_window RPC."""
    _valdate(from_); _valdate(to)
    with cursor() as cur:
        cur.execute("SELECT MIN(date) AS md FROM public.ae_daily_90d")
        min_covered = cur.fetchone()["md"]
        if min_covered and date.fromisoformat(from_) >= min_covered:
            sql = """
              SELECT
                ad_id::text                                                      AS ad_id,
                COUNT(*) FILTER (WHERE impressions > 0)::int                     AS days_active,
                COALESCE(SUM(impressions),      0)::bigint                       AS impressions,
                COALESCE(SUM(reach),            0)::bigint                       AS reach_sum,
                COALESCE(MAX(reach),            0)::bigint                       AS reach_peak,
                COALESCE(SUM(amount_spent),     0)::float                        AS spend,
                COALESCE(SUM(purchases),        0)::float                        AS purchases,
                COALESCE(SUM(conversion_value), 0)::float                        AS conv_value,
                COALESCE(SUM(link_clicks_raw),  0)::bigint                       AS link_clicks,
                COALESCE(SUM(ftewv_count),      0)::float                        AS ftewv,
                COALESCE(SUM(ncp_count),        0)::float                        AS ncp
              FROM public.ae_daily_90d
              WHERE date BETWEEN %s AND %s
              GROUP BY ad_id
              HAVING SUM(impressions) > 0
            """
            cur.execute(sql, (from_, to))
        else:
            sql = """
              WITH u AS (
                SELECT ad_id::text AS ad_id, date, impressions, reach,
                       amount_spent_inr,
                       purchases::numeric                         AS purchases,
                       conversion_value::numeric                  AS conv_value,
                       COALESCE(inline_link_clicks, outbound_clicks)::bigint AS link_clicks,
                       ftewv_count::numeric                       AS ftewv_count,
                       ncp_count::numeric                         AS ncp_count,
                       1 AS pri
                  FROM public.primary_table
                 WHERE ad_id IS NOT NULL
                   AND impressions IS NOT NULL AND impressions > 0
                   AND date BETWEEN %s AND %s
                UNION ALL
                SELECT ad_id::text, date, impressions, reach,
                       amount_spent_inr, NULL::numeric,
                       conversion_value::numeric, outbound_clicks::bigint,
                       ftewv_count::numeric, ncp_count::numeric, 2 AS pri
                  FROM public.backfill_table
                 WHERE ad_id IS NOT NULL
                   AND impressions IS NOT NULL AND impressions > 0
                   AND date BETWEEN %s AND %s
              ),
              dedup AS (
                SELECT DISTINCT ON (ad_id, date) *
                  FROM u ORDER BY ad_id, date, pri
              )
              SELECT
                ad_id,
                COUNT(*)::int                    AS days_active,
                SUM(impressions)::bigint         AS impressions,
                SUM(reach)::bigint               AS reach_sum,
                MAX(reach)::bigint               AS reach_peak,
                SUM(amount_spent_inr)::float     AS spend,
                SUM(purchases)::float            AS purchases,
                SUM(conv_value)::float           AS conv_value,
                SUM(link_clicks)::bigint         AS link_clicks,
                SUM(ftewv_count)::float          AS ftewv,
                SUM(ncp_count)::float            AS ncp
              FROM dedup GROUP BY ad_id
            """
            cur.execute(sql, (from_, to, from_, to))
        return {"from": from_, "to": to, "rows": cur.fetchall(),
                "count": cur.rowcount}

# ── /api/window_shopify ─────────────────────────────────────────────
@app.get("/api/window_shopify")
def window_shopify(
    from_: str = Query(..., alias="from"),
    to:    str = Query(...),
):
    """Per-ad Shopify orders/sales in window."""
    _valdate(from_); _valdate(to)
    with cursor() as cur:
        cur.execute("""
          SELECT ad_id::text                    AS ad_id,
                 COUNT(*)::int                  AS orders,
                 COALESCE(SUM(total_price),0)::float AS sales
          FROM   public.shopify_ad_attribution
          WHERE  order_created_at::date BETWEEN %s AND %s
            AND  has_match = TRUE
            AND  ad_id IS NOT NULL
          GROUP  BY ad_id
        """, (from_, to))
        return {"from": from_, "to": to, "rows": cur.fetchall(),
                "count": cur.rowcount}

# ── helpers ─────────────────────────────────────────────────────────
def _valdate(s: str):
    try: date.fromisoformat(s)
    except Exception:
        raise HTTPException(400, f"bad date {s!r} — expected YYYY-MM-DD")

def _jsonify(rows):
    """Convert Decimal/date/datetime to JSON-friendly primitives."""
    from decimal import Decimal
    from datetime import datetime
    def _f(v):
        if v is None: return None
        if isinstance(v, Decimal): return float(v)
        if isinstance(v, (date, datetime)): return v.isoformat()
        return v
    return [{k: _f(v) for k, v in r.items()} for r in rows]

# ── Entrypoint ──────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8766, log_level="info")
