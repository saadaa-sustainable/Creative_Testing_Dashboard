"""_refresh_all_dashboard_data.py — one-shot orchestrator that refreshes
every table the Creative Testing Dashboard reads from.

Runs every step individually (not the _run_full_update.py black-box) so
the parent process controls timeouts + step ordering directly. Skips
fetch_ad_thumbnails — that one runs on its own daily cron.

Steps are grouped into 4 phases (dependency-correct):

  PHASE 1 — META FETCHERS (throttle-heavy, must run first)
     primary_sync, fetch_reach_incr, fetch_meta_ireach_daily,
     fetch_ig_media, fetch_ad_lifetime_purchases

  PHASE 2 — OTHER INGEST (parallel-safe, no Meta token)
     Google Ads, Shopify sessions/customers/products/orders,
     landing page sessions, cpis-by-sku, asset ID sheet,
     content registries, IG/creatorhub

  PHASE 3 — COMPUTATION (DB-only, depends on Phase 1+2)
     propagate, apply_ctp, refresh_ae, summary, ae_reach_recent,
     google_ads_summary, result_classifier, results_sync,
     rebuild_attribution, l30, session-utm rollups, ae_daily_90d,
     ads_delivered_daily, product_doq, rck_daily/last30

  PHASE 4 — LP RPCs (last, aggregate over freshly-refreshed data)
     refresh_landing_page_analysis_30d,
     refresh_landing_page_ad_breakdown_30d

Total wall time: ~90-150 min without thumbnails.

USAGE
  python _refresh_all_dashboard_data.py            # all 4 phases
  python _refresh_all_dashboard_data.py --phase meta      # only phase 1
  python _refresh_all_dashboard_data.py --phase ingest    # only phase 2
  python _refresh_all_dashboard_data.py --phase compute   # only phase 3+4
  python _refresh_all_dashboard_data.py --skip-meta       # phases 2+3+4 (when
                                                             the token is busy)

Manual attribution overrides in public.ad_attribution_overrides are
preserved — rebuild_attribution_orders.py applies them before writing
shopify_ad_attribution.
"""
import argparse, io, json, os, pathlib, subprocess, sys, time, datetime

# Windows CMD's default codepage (cp1252) can't encode UTF-8 box chars used
# in phase headers. Force stdout to UTF-8 with a safe fallback.
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
    sys.stderr.reconfigure(encoding='utf-8', errors='backslashreplace')
except Exception:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='backslashreplace')

ROOT = pathlib.Path(__file__).parent
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
_TS  = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
LOG  = LOG_DIR / f"refresh_all_{_TS}.log"
STATUS_JSON = LOG_DIR / f"refresh_all_{_TS}.json"
PY   = sys.executable

# ─── PHASE 1 — Meta fetchers (order-independent within the phase, but all
#     touch the Meta token so we run them sequentially to keep the shared
#     BUC budget clean. fetch_ad_thumbnails is deliberately excluded — it
#     runs on its own daily cron; can take 5+ hours on a full cycle) ────
PHASE_META = [
    ("primary_sync_daily",             ["primary_sync.py", "daily"],        5400),
    # NEW: baseline-anchored deduped reach for account/campaign/adset/ad.
    # Powers get_reach_incr_by_window + saturation chart. ~1-2 hours on
    # a cold restart; ~5-10 min on daily incremental once populated.
    ("fetch_reach_incr_all",           ["fetch_reach_incr.py", "--level", "all"], 7200),
    # LEGACY: still feeds the old ireach_* tables. Will be removed once
    # we're confident nothing reads from them.
    ("fetch_meta_ireach_daily",        ["fetch_meta_ireach_daily.py"],      1800),
    ("fetch_ig_media",                 ["fetch_ig_media.py"],               3600),
    ("fetch_ad_lifetime_purchases",    ["fetch_ad_lifetime_purchases.py"],  1800),
]

# ─── PHASE 2 — Other ingestion (no Meta token — can run parallel to phase
#     1 if needed. Google Ads has its own token; Shopify has its own auth) ─
PHASE_INGEST = [
    ("fetch_google_ads_daily",         ["fetch_google_ads_daily.py"],       1800),
    ("fetch_shopify_sessions",         ["fetch_shopify_sessions.py"],       1800),
    ("sync_shopify_customers",         ["sync_shopify_customers.py"],       1800),
    ("sync_shopify_products",          ["sync_shopify_products.py"],         900),
    ("sync_landing_page_sessions",     ["sync_landing_page_sessions.py"],   1800),
    ("fetch_cpis_by_sku",              ["fetch_cpis_by_sku.py"],             900),
    ("import_asset_id_sheet",          ["import_asset_id_sheet.py"],         300),
    ("fetch_content_asset_register",   ["fetch_content_asset_register.py"],  600),
    ("fetch_graphic_sheet",            ["fetch_graphic_sheet.py"],           600),
    ("fetch_historic_video_assets",    ["fetch_historic_video_assets.py"],  1800),
    ("fetch_creatorhub_posts",         ["fetch_creatorhub_posts.py"],        900),
]

# ─── PHASE 3 — Computation (DB-only; every read source is already fresh
#     from phases 1+2). Order matters — attribution must run BEFORE the L30
#     copy, ads_delivered_daily must run after primary is propagated ─────
PHASE_COMPUTE = [
    # Meta rollups (order: propagate → apply_ctp → refresh_ae → summary)
    ("propagate_primary_to_backfill",  ["propagate_primary_to_backfill.py"], 1800),
    ("apply_ctp_unique_ids",           ["apply_ctp_unique_ids.py"],          900),
    ("refresh_ae_table",               ["refresh_ae_table.py"],             1800),
    ("refresh_summary_table",          ["refresh_summary_table.py"],         900),
    ("refresh_ae_reach_recent",        ["refresh_ae_reach_recent.py"],       600),
    # Google Ads summary
    ("refresh_google_ads_summary",     ["refresh_google_ads_summary.py"],    300),
    # Results classifier + snapshot
    ("result_classifier",              ["result_classifier.py"],             900),
    ("results_sync",                   ["results_sync.py"],                 1800),
    # Attribution + L30 (must be after propagate + refresh_ae)
    ("rebuild_attribution_orders",     ["rebuild_attribution_orders.py",
                                        "2026-06-15", "2099-12-31"],        4200),
    ("build_shopify_l30",              ["_build_shopify_l30.py"],            600),
    # Session UTM rollups
    ("sync_sessions_by_utm",           ["sync_sessions_by_utm.py"],          900),
    ("sync_sessions_by_utm_page",      ["sync_sessions_by_utm_page.py"],    1800),
    # 90d + 30d ad rollups
    ("build_ae_daily_90d",             ["_build_ae_daily_90d.py"],          1200),
    ("build_ae_daily_30d",             ["_build_ae_daily_30d.py"],           900),
    # Delivery-ads helper for get_delivery_ads RPC
    ("refresh_ads_delivered_daily",    ["_refresh_ads_delivered_daily.py"],  600),
    # Product economics
    ("refresh_product_doq",            ["refresh_product_doq.py"],           600),
    # RCK sheet-backing tables
    ("build_rck_daily_30d",            ["_build_rck_daily_30d.py"],          600),
    ("build_rck_last30",               ["_build_rck_last30.py"],             600),
]


def log(msg: str) -> None:
    line = f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8", errors="backslashreplace") as f:
        f.write(line + "\n")


def run_step(label: str, argv: list[str], timeout: int) -> dict:
    log(f"-- step: {label}")
    t0 = time.time()
    try:
        # Tee subprocess stdout+stderr into the same log the UI polls, so
        # the pipeline dashboard can grep it for Meta calls, throttle %, and
        # errors — one file has everything.
        with open(LOG, "a", encoding="utf-8", errors="backslashreplace") as f:
            r = subprocess.run([PY, *argv], cwd=str(ROOT), timeout=timeout,
                               stdout=f, stderr=subprocess.STDOUT)
        dt = time.time() - t0
        ok = (r.returncode == 0)
        status = "OK" if ok else "FAIL"
        log(f"   {status:4s}  exit={r.returncode}  duration={dt:.0f}s")
        return {"label": label, "status": status, "exit_code": r.returncode,
                "duration_sec": round(dt, 1)}
    except subprocess.TimeoutExpired:
        dt = time.time() - t0
        log(f"   TIMEOUT after {dt:.0f}s")
        return {"label": label, "status": "TIMEOUT", "exit_code": None,
                "duration_sec": round(dt, 1)}
    except Exception as e:  # noqa: BLE001
        dt = time.time() - t0
        log(f"   EXCEPTION {type(e).__name__}: {e}")
        return {"label": label, "status": "EXCEPTION", "exit_code": None,
                "duration_sec": round(dt, 1), "error": f"{type(e).__name__}: {e}"}


def refresh_lp_rpcs() -> dict:
    """Landing page rollups run as Postgres RPCs (server-side), not scripts."""
    label = "refresh_lp_rpcs"
    log(f"-- step: {label}")
    t0 = time.time()
    try:
        import psycopg2
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env", override=True)
        c = psycopg2.connect(os.environ["SUPABASE_DB_URL"], connect_timeout=30)
        c.autocommit = True
        cur = c.cursor()
        cur.execute("SELECT public.refresh_landing_page_analysis_30d(30)")
        cur.execute("SELECT public.refresh_landing_page_ad_breakdown_30d(30)")
        cur.close(); c.close()
        dt = time.time() - t0
        log(f"   OK  duration={dt:.0f}s")
        return {"label": label, "status": "OK", "exit_code": 0,
                "duration_sec": round(dt, 1)}
    except Exception as e:  # noqa: BLE001
        dt = time.time() - t0
        log(f"   EXCEPTION {type(e).__name__}: {e}")
        return {"label": label, "status": "EXCEPTION", "exit_code": None,
                "duration_sec": round(dt, 1), "error": f"{type(e).__name__}: {e}"}


def refresh_meta_direct_views_rpc() -> dict:
    """Regenerate the 4 meta_direct_* materialized views that mirror the
    Apps Script Meta Direct sheet tabs. Server-side REFRESH CONCURRENTLY;
    depends on primary_table + shopify_ad_attribution being fresh (i.e.
    must run AFTER primary_sync_daily + rebuild_attribution_orders)."""
    label = "refresh_meta_direct_views"
    log(f"-- step: {label}")
    t0 = time.time()
    try:
        import psycopg2
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env", override=True)
        # Bypass the wrapper RPC — Supabase's pooler enforces a ~2min
        # statement_timeout that overrides function-level SETs AND connection
        # `options=-c statement_timeout=…`, so a single big REFRESH inside
        # the RPC dies. Firing each REFRESH as its own execute() sidesteps
        # the pooler cap because plain REFRESH per view is 25-107s, all
        # under the limit. First failure doesn't kill the chain either.
        c = psycopg2.connect(os.environ["SUPABASE_DB_URL"], connect_timeout=30)
        c.autocommit = True
        cur = c.cursor()
        results = []
        VIEWS = ["meta_direct_active_30d", "meta_direct_active_90d",
                 "meta_direct_daily_30d",  "meta_direct_daily_90d"]
        for v in VIEWS:
            vt0 = time.time()
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW public.{v}")
                cur.execute(f"SELECT count(*) FROM public.{v}")
                n = cur.fetchone()[0]
                secs = round(time.time() - vt0, 2)
                results.append((v, n, secs))
                log(f"   {v:30s} {n:>7,} rows  {secs}s")
            except Exception as ve:  # noqa: BLE001
                secs = round(time.time() - vt0, 2)
                log(f"   {v:30s} FAILED after {secs}s: "
                    f"{type(ve).__name__}: {str(ve)[:200]}")
        cur.close(); c.close()
        dt = time.time() - t0
        log(f"   OK  duration={dt:.0f}s")
        return {"label": label, "status": "OK", "exit_code": 0,
                "duration_sec": round(dt, 1)}
    except Exception as e:  # noqa: BLE001
        dt = time.time() - t0
        log(f"   EXCEPTION {type(e).__name__}: {e}")
        return {"label": label, "status": "EXCEPTION", "exit_code": None,
                "duration_sec": round(dt, 1), "error": f"{type(e).__name__}: {e}"}


def run_phase(name: str, steps: list) -> list[dict]:
    log("")
    log(f"==== PHASE: {name.upper()}  ({len(steps)} steps) ====")
    return [run_step(*s) for s in steps]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["meta", "ingest", "compute", "all"],
                    default="all",
                    help="Run only this phase (default: all)")
    ap.add_argument("--skip-meta", action="store_true",
                    help="Skip PHASE 1 (Meta fetchers) — useful when the Meta "
                         "token is being used elsewhere.")
    args = ap.parse_args()

    log(f"=== REFRESH ALL DASHBOARD DATA — log: {LOG.name} ===")
    log(f"    phase={args.phase}  skip_meta={args.skip_meta}")
    overall_t0 = time.time()
    all_results: list[dict] = []

    if args.phase in ("meta", "all") and not args.skip_meta:
        all_results.extend(run_phase("META fetchers", PHASE_META))

    if args.phase in ("ingest", "all"):
        all_results.extend(run_phase("Other INGEST", PHASE_INGEST))

    if args.phase in ("compute", "all"):
        all_results.extend(run_phase("COMPUTATION", PHASE_COMPUTE))
        all_results.append(refresh_lp_rpcs())
        # Meta Direct sheet mirrors — must run AFTER primary_sync +
        # rebuild_attribution_orders so both feeder tables are fresh.
        all_results.append(refresh_meta_direct_views_rpc())
        # Flush FastAPI's response cache so anyone hitting the dashboard
        # after the refresh sees Aug XX data immediately (not the stale
        # cached Aug XX-1 payload). Silent if the API server isn't running.
        try:
            import requests as _rq
            api_base = os.environ.get("API_BASE", "http://127.0.0.1:8000")
            r = _rq.post(f"{api_base}/api/cache/invalidate", timeout=5)
            if r.status_code == 200:
                log(f"-- cache invalidated: {r.json()}")
            else:
                log(f"-- cache invalidate HTTP {r.status_code} — ignoring")
        except Exception as _e:  # noqa: BLE001
            log(f"-- cache invalidate skipped ({type(_e).__name__}: {str(_e)[:80]})")

    total = time.time() - overall_t0
    ok_n   = sum(1 for r in all_results if r["status"] == "OK")
    fail_n = len(all_results) - ok_n

    log("")
    log("=== REFRESH ALL COMPLETE ===")
    log(f"   total wall time: {total/60:.1f} min")
    log(f"   {ok_n}/{len(all_results)} steps OK  ({fail_n} not OK)")
    for r in all_results:
        marker = "OK  " if r["status"] == "OK" else f"{r['status'][:4]:4s}"
        log(f"   {marker}  {r['duration_sec']:>6.0f}s  {r['label']}")

    # Machine-readable status for external monitors / next-day debugging.
    summary = {
        "run_ts": _TS,
        "phase_filter": args.phase,
        "skip_meta": args.skip_meta,
        "total_wall_sec": round(total, 1),
        "ok_count": ok_n,
        "fail_count": fail_n,
        "steps": all_results,
    }
    STATUS_JSON.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    log(f"   status JSON → {STATUS_JSON.name}")


if __name__ == "__main__":
    main()
