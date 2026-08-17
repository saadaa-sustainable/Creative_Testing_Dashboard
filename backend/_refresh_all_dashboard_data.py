"""_refresh_all_dashboard_data.py — one-shot orchestrator that refreshes
every table the Creative Testing Dashboard reads from.

Runs every step individually (not the _run_full_update.py black-box) so
the parent process controls timeouts + step ordering directly. Skips
fetch_ad_thumbnails — that one runs on its own daily cron.

Steps in dependency order:
  · Meta ads pipeline (primary_sync → propagate → apply_ctp → ae_table
    → summary → ae_reach_recent → new_incr → meta_ireach)
  · Google Ads (fetch_google_ads_daily → refresh_google_ads_summary)
  · Result classifier + results_sync
  · Shopify sessions + customers
  · Asset ID sheet + attribution rebuild (respects manual overrides)
  · sync_sessions_by_utm{,_page} → session-utm rollups
  · _build_ae_daily_90d → 90d rollup (with unique_link_clicks)
  · Untested Assets: graphic sheet / historic video / IG media / creatorhub
  · refresh_landing_page_{analysis,ad_breakdown}_30d RPCs

Total wall time: ~60-90 min without thumbnails.

Manual attribution overrides in public.ad_attribution_overrides are
preserved — rebuild_attribution_orders.py applies them before writing
shopify_ad_attribution.
"""
import subprocess, sys, time, datetime, pathlib, os

ROOT = pathlib.Path(__file__).parent
LOG  = ROOT / "logs" / f"refresh_all_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG.parent.mkdir(exist_ok=True)
PY   = sys.executable

# Ordering matters — each step depends on prior tables.
# NOTE: fetch_ad_thumbnails is deliberately EXCLUDED. That step is Meta
# rate-limited and can take 5+ hours; it runs on its own daily cron.
STEPS = [
    # ── Meta ads pipeline ─────────────────────────────────────
    ("primary_sync_daily",             ["primary_sync.py", "daily"],        5400),
    ("propagate_primary_to_backfill",  ["propagate_primary_to_backfill.py"], 1800),
    ("apply_ctp_unique_ids",           ["apply_ctp_unique_ids.py"],          900),
    ("refresh_ae_table",               ["refresh_ae_table.py"],             1800),
    ("refresh_summary_table",          ["refresh_summary_table.py"],         900),
    ("refresh_ae_reach_recent",        ["refresh_ae_reach_recent.py"],       600),
    ("refresh_new_incr_table",         ["refresh_new_incr_table.py"],        300),
    ("fetch_meta_ireach_daily",        ["fetch_meta_ireach_daily.py"],      1800),
    # ── Google Ads ────────────────────────────────────────────
    ("fetch_google_ads_daily",         ["fetch_google_ads_daily.py"],       1800),
    ("refresh_google_ads_summary",     ["refresh_google_ads_summary.py"],    300),
    # ── Result classifier + results sync ──────────────────────
    ("result_classifier",              ["result_classifier.py"],             900),
    ("results_sync",                   ["results_sync.py"],                 1800),
    # ── Shopify sessions + customers + asset ids ──────────────
    ("fetch_shopify_sessions",         ["fetch_shopify_sessions.py"],       1800),
    ("sync_shopify_customers",         ["sync_shopify_customers.py"],       1800),
    ("import_asset_id_sheet",          ["import_asset_id_sheet.py"],         300),
    # ── Shopify attribution (respects manual overrides) ───────
    ("rebuild_attribution_orders",     ["rebuild_attribution_orders.py",
                                        "2026-06-15", "2099-12-31"],        4200),
    # L30 rolling copy — dashboard's "Last Click UTM analysis" queries this
    # for any date range fully within the last 30 days. Must run AFTER
    # rebuild_attribution so it picks up the freshly-attributed orders.
    ("build_shopify_l30",              ["_build_shopify_l30.py"],            600),
    # ── Session UTM rollups ───────────────────────────────────
    ("sync_sessions_by_utm",           ["sync_sessions_by_utm.py"],          900),
    ("sync_sessions_by_utm_page",      ["sync_sessions_by_utm_page.py"],    1800),
    # ── 90d ad rollup (includes new unique_link_clicks column) ─
    ("build_ae_daily_90d",             ["_build_ae_daily_90d.py"],          1200),
    # Helper table for get_delivery_ads RPC — pre-computed DISTINCT
    # (ad_id, date) pairs where impressions>0. Turns the RPC from a 40-90s
    # scan (that timed out on Supabase's 60s cap) into a <300ms lookup.
    ("refresh_ads_delivered_daily",    ["_refresh_ads_delivered_daily.py"],  600),
    # ── Untested Assets (4 sources) ───────────────────────────
    ("fetch_graphic_sheet",            ["fetch_graphic_sheet.py"],           600),
    ("fetch_historic_video_assets",    ["fetch_historic_video_assets.py"],  1800),
    ("fetch_ig_media",                 ["fetch_ig_media.py"],               3600),
    ("fetch_creatorhub_posts",         ["fetch_creatorhub_posts.py"],        900),
]


def log(msg: str) -> None:
    line = f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8", errors="backslashreplace") as f:
        f.write(line + "\n")


def run_step(label: str, argv: list[str], timeout: int) -> tuple[str, bool, float]:
    log(f"-- step: {label}")
    t0 = time.time()
    try:
        r = subprocess.run([PY, *argv], cwd=str(ROOT), timeout=timeout)
        dt = time.time() - t0
        ok = (r.returncode == 0)
        log(f"   {'OK ' if ok else 'FAIL'}  exit={r.returncode}  duration={dt:.0f}s")
        return (label, ok, dt)
    except subprocess.TimeoutExpired:
        dt = time.time() - t0
        log(f"   TIMEOUT after {dt:.0f}s")
        return (label, False, dt)
    except Exception as e:  # noqa: BLE001
        dt = time.time() - t0
        log(f"   EXCEPTION {type(e).__name__}: {e}")
        return (label, False, dt)


def refresh_lp_rpcs() -> tuple[str, bool, float]:
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
        # 30d rollup expects an int arg for days (defaulted to 30 client-side).
        cur.execute("SELECT public.refresh_landing_page_analysis_30d(30)")
        cur.execute("SELECT public.refresh_landing_page_ad_breakdown_30d()")
        cur.close(); c.close()
        dt = time.time() - t0
        log(f"   OK  duration={dt:.0f}s")
        return (label, True, dt)
    except Exception as e:  # noqa: BLE001
        dt = time.time() - t0
        log(f"   EXCEPTION {type(e).__name__}: {e}")
        return (label, False, dt)


def main() -> None:
    log(f"=== REFRESH ALL DASHBOARD DATA — log: {LOG.name} ===")
    overall_t0 = time.time()
    results = [run_step(label, argv, timeout) for label, argv, timeout in STEPS]
    # LP RPCs run last so they aggregate over the freshly-refreshed data.
    results.append(refresh_lp_rpcs())

    total = time.time() - overall_t0
    log("")
    log("=== REFRESH ALL COMPLETE ===")
    log(f"   total wall time: {total/60:.1f} min")
    ok_n = sum(1 for _, ok, _ in results if ok)
    fail_n = len(results) - ok_n
    log(f"   {ok_n}/{len(results)} steps OK  ({fail_n} failed)")
    for lbl, ok, dt in results:
        log(f"   {'OK ' if ok else 'FAIL'}  {dt:>5.0f}s  {lbl}")


if __name__ == "__main__":
    main()
