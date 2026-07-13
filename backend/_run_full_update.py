"""One-shot full pipeline runner. Logs each step + timing + exit code."""
import subprocess, sys, time, datetime, pathlib
ROOT = pathlib.Path(__file__).parent
LOG  = ROOT / "logs" / f"full_update_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG.parent.mkdir(exist_ok=True)
PY   = sys.executable

STEPS = [
    # Meta side first — fast and reliable
    ("primary_sync.py daily",          ["primary_sync.py", "daily"],       3600),
    ("propagate_primary_to_backfill",  ["propagate_primary_to_backfill.py"], 1800),
    ("apply_ctp_unique_ids",           ["apply_ctp_unique_ids.py"],         900),
    ("refresh_ae_table",               ["refresh_ae_table.py"],            1800),
    ("refresh_summary_table",          ["refresh_summary_table.py"],        900),
    ("refresh_ae_daily_agg",           ["refresh_ae_daily_agg.py"],         600),
    ("refresh_ae_reach_recent",        ["refresh_ae_reach_recent.py"],      600),
    # Campaign + Adset daily reach agg (drives Incremental Analysis).
    # Same dedup logic as ae_reach_recent; rebuilt once per pipeline run.
    ("refresh_ireach_daily",           ["refresh_ireach_daily.py"],         600),
    ("result_classifier",              ["result_classifier.py"],            900),
    ("results_sync",                   ["results_sync.py"],                1800),
    # Shopify sessions per landing-page — daily incremental via ShopifyQL.
    # Fetcher auto-detects max(session_date) via PostgREST (falls back to
    # 2025-01-01 if the table is empty) and pulls a 3-day overlap window
    # into today so late-arriving data still lands.  Writes via PostgREST
    # since the pooler URL isn't provisioned in every environment.
    ("fetch_shopify_sessions",         ["fetch_shopify_sessions.py"],       1800),
    # Shopify customers mirror — pull the full customer directory into
    # public.customers on the Saada_Shopify_Data project. Idempotent
    # upsert by customer id; on the first run it walks the whole
    # catalogue, subsequent runs re-fetch every customer so tags /
    # amount_spent / addresses / marketing consent stay fresh. Ceiling
    # 30 min covers the ~10-15k customer catalogue at 100/page
    # (~1s each with a 0.25s inter-page throttle).
    ("sync_shopify_customers",         ["sync_shopify_customers.py"],       1800),
    # Asset ID mapping — pull the CTP-Asset_Sheet_v1 Google Sheet's
    # External tab and upsert ad_id → asset_id into public.ad_asset_ids.
    # Sheet is publicly readable (anyone with link); no OAuth needed.
    # ~15k rows fetch in a few seconds so 5 min is a generous cap.
    ("import_asset_id_sheet",          ["import_asset_id_sheet.py"],         300),
    # Shopify attribution LAST — slow and network-flaky; isolates the long step at the end
    ("rebuild_attribution_orders",     ["rebuild_attribution_orders.py", "2026-06-15", "2099-12-31"],  3600),
    # Thumbnail refresh — Meta's fbcdn URLs expire in ~48-72h so we cycle the
    # 2-day-and-older half of the table on every daily run.  Uses the
    # default incremental mode (REFRESH_DAYS=2 in fetch_ad_thumbnails.py).
    # 5-hour cap: Meta's rate limit throttles at ~1600/hr, and ~half the
    # ~15k universe expires each day. If it hits the cap the remaining
    # oldest-first ads pick up on tomorrow's pipeline run.
    ("fetch_ad_thumbnails",            ["fetch_ad_thumbnails.py"],           18000),
]

def log(msg):
    line = f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8", errors="backslashreplace") as f:
        f.write(line + "\n")

log(f"=== FULL UPDATE START - log: {LOG.name} ===")
overall_t0 = time.time()
results = []
for label, argv, timeout in STEPS:
    log(f"-- step: {label}")
    t0 = time.time()
    try:
        r = subprocess.run([PY, *argv], cwd=str(ROOT), timeout=timeout)
        dt = time.time() - t0
        ok = (r.returncode == 0)
        log(f"   {'OK ' if ok else 'FAIL'}  exit={r.returncode}  duration={dt:.0f}s")
        results.append((label, ok, dt))
        if not ok:
            log(f"   !! step '{label}' failed - continuing anyway")
    except subprocess.TimeoutExpired:
        dt = time.time() - t0
        log(f"   TIMEOUT after {dt:.0f}s")
        results.append((label, False, dt))
    except Exception as e:
        dt = time.time() - t0
        log(f"   EXCEPTION {type(e).__name__}: {e}")
        results.append((label, False, dt))

total = time.time() - overall_t0
log("")
log("=== FULL UPDATE COMPLETE ===")
log(f"   total wall time: {total/60:.1f} min")
ok_n  = sum(1 for _, ok, _ in results if ok)
fail_n = len(results) - ok_n
log(f"   {ok_n}/{len(results)} steps OK")
for lbl, ok, dt in results:
    log(f"   {'OK ' if ok else 'FAIL'}  {dt:>5.0f}s  {lbl}")
