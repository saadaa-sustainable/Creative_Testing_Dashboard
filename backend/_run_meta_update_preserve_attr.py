"""Meta-only pipeline runner. Preserves manual shopify_ad_attribution edits
(the 'spend-weight', 'spend-weight-hashed', slug-inherit, MEGHA→DIVYA
markers) by SKIPPING rebuild_attribution_orders only.

After the Meta refresh, rebuilds the two helper tables the Google Sheets
depend on (ad_utm_mode, ae_daily_30d) so the sheet picks up new Meta
delivery/creative changes WITHOUT re-deriving attribution from raw UTMs.

Steps kept (no attribution overwrite):
  primary_sync, propagate_primary_to_backfill, apply_ctp_unique_ids,
  refresh_ae_table, refresh_summary_table, refresh_ae_reach_recent,
  refresh_new_incr_table, fetch_meta_ireach_daily,
  fetch_google_ads_daily, refresh_google_ads_summary,
  result_classifier, fetch_shopify_sessions, sync_shopify_customers,
  import_asset_id_sheet, results_sync, fetch_ad_thumbnails,
  _build_ad_utm_mode, _build_ae_daily_30d

Step skipped (would clobber manual attribution):
  rebuild_attribution_orders   ← re-derives from raw UTMs, undoes manual fixes

Note: results_sync only reads primary_table (not shopify_ad_attribution),
so it's safe to run — an earlier version of this runner mistakenly
skipped it, which staled the dashboard's fast-path cache and forced the
frontend into the slow live-aggregation over 180k+ primary_table rows.
"""
import subprocess, sys, time, datetime, pathlib, io
# Reroute stdout so cp1252 consoles don't crash on unicode arrows / em-dashes
# in the log lines below (a Unicode error on the final summary print killed
# the previous pipeline run with exit code 1 despite all data work succeeding).
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
ROOT = pathlib.Path(__file__).parent
LOG  = ROOT / "logs" / f"meta_preserve_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG.parent.mkdir(exist_ok=True)
PY   = sys.executable

STEPS = [
    # ── Meta side ────────────────────────────────────────────────────────
    ("primary_sync.py daily",          ["primary_sync.py", "daily"],       3600),
    ("propagate_primary_to_backfill",  ["propagate_primary_to_backfill.py"], 1800),
    ("apply_ctp_unique_ids",           ["apply_ctp_unique_ids.py"],         900),
    ("refresh_ae_table",               ["refresh_ae_table.py"],            1800),
    ("refresh_summary_table",          ["refresh_summary_table.py"],        900),
    ("refresh_ae_reach_recent",        ["refresh_ae_reach_recent.py"],      600),
    ("refresh_new_incr_table",         ["refresh_new_incr_table.py"],       300),
    ("fetch_meta_ireach_daily",        ["fetch_meta_ireach_daily.py"],     1800),
    # ── Google Ads ───────────────────────────────────────────────────────
    ("fetch_google_ads_daily",         ["fetch_google_ads_daily.py"],       1800),
    ("refresh_google_ads_summary",     ["refresh_google_ads_summary.py"],    300),
    # ── Classification (no attribution overwrite) ────────────────────────
    ("result_classifier",              ["result_classifier.py"],            900),
    # ── Shopify metadata refreshes (safe — no attribution rebuild) ───────
    ("fetch_shopify_sessions",         ["fetch_shopify_sessions.py"],       1800),
    ("sync_shopify_customers",         ["sync_shopify_customers.py"],       1800),
    ("import_asset_id_sheet",          ["import_asset_id_sheet.py"],         300),
    # ── Dashboard fast-path cache (30-day snapshot, primary_table only) ─
    # Skipping this stales results_table and forces the browser into a
    # slow live aggregation of 180k+ primary_table rows on every page open.
    ("results_sync",                   ["results_sync.py"],                 1800),
    # ── SKIPPED — would undo manual attribution work ─────────────────────
    #  rebuild_attribution_orders     — RE-derives all attributions from raw UTMs
    # ── Creative refresh ─────────────────────────────────────────────────
    ("fetch_ad_thumbnails",            ["fetch_ad_thumbnails.py"],           18000),
    # ── Helper tables for the Google Sheets ──────────────────────────────
    ("_build_ad_utm_mode",             ["_build_ad_utm_mode.py"],            600),
    ("_build_ae_daily_30d",            ["_build_ae_daily_30d.py"],           600),
]

def log(msg):
    line = f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8", errors="backslashreplace") as f:
        f.write(line + "\n")

log(f"=== META-ONLY UPDATE START (attribution preserved) — log: {LOG.name} ===")
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
            log(f"   !! step '{label}' failed — continuing anyway")
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
log("=== META-ONLY UPDATE COMPLETE ===")
log(f"   total wall time: {total/60:.1f} min")
ok_n  = sum(1 for _, ok, _ in results if ok)
fail_n = len(results) - ok_n
log(f"   {ok_n}/{len(results)} steps OK")
for lbl, ok, dt in results:
    log(f"   {'OK ' if ok else 'FAIL'}  {dt:>5.0f}s  {lbl}")
log("")
log("REMINDER: manual attribution edits preserved. To pick them up in the")
log("Google Sheet, refresh the two menus once this run completes:")
log("  Ad Intel        → Refresh AE Table (ACTIVE only)")
log("  Ad Intel Daily  → Refresh 30-day breakdown")
