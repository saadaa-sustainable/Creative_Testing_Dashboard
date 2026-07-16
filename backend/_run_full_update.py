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
    # (ae_daily_agg_mat retired 2026-07-13; its three consumers
    #  now compute the primary+backfill dedup inline in their RPC
    #  bodies, so no matview refresh needed.)
    ("refresh_ae_reach_recent",        ["refresh_ae_reach_recent.py"],      600),
    # new_incr_table = per-ad daily reach series with running cumulative +
    # honest day-over-day delta.  Reconstructed from primary_table + backfill
    # (no Meta calls).  321k rows / ~26MB, TRUNCATE+INSERT via the DB RPC —
    # 5-min ceiling is comfortable.
    ("refresh_new_incr_table",         ["refresh_new_incr_table.py"],       300),
    # Campaign + Adset UNIQUE reach at group level, straight from Meta's
    # /insights (level=campaign / level=adset). Meta returns the deduped
    # audience per group per day — you can't get this by summing ad-level
    # reach from primary_table because that overcounts by 10-60%.
    # Daily incremental picks up the last 15 days on every run so a
    # slow-to-arrive backfill day still lands.
    ("fetch_meta_ireach_daily",        ["fetch_meta_ireach_daily.py"],     1800),
    # Google Ads daily fetch — ad-level performance via /ad_group_ad GAQL,
    # writes google_ads_primary with 3-day overlap.  Grain matches Meta's
    # primary_table (one row per customer, ad_id, date).
    ("fetch_google_ads_daily",         ["fetch_google_ads_daily.py"],       1800),
    # google_ads_summary rollup — one row per ad, lifetime aggregates.
    # Small (~60 ads), full-rebuild each run.
    ("refresh_google_ads_summary",     ["refresh_google_ads_summary.py"],    300),
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
