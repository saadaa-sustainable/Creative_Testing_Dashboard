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
    ("result_classifier",              ["result_classifier.py"],            900),
    ("results_sync",                   ["results_sync.py"],                1800),
    # Shopify attribution LAST — slow and network-flaky; isolates the long step at the end
    ("rebuild_attribution_orders",     ["rebuild_attribution_orders.py", "2026-06-15", "2099-12-31"],  3600),
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
