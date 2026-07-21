"""Parallel Meta pipeline runner — 3× faster nightly sync.

primary_sync.py loops through 3 ad accounts sequentially (~15 min each).
This orchestrator launches 3 separate primary_sync processes in parallel via
the SYNC_ACCOUNT_ID env var — total wall time drops to the slowest account
(~10-15 min instead of ~35-45 min).

After all 3 primary_sync workers finish, the downstream refresh steps run
sequentially (they can't parallelise — each reads the fully-populated
primary_table + backfill_table produced by the fan-out step).

Flow:
    ┌─────────── PARALLEL FAN-OUT ───────────┐
    │  primary_sync (Raho Saadaa)            │
    │  primary_sync (Fourth Ad Account - SD) │   ← 3 subprocesses
    │  primary_sync (Third Ad Account - SD)  │
    └────────────────┬───────────────────────┘
                     │  join
                     ▼
    ┌────── SEQUENTIAL DOWNSTREAM ──────┐
    │  propagate_primary_to_backfill    │
    │  refresh_ae_table                 │
    │  refresh_summary_table            │
    │  refresh_new_incr_table           │
    │  result_classifier                │
    │  refresh_ae_reach_recent          │
    │  results_sync                     │
    └───────────────────────────────────┘

Usage:
    python _run_meta_parallel.py          # daily (default)
    python _run_meta_parallel.py hourly
"""
import os, sys, subprocess, time
from pathlib import Path
from datetime import datetime

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

MODE = (sys.argv[1] if len(sys.argv) > 1 else "daily").lower()
if MODE not in ("daily", "hourly"):
    sys.exit(f"unknown mode: {MODE}. use: daily | hourly")

BACKEND = Path(__file__).parent
PY      = sys.executable

# Account IDs — must match ACCOUNT_*_ID in .env (else primary_sync exits early)
ACCOUNTS = [
    ("Raho Saadaa",            os.getenv("ACCOUNT_1_ID", "1136644150469466")),
    ("Fourth Ad Account - SD", os.getenv("ACCOUNT_2_ID", "1349767139294217")),
    ("Third Ad Account - SD",  os.getenv("ACCOUNT_3_ID", "264868699479122")),
]

DOWNSTREAM = [
    "propagate_primary_to_backfill.py",
    "refresh_ae_table.py",
    "refresh_summary_table.py",
    "refresh_new_incr_table.py",
    "result_classifier.py",
    "refresh_ae_reach_recent.py",
    "results_sync.py",
]

LOG_DIR = BACKEND / "_parallel_logs"
LOG_DIR.mkdir(exist_ok=True)

def say(msg): print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── Phase 1: parallel primary_sync ─────────────────────────────
say(f"launching {len(ACCOUNTS)} primary_sync workers (mode={MODE}) in parallel …")
procs = []
for name, aid in ACCOUNTS:
    env = os.environ.copy()
    env["SYNC_ACCOUNT_ID"] = aid
    log_path = LOG_DIR / f"primary_sync_{aid}.log"
    log_fh = open(log_path, "w", encoding="utf-8", errors="replace")
    p = subprocess.Popen(
        [PY, "primary_sync.py", MODE],
        cwd=str(BACKEND),
        env=env,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )
    procs.append({"name": name, "id": aid, "p": p, "log": log_path, "fh": log_fh,
                  "t0": time.time()})
    say(f"  ↑ [{p.pid}] {name}  →  {log_path.name}")

# Wait for all 3
say("waiting for all workers to finish …")
failed = []
for pr in procs:
    rc = pr["p"].wait()
    pr["fh"].close()
    elapsed = time.time() - pr["t0"]
    tag = "✓" if rc == 0 else f"✗ (exit {rc})"
    say(f"  {tag} {pr['name']:26}  {elapsed:5.0f}s   log: {pr['log'].name}")
    if rc != 0: failed.append(pr["name"])

if failed:
    say(f"[!] {len(failed)} worker(s) failed: {failed}")
    say(f"[!] SKIPPING downstream refreshes — the aggregated tables would be built on incomplete data.")
    say(f"[!] Inspect {LOG_DIR}/ and re-run failed accounts manually before firing downstream.")
    sys.exit(1)

say("all primary_sync workers finished cleanly — running downstream refreshes")

# ── Phase 2: sequential downstream ────────────────────────────
for script in DOWNSTREAM:
    say(f"→ {script}")
    t0 = time.time()
    p = subprocess.run([PY, script], cwd=str(BACKEND))
    dt = time.time() - t0
    if p.returncode != 0:
        say(f"  ✗ {script} exit={p.returncode} after {dt:.0f}s — halting pipeline")
        sys.exit(p.returncode)
    say(f"  ✓ {script}  {dt:.0f}s")

say("META PIPELINE COMPLETE")
