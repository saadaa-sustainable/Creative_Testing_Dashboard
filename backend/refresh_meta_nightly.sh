#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# refresh_meta_nightly.sh — cron-friendly wrapper around
# _refresh_all_dashboard_data.py.
#
# Runs the backend orchestrator so the Meta ads tables + downstream
# aggregations get refreshed unattended overnight. The orchestrator writes
# its own tee'd log to  logs/refresh_all_{TS}.log , which is exactly the
# file the pipeline UI ( backend/pipeline.html ) reads via /pipeline/status
# + /pipeline/logs — so the UI shows automated runs the same way it shows
# manually-triggered ones. No extra wiring needed.
#
# Usage:
#   ./refresh_meta_nightly.sh              # default: --phase meta  (Meta-only)
#   ./refresh_meta_nightly.sh all          # full pipeline (meta + ingest + compute)
#   ./refresh_meta_nightly.sh meta --skip-meta   # extra flags forwarded verbatim
#   PHASE=meta ./refresh_meta_nightly.sh   # env-var form
#
# Cron (Linux/Mac/WSL, 00:30 IST daily):
#   30 0 * * *  /d/Creative_Testing_Dashboard/backend/refresh_meta_nightly.sh \
#                 >> /d/Creative_Testing_Dashboard/backend/logs/cron_wrapper.log 2>&1
#
# Windows Task Scheduler (run under Git Bash, 00:30 daily):
#   schtasks /Create /SC DAILY /ST 00:30 /TN "MetaAdsNightlyRefresh" ^
#     /TR "\"C:\Program Files\Git\bin\bash.exe\" -lc \"cd /d/Creative_Testing_Dashboard/backend && ./refresh_meta_nightly.sh\""
# ─────────────────────────────────────────────────────────────────────────

set -euo pipefail

# Resolve script dir even when invoked via symlink / relative path — critical
# for cron because $PWD there is $HOME, not the project.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Phase: first arg wins, then PHASE env, then default 'meta' ─────────
PHASE="${1:-${PHASE:-meta}}"
shift || true
EXTRA_ARGS=("$@")   # everything after the phase gets forwarded to the orchestrator

# ── Python: PROJECT_PY env overrides, else common Windows/Linux venv paths ──
if [ -n "${PROJECT_PY:-}" ]; then
  PY="$PROJECT_PY"
elif [ -x "$SCRIPT_DIR/../.venv/Scripts/python.exe" ]; then
  PY="$SCRIPT_DIR/../.venv/Scripts/python.exe"
elif [ -x "$SCRIPT_DIR/../.venv/bin/python" ]; then
  PY="$SCRIPT_DIR/../.venv/bin/python"
elif command -v py >/dev/null 2>&1; then
  PY="py"
else
  PY="python"
fi

# ── Wrapper log — separate from the orchestrator's tee'd log. Only holds ──
# ── cron-side breadcrumbs (start/stop/exit/env). The pipeline UI does   ──
# ── NOT read this file; it reads logs/refresh_all_{TS}.log which the    ──
# ── orchestrator opens itself.                                          ──
mkdir -p "$SCRIPT_DIR/logs"
STAMP="$(date +'%Y%m%d_%H%M%S')"
WRAPPER_LOG="$SCRIPT_DIR/logs/cron_wrapper_${STAMP}.log"

{
  echo "===================================================================="
  echo "[cron] $(date '+%Y-%m-%d %H:%M:%S %Z')  wrapper start"
  echo "[cron] phase=$PHASE  extra=(${EXTRA_ARGS[*]:-})"
  echo "[cron] py=$PY"
  echo "[cron] host=$(hostname)  user=${USER:-${USERNAME:-unknown}}"
  echo "[cron] cwd=$SCRIPT_DIR"
  echo "===================================================================="
} | tee -a "$WRAPPER_LOG"

START_TS=$(date +%s)

# Run the orchestrator. Its own tee'd log at logs/refresh_all_{TS}.log is
# what pipeline.html reads. We also mirror stdout/stderr into the wrapper
# log so the cron scheduler has everything in one file if the orchestrator
# crashes before opening its own log.
set +e
"$PY" _refresh_all_dashboard_data.py --phase "$PHASE" "${EXTRA_ARGS[@]}" 2>&1 \
  | tee -a "$WRAPPER_LOG"
EXIT=${PIPESTATUS[0]}
set -e

END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))

{
  echo "===================================================================="
  echo "[cron] $(date '+%Y-%m-%d %H:%M:%S %Z')  wrapper end"
  echo "[cron] exit=$EXIT  elapsed=${ELAPSED}s (~$((ELAPSED/60)) min)"
  echo "===================================================================="
} | tee -a "$WRAPPER_LOG"

# Rotate wrapper logs: keep only newest 30 (~1 month at daily). Uses find
# with -mtime as a safety net so a run gone crazy can't fill the disk.
find "$SCRIPT_DIR/logs" -maxdepth 1 -name 'cron_wrapper_*.log' -type f \
  -printf '%T@ %p\n' 2>/dev/null \
  | sort -nr | awk 'NR>30 {print $2}' | xargs -r rm -f || true

exit $EXIT
