"""wrap_refresh_all.py -- GH-Actions-facing wrapper around
backend/_refresh_all_dashboard_data.py.

Runs the existing orchestrator (untouched -- don't reinvent 314 lines
that already work), then reads its status JSON from backend/logs and
copies the step-by-step summary into public.cron_run_log so the daily
run shows up in the same place as Backend_Project's daily run.

Exits non-zero if ANY step failed, so GitHub marks the workflow red
and sends a notification.

Usage:
    python scripts/wrap_refresh_all.py             # full refresh
    python scripts/wrap_refresh_all.py --skip-meta # phases 2+3+4 only
"""
from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import time
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / "backend" / ".env", override=False)

from utils.cron_log import CronRun  # noqa: E402

ORCHESTRATOR = ROOT / "backend" / "_refresh_all_dashboard_data.py"
LOG_DIR      = ROOT / "backend" / "logs"


def _newest_status_json(after_epoch: float) -> pathlib.Path | None:
    candidates = [
        p for p in LOG_DIR.glob("refresh_all_*.json")
        if p.stat().st_mtime >= after_epoch
    ]
    return max(candidates, default=None, key=lambda p: p.stat().st_mtime)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-meta", action="store_true",
                    help="pass --skip-meta to the underlying orchestrator")
    args = ap.parse_args()

    orchestrator_args = [sys.executable, str(ORCHESTRATOR)]
    if args.skip_meta:
        orchestrator_args.append("--skip-meta")

    with CronRun(project="ctd") as run:
        # Open a single wrapping step that IS the entire orchestrator
        # invocation. Individual sub-steps get replayed into the run's
        # `steps` dict below so cron_run_log gets fine-grained per-script
        # status too.
        wrap_started = time.time()
        with run.step("orchestrator", timeout=10800) as step:
            rc = step.run(orchestrator_args, cwd=str(ROOT))

        # Replay per-script results from the orchestrator's own JSON. The
        # orchestrator writes ONE JSON per invocation to backend/logs;
        # find the newest one that landed AFTER we started.
        status_json = _newest_status_json(wrap_started - 1)
        if status_json is not None:
            try:
                summary = json.loads(status_json.read_text(encoding="utf-8"))
                for s in summary.get("steps", []):
                    run.steps[f"orch:{s['label']}"] = {
                        "status":     "ok" if s.get("status") == "OK" else "failed",
                        "duration_s": s.get("duration_sec"),
                        "error":      s.get("error"),
                    }
                # Re-flush so cron_run_log has the fine-grained view.
                run._flush_progress()
            except Exception as e:
                print(f"[wrap] could not parse status JSON: {e}", flush=True)
        else:
            print("[wrap] no status JSON found -- orchestrator may have "
                  "crashed before writing one", flush=True)

    # Non-zero if the orchestrator itself failed OR any sub-step failed.
    orch_ok    = run.steps.get("orchestrator", {}).get("status") == "ok"
    sub_fail_n = sum(1 for k, v in run.steps.items()
                     if k.startswith("orch:") and v["status"] == "failed")
    return 0 if (orch_ok and sub_fail_n == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
