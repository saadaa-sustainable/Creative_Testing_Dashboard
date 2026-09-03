"""check_cron_status.py -- read recent cron_run_log rows.

Run from either repo. Shows the last N runs per project with a
color-blind-friendly status marker + step counts + duration + workflow
run URL. Useful for:

  - Confirming this morning's refresh actually completed
  - Diagnosing which step failed without opening GitHub
  - Debugging why the dashboard is stale ("cron ran? or ran + failed?")

Usage:
    ./.venv/Scripts/python.exe scripts/check_cron_status.py
    ./.venv/Scripts/python.exe scripts/check_cron_status.py --project ctd
    ./.venv/Scripts/python.exe scripts/check_cron_status.py --limit 20 --failed-only
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

from dotenv import load_dotenv

load_dotenv(pathlib.Path(__file__).resolve().parents[1] / ".env", override=False)

import psycopg2  # noqa: E402


def _dsn() -> str:
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("need DATABASE_URL_SYNC or SUPABASE_DB_URL in .env")
    return url.replace("postgresql+psycopg2://", "postgresql://").split("?")[0]


def _dur(sec: float | None) -> str:
    if not sec:
        return "  -  "
    m, s = divmod(int(sec), 60)
    return f"{m:>3}m{s:02}s" if m > 0 else f"    {s:2}s"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=None,
                    help="ctd | backend_project (default: both)")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--failed-only", action="store_true")
    ap.add_argument("--show-steps", action="store_true",
                    help="expand the per-step json for the newest run")
    args = ap.parse_args()

    where = []
    params: list = []
    if args.project:
        where.append("project = %s")
        params.append(args.project)
    if args.failed_only:
        where.append("status IN ('failed','partial')")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    conn = psycopg2.connect(_dsn())
    cur  = conn.cursor()
    cur.execute(f"""
        SELECT id, project, run_started, run_finished, duration_s, status,
               workflow_run, n_steps_ok, n_steps_fail, error, steps
          FROM public.cron_run_log
          {where_sql}
      ORDER BY run_started DESC
         LIMIT %s
    """, [*params, args.limit])
    rows = cur.fetchall()

    if not rows:
        print("no runs matching filters")
        return 0

    print(f"\n{'when (IST)':<18} {'project':<18} {'status':<8} "
          f"{'dur':<8} {'ok/fail':<7} {'gh run':<12}  error")
    print("-" * 100)
    for id_, project, started, finished, dur, status, wf, ok, fail, err, steps in rows:
        # started is UTC in DB; add 5.5h for IST display without pulling pytz
        ist = (started.astimezone() if started.tzinfo else started).timestamp()
        from datetime import datetime, timezone, timedelta
        ist_dt = datetime.fromtimestamp(ist, timezone.utc) + timedelta(hours=5, minutes=30)
        when = ist_dt.strftime("%m-%d %H:%M")

        marker = {
            "ok":      "OK",
            "partial": "PART",
            "failed":  "FAIL",
            "running": "RUN",
        }.get(status, status)

        print(f"{when:<18} {project:<18} {marker:<8} {_dur(dur):<8} "
              f"{ok or 0:>2}/{fail or 0:<3} {wf or '-':<12}  "
              f"{(err or '')[:60]}")

    if args.show_steps and rows:
        _, project, started, _, _, status, _, _, _, _, steps = rows[0]
        print(f"\nSteps for newest run  (project={project}, status={status}):")
        if steps:
            for name, meta in steps.items():
                st = meta.get("status", "?")
                d  = meta.get("duration_s")
                e  = meta.get("error")
                print(f"  {st:<8} {_dur(d):<8}  {name}")
                if e:
                    print(f"       -> {e[:200]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
