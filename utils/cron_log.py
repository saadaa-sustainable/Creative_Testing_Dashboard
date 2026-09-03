"""cron_log — thin wrapper for writing daily-refresh status to Supabase.

Used by BOTH scripts/refresh_all_daily.py (in this repo) and its CTD
counterpart (scripts/wrap_refresh_all.py). Keeping the module tiny +
identical in both repos means the target table stays consistent and I
(Claude) can read a single log from any future session with:

    ./.venv/Scripts/python.exe scripts/check_cron_status.py

Table shape lives in the table itself; we only need to know column names
here. See docs on `public.cron_run_log`.

The pattern is:

    from utils.cron_log import CronRun
    with CronRun(project="backend_project") as run:
        for name, cmd, timeout in STEPS:
            with run.step(name, timeout=timeout) as step:
                step.run(cmd)          # subprocess with capture

    # run.__exit__ writes the final row -- one row per orchestrator
    # invocation, whether it succeeded or blew up.

No dependency on any web framework -- just psycopg2, so this works in
GitHub Actions Ubuntu runners the same way it works locally.
"""
from __future__ import annotations

import json
import os
import platform
import socket
import subprocess
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

import psycopg2
import psycopg2.extras


# GitHub Actions exposes these; when running locally they'll just be None.
_GH_RUN_ID  = os.environ.get("GITHUB_RUN_ID")
_GH_SHA     = os.environ.get("GITHUB_SHA")
_HOST_LABEL = (
    f"gh-actions:{_GH_RUN_ID}" if _GH_RUN_ID
    else f"{platform.system()}:{socket.gethostname()}"
)


def _dsn() -> str:
    # Prefer DATABASE_URL_SYNC (BP convention). Fall back to SUPABASE_DB_URL
    # (CTD convention) so this same module works verbatim in both repos.
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError(
            "cron_log needs DATABASE_URL_SYNC or SUPABASE_DB_URL in the environment"
        )
    return url.replace("postgresql+psycopg2://", "postgresql://").split("?")[0]


class _Step:
    """Per-step timer + subprocess runner. Captures return code + tail of
    stdout/stderr as the 'error' if the step fails."""

    def __init__(self, name: str, timeout: int):
        self.name    = name
        self.timeout = timeout
        self.status  = "pending"      # ok|failed|skipped
        self.error: str | None = None
        self._t0     = 0.0
        self.dur_s   = 0.0

    def run(self, cmd: list[str], cwd: str | None = None) -> int:
        """Fork the subprocess and stream to console. On non-zero exit we
        record the tail of the merged output; the orchestrator sees the
        raised CalledProcessError so it can decide to continue vs abort."""
        try:
            proc = subprocess.run(
                cmd,
                cwd=cwd,
                timeout=self.timeout,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except subprocess.TimeoutExpired as e:
            self.status = "failed"
            self.error  = f"timeout after {self.timeout}s"
            print(f"[{self.name}] TIMEOUT after {self.timeout}s", flush=True)
            raise

        # Always echo the child's stdout so GH Actions log shows it live-ish
        # (subprocess.PIPE buffers, so it's not fully streaming -- fine for
        # end-of-day cron use).
        if proc.stdout:
            print(proc.stdout, end="", flush=True)
        if proc.returncode != 0:
            self.status = "failed"
            # Tail so we don't blow the row size; last 2 KB is usually enough
            # to spot the traceback root.
            tail = (proc.stdout or "").strip()[-2000:]
            self.error = f"exit={proc.returncode}\n{tail}"
        else:
            self.status = "ok"
        return proc.returncode

    def skip(self, reason: str) -> None:
        self.status = "skipped"
        self.error  = reason


class CronRun:
    """Context manager that owns one row in cron_run_log."""

    def __init__(self, project: str):
        self.project = project
        self.steps: dict[str, dict[str, Any]] = {}
        self._t0 = 0.0
        self._conn: psycopg2.extensions.connection | None = None
        self._id: int | None = None

    def __enter__(self) -> "CronRun":
        self._t0 = time.time()
        self._conn = psycopg2.connect(_dsn())
        self._conn.autocommit = True
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.cron_run_log
                    (project, run_started, status, workflow_run, git_sha, host, steps)
                VALUES (%s, now(), 'running', %s, %s, %s, '{}'::jsonb)
                RETURNING id
                """,
                (self.project, _GH_RUN_ID, _GH_SHA, _HOST_LABEL),
            )
            self._id = cur.fetchone()[0]
        print(f"[cron_log] opened run id={self._id} project={self.project}", flush=True)
        return self

    @contextmanager
    def step(self, name: str, timeout: int = 3600) -> Iterator[_Step]:
        s = _Step(name, timeout)
        s._t0 = time.time()
        print(f"\n===== {name} =====", flush=True)
        try:
            yield s
        except Exception as e:
            if s.status == "pending":
                s.status = "failed"
                s.error  = f"{type(e).__name__}: {e}"
        finally:
            if s.status == "pending":
                s.status = "ok"
            s.dur_s = round(time.time() - s._t0, 2)
            self.steps[name] = {
                "status":     s.status,
                "duration_s": s.dur_s,
                "error":      s.error,
            }
            print(f"[{name}] {s.status} in {s.dur_s:.1f}s", flush=True)
            # Persist progress after every step so a mid-run crash still
            # leaves partial state visible in Supabase.
            self._flush_progress()

    def _flush_progress(self) -> None:
        assert self._conn is not None and self._id is not None
        ok    = sum(1 for s in self.steps.values() if s["status"] == "ok")
        fail  = sum(1 for s in self.steps.values() if s["status"] == "failed")
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.cron_run_log
                   SET steps = %s::jsonb,
                       n_steps_ok = %s,
                       n_steps_fail = %s,
                       duration_s = EXTRACT(EPOCH FROM (now() - run_started))
                 WHERE id = %s
                """,
                (json.dumps(self.steps), ok, fail, self._id),
            )

    def __exit__(self, exc_type, exc, tb) -> bool:
        assert self._conn is not None
        ok    = sum(1 for s in self.steps.values() if s["status"] == "ok")
        fail  = sum(1 for s in self.steps.values() if s["status"] == "failed")
        if exc is not None:
            outcome = "failed"
            err_text = f"{exc_type.__name__}: {exc}\n{''.join(traceback.format_tb(tb))[-1500:]}"
        elif fail == 0:
            outcome = "ok"
            err_text = None
        elif ok > 0:
            outcome = "partial"
            err_text = f"{fail} step(s) failed"
        else:
            outcome = "failed"
            err_text = "all steps failed"

        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.cron_run_log
                   SET run_finished = now(),
                       duration_s   = EXTRACT(EPOCH FROM (now() - run_started)),
                       status       = %s,
                       steps        = %s::jsonb,
                       n_steps_ok   = %s,
                       n_steps_fail = %s,
                       error        = %s
                 WHERE id = %s
                """,
                (outcome, json.dumps(self.steps), ok, fail, err_text, self._id),
            )
        self._conn.close()
        print(f"\n[cron_log] closed run id={self._id} outcome={outcome} "
              f"ok={ok} fail={fail}", flush=True)
        # Never swallow the outer exception -- let GH Actions mark the job red.
        return False
