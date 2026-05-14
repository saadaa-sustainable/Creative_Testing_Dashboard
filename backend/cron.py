"""
cron_runner.py — SAADAA Meta Ads Sync Scheduler
═══════════════════════════════════════════════════════════════════════
Schedule (post-refactor — only one cadence):
  primary_table  → every night at 12:30 AM IST. Immediately after the primary
                   sync completes, the lifecycle classifier and results_table
                   (rolling-30-day snapshot) are refreshed in sequence.

The previous 3-hourly results_sync trigger has been removed: results_table
now only stores ONE rolling-30-day snapshot per account, refreshed nightly,
so intra-day refreshes added churn without value.

Run:
  python cron.py           # starts the scheduler (runs forever)
  python cron.py --now primary   # run primary sync immediately
  python cron.py --now results   # run results sync immediately (manual)
  python cron.py --status        # show next run time

Keep alive:  nohup python cron.py > logs/cron.log 2>&1 &
Or Windows:  pythonw cron.py
"""

import sys
import os
import time
import logging
import subprocess
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Project root — adjust if cron_runner.py is not in the same folder ──
PROJECT_DIR = Path(__file__).parent.resolve()
PYTHON = sys.executable  # same python that's running this script
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ── Logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "cron.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("cron_runner")

# ── IST timezone (UTC+5:30) ──────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))


def now_ist():
    return datetime.now(IST)


def fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S IST")


# ════════════════════════════════════════════════════════════════════════
# JOB DEFINITIONS
# ════════════════════════════════════════════════════════════════════════


def run_primary_sync():
    """
    Primary table sync — fetches last 2 days from Meta API → primary_table.
    Runs nightly at 12:30 AM IST.
    Uses primary_sync.py which handles the full pipeline.
    """
    log.info("=" * 60)
    log.info("▶  PRIMARY SYNC — primary_table update")
    log.info(f"   Started: {fmt(now_ist())}")
    log.info("=" * 60)

    script = PROJECT_DIR / "primary_sync.py"
    if not script.exists():
        log.error(f"primary_sync.py not found at {script}")
        return False

    try:
        result = subprocess.run(
            [PYTHON, str(script), "daily"],
            cwd=str(PROJECT_DIR),
            capture_output=False,  # let output stream to log file
            timeout=3600,  # 1hr max
        )
        success = result.returncode == 0
        log.info(
            f'   Primary sync {"✓ done" if success else "❌ FAILED"} — exit code {result.returncode}'
        )
        return success
    except subprocess.TimeoutExpired:
        log.error("   Primary sync TIMED OUT after 60 minutes")
        return False
    except Exception as e:
        log.error(f"   Primary sync ERROR: {e}")
        traceback.print_exc()
        return False


def run_lifecycle_classifier():
    """
    14-day-buffer lifecycle resolver — recomputes ad_results for every ad in
    primary_table. Runs after each successful primary sync so today's
    Result Pending → Failed transitions land same-night.
    """
    log.info("-" * 60)
    log.info("▶  LIFECYCLE CLASSIFIER — ad_results update")
    log.info(f"   Started: {fmt(now_ist())}")
    log.info("-" * 60)

    script = PROJECT_DIR / "result_classifier.py"
    if not script.exists():
        log.error(f"result_classifier.py not found at {script}")
        return False

    try:
        result = subprocess.run(
            [PYTHON, str(script)],
            cwd=str(PROJECT_DIR),
            capture_output=False,
            timeout=600,  # 10min — pure compute, no Meta API
        )
        success = result.returncode == 0
        log.info(
            f'   Lifecycle classifier {"✓ done" if success else "❌ FAILED"} — exit code {result.returncode}'
        )
        return success
    except subprocess.TimeoutExpired:
        log.error("   Lifecycle classifier TIMED OUT after 10 minutes")
        return False
    except Exception as e:
        log.error(f"   Lifecycle classifier ERROR: {e}")
        traceback.print_exc()
        return False


def run_results_sync():
    """
    Results table sync — reads primary_table → computes metrics → results_table.
    Runs every 3 hours.
    Uses results_sync.py.
    """
    log.info("-" * 60)
    log.info("▶  RESULTS SYNC — results_table update")
    log.info(f"   Started: {fmt(now_ist())}")
    log.info("-" * 60)

    script = PROJECT_DIR / "results_sync.py"
    if not script.exists():
        log.error(f"results_sync.py not found at {script}")
        return False

    try:
        result = subprocess.run(
            [PYTHON, str(script)],
            cwd=str(PROJECT_DIR),
            capture_output=False,
            timeout=1800,  # 30min max (it's a local computation)
        )
        success = result.returncode == 0
        log.info(
            f'   Results sync {"✓ done" if success else "❌ FAILED"} — exit code {result.returncode}'
        )
        return success
    except subprocess.TimeoutExpired:
        log.error("   Results sync TIMED OUT after 30 minutes")
        return False
    except Exception as e:
        log.error(f"   Results sync ERROR: {e}")
        traceback.print_exc()
        return False


def run_hourly_impressions():
    """
    Hourly impressions refresh — fetches today's Meta data, combines with the
    last 30 days from primary_table (read-only), recomputes per-ad aggregates,
    and writes a fresh row into results_table. Does NOT modify primary_table.
    Runs every hour at :30 IST except 00:30 (which is the nightly primary slot).
    """
    log.info("-" * 60)
    log.info("▶  HOURLY IMPRESSIONS — results_table refresh from Meta API")
    log.info(f"   Started: {fmt(now_ist())}")
    log.info("-" * 60)

    script = PROJECT_DIR / "hourly_impressions.py"
    if not script.exists():
        log.error(f"hourly_impressions.py not found at {script}")
        return False

    try:
        result = subprocess.run(
            [PYTHON, str(script)],
            cwd=str(PROJECT_DIR),
            capture_output=False,
            timeout=600,  # 10 min max — single Meta call + DB writes
        )
        success = result.returncode == 0
        log.info(
            f'   Hourly impressions {"✓ done" if success else "❌ FAILED"} — exit code {result.returncode}'
        )
        return success
    except subprocess.TimeoutExpired:
        log.error("   Hourly impressions TIMED OUT after 10 minutes")
        return False
    except Exception as e:
        log.error(f"   Hourly impressions ERROR: {e}")
        traceback.print_exc()
        return False


# ════════════════════════════════════════════════════════════════════════
# SCHEDULE LOGIC
# ════════════════════════════════════════════════════════════════════════

# Primary sync fires at 00:30 IST; results_table refresh chains after it.
PRIMARY_HOUR_IST = 0
PRIMARY_MINUTE_IST = 30

# Hourly impressions refresh fires at :30 of every hour EXCEPT 00:30 (handled by primary).
HOURLY_MINUTE_IST = 30


def next_run_times():
    """Return next primary + next hourly times."""
    now = now_ist()
    primary_today = now.replace(
        hour=PRIMARY_HOUR_IST, minute=PRIMARY_MINUTE_IST, second=0, microsecond=0
    )
    primary_next = primary_today + timedelta(days=1) if now >= primary_today else primary_today

    # Next hourly slot — the next :30 mark after `now`, skipping 00:30.
    hourly_next = now.replace(minute=HOURLY_MINUTE_IST, second=0, microsecond=0)
    if hourly_next <= now:
        hourly_next += timedelta(hours=1)
    if hourly_next.hour == PRIMARY_HOUR_IST:  # skip 00:30 — that's the primary slot
        hourly_next += timedelta(hours=1)
    return primary_next, hourly_next


def should_run_primary(now):
    return now.hour == PRIMARY_HOUR_IST and now.minute == PRIMARY_MINUTE_IST


def should_run_hourly(now):
    # Fire at :30 of every hour, except 00:30 (handled by primary which chains
    # the full refresh including results_table).
    return now.minute == HOURLY_MINUTE_IST and now.hour != PRIMARY_HOUR_IST


# ════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ════════════════════════════════════════════════════════════════════════


def run_scheduler():
    log.info("=" * 60)
    log.info("  SAADAA Meta Ads Cron Runner — starting")
    log.info(f"  Project dir : {PROJECT_DIR}")
    log.info(f"  Python      : {PYTHON}")
    log.info(f"  Log dir     : {LOG_DIR}")
    log.info("=" * 60)

    primary_next, hourly_next = next_run_times()
    log.info(f"  Next primary sync  : {fmt(primary_next)}")
    log.info(f"  Next hourly refresh: {fmt(hourly_next)}")
    log.info("  (Primary at 00:30 IST nightly; hourly impressions refresh at :30 of every other hour)")
    log.info("")

    last_primary_fired = None
    last_hourly_fired = None

    while True:
        try:
            now = now_ist()
            minute_key = now.strftime("%Y-%m-%d %H:%M")

            # ── Primary sync at 00:30 IST ─────────────────────────
            if should_run_primary(now) and last_primary_fired != minute_key:
                last_primary_fired = minute_key
                log.info(f"\n🌙 Nightly primary sync triggered at {fmt(now)}")
                ok = run_primary_sync()

                # After primary, refresh ad_results lifecycle and results_table
                if ok:
                    log.info("  → Recomputing ad_results lifecycle...")
                    run_lifecycle_classifier()
                    log.info("  → Refreshing results_table (rolling 30-day snapshot)...")
                    run_results_sync()

                primary_next, hourly_next = next_run_times()
                log.info(f"  Next primary sync: {fmt(primary_next)}")

            # ── Hourly impressions refresh at :30 of every hour (except 00:30) ──
            elif should_run_hourly(now) and last_hourly_fired != minute_key:
                last_hourly_fired = minute_key
                log.info(f"\n⏱  Hourly impressions refresh triggered at {fmt(now)}")
                run_hourly_impressions()
                primary_next, hourly_next = next_run_times()
                log.info(f"  Next hourly refresh: {fmt(hourly_next)}")

            # Sleep 30 seconds between checks
            time.sleep(30)

        except KeyboardInterrupt:
            log.info("\n⛔ Scheduler stopped by user (Ctrl+C)")
            break
        except Exception as e:
            log.error(f"Scheduler loop error: {e}")
            traceback.print_exc()
            time.sleep(60)  # back off on error


# ════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    args = sys.argv[1:]

    if "--now" in args:
        job = (
            args[args.index("--now") + 1] if len(args) > args.index("--now") + 1 else ""
        )
        if job == "primary":
            log.info("Running primary sync immediately (--now)")
            run_primary_sync()
        elif job == "results":
            log.info("Running results sync immediately (--now)")
            run_results_sync()
        elif job == "lifecycle":
            log.info("Running lifecycle classifier immediately (--now)")
            run_lifecycle_classifier()
        elif job == "hourly":
            log.info("Running hourly impressions refresh immediately (--now)")
            run_hourly_impressions()
        elif job == "both":
            log.info("Running both syncs immediately (--now)")
            run_primary_sync()
            run_lifecycle_classifier()
            run_results_sync()
        else:
            print("Usage: python cron_runner.py --now [primary|results|lifecycle|hourly|both]")

    elif "--status" in args:
        primary_next, hourly_next = next_run_times()
        now = now_ist()
        print(f"\nCurrent time      : {fmt(now)}")
        print(f"Next primary sync : {fmt(primary_next)}  (nightly 12:30 AM IST)")
        print(f"                    (lifecycle classifier + results_table chain after it)")
        print(f"Next hourly refresh: {fmt(hourly_next)}  (every hour at :30, skipping 00:30)")
        secs_p = int((primary_next - now).total_seconds())
        secs_h = int((hourly_next - now).total_seconds())
        print(f"  Primary in: {secs_p//3600}h {(secs_p%3600)//60}m")
        print(f"  Hourly in : {secs_h//3600}h {(secs_h%3600)//60}m")

    else:
        run_scheduler()
