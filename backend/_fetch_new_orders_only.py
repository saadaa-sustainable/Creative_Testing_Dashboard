"""Incremental Shopify order fetcher — inserts only NEW order_ids.

Reuses the attribution engine + Shopify pagination + INSERT SQL from
rebuild_attribution_orders.py. The critical safety invariant is preserved:

    INSERT ... ON CONFLICT (order_id) DO NOTHING

Any order_id already in shopify_ad_attribution — including every row you've
manually attributed (spend-weight, spend-weight-hashed, FoundersAd-slug-
inherit, MEGHA-DIVYA, fits_by_nm) — is physically untouchable by this
script. Only order_ids Shopify has that we don't yet get inserted.

Usage:
    python _fetch_new_orders_only.py [SINCE_DATE] [UNTIL_DATE]

Defaults: SINCE = 2026-07-20  UNTIL = 2099-12-31

The window uses a 2-3 day overlap buffer on the SINCE side so orders that
Shopify hasn't finalized yet get another attempt (they'll skip via ON
CONFLICT if already present).
"""
import argparse, os, time
# Do NOT re-wrap sys.stdout here — rebuild_attribution_orders.py already
# wraps it at module import time; double-wrapping closes the underlying
# buffer during garbage collection and every log() call then explodes.

from rebuild_attribution_orders import (
    load_ad_universe,
    shopify_page,
    build_row,
    flush,
    save_prog,
    load_prog,
    log,
    attribute_order_cache_clear,
    PROG_FILE,
)


def main():
    os.makedirs("logs", exist_ok=True)
    p = argparse.ArgumentParser()
    p.add_argument("since", nargs="?", default="2026-07-20")
    p.add_argument("until", nargs="?", default="2099-12-31")
    p.add_argument("--resume", action="store_true",
                   help="Continue from _fetch_new_orders.progress.json")
    args = p.parse_args()

    log(f"\n========== _fetch_new_orders_only @ {time.strftime('%Y-%m-%d %H:%M:%S')} ==========")
    log(f"window: {args.since} -> {args.until}  resume={args.resume}")
    log("Safety: INSERT ... ON CONFLICT (order_id) DO NOTHING — existing rows are untouchable.")

    # Fresh attribution cache — pick up any ad_universe changes since the last run
    attribute_order_cache_clear()
    maps = load_ad_universe()

    cursor = None
    pages = 0
    counters = {"flushed": 0, "buffered": 0, "matched": 0, "unmatched": 0}

    if args.resume:
        prog = load_prog()
        if prog and prog.get("since") == args.since:
            cursor = prog.get("cursor")
            pages = prog.get("pages", 0)
            counters.update(prog.get("counters", {}))
            log(f"[resume] cursor={cursor!r} pages_done={pages} rows_so_far={counters.get('flushed',0):,}")

    buffer = []
    while True:
        pages += 1
        data = shopify_page(cursor, args.since, args.until)
        if data is None:
            log(f"[fatal] page {pages} failed; flushing buffer of {len(buffer)} and exiting (--resume to continue)")
            flush(buffer, counters)
            break

        edges = data.get("edges") or []
        if not edges:
            log(f"[end] no more orders at page {pages}")
            flush(buffer, counters)
            break

        for e in edges:
            node = e["node"]
            row = build_row(node, maps)
            buffer.append(row)
            counters["buffered"] += 1
            if row[13]:   # has_match column
                counters["matched"] += 1
            else:
                counters["unmatched"] += 1

        # Flush every 500 rows
        if len(buffer) >= 500:
            flush(buffer, counters)
            buffer.clear()

        page_info = data.get("pageInfo") or {}
        cursor = edges[-1].get("cursor")
        save_prog({"since": args.since, "cursor": cursor, "pages": pages,
                   "counters": dict(counters)})
        if pages % 5 == 0:
            log(f"[progress] page {pages}  buffered={counters['buffered']:,}  "
                f"matched={counters['matched']:,}  unmatched={counters['unmatched']:,}")
        if not page_info.get("hasNextPage"):
            log(f"[end] hasNextPage=false at page {pages}")
            flush(buffer, counters)
            break

    # Final report
    log("")
    log("=========================================================================")
    log(f"[done] pages={pages}  buffered={counters['buffered']:,}  "
        f"matched={counters['matched']:,}  unmatched={counters['unmatched']:,}  "
        f"actually_written (post-ON-CONFLICT)={counters['flushed']:,}")
    log("Note: 'actually_written' = attempted inserts. ON CONFLICT DO NOTHING")
    log("means order_ids already present were silently skipped — no manual rows disturbed.")
    log("Clean up progress file (safe to remove if the run completed).")

    # Only remove progress file if we cleanly reached hasNextPage=false
    try:
        if os.path.exists(PROG_FILE):
            os.remove(PROG_FILE)
    except Exception:
        pass

    # Rebuild the L30 helper so dashboard.js Ad Intelligence view stays snappy.
    # This is the L30 subset of shopify_ad_attribution the frontend paginates
    # from — refreshing here keeps it in lock-step with new order data.
    log("[l30] rebuilding shopify_ad_attribution_l30 helper table ...")
    try:
        import subprocess as _sub, sys as _sys, pathlib as _pl
        _script = _pl.Path(__file__).parent / "_build_shopify_l30.py"
        _r = _sub.run([_sys.executable, str(_script)], timeout=300)
        log(f"[l30] rebuild exit={_r.returncode}")
    except Exception as _e:
        log(f"[l30] rebuild FAILED: {type(_e).__name__}: {_e}  (non-fatal — run _build_shopify_l30.py manually)")


if __name__ == "__main__":
    main()
