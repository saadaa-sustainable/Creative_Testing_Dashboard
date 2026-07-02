"""
run_all.py — orchestrate the QA suite, produce REPORT.md + _results.json.

Usage:
  python -m backend.qa.run_all              (from repo root)
  python backend/qa/run_all.py              (from repo root)
  python run_all.py                         (from backend/qa/)

Exit codes:
  0  every check PASS or WARN
  1  one or more checks FAIL
"""
from __future__ import annotations
import sys, json, time
from datetime import datetime, timezone
from pathlib import Path

# Add the parent (backend/) directory to path so `qa.` package imports work
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE.parent.parent))

from backend.qa._common import run_suite, RESULTS_JSON, REPORT_MD, scrub
from backend.qa import (
    test_01_meta_vs_db,
    test_02_category_logic,
    test_03_ae_table_view,
    test_04_freq_lifecycle,
    test_05_shopify_attribution,
    test_06_cross_view,
    test_07_dashboard_ui,
    test_08_pipeline_health,
)

MODULES = [
    ("01 · Meta API vs Supabase",       test_01_meta_vs_db),
    ("02 · Category algorithm",         test_02_category_logic),
    ("03 · ae_table_view integrity",    test_03_ae_table_view),
    ("04 · Freq lifecycle integrity",   test_04_freq_lifecycle),
    ("05 · Shopify attribution",        test_05_shopify_attribution),
    ("06 · Cross-view parity",          test_06_cross_view),
    ("07 · Dashboard UI (Playwright)",  test_07_dashboard_ui),
    ("08 · Pipeline health",            test_08_pipeline_health),
]

def main():
    started = time.time()
    print("─" * 78)
    print(f"  Saadaa Dashboard — Production Readiness QA Suite")
    print(f"  {datetime.now().isoformat(timespec='seconds')}")
    print("─" * 78)

    suites = []
    for name, mod in MODULES:
        s = run_suite(name, mod.run)
        suites.append(s)

    # Roll-up
    totals = {"PASS":0,"FAIL":0,"WARN":0,"SKIP":0}
    for s in suites:
        for k, v in s.counts.items(): totals[k] += v

    duration = time.time() - started

    # Console summary
    print("\n" + "═" * 78)
    print(f"  SUMMARY")
    print("═" * 78)
    for s in suites:
        c = s.counts
        line = f"    {s.module:44s}  " \
               f"✓{c['PASS']:>3}  ✗{c['FAIL']:>3}  !{c['WARN']:>3}  ·{c['SKIP']:>3}"
        print(line)
    print("─" * 78)
    print(f"    {'TOTAL':44s}  " \
          f"✓{totals['PASS']:>3}  ✗{totals['FAIL']:>3}  !{totals['WARN']:>3}  ·{totals['SKIP']:>3}")
    print(f"    duration: {duration:.1f}s")

    # JSON summary
    payload = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(duration, 2),
        "totals":     totals,
        "suites":     [s.to_dict() for s in suites],
    }
    RESULTS_JSON.write_text(json.dumps(payload, indent=2, default=str),
                             encoding="utf-8")

    # Markdown report
    md = []
    md.append(f"# Dashboard QA Report")
    md.append("")
    md.append(f"_Generated {datetime.now().isoformat(timespec='seconds')} · "
              f"{duration:.1f}s wall clock_")
    md.append("")
    verdict = ("**PRODUCTION READY**" if totals["FAIL"] == 0
               else "**BLOCKED — one or more failures**")
    md.append(f"## Verdict: {verdict}")
    md.append("")
    md.append("```")
    md.append(f"  PASS = {totals['PASS']:>4}")
    md.append(f"  FAIL = {totals['FAIL']:>4}")
    md.append(f"  WARN = {totals['WARN']:>4}")
    md.append(f"  SKIP = {totals['SKIP']:>4}")
    md.append("```")
    md.append("")
    md.append("## Per-suite summary")
    md.append("")
    md.append("```")
    for s in suites:
        c = s.counts
        md.append(f"  {s.module:44s}  ✓{c['PASS']:>3}  ✗{c['FAIL']:>3}  !{c['WARN']:>3}  ·{c['SKIP']:>3}")
    md.append("```")
    md.append("")

    for s in suites:
        md.append(f"## {s.module}")
        md.append("")
        for c in s.checks:
            mark = {"PASS":"✓","FAIL":"✗","WARN":"!","SKIP":"·"}[c.status]
            body = f"- **{mark} {c.name}** — {c.detail}"
            if c.expected or c.actual:
                body += f"  (expected `{c.expected}` · actual `{c.actual}`)"
            md.append(scrub(body))
        md.append("")

    REPORT_MD.write_text("\n".join(md), encoding="utf-8")
    print(f"\n  → {RESULTS_JSON.relative_to(_HERE.parent.parent)}")
    print(f"  → {REPORT_MD.relative_to(_HERE.parent.parent)}")

    return 1 if totals["FAIL"] else 0

if __name__ == "__main__":
    sys.exit(main())
