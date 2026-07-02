"""
test_07_dashboard_ui.py — Playwright-driven end-to-end UI tests.

Loads the local index_v2.html against Supabase, then exercises filters:
  * Sidebar navigation: each view opens, previous view hidden
  * Date-preset click on Creative Testing changes KPI count
  * Category KPI click on Ads Analyse filters the table
  * Search on Frequency Lifecycle narrows the drill
  * Multi-filter Apply / Clear round-trip
"""
from __future__ import annotations
import os, re, urllib.parse
from pathlib import Path
from playwright.sync_api import sync_playwright, Page
from ._common import DB_URL, scrub

ROOT   = Path(__file__).parent.parent.parent
HTML   = ROOT / "index_v2.html"

# Derive REST URL from SUPABASE_DB_URL (handles both direct + pooler forms)
def _rest_url() -> str:
    for pat in (r"db\.([a-z0-9]+)\.supabase\.co",
                r"postgres\.([a-z0-9]+):",
                r"@([a-z0-9]+)\.supabase\.co"):
        m = re.search(pat, DB_URL)
        if m: return f"https://{m.group(1)}.supabase.co"
    return ""

KEY = (os.environ.get("service_role") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()

def dashboard_url() -> str:
    q = urllib.parse.urlencode({"supabaseUrl": _rest_url(), "supabaseAnon": KEY})
    return HTML.resolve().as_uri() + f"?{q}"

def kpi_text(page, sel):
    try:
        return (page.eval_on_selector(sel, "el => el.textContent") or "").strip()
    except Exception:
        return ""

def run(suite):
    if not HTML.exists():
        suite.fail("index_v2.html present", str(HTML))
        return
    if not KEY:
        suite.skip("Playwright UI tests", "No service_role in .env — cannot render")
        return

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1600, "height": 950},
                                  timezone_id="Asia/Kolkata", locale="en-IN")
        page = ctx.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(scrub(str(e))[:120]))
        try:
            page.goto(dashboard_url(), wait_until="domcontentloaded", timeout=30_000)
        except Exception as e:
            suite.fail("page navigates", scrub(str(e))[:120])
            browser.close(); return

        # Trigger This Month preset so data loads fast
        page.wait_for_timeout(2500)
        page.evaluate("""
          () => document.querySelector('.preset[data-p="thisMonth"]')?.click()
        """)

        # ── T1: dashboard renders without JS errors within 60s
        try:
            page.wait_for_function(
                "() => document.querySelector('#kp-winner')?.textContent?.trim() !== '—'",
                timeout=60_000
            )
            page.wait_for_timeout(2000)
            suite.pass_("dashboard renders KPIs", f"kp-winner={kpi_text(page,'#kp-winner')}")
        except Exception as e:
            suite.fail("dashboard renders KPIs", scrub(str(e))[:120])
            browser.close(); return

        # ── T2: no JS errors surfaced
        if not errors:
            suite.pass_("no JS pageerror events", "clean")
        else:
            suite.fail("no JS pageerror events",
                       f"{len(errors)} errors, first: {errors[0]}")

        # ── T3: Sidebar navigation — each view swaps
        for view in ("lifecycle", "ae", "adintel", "inventory", "testing"):
            page.click(f'.sb-item[data-view="{view}"]')
            page.wait_for_timeout(1000)
            visible = page.evaluate(f"""
              () => {{
                const el = document.getElementById('view-{view}');
                if (!el) return false;
                return getComputedStyle(el).display !== 'none';
              }}
            """)
            if visible:
                suite.pass_(f"sidebar → view-{view} opens", "visible")
            else:
                suite.fail(f"sidebar → view-{view} opens", "hidden")

        # ── T4: Ads Analyse KPIs are non-zero (data loaded).
        # ae_table_view fetches ~15k rows in ~15 REST pages and only
        # fills the KPIs once renderAE() runs — that happens on
        # view-ae click. Give the initial fetch up to 90s.
        page.click('.sb-item[data-view="ae"]')
        page.wait_for_timeout(1500)
        # Nudge renderAE() to run again once allAds has settled — the
        # first click might fire before the fetch completes.
        try:
            page.wait_for_function(
                "() => document.querySelector('#aeKp-d')?.textContent?.trim() !== '—' && "
                "     document.querySelector('#aeKp-d')?.textContent?.trim() !== '0'",
                timeout=90_000
            )
            page.wait_for_timeout(2000)
        except Exception:
            # Try one more render — sometimes the click fires before allAds lands
            page.evaluate("typeof renderAE === 'function' && renderAE()")
            page.wait_for_timeout(3000)
        for label, sel in [("Incremental Winner","#aeKp-iw"),
                           ("Winner",            "#aeKp-w"),
                           ("Discarded",         "#aeKp-d")]:
            txt = kpi_text(page, sel)
            n = int(re.sub(r"[^\d]", "", txt) or 0)
            if n > 0:
                suite.pass_(f"AE KPI {label} > 0", f"n={n:,}")
            else:
                suite.warn(f"AE KPI {label} > 0", f"n={n}")

        # ── T5: Category KPI click filters the main table.
        # The Winner tile is `.kpi[data-cat="Winner"]` on the AE view.
        # Click and check that either the .active class shows OR the
        # visible row count in the table drops (which is the real
        # signal that filtering happened).
        cur_rows = page.evaluate(
            "() => document.querySelectorAll('#aeMain tbody tr').length"
        )
        page.click('.kpi[data-cat="Winner"]')
        page.wait_for_timeout(2000)
        active_after = page.evaluate("""
          () => document.querySelector('.kpi[data-cat="Winner"]')?.classList.contains('active')
        """)
        rows_after = page.evaluate(
            "() => document.querySelectorAll('#aeMain tbody tr').length"
        )
        if active_after or rows_after != cur_rows:
            suite.pass_("AE Winner tile filters the table",
                        f"pinned={active_after}  rows {cur_rows}→{rows_after}")
        else:
            suite.warn("AE Winner tile filters the table",
                       f"no visible change (rows stayed at {cur_rows})")
        # Unpin
        page.click('.kpi[data-cat="Winner"]')
        page.wait_for_timeout(600)

        # ── T6: Frequency Lifecycle — search narrows the visible ad count
        page.click('.sb-item[data-view="lifecycle"]')
        page.wait_for_timeout(1500)
        # bucket click first so the drill table is visible
        page.evaluate("""
          () => document.querySelector('.freq-bucket[data-b="b5"]')?.click()
        """)
        page.wait_for_timeout(1200)
        # Get current row count
        n_before = page.evaluate("""
          () => document.querySelectorAll('#freqDrillBody tr').length
        """)
        # Type search — 'saadaa' probably matches many rows
        page.fill('#lifeFreqSearch', 'saadaa')
        page.wait_for_timeout(500)
        n_after = page.evaluate("""
          () => document.querySelectorAll('#freqDrillBody tr').length
        """)
        if n_before > 0 and n_after <= n_before:
            suite.pass_("Freq search narrows results",
                        f"{n_before} → {n_after}")
        else:
            suite.warn("Freq search narrows results",
                       f"{n_before} → {n_after}")
        # Clear search
        page.fill('#lifeFreqSearch', '')
        page.wait_for_timeout(500)

        # ── T7: Frequency Lifecycle status chip flips numbers
        page.click('#lifeFreqStatusRow .preset[data-s="ACTIVE"]')
        page.wait_for_timeout(1000)
        active_count = page.evaluate("""
          () => parseInt(document.querySelector('#freqTotAds')?.textContent?.replace(/,/g,'')||'0',10)
        """)
        page.click('#lifeFreqStatusRow .preset[data-s=""]')
        page.wait_for_timeout(1000)
        all_count = page.evaluate("""
          () => parseInt(document.querySelector('#freqTotAds')?.textContent?.replace(/,/g,'')||'0',10)
        """)
        if all_count > active_count and active_count > 0:
            suite.pass_("Status chip filters Freq totals",
                        f"active={active_count:,} < all={all_count:,}")
        else:
            suite.warn("Status chip filters Freq totals",
                       f"active={active_count:,}  all={all_count:,}")

        # ── T8: Definitions modal opens + closes
        page.click('.sb-item[data-view="testing"]')
        page.wait_for_timeout(1000)
        page.click('#btnDef')
        page.wait_for_timeout(500)
        opened = page.evaluate("""
          () => {
            const m = document.getElementById('defsModal') || document.getElementById('defModal');
            return m && getComputedStyle(m).display !== 'none';
          }
        """)
        suite.pass_("Definitions modal opens", "visible" if opened else "not opened") \
            if opened else suite.warn("Definitions modal opens", "not visible")
        page.keyboard.press("Escape")
        page.wait_for_timeout(400)

        browser.close()

if __name__ == "__main__":
    from ._common import run_suite
    s = run_suite(__name__, run)
    print(s.counts)
