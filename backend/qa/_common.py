"""
_common.py — shared helpers for the QA suite.

  * Credential-safe loading (dotenv, no printing).
  * Regex-scrubs any JWT / access-token from anything that could leak.
  * Small assertion helpers that record PASS / FAIL / WARN per check.

Every qa/test_*.py imports from here so results roll up uniformly.
"""
from __future__ import annotations
import os, re, sys, json, time, io, traceback
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timezone
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
# Load .env from backend/ regardless of the shell's cwd.
_ENV = Path(__file__).parent.parent / ".env"
load_dotenv(_ENV if _ENV.exists() else None, override=True)

# ─── credentials ─────────────────────────────────────────────────────
DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
META_TOK = (os.environ.get("META_ACCESS_TOKEN") or "").strip()
if not DB_URL:
    raise SystemExit("Missing SUPABASE_DB_URL in .env")

TOK_RE = re.compile(r"(?:eyJ[\w\-.]{40,}|EAA[A-Za-z0-9]{30,}|IGQ[\w\-]{20,})")
def scrub(s) -> str:
    s = TOK_RE.sub("<REDACTED>", str(s or ""))
    return re.sub(r"(access_token|apikey|token)=[^&\s\"']+", r"\1=<REDACTED>",
                  s, flags=re.I)

# ─── Meta accounts ───────────────────────────────────────────────────
META_ACCOUNTS = [
    ("Raho Saadaa",            "1136644150469466"),
    ("Fourth Ad Account - SD", "1349767139294217"),
    ("Third Ad Account - SD",  "264868699479122"),
]
META_API_VER = os.environ.get("META_API_VERSION", "v22.0")

# ─── result recording ────────────────────────────────────────────────
@dataclass
class Check:
    name: str
    status: str         # 'PASS' | 'FAIL' | 'WARN' | 'SKIP'
    detail: str = ""
    expected: str = ""
    actual: str = ""

@dataclass
class Suite:
    module: str
    checks: list = field(default_factory=list)
    started: float = 0.0
    ended:   float = 0.0

    def record(self, name, status, detail="", expected=None, actual=None):
        c = Check(name=name, status=status, detail=detail,
                  expected="" if expected is None else str(expected),
                  actual  ="" if actual   is None else str(actual))
        self.checks.append(c)
        marker = {"PASS":"✓","FAIL":"✗","WARN":"!","SKIP":"·"}.get(status, "?")
        print(f"    [{marker}] {name:52s} {detail}"[:180])
        return c

    def pass_(self, name, detail="", **kw): return self.record(name, "PASS", detail, **kw)
    def fail (self, name, detail="", expected=None, actual=None, **kw):
        return self.record(name, "FAIL", detail, expected=expected, actual=actual)
    def warn (self, name, detail="", **kw): return self.record(name, "WARN", detail)
    def skip (self, name, detail="", **kw): return self.record(name, "SKIP", detail)

    @property
    def counts(self):
        d = {"PASS":0,"FAIL":0,"WARN":0,"SKIP":0}
        for c in self.checks: d[c.status] = d.get(c.status,0) + 1
        return d

    def to_dict(self):
        return {
            "module":  self.module,
            "started": self.started,
            "ended":   self.ended,
            "counts":  self.counts,
            "checks":  [c.__dict__ for c in self.checks],
        }

def approx_equal(a, b, tol=0.005) -> bool:
    """|a-b| / max(|a|,|b|,1) <= tol"""
    a, b = float(a or 0), float(b or 0)
    denom = max(abs(a), abs(b), 1)
    return abs(a - b) / denom <= tol

def within_pct(a, b, pct=0.5) -> bool:
    return approx_equal(a, b, tol=pct/100)

def run_suite(module_name: str, run_fn):
    """Wrap a test module's run(suite) — catch exceptions, time it."""
    suite = Suite(module=module_name, started=time.time())
    print(f"\n── {module_name} ──")
    try:
        run_fn(suite)
    except Exception:
        buf = io.StringIO(); traceback.print_exc(file=buf)
        suite.fail(f"{module_name} raised uncaught exception",
                   scrub(buf.getvalue())[:300])
    suite.ended = time.time()
    return suite

RESULTS_JSON = Path(__file__).parent / "_results.json"
REPORT_MD    = Path(__file__).parent / "REPORT.md"
