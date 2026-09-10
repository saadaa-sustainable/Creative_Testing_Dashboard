"""
list_custom_conversions.py — what custom conversions does Meta actually have?

FTEWV and NCP are resolved by NAME: primary_sync.py asks Meta for the
account's custom conversions and looks up CUSTOM_METRIC_FTEWV /
CUSTOM_METRIC_NCP in the result. When that lookup misses, the sync stores no
value and the dashboard's F3/F4 filters go dead.

This prints, per account, every custom conversion Meta returns and whether
the configured names match — so the two secrets can be set to strings that
actually exist. Names are safe to print; no token or ID secret is echoed.

Usage:
    python list_custom_conversions.py
"""

import os
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception:
    pass

ACCESS_TOKEN = (os.getenv("META_ACCESS_TOKEN") or "").strip()
API_VERSION  = (os.getenv("META_API_VERSION") or "v21.0").strip()
BASE_URL     = f"https://graph.facebook.com/{API_VERSION}"

# Same resolution as primary_sync.py, including the empty-string fallback.
WANT_FTEWV = (os.getenv("CUSTOM_METRIC_FTEWV") or "").strip() or "First-time EWV"
WANT_NCP   = (os.getenv("CUSTOM_METRIC_NCP")   or "").strip() or "NCP"

ACCOUNTS = []
for i in (1, 2, 3):
    acct_id = (os.getenv(f"ACCOUNT_{i}_ID") or "").strip()
    if acct_id:
        ACCOUNTS.append({
            "id": acct_id,
            "name": (os.getenv(f"ACCOUNT_{i}_NAME") or f"Account {i}").strip(),
        })


def main() -> int:
    if not ACCESS_TOKEN:
        print("META_ACCESS_TOKEN is not set — cannot query Meta.")
        return 1
    if not ACCOUNTS:
        print("No ACCOUNT_n_ID values set — nothing to query.")
        return 1

    print(f"Looking for FTEWV name: {WANT_FTEWV!r}")
    print(f"Looking for NCP   name: {WANT_NCP!r}")
    print(f"(env CUSTOM_METRIC_FTEWV is "
          f"{'EMPTY/UNSET — using the built-in default' if not (os.getenv('CUSTOM_METRIC_FTEWV') or '').strip() else 'set'}, "
          f"CUSTOM_METRIC_NCP is "
          f"{'EMPTY/UNSET — using the built-in default' if not (os.getenv('CUSTOM_METRIC_NCP') or '').strip() else 'set'})")

    exit_code = 0
    for acct in ACCOUNTS:
        print(f"\n=== {acct['name']} (act_{acct['id']}) ===")
        try:
            r = requests.get(
                f"{BASE_URL}/act_{acct['id']}/customconversions",
                params={"fields": "name,id", "access_token": ACCESS_TOKEN,
                        "limit": 200},
                timeout=60,
            )
        except Exception as e:  # noqa: BLE001
            print(f"  request failed: {type(e).__name__}: {e}")
            exit_code = 1
            continue

        if r.status_code != 200:
            # A 403/190 here is its own answer: the token cannot read custom
            # conversions, which looks identical to "conversion not found".
            print(f"  HTTP {r.status_code}: {r.text[:300]}")
            exit_code = 1
            continue

        rows = r.json().get("data", [])
        if not rows:
            print("  (none returned)")
            exit_code = 1
            continue

        names = {c["name"].lower().strip(): c["name"] for c in rows}
        for c in sorted(rows, key=lambda c: c["name"].lower()):
            print(f"  {c['name']!r}  id={c['id']}")

        for label, want in (("FTEWV", WANT_FTEWV), ("NCP", WANT_NCP)):
            hit = names.get(want.lower().strip())
            if hit:
                print(f"  -> {label}: MATCHES {hit!r}")
            else:
                print(f"  -> {label}: NO MATCH for {want!r} — this is why it stores nothing")
                exit_code = 1

    print("\nSet CUSTOM_METRIC_FTEWV / CUSTOM_METRIC_NCP (repo secrets) to names "
          "listed above, exactly as spelled.")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
