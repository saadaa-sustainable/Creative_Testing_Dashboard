"""
Verifies the Google Ads MCC credentials in .env can reach the API.

Prints the list of child (client) accounts under the MCC. Writes
nothing to any database. Run this FIRST before wiring the daily
fetcher into the pipeline.

Requires: pip install google-ads
"""
import os, sys
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DEV_TOKEN   = (os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip()
CLIENT_ID   = (os.environ.get("GOOGLE_ADS_CLIENT_ID") or "").strip()
CLIENT_SEC  = (os.environ.get("GOOGLE_ADS_CLIENT_SECRET") or "").strip()
REFRESH_TOK = (os.environ.get("GOOGLE_ADS_REFRESH_TOKEN") or "").strip()
MCC_ID      = (os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or "").strip()

def _redact(v):  # never print raw secrets
    return (v[:4] + "…" + v[-4:]) if v and len(v) > 10 else ("<UNSET>" if not v else "<short>")

print("[credential audit]")
print(f"  GOOGLE_ADS_DEVELOPER_TOKEN     : {'SET' if DEV_TOKEN else 'MISSING'}  {_redact(DEV_TOKEN)}")
print(f"  GOOGLE_ADS_CLIENT_ID           : {'SET' if CLIENT_ID else 'MISSING'}  {_redact(CLIENT_ID)}")
print(f"  GOOGLE_ADS_CLIENT_SECRET       : {'SET' if CLIENT_SEC else 'MISSING'}  {_redact(CLIENT_SEC)}")
print(f"  GOOGLE_ADS_REFRESH_TOKEN       : {'SET' if REFRESH_TOK else 'MISSING'}  {_redact(REFRESH_TOK)}")
print(f"  GOOGLE_ADS_LOGIN_CUSTOMER_ID   : {'SET' if MCC_ID else 'MISSING'}  {MCC_ID or '<UNSET>'}")

missing = [k for k, v in [
    ("DEVELOPER_TOKEN", DEV_TOKEN),
    ("CLIENT_ID", CLIENT_ID),
    ("CLIENT_SECRET", CLIENT_SEC),
    ("REFRESH_TOKEN", REFRESH_TOK),
    ("LOGIN_CUSTOMER_ID", MCC_ID),
] if not v]
if missing:
    print()
    print("[!] Cannot connect — set the missing vars first:")
    for m in missing: print(f"    GOOGLE_ADS_{m}")
    print()
    print("See backend/GOOGLE_ADS_SETUP.md for what each value is and where to get it.")
    sys.exit(1)

try:
    from google.ads.googleads.client import GoogleAdsClient
except ImportError:
    print()
    print("[!] google-ads Python client not installed. Install with:")
    print("    pip install google-ads")
    sys.exit(1)

config = {
    "developer_token":     DEV_TOKEN,
    "client_id":           CLIENT_ID,
    "client_secret":       CLIENT_SEC,
    "refresh_token":       REFRESH_TOK,
    "login_customer_id":   MCC_ID,
    "use_proto_plus":      True,
}

print()
print("[*] Connecting to Google Ads API …")
try:
    client = GoogleAdsClient.load_from_dict(config)
    svc = client.get_service("CustomerService")
    resource_names = svc.list_accessible_customers().resource_names
except Exception as e:
    print(f"[!] Connection failed: {type(e).__name__}: {str(e)[:220]}")
    sys.exit(1)

print(f"[ok] MCC {MCC_ID} has {len(resource_names)} accessible customer(s):")

# For each accessible customer, resolve to a friendly (id, descriptive_name).
ga = client.get_service("GoogleAdsService")
for rn in resource_names:
    cid = rn.split("/")[-1]
    try:
        rows = ga.search(customer_id=cid, query="""
            SELECT customer.id, customer.descriptive_name, customer.manager,
                   customer.currency_code, customer.time_zone
            FROM customer LIMIT 1
        """)
        for row in rows:
            c = row.customer
            tag = " [MCC]" if c.manager else ""
            print(f"    {c.id:>12}  {c.descriptive_name}{tag}"
                  f"   ccy={c.currency_code}  tz={c.time_zone}")
    except Exception as e:
        print(f"    {cid:>12}  <cannot introspect: {type(e).__name__}>")

print()
print("[done] Credentials look good. Next: run fetch_google_ads_daily.py --dry-run")
