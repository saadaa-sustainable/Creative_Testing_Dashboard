"""
Verify Google Ads API connectivity using:
  * developer token   → from backend/.env  (GOOGLE_ADS_DEVELOPER_TOKEN)
  * OAuth credentials → gcloud Application Default Credentials
                        (~/.config/gcloud/application_default_credentials.json
                         on POSIX, %APPDATA%\\gcloud\\... on Windows)

We DON'T ask the user for client_id/client_secret/refresh_token — the
gcloud ADC file already contains a working user-consented OAuth grant,
so we read that directly and pass it to the Google Ads client. If the
ADC hasn't been consented for the adwords scope yet, this script prints
the exact command to run.

Read-only. No DB writes.
"""
import os, sys, json, pathlib
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DEV_TOKEN = (os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip()
LOGIN_CID = (os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or "").strip()

def _redact(v):
    return (v[:4] + "…" + v[-4:]) if v and len(v) > 10 else ("<UNSET>" if not v else "<short>")

def _adc_path():
    if os.name == "nt":
        appdata = os.environ.get("APPDATA")
        return pathlib.Path(appdata) / "gcloud" / "application_default_credentials.json" if appdata else None
    return pathlib.Path.home() / ".config" / "gcloud" / "application_default_credentials.json"

def _load_adc():
    p = _adc_path()
    if not p or not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

print("[credential audit]")
print(f"  GOOGLE_ADS_DEVELOPER_TOKEN   : {'SET' if DEV_TOKEN else 'MISSING'}  {_redact(DEV_TOKEN)}")
print(f"  GOOGLE_ADS_LOGIN_CUSTOMER_ID : {LOGIN_CID if LOGIN_CID else '<optional / auto-detect>'}")

if not DEV_TOKEN:
    print()
    print("[!] Add GOOGLE_ADS_DEVELOPER_TOKEN=<your token> to backend/.env")
    sys.exit(1)

adc = _load_adc()
if adc is None:
    print(f"  ADC file                     : NOT FOUND at {_adc_path()}")
    print()
    print("[!] gcloud ADC missing. Run this once and re-run the probe:")
    print("    gcloud auth application-default login --scopes=https://www.googleapis.com/auth/adwords,openid,https://www.googleapis.com/auth/userinfo.email")
    sys.exit(1)

# ADC must be user-consented (has refresh_token). Service-account ADC won't work here.
if not adc.get("refresh_token"):
    print(f"  ADC file                     : found, but no refresh_token")
    print()
    print("[!] ADC is a service account — Google Ads needs a user OAuth grant.")
    print("    Run:  gcloud auth application-default login --scopes=https://www.googleapis.com/auth/adwords,openid,https://www.googleapis.com/auth/userinfo.email")
    sys.exit(1)

print(f"  ADC file                     : loaded  ({_adc_path()})")
print(f"  ADC client_id                : {_redact(adc.get('client_id',''))}")
print(f"  ADC refresh_token            : {_redact(adc.get('refresh_token',''))}")
print()

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError:
    print("[!] google-ads not installed. Run: pip install google-ads")
    sys.exit(1)

# Build the client config from ADC + dev token. Google Ads client accepts
# {client_id, client_secret, refresh_token} in the same shape ADC uses.
cfg = {
    "developer_token": DEV_TOKEN,
    "client_id":       adc.get("client_id"),
    "client_secret":   adc.get("client_secret"),
    "refresh_token":   adc.get("refresh_token"),
    "use_proto_plus":  True,
}
if LOGIN_CID:
    cfg["login_customer_id"] = LOGIN_CID

print("[*] Connecting to Google Ads API …")
try:
    client = GoogleAdsClient.load_from_dict(cfg)
    svc = client.get_service("CustomerService")
    resource_names = svc.list_accessible_customers().resource_names
except GoogleAdsException as e:
    print(f"[!] Google Ads API error: {e.error.code().name}")
    for err in e.failure.errors:
        print(f"    {err.message}")
    print()
    print("Common causes:")
    print("  * developer token still in Test Access mode (works only against test accounts)")
    print("  * user account not linked to any Google Ads customer")
    print("  * ADC scope missing — re-run: gcloud auth application-default login \\")
    print("      --scopes=https://www.googleapis.com/auth/adwords,openid,https://www.googleapis.com/auth/userinfo.email")
    sys.exit(1)
except Exception as e:
    print(f"[!] Connection failed: {type(e).__name__}: {str(e)[:220]}")
    sys.exit(1)

print(f"[ok] {len(resource_names)} accessible customer(s):")
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
    except GoogleAdsException as e:
        first_err = next(iter(e.failure.errors), None)
        msg = first_err.message if first_err else str(e)
        print(f"    {cid:>12}  <no read access: {msg[:100]}>")
    except Exception as e:
        print(f"    {cid:>12}  <cannot introspect: {type(e).__name__}>")

print()
print("[done] Credentials look good. Next: fetch_google_ads_daily.py --dry-run")
