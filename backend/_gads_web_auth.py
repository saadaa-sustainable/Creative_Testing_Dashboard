"""Manual OAuth2 flow using a Web-app credential JSON (since gcloud ADC
only accepts Desktop apps).

Uses google_auth_oauthlib.flow.Flow with an explicit http://localhost
redirect and a tiny local HTTP server to catch the auth-code redirect.
Writes the resulting credentials into gcloud's ADC file location so
fetch_google_ads_daily.py picks them up.

The script does NOT read/print the JSON's contents — it hands the path
to google-auth-oauthlib, which internally validates + uses the file.

USAGE:
    python _gads_web_auth.py --json "C:\\path\\to\\client_secret_XXX.json"

Requirement: the Web credential's OAuth consent screen must have
    http://localhost:8090/
in its Authorized redirect URIs.  If missing, Google will show an
'Error 400: redirect_uri_mismatch' page during auth.  Add it in
Google Cloud Console → APIs & Services → Credentials → (your Web
client) → Add URI: http://localhost:8090/ → Save.
"""
from __future__ import annotations
import os, sys, pathlib, argparse, webbrowser, json, time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, urlencode

try:
    from google_auth_oauthlib.flow import Flow
    from google.auth.transport.requests import Request
except ImportError:
    sys.exit("pip install google-auth-oauthlib  (missing from venv)")

SCOPES = [
    'https://www.googleapis.com/auth/adwords',
    'https://www.googleapis.com/auth/cloud-platform',
    'openid',
    'https://www.googleapis.com/auth/userinfo.email',
]
PORT       = 8090
REDIR_URI  = f'http://localhost:{PORT}/'
CAUGHT     = {}   # populated by the request handler

class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *a, **kw): pass    # silence stderr
    def do_GET(self):
        qs = parse_qs(urlparse(self.path).query)
        CAUGHT.update({k: v[0] for k, v in qs.items()})
        self.send_response(200); self.send_header('Content-Type','text/html'); self.end_headers()
        if 'code' in CAUGHT:
            self.wfile.write(b"<html><body><h2>Authorised.</h2>"
                             b"You can close this tab and return to the terminal.</body></html>")
        else:
            self.wfile.write(b"<html><body><h2>No auth code received.</h2>"
                             b"Check the terminal.</body></html>")

def _adc_path():
    """Where gcloud's ADC file lives — same file fetch_google_ads_daily.py reads."""
    if os.name == 'nt':
        return pathlib.Path(os.environ['APPDATA']) / 'gcloud' / 'application_default_credentials.json'
    return pathlib.Path.home() / '.config' / 'gcloud' / 'application_default_credentials.json'

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--json', required=True,
                    help='Path to the Web-app OAuth client_secret_*.json (not read here, passed to google-auth)')
    args = ap.parse_args()

    if not pathlib.Path(args.json).is_file():
        sys.exit(f"JSON not found: {args.json}")

    flow = Flow.from_client_secrets_file(args.json, scopes=SCOPES, redirect_uri=REDIR_URI)
    auth_url, state = flow.authorization_url(
        access_type='offline', include_granted_scopes='true', prompt='consent')

    print(f"[*] starting local server on {REDIR_URI}")
    server = HTTPServer(('localhost', PORT), _Handler)

    print("[*] opening browser for consent…")
    print(f"    if the browser doesn't open, paste this into it:\n    {auth_url}\n")
    try: webbrowser.open(auth_url, new=2)
    except Exception: pass

    print("[*] waiting for redirect (timeout 180 s)…")
    server.timeout = 180
    while 'code' not in CAUGHT and 'error' not in CAUGHT:
        server.handle_request()
    server.server_close()

    if 'error' in CAUGHT:
        sys.exit(f"OAuth error: {CAUGHT.get('error')} — {CAUGHT.get('error_description','')}")
    if 'code' not in CAUGHT:
        sys.exit("Timed out waiting for redirect.")

    print("[*] exchanging code for refresh token…")
    flow.fetch_token(code=CAUGHT['code'])
    creds = flow.credentials

    adc = _adc_path()
    adc.parent.mkdir(parents=True, exist_ok=True)

    # gcloud ADC format the google-ads SDK understands (authorized_user)
    payload = {
        'client_id':      creds.client_id,
        'client_secret':  creds.client_secret,
        'refresh_token':  creds.refresh_token,
        'type':           'authorized_user',
    }
    if not creds.refresh_token:
        sys.exit("No refresh_token in response — check that prompt=consent worked. "
                 "Consent screen may need to be reset.")
    adc.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    print(f"[ok] wrote credentials to  {adc}")
    print(f"[ok] scopes granted: {' '.join(creds.scopes or [])}")
    print("\nNext step: run  python _probe_google_ads_connect.py  to confirm the adwords scope reaches the API.")

if __name__ == '__main__':
    main()
