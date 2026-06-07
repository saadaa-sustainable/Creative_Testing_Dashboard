"""
validate_ae_table_view.py - rigorous cross-check of ae_table_view values
against Meta Ads API ground truth.

For a stratified sample of ads across categories, fetches lifetime insights
from Meta, compares core fields (impressions, reach, spend, frequency,
roas, ftewv/ncp counts, conv_value, ad_status, ad_created), re-derives the
6-category status from raw values, and reports any mismatches.

TOKEN SAFETY:
- META_ACCESS_TOKEN is read from os.environ via load_dotenv. Never echoed,
  never logged, never written to disk. URLs that embed the token are not
  printed in any error message.
- All output (including log file) goes through a sanitizer that scrubs
  anything resembling a Meta token (40+ alphanumeric chars starting with
  'EAA' or 'shpa').
"""
import os, re, sys, json, time, random
import requests, psycopg2
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()
DB_URL       = os.environ["SUPABASE_DB_URL"].strip()
META_TOKEN   = os.environ["META_ACCESS_TOKEN"].strip()
META_API_VER = os.getenv("META_API_VERSION", "v21.0").strip()

# Targets / thresholds from getAdCategory (match the JS defaults)
F1_IMP     = 50_000
F2_ROAS    = 3.2
F3_CPN     = 525
F4_CPFTEWV = 12

LOG_FILE = "validate_ae_table_view.log"
OUT_CSV  = "validate_ae_table_view.csv"

# --- token sanitization -----------------------------------------------------
TOKEN_RE = re.compile(r"(?:EAA[a-zA-Z0-9]{30,}|shpa_[a-zA-Z0-9]{20,})")
def _scrub(s):
    if s is None: return s
    try: return TOKEN_RE.sub("<REDACTED>", str(s))
    except Exception: return "<scrub_err>"

def log(*a):
    msg = " ".join(_scrub(x) for x in a)
    print(msg, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# --- DB helpers -------------------------------------------------------------
def pick_sample():
    """Stratified pick: ~6 ads per category, biased to ads with >10K spend.
    Also returns account_name so we can look up the right custom-conversion IDs."""
    conn = psycopg2.connect(DB_URL); cur = conn.cursor()
    cur.execute("""
        SELECT category, ad_id, ad_name, ad_status, ad_created, account_name,
               impressions, reach, frequency, amount_spent, roas_ma,
               ftewv_count, ncp_count, conv_value
        FROM ae_table_view
        WHERE amount_spent > 10000 AND ad_id IS NOT NULL
    """)
    rows = cur.fetchall()
    conn.close()
    by_cat = defaultdict(list)
    for r in rows: by_cat[r[0]].append(r)
    sample = []
    for cat, rs in by_cat.items():
        random.shuffle(rs)
        sample.extend(rs[:6])
    log(f"[sample] {len(sample)} ads across {len(by_cat)} categories")
    return sample

# Per-account custom-conversion ID lookup (FTEWV / NCP names from primary_sync)
CUSTOM_FTEWV_NAME = os.getenv("CUSTOM_METRIC_FTEWV", "First-time EWV").lower().strip()
CUSTOM_NCP_NAME   = os.getenv("CUSTOM_METRIC_NCP",   "NCP").lower().strip()

ACCOUNTS = {
    os.getenv("ACCOUNT_1_NAME","Raho Saadaa"):           os.getenv("ACCOUNT_1_ID"),
    os.getenv("ACCOUNT_2_NAME","Fourth Ad Account - SD"): os.getenv("ACCOUNT_2_ID"),
    os.getenv("ACCOUNT_3_NAME","Third Ad Account - SD"):  os.getenv("ACCOUNT_3_ID"),
}

_conv_cache = {}
def get_conv_ids(account_name):
    """Returns {'ftewv': id, 'ncp': id} for an account. Cached."""
    if account_name in _conv_cache: return _conv_cache[account_name]
    acct_id = ACCOUNTS.get(account_name)
    if not acct_id:
        _conv_cache[account_name] = {"ftewv":"","ncp":""}; return _conv_cache[account_name]
    try:
        r = requests.get(
            f"https://graph.facebook.com/{META_API_VER}/act_{acct_id}/customconversions",
            params={"fields":"name,id","access_token":META_TOKEN,"limit":200},
            timeout=60,
        )
        if r.status_code != 200:
            _conv_cache[account_name] = {"ftewv":"","ncp":""}; return _conv_cache[account_name]
        m = {c["name"].lower().strip(): c["id"] for c in r.json().get("data", [])}
        out = {"ftewv": m.get(CUSTOM_FTEWV_NAME,""), "ncp": m.get(CUSTOM_NCP_NAME,"")}
        _conv_cache[account_name] = out
        log(f"  [conv] {account_name}: ftewv_id={out['ftewv']}  ncp_id={out['ncp']}")
        return out
    except Exception as e:
        log(f"  [conv] {account_name}: error {type(e).__name__}")
        _conv_cache[account_name] = {"ftewv":"","ncp":""}; return _conv_cache[account_name]

# --- Meta API ---------------------------------------------------------------
def meta_ad_details(ad_id):
    """Returns dict of ad-level info from Meta. Token passed in params (not URL string)
    so it never lands in any error/log message even if exceptions are raised."""
    try:
        # Ad object (status, name, created_time, adset/campaign)
        r = requests.get(
            f"https://graph.facebook.com/{META_API_VER}/{ad_id}",
            params={
                "fields": "id,name,status,effective_status,created_time,adset_id,campaign_id",
                "access_token": META_TOKEN,
            },
            timeout=60,
        )
        if r.status_code != 200:
            return None, f"ad-get HTTP {r.status_code}: {_scrub(r.text[:200])}"
        ad_info = r.json()

        # Lifetime insights — includes video_thruplay_watched_actions as a separate field
        r2 = requests.get(
            f"https://graph.facebook.com/{META_API_VER}/{ad_id}/insights",
            params={
                "fields": "impressions,reach,frequency,spend,purchase_roas,actions,action_values,video_thruplay_watched_actions",
                "date_preset": "maximum",
                "access_token": META_TOKEN,
            },
            timeout=60,
        )
        if r2.status_code != 200:
            return None, f"insights HTTP {r2.status_code}: {_scrub(r2.text[:200])}"
        data = (r2.json().get("data") or [{}])[0]

        return {"ad": ad_info, "insights": data}, None
    except requests.RequestException as e:
        return None, f"network: {_scrub(type(e).__name__)}"

def _action_value(actions, key):
    if not actions: return 0.0
    for a in actions:
        if a.get("action_type") == key:
            try: return float(a.get("value") or 0)
            except: return 0.0
    return 0.0

def parse_insights(data, conv_ids):
    """conv_ids = {'ftewv': '...', 'ncp': '...'} — custom conversion IDs for this account."""
    impr = int(data.get("impressions") or 0)
    reach = int(data.get("reach") or 0)
    freq = float(data.get("frequency") or 0)
    spend = float(data.get("spend") or 0)
    # ROAS — list of dicts with action_type
    roas = 0.0
    proas = data.get("purchase_roas") or []
    for x in proas:
        if x.get("action_type") in ("omni_purchase","purchase","website_purchase"):
            try: roas = max(roas, float(x.get("value") or 0))
            except: pass
    actions = data.get("actions") or []
    avals   = data.get("action_values") or []
    # FTEWV / NCP — custom conversion action types (offsite_conversion.custom.<id>)
    ftewv = _action_value(actions, f"offsite_conversion.custom.{conv_ids['ftewv']}") if conv_ids.get("ftewv") else 0.0
    ncp   = _action_value(actions, f"offsite_conversion.custom.{conv_ids['ncp']}")   if conv_ids.get("ncp")   else 0.0
    # Conversion value — total purchase value
    conv_v = _action_value(avals, "omni_purchase") or _action_value(avals, "purchase")
    # video_thruplay_watched_actions is a separate top-level field, list of {action_type,value}
    tp_list = data.get("video_thruplay_watched_actions") or []
    thruplay = 0.0
    if tp_list:
        try: thruplay = float(tp_list[0].get("value") or 0)
        except: pass
    return dict(impressions=impr, reach=reach, frequency=freq, amount_spent=spend,
                roas=roas, ncp_count=ncp, ftewv_count=ftewv, conv_value=conv_v,
                thruplay=thruplay)

# --- Category derivation (mirrors getAdCategory) ----------------------------
def derive_category(impr, roas, cpn, cp_ftewv):
    f1 = impr >= F1_IMP
    f2 = roas >= F2_ROAS
    f3 = (cpn > 0) and (cpn <= F3_CPN)
    f4 = (cp_ftewv > 0) and (cp_ftewv <= F4_CPFTEWV)
    if f1 and f2 and f3 and f4: return "Incremental Winner"
    if f1 and f2 and f3:        return "Winner"
    if f1 and f4:               return "Priority"
    if f1:                      return "Analyze 1"
    if f2:                      return "Analyze 2"
    return "Discarded"

# --- Diff helpers -----------------------------------------------------------
def _pct_diff(a, b):
    if (a or 0) == 0 and (b or 0) == 0: return 0.0
    base = max(abs(a or 0), abs(b or 0), 1)
    return 100.0 * abs((a or 0) - (b or 0)) / base

# --- main -------------------------------------------------------------------
def main():
    open(LOG_FILE, "w").close()
    log(f"=== ae_table_view validation against Meta API ===")
    log(f"using Meta API {META_API_VER}; sample stratified by category (~30 ads)")

    sample = pick_sample()
    results = []
    discrepancies = defaultdict(int)
    err_count = 0

    log(f"\n[fetching meta for {len(sample)} ads]")
    for i, row in enumerate(sample, 1):
        (db_cat, ad_id, ad_name, db_status, db_created, db_account,
         db_impr, db_reach, db_freq, db_spend, db_roas,
         db_ftewv, db_ncp, db_conv) = row

        conv_ids = get_conv_ids(db_account)
        info, err = meta_ad_details(ad_id)
        if not info:
            log(f"  [{i:>2}/{len(sample)}] {ad_id}  ad_name=\"{(ad_name or '')[:45]}\"  -> ERR {err}")
            err_count += 1; continue
        meta_ins = parse_insights(info["insights"], conv_ids)
        meta_ad  = info["ad"]

        # Per-field check (pct diff)
        diffs = {}
        for key, dbv, mv in [
            ("impressions",  db_impr,  meta_ins["impressions"]),
            ("reach",        db_reach, meta_ins["reach"]),
            ("frequency",    db_freq,  meta_ins["frequency"]),
            ("amount_spent", db_spend, meta_ins["amount_spent"]),
            ("roas",         db_roas,  meta_ins["roas"]),
            ("ncp_count",    db_ncp,   meta_ins["ncp_count"]),
            ("ftewv_count",  db_ftewv, meta_ins["ftewv_count"]),
            ("conv_value",   db_conv,  meta_ins["conv_value"]),
        ]:
            p = _pct_diff(float(dbv or 0), float(mv or 0))
            diffs[key] = (float(dbv or 0), float(mv or 0), p)
            if p > 2.0:   # >2% off counts as discrepancy
                discrepancies[key] += 1

        # Status check
        meta_status  = meta_ad.get("status") or meta_ad.get("effective_status") or ""
        status_match = (str(db_status).upper() == meta_status.upper())
        if not status_match: discrepancies["ad_status"] += 1

        # Category re-derivation: from META values
        cp_ftewv = (meta_ins["amount_spent"] / meta_ins["ftewv_count"]) if meta_ins["ftewv_count"] > 0 else 0
        cpn      = (meta_ins["amount_spent"] / meta_ins["ncp_count"])  if meta_ins["ncp_count"]   > 0 else 0
        derived_cat = derive_category(meta_ins["impressions"], meta_ins["roas"], cpn, cp_ftewv)
        cat_match = (db_cat == derived_cat)
        if not cat_match: discrepancies["category"] += 1

        log(f"  [{i:>2}/{len(sample)}] {ad_id}  ad_name=\"{(ad_name or '')[:40]}\"  cat={db_cat}/{derived_cat}{' MISMATCH' if not cat_match else ''}")
        worst = max(diffs.items(), key=lambda x: x[1][2])
        if worst[1][2] > 2.0:
            wk, (a, b, p) = worst
            log(f"        worst gap: {wk}  db={a:,.2f}  meta={b:,.2f}  diff={p:.2f}%")

        results.append({
            "ad_id": ad_id, "ad_name": ad_name,
            "db_status": db_status, "meta_status": meta_status, "status_match": status_match,
            "db_category": db_cat, "derived_category": derived_cat, "cat_match": cat_match,
            **{f"{k}_db": v[0] for k,v in diffs.items()},
            **{f"{k}_meta": v[1] for k,v in diffs.items()},
            **{f"{k}_pct_diff": round(v[2],3) for k,v in diffs.items()},
        })
        time.sleep(0.3)

    # Summary
    log("\n========= SUMMARY =========")
    log(f"Ads sampled         : {len(sample)}")
    log(f"Meta fetch errors   : {err_count}")
    log(f"\nDiscrepancies (db vs Meta) >2% pct_diff or status/cat mismatch:")
    for k, n in sorted(discrepancies.items(), key=lambda x:-x[1]):
        log(f"  {k:<14}: {n}")
    if not discrepancies:
        log("  (none) — every sampled ad matches Meta within 2%")

    # CSV
    import csv
    if results:
        keys = list(results[0].keys())
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
            for r in results: w.writerow(r)
        log(f"\n[csv] {OUT_CSV}")

if __name__ == "__main__":
    main()
