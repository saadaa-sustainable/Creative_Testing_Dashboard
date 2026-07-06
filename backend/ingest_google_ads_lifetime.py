"""
ingest_google_ads_lifetime.py — one-shot ingest of the consolidated Google
Ads export that Saadaa downloads manually from Google Ads Report Editor.

Input  : c:\\Users\\Saadaa\\Downloads\\Ads performance.csv
         (UTF-16 LE, tab-separated, 2 title rows before the header)
Outputs:
  1. backend/google_ads_lifetime.csv          — clean UTF-8 mirror
  2. Supabase table public.google_ads_lifetime_summary  (upsert on ad_name+campaign)

Note: the current Report Editor view does NOT include Campaign ID / Ad ID,
so we cannot join to shopify_ad_attribution.utm_campaign (11-digit id).
Re-exporting with those two ID columns added is what unlocks the join.
The ingest is defensive — if Campaign ID / Ad ID columns are present on
a future export, they are picked up automatically.
"""
from __future__ import annotations
import os, sys, csv, re, pathlib, psycopg2
from datetime import date
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DEFAULT_SRC = pathlib.Path(r"C:\Users\Saadaa\Downloads\Ads performance.csv")
SRC = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SRC
OUT = pathlib.Path(__file__).parent / "google_ads_lifetime.csv"

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: raise SystemExit("Missing SUPABASE_DB_URL in .env")

def num(v: str):
    v = (v or "").replace('"', "").replace(",", "").strip()
    if v in ("", "--", "-"): return None
    try:
        if "." in v: return float(v)
        return int(v)
    except Exception: return None

def pct(v: str):
    v = (v or "").replace('"', "").replace(",", "").replace("%", "").strip()
    if v in ("", "--", "-"): return None
    try: return float(v)
    except Exception: return None

def parse_report(path: pathlib.Path):
    raw = path.read_bytes()
    # Google exports as UTF-16 LE with BOM
    for enc in ("utf-16", "utf-16-le", "utf-8-sig", "utf-8"):
        try:
            txt = raw.decode(enc); break
        except UnicodeError: continue
    else:
        raise SystemExit("could not decode CSV — unknown encoding")
    txt = txt.replace("\r", "")
    lines = txt.split("\n")

    # Title row + date range row + header row
    title = lines[0].split("\t")[0].strip()
    period = lines[1].split("\t")[0].strip()
    hdr = lines[2].split("\t")

    # Parse the "1 January 2025 - 6 July 2026" period
    m = re.match(r"(\d{1,2}\s+\w+\s+\d{4})\s*-\s*(\d{1,2}\s+\w+\s+\d{4})", period)
    date_from = date_to = None
    if m:
        from datetime import datetime
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                date_from = datetime.strptime(m.group(1), fmt).date()
                date_to   = datetime.strptime(m.group(2), fmt).date()
                break
            except ValueError: continue

    def col(name):  # index or -1
        try: return hdr.index(name)
        except ValueError: return -1

    idx = {k: col(k) for k in (
        "Ad state","Ad type","Ad name","Campaign","Ad group","Campaign type",
        "Campaign subtype","Campaign ID","Ad group ID","Ad ID","Final URL",
        "Clicks","Impr.","CTR","Avg. CPC","Cost","Currency code",
        "Conversions","View-through conv.","Cost / conv.","Conv. rate")}

    rows = []
    for line in lines[3:]:
        if not line.strip(): continue
        c = line.split("\t")
        if len(c) < len(hdr): continue
        def g(k): return c[idx[k]].strip() if idx[k] >= 0 else ""
        rows.append({
            "ad_state":         g("Ad state"),
            "ad_type":          g("Ad type"),
            "ad_name":          g("Ad name"),
            "campaign":         g("Campaign"),
            "ad_group":         g("Ad group"),
            "campaign_type":    g("Campaign type"),
            "campaign_subtype": g("Campaign subtype"),
            "campaign_id":      g("Campaign ID"),
            "ad_group_id":      g("Ad group ID"),
            "ad_id":            g("Ad ID"),
            "final_url":        g("Final URL"),
            "clicks":           num(g("Clicks")),
            "impressions":      num(g("Impr.")),
            "ctr":              pct(g("CTR")),
            "avg_cpc":          num(g("Avg. CPC")),
            "cost":             num(g("Cost")),
            "currency":         g("Currency code") or "INR",
            "conversions":      num(g("Conversions")),
            "vtc":              num(g("View-through conv.")),
            "cost_per_conv":    num(g("Cost / conv.")),
            "conv_rate":        pct(g("Conv. rate")),
        })
    return title, period, date_from, date_to, rows

# ── parse ──────────────────────────────────────────────────────────
title, period, dfrom, dto, rows = parse_report(SRC)
print(f"[*] source     : {SRC}")
print(f"[*] title      : {title}")
print(f"[*] period     : {period}  ({dfrom} → {dto})")
print(f"[*] data rows  : {len(rows):,}")

has_ids = any(r["campaign_id"] for r in rows)
print(f"[*] campaign_id column present: {has_ids}")
if not has_ids:
    print("    ⚠ current export lacks Campaign ID → cannot join to shopify_ad_attribution")

# ── clean UTF-8 CSV mirror ─────────────────────────────────────────
with open(OUT, "w", encoding="utf-8-sig", newline="") as f:
    w = csv.writer(f)
    w.writerow(["ad_state","ad_type","ad_name","campaign","ad_group",
                "campaign_type","campaign_subtype","campaign_id","ad_group_id","ad_id",
                "final_url","clicks","impressions","ctr","avg_cpc","cost","currency",
                "conversions","vtc","cost_per_conv","conv_rate",
                "date_from","date_to"])
    for r in rows:
        w.writerow([r["ad_state"], r["ad_type"], r["ad_name"], r["campaign"],
                    r["ad_group"], r["campaign_type"], r["campaign_subtype"],
                    r["campaign_id"], r["ad_group_id"], r["ad_id"],
                    r["final_url"], r["clicks"], r["impressions"], r["ctr"],
                    r["avg_cpc"], r["cost"], r["currency"], r["conversions"],
                    r["vtc"], r["cost_per_conv"], r["conv_rate"],
                    dfrom.isoformat() if dfrom else "",
                    dto.isoformat() if dto else ""])
print(f"[ok] wrote     : {OUT}  ({OUT.stat().st_size:,} bytes)")

# ── ensure table + upsert ──────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS public.google_ads_lifetime_summary (
    row_key           text PRIMARY KEY,   -- md5(campaign || ad_group || ad_name)
    ad_state          text,
    ad_type           text,
    ad_name           text,
    campaign          text,
    ad_group          text,
    campaign_type     text,
    campaign_subtype  text,
    campaign_id       text,
    ad_group_id       text,
    ad_id             text,
    final_url         text,
    clicks            bigint,
    impressions       bigint,
    ctr               numeric,
    avg_cpc           numeric,
    cost              numeric,
    currency          text,
    conversions       numeric,
    vtc               numeric,
    cost_per_conv     numeric,
    conv_rate         numeric,
    date_from         date,
    date_to           date,
    synced_at         timestamptz DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_gals_campaign_id ON public.google_ads_lifetime_summary(campaign_id);
CREATE INDEX IF NOT EXISTS ix_gals_ad_id       ON public.google_ads_lifetime_summary(ad_id);
"""

UPSERT = """
INSERT INTO public.google_ads_lifetime_summary (
    row_key, ad_state, ad_type, ad_name, campaign, ad_group,
    campaign_type, campaign_subtype, campaign_id, ad_group_id, ad_id,
    final_url, clicks, impressions, ctr, avg_cpc, cost, currency,
    conversions, vtc, cost_per_conv, conv_rate, date_from, date_to, synced_at
) VALUES (
    md5(%s || '|' || %s || '|' || %s),
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW()
)
ON CONFLICT (row_key) DO UPDATE SET
    ad_state         = EXCLUDED.ad_state,
    ad_type          = EXCLUDED.ad_type,
    ad_name          = EXCLUDED.ad_name,
    campaign         = EXCLUDED.campaign,
    ad_group         = EXCLUDED.ad_group,
    campaign_type    = EXCLUDED.campaign_type,
    campaign_subtype = EXCLUDED.campaign_subtype,
    campaign_id      = EXCLUDED.campaign_id,
    ad_group_id      = EXCLUDED.ad_group_id,
    ad_id            = EXCLUDED.ad_id,
    final_url        = EXCLUDED.final_url,
    clicks           = EXCLUDED.clicks,
    impressions      = EXCLUDED.impressions,
    ctr              = EXCLUDED.ctr,
    avg_cpc          = EXCLUDED.avg_cpc,
    cost             = EXCLUDED.cost,
    currency         = EXCLUDED.currency,
    conversions      = EXCLUDED.conversions,
    vtc              = EXCLUDED.vtc,
    cost_per_conv    = EXCLUDED.cost_per_conv,
    conv_rate        = EXCLUDED.conv_rate,
    date_from        = EXCLUDED.date_from,
    date_to          = EXCLUDED.date_to,
    synced_at        = NOW()
"""

with psycopg2.connect(DB_URL) as conn:
    with conn.cursor() as cur:
        cur.execute(DDL)
        # Lifetime summary is always a full snapshot — clear stale rows first
        cur.execute("TRUNCATE TABLE public.google_ads_lifetime_summary")
        for r in rows:
            cur.execute(UPSERT, (
                r["campaign"], r["ad_group"], r["ad_name"],
                r["ad_state"], r["ad_type"], r["ad_name"], r["campaign"], r["ad_group"],
                r["campaign_type"], r["campaign_subtype"],
                r["campaign_id"] or None, r["ad_group_id"] or None, r["ad_id"] or None,
                r["final_url"] or None,
                r["clicks"], r["impressions"], r["ctr"], r["avg_cpc"],
                r["cost"], r["currency"] or "INR",
                r["conversions"], r["vtc"], r["cost_per_conv"], r["conv_rate"],
                dfrom, dto,
            ))
        conn.commit()
        cur.execute("SELECT COUNT(*), SUM(cost)::bigint, SUM(conversions)::numeric FROM public.google_ads_lifetime_summary")
        n, total_cost, total_conv = cur.fetchone()

print(f"[ok] Supabase  : google_ads_lifetime_summary  rows={n:,}  cost=INR {int(total_cost or 0):,}  conv={float(total_conv or 0):,.1f}")
