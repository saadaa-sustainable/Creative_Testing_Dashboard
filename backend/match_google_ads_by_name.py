"""
match_google_ads_by_name.py — enrich shopify_ad_attribution rows for
Google-attributed orders where the utm parameters carry campaign/ad
NAMES (i.e. tracking template used {campaignname} + custom ad_name),
not the numeric IDs.

Match rule (4-tier, strongest first):
  G1 — utm_content = ad_id (numeric, from Google's {creative} macro)
  G2 — utm_campaign = campaign_id (numeric, from Google auto-tagging)
  G3 — utm_content = ad_name AND utm_campaign = campaign
  G4 — utm_content = ad_name alone AND that ad_name is unique in the CSV

Each tier only touches rows the earlier tiers didn't catch (matched_tier
NOT LIKE 'G%'), so upgrades to a stronger tier never happen mid-run.

Writes:
  - shopify_ad_attribution.ad_name         := CSV ad_name
  - shopify_ad_attribution.campaign_name   := CSV campaign
  - shopify_ad_attribution.matched_value   := ad_name
  - shopify_ad_attribution.matched_tier    := 'G1' or 'G2'
  - shopify_ad_attribution.has_match       := true
"""
from __future__ import annotations
import os, sys, psycopg2
from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass
load_dotenv()

DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: raise SystemExit("Missing SUPABASE_DB_URL in .env")

# ── G1: exact numeric ad_id match (Google {creative} macro) ────────
G1_SQL = """
WITH cand AS (
    SELECT DISTINCT ON (s.order_id)
           s.order_id, g.ad_id, g.ad_name, g.campaign, g.campaign_id
    FROM public.shopify_ad_attribution s
    JOIN public.google_ads_lifetime_summary g
      ON s.utm_content = g.ad_id
    WHERE s.utm_source IN ('google','google_ads')
      AND s.order_created_at >= '2025-01-01'
      AND s.utm_content ~ '^[0-9]+$'
      AND g.ad_id IS NOT NULL AND g.ad_id <> ''
      AND (s.matched_tier IS NULL OR s.matched_tier NOT LIKE 'G%')
    ORDER BY s.order_id, g.cost DESC NULLS LAST
)
UPDATE public.shopify_ad_attribution s
   SET ad_id         = c.ad_id,
       ad_name       = c.ad_name,
       campaign_name = c.campaign,
       has_match     = true,
       matched_value = c.ad_id,
       matched_tier  = 'G1'
  FROM cand c
 WHERE s.order_id = c.order_id
"""

# ── G2: exact numeric campaign_id match (Google auto-tagging) ──────
G2_SQL = """
WITH cand AS (
    SELECT DISTINCT ON (s.order_id)
           s.order_id, g.campaign, g.campaign_id
    FROM public.shopify_ad_attribution s
    JOIN public.google_ads_lifetime_summary g
      ON s.utm_campaign = g.campaign_id
    WHERE s.utm_source IN ('google','google_ads')
      AND s.order_created_at >= '2025-01-01'
      AND s.utm_campaign ~ '^[0-9]+$'
      AND g.campaign_id IS NOT NULL AND g.campaign_id <> ''
      AND (s.matched_tier IS NULL OR s.matched_tier NOT LIKE 'G%')
    ORDER BY s.order_id, g.cost DESC NULLS LAST
)
UPDATE public.shopify_ad_attribution s
   SET campaign_name = c.campaign,
       has_match     = true,
       matched_value = c.campaign_id,
       matched_tier  = 'G2'
  FROM cand c
 WHERE s.order_id = c.order_id
"""

# ── G3: joint (campaign_name, ad_name) match ───────────────────────
G3_SQL = """
WITH cand AS (
    SELECT DISTINCT s.order_id, g.ad_name, g.campaign
    FROM public.shopify_ad_attribution s
    JOIN public.google_ads_lifetime_summary g
      ON s.utm_content  = g.ad_name
     AND s.utm_campaign = g.campaign
    WHERE s.utm_source IN ('google','google_ads')
      AND s.order_created_at >= '2025-01-01'
      AND g.ad_name    NOT IN ('','--','-') AND g.ad_name    IS NOT NULL
      AND g.campaign   NOT IN ('','--','-') AND g.campaign   IS NOT NULL
      AND (s.matched_tier IS NULL OR s.matched_tier NOT LIKE 'G%')
)
UPDATE public.shopify_ad_attribution s
   SET ad_name       = c.ad_name,
       campaign_name = c.campaign,
       has_match     = true,
       matched_value = c.ad_name,
       matched_tier  = 'G3'
  FROM cand c
 WHERE s.order_id = c.order_id
"""

# ── G4: ad_name uniquely identifies one campaign in the CSV ────────
G4_SQL = """
WITH unique_ad_names AS (
    SELECT ad_name, MIN(campaign) AS campaign
    FROM public.google_ads_lifetime_summary
    WHERE ad_name NOT IN ('','--','-') AND ad_name IS NOT NULL
    GROUP BY ad_name
    HAVING COUNT(DISTINCT campaign) = 1
),
cand AS (
    SELECT DISTINCT s.order_id, u.ad_name, u.campaign
    FROM public.shopify_ad_attribution s
    JOIN unique_ad_names u
      ON s.utm_content = u.ad_name
    WHERE s.utm_source IN ('google','google_ads')
      AND s.order_created_at >= '2025-01-01'
      AND (s.matched_tier IS NULL OR s.matched_tier NOT LIKE 'G%')
)
UPDATE public.shopify_ad_attribution s
   SET ad_name       = c.ad_name,
       campaign_name = c.campaign,
       has_match     = true,
       matched_value = c.ad_name,
       matched_tier  = 'G4'
  FROM cand c
 WHERE s.order_id = c.order_id
"""

def main():
    conn = psycopg2.connect(DB_URL); cur = conn.cursor()

    print("[*] pre-check: current Google matches")
    cur.execute("""SELECT matched_tier, COUNT(*)
                   FROM shopify_ad_attribution
                   WHERE utm_source IN ('google','google_ads')
                     AND matched_tier LIKE 'G%'
                   GROUP BY 1 ORDER BY 1""")
    for r in cur.fetchall(): print(f"    {r[0]:>4}  {r[1]:>6,}")

    # Clear prior G-tier assignments so a re-run picks up fresh CSV data
    cur.execute("""UPDATE public.shopify_ad_attribution
                      SET matched_tier=NULL, matched_value=NULL,
                          has_match=false, ad_name=NULL, ad_id=NULL,
                          campaign_name=NULL
                    WHERE utm_source IN ('google','google_ads')
                      AND matched_tier LIKE 'G%'""")
    print(f"    reset prior G-tier rows: {cur.rowcount:,}")

    print("[*] G1 — ad_id numeric match ({creative} macro)")
    cur.execute(G1_SQL); g1 = cur.rowcount
    print(f"    G1 updated: {g1:,} rows")

    print("[*] G2 — campaign_id numeric match (Google auto-tag)")
    cur.execute(G2_SQL); g2 = cur.rowcount
    print(f"    G2 updated: {g2:,} rows")

    print("[*] G3 — joint campaign_name + ad_name match")
    cur.execute(G3_SQL); g3 = cur.rowcount
    print(f"    G3 updated: {g3:,} rows")

    print("[*] G4 — unique ad_name across CSV")
    cur.execute(G4_SQL); g4 = cur.rowcount
    print(f"    G4 updated: {g4:,} rows")

    conn.commit()

    print()
    print("[*] post-check: Google match distribution")
    cur.execute("""
        SELECT matched_tier,
               COUNT(*) AS orders,
               SUM(total_price::numeric)::bigint AS sales
        FROM shopify_ad_attribution
        WHERE utm_source IN ('google','google_ads')
        GROUP BY 1 ORDER BY 2 DESC
    """)
    for r in cur.fetchall():
        tier = r[0] or '(unmatched)'
        print(f"    {tier:<12}  {r[1]:>7,} orders  INR {r[2] or 0:>12,}")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()
