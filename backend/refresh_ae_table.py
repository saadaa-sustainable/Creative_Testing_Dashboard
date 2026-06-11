"""One-shot refresh of ae_table_view from primary_table.

Truncates the table then re-populates with computed Book1 metrics for every
ad in primary_table. Re-run after each primary_sync.
"""

import os, time
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SETUP_SQL = """
-- Idempotent — drops only if exists; never errors on first run.
-- The table currently has 56 columns (incl. conv_value, purchases, link_clicks_raw,
-- ci_count, atc_count, source). Keep the CREATE in sync.
DROP TABLE IF EXISTS ae_table_view CASCADE;
DROP VIEW  IF EXISTS ae_table_view CASCADE;
CREATE TABLE ae_table_view (
    account_name         TEXT,
    campaign_name        TEXT,
    adset_id             TEXT,
    adset_name           TEXT,
    ad_id                TEXT PRIMARY KEY,
    ad_name              TEXT,
    ad_created           DATE,
    first_seen_date      DATE,   -- first date this ad served impressions (>=1)
    reporting_starts     DATE,
    reporting_ends       DATE,
    date_target_imp_achieved DATE,
    date_of_result       DATE,
    days_to_result       INT,
    days_to_target_f1    INT,
    ad_status            TEXT,
    f1_pass              BOOLEAN,
    f2_pass              BOOLEAN,
    f3_pass              BOOLEAN,
    f4_pass              BOOLEAN,
    category             TEXT,
    impressions          BIGINT,
    reach                BIGINT,
    reach_weight_pct     NUMERIC(8,3),
    cost_per_1000        NUMERIC(14,2),
    frequency            NUMERIC(10,2),
    amount_spent         NUMERIC(14,2),
    cpc_link             NUMERIC(12,2),
    ctr_pct              NUMERIC(8,2),
    checkout_compl_pct   NUMERIC(8,2),
    cr_link_clicks_pct   NUMERIC(8,2),
    atc_lc_pct           NUMERIC(8,2),
    ci_atc_pct           NUMERIC(8,2),
    roas_ma              NUMERIC(10,3),
    cost_per_ftewv       NUMERIC(12,2),
    ftewv_count          BIGINT,
    cost_per_ncp         NUMERIC(12,2),
    pct_reach_ftewv      NUMERIC(8,3),
    profit_efficiency    NUMERIC(16,2),
    contrib_margin_pct   NUMERIC(8,2),
    delivery_eff         NUMERIC(12,3),
    sales_spend_eff      NUMERIC(12,3),
    blended_eff          NUMERIC(12,3),
    cpr_eff              NUMERIC(12,3),
    ftv_contrib_eff      NUMERIC(12,3),
    ftev_volume          NUMERIC(12,3),
    ncp_cost_eff         NUMERIC(12,3),
    roas_eff             NUMERIC(12,3),
    profit_vol_eff       NUMERIC(12,3),
    ncp_count            BIGINT,
    ltv_reach            NUMERIC(14,2),
    ltv_frequency        NUMERIC(8,4),
    engagement_count     BIGINT,
    preview_link         TEXT,
    ad_link              TEXT,
    refreshed_at         TIMESTAMPTZ DEFAULT NOW(),
    conv_value           NUMERIC(14,2),
    purchases            NUMERIC(12,2),
    link_clicks_raw      BIGINT,
    ci_count             NUMERIC(12,2),
    atc_count            NUMERIC(12,2),
    source               TEXT DEFAULT 'primary_backfill'
);
CREATE INDEX IF NOT EXISTS idx_ae_account  ON ae_table_view(account_name);
CREATE INDEX IF NOT EXISTS idx_ae_category ON ae_table_view(category);
CREATE INDEX IF NOT EXISTS idx_ae_campaign ON ae_table_view(campaign_name);
"""

SQL = """
TRUNCATE ae_raw_view;

INSERT INTO ae_raw_view (
    account_name, campaign_name, adset_id, adset_name, ad_id, ad_name,
    ad_created, first_seen_date, reporting_starts, reporting_ends,
    date_target_imp_achieved, date_of_result, days_to_result, days_to_target_f1,
    ad_status, f1_pass, f2_pass, f3_pass, f4_pass,
    impressions, reach, cost_per_1000, frequency,
    amount_spent, cpc_link, ctr_pct, checkout_compl_pct,
    cr_link_clicks_pct, atc_lc_pct, ci_atc_pct, roas_ma,
    cost_per_ftewv, ftewv_count, cost_per_ncp,
    ncp_count, ltv_reach, ltv_frequency,
    engagement_count, preview_link, ad_link,
    conv_value, purchases, link_clicks_raw, ci_count, atc_count, source
)
WITH f1_hit AS (
    -- Compute F1-target-hit date per ad_id using cumulative impressions
    -- across daily rows from BOTH backfill_table and primary_table (deduped by MAX).
    SELECT ad_id, MIN(date) AS date_target_imp_achieved
    FROM (
        SELECT
            ad_id, date, cum_imp
        FROM (
            SELECT
                ad_id, date,
                SUM(impressions) OVER (PARTITION BY ad_id ORDER BY date ROWS UNBOUNDED PRECEDING) AS cum_imp
            FROM (
                SELECT ad_id, date, MAX(impressions) AS impressions
                FROM (
                    SELECT ad_id, date, impressions FROM backfill_table WHERE ad_id IS NOT NULL
                    UNION ALL
                    SELECT ad_id, date, impressions FROM primary_table  WHERE ad_id IS NOT NULL
                ) u
                GROUP BY ad_id, date
            ) d
        ) c
        WHERE cum_imp >= 50000
    ) h
    GROUP BY ad_id
),
backfill_agg AS (
    -- Lifetime aggregation from backfill_table (goes back to 2023)
    SELECT
        ad_id,
        (ARRAY_AGG(account_name  ORDER BY date DESC))[1] AS account_name,
        (ARRAY_AGG(ad_name       ORDER BY date DESC))[1] AS ad_name,
        (ARRAY_AGG(campaign_name ORDER BY date DESC))[1] AS campaign_name,
        (ARRAY_AGG(ad_status     ORDER BY date DESC))[1] AS ad_status_bf,
        MIN(ad_created_date) AS ad_created,
        MIN(CASE WHEN impressions > 0 THEN date END) AS first_seen_date,
        MIN(date)            AS reporting_starts,
        MAX(date)            AS reporting_ends,
        COALESCE(SUM(impressions),      0)          AS impressions,
        COALESCE(SUM(reach),            0)          AS reach,
        COALESCE(SUM(amount_spent_inr), 0)::numeric AS amount_spent,
        COALESCE(SUM(conversion_value), 0)::numeric AS conv_value,
        COALESCE(SUM(outbound_clicks),  0)          AS outbound_clicks,
        COALESCE(SUM(ftewv_count),      0)          AS ftewv_count,
        COALESCE(SUM(ncp_count),        0)          AS ncp_count,
        MAX(ltv_reach)     AS ltv_reach,
        MAX(ltv_frequency) AS ltv_frequency,
        -- Backfill_table is the lifetime store; carry ad_link / adset info
        -- forward so pre-2026 ads (no primary_table row) still surface them.
        (ARRAY_AGG(ad_link ORDER BY date DESC) FILTER (WHERE ad_link IS NOT NULL AND ad_link <> ''))[1] AS ad_link_bf,
        (ARRAY_AGG(adset_id   ORDER BY date DESC) FILTER (WHERE adset_id   IS NOT NULL AND adset_id   <> ''))[1] AS adset_id_bf,
        (ARRAY_AGG(adset_name ORDER BY date DESC) FILTER (WHERE adset_name IS NOT NULL AND adset_name <> ''))[1] AS adset_name_bf
    FROM backfill_table
    WHERE ad_id IS NOT NULL
    GROUP BY ad_id
),
primary_agg AS (
    -- Funnel + metadata that only exists in primary_table (2026-01-01 onwards)
    SELECT
        ad_id,
        (ARRAY_AGG(adset_id    ORDER BY date DESC))[1] AS adset_id,
        (ARRAY_AGG(adset_name  ORDER BY date DESC))[1] AS adset_name,
        (ARRAY_AGG(ad_status   ORDER BY date DESC))[1] AS ad_status_pri,
        (ARRAY_AGG(account_name ORDER BY date DESC))[1] AS account_name_pri,
        (ARRAY_AGG(ad_name      ORDER BY date DESC))[1] AS ad_name_pri,
        (ARRAY_AGG(campaign_name ORDER BY date DESC))[1] AS campaign_name_pri,
        MIN(ad_created_date) AS ad_created_pri,
        MIN(CASE WHEN impressions > 0 THEN date END) AS first_seen_date_pri,
        MIN(date) AS reporting_starts_pri,
        MAX(date) AS reporting_ends_pri,
        COALESCE(SUM(inline_link_clicks),0)          AS link_clicks,
        COALESCE(SUM(purchases),         0)::numeric AS purchases,
        COALESCE(SUM(initiate_checkout), 0)::numeric AS ci,
        COALESCE(SUM(add_to_cart),       0)::numeric AS atc,
        COALESCE(SUM(engagement_count),  0)          AS engagement_count,
        (ARRAY_AGG(preview_link ORDER BY date DESC) FILTER (WHERE preview_link IS NOT NULL AND preview_link <> ''))[1] AS preview_link,
        (ARRAY_AGG(ad_link      ORDER BY date DESC) FILTER (WHERE ad_link      IS NOT NULL AND ad_link      <> ''))[1] AS ad_link
    FROM primary_table
    WHERE ad_id IS NOT NULL
    GROUP BY ad_id
),
all_ad_ids AS (
    SELECT ad_id FROM backfill_agg
    UNION
    SELECT ad_id FROM primary_agg
),
per_ad AS (
    SELECT
        a.ad_id,
        COALESCE(b.account_name,  p.account_name_pri)   AS account_name,
        COALESCE(b.campaign_name, p.campaign_name_pri)  AS campaign_name,
        -- adset_id/name: primary_table has it for only ~46% of ads (recent
        -- sync window); backfill_table has 99.6% coverage. Prefer primary
        -- (freshest source) but fall back to backfill so pre-2026 ads aren't blank.
        COALESCE(NULLIF(p.adset_id,   ''), b.adset_id_bf,   '') AS adset_id,
        COALESCE(NULLIF(p.adset_name, ''), b.adset_name_bf, '') AS adset_name,
        COALESCE(b.ad_name, p.ad_name_pri)               AS ad_name,
        COALESCE(p.ad_status_pri, b.ad_status_bf)        AS ad_status,
        COALESCE(b.ad_created, p.ad_created_pri)         AS ad_created,
        LEAST(b.first_seen_date, p.first_seen_date_pri)  AS first_seen_date,
        LEAST(b.reporting_starts, p.reporting_starts_pri) AS reporting_starts,
        GREATEST(b.reporting_ends, p.reporting_ends_pri)  AS reporting_ends,
        COALESCE(b.impressions,    0) AS impressions,
        -- Reach: prefer Meta's lifetime-deduplicated reach (`ltv_reach`).
        -- The bare `SUM(b.reach)` from backfill_table double-counts a person
        -- who saw the ad on multiple days (Meta dedupes per day, not across
        -- days). Validated against Meta Admin API: SUM(reach) overstates
        -- true lifetime reach by 30–75% on average. ltv_reach is correct.
        COALESCE(NULLIF(b.ltv_reach, 0), b.reach, 0)::bigint AS reach,
        COALESCE(b.amount_spent,   0::numeric) AS amount_spent,
        COALESCE(b.outbound_clicks,0) AS outbound_clicks,
        COALESCE(p.link_clicks,    0) AS link_clicks,
        COALESCE(p.purchases,      0::numeric) AS purchases,
        COALESCE(p.ci,             0::numeric) AS ci,
        COALESCE(p.atc,            0::numeric) AS atc,
        COALESCE(b.conv_value,     0::numeric) AS conv_value,
        COALESCE(b.ftewv_count,    0) AS ftewv_count,
        COALESCE(b.ncp_count,      0) AS ncp_count,
        COALESCE(p.engagement_count,0) AS engagement_count,
        b.ltv_reach,
        b.ltv_frequency,
        p.preview_link,
        -- Prefer the freshest primary_table ad_link; fall back to backfill_table's
        -- value so older ads (pre-2026, only in backfill) still surface a link.
        COALESCE(NULLIF(p.ad_link, ''), b.ad_link_bf) AS ad_link
    FROM all_ad_ids a
    LEFT JOIN backfill_agg b ON b.ad_id = a.ad_id
    LEFT JOIN primary_agg  p ON p.ad_id = a.ad_id
),
globals AS (
    SELECT
        SUM(reach)::numeric         AS g_reach,
        SUM(amount_spent)::numeric  AS g_spend,
        SUM(ftewv_count)::numeric   AS g_ftewv,
        SUM(ncp_count)::numeric     AS g_ncp,
        SUM(conv_value)::numeric    AS g_conv,
        (percentile_cont(0.5) WITHIN GROUP (ORDER BY ftewv_count))::numeric AS med_ftewv,
        (percentile_cont(0.5) WITHIN GROUP (ORDER BY (conv_value - amount_spent)))::numeric AS med_profit
    FROM per_ad
),
calc AS (
    SELECT
        p.*,
        g.g_reach, g.g_spend, g.g_ftewv, g.g_ncp, g.g_conv, g.med_ftewv, g.med_profit,
        CASE WHEN p.reach > 0       THEN (p.impressions::numeric / p.reach)              END AS frequency,
        CASE WHEN p.reach > 0       THEN (p.amount_spent / p.reach * 1000)               END AS cpr_1000,
        CASE WHEN p.link_clicks > 0 THEN (p.amount_spent / p.link_clicks)                END AS cpc_link,
        CASE WHEN p.impressions > 0 THEN (p.link_clicks::numeric / p.impressions * 100)  END AS ctr_pct,
        CASE WHEN p.ci > 0          THEN (p.purchases / p.ci * 100)                       END AS checkout_compl_pct,
        CASE WHEN p.link_clicks > 0 THEN (p.purchases::numeric / p.link_clicks * 100)     END AS cr_lc_pct,
        CASE WHEN p.link_clicks > 0 THEN (p.atc / p.link_clicks * 100)                    END AS atc_lc_pct,
        CASE WHEN p.atc > 0         THEN (p.ci  / p.atc * 100)                            END AS ci_atc_pct,
        CASE WHEN p.amount_spent > 0 THEN (p.conv_value / p.amount_spent) ELSE 0::numeric  END AS roas_ma,
        CASE WHEN p.ftewv_count > 0 THEN (p.amount_spent / p.ftewv_count)                  END AS cost_per_ftewv,
        CASE WHEN p.ncp_count   > 0 THEN (p.amount_spent / p.ncp_count)                    END AS cost_per_ncp,
        CASE WHEN p.reach > 0       THEN (p.ftewv_count::numeric / p.reach * 100)          END AS pct_reach_ftewv,
        CASE WHEN g.g_reach > 0     THEN (p.reach::numeric / g.g_reach * 100)              END AS reach_weight_pct,
        (p.conv_value - p.amount_spent)::numeric                                              AS profit_efficiency,
        CASE WHEN p.amount_spent > 0 AND p.conv_value > 0
             THEN (1 - p.amount_spent / NULLIF(p.conv_value, 0)) * 100
             ELSE -100::numeric END AS contrib_margin_pct,
        CASE WHEN g.g_reach > 0 THEN g.g_spend / g.g_reach * 1000 END AS anchor_cpr,
        CASE WHEN g.g_spend > 0 THEN g.g_conv  / g.g_spend         END AS anchor_roas,
        CASE WHEN g.g_reach > 0 THEN g.g_ftewv / g.g_reach          END AS anchor_ftewv_pct,
        CASE WHEN g.g_ftewv > 0 THEN g.g_spend / g.g_ftewv          END AS anchor_cpftewv
    FROM per_ad p, globals g
),
efficiency AS (
    SELECT
        c.*,
        CASE WHEN c.cpr_1000 > 0 AND c.anchor_cpr IS NOT NULL THEN c.anchor_cpr / c.cpr_1000 ELSE 0::numeric END AS x_cpr_eff,
        CASE WHEN c.reach > 0 AND c.anchor_ftewv_pct > 0
             THEN (c.ftewv_count::numeric / c.reach) / c.anchor_ftewv_pct ELSE 0::numeric END AS y_ftv_contrib_eff,
        CASE WHEN c.med_ftewv > 0 THEN c.ftewv_count::numeric / c.med_ftewv ELSE 0::numeric END AS z_ftev_volume,
        -- AA NCP Cost Eff = anchor_ncp / row_CPN (matches Apps Script canonical)
        CASE WHEN c.cost_per_ncp > 0 AND c.g_ncp > 0 THEN (c.g_spend / c.g_ncp) / c.cost_per_ncp ELSE 0::numeric END AS aa_ncp_cost_eff,
        CASE WHEN c.roas_ma > 0 AND c.anchor_roas > 0 THEN c.roas_ma / c.anchor_roas ELSE 0::numeric END AS ab_roas_eff,
        CASE WHEN c.med_profit <> 0 THEN c.profit_efficiency / c.med_profit ELSE 0::numeric END AS ac_profit_vol_eff
    FROM calc c
)
SELECT
    e.account_name, e.campaign_name, e.adset_id, e.adset_name, e.ad_id, e.ad_name,
    e.ad_created, e.first_seen_date, e.reporting_starts, e.reporting_ends,
    -- Result-timing fields (all anchored on first_seen_date — when the ad actually
    -- started delivering impressions, NOT the day Meta says it was "created").
    f.date_target_imp_achieved                                                              AS date_target_imp_achieved,
    CASE
        WHEN f.date_target_imp_achieved IS NOT NULL                THEN f.date_target_imp_achieved
        WHEN e.first_seen_date IS NOT NULL                         THEN (e.first_seen_date + INTERVAL '14 days')::date
        WHEN e.ad_created IS NOT NULL                              THEN (e.ad_created + INTERVAL '14 days')::date
        ELSE NULL
    END                                                                                     AS date_of_result,
    CASE
        WHEN e.first_seen_date IS NULL THEN NULL
        ELSE GREATEST(0,
            (COALESCE(
                f.date_target_imp_achieved,
                (e.first_seen_date + INTERVAL '14 days')::date
            ) - e.first_seen_date)::int)
    END                                                                                     AS days_to_result,
    CASE
        WHEN f.date_target_imp_achieved IS NULL OR e.first_seen_date IS NULL THEN NULL
        ELSE GREATEST(0, (f.date_target_imp_achieved - e.first_seen_date)::int)
    END                                                                                     AS days_to_target_f1,
    e.ad_status,
    (e.impressions >= 50000),
    (amount_spent > 0 AND conv_value / amount_spent >= 3.2),
    (ncp_count   > 0 AND amount_spent / ncp_count <= 525),
    (ftewv_count > 0 AND amount_spent / ftewv_count <= 25),
    impressions,
    reach,
    ROUND(cpr_1000::numeric, 2),
    ROUND(frequency::numeric, 2),
    ROUND(amount_spent::numeric, 2),
    ROUND(cpc_link::numeric, 2),
    ROUND(ctr_pct::numeric, 2),
    ROUND(checkout_compl_pct::numeric, 2),
    ROUND(cr_lc_pct::numeric, 2),
    ROUND(atc_lc_pct::numeric, 2),
    ROUND(ci_atc_pct::numeric, 2),
    ROUND(roas_ma::numeric, 3),
    ROUND(cost_per_ftewv::numeric, 2),
    ftewv_count,
    ROUND(cost_per_ncp::numeric, 2),
    ncp_count,
    ltv_reach,
    ltv_frequency,
    engagement_count,
    preview_link,
    ad_link,
    -- new columns
    ROUND(conv_value::numeric, 2)         AS conv_value,
    ROUND(purchases::numeric, 2)          AS purchases,
    link_clicks                            AS link_clicks_raw,
    ROUND(ci::numeric, 2)                 AS ci_count,
    ROUND(atc::numeric, 2)                AS atc_count,
    'primary_backfill'                    AS source
FROM efficiency e
LEFT JOIN f1_hit f ON f.ad_id = e.ad_id;
"""

t0 = time.time()
conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"])
conn.autocommit = False
cur = conn.cursor()
cur.execute("SET statement_timeout = '180s'")
# SETUP_SQL is no longer run — ae_raw_view is the base table now (managed via ALTER),
# and ae_table_view exists as a VIEW (CREATE OR REPLACE VIEW). Skipping table re-creation
# so the VIEW dependency isn't broken.
print("Running TRUNCATE + INSERT into ae_raw_view ...")
cur.execute(SQL)
conn.commit()
elapsed = time.time() - t0
print(f"Done in {elapsed:.1f}s")

cur.execute("SELECT COUNT(*) FROM ae_raw_view")
n_rows = cur.fetchone()[0]
cur.execute("SELECT COUNT(DISTINCT category) FROM ae_table_view")
n_cats = cur.fetchone()[0]
print(f"\nae_raw_view: {n_rows:,} rows | ae_table_view VIEW: {n_cats} distinct categories")

cur.execute("""
    SELECT category, COUNT(*) FROM ae_table_view
    GROUP BY category ORDER BY COUNT(*) DESC
""")
print("\nPer-category breakdown:")
for cat, n in cur.fetchall():
    print(f"  {cat:25s} {n:>6,}")

cur.execute("""
    SELECT account_name, COUNT(*) FROM ae_table_view
    GROUP BY account_name ORDER BY account_name
""")
print("\nPer-account breakdown:")
for acct, n in cur.fetchall():
    print(f"  {acct:30s} {n:>6,}")

# ── ae_view_full = ae_table_view + per-ad shopify aggregates ───────────────
# Materialized view so Supabase REST queries stay under the 8s statement_timeout
# (the inline aggregation against 660k shopify_ad_attribution rows was timing out).
# Cheap to rebuild — 2-3s on the current data volume.
print("\nRefreshing ae_view_full (materialized) — ae_table_view + shopify aggregates...")
cur.execute("DROP MATERIALIZED VIEW IF EXISTS ae_view_full CASCADE;")
cur.execute("""
CREATE MATERIALIZED VIEW ae_view_full AS
WITH shopify_agg AS (
  SELECT ad_id,
         COUNT(*)                          AS shopify_orders,
         ROUND(SUM(total_price), 2)        AS shopify_sales,
         ROUND(AVG(total_price), 2)        AS shopify_aov,
         MIN(order_created_at)::date       AS shopify_first_order,
         MAX(order_created_at)::date       AS shopify_last_order
  FROM shopify_ad_attribution
  WHERE has_match AND ad_id IS NOT NULL AND ad_id <> ''
  GROUP BY ad_id
),
shopify_top_tier AS (
  SELECT DISTINCT ON (ad_id) ad_id, matched_tier AS shopify_top_tier
  FROM (
    SELECT ad_id, matched_tier, SUM(total_price) AS tier_sales
    FROM shopify_ad_attribution
    WHERE has_match AND ad_id IS NOT NULL AND ad_id <> ''
    GROUP BY ad_id, matched_tier
  ) t
  ORDER BY ad_id, tier_sales DESC
)
SELECT v.*,
       sa.shopify_orders,
       sa.shopify_sales,
       sa.shopify_aov,
       sa.shopify_first_order,
       sa.shopify_last_order,
       st.shopify_top_tier,
       CASE WHEN v.amount_spent > 0 AND sa.shopify_sales IS NOT NULL
            THEN ROUND((sa.shopify_sales / v.amount_spent)::numeric, 3)
            ELSE NULL END AS shopify_roas
FROM ae_table_view v
LEFT JOIN shopify_agg       sa USING (ad_id)
LEFT JOIN shopify_top_tier  st USING (ad_id);
""")
cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_aevf_ad_id    ON ae_view_full (ad_id);")
cur.execute("CREATE INDEX        IF NOT EXISTS idx_aevf_impr     ON ae_view_full (impressions DESC NULLS LAST);")
cur.execute("CREATE INDEX        IF NOT EXISTS idx_aevf_account  ON ae_view_full (account_name);")
cur.execute("CREATE INDEX        IF NOT EXISTS idx_aevf_category ON ae_view_full (category);")
cur.execute("GRANT SELECT ON ae_view_full TO anon, authenticated, service_role;")
cur.execute("NOTIFY pgrst, 'reload schema';")
conn.commit()
cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE shopify_orders > 0) FROM ae_view_full")
n_v, n_sh = cur.fetchone()
print(f"  ae_view_full: {n_v:,} rows  ({n_sh:,} with shopify orders attached)")

cur.close()
conn.close()
