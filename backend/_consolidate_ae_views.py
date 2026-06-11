"""
_consolidate_ae_views.py — single ae_table_view as a REGULAR VIEW backed by
a small pre-aggregated shopify_ad_agg table (so queries stay fast without
materialization). Refresh shopify_ad_agg whenever shopify_ad_attribution
changes; refresh_ae_table.py handles that.
"""
import sys, io, os, time, psycopg2
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="backslashreplace")
from dotenv import load_dotenv
load_dotenv()

DDL = """
-- Drop both the old materialized view and any prior plain view of ae_table_view
DO $$
DECLARE k char;
BEGIN
  SELECT c.relkind INTO k FROM pg_class c
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relname='ae_table_view' AND n.nspname='public';
  IF k = 'v' THEN EXECUTE 'DROP VIEW ae_table_view CASCADE';
  ELSIF k = 'm' THEN EXECUTE 'DROP MATERIALIZED VIEW ae_table_view CASCADE';
  END IF;
END$$;

DROP MATERIALIZED VIEW IF EXISTS ae_view_full CASCADE;
DROP VIEW              IF EXISTS ae_view_full CASCADE;

-- Pre-aggregated per-ad shopify summary (small: 1 row per ad_id with matches).
-- Refreshed by refresh_ae_table.py after each ae_raw_view rebuild.
DROP TABLE IF EXISTS shopify_ad_agg CASCADE;
CREATE TABLE shopify_ad_agg AS
WITH base AS (
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
top_tier AS (
  SELECT DISTINCT ON (ad_id) ad_id, matched_tier AS shopify_top_tier
  FROM (
    SELECT ad_id, matched_tier, SUM(total_price) AS tier_sales
    FROM shopify_ad_attribution
    WHERE has_match AND ad_id IS NOT NULL AND ad_id <> ''
    GROUP BY ad_id, matched_tier
  ) t
  ORDER BY ad_id, tier_sales DESC
)
SELECT b.ad_id, b.shopify_orders, b.shopify_sales, b.shopify_aov,
       b.shopify_first_order, b.shopify_last_order,
       tt.shopify_top_tier
FROM base b
LEFT JOIN top_tier tt USING (ad_id);
ALTER TABLE shopify_ad_agg ADD PRIMARY KEY (ad_id);
GRANT SELECT ON shopify_ad_agg TO anon, authenticated, service_role;

-- Now ae_table_view = ae_raw_view + globals + lookup to the (tiny, indexed)
-- shopify_ad_agg table. Regular VIEW, fast because the JOIN is on a 2.6k-row
-- pre-aggregated table instead of scanning 663k orders.
CREATE VIEW ae_table_view AS
WITH globals AS (
  SELECT NULLIF(SUM(reach), 0::numeric) AS g_reach,
         NULLIF(SUM(amount_spent), 0::numeric) AS g_spend,
         NULLIF(SUM(ftewv_count), 0::numeric) AS g_ftewv,
         NULLIF(SUM(ncp_count), 0::numeric) AS g_ncp,
         NULLIF(SUM(conv_value), 0::numeric) AS g_conv,
         percentile_cont(0.5::double precision) WITHIN GROUP
           (ORDER BY (ftewv_count::double precision))::numeric AS med_ftewv,
         percentile_cont(0.5::double precision) WITHIN GROUP
           (ORDER BY ((conv_value - amount_spent)::double precision))::numeric AS med_profit
  FROM ae_raw_view
)
SELECT r.account_name, r.campaign_name, r.adset_id, r.adset_name, r.ad_id, r.ad_name,
       r.ad_created, r.first_seen_date, r.reporting_starts, r.reporting_ends,
       r.date_target_imp_achieved, r.date_of_result, r.days_to_result, r.days_to_target_f1,
       r.ad_status, r.f1_pass, r.f2_pass, r.f3_pass, r.f4_pass,
       r.impressions, r.reach, r.cost_per_1000, r.frequency,
       r.amount_spent, r.cpc_link, r.ctr_pct, r.checkout_compl_pct,
       r.cr_link_clicks_pct, r.atc_lc_pct, r.ci_atc_pct, r.roas_ma,
       r.cost_per_ftewv, r.ftewv_count, r.cost_per_ncp, r.ncp_count,
       r.ltv_reach, r.ltv_frequency, r.engagement_count,
       r.preview_link, r.ad_link, r.refreshed_at,
       r.conv_value, r.purchases, r.link_clicks_raw, r.ci_count, r.atc_count, r.source,
       ROUND(r.reach::numeric / NULLIF(g.g_reach, 0::numeric) * 100::numeric, 3) AS reach_weight_pct,
       ROUND(r.ftewv_count::numeric / NULLIF(r.reach, 0)::numeric * 100::numeric, 3) AS pct_reach_ftewv,
       ROUND(r.conv_value - r.amount_spent, 2) AS profit_efficiency,
       ROUND(
         CASE WHEN r.conv_value > 0::numeric AND r.amount_spent > 0::numeric
              THEN (1::numeric - r.amount_spent / NULLIF(r.conv_value, 0::numeric)) * 100::numeric
              ELSE -100::numeric END, 2) AS contrib_margin_pct,
       ROUND(
         CASE WHEN r.cost_per_1000 > 0::numeric AND g.g_reach IS NOT NULL
              THEN g.g_spend / g.g_reach * 1000::numeric / r.cost_per_1000
              ELSE 0::numeric END, 3) AS cpr_eff,
       ROUND(
         CASE WHEN r.reach > 0 AND g.g_ftewv IS NOT NULL AND g.g_reach IS NOT NULL
              THEN r.ftewv_count::numeric / r.reach::numeric / (g.g_ftewv / g.g_reach)
              ELSE 0::numeric END, 3) AS ftv_contrib_eff,
       ROUND(
         CASE WHEN g.med_ftewv IS NOT NULL AND g.med_ftewv > 0::numeric
              THEN r.ftewv_count::numeric / g.med_ftewv
              ELSE 0::numeric END, 3) AS ftev_volume,
       ROUND(
         CASE WHEN r.cost_per_ncp > 0::numeric AND g.g_ncp IS NOT NULL
              THEN g.g_spend / g.g_ncp / r.cost_per_ncp
              ELSE 0::numeric END, 3) AS ncp_cost_eff,
       ROUND(
         CASE WHEN r.roas_ma > 0::numeric AND g.g_spend IS NOT NULL
              THEN r.roas_ma / (g.g_conv / g.g_spend)
              ELSE 0::numeric END, 3) AS roas_eff,
       ROUND(
         CASE WHEN g.med_profit IS NOT NULL AND g.med_profit <> 0::numeric
              THEN (r.conv_value - r.amount_spent) / g.med_profit
              ELSE 0::numeric END, 3) AS profit_vol_eff,
       ROUND(
         CASE WHEN r.cost_per_1000 > 0::numeric AND g.g_reach IS NOT NULL
              THEN g.g_spend / g.g_reach * 1000::numeric / r.cost_per_1000
              ELSE 0::numeric END
       + CASE WHEN r.reach > 0 AND g.g_ftewv IS NOT NULL AND g.g_reach IS NOT NULL
              THEN r.ftewv_count::numeric / r.reach::numeric / (g.g_ftewv / g.g_reach)
              ELSE 0::numeric END
       + CASE WHEN r.cost_per_ftewv > 0::numeric AND g.g_ftewv IS NOT NULL
              THEN g.g_spend / g.g_ftewv / r.cost_per_ftewv
              ELSE 0::numeric END
       , 3) AS delivery_eff,
       ROUND(
         CASE WHEN r.cost_per_ncp > 0::numeric AND g.g_ncp IS NOT NULL
              THEN g.g_spend / g.g_ncp / r.cost_per_ncp
              ELSE 0::numeric END
       + CASE WHEN r.roas_ma > 0::numeric AND g.g_spend IS NOT NULL
              THEN r.roas_ma / (g.g_conv / g.g_spend)
              ELSE 0::numeric END
       , 3) AS sales_spend_eff,
       ROUND(0.10 *
         CASE WHEN r.cost_per_1000 > 0::numeric AND g.g_reach IS NOT NULL
              THEN g.g_spend / g.g_reach * 1000::numeric / r.cost_per_1000
              ELSE 0::numeric END
       + 0.25 *
         CASE WHEN r.reach > 0 AND g.g_ftewv IS NOT NULL AND g.g_reach IS NOT NULL
              THEN r.ftewv_count::numeric / r.reach::numeric / (g.g_ftewv / g.g_reach)
              ELSE 0::numeric END
       + 0.15 *
         CASE WHEN g.med_ftewv IS NOT NULL AND g.med_ftewv > 0::numeric
              THEN r.ftewv_count::numeric / g.med_ftewv
              ELSE 0::numeric END
       + 0.20 *
         CASE WHEN r.cost_per_ncp > 0::numeric AND g.g_ncp IS NOT NULL
              THEN g.g_spend / g.g_ncp / r.cost_per_ncp
              ELSE 0::numeric END
       + 0.20 *
         CASE WHEN r.roas_ma > 0::numeric AND g.g_spend IS NOT NULL
              THEN r.roas_ma / (g.g_conv / g.g_spend)
              ELSE 0::numeric END
       + 0.10 *
         CASE WHEN g.med_profit IS NOT NULL AND g.med_profit <> 0::numeric
              THEN (r.conv_value - r.amount_spent) / g.med_profit
              ELSE 0::numeric END
       , 3) AS blended_eff,
       CASE
         WHEN r.f1_pass AND r.f2_pass AND r.f3_pass AND r.f4_pass THEN 'Incremental Winner'::text
         WHEN r.f1_pass AND r.f2_pass AND r.f3_pass             THEN 'Winner'::text
         WHEN r.f1_pass AND r.f4_pass                            THEN 'Priority'::text
         WHEN r.f1_pass                                          THEN 'Analyze 1'::text
         WHEN r.f2_pass                                          THEN 'Analyze 2'::text
         ELSE 'Discarded'::text
       END AS category,
       -- Shopify attribution columns: COALESCE numeric ones to 0 so the dashboard
       -- table doesn't render "—" for every non-attributed ad. Dates and top_tier
       -- stay NULL (more honest signal "no orders").
       COALESCE(sa.shopify_orders, 0)::bigint            AS shopify_orders,
       COALESCE(sa.shopify_sales,  0::numeric)::numeric(14,2) AS shopify_sales,
       COALESCE(sa.shopify_aov,    0::numeric)::numeric(12,2) AS shopify_aov,
       sa.shopify_first_order,
       sa.shopify_last_order,
       sa.shopify_top_tier,
       CASE WHEN r.amount_spent > 0 AND sa.shopify_sales IS NOT NULL
            THEN ROUND((sa.shopify_sales / r.amount_spent)::numeric, 3)
            ELSE NULL END AS shopify_roas
FROM ae_raw_view r
  CROSS JOIN globals g
  LEFT JOIN shopify_ad_agg sa USING (ad_id);

GRANT SELECT ON ae_table_view TO anon, authenticated, service_role;
NOTIFY pgrst, 'reload schema';
"""


def main():
    conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"].strip())
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")
    t0 = time.time()
    print("Rebuilding shopify_ad_agg + ae_table_view as a regular view ...")
    cur.execute(DDL)
    conn.commit()
    print(f"  done in {time.time()-t0:.1f}s")

    cur.execute("SELECT COUNT(*) FROM shopify_ad_agg")
    print(f"\n  shopify_ad_agg rows  : {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE shopify_orders > 0) FROM ae_table_view")
    n, w = cur.fetchone()
    print(f"  ae_table_view rows   : {n:,}   ({w:,} with shopify orders)")

    cur.execute("""SELECT
        c.relname,
        CASE c.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'matview' WHEN 'r' THEN 'table' END
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='public' AND c.relname IN ('ae_table_view','ae_view_full','shopify_ad_agg','ae_raw_view')
      ORDER BY c.relname""")
    print("\n  Object inventory:")
    for nm, kind in cur.fetchall():
        print(f"    {nm:<22} {kind}")
    conn.close()


if __name__ == "__main__":
    main()
