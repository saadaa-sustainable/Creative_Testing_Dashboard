CREATE OR REPLACE VIEW ae_view_full AS
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
