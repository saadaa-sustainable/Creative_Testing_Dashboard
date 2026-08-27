/** Row shape returned by GET /api/ads — mirrors _AE_COLS in backend/api_ae.py. */
export interface Ad {
  account_name: string | null;
  campaign_name: string | null;
  adset_id: string | null;
  adset_name: string | null;
  ad_id: string;
  ad_name: string | null;
  ad_created: string | null;         // ISO date
  first_seen_date: string | null;
  reporting_starts: string | null;
  reporting_ends: string | null;
  date_target_imp_achieved: string | null;
  date_of_result: string | null;
  days_to_result: number | null;
  days_to_target_f1: number | null;
  ad_status: string | null;
  category: string | null;
  f1_pass: boolean | null;
  f2_pass: boolean | null;
  f3_pass: boolean | null;
  f4_pass: boolean | null;
  impressions: number | null;
  reach: number | null;
  reach_weight_pct: number | null;
  frequency: number | null;
  ltv_reach: number | null;
  ltv_frequency: number | null;
  amount_spent: number | null;
  cost_per_1000: number | null;
  cpc_link: number | null;
  ctr_pct: number | null;
  link_clicks_raw: number | null;
  checkout_compl_pct: number | null;
  cr_link_clicks_pct: number | null;
  atc_lc_pct: number | null;
  atc_count: number | null;
  ci_atc_pct: number | null;
  ci_count: number | null;
  roas_ma: number | null;
  ftewv_count: number | null;
  pct_reach_ftewv: number | null;
  cost_per_ftewv: number | null;
  cost_per_ncp: number | null;
  ncp_count: number | null;
  conv_value: number | null;
  purchases: number | null;
  profit_efficiency: number | null;
  contrib_margin_pct: number | null;
  delivery_eff: number | null;
  sales_spend_eff: number | null;
  blended_eff: number | null;
  cpr_eff: number | null;
  ftv_contrib_eff: number | null;
  ftev_volume: number | null;
  ncp_cost_eff: number | null;
  roas_eff: number | null;
  profit_vol_eff: number | null;
  engagement_count: number | null;
  preview_link: string | null;
  ad_link: string | null;
  shopify_orders: number | null;
  shopify_sales: number | null;
  shopify_top_tier: string | null;
  shopify_roas: number | null;
}

export interface AdsResponse {
  rows: Ad[];
  count: number;
}
