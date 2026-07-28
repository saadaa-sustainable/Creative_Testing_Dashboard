/**
 * RCK Daily — 30-day daywise breakdown for RCK campaign ads.
 *
 * One row per (ad_id × date) for every ad in an RCK campaign, last 30 days.
 * Includes ALL statuses (not just currently-ACTIVE) — same permissive
 * scope as rck_last30.
 *
 * Sources:
 *   public.rck_daily_30d       — daily metrics per (ad_id, date)
 *   public.rck_last30          — lifetime-within-window context (repeated
 *                                per day, e.g. days_active, ad_status,
 *                                shop_minus_meta, roas_ma, first/last_seen)
 *   public.ad_asset_ids        — asset_id per ad
 *   public.ad_utm_mode         — mode utm/matched values per ad
 *
 * Rate columns (cost_per_1000, ctr_pct, roas_ma, etc.) are derived
 * per-day from the raw counts in rck_daily_30d.
 *
 * Naming: all symbols prefixed R30_ to avoid collision with the L30 and
 * lifetime scripts (which use D30_ / no-prefix respectively).
 */

const R30_SHEET_NAME  = 'RCK_daily';
const R30_TABLE       = 'rck_daily_30d';
const R30_LIFETIME    = 'rck_last30';

const R30_HEADERS = [
  'date',
  'account_name', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name',
  'asset_id', 'ad_status',
  'first_seen_date', 'last_seen_date', 'days_active',
  'impressions', 'reach', 'cost_per_1000', 'frequency', 'amount_spent',
  'cpc_link', 'ctr_pct', 'checkout_compl_pct', 'cr_link_clicks_pct',
  'atc_lc_pct', 'ci_atc_pct', 'roas_ma',
  'cost_per_ftewv', 'ftewv_count', 'cost_per_ncp', 'ncp_count',
  'ltv_reach', 'ltv_frequency',
  'outbound_clicks', 'link_clicks_raw', 'thruplays', 'three_sec_plays',
  'post_engagements', 'engagement_count', 'video_play_time',
  'conv_value', 'purchases', 'ci_count', 'atc_count',
  'shopify_orders', 'shopify_sales', 'shop_minus_meta', 'shop_vs_meta_pct',
  'shopify_roas',
  'utm_content_top', 'utm_term_top', 'utm_campaign_top',
  'matched_value_top', 'matched_tier_top',
  'refreshed_at'
];

// Fields sourced from the DAILY row (from rck_daily_30d).
// The daily table uses primary_table naming for some fields
// (conversion_value not conv_value); map before lookup.
const R30_HEADER_TO_DAILY = {
  'conv_value': 'conversion_value',
};
const R30_DAILY_OVERRIDES = new Set([
  'date', 'ad_status',
  'impressions', 'reach', 'frequency', 'amount_spent',
  'conv_value', 'purchases',
  'link_clicks_raw', 'ci_count', 'atc_count', 'engagement_count',
  'ftewv_count', 'ncp_count', 'ltv_reach', 'ltv_frequency',
  'outbound_clicks', 'thruplays', 'three_sec_plays', 'post_engagements', 'video_play_time',
  'shopify_orders', 'shopify_sales', 'shopify_roas',
  // Derived per-day (computed below):
  'cost_per_1000', 'cpc_link', 'ctr_pct',
  'checkout_compl_pct', 'cr_link_clicks_pct',
  'atc_lc_pct', 'ci_atc_pct', 'roas_ma',
  'cost_per_ftewv', 'cost_per_ncp',
]);

// ── Main ───────────────────────────────────────────────────────────────
function refreshRckDaily() {
  const {url, key} = getConfig_();   // shared helper from active_ads.js
  const t0 = Date.now();
  console.log('Fetching rck_last30 (lifetime context) ...');
  const lifetime = R30_fetchLifetime_(url, key);
  console.log('Fetching ad_asset_ids ...');
  const assets = fetchAssetMap_(url, key);   // shared helper
  console.log('Fetching ad_utm_mode ...');
  const utmMap = R30_fetchUtmMode_(url, key);
  console.log('Fetching rck_daily_30d ...');
  const daily = R30_fetchDaily_(url, key);
  console.log(`Daily rows: ${daily.length}. Merging + writing ...`);
  const rows = R30_merge_(daily, lifetime, assets, utmMap);
  R30_write_(rows);
  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  SpreadsheetApp.getActiveSpreadsheet()
    .toast(`Wrote ${rows.length} RCK-daily rows in ${secs}s`, 'RCK Daily', 6);
}

// ── Fetchers ───────────────────────────────────────────────────────────
function R30_fetchLifetime_(url, key) {
  const out = {};
  let offset = 0;
  const endpoint = url + '/rest/v1/' + R30_LIFETIME + '?select=*';
  while (true) {
    const resp = R30_get_(endpoint, key, offset);
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    for (const r of batch) if (r.ad_id) out[r.ad_id] = r;
    if (batch.length < 1000) break;
    offset += 1000;
    Utilities.sleep(50);
  }
  return out;
}

function R30_fetchDaily_(url, key) {
  const out = [];
  let offset = 0;
  const endpoint = url + '/rest/v1/' + R30_TABLE +
                   '?select=*&order=ad_id.asc,date.asc';
  while (true) {
    const resp = R30_get_(endpoint, key, offset);
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    out.push.apply(out, batch);
    if (batch.length < 1000) break;
    offset += 1000;
    Utilities.sleep(50);
  }
  return out;
}

function R30_fetchUtmMode_(url, key) {
  const out = {};
  let offset = 0;
  const endpoint = url + '/rest/v1/ad_utm_mode' +
                   '?select=ad_id,utm_content_top,utm_term_top,utm_campaign_top,matched_value_top,matched_tier_top';
  while (true) {
    const resp = R30_get_(endpoint, key, offset);
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    for (const r of batch) if (r.ad_id) out[r.ad_id] = r;
    if (batch.length < 1000) break;
    offset += 1000;
    Utilities.sleep(50);
  }
  return out;
}

function R30_get_(endpoint, key, offset) {
  const resp = UrlFetchApp.fetch(endpoint, {
    method: 'get',
    headers: {
      apikey:        key,
      Authorization: 'Bearer ' + key,
      Range:         offset + '-' + (offset + 999),
      'Range-Unit':  'items',
    },
    muteHttpExceptions: true,
  });
  const code = resp.getResponseCode();
  if (code !== 200 && code !== 206) {
    throw new Error('HTTP ' + code + ' at ' + endpoint.slice(0, 120) +
                    ' → ' + resp.getContentText().slice(0, 300));
  }
  return resp;
}

// ── Merge ──────────────────────────────────────────────────────────────
function R30_merge_(daily, lifetime, assets, utmMap) {
  const out = [];
  for (const d of daily) {
    const life = lifetime[d.ad_id] || {};
    const utm  = utmMap[d.ad_id]   || {};
    const row  = {};

    // Per-day raw counts (for rate derivation)
    const impr  = Number(d.impressions)       || 0;
    const reach = Number(d.reach)             || 0;
    const spend = Number(d.amount_spent)      || 0;
    const conv  = Number(d.conversion_value)  || 0;
    const purch = Number(d.purchases)         || 0;
    const lc    = Number(d.link_clicks_raw)   || 0;
    const ci    = Number(d.ci_count)          || 0;
    const atc   = Number(d.atc_count)         || 0;
    const ftewv = Number(d.ftewv_count)       || 0;
    const ncp   = Number(d.ncp_count)         || 0;
    const shop  = Number(d.shopify_sales)     || 0;

    const derived = {
      cost_per_1000:      reach > 0 ? +(spend / reach * 1000).toFixed(2) : '',
      cpc_link:           lc    > 0 ? +(spend / lc).toFixed(2)          : '',
      ctr_pct:            impr  > 0 ? +(lc / impr * 100).toFixed(2)     : '',
      checkout_compl_pct: ci    > 0 ? +(purch / ci * 100).toFixed(2)    : '',
      cr_link_clicks_pct: lc    > 0 ? +(purch / lc * 100).toFixed(2)    : '',
      atc_lc_pct:         lc    > 0 ? +(atc / lc * 100).toFixed(2)      : '',
      ci_atc_pct:         atc   > 0 ? +(ci / atc * 100).toFixed(2)      : '',
      roas_ma:            spend > 0 ? +(conv / spend).toFixed(3)        : '',
      cost_per_ftewv:     ftewv > 0 ? +(spend / ftewv).toFixed(2)       : '',
      cost_per_ncp:       ncp   > 0 ? +(spend / ncp).toFixed(2)         : '',
    };

    for (const h of R30_HEADERS) {
      if (h === 'date') { row.date = d.date; continue; }
      if (h === 'asset_id') { row.asset_id = assets[d.ad_id] || ''; continue; }
      if (h === 'utm_content_top' || h === 'utm_term_top' || h === 'utm_campaign_top'
          || h === 'matched_value_top' || h === 'matched_tier_top') {
        row[h] = utm[h] || ''; continue;
      }
      if (h === 'shop_minus_meta' || h === 'shop_vs_meta_pct') continue;  // sheet formulas
      if (derived.hasOwnProperty(h)) { row[h] = derived[h]; continue; }
      if (R30_DAILY_OVERRIDES.has(h)) {
        const dailyKey = R30_HEADER_TO_DAILY[h] || h;
        row[h] = (d[dailyKey] !== undefined && d[dailyKey] !== null) ? d[dailyKey] : '';
      } else {
        row[h] = (life[h] !== undefined && life[h] !== null) ? life[h] : '';
      }
    }
    out.push(row);
  }
  return out;
}

// ── Write ──────────────────────────────────────────────────────────────
function R30_write_(rows) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(R30_SHEET_NAME);
  if (!sh) sh = ss.insertSheet(R30_SHEET_NAME);
  sh.clearContents();
  sh.clearFormats();
  if (!rows.length) {
    sh.getRange(1, 1).setValue('No RCK daily rows for last 30 days.');
    return;
  }
  const nCols = R30_HEADERS.length;

  sh.getRange(1, 1, 1, nCols).setValues([R30_HEADERS])
    .setFontWeight('bold').setBackground('#f0f0f0');
  sh.setFrozenRows(1);
  sh.setFrozenColumns(7);

  const data = rows.map(function(r) {
    return R30_HEADERS.map(function(h) {
      const v = r[h];
      if (v === null || v === undefined) return '';
      if (typeof v === 'object') return JSON.stringify(v);
      return v;
    });
  });

  const CHUNK = 5000;
  for (let i = 0; i < data.length; i += CHUNK) {
    const slice = data.slice(i, i + CHUNK);
    sh.getRange(2 + i, 1, slice.length, nCols).setValues(slice);
  }

  // Filter
  const range = sh.getRange(1, 1, data.length + 1, nCols);
  const filter = sh.getFilter(); if (filter) filter.remove();
  range.createFilter();

  // Column widths
  for (let c = 1; c <= nCols; c++) sh.setColumnWidth(c, 110);
  const widen = (name, w) => { const i = R30_HEADERS.indexOf(name); if (i >= 0) sh.setColumnWidth(i + 1, w); };
  widen('date', 90);
  widen('ad_name', 260); widen('adset_name', 220); widen('campaign_name', 260);
  widen('asset_id', 130);
  widen('utm_content_top', 260); widen('utm_term_top', 180);
  widen('utm_campaign_top', 260); widen('matched_value_top', 260);

  // Native sheet formulas — shop_minus_meta + shop_vs_meta_pct
  const convIdx = R30_HEADERS.indexOf('conv_value');
  const shopIdx = R30_HEADERS.indexOf('shopify_sales');
  const diffIdx = R30_HEADERS.indexOf('shop_minus_meta');
  const pctIdx  = R30_HEADERS.indexOf('shop_vs_meta_pct');
  if (convIdx >= 0 && shopIdx >= 0) {
    const convCol = R30_colLetter_(convIdx + 1);
    const shopCol = R30_colLetter_(shopIdx + 1);
    const diffF = [], pctF = [];
    for (let i = 0; i < data.length; i++) {
      const r = i + 2;
      // diff = shop - conv always; pct guards against #DIV/0 with 0
      diffF.push([`=${shopCol}${r}-${convCol}${r}`]);
      pctF .push([`=IF(N(${convCol}${r})>0, (${shopCol}${r}-${convCol}${r})/${convCol}${r}*100, 0)`]);
    }
    if (diffIdx >= 0)
      sh.getRange(2, diffIdx + 1, data.length, 1).setFormulas(diffF)
        .setNumberFormat('#,##0.00;[Red]-#,##0.00;0');
    if (pctIdx >= 0)
      sh.getRange(2, pctIdx + 1, data.length, 1).setFormulas(pctF)
        .setNumberFormat('+0.0"%";[Red]-0.0"%";0.0"%"');
  }

  const foot = data.length + 2;
  sh.getRange(foot, 1).setValue('Refreshed at: ' + new Date().toISOString())
    .setFontStyle('italic').setFontColor('#666');
}

function R30_colLetter_(n) {
  let s = '';
  while (n > 0) { const r = (n - 1) % 26; s = String.fromCharCode(65 + r) + s; n = Math.floor((n - 1) / 26); }
  return s;
}
