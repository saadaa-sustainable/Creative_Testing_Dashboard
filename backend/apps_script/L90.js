/**
 * Ad Intelligence — 90-day daywise breakdown.
 *
 * MIRROR of L30.js — same headers, same merge logic, same fetchers, but
 * pointed at public.ae_daily_90d and writing to a separate sheet tab.
 * All symbols prefixed D90_ to coexist with L30.js (D30_*) and the
 * lifetime script in the same Apps Script project.
 *
 * ── MENU MERGE — REQUIRED ONCE ────────────────────────────────────────────
 * Open active_ads.js (or ae_table_gsheets_apps_script.gs — whichever holds
 * the project's onOpen) and add a third createMenu block:
 *
 *   SpreadsheetApp.getUi()
 *     .createMenu('Ad Intel 90d')
 *       .addItem('Refresh 90-day breakdown',        'refreshDailyTable90')
 *       .addSeparator()
 *       .addItem('Install daily refresh trigger',   'installDailyTrigger90')
 *       .addItem('Uninstall daily refresh trigger', 'D90_uninstallTrigger')
 *     .addToUi();
 *
 * (Apps Script allows only one onOpen per project, so all menus must be
 * created inside that single onOpen function.)
 */

const D90_SHEET_NAME  = 'AE Table 90d Daily';
const D90_DAILY_TABLE = 'ae_daily_90d';
const D90_VIEW_NAME   = 'ae_table_view';   // for lifetime context

// Header order — identical to L30 so the two tabs are directly comparable.
const D90_HEADERS = [
  'date',
  'account_name', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name',
  'asset_id',
  'ad_created', 'first_seen_date', 'reporting_starts', 'reporting_ends',
  'date_target_imp_achieved', 'date_of_result',
  'days_to_result', 'days_to_target_f1',
  'ad_status',
  'f1_pass', 'f2_pass', 'f3_pass', 'f4_pass',
  'impressions', 'reach', 'cost_per_1000', 'frequency', 'amount_spent',
  'cpc_link', 'ctr_pct', 'checkout_compl_pct', 'cr_link_clicks_pct',
  'atc_lc_pct', 'ci_atc_pct', 'roas_ma',
  'cost_per_ftewv', 'ftewv_count', 'cost_per_ncp', 'ncp_count',
  'ltv_reach', 'ltv_frequency', 'engagement_count',
  'preview_link', 'ad_link', 'refreshed_at',
  'conv_value', 'purchases', 'link_clicks_raw', 'ci_count', 'atc_count', 'source',
  'reach_weight_pct', 'pct_reach_ftewv', 'profit_efficiency', 'contrib_margin_pct',
  'cpr_eff', 'ftv_contrib_eff', 'ftev_volume', 'ncp_cost_eff', 'roas_eff',
  'profit_vol_eff', 'delivery_eff', 'sales_spend_eff', 'blended_eff', 'category',
  'shopify_orders', 'shopify_sales', 'shop_minus_meta', 'shop_vs_meta_pct',
  'shopify_aov', 'shopify_first_order', 'shopify_last_order', 'shopify_top_tier',
  'shopify_roas',
  'utm_content_top', 'utm_term_top', 'utm_campaign_top',
  'matched_value_top', 'matched_tier_top'
];

const D90_HEADER_TO_DAILY = {
  'conv_value': 'conversion_value',
};

const D90_DAILY_OVERRIDES = new Set([
  'date', 'ad_status',
  'impressions', 'reach', 'frequency', 'amount_spent',
  'conv_value', 'purchases',
  'link_clicks_raw', 'ci_count', 'atc_count', 'engagement_count',
  'ftewv_count', 'cost_per_ftewv', 'ncp_count', 'cost_per_ncp',
  'ltv_reach', 'ltv_frequency',
  'shopify_orders', 'shopify_sales', 'shopify_roas',
  'cost_per_1000', 'cpc_link', 'ctr_pct',
  'checkout_compl_pct', 'cr_link_clicks_pct',
  'atc_lc_pct', 'ci_atc_pct', 'roas_ma'
]);

// ── Main ───────────────────────────────────────────────────────────────
function refreshDailyTable90() {
  const {url, key} = getConfig_();
  const t0 = Date.now();
  console.log('Fetching ae_table_view (lifetime context)…');
  const lifetime = D90_fetchLifetime_(url, key);
  console.log('Fetching ad_asset_ids…');
  const assets   = fetchAssetMap_(url, key);
  console.log('Fetching ae_daily_90d…');
  const daily    = D90_fetchDaily_(url, key);
  console.log(`Daily rows: ${daily.length}. Fetching UTM modes…`);
  const utmMap   = D90_fetchUtmModeMap_(url, key);

  console.log('Merging + writing to sheet…');
  const rows = D90_merge_(daily, lifetime, assets, utmMap);
  D90_writeToSheet_(rows);

  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  SpreadsheetApp.getActiveSpreadsheet()
    .toast(`Wrote ${rows.length} rows (90-day daywise) in ${secs}s`, 'AE 90d', 6);
}

// ── Fetchers ───────────────────────────────────────────────────────────
function D90_fetchLifetime_(url, key) {
  const out = {};
  let offset = 0;
  const endpoint = url + '/rest/v1/' + D90_VIEW_NAME +
                   '?reporting_ends=gte.2025-01-01' +
                   '&select=*';
  while (true) {
    const resp = D90_get_(endpoint, key, offset);
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    for (const r of batch) if (r.ad_id) out[r.ad_id] = r;
    if (batch.length < 1000) break;
    offset += 1000;
    Utilities.sleep(50);
  }
  return out;
}

function D90_fetchDaily_(url, key) {
  const out = [];
  let offset = 0;
  const endpoint = url + '/rest/v1/' + D90_DAILY_TABLE +
                   '?select=*&order=ad_id.asc,date.asc';
  while (true) {
    const resp = D90_get_(endpoint, key, offset);
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    out.push.apply(out, batch);
    if (batch.length < 1000) break;
    offset += 1000;
    Utilities.sleep(50);
  }
  return out;
}

function D90_fetchUtmModeMap_(url, key) {
  const out = {};
  let offset = 0;
  const endpoint = url + '/rest/v1/ad_utm_mode' +
                   '?select=ad_id,utm_content_top,utm_term_top,utm_campaign_top,matched_value_top,matched_tier_top';
  while (true) {
    const resp = D90_get_(endpoint, key, offset);
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    for (const r of batch) if (r.ad_id) out[r.ad_id] = r;
    if (batch.length < 1000) break;
    offset += 1000;
    Utilities.sleep(50);
  }
  console.log(`  ad_utm_mode rows loaded: ${Object.keys(out).length}`);
  return out;
}

function D90_get_(baseEndpoint, key, offset) {
  const resp = UrlFetchApp.fetch(baseEndpoint, {
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
    throw new Error('HTTP ' + code + ' at ' + baseEndpoint.slice(0, 120) + ' → ' +
                    resp.getContentText().slice(0, 300));
  }
  return resp;
}

// ── Merge ──────────────────────────────────────────────────────────────
function D90_merge_(daily, lifetime, assets, utmMap) {
  const out = [];
  for (const d of daily) {
    const life = lifetime[d.ad_id] || {};
    const utm  = utmMap[d.ad_id]   || {};
    const row  = {};

    const impr  = Number(d.impressions)      || 0;
    const reach = Number(d.reach)            || 0;
    const spend = Number(d.amount_spent)     || 0;
    const conv  = Number(d.conversion_value) || 0;
    const purch = Number(d.purchases)        || 0;
    const lc    = Number(d.link_clicks_raw)  || 0;
    const ci    = Number(d.ci_count)         || 0;
    const atc   = Number(d.atc_count)        || 0;

    const derived = {
      cost_per_1000:      reach > 0 ? +(spend / reach * 1000).toFixed(2) : '',
      cpc_link:           lc    > 0 ? +(spend / lc).toFixed(2)          : '',
      ctr_pct:            impr  > 0 ? +(lc / impr * 100).toFixed(2)     : '',
      checkout_compl_pct: ci    > 0 ? +(purch / ci * 100).toFixed(2)    : '',
      cr_link_clicks_pct: lc    > 0 ? +(purch / lc * 100).toFixed(2)    : '',
      atc_lc_pct:         lc    > 0 ? +(atc / lc * 100).toFixed(2)      : '',
      ci_atc_pct:         atc   > 0 ? +(ci / atc * 100).toFixed(2)      : '',
      roas_ma:            spend > 0 ? +(conv / spend).toFixed(3)        : '',
    };

    for (const h of D90_HEADERS) {
      if (h === 'date') { row.date = d.date; continue; }
      if (h === 'asset_id') { row.asset_id = assets[d.ad_id] || ''; continue; }
      if (h === 'utm_content_top' || h === 'utm_term_top' || h === 'utm_campaign_top'
          || h === 'matched_value_top' || h === 'matched_tier_top') {
        row[h] = utm[h] || ''; continue;
      }
      if (h === 'shop_minus_meta' || h === 'shop_vs_meta_pct') continue;
      if (derived.hasOwnProperty(h)) { row[h] = derived[h]; continue; }
      if (D90_DAILY_OVERRIDES.has(h)) {
        const dailyKey = D90_HEADER_TO_DAILY[h] || h;
        row[h] = (d[dailyKey] !== undefined && d[dailyKey] !== null) ? d[dailyKey] : '';
      } else {
        row[h] = (life[h] !== undefined && life[h] !== null) ? life[h] : '';
      }
    }

    row.shop_minus_meta  = '';
    row.shop_vs_meta_pct = '';
    out.push(row);
  }
  return out;
}

// ── Write ──────────────────────────────────────────────────────────────
function D90_writeToSheet_(rows) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(D90_SHEET_NAME);
  if (!sh) sh = ss.insertSheet(D90_SHEET_NAME);
  sh.clearContents();
  sh.clearFormats();
  if (!rows.length) {
    sh.getRange(1, 1).setValue('No rows returned.');
    return;
  }
  const nCols = D90_HEADERS.length;

  sh.getRange(1, 1, 1, nCols).setValues([D90_HEADERS])
    .setFontWeight('bold').setBackground('#f0f0f0');
  sh.setFrozenRows(1);
  sh.setFrozenColumns(7);

  const data = rows.map(function(r) {
    return D90_HEADERS.map(function(h) {
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

  const convIdx = D90_HEADERS.indexOf('conv_value');
  const shopIdx = D90_HEADERS.indexOf('shopify_sales');
  const diffIdx = D90_HEADERS.indexOf('shop_minus_meta');
  const pctIdx  = D90_HEADERS.indexOf('shop_vs_meta_pct');
  if (convIdx >= 0 && shopIdx >= 0 && (diffIdx >= 0 || pctIdx >= 0)) {
    const convCol = D90_colLetter_(convIdx + 1);
    const shopCol = D90_colLetter_(shopIdx + 1);
    const diffFormulas = [], pctFormulas = [];
    for (let i = 0; i < data.length; i++) {
      const r = i + 2;
      const c = convCol + r;
      const s = shopCol + r;
      diffFormulas.push([`=${s}-${c}`]);
      pctFormulas .push([`=IF(N(${c})>0, (${s}-${c})/${c}*100, 0)`]);
    }
    if (diffIdx >= 0)
      sh.getRange(2, diffIdx + 1, data.length, 1).setFormulas(diffFormulas);
    if (pctIdx >= 0)
      sh.getRange(2, pctIdx + 1, data.length, 1).setFormulas(pctFormulas);
  }

  const range = sh.getRange(1, 1, data.length + 1, nCols);
  const filter = sh.getFilter(); if (filter) filter.remove();
  range.createFilter();

  for (let c = 1; c <= nCols; c++) sh.setColumnWidth(c, 110);
  const widen = (name, w) => { const i = D90_HEADERS.indexOf(name); if (i >= 0) sh.setColumnWidth(i + 1, w); };
  widen('date', 90);
  widen('ad_name', 260); widen('adset_name', 220); widen('campaign_name', 220);
  widen('asset_id', 130);
  widen('utm_content_top', 260); widen('utm_term_top', 180);
  widen('utm_campaign_top', 260); widen('matched_value_top', 260);

  if (diffIdx >= 0)
    sh.getRange(2, diffIdx + 1, data.length, 1)
      .setNumberFormat('#,##0.00;[Red]-#,##0.00;0');
  if (pctIdx >= 0)
    sh.getRange(2, pctIdx + 1, data.length, 1)
      .setNumberFormat('+0.0"%";[Red]-0.0"%";0.0"%"');

  const foot = data.length + 2;
  sh.getRange(foot, 1).setValue('Refreshed at: ' + new Date().toISOString())
    .setFontStyle('italic').setFontColor('#666');
}

// ── Triggers ───────────────────────────────────────────────────────────
function installDailyTrigger90() {
  D90_uninstallTrigger();
  ScriptApp.newTrigger('refreshDailyTable90').timeBased().everyDays(1).atHour(4).create();
  SpreadsheetApp.getActiveSpreadsheet()
    .toast('Daily 4am refresh trigger installed.', 'AE 90d', 4);
}
function D90_uninstallTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'refreshDailyTable90') ScriptApp.deleteTrigger(t);
  });
}

// ── Utility: 1-based col index → A1 letter (1→A, 27→AA, etc.) ──────────
function D90_colLetter_(n) {
  let s = '';
  while (n > 0) {
    const r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}
