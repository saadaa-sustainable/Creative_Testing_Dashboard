/**
 * Ad Intelligence — 30-day daywise breakdown.
 *
 * COEXISTS with ae_table_gsheets_apps_script.gs in the same Apps Script project.
 * All shared symbols (SHEET_NAME, PAGE_SIZE, HEADERS, getConfig_, fetchAssetMap_,
 * fetchUtmSummaryMap_, _get_, writeToSheet_, onOpen, uninstallTrigger) are
 * prefixed D30_ here to avoid duplicate-declaration errors, or reused directly
 * from the other file when the implementation is identical.
 *
 * ── MENU MERGE — REQUIRED ONCE ────────────────────────────────────────────
 * Open ae_table_gsheets_apps_script.gs and replace its onOpen() with:
 *
 *   function onOpen() {
 *     SpreadsheetApp.getUi()
 *       .createMenu('Ad Intel')
 *         .addItem('Refresh AE Table (ACTIVE only)',   'refreshAETable')
 *         .addSeparator()
 *         .addItem('Install hourly refresh trigger',   'installHourlyTrigger')
 *         .addItem('Uninstall refresh trigger',        'uninstallTrigger')
 *       .addToUi();
 *     SpreadsheetApp.getUi()
 *       .createMenu('Ad Intel Daily')
 *         .addItem('Refresh 30-day breakdown',         'refreshDailyTable')
 *         .addSeparator()
 *         .addItem('Install hourly refresh trigger',   'installDailyTrigger')
 *         .addItem('Uninstall hourly refresh trigger', 'D30_uninstallTrigger')
 *       .addToUi();
 *   }
 *
 * (Apps Script allows only one onOpen per project, so both menus must be
 * created inside a single onOpen function.)
 */

const D30_SHEET_NAME  = 'AE Table 30d Daily';
const D30_DAILY_TABLE = 'ae_daily_30d';
const D30_VIEW_NAME   = 'ae_table_view';   // for lifetime context

// Header order EXACTLY as requested. `date` is prepended as the first column
// since this is daywise data (essential context).
const D30_HEADERS = [
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

// Header → daily-column mapping.
// Header uses ae_table_view naming; ae_daily_30d uses primary_table naming for
// some fields. This maps between them.
const D30_HEADER_TO_DAILY = {
  'conv_value':      'conversion_value',
  // others below use the same name in both (impressions, reach, frequency,
  // amount_spent, purchases, link_clicks_raw, ci_count, atc_count,
  // engagement_count, ftewv_count, ncp_count, ltv_*, etc.)
};

// Which HEADER names should be pulled from the DAILY row (via mapping above
// when needed). Everything else falls back to lifetime.
const D30_DAILY_OVERRIDES = new Set([
  'date', 'ad_status',
  'impressions', 'reach', 'frequency', 'amount_spent',
  'conv_value', 'purchases',
  'link_clicks_raw', 'ci_count', 'atc_count', 'engagement_count',
  'ftewv_count', 'cost_per_ftewv', 'ncp_count', 'cost_per_ncp',
  'ltv_reach', 'ltv_frequency',
  'shopify_orders', 'shopify_sales', 'shopify_roas',
  // Derived rates — computed per day in D30_merge_ from raw daily counts:
  'cost_per_1000', 'cpc_link', 'ctr_pct',
  'checkout_compl_pct', 'cr_link_clicks_pct',
  'atc_lc_pct', 'ci_atc_pct', 'roas_ma'
]);

// ── Main ───────────────────────────────────────────────────────────────
// (No onOpen here — merge menu items into the lifetime script's onOpen.)
function refreshDailyTable() {
  const {url, key} = getConfig_();          // shared with lifetime script
  const t0 = Date.now();
  console.log('Fetching ae_table_view (lifetime context)…');
  const lifetime = D30_fetchLifetime_(url, key);
  console.log('Fetching ad_asset_ids…');
  const assets   = fetchAssetMap_(url, key);   // shared with lifetime script
  console.log('Fetching ae_daily_30d…');
  const daily    = D30_fetchDaily_(url, key);
  console.log(`Daily rows: ${daily.length}. Fetching UTM modes (from ad_utm_mode helper table)…`);
  const utmMap   = D30_fetchUtmModeMap_(url, key);   // single paginated read of the pre-aggregated table

  console.log('Merging + writing to sheet…');
  const rows = D30_merge_(daily, lifetime, assets, utmMap);
  D30_writeToSheet_(rows);

  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  SpreadsheetApp.getActiveSpreadsheet()
    .toast(`Wrote ${rows.length} rows (30-day daywise) in ${secs}s`, 'AE Daily', 6);
}

// ── Fetchers (unique to daily) ─────────────────────────────────────────
function D30_fetchLifetime_(url, key) {
  // NOTE: no ad_status filter here — ae_daily_30d includes ads that were
  // ACTIVE at some point in the window but may now be PAUSED / WITH_ISSUES /
  // CAMPAIGN_PAUSED / ARCHIVED. Filtering to ad_status=ACTIVE at fetch time
  // dropped 171 ads and left their lifetime columns blank in the sheet.
  const out = {};
  let offset = 0;
  const endpoint = url + '/rest/v1/' + D30_VIEW_NAME +
                   '?reporting_ends=gte.2025-01-01' +
                   '&select=*';
  while (true) {
    const resp = D30_get_(endpoint, key, offset);
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    for (const r of batch) if (r.ad_id) out[r.ad_id] = r;
    if (batch.length < 1000) break;   // page size
    offset += 1000;
    Utilities.sleep(50);
  }
  return out;
}

function D30_fetchDaily_(url, key) {
  const out = [];
  let offset = 0;
  const endpoint = url + '/rest/v1/' + D30_DAILY_TABLE +
                   '?select=*&order=ad_id.asc,date.asc';
  while (true) {
    const resp = D30_get_(endpoint, key, offset);
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    out.push.apply(out, batch);
    if (batch.length < 1000) break;
    offset += 1000;
    Utilities.sleep(50);
  }
  return out;
}

// Fetch pre-aggregated UTM mode per ad_id from public.ad_utm_mode.
// ~4,100 rows → 5 paginated requests. Returns { ad_id → row } keyed by the
// _top field names already used by D30_merge_.
function D30_fetchUtmModeMap_(url, key) {
  const out = {};
  let offset = 0;
  const endpoint = url + '/rest/v1/ad_utm_mode' +
                   '?select=ad_id,utm_content_top,utm_term_top,utm_campaign_top,matched_value_top,matched_tier_top';
  while (true) {
    const resp = D30_get_(endpoint, key, offset);
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

// Local page-fetcher; the lifetime script has its own _get_ but this is
// prefixed to avoid collision.
function D30_get_(baseEndpoint, key, offset) {
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

// ── Merge daily + lifetime + assets + utm ──────────────────────────────
// Rate formulas verified against ae_raw_view (lifetime) for one sample ad —
// use REACH for cost_per_1000 (matches Meta's CPM convention on ae_raw_view).
function D30_merge_(daily, lifetime, assets, utmMap) {
  const out = [];
  for (const d of daily) {
    const life = lifetime[d.ad_id] || {};
    const utm  = utmMap[d.ad_id]   || {};
    const row  = {};

    // Per-day raw counts (used for rate derivations below)
    const impr  = Number(d.impressions)      || 0;
    const reach = Number(d.reach)            || 0;
    const spend = Number(d.amount_spent)     || 0;
    const conv  = Number(d.conversion_value) || 0;
    const purch = Number(d.purchases)        || 0;
    const lc    = Number(d.link_clicks_raw)  || 0;
    const ci    = Number(d.ci_count)         || 0;
    const atc   = Number(d.atc_count)        || 0;
    const shop  = Number(d.shopify_sales)    || 0;

    // Per-day derived rates (blank when denominator is 0 to avoid "0"/"n/a" noise)
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

    for (const h of D30_HEADERS) {
      if (h === 'date') { row.date = d.date; continue; }
      if (h === 'asset_id') { row.asset_id = assets[d.ad_id] || ''; continue; }
      // D30_fetchUtmModeMap_ returns rows from public.ad_utm_mode whose
      // columns are already '_top'-suffixed — direct lookup, no strip needed.
      if (h === 'utm_content_top' || h === 'utm_term_top' || h === 'utm_campaign_top'
          || h === 'matched_value_top' || h === 'matched_tier_top') {
        row[h] = utm[h] || ''; continue;
      }
      if (h === 'shop_minus_meta' || h === 'shop_vs_meta_pct') continue; // computed below
      // Derived rate?
      if (derived.hasOwnProperty(h)) { row[h] = derived[h]; continue; }
      // Raw daily override — apply header→daily column mapping if needed
      if (D30_DAILY_OVERRIDES.has(h)) {
        const dailyKey = D30_HEADER_TO_DAILY[h] || h;
        row[h] = (d[dailyKey] !== undefined && d[dailyKey] !== null) ? d[dailyKey] : '';
      } else {
        // Lifetime field from ae_table_view
        row[h] = (life[h] !== undefined && life[h] !== null) ? life[h] : '';
      }
    }

    // shop_minus_meta / shop_vs_meta_pct are written as SHEET FORMULAS in
    // D30_writeToSheet_ (below) so they auto-recompute when conv_value or
    // shopify_sales cells change. Leave the merged row's placeholders empty here.
    row.shop_minus_meta  = '';
    row.shop_vs_meta_pct = '';
    out.push(row);
  }
  return out;
}

// ── Write (unique — different layout from lifetime script) ─────────────
function D30_writeToSheet_(rows) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(D30_SHEET_NAME);
  if (!sh) sh = ss.insertSheet(D30_SHEET_NAME);
  sh.clearContents();
  sh.clearFormats();
  if (!rows.length) {
    sh.getRange(1, 1).setValue('No rows returned.');
    return;
  }
  const nCols = D30_HEADERS.length;

  sh.getRange(1, 1, 1, nCols).setValues([D30_HEADERS])
    .setFontWeight('bold').setBackground('#f0f0f0');
  sh.setFrozenRows(1);
  sh.setFrozenColumns(7);

  const data = rows.map(function(r) {
    return D30_HEADERS.map(function(h) {
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

  // ── shop_minus_meta / shop_vs_meta_pct as native Sheet formulas ──
  // Same formula as Ads Analyse (dashboard.js:5452):
  //   diff  = shopify_sales - conv_value   (blank when conv=0)
  //   pct   = (shopify_sales - conv_value) / conv_value * 100
  const convIdx = D30_HEADERS.indexOf('conv_value');       // 0-based
  const shopIdx = D30_HEADERS.indexOf('shopify_sales');
  const diffIdx = D30_HEADERS.indexOf('shop_minus_meta');
  const pctIdx  = D30_HEADERS.indexOf('shop_vs_meta_pct');
  if (convIdx >= 0 && shopIdx >= 0 && (diffIdx >= 0 || pctIdx >= 0)) {
    const convCol = D30_colLetter_(convIdx + 1);
    const shopCol = D30_colLetter_(shopIdx + 1);
    const diffFormulas = [], pctFormulas = [];
    for (let i = 0; i < data.length; i++) {
      const r = i + 2;   // sheet row (1-based, +1 for header)
      const c = convCol + r;
      const s = shopCol + r;
      // diff = shop - conv always (well-defined even when conv=0)
      // pct  = (shop - conv)/conv * 100 when conv>0, else 0 (avoid #DIV/0)
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
  const widen = (name, w) => { const i = D30_HEADERS.indexOf(name); if (i >= 0) sh.setColumnWidth(i + 1, w); };
  widen('date', 90);
  widen('ad_name', 260); widen('adset_name', 220); widen('campaign_name', 220);
  widen('asset_id', 130);
  widen('utm_content_top', 260); widen('utm_term_top', 180);
  widen('utm_campaign_top', 260); widen('matched_value_top', 260);

  // Number formats — Sheets format string is 3 sections: positive;negative;zero.
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

// ── Triggers (unique names — install/uninstall the daily job only) ─────
// Runs hourly so the sheet auto-catches any DB refresh (pipeline runs
// overnight + daily). Google's time-based triggers fire server-side, so
// the sheet updates even when the user's laptop is asleep or off.
function installDailyTrigger() {
  D30_uninstallTrigger();
  ScriptApp.newTrigger('refreshDailyTable').timeBased().everyHours(1).create();
  SpreadsheetApp.getActiveSpreadsheet()
    .toast('Hourly refresh trigger installed.', 'AE Daily', 4);
}
function D30_uninstallTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'refreshDailyTable') ScriptApp.deleteTrigger(t);
  });
}

// ── Utility: 1-based col index → A1 letter (1→A, 27→AA, etc.) ──────────
function D30_colLetter_(n) {
  let s = '';
  while (n > 0) {
    const r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}
