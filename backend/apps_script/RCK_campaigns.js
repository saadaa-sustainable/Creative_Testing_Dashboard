/**
 * RCK Campaigns — L30 aggregates for every ad in a campaign with 'RCK' in
 * the name. Includes paused / archived ads that had ANY delivery in the
 * last 30 days across all 3 accounts.
 *
 * Source: public.rck_last30 (rebuilt daily by _build_rck_last30.py). Adds
 * asset_id + utm/matched modes from the same helper tables the other
 * sheets use.
 *
 * ── MENU MERGE ────────────────────────────────────────────────────────
 * Add this line inside the existing onOpen() function alongside the
 * other Ad Intel menus:
 *
 *   SpreadsheetApp.getUi().createMenu('RCK Campaigns')
 *     .addItem('Refresh RCK sheet', 'refreshRckCampaigns').addToUi();
 *
 * (Apps Script allows only one onOpen per project.)
 */

const RCK_SHEET_NAME  = 'RCK_campaigns';
const RCK_TABLE       = 'rck_last30';

const RCK_HEADERS = [
  'account_name', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name',
  'ad_id', 'ad_name', 'asset_id', 'ad_status',
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

// ── Main ───────────────────────────────────────────────────────────────
function refreshRckCampaigns() {
  const {url, key} = getConfig_();   // shared helper from active_ads.js
  const t0 = Date.now();
  console.log('Fetching public.rck_last30 ...');
  const rows = RCK_fetch_(url, key);
  console.log(`RCK rows: ${rows.length}. Writing to sheet ...`);
  RCK_writeToSheet_(rows);
  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  SpreadsheetApp.getActiveSpreadsheet()
    .toast(`Wrote ${rows.length} RCK ads (last 30 days) in ${secs}s`, 'RCK Campaigns', 6);
}

// ── Fetcher ────────────────────────────────────────────────────────────
function RCK_fetch_(url, key) {
  const out = [];
  let offset = 0;
  const endpoint = url + '/rest/v1/' + RCK_TABLE +
                   '?select=*&order=amount_spent.desc.nullslast';
  while (true) {
    const resp = UrlFetchApp.fetch(endpoint, {
      method:'get',
      headers:{
        apikey:        key,
        Authorization: 'Bearer ' + key,
        Range:         offset + '-' + (offset + 999),
        'Range-Unit':  'items',
      },
      muteHttpExceptions: true,
    });
    const code = resp.getResponseCode();
    if (code !== 200 && code !== 206) {
      throw new Error('HTTP ' + code + ' fetching rck_last30: ' +
                      resp.getContentText().slice(0, 300));
    }
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    out.push.apply(out, batch);
    if (batch.length < 1000) break;
    offset += 1000;
    Utilities.sleep(50);
  }
  return out;
}

// ── Writer ─────────────────────────────────────────────────────────────
function RCK_writeToSheet_(rows) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(RCK_SHEET_NAME);
  if (!sh) sh = ss.insertSheet(RCK_SHEET_NAME);
  sh.clearContents();
  sh.clearFormats();
  if (!rows.length) {
    sh.getRange(1, 1).setValue('No RCK ads found for last 30 days.');
    return;
  }
  const nCols = RCK_HEADERS.length;

  sh.getRange(1, 1, 1, nCols).setValues([RCK_HEADERS])
    .setFontWeight('bold').setBackground('#f0f0f0');
  sh.setFrozenRows(1);
  sh.setFrozenColumns(7);

  const data = rows.map(function(r) {
    return RCK_HEADERS.map(function(h) {
      const v = r[h];
      if (v === null || v === undefined) return '';
      if (typeof v === 'object') return JSON.stringify(v);
      return v;
    });
  });

  const CHUNK = 2000;
  for (let i = 0; i < data.length; i += CHUNK) {
    const slice = data.slice(i, i + CHUNK);
    sh.getRange(2 + i, 1, slice.length, nCols).setValues(slice);
  }

  // Filter (rebuild if present)
  const range = sh.getRange(1, 1, data.length + 1, nCols);
  const filter = sh.getFilter(); if (filter) filter.remove();
  range.createFilter();

  // Column widths
  for (let c = 1; c <= nCols; c++) sh.setColumnWidth(c, 120);
  const widen = (name, w) => { const i = RCK_HEADERS.indexOf(name); if (i >= 0) sh.setColumnWidth(i + 1, w); };
  widen('ad_name', 260); widen('adset_name', 220); widen('campaign_name', 260);
  widen('asset_id', 130);
  widen('utm_content_top', 260); widen('utm_term_top', 180);
  widen('utm_campaign_top', 260); widen('matched_value_top', 260);

  // Native sheet formulas for shop_minus_meta / shop_vs_meta_pct so they
  // auto-recompute if any cell is edited. Same guard as Ads Analyse:
  // blank when conv_value = 0.
  const convIdx = RCK_HEADERS.indexOf('conv_value');
  const shopIdx = RCK_HEADERS.indexOf('shopify_sales');
  const diffIdx = RCK_HEADERS.indexOf('shop_minus_meta');
  const pctIdx  = RCK_HEADERS.indexOf('shop_vs_meta_pct');
  if (convIdx >= 0 && shopIdx >= 0) {
    const convCol = RCK_colLetter_(convIdx + 1);
    const shopCol = RCK_colLetter_(shopIdx + 1);
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

  // Footer
  const foot = data.length + 2;
  sh.getRange(foot, 1).setValue('Refreshed at: ' + new Date().toISOString())
    .setFontStyle('italic').setFontColor('#666');
}

// A1 col letter (1→A, 27→AA)
function RCK_colLetter_(n) {
  let s = '';
  while (n > 0) { const r = (n - 1) % 26; s = String.fromCharCode(65 + r) + s; n = Math.floor((n - 1) / 26); }
  return s;
}
