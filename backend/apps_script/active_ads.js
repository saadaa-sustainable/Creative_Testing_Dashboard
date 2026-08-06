/**
 * Ad Intelligence — pull public.ae_table_view into this sheet.
 * Filter: ad_status = ACTIVE AND reporting_ends >= 2025-01-01
 * (aligned with the Shopify order window; ads with no 2025+ activity are
 *  excluded because they can't attribute to any in-scope order).
 * 68 native cols + 8 derived cols
 *   asset_id                                  (right after ad_name)
 *   shop_minus_meta, shop_vs_meta_pct         (right after shopify_sales)
 *   utm_content_top, utm_term_top,
 *   utm_campaign_top, matched_value_top,
 *   matched_tier_top                          (right after shopify_roas)
 * The utm/matched values are the MODE (most common) across each ad's
 * attributed Shopify orders, so you can see on what basis each ad was matched.
 *
 * ── Setup (one-time) ──────────────────────────────────────────────────────
 *  1. Extensions > Apps Script
 *  2. Paste this file. Save.
 *  3. Project settings (gear icon) > Script properties > Add script property:
 *        SUPABASE_URL   =  https://<your-project-ref>.supabase.co
 *        SUPABASE_KEY   =  <service_role key>   (Settings > API > service_role)
 *     Notes on key choice:
 *      - service_role bypasses RLS and can read the view without any config.
 *      - anon key works only if RLS on the underlying tables (ae_raw_view,
 *        ae_shopify_enriched, shopify_ad_agg) permits SELECT for anon.
 *      - Since Apps Script runs server-side, service_role is safe here — the
 *        key never leaves Google's servers.
 *  4. Reload the sheet. A new menu "Ad Intel" appears.
 *  5. Menu > Ad Intel > Refresh AE Table (ACTIVE only).
 *
 * ── Optional: auto-refresh ───────────────────────────────────────────────
 *  Menu > Ad Intel > Install hourly refresh trigger
 *  (Uninstalls any prior trigger; then re-schedules every hour.)
 */

const SHEET_NAME       = 'AE Table View';
const VIEW_NAME        = 'ae_table_view';
const PAGE_SIZE        = 1000;                 // PostgREST server-side max
const STATUS_FILTER    = 'eq.ACTIVE';           // ad_status must equal ACTIVE
// Only ads with activity on/after 2025-01-01 (aligned with Shopify order
// window — earlier ads can't be attributed to any in-scope order).
const WINDOW_START     = '2025-01-01';
const WINDOW_FIELD     = 'reporting_ends';      // last-active-date column on ae_table_view
const ORDER_BY         = 'amount_spent.desc.nullslast';

// ── Menu ───────────────────────────────────────────────────────────────
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Ad Intel')
      .addItem('Refresh AE Table (ACTIVE only)', 'refreshAETable')
      .addSeparator()
      .addItem('Install hourly refresh trigger', 'installHourlyTrigger')
      .addItem('Uninstall refresh trigger',      'uninstallTrigger')
    .addToUi();
  SpreadsheetApp.getUi()
    .createMenu('Ad Intel Daily')
      .addItem('Refresh 30-day breakdown',        'refreshDailyTable')
      .addSeparator()
      .addItem('Install daily refresh trigger',   'installDailyTrigger')
      .addItem('Uninstall daily refresh trigger', 'D30_uninstallTrigger')
    .addToUi();
  SpreadsheetApp.getUi()
    .createMenu('Ad Intel 90d')
      .addItem('Refresh 90-day breakdown',        'refreshDailyTable90')
      .addSeparator()
      .addItem('Install daily refresh trigger',   'installDailyTrigger90')
      .addItem('Uninstall daily refresh trigger', 'D90_uninstallTrigger')
    .addToUi();
  SpreadsheetApp.getUi()
    .createMenu('RCK Campaigns')
      .addItem('Refresh RCK sheet (L30)', 'refreshRckCampaigns')
      .addSeparator()
      .addItem('Refresh RCK daily (30d)', 'refreshRckDaily')
    .addToUi();
}

// ── Main entry ─────────────────────────────────────────────────────────
function refreshAETable() {
  const {url, key} = getConfig_();
  const t0 = Date.now();
  const rows = fetchAllActive_(url, key);
  const assetMap = fetchAssetMap_(url, key);
  const adIds = rows.map(r => String(r.ad_id)).filter(Boolean);
  const utmMap = fetchUtmSummaryMap_(url, key, adIds);
  enrichRows_(rows, assetMap, utmMap);
  writeToSheet_(rows);
  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  SpreadsheetApp.getActiveSpreadsheet()
    .toast(`Wrote ${rows.length} ACTIVE ads with activity ≥ ${WINDOW_START} to '${SHEET_NAME}' in ${secs}s`, 'AE Table', 6);
}

// ── Config ─────────────────────────────────────────────────────────────
function getConfig_() {
  const props = PropertiesService.getScriptProperties();
  const url = (props.getProperty('SUPABASE_URL') || '').replace(/\/$/, '');
  const key = props.getProperty('SUPABASE_KEY');
  if (!url || !key) {
    throw new Error('Missing SUPABASE_URL or SUPABASE_KEY in Script Properties. ' +
                    'Project settings ▸ Script properties.');
  }
  return {url, key};
}

// ── Fetch with pagination (Range header, PostgREST style) ──────────────
function fetchAllActive_(url, key) {
  const out = [];
  let offset = 0;
  const endpoint = url + '/rest/v1/' + VIEW_NAME +
                   '?ad_status=' + STATUS_FILTER +
                   '&' + WINDOW_FIELD + '=gte.' + WINDOW_START +
                   '&select=*' +
                   '&order=' + ORDER_BY;
  while (true) {
    const resp = UrlFetchApp.fetch(endpoint, {
      method: 'get',
      headers: {
        apikey:        key,
        Authorization: 'Bearer ' + key,
        Range:         offset + '-' + (offset + PAGE_SIZE - 1),
        'Range-Unit':  'items',
        Prefer:        'count=exact',
      },
      muteHttpExceptions: true,
    });
    const code = resp.getResponseCode();
    if (code !== 200 && code !== 206) {
      throw new Error('Supabase returned HTTP ' + code + ': ' +
                      resp.getContentText().slice(0, 500));
    }
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    out.push.apply(out, batch);
    if (batch.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    Utilities.sleep(50);   // be gentle
  }
  return out;
}

// ── Fetch ad_asset_ids into { ad_id: asset_id } map ────────────────────
function fetchAssetMap_(url, key) {
  const map = {};
  let offset = 0;
  const endpoint = url + '/rest/v1/ad_asset_ids?select=ad_id,asset_id';
  while (true) {
    const resp = UrlFetchApp.fetch(endpoint, {
      method: 'get',
      headers: {
        apikey:        key,
        Authorization: 'Bearer ' + key,
        Range:         offset + '-' + (offset + PAGE_SIZE - 1),
        'Range-Unit':  'items',
      },
      muteHttpExceptions: true,
    });
    const code = resp.getResponseCode();
    if (code !== 200 && code !== 206) {
      throw new Error('ad_asset_ids fetch HTTP ' + code + ': ' +
                      resp.getContentText().slice(0, 300));
    }
    const batch = JSON.parse(resp.getContentText());
    if (!batch || batch.length === 0) break;
    for (const r of batch) if (r.ad_id) map[r.ad_id] = r.asset_id || '';
    if (batch.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    Utilities.sleep(50);
  }
  return map;
}

// ── Fetch per-ad most-common utm_content / utm_term / utm_campaign +
//    representative matched_value + matched_tier from shopify_ad_attribution.
//    Batched by ad_id (100 per request) to stay under URL length limits.
function fetchUtmSummaryMap_(url, key, adIds) {
  const map = {};
  if (!adIds || !adIds.length) return map;
  // Google's UrlFetchApp URL cap is strict (BATCH=40 empirically overflowed).
  // 20 keeps the URL under ~700 chars total including host + query + encoding.
  const BATCH = 20;
  // Modes we're collecting per ad_id
  const counts = {};   // { ad_id: { utm_content:{v:n}, utm_term:{v:n}, utm_campaign:{v:n}, matched_value:{v:n}, matched_tier:{v:n} } }
  for (let i = 0; i < adIds.length; i += BATCH) {
    const chunk = adIds.slice(i, i + BATCH);
    const inList = '(' + chunk.map(a => '"' + a + '"').join(',') + ')';
    let offset = 0;
    while (true) {
      const endpoint = url + '/rest/v1/shopify_ad_attribution' +
                       '?ad_id=in.' + encodeURIComponent(inList) +
                       '&select=ad_id,utm_content,utm_term,utm_campaign,matched_value,matched_tier';
      const resp = UrlFetchApp.fetch(endpoint, {
        method: 'get',
        headers: {
          apikey:        key,
          Authorization: 'Bearer ' + key,
          Range:         offset + '-' + (offset + PAGE_SIZE - 1),
          'Range-Unit':  'items',
        },
        muteHttpExceptions: true,
      });
      const code = resp.getResponseCode();
      if (code !== 200 && code !== 206) {
        throw new Error('utm summary fetch HTTP ' + code + ' (chunk ' + i + '): ' +
                        resp.getContentText().slice(0, 300));
      }
      const batch = JSON.parse(resp.getContentText());
      if (!batch || batch.length === 0) break;
      for (const row of batch) {
        if (!row.ad_id) continue;
        const c = counts[row.ad_id] = counts[row.ad_id] ||
          {utm_content:{}, utm_term:{}, utm_campaign:{}, matched_value:{}, matched_tier:{}};
        for (const field of ['utm_content','utm_term','utm_campaign','matched_value','matched_tier']) {
          const v = row[field];
          if (v === null || v === undefined || v === '') continue;
          c[field][v] = (c[field][v] || 0) + 1;
        }
      }
      if (batch.length < PAGE_SIZE) break;
      offset += PAGE_SIZE;
      Utilities.sleep(50);
    }
  }
  // Reduce each ad's per-field counts to the mode (most common value)
  function mode(dict) {
    let best = ''; let bestN = 0;
    for (const v in dict) {
      if (dict[v] > bestN) { best = v; bestN = dict[v]; }
    }
    return best;
  }
  for (const ad_id in counts) {
    const c = counts[ad_id];
    map[ad_id] = {
      utm_content:   mode(c.utm_content),
      utm_term:      mode(c.utm_term),
      utm_campaign:  mode(c.utm_campaign),
      matched_value: mode(c.matched_value),
      matched_tier:  mode(c.matched_tier),
    };
  }
  return map;
}

// ── Enrich rows: add asset_id + shop_minus_meta + shop_vs_meta_pct + utm block ─
// Matches the guard used in Ads Analyse (dashboard.js:5452):
//   diff = conv > 0 ? shop - conv : null  →  displayed blank when Meta reports 0.
function enrichRows_(rows, assetMap, utmMap) {
  for (const r of rows) {
    // 1. asset_id (from ad_asset_ids)
    r.asset_id = assetMap[r.ad_id] || '';

    // 2/3. shop_minus_meta and shop_vs_meta_pct — both blank when conv_value = 0
    //      so the sheet matches Ads Analyse "—" behaviour.
    const shop = Number(r.shopify_sales) || 0;
    const conv = Number(r.conv_value)    || 0;
    if (conv > 0) {
      r.shop_minus_meta   = +(shop - conv).toFixed(2);
      r.shop_vs_meta_pct  = +(((shop - conv) / conv) * 100).toFixed(2);
    } else {
      r.shop_minus_meta  = '';
      r.shop_vs_meta_pct = '';
    }

    // 4. utm block — mode across this ad's attributed orders
    const u = (utmMap && utmMap[r.ad_id]) || {};
    r.utm_content_top   = u.utm_content   || '';
    r.utm_term_top      = u.utm_term      || '';
    r.utm_campaign_top  = u.utm_campaign  || '';
    r.matched_value_top = u.matched_value || '';
    r.matched_tier_top  = u.matched_tier  || '';
  }
}

// ── Write to sheet ─────────────────────────────────────────────────────
function writeToSheet_(rows) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) sh = ss.insertSheet(SHEET_NAME);
  sh.clearContents();
  sh.clearFormats();

  if (!rows.length) {
    sh.getRange(1, 1).setValue('No rows returned. Check ad_status filter or Supabase key.');
    return;
  }

  // Column order = keys of the first row (PostgREST preserves view column order).
  // Reorder so the derived cols land in a natural place:
  //   - asset_id           : right after ad_name
  //   - shop_minus_meta    : right after shopify_sales
  //   - shop_vs_meta_pct   : right after shop_minus_meta
  //   - utm_content_top    : right after shopify_roas
  //   - utm_term_top       : after utm_content_top
  //   - utm_campaign_top   : after utm_term_top
  //   - matched_value_top  : after utm_campaign_top
  //   - matched_tier_top   : after matched_value_top
  const DERIVED = new Set([
    'asset_id','shop_minus_meta','shop_vs_meta_pct',
    'utm_content_top','utm_term_top','utm_campaign_top',
    'matched_value_top','matched_tier_top'
  ]);
  const nativeKeys = Object.keys(rows[0]).filter(k => !DERIVED.has(k));
  const headers = [];
  for (const k of nativeKeys) {
    headers.push(k);
    if (k === 'ad_name')       headers.push('asset_id');
    if (k === 'shopify_sales') { headers.push('shop_minus_meta'); headers.push('shop_vs_meta_pct'); }
    if (k === 'shopify_roas')  {
      headers.push('utm_content_top');
      headers.push('utm_term_top');
      headers.push('utm_campaign_top');
      headers.push('matched_value_top');
      headers.push('matched_tier_top');
    }
  }
  const nCols = headers.length;

  // Write header
  sh.getRange(1, 1, 1, nCols).setValues([headers])
    .setFontWeight('bold').setBackground('#f0f0f0');
  sh.setFrozenRows(1);
  sh.setFrozenColumns(6);   // freeze account/campaign/adset/ad + names for scrolling

  // Serialize values (nulls → '', objects → JSON, ISO dates as strings)
  const data = rows.map(function(r) {
    return headers.map(function(h) {
      const v = r[h];
      if (v === null || v === undefined) return '';
      if (typeof v === 'object') return JSON.stringify(v);
      return v;
    });
  });

  sh.getRange(2, 1, data.length, nCols).setValues(data);

  // Add a filter for easy sort/search
  const range = sh.getRange(1, 1, data.length + 1, nCols);
  const existingFilter = sh.getFilter();
  if (existingFilter) existingFilter.remove();
  range.createFilter();

  // Reasonable column widths (don't autoResize — slow for 76 cols)
  for (let c = 1; c <= nCols; c++) sh.setColumnWidth(c, 120);
  const widen = (name, w) => { const i = headers.indexOf(name); if (i >= 0) sh.setColumnWidth(i + 1, w); };
  widen('ad_name',           260);
  widen('asset_id',          140);
  widen('shop_minus_meta',   140);
  widen('shop_vs_meta_pct',  140);
  widen('utm_content_top',   260);
  widen('utm_term_top',      180);
  widen('utm_campaign_top',  260);
  widen('matched_value_top', 260);
  widen('matched_tier_top',  120);
  const diffIdx = headers.indexOf('shop_minus_meta');
  const pctIdx  = headers.indexOf('shop_vs_meta_pct');

  // Number formats matching Ads Analyse — 3 sections in Sheets format:
  //   positive ; negative ; zero
  // Values are stored as plain numbers (percentage as 12.3, not 0.123), so
  // we append the literal '%' rather than using Sheets' percent format.
  if (diffIdx >= 0)
    sh.getRange(2, diffIdx + 1, data.length, 1)
      .setNumberFormat('#,##0.00;[Red]-#,##0.00;0');
  if (pctIdx >= 0)
    sh.getRange(2, pctIdx + 1, data.length, 1)
      .setNumberFormat('+0.0"%";[Red]-0.0"%";0.0"%"');

  // Append a footer with metadata
  const foot = data.length + 2;
  sh.getRange(foot, 1).setValue('Refreshed at: ' + new Date().toISOString());
  sh.getRange(foot, 1).setFontStyle('italic').setFontColor('#666');
}

// ── Triggers ───────────────────────────────────────────────────────────
function installHourlyTrigger() {
  uninstallTrigger();
  ScriptApp.newTrigger('refreshAETable').timeBased().everyHours(1).create();
  SpreadsheetApp.getActiveSpreadsheet()
    .toast('Hourly refresh trigger installed.', 'AE Table', 4);
}

function uninstallTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'refreshAETable') ScriptApp.deleteTrigger(t);
  });
}
