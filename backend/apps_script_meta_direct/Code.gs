/**
 * ═══════════════════════════════════════════════════════════════════════
 * Meta Ads Direct — Google Sheet fetcher
 *
 * Fetches Meta Ads Insights directly from Meta's Graph API and writes to
 * four sheet tabs. No Supabase / no primary_sync — this is self-contained.
 *
 * ┌──────────────────────┬──────────────────────────────────────────────┐
 * │ Tab                  │ Content                                      │
 * ├──────────────────────┼──────────────────────────────────────────────┤
 * │ Active Ads 30d       │ ad_status=ACTIVE, last-30d totals per ad     │
 * │ Active Ads 90d       │ ad_status=ACTIVE, last-90d totals per ad     │
 * │ Daily Breakdown 30d  │ one row per (ad × date) for last 30 days     │
 * │ Daily Breakdown 90d  │ one row per (ad × date) for last 90 days     │
 * └──────────────────────┴──────────────────────────────────────────────┘
 *
 * ── SETUP (one-time) ────────────────────────────────────────────────────
 * 1. Extensions → Apps Script → paste this file. Save.
 * 2. Project Settings (gear icon) → Script Properties → add:
 *      META_ACCESS_TOKEN   =  <fresh long-lived token>
 *      META_API_VERSION    =  v22.0
 *      ACCOUNT_1_ID        =  1136644150469466   (Raho Saadaa)
 *      ACCOUNT_1_NAME      =  Raho Saadaa
 *      ACCOUNT_2_ID        =  1349767139294217   (Fourth Ad Account - SD)
 *      ACCOUNT_2_NAME      =  Fourth Ad Account - SD
 *      ACCOUNT_3_ID        =  264868699479122    (Third Ad Account - SD)
 *      ACCOUNT_3_NAME      =  Third Ad Account - SD
 * 3. Reload the sheet. Menu bar shows "Meta Direct" with four Refresh items.
 * 4. Menu → Meta Direct → Refresh Active Ads 30d  (etc.)
 *
 * ── AUTO-REFRESH (optional) ─────────────────────────────────────────────
 * Menu → Meta Direct → Install hourly triggers  —  fires all four
 * refreshes hourly on Google's servers (independent of laptop state).
 *
 * Menu → Meta Direct → Install nightly triggers (5–7 AM IST) — fires
 * once per day AFTER the backend Python orchestrator finishes rebuilding
 * primary_table + shopify_ad_attribution + ae_table_view. Use this when
 * you want the order-mapped shopify_orders / utm_* columns to always be
 * fresh (hourly triggers pull mid-refresh and get inconsistent state).
 * Requires the Apps Script project timezone to be set to Asia/Kolkata.
 * ═══════════════════════════════════════════════════════════════════════
 */

// ── Config ──────────────────────────────────────────────────────────────
function getConfig_() {
  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('META_ACCESS_TOKEN');
  if (!token) throw new Error('Missing META_ACCESS_TOKEN in Script Properties.');
  const version = (props.getProperty('META_API_VERSION') || 'v22.0').trim();
  const accounts = [];
  for (let i = 1; i <= 5; i++) {
    const id   = props.getProperty('ACCOUNT_' + i + '_ID');
    const name = props.getProperty('ACCOUNT_' + i + '_NAME') || ('Account ' + i);
    if (id) accounts.push({id: id.trim(), name: name.trim()});
  }
  if (!accounts.length) throw new Error('No ACCOUNT_*_ID in Script Properties.');
  // Supabase last-click UTM attribution — optional. If SUPABASE_URL +
  // SUPABASE_ANON are set, we fetch shopify_ad_attribution and merge
  // per-ad orders / sales into the sheet. If unset, the shopify columns
  // stay empty and everything else works fine.
  const supUrl  = (props.getProperty('SUPABASE_URL')  || '').replace(/\/$/, '');
  const supAnon = (props.getProperty('SUPABASE_ANON') || '').trim();
  return {token: token.trim(), version: version, accounts: accounts,
          base: 'https://graph.facebook.com/' + version,
          supUrl: supUrl, supAnon: supAnon};
}

// ── Menu ────────────────────────────────────────────────────────────────
// Daily 90d must be split PER ACCOUNT — the whole-account Raho fetch
// (~15 min under Meta throttle) blows past Apps Script's 6-min per-function
// ceiling. Splitting by account also lets each menu item write to the
// same sheet tab additively (append mode, keyed by ad_id+date).
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Meta Direct')
      .addItem('Refresh Active Ads 30d',           'refreshActive30')
      .addItem('Refresh Active Ads 90d',           'refreshActive90')
      .addItem('Refresh Daily Breakdown 30d',      'refreshDaily30')
      .addSeparator()
      .addItem('Daily 90d — ALL accounts (fast)',  'refreshDaily90All')
      .addItem('Daily 90d — Raho only',            'refreshDaily90Raho')
      .addItem('Daily 90d — Fourth SD only',       'refreshDaily90Fourth')
      .addItem('Daily 90d — Third SD only',        'refreshDaily90Third')
      .addSeparator()
      .addItem('Refresh ALL four tabs',            'refreshAllTabs')
      .addSeparator()
      .addSubMenu(SpreadsheetApp.getUi().createMenu('Enrich existing tab (UTMs + status)')
        .addItem('Enrich Active Ads 30d',       'enrichActive30')
        .addItem('Enrich Active Ads 90d',       'enrichActive90')
        .addItem('Enrich Daily Breakdown 30d',  'enrichDaily30')
        .addItem('Enrich Daily Breakdown 90d',  'enrichDaily90'))
      .addSeparator()
      .addItem('Diagnose accounts (ping each)',    'diagnoseAccounts')
      .addSeparator()
      .addItem('Install nightly triggers (5–7 AM IST)', 'installNightlyTriggers')
      .addItem('Install hourly triggers',          'installHourlyTriggers')
      .addItem('Uninstall triggers',               'uninstallAllTriggers')
    .addToUi();
}

// ── Diagnose: hit each account with a minimal /act_<id>?fields=name call ──
// This surfaces token/permission/id issues in seconds without pulling any
// Insights data. Writes results to a "Diagnostics" sheet tab AND toasts a
// summary.
function diagnoseAccounts() {
  const cfg = getConfig_();
  const ss  = SpreadsheetApp.getActiveSpreadsheet();
  const sh  = ss.getSheetByName('Diagnostics') || ss.insertSheet('Diagnostics');
  sh.clearContents(); sh.clearFormats();
  sh.getRange(1, 1, 1, 5).setValues([['name','account_id','http','result','details']])
    .setFontWeight('bold').setBackground('#f0f0f0');
  sh.setFrozenRows(1);

  const results = [];
  for (const acct of cfg.accounts) {
    const url = cfg.base + '/act_' + acct.id +
                '?fields=name,account_status,disable_reason' +
                '&access_token=' + encodeURIComponent(cfg.token);
    const t0 = Date.now();
    let http = 0, ok = false, detail = '';
    try {
      const r = UrlFetchApp.fetch(url, {method: 'get', muteHttpExceptions: true});
      http = r.getResponseCode();
      const body = r.getContentText().slice(0, 400);
      if (http === 200) {
        ok = true;
        const j = JSON.parse(body);
        detail = 'name="' + (j.name || '?') + '"  status=' +
                 (j.account_status || '?');
      } else {
        detail = body;
      }
    } catch (e) {
      detail = 'exception: ' + e.message;
    }
    const secs = ((Date.now() - t0) / 1000).toFixed(1);
    results.push([acct.name, acct.id, http, ok ? 'OK ('+secs+'s)' : 'FAILED', detail]);
    Utilities.sleep(500);
  }

  sh.getRange(2, 1, results.length, 5).setValues(results);
  for (let i = 0; i < results.length; i++) {
    const ok = results[i][3].indexOf('OK') === 0;
    sh.getRange(i + 2, 4).setFontColor(ok ? '#0a7d2c' : '#c0392b')
      .setFontWeight('bold');
  }
  sh.setColumnWidth(1, 220); sh.setColumnWidth(2, 180);
  sh.setColumnWidth(3, 60);  sh.setColumnWidth(4, 100);
  sh.setColumnWidth(5, 520);

  const failed = results.filter(r => r[3] !== '').filter(r => r[3].indexOf('OK') !== 0);
  const msg = failed.length
    ? 'Diagnostics: ' + failed.length + ' account(s) FAILED — ' +
      failed.map(r => r[0]).join(', ')
    : 'Diagnostics: all ' + results.length + ' accounts OK';
  ss.toast(msg, 'Meta Direct', 8);
  ss.setActiveSheet(sh);
}

// ── Public actions (menu handlers) ──────────────────────────────────────
function refreshActive30() { runOne_('Active Ads 30d',       30, false); }
function refreshActive90() { runOne_('Active Ads 90d',       90, false); }
function refreshDaily30()  { runOne_('Daily Breakdown 30d',  30, true);  }

// Daily 90d — three per-account variants to stay under Apps Script's
// 6-min ceiling. Each writes ONLY its own account's rows to the shared
// 'Daily Breakdown 90d' tab in append mode; the "ALL" variant clears
// the tab and runs all three back-to-back (only works if total time
// fits in 6 min, which is only true when Raho has <400 delivering ads).
function refreshDaily90Raho()   { runOne_('Daily Breakdown 90d', 90, true, 'Raho Saadaa',            /*append*/true); }
function refreshDaily90Fourth() { runOne_('Daily Breakdown 90d', 90, true, 'Fourth Ad Account - SD', /*append*/true); }
function refreshDaily90Third()  { runOne_('Daily Breakdown 90d', 90, true, 'Third Ad Account - SD',  /*append*/true); }
function refreshDaily90All()    { runOne_('Daily Breakdown 90d', 90, true); }

function refreshAllTabs() {
  refreshActive30(); refreshActive90(); refreshDaily30(); refreshDaily90All();
}

// Standalone enrich — overlays the 6 UTM/status columns onto an already-
// populated tab without re-fetching Meta. Cheap: ~5-10s per tab.
function enrichActive30() { enrichExistingTab_('Active Ads 30d',      30, false); }
function enrichActive90() { enrichExistingTab_('Active Ads 90d',      90, false); }
function enrichDaily30()  { enrichExistingTab_('Daily Breakdown 30d', 30, true);  }
function enrichDaily90()  { enrichExistingTab_('Daily Breakdown 90d', 90, true);  }

// ── Core: fetch → write ─────────────────────────────────────────────────
// `accountFilter` (optional) restricts to a single account by name — used
// by the Daily 90d per-account menu items.
// `append` (optional) skips clearing the sheet, so multiple per-account
// runs stack rows in the same tab.
function runOne_(sheetName, days, daily, accountFilter, append) {
  const cfg = getConfig_();
  const t0 = Date.now();
  const {since, until} = window_(days);
  const scope = accountFilter ? cfg.accounts.filter(a => a.name === accountFilter) : cfg.accounts;
  console.log(sheetName + '  ' + since + ' → ' + until + '  daily=' + daily +
              '  accts=' + scope.map(a => a.name).join(',') + '  append=' + !!append);

  const allRows = [];
  // Per-account status — surfaced in the sheet footer so failures are visible
  // without opening Apps Script → Executions.
  const perAcct = [];
  for (const acct of scope) {
    console.log('  fetching ' + acct.name + ' (act_' + acct.id + ')');
    const at0 = Date.now();
    try {
      const rows = fetchInsights_(cfg, acct, since, until, daily);
      const secs = ((Date.now() - at0) / 1000).toFixed(1);
      const bailed = !!rows._budgetBailed;
      const suffix = bailed ? ' [BUDGET-BAILED: re-run to get remaining chunks]' : '';
      console.log('    ' + acct.name + ': ' + rows.length + ' rows (' + secs + 's)' + suffix);
      allRows.push.apply(allRows, rows);
      perAcct.push({name: acct.name, id: acct.id, rows: rows.length,
                    secs: secs, error: bailed ? 'PARTIAL: Apps Script 6-min budget hit — re-run to fill missing chunks' : null});
    } catch (e) {
      const secs = ((Date.now() - at0) / 1000).toFixed(1);
      const safeMsg = _redactToken_(e && e.message ? e.message : String(e));
      console.log('    [!] ' + acct.name + ' failed after ' + secs + 's: ' + safeMsg);
      perAcct.push({name: acct.name, id: acct.id, rows: 0,
                    secs: secs, error: safeMsg});
    }
  }

  // Last-click UTM attribution + ad status/created overlay (optional — needs SUPABASE_* props).
  //  · aggregated tabs: shopify_orders + shopify_sales per ad_id, plus most-recent order's UTMs
  //  · daily tabs:      shopify_orders + shopify_sales per (ad_id × date), plus latest UTMs of that day
  //  · ad_status + ad_created come from ae_table_view (keyed by ad_id only)
  if (cfg.supUrl && cfg.supAnon) {
    console.log('  fetching last-click UTM attribution + ad status from Supabase …');
    try {
      const attrib = fetchShopifyAttribution_(cfg, since, until, daily);
      const adIds  = Array.from(new Set(allRows.map(r => r.ad_id).filter(Boolean)));
      const statuses = fetchAdStatuses_(cfg, adIds);
      const key = daily ? (r => r.ad_id + '|' + r.date_start)
                        : (r => r.ad_id);
      let hits = 0, stHits = 0;
      for (const r of allRows) {
        const a = attrib[key(r)];
        if (a) {
          r.shopify_orders = a.orders; r.shopify_sales = a.sales; hits++;
          r.utm_term = a.utm_term; r.utm_content = a.utm_content;
          r.utm_campaign = a.utm_campaign; r.matched_tier = a.matched_tier;
        } else {
          r.shopify_orders = 0; r.shopify_sales = 0;
          r.utm_term = ''; r.utm_content = ''; r.utm_campaign = ''; r.matched_tier = '';
        }
        const st = statuses[r.ad_id];
        if (st) { r.ad_status = st.ad_status; r.ad_created = st.ad_created; stHits++; }
        else    { r.ad_status = ''; r.ad_created = ''; }
      }
      console.log('    attribution merged: ' + hits + ' / ' + allRows.length + ' rows; status merged: ' + stHits);
    } catch (e) {
      console.log('    [!] enrichment fetch failed: ' + e.message);
      for (const r of allRows) {
        r.shopify_orders = ''; r.shopify_sales = '';
        r.utm_term = ''; r.utm_content = ''; r.utm_campaign = ''; r.matched_tier = '';
        r.ad_status = ''; r.ad_created = '';
      }
    }
  } else {
    console.log('  SUPABASE_URL / SUPABASE_ANON not set — skipping enrichment overlay');
    for (const r of allRows) {
      r.shopify_orders = ''; r.shopify_sales = '';
      r.utm_term = ''; r.utm_content = ''; r.utm_campaign = ''; r.matched_tier = '';
      r.ad_status = ''; r.ad_created = '';
    }
  }

  // Sort so consecutive rows for the same ad are grouped together, with
  // dates ascending within each ad (mirrors AE Table 30d Daily's ORDER BY
  // ad_id.asc, date.asc). Aggregated tabs get ad_id-only sort for the same
  // stable-order-per-run guarantee.
  if (daily) {
    allRows.sort((a, b) => {
      if (a.ad_id !== b.ad_id) return a.ad_id < b.ad_id ? -1 : 1;
      return (a.date_start || '') < (b.date_start || '') ? -1 : 1;
    });
  } else {
    allRows.sort((a, b) => (a.ad_id || '') < (b.ad_id || '') ? -1 : 1);
  }

  writeSheet_(sheetName, allRows, daily, !!append, perAcct);
  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  try {
    const failed = perAcct.filter(p => p.error).map(p => p.name);
    const okMsg  = (append ? 'Appended ' : 'Wrote ') + allRows.length +
                   ' rows to ' + sheetName + ' in ' + secs + 's';
    const msg    = failed.length
      ? okMsg + ' — FAILED: ' + failed.join(', ') + ' (see footer for reason)'
      : okMsg;
    SpreadsheetApp.getActiveSpreadsheet().toast(msg, 'Meta Direct', 8);
  } catch (_) {}
}

// ── Supabase: last-click UTM attribution overlay ────────────────────────
// Reads public.shopify_ad_attribution (has_match=true) via PostgREST anon,
// aggregates client-side into a per-key bin:
//   {ad_id: {orders, sales, utm_term, utm_content, utm_campaign, matched_tier}}
//     for aggregated tabs; keyed by ad_id|YYYY-MM-DD for daily tabs.
// UTM fields hold the values from the MOST RECENT order in the bin
// (we page ordered by order_created_at DESC, so the first hit wins).
function fetchShopifyAttribution_(cfg, since, until, daily) {
  const out = {};
  const cols = 'ad_id,total_price,order_created_at,utm_term,utm_content,utm_campaign,matched_tier';
  const BATCH = 1000;
  let offset = 0;
  while (true) {
    const url = cfg.supUrl + '/rest/v1/shopify_ad_attribution?select=' + cols +
                '&has_match=eq.true&ad_id=not.is.null' +
                '&order_created_at=gte.' + since + 'T00:00:00' +
                '&order_created_at=lte.' + until + 'T23:59:59' +
                '&order=order_created_at.desc';
    const resp = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: {apikey: cfg.supAnon, Authorization: 'Bearer ' + cfg.supAnon,
                Range: offset + '-' + (offset + BATCH - 1), 'Range-Unit': 'items'},
      muteHttpExceptions: true,
    });
    const code = resp.getResponseCode();
    if (code !== 200 && code !== 206) {
      throw new Error('Supabase HTTP ' + code + ': ' + resp.getContentText().slice(0, 200));
    }
    const chunk = JSON.parse(resp.getContentText() || '[]');
    if (!chunk.length) break;
    for (const row of chunk) {
      if (!row.ad_id) continue;
      const price = +row.total_price || 0;
      let key;
      if (daily) {
        const dt = String(row.order_created_at || '').slice(0, 10);
        key = row.ad_id + '|' + dt;
      } else {
        key = row.ad_id;
      }
      let bin = out[key];
      if (!bin) {
        // First row for this key — since sorted DESC, this is the most-recent
        // order. Capture its UTMs as the "last-click" values.
        bin = out[key] = {
          orders: 0, sales: 0,
          utm_term:     row.utm_term     || '',
          utm_content:  row.utm_content  || '',
          utm_campaign: row.utm_campaign || '',
          matched_tier: row.matched_tier || '',
        };
      }
      bin.orders += 1;
      bin.sales  += price;
    }
    if (chunk.length < BATCH) break;
    offset += BATCH;
    Utilities.sleep(100);
    if (offset > 300000) break;
  }
  console.log('    aggregated ' + Object.keys(out).length + ' bins from Supabase');
  return out;
}

// ── Supabase: ad_status + ad_created lookup from ae_table_view ──────────
// Batches ad_ids in `in.(...)` chunks (URL length caps out ~2000 chars).
function fetchAdStatuses_(cfg, adIds) {
  const out = {};
  if (!adIds || !adIds.length) return out;
  const IN_BATCH = 80;   // ~80 * 18-char ids + commas ≈ 1600 chars
  for (let i = 0; i < adIds.length; i += IN_BATCH) {
    const slice = adIds.slice(i, i + IN_BATCH);
    const url = cfg.supUrl + '/rest/v1/ae_table_view' +
                '?select=ad_id,ad_status,ad_created' +
                '&ad_id=in.(' + slice.join(',') + ')';
    const resp = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: {apikey: cfg.supAnon, Authorization: 'Bearer ' + cfg.supAnon},
      muteHttpExceptions: true,
    });
    const code = resp.getResponseCode();
    if (code !== 200) {
      console.log('    ae_table_view HTTP ' + code + ': ' + resp.getContentText().slice(0, 200));
      continue;
    }
    const rows = JSON.parse(resp.getContentText() || '[]');
    for (const r of rows) {
      out[r.ad_id] = {ad_status: r.ad_status || '', ad_created: r.ad_created || ''};
    }
    if (i + IN_BATCH < adIds.length) Utilities.sleep(80);
  }
  console.log('    ad_status/created rows: ' + Object.keys(out).length + ' / ' + adIds.length);
  return out;
}

// ── Standalone: overlay Shopify + UTM + status columns onto an existing tab ──
// Reads the ad_ids (and date, for daily tabs) already in the sheet, fetches
// enrichment from Supabase, writes 8 columns:
//     shopify_orders, shopify_sales,
//     ad_status, ad_created,
//     utm_term, utm_content, utm_campaign, matched_tier
// Idempotent — re-running overwrites values in place. Columns that already
// exist in the sheet (typically shopify_orders/shopify_sales from Refresh)
// are updated at their current position; new columns get appended.
// Does NOT re-fetch Meta, so it's cheap (~5-10s per tab).
function enrichExistingTab_(sheetName, days, daily) {
  const cfg = getConfig_();
  if (!cfg.supUrl || !cfg.supAnon) {
    throw new Error('Set SUPABASE_URL and SUPABASE_ANON in Script Properties first.');
  }
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(sheetName);
  if (!sh) throw new Error('Sheet tab not found: ' + sheetName);

  const lastCol = sh.getLastColumn();
  const lastRow = sh.getLastRow();
  if (lastRow < 2) throw new Error('Sheet has no data rows: ' + sheetName);

  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
  const idxAdId = headers.indexOf('ad_id');
  const idxDate = daily ? headers.indexOf('date') : -1;
  if (idxAdId < 0) throw new Error('ad_id column not found in ' + sheetName);
  if (daily && idxDate < 0) throw new Error('date column not found in ' + sheetName);

  const {since, until} = window_(days);
  console.log('Enrich ' + sheetName + '  window ' + since + ' → ' + until + '  daily=' + daily);

  const rng = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();
  const dataRows = rng.filter(r => {
    const v = r[idxAdId];
    return v !== '' && v != null && !String(v).startsWith('Refreshed');
  });
  const adIds = Array.from(new Set(dataRows.map(r => String(r[idxAdId]))));
  console.log('  ' + dataRows.length + ' data rows · ' + adIds.length + ' unique ads');

  // Fetch both enrichment sources
  const attrib   = fetchShopifyAttribution_(cfg, since, until, daily);
  const statuses = fetchAdStatuses_(cfg, adIds);

  // Column layout: shopify pair first (aligned with existing schema), then the six
  // status/UTM columns. For each, find existing header index or plan an append slot.
  const FIELDS = [
    'shopify_orders','shopify_sales',
    'ad_status','ad_created',
    'utm_term','utm_content','utm_campaign','matched_tier',
  ];
  const colFor = {};                  // field -> 1-based column
  const newFields = [];               // fields that need to be appended
  for (const f of FIELDS) {
    const at = headers.indexOf(f);
    if (at >= 0) colFor[f] = at + 1;
    else         newFields.push(f);
  }
  let appendStart = lastCol + 1;
  for (const f of newFields) {
    colFor[f] = appendStart++;
  }
  if (newFields.length) {
    sh.getRange(1, lastCol + 1, 1, newFields.length).setValues([newFields])
      .setFontWeight('bold').setBackground('#f0f0f0');
  }

  // Build per-field column arrays in the SAME row order as rng
  const arrs = {}; for (const f of FIELDS) arrs[f] = [];
  for (const r of rng) {
    const adId = String(r[idxAdId]);
    const isFooter = !adId || String(r[0]).startsWith('Refreshed');
    if (isFooter) {
      for (const f of FIELDS) arrs[f].push(['']);
      continue;
    }
    let key;
    if (daily) {
      const dv = r[idxDate];
      const dt = (dv instanceof Date)
        ? Utilities.formatDate(dv, 'GMT', 'yyyy-MM-dd')
        : String(dv).slice(0, 10);
      key = adId + '|' + dt;
    } else {
      key = adId;
    }
    const a  = attrib[key]   || {};
    const st = statuses[adId] || {};
    arrs.shopify_orders.push([a.orders != null ? a.orders : 0]);
    arrs.shopify_sales .push([a.sales  != null ? a.sales  : 0]);
    arrs.ad_status     .push([st.ad_status  || '']);
    arrs.ad_created    .push([st.ad_created || '']);
    arrs.utm_term      .push([a.utm_term     || '']);
    arrs.utm_content   .push([a.utm_content  || '']);
    arrs.utm_campaign  .push([a.utm_campaign || '']);
    arrs.matched_tier  .push([a.matched_tier || '']);
  }

  // Write each field one column at a time (avoids clobbering unrelated columns
  // when shopify_orders/shopify_sales aren't adjacent to the new ones)
  const CHUNK = 5000;
  for (const f of FIELDS) {
    const col = colFor[f];
    const data = arrs[f];
    for (let i = 0; i < data.length; i += CHUNK) {
      const slice = data.slice(i, i + CHUNK);
      sh.getRange(2 + i, col, slice.length, 1).setValues(slice);
    }
    sh.setColumnWidth(col, 140);
  }

  // Small nicety: count how many rows actually got matched (non-zero shopify_orders)
  const matched = arrs.shopify_orders.filter(v => v[0] && v[0] > 0).length;
  const stMatched = arrs.ad_status.filter(v => v[0] !== '').length;
  console.log('  enriched: ' + matched + ' rows with orders · ' + stMatched + ' rows with status');

  try {
    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Enriched ' + dataRows.length + ' rows in ' + sheetName +
      ' (matched: ' + matched + ' shopify, ' + stMatched + ' status)',
      'Meta Direct', 8);
  } catch (_) {}
}

// ── Meta Insights fetch with time-chunking + paging + retry ─────────────
// Meta rejects wide time_ranges + time_increment=1 with error subcode 1487534
// "too many rows". Chunk the DAILY variant into 7-day slices so each call
// stays small. The AGGREGATED variant returns just one row per ad — no
// chunking needed (chunking would create duplicate rows per ad).
//
// Time-budget awareness: Apps Script hard-kills any execution >6 minutes.
// We stamp t0 on entry and bail early with what we have when we cross 5m30s
// (330000 ms). Callers see partial data + a warning in the footer.
var APPS_SCRIPT_TIME_BUDGET_MS = 330000;   // 5m30s — leaves 30s for cleanup

function fetchInsights_(cfg, acct, since, until, daily) {
  const t0 = Date.now();
  const chunks = daily ? weeklyChunks_(since, until, 7) : [[since, until]];
  const out = [];
  var _budgetBail = false;
  for (let i = 0; i < chunks.length; i++) {
    if (Date.now() - t0 > APPS_SCRIPT_TIME_BUDGET_MS) {
      console.log('    [!] Apps Script time budget exhausted after ' +
                  ((Date.now() - t0) / 1000).toFixed(0) + 's — bailing with ' +
                  out.length + ' rows (' + (chunks.length - i) +
                  ' chunks unfetched)');
      _budgetBail = true;
      break;
    }
    const [cSince, cUntil] = chunks[i];
    if (chunks.length > 1) {
      console.log('    chunk ' + (i+1) + '/' + chunks.length +
                  ' ' + cSince + ' → ' + cUntil);
    }
    // Auto chunk-halving: on persistent error, split the chunk in half and
    // retry each half once. Meta's code=2 intermittent errors on Raho are
    // almost always query-size related — a 3.5-day chunk usually works when
    // a 7-day one doesn't. Depth capped at 2 (7→3.5→1.75 days) so we never
    // spiral into hundreds of tiny calls.
    fetchChunkWithHalving_(cfg, acct, cSince, cUntil, daily, out, /*depth*/ 0, t0);
    if (i + 1 < chunks.length) Utilities.sleep(800);
  }
  if (_budgetBail) out._budgetBailed = true;
  return out;
}

// Recursive helper: try a chunk; on skip, split in half and retry each half.
function fetchChunkWithHalving_(cfg, acct, since, until, daily, out, depth, t0) {
  if (t0 && Date.now() - t0 > APPS_SCRIPT_TIME_BUDGET_MS) return;
  const before = out.length;
  const outcome = fetchInsightsChunk_(cfg, acct, since, until, daily, out);
  const added = out.length - before;
  // Halve only when the chunk actually SKIPPED (had zero pages that succeeded).
  // A chunk that returned 0 rows because the account had no delivery in that
  // range would also skip — but that's fine, halving is cheap and returns
  // fast on empty ranges.
  if (outcome.skipped && added === 0 && depth < 2) {
    const dSince = new Date(since + 'T00:00:00Z');
    const dUntil = new Date(until + 'T00:00:00Z');
    const spanDays = Math.round((dUntil - dSince) / 86400000) + 1;
    if (spanDays >= 2) {
      const mid = new Date(dSince.getTime() +
                           Math.floor(spanDays / 2) * 86400000);
      const midEnd  = new Date(mid.getTime() - 86400000);
      const halfA_since = since;
      const halfA_until = Utilities.formatDate(midEnd, 'GMT', 'yyyy-MM-dd');
      const halfB_since = Utilities.formatDate(mid,    'GMT', 'yyyy-MM-dd');
      const halfB_until = until;
      console.log('    halving chunk (depth=' + (depth + 1) + '): ' +
                  halfA_since + '→' + halfA_until + ' + ' +
                  halfB_since + '→' + halfB_until);
      Utilities.sleep(500);
      fetchChunkWithHalving_(cfg, acct, halfA_since, halfA_until, daily, out, depth + 1, t0);
      Utilities.sleep(500);
      fetchChunkWithHalving_(cfg, acct, halfB_since, halfB_until, daily, out, depth + 1, t0);
    }
  }
}

function fetchInsightsChunk_(cfg, acct, since, until, daily, out) {
  const fields = [
    'ad_id','ad_name','adset_id','adset_name','campaign_id','campaign_name',
    'account_name','account_id','date_start','date_stop',
    'impressions','reach','frequency','spend',
    'inline_link_clicks','outbound_clicks','clicks',
    'ctr','cpc','cpm',
    'purchase_roas','actions','action_values',
    'video_thruplay_watched_actions','video_p100_watched_actions',
  ].join(',');

  const params = [
    'access_token=' + encodeURIComponent(cfg.token),
    'level=ad',
    'time_range=' + encodeURIComponent(JSON.stringify({since: since, until: until})),
    'fields=' + encodeURIComponent(fields),
    // ACTIVE-only. Broader filters (adding WITH_ISSUES / PAUSED /
    // CAMPAIGN_PAUSED / ADSET_PAUSED) doubled the per-call latency on Raho
    // and tripped Meta's HTTP 500 empty-body response — measured live via
    // _dry_run_meta_live.py: ACTIVE 11.6s → ACTIVE+ISSUES 24.4s → all 5
    // statuses HTTP 500. User wants ACTIVE ads only anyway.
    'filtering=' + encodeURIComponent(JSON.stringify([
      {field:'ad.effective_status', operator:'IN', value:['ACTIVE']}
    ])),
    // limit=250 (was 500). Smaller pages = smaller Meta responses = fewer
    // "Service temporarily unavailable" (code=2) errors on Raho's fat
    // 90-day daily fetches. Trade-off is 2x paging round-trips, but each
    // is faster + more reliable — measured ~30% net speedup on Raho after
    // this change because we don't burn 3min of retries per chunk.
    'limit=250',
  ];
  if (daily) params.push('time_increment=1');

  let url = cfg.base + '/act_' + acct.id + '/insights?' + params.join('&');
  let page = 0;
  let skipped = false;
  let pagesOk = 0;
  while (url && page < 200) {
    page++;
    const resp = metaGet_(url);
    if (!resp) {
      // metaGet_ exhausted retries and returned null (skip semantics from
      // primary_sync). We can't page forward without the cursor from the
      // failed response, so this chunk stops here.
      skipped = true;
      console.log('    page ' + page + ' skipped (throttle/error persisted) ' +
                  '— will try chunk-halving in fetchChunkWithHalving_');
      break;
    }
    pagesOk++;
    const j = JSON.parse(resp.getContentText() || '{}');
    for (const row of (j.data || [])) {
      out.push(flattenRow_(row, acct.name));
    }
    url = (j.paging && j.paging.next) || '';
    if (url) Utilities.sleep(300);
  }
  return {skipped: skipped, pagesOk: pagesOk};
}

// Split [since, until] into contiguous chunks of at most `daysPerChunk` days each.
// Returns [[since, until], ...] with ISO dates.
function weeklyChunks_(since, until, daysPerChunk) {
  const dSince = new Date(since + 'T00:00:00Z');
  const dUntil = new Date(until + 'T00:00:00Z');
  const chunks = [];
  let cur = dSince;
  while (cur <= dUntil) {
    const chunkEnd = new Date(cur.getTime() + (daysPerChunk - 1) * 86400000);
    const end = chunkEnd > dUntil ? dUntil : chunkEnd;
    chunks.push([
      Utilities.formatDate(cur, 'GMT', 'yyyy-MM-dd'),
      Utilities.formatDate(end, 'GMT', 'yyyy-MM-dd'),
    ]);
    cur = new Date(end.getTime() + 86400000);
  }
  return chunks;
}

// ── HTTP with retry/skip — mirrors backend/primary_sync.py:_get() ───────
// Semantics match primary_sync exactly:
//   · HTTP 403                      → wait 60s, retry up to MAX_RETRIES,
//                                      then SKIP (return null, no throw)
//   · HTTP 400 + rate-limit body    → wait 90s·attempt (90/180/270),
//                                      retry up to MAX_RETRIES, then SKIP
//   · HTTP 500                      → wait RETRY_DELAY·attempt·3 (15/30/45),
//                                      retry up to MAX_RETRIES, then SKIP
//   · HTTP 400 real validation      → throw (don't burn retries on those)
//   · Other errors                  → wait RETRY_DELAY·attempt (5/10/15),
//                                      retry up to MAX_RETRIES, then throw
// Rate-limit body sniff: "too many calls" | "rate" | "throttl" — same as
// primary_sync — PLUS Meta's numeric subcodes (17, 613, 4, 80000..80099)
// which primary_sync doesn't check because it never seems to hit them, but
// the Apps Script route does since it uses a different token scope.
// SKIP semantics: returning null tells fetchInsightsChunk_ to break out
// of the current chunk's paging loop. The outer weekly-chunk loop in
// fetchInsights_ continues — so a throttled page yields PARTIAL data
// instead of zero data for that account. This is exactly why primary_sync
// keeps ingesting Raho even under heavy throttle.
var MAX_RETRIES = 3;
var RETRY_DELAY = 5000;   // 5s (ms — primary_sync uses seconds)

function metaGet_(url, attempt) {
  attempt = attempt || 1;
  var r;
  try {
    r = UrlFetchApp.fetch(url, {method: 'get', muteHttpExceptions: true});
  } catch (e) {
    // Network / DNS / timeout — same pattern as primary_sync's except block
    var msg = _redactToken_(String(e && e.message || e));
    if (attempt < MAX_RETRIES) {
      console.log('    Retry ' + attempt + '/' + MAX_RETRIES + ': ' + msg);
      Utilities.sleep(RETRY_DELAY * attempt);
      return metaGet_(url, attempt + 1);
    }
    throw new Error(msg);
  }

  // Proactive backoff via usage headers — sleep BEFORE returning so the next
  // call doesn't get hard-throttled. Applies even on HTTP 200. Not in
  // primary_sync (Python-side headers work the same way; kept here because
  // the Apps Script UrlFetchApp exposes them easily).
  _throttleBackoff_(r.getAllHeaders());

  var code = r.getResponseCode();
  if (code === 200) return r;

  var body = r.getContentText() || '';
  var bodyShort = body.slice(0, 400);

  // HTTP 403 — hard rate limit at the app/token level
  if (code === 403) {
    if (attempt < MAX_RETRIES) {
      console.log('    Meta 403 rate limit — waiting 60s (attempt ' +
                  attempt + '/' + MAX_RETRIES + ')');
      Utilities.sleep(60000);
      return metaGet_(url, attempt + 1);
    }
    console.log('    Meta 403 persists — skipping page');
    return null;
  }

  // HTTP 400 — could be rate limit, transient upstream error, OR real
  // validation. Sniff body to categorise, otherwise all 400s look the same.
  if (code === 400) {
    var msg400 = '';
    var subcode = 0;
    var subsubcode = 0;
    try {
      var jb = JSON.parse(body);
      msg400     = ((jb.error || {}).message || '').toLowerCase();
      subcode    = +((jb.error || {}).code)          || 0;
      subsubcode = +((jb.error || {}).error_subcode) || 0;
    } catch (_) {}

    // Rate-limit signatures (matches primary_sync + Meta's numeric subcodes)
    var isRateLimit400 =
      msg400.indexOf('too many calls')            >= 0 ||
      msg400.indexOf('rate')                      >= 0 ||
      msg400.indexOf('throttl')                   >= 0 ||
      msg400.indexOf('user request limit')        >= 0 ||
      msg400.indexOf('application request limit') >= 0 ||
      msg400.indexOf('call rate')                 >= 0 ||
      msg400.indexOf('reduce the amount of data') >= 0 ||
      subcode === 4 || subcode === 17 || subcode === 32 || subcode === 613 ||
      (subcode >= 80000 && subcode <= 80099);

    // "Transient / intermittent" signatures — Meta serves these as HTTP 400
    // but the error_user_msg literally says "please retry at a later time".
    // is_transient is UNRELIABLE (subcode=2 sets it to false even though
    // the error is 100% retryable), so we detect via subcode + message text.
    //   code=1     — Unknown error
    //   code=2     — Service temporarily unavailable  ← the one that hit Raho
    //   subcode=1504xxx — Meta's internal ads-serving intermittent errors
    var isTransient400 =
      subcode === 1 || subcode === 2 ||
      (subsubcode >= 1504000 && subsubcode <= 1504999) ||
      msg400.indexOf('service temporarily unavailable') >= 0 ||
      msg400.indexOf('intermittent')                    >= 0 ||
      msg400.indexOf('please retry')                    >= 0 ||
      msg400.indexOf('please try again')                >= 0 ||
      msg400.indexOf('an unexpected error')             >= 0;

    if (isRateLimit400 || isTransient400) {
      if (attempt < MAX_RETRIES) {
        // Rate limits need long waits (Meta's throttle window is minutes);
        // intermittent errors need SHORT waits — Apps Script has a 6-min
        // hard ceiling, and burning 3+ min per chunk means Raho's 13-chunk
        // Daily 90d fetch never completes. If a chunk still fails after 90s
        // total wait, fetchChunkWithHalving_ splits it and retries smaller —
        // that's a far more effective recovery than waiting longer.
        var wait400 = isRateLimit400 ? (90000 * attempt)   // 90s, 180s, 270s
                                     : (15000 * attempt);  // 15s, 30s, 45s
        var kind = isRateLimit400 ? 'rate limit' : 'intermittent';
        console.log('    Meta 400 ' + kind + ' (subcode=' + subcode +
                    (subsubcode ? '/' + subsubcode : '') +
                    ') — waiting ' + (wait400/1000) + 's (attempt ' +
                    attempt + '/' + MAX_RETRIES + ')');
        Utilities.sleep(wait400);
        return metaGet_(url, attempt + 1);
      }
      console.log('    Meta 400 ' + (isRateLimit400 ? 'rate limit' :
                  'intermittent error') + ' persists — skipping page');
      return null;
    }
    // Real 400 (validation, bad param, missing permission) — throw so the
    // caller sees it. Don't burn retries; the error is deterministic.
    throw new Error('Meta HTTP 400 (subcode=' + subcode + '): ' + bodyShort);
  }

  // HTTP 429 — formal Too Many Requests. Meta rarely uses this but it's
  // valid, treat like 400 rate limit.
  if (code === 429) {
    if (attempt < MAX_RETRIES) {
      var wait429 = 90000 * attempt;
      console.log('    Meta 429 — waiting ' + (wait429/1000) + 's (attempt ' +
                  attempt + '/' + MAX_RETRIES + ')');
      Utilities.sleep(wait429);
      return metaGet_(url, attempt + 1);
    }
    console.log('    Meta 429 persists — skipping page');
    return null;
  }

  // HTTP 500+ — transient upstream. Same wait ladder as primary_sync.
  if (code >= 500) {
    if (attempt < MAX_RETRIES) {
      var wait500 = RETRY_DELAY * attempt * 3;  // 15s, 30s, 45s
      console.log('    Meta ' + code + ' server error — waiting ' +
                  (wait500/1000) + 's (attempt ' + attempt + '/' +
                  MAX_RETRIES + ')');
      Utilities.sleep(wait500);
      return metaGet_(url, attempt + 1);
    }
    console.log('    Meta ' + code + ' persists — skipping page');
    return null;
  }

  // Anything else — retry linear, then throw.
  if (attempt < MAX_RETRIES) {
    console.log('    Meta HTTP ' + code + ' — retry ' + attempt + '/' +
                MAX_RETRIES + ': ' + bodyShort);
    Utilities.sleep(RETRY_DELAY * attempt);
    return metaGet_(url, attempt + 1);
  }
  throw new Error('Meta HTTP ' + code + ' after ' + MAX_RETRIES +
                  ' attempts: ' + bodyShort);
}

// Scrub *_token= params AND raw EAA... tokens from error messages before
// they hit the log or Sheet footer. Mirrors primary_sync._safe regex.
function _redactToken_(s) {
  if (!s) return s;
  return String(s)
    .replace(/(\w*token\w*=)[^&\s]+/gi, '$1<REDACTED>')
    .replace(/EAA[A-Za-z0-9_\-]{20,}/g, '<REDACTED>');
}

// Peek at Meta's usage headers and sleep if we're near the ceiling on any
// dimension. Headers are JSON objects like:
//   X-App-Usage: {"call_count":45,"total_cputime":12,"total_time":8}
//   X-Business-Use-Case-Usage: {"<biz_id>":[{"type":"ads_insights",
//     "call_count":78,"total_cputime":34,"total_time":22,
//     "estimated_time_to_regain_access":0}]}
// If any percentage ≥ 90 we sleep. If estimated_time_to_regain_access > 0
// we honour it (capped to 10 min so we don't hang for hours).
function _throttleBackoff_(headers) {
  if (!headers) return;
  const maxPct = (obj) => {
    if (!obj) return 0;
    let m = 0;
    for (const k in obj) {
      const v = +obj[k];
      if (isFinite(v) && v > m) m = v;
    }
    return m;
  };
  const parseJson = (v) => { try { return JSON.parse(v); } catch (_) { return null; } };
  let worstPct = 0, waitSec = 0;
  const app = parseJson(headers['X-App-Usage'] || headers['x-app-usage']);
  if (app) worstPct = Math.max(worstPct, maxPct(app));
  const buc = parseJson(headers['X-Business-Use-Case-Usage'] ||
                        headers['x-business-use-case-usage']);
  if (buc) {
    for (const bizId in buc) {
      const arr = buc[bizId];
      if (Array.isArray(arr)) {
        for (const entry of arr) {
          worstPct = Math.max(worstPct, maxPct(entry));
          const est = +entry.estimated_time_to_regain_access || 0;
          if (est > waitSec) waitSec = est;
        }
      }
    }
  }
  const ad = parseJson(headers['X-Ad-Account-Usage'] ||
                       headers['x-ad-account-usage']);
  if (ad) worstPct = Math.max(worstPct, maxPct(ad));

  if (waitSec > 0) {
    const capped = Math.min(waitSec, 600);
    console.log('    [throttle] Meta says wait ' + waitSec +
                's — sleeping ' + capped + 's');
    Utilities.sleep(capped * 1000);
  } else if (worstPct >= 90) {
    console.log('    [throttle ' + worstPct.toFixed(0) +
                '%] approaching cap — sleeping 30s');
    Utilities.sleep(30000);
  } else if (worstPct >= 75) {
    console.log('    [throttle ' + worstPct.toFixed(0) +
                '%] warm — pacing 5s');
    Utilities.sleep(5000);
  }
}

// ── Row flattener — extracts purchases/conv_value from actions[] ────────
function flattenRow_(r, accountName) {
  // Prefer omni_purchase (unified across placements), else 'purchase'.
  const actVal = (arr, order) => {
    if (!arr) return 0;
    const by = {}; for (const a of arr) by[a.action_type] = a.value;
    for (const t of order) { if (by[t] != null) return +by[t] || 0; }
    return 0;
  };
  const PURCH  = ['omni_purchase','purchase'];
  const CI     = ['omni_initiated_checkout','initiate_checkout'];
  const ATC    = ['omni_add_to_cart','add_to_cart'];
  const purchases    = actVal(r.actions,       PURCH);
  const conv_value   = actVal(r.action_values, PURCH);
  const ci_count     = actVal(r.actions,       CI);
  const atc_count    = actVal(r.actions,       ATC);
  const tp = (r.video_thruplay_watched_actions || [])[0];
  const thruplays = tp ? +tp.value || 0 : 0;
  const p100 = (r.video_p100_watched_actions || [])[0];
  const p100_plays = p100 ? +p100.value || 0 : 0;
  const purch_roas = actVal(r.purchase_roas, PURCH);

  return {
    account_name:  accountName,
    campaign_id:   r.campaign_id || '',
    campaign_name: r.campaign_name || '',
    adset_id:      r.adset_id || '',
    adset_name:    r.adset_name || '',
    ad_id:         r.ad_id || '',
    ad_name:       r.ad_name || '',
    date_start:    r.date_start || '',
    date_stop:     r.date_stop  || '',
    impressions:   +r.impressions || 0,
    reach:         +r.reach || 0,
    frequency:     +r.frequency || 0,
    spend:         +r.spend || 0,
    link_clicks:   +r.inline_link_clicks || 0,
    outbound_clicks: +r.outbound_clicks || 0,
    all_clicks:    +r.clicks || 0,
    ctr:           +r.ctr || 0,
    cpc:           +r.cpc || 0,
    cpm:           +r.cpm || 0,
    purchases:     purchases,
    conv_value:    conv_value,
    purchase_roas: purch_roas,
    ci_count:      ci_count,
    atc_count:     atc_count,
    thruplays:     thruplays,
    p100_plays:    p100_plays,
  };
}

// ── Sheet writer ────────────────────────────────────────────────────────
const HEADERS_ACTIVE = [
  'account_name','campaign_id','campaign_name','adset_id','adset_name',
  'ad_id','ad_name','date_start','date_stop',
  'impressions','reach','frequency','spend',
  'link_clicks','outbound_clicks','all_clicks',
  'ctr','cpc','cpm',
  'purchases','conv_value','purchase_roas',
  'ci_count','atc_count','thruplays','p100_plays',
  // Last-click UTM attribution from Supabase (blank if SUPABASE_* not configured)
  'shopify_orders','shopify_sales',
  // Ad status/created + last-click UTM terms + attribution step (from Supabase)
  'ad_status','ad_created',
  'utm_term','utm_content','utm_campaign','matched_tier',
];
const HEADERS_DAILY = HEADERS_ACTIVE.slice();
// The daily tab uses date_start as the per-day date, so surface it first.
HEADERS_DAILY.unshift('date');

function writeSheet_(sheetName, rows, daily, append, perAcct) {
  perAcct = perAcct || [];
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(sheetName);
  if (!sh) sh = ss.insertSheet(sheetName);
  const headers = daily ? HEADERS_DAILY : HEADERS_ACTIVE;

  if (!append) {
    // Full-refresh mode — clear and rewrite from row 1
    sh.clearContents(); sh.clearFormats();
  }
  if (!rows.length && !append) {
    // Even with zero rows, dump per-account status so the user sees WHY
    // (e.g. "Raho: 0 rows — Meta HTTP 400: User request limit reached")
    // instead of a bare "No rows returned" that hides the real cause.
    sh.getRange(1, 1).setValue('No rows returned. Per-account status below:');
    _writePerAcctBlock_(sh, perAcct, /*startRow*/ 3);
    return;
  }

  // Header row (only when clearing / when sheet is empty)
  const firstEmptyRow = sh.getLastRow() + 1;
  const startRow = append && firstEmptyRow > 1 ? firstEmptyRow : 2;
  if (!append || firstEmptyRow <= 1) {
    sh.getRange(1, 1, 1, headers.length).setValues([headers])
      .setFontWeight('bold').setBackground('#f0f0f0');
    sh.setFrozenRows(1);
    sh.setFrozenColumns(daily ? 8 : 7);
  }

  if (!rows.length) {
    // Append call for an account that failed / had zero data. Still stamp
    // the per-account status block at the bottom.
    _writePerAcctBlock_(sh, perAcct, sh.getLastRow() + 3);
    return;
  }

  // Values
  const data = rows.map(r => headers.map(h => {
    if (h === 'date') return r.date_start;
    const v = r[h];
    if (v == null) return '';
    return v;
  }));
  const CHUNK = 5000;
  for (let i = 0; i < data.length; i += CHUNK) {
    const slice = data.slice(i, i + CHUNK);
    sh.getRange(startRow + i, 1, slice.length, headers.length).setValues(slice);
  }

  // Filter + widths (rebuilt on both refresh and append so it covers all rows)
  const filter = sh.getFilter(); if (filter) filter.remove();
  const total = sh.getLastRow();
  sh.getRange(1, 1, total, headers.length).createFilter();
  for (let c = 1; c <= headers.length; c++) sh.setColumnWidth(c, 120);
  const widen = (n, w) => { const i = headers.indexOf(n); if (i >= 0) sh.setColumnWidth(i + 1, w); };
  widen('ad_name', 260); widen('adset_name', 220); widen('campaign_name', 220);

  // Refresh footer + per-account status block
  const footerRow = total + 2;
  sh.getRange(footerRow, 1).setValue('Refreshed at: ' + new Date().toISOString())
    .setFontStyle('italic').setFontColor('#666');
  _writePerAcctBlock_(sh, perAcct, footerRow + 1);
}

// Stamps a color-coded "Per-account status" block into the sheet so failed
// accounts (rate-limited, permission-denied, etc.) are visible without
// diving into Apps Script → Executions.
function _writePerAcctBlock_(sh, perAcct, startRow) {
  if (!perAcct || !perAcct.length) return;
  sh.getRange(startRow, 1).setValue('Per-account status:')
    .setFontWeight('bold').setFontColor('#333');
  for (let i = 0; i < perAcct.length; i++) {
    const p = perAcct[i];
    const row = startRow + 1 + i;
    const ok = !p.error;
    const line = ok
      ? '  ' + p.name + ' (act_' + p.id + ')  →  ' + p.rows + ' rows in ' + p.secs + 's'
      : '  [!] ' + p.name + ' (act_' + p.id + ')  →  FAILED after ' + p.secs + 's · ' + p.error;
    sh.getRange(row, 1).setValue(line)
      .setFontColor(ok ? '#0a7d2c' : '#c0392b')
      .setFontStyle('italic');
  }
}

// ── Helpers ─────────────────────────────────────────────────────────────
function window_(days) {
  const today = new Date();
  const until = Utilities.formatDate(today, 'GMT', 'yyyy-MM-dd');
  const back = new Date(today.getTime() - (days - 1) * 86400000);
  const since = Utilities.formatDate(back, 'GMT', 'yyyy-MM-dd');
  return {since: since, until: until};
}

// ── Triggers ────────────────────────────────────────────────────────────
function installHourlyTriggers() {
  uninstallAllTriggers();
  ['refreshActive30','refreshActive90','refreshDaily30','refreshDaily90All']
    .forEach(fn => ScriptApp.newTrigger(fn).timeBased().everyHours(1).create());
  SpreadsheetApp.getActiveSpreadsheet()
    .toast('4 hourly triggers installed (one per tab)', 'Meta Direct', 5);
}

// Nightly refresh — fires AFTER the backend Python orchestrator finishes.
// The orchestrator (_refresh_all_dashboard_data.py) runs primary_sync +
// rebuild_attribution_orders and takes ~3h wall time starting ~00:00–01:00
// IST → done ~03:30–04:00 IST. We schedule at 05:00 IST onwards so the sheet
// pulls a DB where shopify_ad_attribution + ae_table_view are fully mapped;
// otherwise shopify_orders / utm_* / ad_status columns would be stale.
//
// Timezone: uses the Apps Script project timezone. Set File → Project
// Settings → Timezone to "(GMT+05:30) India Standard Time" for the hours
// below to mean IST literally.
//
// Split across 3 hour slots because Apps Script has a 6-min per-execution
// ceiling — Raho's Daily 90d alone can eat that entire budget, so each
// heavy handler gets its own trigger and the "ALL" variants are avoided.
function installNightlyTriggers() {
  uninstallAllTriggers();
  // 05:00 IST — fast aggregate tabs (~30-60s each)
  ScriptApp.newTrigger('refreshActive30')      .timeBased().atHour(5).everyDays(1).create();
  ScriptApp.newTrigger('refreshActive90')      .timeBased().atHour(5).everyDays(1).create();
  // 06:00 IST — daily 30d (fast) + Raho daily 90d (heavy, own slot)
  ScriptApp.newTrigger('refreshDaily30')       .timeBased().atHour(6).everyDays(1).create();
  ScriptApp.newTrigger('refreshDaily90Raho')   .timeBased().atHour(6).everyDays(1).create();
  // 07:00 IST — smaller accounts' daily 90d (append to Raho's rows)
  ScriptApp.newTrigger('refreshDaily90Fourth') .timeBased().atHour(7).everyDays(1).create();
  ScriptApp.newTrigger('refreshDaily90Third')  .timeBased().atHour(7).everyDays(1).create();
  SpreadsheetApp.getActiveSpreadsheet()
    .toast('6 nightly triggers installed (5–7 AM IST, post-DB-refresh)', 'Meta Direct', 6);
}

function uninstallAllTriggers() {
  const wanted = new Set([
    'refreshActive30','refreshActive90',
    'refreshDaily30','refreshDaily90','refreshDaily90All',
    'refreshDaily90Raho','refreshDaily90Fourth','refreshDaily90Third',
    'refreshAllTabs',
  ]);
  let removed = 0;
  ScriptApp.getProjectTriggers().forEach(t => {
    if (wanted.has(t.getHandlerFunction())) { ScriptApp.deleteTrigger(t); removed++; }
  });
  try {
    SpreadsheetApp.getActiveSpreadsheet()
      .toast('Removed ' + removed + ' trigger(s)', 'Meta Direct', 4);
  } catch (_) {}
}
