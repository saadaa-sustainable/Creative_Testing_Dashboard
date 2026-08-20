/**
 * ═══════════════════════════════════════════════════════════════════════
 * Meta Ads Direct — SUPABASE fast path
 *
 * Fetches from public.meta_direct_* materialized views on Supabase.
 * Replaces the Meta Graph API path — no throttling, no chunk-halving,
 * no partial failures. Typical wall time: 1–5 seconds per tab.
 *
 * ┌──────────────────────┬─────────────────────────────────────────────┐
 * │ Sheet tab            │ Source view                                 │
 * ├──────────────────────┼─────────────────────────────────────────────┤
 * │ Active Ads 30d       │ public.meta_direct_active_30d               │
 * │ Active Ads 90d       │ public.meta_direct_active_90d               │
 * │ Daily Breakdown 30d  │ public.meta_direct_daily_30d                │
 * │ Daily Breakdown 90d  │ public.meta_direct_daily_90d                │
 * └──────────────────────┴─────────────────────────────────────────────┘
 *
 * ── SETUP (one-time) ────────────────────────────────────────────────────
 * 1. Extensions → Apps Script → New file → paste this. Save.
 * 2. Project Settings (gear icon) → Script Properties → confirm:
 *      SUPABASE_URL              =  https://<project>.supabase.co
 *      SUPABASE_ANON             =  eyJ… (anon key)
 *      REFRESH_DB_BEFORE_FETCH   =  false      (optional, default false)
 * 3. In your existing Code.gs, ADD this ONE LINE inside your onOpen():
 *      sbBuildMenu();
 *    (or, if you don't have a Code.gs onOpen anymore, this file's own
 *    onOpenSupabase() trigger will be picked up automatically.)
 * 4. Reload the sheet → new "Meta Direct — Supabase" menu appears.
 *
 * ── HOW REFRESH_DB_BEFORE_FETCH WORKS ───────────────────────────────────
 * DEFAULT FALSE. The sheet just reads the cached materialized views —
 * always fast (~1-3s per tab). The nightly orchestrator refreshes the
 * views via public.refresh_meta_direct_views() around 05:00 IST so morning
 * fetches see the freshest data.
 *
 * Set to TRUE only when you need on-demand freshness in the middle of the
 * day (e.g. right after a manual primary_sync run). Each refresh then adds
 * ~4 min of wall time before the fetch, so use sparingly. You can also just
 * click "Rebuild source views (RPC)" once, then click your desired refresh
 * — same effect without changing the property.
 * ═══════════════════════════════════════════════════════════════════════
 */

// ── Menu ─────────────────────────────────────────────────────────────
// Fallback onOpen — active only if the existing Code.gs onOpen doesn't
// call sbBuildMenu() itself. Both menus will show.
function onOpenSupabase() { sbBuildMenu(); }

function sbBuildMenu() {
  SpreadsheetApp.getUi()
    .createMenu('Meta Direct — Supabase')
      .addItem('Refresh Active Ads 30d',           'sbRefreshActive30')
      .addItem('Refresh Active Ads 90d',           'sbRefreshActive90')
      .addItem('Refresh Daily Breakdown 30d',      'sbRefreshDaily30')
      .addItem('Refresh Daily Breakdown 90d',      'sbRefreshDaily90')
      .addSeparator()
      .addItem('Refresh ALL 4 tabs',               'sbRefreshAll')
      .addSeparator()
      .addItem('Rebuild source views (RPC)',       'sbRebuildViews')
      .addSeparator()
      .addItem('Install nightly trigger (5 AM IST)', 'sbInstallNightlyTrigger')
      .addItem('Install hourly trigger',           'sbInstallHourlyTrigger')
      .addItem('Uninstall Supabase triggers',      'sbUninstallTriggers')
    .addToUi();
}

// ── Config ───────────────────────────────────────────────────────────
function sbGetConfig_() {
  const props = PropertiesService.getScriptProperties();
  const url  = (props.getProperty('SUPABASE_URL')  || '').replace(/\/$/, '');
  const anon = (props.getProperty('SUPABASE_ANON') || '').trim();
  if (!url)  throw new Error('Missing SUPABASE_URL in Script Properties.');
  if (!anon) throw new Error('Missing SUPABASE_ANON in Script Properties.');
  // Default FALSE: sheet fetches always read the latest cached view state
  // (instant, ~1-3s per tab). The nightly orchestrator refreshes the views
  // via public.refresh_meta_direct_views(). Set to 'true' if you want each
  // menu-click to trigger a fresh rebuild first (~4 min wall time).
  const rf = (props.getProperty('REFRESH_DB_BEFORE_FETCH') || 'false')
             .toString().toLowerCase();
  return {url: url, anon: anon, refreshFirst: rf === 'true'};
}

// ── Column order (mirrors materialized view schema) ─────────────────
const SB_HEADERS_ACTIVE = [
  'account_name','campaign_name','campaign_id','adset_id','adset_name',
  'ad_id','ad_name',
  'impressions','reach','frequency','spend',
  'link_clicks','outbound_clicks','all_clicks',
  'ctr','cpc','cpm',
  'purchases','conv_value','purchase_roas',
  'ci_count','atc_count','thruplays','p100_plays',
  'ftewv_count','ncp_count',
  'date_start','date_stop',
  'ad_status','ad_created',
  'shopify_orders','shopify_sales','shopify_roas',
  'matched_tier','utm_content','utm_term','utm_campaign',
];
// Daily rows have `date` first + no aggregate date_start/stop
const SB_HEADERS_DAILY = ['date'].concat(
  SB_HEADERS_ACTIVE.filter(h => h !== 'date_start' && h !== 'date_stop')
);

// ── Menu handlers ────────────────────────────────────────────────────
function sbRefreshActive30() { sbRunOne_('Active Ads 30d',      'meta_direct_active_30d', false); }
function sbRefreshActive90() { sbRefreshActive90_impl(); }
function sbRefreshActive90_impl() { sbRunOne_('Active Ads 90d', 'meta_direct_active_90d', false); }
function sbRefreshDaily30()  { sbRunOne_('Daily Breakdown 30d', 'meta_direct_daily_30d',  true);  }
function sbRefreshDaily90()  { sbRunOne_('Daily Breakdown 90d', 'meta_direct_daily_90d',  true);  }

// Refresh all 4 tabs — rebuild views ONCE, then fetch each without
// re-refreshing (saves 3x the RPC cost).
function sbRefreshAll() {
  const cfg = sbGetConfig_();
  const t0 = Date.now();
  if (cfg.refreshFirst) {
    sbRebuildViewsQuiet_(cfg);
  }
  // All 4 skip the per-run refresh since we just did one.
  sbRunOne_('Active Ads 30d',      'meta_direct_active_30d', false, /*skipRefresh*/ true);
  sbRunOne_('Active Ads 90d',      'meta_direct_active_90d', false, true);
  sbRunOne_('Daily Breakdown 30d', 'meta_direct_daily_30d',  true,  true);
  sbRunOne_('Daily Breakdown 90d', 'meta_direct_daily_90d',  true,  true);
  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  try {
    SpreadsheetApp.getActiveSpreadsheet()
      .toast('All 4 tabs refreshed in ' + secs + 's', 'Meta Direct — Supabase', 6);
  } catch (_) {}
}

// Explicit menu item — trigger the DB-side view rebuild without fetching.
function sbRebuildViews() {
  const cfg = sbGetConfig_();
  const t0 = Date.now();
  const summary = sbRebuildViewsQuiet_(cfg);
  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  try {
    SpreadsheetApp.getActiveSpreadsheet()
      .toast('Views rebuilt in ' + secs + 's · ' + summary,
             'Meta Direct — Supabase', 8);
  } catch (_) {}
}

// ── Core: rebuild → fetch → write ────────────────────────────────────
// tableView: 'meta_direct_active_30d' etc.
// daily:     true if the sheet needs the `date` column prepended.
function sbRunOne_(sheetName, tableView, daily, skipRefresh) {
  const cfg = sbGetConfig_();
  const t0 = Date.now();

  if (cfg.refreshFirst && !skipRefresh) {
    sbRebuildViewsQuiet_(cfg);
  }

  console.log('[SB] fetching ' + tableView + ' → tab "' + sheetName + '"');
  const rows = sbFetchTable_(cfg, tableView);
  const fetchSecs = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('[SB]   pulled ' + rows.length + ' rows in ' + fetchSecs + 's');

  sbWriteSheet_(sheetName, rows, daily);
  const totalSecs = ((Date.now() - t0) / 1000).toFixed(1);
  console.log('[SB]   wrote ' + rows.length + ' rows to "' + sheetName +
              '" — total ' + totalSecs + 's');
  try {
    SpreadsheetApp.getActiveSpreadsheet()
      .toast('Wrote ' + rows.length + ' rows to ' + sheetName +
             ' in ' + totalSecs + 's', 'Meta Direct — Supabase', 5);
  } catch (_) {}
}

// Fire the DB-side refresh RPC. Returns a short summary like
//   "active_30d:922 rows/3.2s · daily_30d:17229 rows/8.1s · …"
// Runs CONCURRENTLY so anon reads never block.
function sbRebuildViewsQuiet_(cfg) {
  const url = cfg.url + '/rest/v1/rpc/refresh_meta_direct_views';
  console.log('[SB] rebuilding views via refresh_meta_direct_views()');
  const t0 = Date.now();
  const r = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    headers: {apikey: cfg.anon, Authorization: 'Bearer ' + cfg.anon,
              Prefer: 'count=none'},
    payload: '{}',
    muteHttpExceptions: true,
  });
  const code = r.getResponseCode();
  if (code !== 200) {
    console.log('[SB]   ! refresh RPC failed HTTP ' + code + ': ' +
                r.getContentText().slice(0, 200));
    return 'refresh RPC failed HTTP ' + code;
  }
  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  let rows;
  try { rows = JSON.parse(r.getContentText() || '[]'); } catch (_) { rows = []; }
  const parts = rows.map(x => (x.view_name || '?').replace('meta_direct_', '') +
                              ':' + (x.rows_after || 0) + 'rows/' +
                              (x.refresh_secs || 0) + 's');
  const summary = parts.join(' · ');
  console.log('[SB]   ' + summary + ' (total ' + secs + 's)');
  return summary;
}

// Paginated PostgREST fetch. Uses Range header so we survive PostgREST's
// 1000-row soft cap on anon requests. For the 90d daily view (~55k rows)
// this typically returns in 3–8 s total across ~6 batches.
function sbFetchTable_(cfg, tableView) {
  const BATCH = 10000;
  const out = [];
  let offset = 0;
  for (let page = 0; page < 100; page++) {
    const url = cfg.url + '/rest/v1/' + tableView + '?select=*' +
                '&limit=' + BATCH + '&offset=' + offset;
    const r = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: {apikey: cfg.anon, Authorization: 'Bearer ' + cfg.anon,
                Prefer: 'count=none'},
      muteHttpExceptions: true,
    });
    const code = r.getResponseCode();
    if (code !== 200) {
      throw new Error('Supabase HTTP ' + code + ' fetching ' + tableView +
                      ': ' + r.getContentText().slice(0, 200));
    }
    let chunk;
    try { chunk = JSON.parse(r.getContentText() || '[]'); }
    catch (e) { throw new Error('JSON parse failed for ' + tableView +
                                ': ' + e.message); }
    if (!Array.isArray(chunk) || chunk.length === 0) break;
    for (const row of chunk) out.push(row);
    if (chunk.length < BATCH) break;
    offset += BATCH;
  }
  return out;
}

// ── Sheet writer ─────────────────────────────────────────────────────
function sbWriteSheet_(sheetName, rows, daily) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(sheetName);
  if (!sh) sh = ss.insertSheet(sheetName);
  const headers = daily ? SB_HEADERS_DAILY : SB_HEADERS_ACTIVE;

  // Full refresh — clear then rewrite
  sh.clearContents();
  sh.clearFormats();

  if (!rows.length) {
    sh.getRange(1, 1).setValue(
      'No rows returned. Check that the source views are populated + ' +
      'SUPABASE_URL/SUPABASE_ANON are set in Script Properties.');
    return;
  }

  // Header row
  sh.getRange(1, 1, 1, headers.length).setValues([headers])
    .setFontWeight('bold').setBackground('#f0f0f0');
  sh.setFrozenRows(1);
  sh.setFrozenColumns(daily ? 8 : 7);

  // Values — map row objects to header order. Convert nulls to '' so
  // Sheets doesn't render "null" as literal text.
  const data = rows.map(r => headers.map(h => {
    const v = r[h];
    if (v == null) return '';
    return v;
  }));
  const CHUNK = 5000;
  for (let i = 0; i < data.length; i += CHUNK) {
    const slice = data.slice(i, i + CHUNK);
    sh.getRange(2 + i, 1, slice.length, headers.length).setValues(slice);
  }

  // Filter + column widths
  const filter = sh.getFilter(); if (filter) filter.remove();
  const total = sh.getLastRow();
  sh.getRange(1, 1, total, headers.length).createFilter();
  for (let c = 1; c <= headers.length; c++) sh.setColumnWidth(c, 120);
  const widen = (n, w) => {
    const i = headers.indexOf(n);
    if (i >= 0) sh.setColumnWidth(i + 1, w);
  };
  widen('ad_name', 260); widen('adset_name', 220); widen('campaign_name', 220);
  widen('utm_content', 200); widen('utm_term', 200); widen('utm_campaign', 200);

  // Footer
  sh.getRange(total + 2, 1)
    .setValue('Refreshed from Supabase at ' + new Date().toISOString() +
              ' — source: public.meta_direct_' +
              (sheetName === 'Active Ads 30d'      ? 'active_30d' :
               sheetName === 'Active Ads 90d'      ? 'active_90d' :
               sheetName === 'Daily Breakdown 30d' ? 'daily_30d'  : 'daily_90d'))
    .setFontStyle('italic').setFontColor('#666');
}

// ── Triggers ─────────────────────────────────────────────────────────
// Nightly — fires at 5 AM (project timezone). Assumes the orchestrator
// has completed by then and refreshed primary_table + attribution.
// (Set File → Project Settings → Timezone to Asia/Kolkata for IST.)
function sbInstallNightlyTrigger() {
  sbUninstallTriggers();
  ScriptApp.newTrigger('sbRefreshAll').timeBased().atHour(5).everyDays(1).create();
  SpreadsheetApp.getActiveSpreadsheet()
    .toast('Nightly trigger installed — sbRefreshAll @ 5 AM IST',
           'Meta Direct — Supabase', 6);
}

function sbInstallHourlyTrigger() {
  sbUninstallTriggers();
  ScriptApp.newTrigger('sbRefreshAll').timeBased().everyHours(1).create();
  SpreadsheetApp.getActiveSpreadsheet()
    .toast('Hourly trigger installed — sbRefreshAll every 1h',
           'Meta Direct — Supabase', 6);
}

function sbUninstallTriggers() {
  const wanted = new Set([
    'sbRefreshAll', 'sbRefreshActive30', 'sbRefreshActive90',
    'sbRefreshDaily30', 'sbRefreshDaily90', 'sbRebuildViews',
  ]);
  let removed = 0;
  ScriptApp.getProjectTriggers().forEach(t => {
    if (wanted.has(t.getHandlerFunction())) {
      ScriptApp.deleteTrigger(t); removed++;
    }
  });
  try {
    SpreadsheetApp.getActiveSpreadsheet()
      .toast('Removed ' + removed + ' Supabase trigger(s)',
             'Meta Direct — Supabase', 4);
  } catch (_) {}
}
