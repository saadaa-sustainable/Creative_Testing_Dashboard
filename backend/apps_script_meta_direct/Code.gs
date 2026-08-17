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
      .addItem('Install hourly triggers',          'installHourlyTriggers')
      .addItem('Uninstall triggers',               'uninstallAllTriggers')
    .addToUi();
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
  for (const acct of scope) {
    console.log('  fetching ' + acct.name + ' (act_' + acct.id + ')');
    try {
      const rows = fetchInsights_(cfg, acct, since, until, daily);
      console.log('    ' + acct.name + ': ' + rows.length + ' rows');
      allRows.push.apply(allRows, rows);
    } catch (e) {
      console.log('    [!] ' + acct.name + ' failed: ' + e.message);
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

  writeSheet_(sheetName, allRows, daily, !!append);
  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  try {
    const toastMsg = (append ? 'Appended ' : 'Wrote ') + allRows.length +
                     ' rows to ' + sheetName + ' in ' + secs + 's';
    SpreadsheetApp.getActiveSpreadsheet().toast(toastMsg, 'Meta Direct', 6);
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
function fetchInsights_(cfg, acct, since, until, daily) {
  const chunks = daily ? weeklyChunks_(since, until, 7) : [[since, until]];
  const out = [];
  for (let i = 0; i < chunks.length; i++) {
    const [cSince, cUntil] = chunks[i];
    if (chunks.length > 1) {
      console.log('    chunk ' + (i+1) + '/' + chunks.length +
                  ' ' + cSince + ' → ' + cUntil);
    }
    fetchInsightsChunk_(cfg, acct, cSince, cUntil, daily, out);
    if (i + 1 < chunks.length) Utilities.sleep(800);
  }
  return out;
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
    'limit=500',
  ];
  if (daily) params.push('time_increment=1');

  let url = cfg.base + '/act_' + acct.id + '/insights?' + params.join('&');
  let page = 0;
  while (url && page < 200) {
    page++;
    const resp = metaGet_(url);
    if (!resp) break;
    const j = JSON.parse(resp.getContentText() || '{}');
    for (const row of (j.data || [])) {
      out.push(flattenRow_(row, acct.name));
    }
    url = (j.paging && j.paging.next) || '';
    if (url) Utilities.sleep(300);
  }
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

// ── HTTP with backoff on 500 / 400 rate-limit ───────────────────────────
function metaGet_(url) {
  const waits = [15000, 45000, 120000];   // 15s, 45s, 2min
  for (let attempt = 0; attempt <= waits.length; attempt++) {
    const r = UrlFetchApp.fetch(url, {method: 'get', muteHttpExceptions: true});
    const code = r.getResponseCode();
    if (code === 200) return r;
    const body = r.getContentText().slice(0, 300);
    console.log('    Meta HTTP ' + code + '  body: ' + body);
    // 400 with is_transient=true OR 5xx → retry
    const transient = code >= 500 || (code === 400 && body.indexOf('rate limit') >= 0)
                                  || (code === 400 && body.indexOf('transient') >= 0);
    if (!transient || attempt === waits.length) {
      throw new Error('Meta HTTP ' + code + ' after ' + (attempt+1) + ' attempts: ' + body);
    }
    console.log('    retrying in ' + (waits[attempt] / 1000) + 's');
    Utilities.sleep(waits[attempt]);
  }
  return null;
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

function writeSheet_(sheetName, rows, daily, append) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(sheetName);
  if (!sh) sh = ss.insertSheet(sheetName);
  const headers = daily ? HEADERS_DAILY : HEADERS_ACTIVE;

  if (!append) {
    // Full-refresh mode — clear and rewrite from row 1
    sh.clearContents(); sh.clearFormats();
  }
  if (!rows.length && !append) {
    sh.getRange(1, 1).setValue('No rows returned. Check META_ACCESS_TOKEN + ACCOUNT_* in Script Properties.');
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

  if (!rows.length) return;

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

  // Refresh footer
  sh.getRange(total + 2, 1).setValue('Refreshed at: ' + new Date().toISOString())
    .setFontStyle('italic').setFontColor('#666');
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
  ['refreshActive30','refreshActive90','refreshDaily30','refreshDaily90']
    .forEach(fn => ScriptApp.newTrigger(fn).timeBased().everyHours(1).create());
  SpreadsheetApp.getActiveSpreadsheet()
    .toast('4 hourly triggers installed (one per tab)', 'Meta Direct', 5);
}
function uninstallAllTriggers() {
  const wanted = new Set(['refreshActive30','refreshActive90','refreshDaily30','refreshDaily90','refreshAllTabs']);
  ScriptApp.getProjectTriggers().forEach(t => {
    if (wanted.has(t.getHandlerFunction())) ScriptApp.deleteTrigger(t);
  });
}
