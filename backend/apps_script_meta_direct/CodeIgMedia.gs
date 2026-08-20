/**
 * ═══════════════════════════════════════════════════════════════════════
 * Instagram Media — SUPABASE fetcher (with user-column preservation)
 *
 * Fetches from public.ig_media into an "Instagram Media" sheet tab.
 * Keys every row by media_id (Instagram's globally-unique media ID) so
 * that ANY columns you add to the right of the managed columns are
 * preserved across refreshes and stay attached to the same media_id.
 *
 * ── Setup (one-time) ────────────────────────────────────────────────────
 * 1. Extensions → Apps Script → New file → paste this. Save.
 * 2. Script Properties (already set for the Meta Direct — Supabase file):
 *      SUPABASE_URL   =  https://<project>.supabase.co
 *      SUPABASE_ANON  =  eyJ… (anon key)
 * 3. Reload sheet → "Instagram Media" menu appears (top bar).
 * 4. Click "Refresh Instagram Media" → creates the tab if missing, writes
 *    managed columns, preserves your custom columns.
 * 5. Optional: click "Install nightly trigger (midnight)" to auto-refresh.
 *
 * ── How the custom-column preservation works ────────────────────────────
 * The managed columns are the ones IG_HEADERS defines below. Anything you
 * add to the RIGHT of those is treated as "user column" and preserved.
 *
 * On every refresh:
 *   1. Read the existing sheet — build a map keyed by media_id of every
 *      cell value in user columns (anything past IG_HEADERS.length).
 *   2. Fetch fresh ig_media rows from Supabase.
 *   3. Write the sheet from scratch:
 *      · Managed columns → from Supabase (values change day-to-day)
 *      · User columns    → from the map, looked up by media_id
 *   4. Rows in Supabase but not the old sheet → new row, empty user cols
 *      Rows in old sheet but not Supabase → kept at bottom (orphaned)
 *
 * So if you add a column called "Winner Rank" and mark row for
 * media_id=17910123 as "1", that "1" stays attached to that media_id
 * forever — even if the row moves position (sort changes, new posts
 * published, etc.).
 *
 * Do NOT insert user columns BETWEEN the managed ones. Add them at the
 * right. Also don't rename managed column headers — they're the lookup
 * key. If you want to rename, update IG_HEADERS in this file to match.
 * ═══════════════════════════════════════════════════════════════════════
 */

// ── Menu ─────────────────────────────────────────────────────────────
function onOpenIgMedia() { igmBuildMenu(); }

function igmBuildMenu() {
  SpreadsheetApp.getUi()
    .createMenu('Instagram Media')
      .addItem('Refresh Instagram Media',            'igmRefresh')
      .addSeparator()
      .addItem('Install nightly trigger (midnight)', 'igmInstallMidnightTrigger')
      .addItem('Install trigger @ 6 AM IST',         'igmInstall6amTrigger')
      .addItem('Uninstall IG triggers',              'igmUninstallTriggers')
    .addToUi();
}

// ── Config (reuses Meta Direct Supabase properties) ────────────────
function igmGetConfig_() {
  const props = PropertiesService.getScriptProperties();
  const url  = (props.getProperty('SUPABASE_URL')  || '').replace(/\/$/, '');
  const anon = (props.getProperty('SUPABASE_ANON') || '').trim();
  if (!url)  throw new Error('Missing SUPABASE_URL in Script Properties.');
  if (!anon) throw new Error('Missing SUPABASE_ANON in Script Properties.');
  return {url: url, anon: anon};
}

// ── Managed columns ─────────────────────────────────────────────────
// These are pulled fresh from Supabase every refresh. Order = column
// order in the sheet. Any column NOT in this list — if it appears in
// the sheet's header row — is treated as a user column and preserved.
//
// media_id MUST be first — it's the row-identity key.
const IG_MEDIA_TAB   = 'Instagram Media';
const IG_MEDIA_KEY   = 'media_id';
const IG_HEADERS = [
  'media_id', 'ig_username', 'publish_date',
  'media_type', 'media_product_type',
  'permalink', 'shortcode',
  'thumbnail_url',
  'caption',                                    // truncated to 500 chars below
  // Core insights (daily-monitoring signal)
  'reach', 'impressions', 'views',
  'likes', 'comments', 'shares', 'saved', 'total_interactions',
  'profile_visits', 'follows', 'link_clicks',
  // Reels-specific
  'ig_reels_avg_watch_time', 'reels_skip_rate',
  // Meta
  'status', 'insights_status', 'synced_at',
];
const IG_ORDER_BY   = 'publish_date.desc.nullslast';
const IG_ROW_CAP    = 20000;   // safety cap; ig_media has ~2k rows today

// ── Public actions ───────────────────────────────────────────────────
function igmRefresh() {
  const cfg = igmGetConfig_();
  const t0  = Date.now();

  // 1) Read existing sheet — grab user-added columns keyed by media_id.
  const {ss, sh, existingHeaders, userColHeaders, userValsByKey,
         existingKeysSet} = igmReadExisting_();

  // 2) Fetch fresh rows from Supabase.
  const cols = IG_HEADERS.join(',');
  const rows = igmFetch_(cfg, cols);
  console.log('[IGM] fetched ' + rows.length + ' rows from ig_media');

  // 3) Compose the new sheet contents.
  //    Managed cols first (from Supabase), then user cols (from map).
  const seenKeys = new Set();
  const managedRows = rows.map(r => {
    seenKeys.add(String(r[IG_MEDIA_KEY] || ''));
    return IG_HEADERS.map(h => igmCoerce_(r[h], h));
  });

  // Orphans: rows that existed in the sheet with user annotations but are
  // no longer in Supabase (e.g. IG post deleted). Keep them at the bottom
  // with all managed cols blank EXCEPT media_id so the user annotations
  // stay findable + visibly tagged as "not in DB".
  const orphanRows = [];
  for (const key of existingKeysSet) {
    if (seenKeys.has(key) || !key) continue;
    const row = IG_HEADERS.map(h => (h === IG_MEDIA_KEY ? key :
                                     (h === 'status'   ? 'ORPHAN (not in Supabase)' :
                                                          '')));
    orphanRows.push(row);
  }
  console.log('[IGM] orphan rows kept: ' + orphanRows.length);

  // 4) Attach user columns to each row via the media_id lookup.
  const finalHeaders = IG_HEADERS.concat(userColHeaders);
  const fillUserVals = (row) => {
    const key = String(row[0] || '');
    const stash = userValsByKey[key] || {};
    return row.concat(userColHeaders.map(h => stash[h] != null ? stash[h] : ''));
  };
  const allRows = managedRows.concat(orphanRows).map(fillUserVals);

  // 5) Write the sheet from scratch.
  igmWriteSheet_(ss, sh, finalHeaders, allRows,
                 /*managedCount*/ IG_HEADERS.length,
                 /*userCount*/    userColHeaders.length);

  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  const msg = 'IG Media: ' + rows.length + ' fresh rows + ' + orphanRows.length +
              ' orphans, ' + userColHeaders.length + ' user cols preserved (' +
              secs + 's)';
  console.log('[IGM] ' + msg);
  try { SpreadsheetApp.getActiveSpreadsheet().toast(msg, 'Instagram Media', 8); }
  catch (_) {}
}

// ── Read existing sheet: header layout + user-column values ──────────
function igmReadExisting_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(IG_MEDIA_TAB);
  const out = {
    ss: ss, sh: sh,
    existingHeaders: [],
    userColHeaders: [],       // headers not in IG_HEADERS
    userValsByKey: {},        // {media_id: {colName: value}}
    existingKeysSet: new Set(),
  };
  if (!sh) return out;

  const lastCol = sh.getLastColumn();
  const lastRow = sh.getLastRow();
  if (lastCol < 1 || lastRow < 1) return out;

  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
  out.existingHeaders = headers;

  // Identify user columns (anything past IG_HEADERS that isn't a managed name).
  const managedSet = new Set(IG_HEADERS);
  const userColIdx = [];   // [{header, idx}]  1-based idx
  for (let i = 0; i < headers.length; i++) {
    const h = headers[i];
    if (!managedSet.has(h) && h.trim() !== '') {
      out.userColHeaders.push(h);
      userColIdx.push({header: h, idx: i});
    }
  }

  // Find where media_id lives in the existing sheet (may not be col 1
  // if the user reordered — we tolerate that but Refresh will re-order).
  const keyColIdx = headers.indexOf(IG_MEDIA_KEY);
  if (keyColIdx < 0 || lastRow < 2) return out;

  // Bulk-read all data at once
  const data = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();
  for (const row of data) {
    const key = String(row[keyColIdx] || '').trim();
    if (!key) continue;
    out.existingKeysSet.add(key);
    if (userColIdx.length) {
      const stash = {};
      for (const {header, idx} of userColIdx) stash[header] = row[idx];
      out.userValsByKey[key] = stash;
    }
  }
  console.log('[IGM] existing sheet — ' + out.existingKeysSet.size +
              ' rows, ' + out.userColHeaders.length + ' user cols: ' +
              (out.userColHeaders.join(', ') || '(none)'));
  return out;
}

// ── Fetch from Supabase (paginated) ──────────────────────────────────
function igmFetch_(cfg, cols) {
  const BATCH = 10000;
  const out = [];
  let offset = 0;
  for (let page = 0; page < 100; page++) {
    const url = cfg.url + '/rest/v1/ig_media?select=' + cols +
                '&order=' + IG_ORDER_BY +
                '&limit=' + BATCH + '&offset=' + offset;
    const r = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: {apikey: cfg.anon, Authorization: 'Bearer ' + cfg.anon,
                Prefer: 'count=none'},
      muteHttpExceptions: true,
    });
    const code = r.getResponseCode();
    if (code !== 200) {
      throw new Error('Supabase HTTP ' + code + ' fetching ig_media: ' +
                      r.getContentText().slice(0, 300));
    }
    const chunk = JSON.parse(r.getContentText() || '[]');
    if (!Array.isArray(chunk) || chunk.length === 0) break;
    for (const row of chunk) out.push(row);
    if (chunk.length < BATCH) break;
    offset += BATCH;
    if (out.length >= IG_ROW_CAP) break;
  }
  return out;
}

// ── Cell coercion — strings for JSON/dates, truncate long captions ──
function igmCoerce_(v, header) {
  if (v == null) return '';
  if (header === 'caption' && typeof v === 'string') {
    // Sheets caps a cell at 50k chars; captions are often ≤2200 but we
    // shorten to 500 for readability. Full caption is in the DB.
    return v.length > 500 ? v.slice(0, 500) + '…' : v;
  }
  if (header === 'publish_date' || header === 'synced_at') {
    // Return as ISO string — Sheets auto-detects timestamps.
    return String(v).replace('T', ' ').replace(/\..*Z?$/, '');
  }
  if (typeof v === 'object') return JSON.stringify(v);
  return v;
}

// ── Write the sheet ──────────────────────────────────────────────────
function igmWriteSheet_(ss, sh, headers, rows, managedCount, userCount) {
  if (!sh) sh = ss.insertSheet(IG_MEDIA_TAB);

  sh.clearContents(); sh.clearFormats();

  // Header row — managed cols greenish, user cols greyish so it's clear
  // which are yours to edit.
  sh.getRange(1, 1, 1, headers.length).setValues([headers])
    .setFontWeight('bold');
  if (managedCount > 0) {
    sh.getRange(1, 1, 1, managedCount).setBackground('#e6f3e6');   // managed
  }
  if (userCount > 0) {
    sh.getRange(1, managedCount + 1, 1, userCount).setBackground('#fff2cc'); // user
  }
  sh.setFrozenRows(1);
  sh.setFrozenColumns(3);   // media_id, ig_username, publish_date

  if (rows.length) {
    // Chunked write to stay under Sheets' single-call size limit.
    const CHUNK = 3000;
    for (let i = 0; i < rows.length; i += CHUNK) {
      const slice = rows.slice(i, i + CHUNK);
      sh.getRange(2 + i, 1, slice.length, headers.length).setValues(slice);
    }
  }

  // Column widths — some fields deserve extra room
  const filter = sh.getFilter(); if (filter) filter.remove();
  const total = sh.getLastRow();
  if (total > 1) sh.getRange(1, 1, total, headers.length).createFilter();
  for (let c = 1; c <= headers.length; c++) sh.setColumnWidth(c, 120);
  const widen = (n, w) => {
    const i = headers.indexOf(n);
    if (i >= 0) sh.setColumnWidth(i + 1, w);
  };
  widen('caption', 320); widen('permalink', 260); widen('thumbnail_url', 200);
  widen('media_id', 180); widen('publish_date', 150); widen('synced_at', 150);

  // Footer
  sh.getRange(total + 2, 1)
    .setValue('Refreshed from Supabase at ' + new Date().toISOString() +
              ' · ' + rows.length + ' rows · ' + userCount +
              ' user column(s) preserved (yellow-headed)')
    .setFontStyle('italic').setFontColor('#666');
}

// ── Triggers ─────────────────────────────────────────────────────────
// Midnight = atHour(0) fires 00:00-01:00 in the project timezone.
// NOTE: the backend orchestrator fetch_ig_media step usually runs at
// ~01:30-02:00 IST. So a midnight trigger reads YESTERDAY's data —
// use the 6 AM trigger instead if you want the freshest.
function igmInstallMidnightTrigger() {
  igmUninstallTriggers();
  ScriptApp.newTrigger('igmRefresh').timeBased().atHour(0).everyDays(1).create();
  SpreadsheetApp.getActiveSpreadsheet()
    .toast('IG Media trigger installed @ 00:00 (project TZ)',
           'Instagram Media', 5);
}
function igmInstall6amTrigger() {
  igmUninstallTriggers();
  ScriptApp.newTrigger('igmRefresh').timeBased().atHour(6).everyDays(1).create();
  SpreadsheetApp.getActiveSpreadsheet()
    .toast('IG Media trigger installed @ 06:00 IST (after backend refresh)',
           'Instagram Media', 5);
}
function igmUninstallTriggers() {
  let removed = 0;
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'igmRefresh') {
      ScriptApp.deleteTrigger(t); removed++;
    }
  });
  try {
    SpreadsheetApp.getActiveSpreadsheet()
      .toast('Removed ' + removed + ' IG trigger(s)', 'Instagram Media', 4);
  } catch (_) {}
}
