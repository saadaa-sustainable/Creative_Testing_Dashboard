/**
 * IG_Sheet.gs — pull Instagram organic post + insights into a Google Sheet.
 *
 * Setup (one time):
 *   1. Open the Sheet → Extensions → Apps Script
 *   2. Paste this file. Save.
 *   3. Apps Script project → Project Settings (gear icon) → Script Properties:
 *        IG_ACCESS_TOKEN = <your IG token>     ← never paste in code
 *        IG_USER_ID      = 17841412619002528
 *   4. Reload the Sheet. The "Instagram" menu appears.
 *
 * Permissions on the token:
 *   instagram_basic, instagram_manage_insights, pages_show_list, pages_read_engagement
 */

var IG_API = "https://graph.facebook.com/v21.0";

// User-requested 8-column layout in this exact order.
// Status is a user-managed column (left blank for manual entry).
var HEADER = [
  "Publish Date", "Permalink", "Media URL", "Media Type",
  "Reach", "Impressions", "Views", "Status"
];

// Insights metric sets per media_product_type.
// `impressions` is requested for ALL types — it still works for posts created
// before the v22 deprecation (~mid-2024). For newer posts the API returns an
// error which is silently caught by the per-metric fallback in fetchInsights_().
var METRICS_FEED  = ["reach", "impressions", "views"];
var METRICS_REEL  = ["reach", "impressions", "views"];
var METRICS_STORY = ["reach", "impressions"];

function metricsFor(mpt, mtype) {
  if (mpt === "STORY") return METRICS_STORY;
  if (mpt === "REELS" || (mtype === "VIDEO" && mpt === "CLIPS")) return METRICS_REEL;
  return METRICS_FEED;
}

// ─────────────────────────────────────────────────────────────────
// MENU
// ─────────────────────────────────────────────────────────────────
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Instagram")
    .addItem("Fetch from 1 June 2026",      "fetchFromJune1")
    .addItem("Fetch from 1 April 2026",     "fetchFromApril1")
    .addItem("Fetch from earliest post (resumable)", "fetchAllTime")
    .addItem("Show all-time fetch status",           "showHistoricStatus")
    .addItem("Reset all-time / historical state",    "clearHistoricProgress")
    .addSeparator()
    .addItem("Fetch yesterday",              "fetchYesterday")
    .addItem("Fetch range…",                 "fetchPromptRange")
    .addSeparator()
    .addSubMenu(SpreadsheetApp.getUi().createMenu("Historical (resumable)")
      .addItem("Start / continue: 2020 → 2024", "fetchHistoric2020to2024")
      .addItem("Show resume status",            "showHistoricStatus")
      .addItem("Reset resume cursor",           "clearHistoricProgress"))
    .addSeparator()
    .addItem("Set / update token",           "setToken")
    .addToUi();
}

function setToken() {
  var ui = SpreadsheetApp.getUi();
  var r = ui.prompt("Paste IG access token", "Stored in Script Properties. Never echoed.", ui.ButtonSet.OK_CANCEL);
  if (r.getSelectedButton() !== ui.Button.OK) return;
  var t = r.getResponseText().trim();
  if (!t) return;
  PropertiesService.getScriptProperties().setProperty("IG_ACCESS_TOKEN", t);
  ui.alert("Token saved (length " + t.length + "). Last 6: …" + t.slice(-6));
}

function fetchFromJune1()  { fetchRange_("2026-06-01", todayUtc_()); }
function fetchFromApril1() { fetchRange_("2026-04-01", todayUtc_()); }
// All-time uses the resumable historical infrastructure so a 5-min Apps Script
// bail doesn't strand you mid-history. Each click continues from saved cursor;
// "1900-01-01" is just an "older than any IG post ever" sentinel.
function fetchAllTime()    { fetchHistoricRange_("1900-01-01", todayUtc_()); }
function fetchAllTimeContinue() { fetchAllTime(); }  // alias for menu clarity

function fetchYesterday() {
  var d = new Date(); d.setUTCDate(d.getUTCDate() - 1);
  fetchRange_(formatDate_(d), formatDate_(d));
}

function fetchPromptRange() {
  var ui = SpreadsheetApp.getUi();
  var r1 = ui.prompt("From date (inclusive)", "YYYY-MM-DD — or blank for earliest", ui.ButtonSet.OK_CANCEL);
  if (r1.getSelectedButton() !== ui.Button.OK) return;
  var r2 = ui.prompt("To date (inclusive)", "YYYY-MM-DD — or blank for today", ui.ButtonSet.OK_CANCEL);
  if (r2.getSelectedButton() !== ui.Button.OK) return;
  fetchRange_(r1.getResponseText().trim() || null, r2.getResponseText().trim() || todayUtc_());
}

// ─────────────────────────────────────────────────────────────────
// CORE: paginate /media once across the whole range, then insights per item.
// ─────────────────────────────────────────────────────────────────
function fetchRange_(fromDate, toDate) {
  var props = PropertiesService.getScriptProperties();
  var token  = props.getProperty("IG_ACCESS_TOKEN");
  var igUser = props.getProperty("IG_USER_ID");
  if (!token || !igUser) {
    SpreadsheetApp.getUi().alert("Set IG_ACCESS_TOKEN and IG_USER_ID in Script Properties first.");
    return;
  }

  var fromMs = fromDate ? new Date(fromDate + "T00:00:00Z").getTime() : -Infinity;
  var toMs   = new Date(toDate   + "T23:59:59Z").getTime();

  var start = new Date().getTime();
  var sh = SpreadsheetApp.getActiveSheet();
  ensureHeader_(sh);
  // (Dedupe-by-permalink removed per user request: keep duplicates so re-fetches
  // and re-posts of the same media are visible. Sort the sheet by Permalink in
  // Sheets to spot any.)

  SpreadsheetApp.getActiveSpreadsheet().toast(
    "Fetching " + (fromDate || "all-time") + " → " + toDate + " …",
    "Instagram", 4
  );

  // ── 1) page through /media ──
  var fields = "id,media_type,media_product_type,media_url,permalink,timestamp";
  var nextUrl = IG_API + "/" + igUser + "/media?fields=" + encodeURIComponent(fields) + "&limit=50&access_token=" + token;
  var media = [];
  var pages = 0;
  while (nextUrl) {
    pages++;
    var resp = UrlFetchApp.fetch(nextUrl, {muteHttpExceptions:true});
    if (resp.getResponseCode() !== 200) {
      Logger.log("media page %s HTTP %s: %s", pages, resp.getResponseCode(), scrub_(resp.getContentText()).slice(0,200));
      break;
    }
    var json = JSON.parse(resp.getContentText());
    var items = json.data || [];
    if (!items.length) break;
    for (var i=0; i<items.length; i++) {
      var ts = parseTs_(items[i].timestamp);
      if (ts >= fromMs && ts <= toMs) media.push(items[i]);
    }
    // stop pagination once the page's last item predates the range
    var earliest = parseTs_(items[items.length-1].timestamp);
    if (earliest < fromMs) break;
    nextUrl = (json.paging || {}).next || null;
    Utilities.sleep(200);

    // Apps Script 6-min ceiling — bail early with what we have if we get close
    if (new Date().getTime() - start > 5*60*1000) {
      Logger.log("Approaching 6-min limit at page %s; stopping pagination", pages);
      break;
    }
  }

  // ── 2) per-media insights + row build ──
  var rows = [];
  for (var i=0; i<media.length; i++) {
    if (new Date().getTime() - start > 5*60*1000) break;  // soft-bail
    var m = media[i];
    // No dedupe — every fetch appends, so duplicate runs / re-posts both show up
    var mpt = m.media_product_type || "FEED";
    var metrics = metricsFor(mpt, m.media_type);
    var insights = fetchInsights_(token, m.id, metrics);

    // Impressions column: STORY returns real impressions, FEED/REELS return null
    // because Meta retired the metric at v22+. Per Meta's own docs the replacement
    // for "times shown" is `views`, so we fall back to that for non-Story media.
    var impressionsForRow = insights.impressions != null
      ? insights.impressions
      : (mpt !== "STORY" ? (insights.views ?? "") : "");

    rows.push([
      m.timestamp || "",            // Publish Date
      m.permalink || "",            // Permalink
      m.media_url || "",            // Media URL
      m.media_type || "",           // Media Type
      insights.reach        ?? "",  // Reach
      impressionsForRow,            // Impressions (real if API has it, else =views)
      insights.views        ?? "",  // Views
      "",                            // Status (user-managed)
    ]);
    // Per-post debug — view in Apps Script editor → Executions → most recent → Logs
    Logger.log("[%s/%s] %s  %s  reach=%s  imp=%s  views=%s  %s",
               i+1, media.length, m.timestamp, mpt,
               insights.reach, impressionsForRow, insights.views, m.permalink);
    Utilities.sleep(120);
  }

  if (rows.length) {
    sh.getRange(sh.getLastRow()+1, 1, rows.length, HEADER.length).setValues(rows);
  }

  var secs = ((new Date().getTime() - start) / 1000).toFixed(1);
  SpreadsheetApp.getActiveSpreadsheet().toast(
    "Done in " + secs + "s — " + rows.length + " rows appended (duplicates kept)",
    "Instagram", 8
  );
}

// ─────────────────────────────────────────────────────────────────
// HISTORICAL RESUMABLE FETCH (handles many years across multiple runs)
// ─────────────────────────────────────────────────────────────────
// Saved between runs:
//   IG_HIST_FROM, IG_HIST_TO    — target range (e.g. 2020-01-01 .. 2024-12-31)
//   IG_HIST_CURSOR              — next /media page URL (null = restart from newest)
//   IG_HIST_REACHED_RANGE       — "1" once we've stepped past the right edge into range
//   IG_HIST_DONE                — "1" when walker has stepped past the left edge
//   IG_HIST_TOTAL_FETCHED       — running count
function fetchHistoric2020to2024() { fetchHistoricRange_("2020-01-01", "2024-12-31"); }

function showHistoricStatus() {
  var p = PropertiesService.getScriptProperties();
  var info = [
    "Range          : " + (p.getProperty("IG_HIST_FROM") || "(unset)") + " -> " + (p.getProperty("IG_HIST_TO") || "(unset)"),
    "Done?          : " + (p.getProperty("IG_HIST_DONE") === "1" ? "YES (fully fetched)" : "no (more to fetch)"),
    "Total fetched  : " + (p.getProperty("IG_HIST_TOTAL_FETCHED") || "0"),
    "Cursor saved?  : " + (p.getProperty("IG_HIST_CURSOR") ? "yes — next run resumes" : "no — next run starts from newest"),
    "In range yet?  : " + (p.getProperty("IG_HIST_REACHED_RANGE") === "1" ? "yes" : "no (still scrolling from newest)")
  ].join("\n");
  SpreadsheetApp.getUi().alert("Historical fetch status", info, SpreadsheetApp.getUi().ButtonSet.OK);
}

function clearHistoricProgress() {
  ["IG_HIST_FROM","IG_HIST_TO","IG_HIST_CURSOR","IG_HIST_REACHED_RANGE","IG_HIST_DONE","IG_HIST_TOTAL_FETCHED"]
    .forEach(function(k){ PropertiesService.getScriptProperties().deleteProperty(k); });
  SpreadsheetApp.getUi().alert("Historical resume state cleared. Next 'Start / continue' run will restart from newest.");
}

function fetchHistoricRange_(fromDate, toDate) {
  var props = PropertiesService.getScriptProperties();
  var token = props.getProperty("IG_ACCESS_TOKEN");
  var igUser = props.getProperty("IG_USER_ID");
  if (!token || !igUser) {
    SpreadsheetApp.getUi().alert("Set IG_ACCESS_TOKEN and IG_USER_ID in Script Properties first.");
    return;
  }
  // If a different range is requested, reset progress automatically
  var savedFrom = props.getProperty("IG_HIST_FROM"), savedTo = props.getProperty("IG_HIST_TO");
  if (savedFrom !== fromDate || savedTo !== toDate) {
    ["IG_HIST_CURSOR","IG_HIST_REACHED_RANGE","IG_HIST_DONE","IG_HIST_TOTAL_FETCHED"]
      .forEach(function(k){ props.deleteProperty(k); });
    props.setProperty("IG_HIST_FROM", fromDate);
    props.setProperty("IG_HIST_TO",   toDate);
  }
  if (props.getProperty("IG_HIST_DONE") === "1") {
    SpreadsheetApp.getUi().alert("Range " + fromDate + " -> " + toDate + " is already fully fetched. Use 'Reset resume cursor' to redo.");
    return;
  }

  var fromMs = new Date(fromDate + "T00:00:00Z").getTime();
  var toMs   = new Date(toDate   + "T23:59:59Z").getTime();
  var reachedRange = (props.getProperty("IG_HIST_REACHED_RANGE") === "1");
  var totalFetched = parseInt(props.getProperty("IG_HIST_TOTAL_FETCHED") || "0", 10);

  var sh = SpreadsheetApp.getActiveSheet();
  ensureHeader_(sh);

  var fields = "id,media_type,media_product_type,media_url,permalink,timestamp";
  var savedCursor = props.getProperty("IG_HIST_CURSOR");
  var nextUrl = savedCursor || (IG_API + "/" + igUser + "/media?fields=" + encodeURIComponent(fields) + "&limit=50&access_token=" + token);

  var start = new Date().getTime();
  var pageRowsBuf = [];
  var pagesThisRun = 0;
  var fetchedThisRun = 0;

  SpreadsheetApp.getActiveSpreadsheet().toast(
    "Historical: " + fromDate + " -> " + toDate + " — " + (savedCursor ? "resuming" : "starting") +
    " (already saved: " + totalFetched + ")",
    "Instagram", 5
  );

  while (nextUrl) {
    // Soft-bail before exhausting 6-min ceiling
    if (new Date().getTime() - start > 5*60*1000) {
      props.setProperty("IG_HIST_CURSOR", nextUrl);
      break;
    }
    pagesThisRun++;
    var resp = UrlFetchApp.fetch(nextUrl, {muteHttpExceptions:true});
    if (resp.getResponseCode() !== 200) {
      Logger.log("media HTTP %s on page %s: %s", resp.getResponseCode(), pagesThisRun, scrub_(resp.getContentText()).slice(0,200));
      props.setProperty("IG_HIST_CURSOR", nextUrl);
      break;
    }
    var json = JSON.parse(resp.getContentText());
    var items = json.data || [];
    if (!items.length) {
      props.setProperty("IG_HIST_DONE", "1");
      props.deleteProperty("IG_HIST_CURSOR");
      break;
    }
    var earliestInPage = parseTs_(items[items.length-1].timestamp);
    var latestInPage   = parseTs_(items[0].timestamp);

    for (var i = 0; i < items.length; i++) {
      if (new Date().getTime() - start > 5*60*1000) break;
      var m = items[i];
      var ts = parseTs_(m.timestamp);
      if (ts > toMs) continue;                          // newer than target; skip
      if (ts < fromMs) { /* past left edge */ continue; }

      reachedRange = true;
      var mpt = m.media_product_type || "FEED";
      var metrics = metricsFor(mpt, m.media_type);
      var insights = fetchInsights_(token, m.id, metrics);
      var impressionsForRow = insights.impressions != null
        ? insights.impressions
        : (mpt !== "STORY" ? (insights.views ?? "") : "");
      pageRowsBuf.push([
        m.timestamp || "", m.permalink || "", m.media_url || "", m.media_type || "",
        insights.reach ?? "", impressionsForRow, insights.views ?? "", ""
      ]);
      fetchedThisRun++;
      // Per-post debug — view in Apps Script editor → Executions → most recent → Logs
      Logger.log("[hist+%s] %s  %s  reach=%s  imp=%s  views=%s  %s",
                 fetchedThisRun, m.timestamp, mpt,
                 insights.reach, impressionsForRow, insights.views, m.permalink);
      Utilities.sleep(120);
    }

    // Flush this page's rows to the sheet, then advance cursor
    if (pageRowsBuf.length) {
      sh.getRange(sh.getLastRow()+1, 1, pageRowsBuf.length, HEADER.length).setValues(pageRowsBuf);
      totalFetched += pageRowsBuf.length;
      pageRowsBuf = [];
      props.setProperty("IG_HIST_TOTAL_FETCHED", String(totalFetched));
    }
    if (reachedRange) props.setProperty("IG_HIST_REACHED_RANGE", "1");

    // Walked past left edge of the range? Done.
    if (earliestInPage < fromMs) {
      props.setProperty("IG_HIST_DONE", "1");
      props.deleteProperty("IG_HIST_CURSOR");
      break;
    }
    nextUrl = (json.paging || {}).next || null;
    if (!nextUrl) {
      props.setProperty("IG_HIST_DONE", "1");
      props.deleteProperty("IG_HIST_CURSOR");
      break;
    }
    props.setProperty("IG_HIST_CURSOR", nextUrl);
    Utilities.sleep(200);
  }

  var secs = ((new Date().getTime() - start) / 1000).toFixed(1);
  var doneNow = (props.getProperty("IG_HIST_DONE") === "1");
  SpreadsheetApp.getActiveSpreadsheet().toast(
    "Historical run: +" + fetchedThisRun + " rows in " + secs + "s. Total so far: " + totalFetched +
      (doneNow ? "  — DONE." : "  — re-run menu item to continue."),
    "Instagram", 10
  );
}

function fetchInsights_(token, mediaId, metrics) {
  var out = {};
  var url = IG_API + "/" + mediaId + "/insights?metric=" + metrics.join(",") + "&access_token=" + token;
  var resp = UrlFetchApp.fetch(url, {muteHttpExceptions:true});
  if (resp.getResponseCode() === 200) {
    var d = JSON.parse(resp.getContentText());
    (d.data || []).forEach(function(entry){
      var v = (entry.values && entry.values[0]) ? entry.values[0].value : null;
      out[entry.name] = v;
    });
  } else {
    // batch failed — try one at a time
    metrics.forEach(function(met){
      var single = UrlFetchApp.fetch(IG_API + "/" + mediaId + "/insights?metric=" + met + "&access_token=" + token,
                                     {muteHttpExceptions:true});
      if (single.getResponseCode() === 200) {
        var dd = JSON.parse(single.getContentText());
        (dd.data || []).forEach(function(entry){
          var v = (entry.values && entry.values[0]) ? entry.values[0].value : null;
          out[entry.name] = v;
        });
      }
    });
  }
  return out;
}

// ─────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────
function ensureHeader_(sh) {
  if (sh.getLastRow() === 0) {
    sh.getRange(1,1,1,HEADER.length).setValues([HEADER]).setFontWeight("bold");
    sh.setFrozenRows(1);
  }
}

function existingPermalinks_(sh) {
  var seen = {};
  if (sh.getLastRow() < 2) return seen;
  var col = HEADER.indexOf("Permalink") + 1;
  var vals = sh.getRange(2, col, sh.getLastRow()-1, 1).getValues();
  for (var i=0; i<vals.length; i++) {
    var v = String(vals[i][0] || "").trim();
    if (v) seen[v] = true;
  }
  return seen;
}

function todayUtc_()   { return formatDate_(new Date()); }
function formatDate_(d){ var z=function(n){return n<10?"0"+n:""+n;}; return d.getUTCFullYear()+"-"+z(d.getUTCMonth()+1)+"-"+z(d.getUTCDate()); }
function parseTs_(s)   { return new Date(String(s).replace("+0000","Z")).getTime(); }
function scrub_(s)     { return String(s).replace(/access_token=[^&\s]+/g, "access_token=<REDACTED>"); }
