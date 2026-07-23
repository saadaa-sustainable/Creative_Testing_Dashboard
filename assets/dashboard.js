/* ============================================================
   v2 — data + interactions
   ============================================================ */
const params      = new URLSearchParams(window.location.search);
const SUPABASE_URL  = params.get('supabaseUrl')  || '';
const SUPABASE_ANON = params.get('supabaseAnon') || '';
// Saada_Shopify_Data project (siymyhhrpzzbowfqtauf) — dashboarding needs
// direct read access to the sessions rollup for the Landing Page Focus
// overlay. RLS is disabled on public.sessions so this anon key can only
// call the get_sessions_by_lp RPC that we've explicitly granted; it can't
// exfiltrate individual rows over PostgREST beyond what the RPC returns.
// The same key already ships in sync_orders_via_rest.py.
const SHOPIFY_URL  = params.get('shopifyUrl') ||
  'https://siymyhhrpzzbowfqtauf.supabase.co';
const SHOPIFY_ANON = params.get('shopifyAnon') ||
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNpeW15aGhycHp6Ym93ZnF0YXVmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzgzMTQwODEsImV4cCI6MjA5Mzg5MDA4MX0.ocx4jlY3KeXdF_-5JI3_SDcLekmk8hrfWXba7EXEDgo';
const dbStat = document.getElementById('dbStat');

const fmtInt = n => (n==null||isNaN(n)) ? '—' : Math.round(+n).toLocaleString('en-IN');
const fmtRs  = n => (n==null||isNaN(n)) ? '—' : '₹' + fmtInt(n);
const fmtRoas= n => (n==null||isNaN(n)) ? '—' : (+n).toFixed(2)+'x';
// Global 2-decimal formatter — used by the Incremental Reach modal and
// anywhere else outside renderAE()'s local scope.
const fmtNum2 = v => (v==null||v===''||isNaN(+v)) ? '—' : (+v).toLocaleString('en-IN',{minimumFractionDigits:2, maximumFractionDigits:2});

/* ─────────────────────────────────────────────────────────────
   exportVisibleTableCsv — one exporter for every section.
   • Reads column labels + row-keys from the given table's <thead>
     (data-sort / data-aisort → row key; falls back to _colN).
   • Skips any <th> that CSS has hidden (display:none) — this makes
     the Ads Analyse column-picker automatically reshape the CSV.
   • Uses the caller-supplied filtered rows so pagination / sort /
     status filters are already baked in.
   • opts.deriveRow(r) can return an enriched row (e.g. resolve
     Ad Intelligence's step label + ad_status).
   ───────────────────────────────────────────────────────────── */
function exportVisibleTableCsv(tableSel, rows, opts){
  opts = opts || {};
  const table = document.querySelector(tableSel);
  if (!table){ console.warn('[export] table not found:', tableSel); return; }
  const ths = Array.from(table.querySelectorAll('thead th'));
  const cols = [];
  ths.forEach((th, i) => {
    const style = getComputedStyle(th);
    if (style.display === 'none' || style.visibility === 'hidden') return;
    const key   = th.dataset.sort || th.dataset.aisort || null;
    // First text node so header-embedded badges (✎ / arrows / spans)
    // don't leak into the CSV label.
    const label = (th.childNodes[0]?.nodeValue || th.textContent || '')
                    .replace(/\s+/g, ' ').trim() || ('col ' + (i + 1));
    cols.push({ key, label });
  });
  const esc = v => {
    if (v == null) return '';
    // Preserve numeric zero and false — only blank on null/undefined.
    const s = String(v).replace(/"/g, '""');
    return /[",\n\r]/.test(s) ? '"' + s + '"' : s;
  };
  const lines = [cols.map(c => esc(c.label)).join(',')];
  for (const raw of rows){
    const r = opts.deriveRow ? opts.deriveRow(raw) : raw;
    lines.push(cols.map(c => {
      if (opts.getValue){
        const v = opts.getValue(r, c);
        if (v !== undefined) return esc(v);
      }
      return esc(c.key ? r[c.key] : '');
    }).join(','));
  }
  // BOM so Excel opens UTF-8 CSVs with accented / rupee glyphs correctly.
  const blob = new Blob(['﻿' + lines.join('\n')], {type:'text/csv;charset=utf-8'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  const stamp = new Date().toISOString().slice(0,16).replace('T','_').replace(':','');
  a.download = (opts.filenamePrefix || 'export') + '_' + stamp + '.csv';
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1500);
}

let allAds      = [];   // ae_table_view rows → drives Ads Analyse + Lifecycle
let primaryAds  = [];   // primary_table aggregated per ad → drives Creative Testing
                        // (matches the old dashboard exactly, including its
                        // "primary_table only" date coverage)
let thumbsByAdId = {};  // {ad_id: thumbnail_url}  — populated from ad_thumbnails
                        // table (Meta Graph API fetched server-side)
let state = {acct:'', status:'', campaign:'', content:'', tier:'',
             dateFrom:'', dateTo:'',
             // Which ad-date field the top range filters by. Default is
             // 'created' — dashboard opens showing ads created in the last
             // 30d so users see the freshest creative slate. 'delivery' keeps
             // the old semantics (window = primary_table filter, no ad-level
             // filter). 'first_seen' and 'result' are client-side filters
             // on their respective per-ad date fields.
             dateField:'created',
             exclCopy:true,                  // hide ads whose name contains "copy"
             search:'', searchMode:'contains'};
const IS_COPY_RE   = /copy/i;

function detectCtype(name){
  const n = (name||'').toUpperCase();
  if(n.includes('IFAD'))  return 'IFAD';
  if(n.includes('GAD'))   return 'Graphic AD';
  // VID markers — legacy tokens (VRP, NNC, VIDEO, IGP, NO-ID, VID-AD prefix)
  // plus the naming-convention tokens the CT team added later: OSP, CPL,
  // USP, CSR, ITE. Any ad name containing one of these is a video creative.
  if(n.includes('VRP')||n.includes('NNC')||n.includes('VIDEO')||n.includes('IGP')||n.includes('NO-ID')||/^VID-AD/.test(n)
     ||n.includes('OSP')||n.includes('CPL')||n.includes('USP')||n.includes('CSR')||n.includes('ITE')) return 'VID';
  if(n.includes('STATIC')||n.includes('_ST_')||n.includes('+ST+')) return 'STATIC';
  return 'VID';
}

async function fetchAds(){
  if (!SUPABASE_URL || !SUPABASE_ANON){
    dbStat.textContent = 'Missing ?supabaseUrl & ?supabaseAnon';
    return [];
  }
  dbStat.innerHTML = 'Loading <span class="spinner"></span>';
  const cols = [
    'account_name','campaign_name','adset_id','adset_name','ad_id','ad_name','ad_created',
    'first_seen_date','reporting_starts','reporting_ends','date_target_imp_achieved',
    'date_of_result','days_to_result','days_to_target_f1',
    'ad_status','category','f1_pass','f2_pass','f3_pass','f4_pass',
    'impressions','reach','reach_weight_pct','frequency','ltv_reach','ltv_frequency',
    'amount_spent','cost_per_1000',
    'cpc_link','ctr_pct','link_clicks_raw',
    'checkout_compl_pct','cr_link_clicks_pct','atc_lc_pct','atc_count',
    'ci_atc_pct','ci_count',
    'roas_ma','ftewv_count','pct_reach_ftewv',
    'cost_per_ftewv','cost_per_ncp','ncp_count','conv_value','purchases',
    'profit_efficiency','contrib_margin_pct','delivery_eff','sales_spend_eff',
    'blended_eff','cpr_eff','ftv_contrib_eff','ftev_volume',
    'ncp_cost_eff','roas_eff','profit_vol_eff','engagement_count',
    'preview_link','ad_link',
    'shopify_orders','shopify_sales','shopify_top_tier','shopify_roas'
  ].join(',');
  const headers = {'apikey':SUPABASE_ANON,'Authorization':'Bearer '+SUPABASE_ANON};
  let out=[], offset=0, BATCH=1000;
  while(true){
    const url=SUPABASE_URL+'/rest/v1/ae_table_view?select='+cols+
              '&order=amount_spent.desc.nullslast&limit='+BATCH+'&offset='+offset;
    const r=await fetch(url,{headers});
    if(!r.ok){dbStat.textContent='Error: '+r.status; break}
    const j=await r.json();
    out=out.concat(j);
    if(j.length<BATCH) break;
    offset+=BATCH;
    if(offset>=20000) break;
  }
  dbStat.innerHTML='Loaded <span class="mono">'+fmtInt(out.length)+'</span> ads';
  return out;
}

/* Pull ad_thumbnails once and index by ad_id. Each entry stores BOTH the
   small thumbnail (for inline 44x44 table cells) AND the higher-res image
   (used in the drawer preview where upscaling a small thumb shows blur). */
const thumbUrlOf  = e => (e ? e.t : '') || '';
const previewUrlOf = e => (e ? (e.i || e.t) : '') || '';
/* Per-ad incremental reach snapshot from public.ae_reach_recent.
   Formula (computed in the view): incremental_reach = latest_daily_reach -
   previous_daily_reach; cost_per_incremental_reach = latest_daily_spend /
   incremental_reach. Merged into allAds by ad_id at load time. */
/* Per-ad frequency crossings from public.ae_freq_lifecycle. For every ad,
   the date it first crossed cumulative frequency 1.0 / 1.5 / 2.0 / 2.5 / 3.0
   plus impressions/reach/spend ON that day. Fed into the Creative Lifecycle
   frequency-bucket view. */
async function fetchFreqLifecycle(){
  if (!SUPABASE_URL || !SUPABASE_ANON) return {};
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   Prefer:'count=none'};
  const out = {};
  const cols = 'ad_id,first_date,last_date,max_cum_freq,'+
               'd_1,imp_at_1,reach_at_1,spend_at_1,'+
               'd_1_5,imp_at_1_5,reach_at_1_5,spend_at_1_5,'+
               'd_2,imp_at_2,reach_at_2,spend_at_2,'+
               'd_2_5,imp_at_2_5,reach_at_2_5,spend_at_2_5,'+
               'd_3,imp_at_3,reach_at_3,spend_at_3';
  let offset = 0, BATCH = 1000;
  while (true){
    const url = SUPABASE_URL+'/rest/v1/ae_freq_lifecycle?select='+cols+
                '&limit='+BATCH+'&offset='+offset;
    const r = await fetch(url,{headers});
    if (!r.ok){
      console.warn('[fetchFreqLifecycle] HTTP', r.status, 'at offset', offset,
                   '— frequency buckets will show 0. Body:', await r.text().catch(()=>'?'));
      break;
    }
    const j = await r.json();
    if (!Array.isArray(j) || !j.length) break;
    for (const row of j){ if (row.ad_id) out[row.ad_id] = row; }
    if (j.length < BATCH) break;
    offset += BATCH;
  }
  return out;
}
let freqLifecycleByAdId = {};

async function fetchReachRecent(){
  if (!SUPABASE_URL || !SUPABASE_ANON) return {};
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   Prefer:'count=none'};
  const out = {};
  let offset = 0, BATCH = 1000;
  const cols = 'ad_id,latest_date,latest_reach,previous_reach,latest_spend,'+
               'incremental_reach,cost_per_incremental_reach,cost_per_1000_incremental_reach';
  while (true){
    const url = SUPABASE_URL+'/rest/v1/ae_reach_recent?select='+cols+
                '&limit='+BATCH+'&offset='+offset;
    const r = await fetch(url,{headers});
    if (!r.ok) break;
    const j = await r.json();
    if (!Array.isArray(j) || !j.length) break;
    for (const row of j){
      if (row.ad_id) out[row.ad_id] = row;
    }
    if (j.length < BATCH) break;
    offset += BATCH;
  }
  return out;
}
let reachRecentByAdId = {};

async function fetchThumbnails(){
  if (!SUPABASE_URL || !SUPABASE_ANON) return {};
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   Prefer:'count=none'};
  const out = {};
  let offset = 0, BATCH = 1000;
  while (true){
    const url = SUPABASE_URL+'/rest/v1/ad_thumbnails?select=ad_id,thumbnail_url,image_url,instagram_permalink,fb_permalink,video_source_url,video_source_fetched_at'+
                '&or=(thumbnail_url.not.is.null,image_url.not.is.null,video_source_url.not.is.null)'+
                '&limit='+BATCH+'&offset='+offset;
    const r = await fetch(url,{headers});
    if (!r.ok) break;
    const j = await r.json();
    if (!Array.isArray(j) || !j.length) break;
    for (const row of j){
      if (!row.ad_id) continue;
      out[row.ad_id] = {
        t:   row.thumbnail_url || '',
        i:   row.image_url || '',
        ig:  row.instagram_permalink || '',
        fb:  row.fb_permalink || '',
        v:   row.video_source_url || '',
        vt:  row.video_source_fetched_at || '',
      };
    }
    if (j.length < BATCH) break;
    offset += BATCH;
  }
  return out;
}

/* ─────────────────────────────────────────────────────────────────
   PRIMARY TABLE — same path the OLD dashboard uses for Creative
   Testing.  Fetches raw daily rows from `primary_table`, aggregates
   per ad_id client-side, derives ROAS / cost-per-NCP / cost-per-FTEWV,
   then applies the exact same F1-F4 thresholds the OLD dashboard
   applied (50000 / 3 / 525 / 12).  Returns per-ad rows shaped like
   ae_table_view rows so the existing Creative Testing renderers
   (renderKpis, renderFunnel, renderFocusStrips, etc.) work unchanged.
   ───────────────────────────────────────────────────────────────── */
const CT_ACTIVE_STATUSES = new Set([
  'ACTIVE','WITH_ISSUES','PENDING_REVIEW','PREAPPROVED','IN_PROCESS',
  'PENDING_BILLING_INFO','CAMPAIGN_PAUSED','ADSET_PAUSED'
]);
// 14-day evaluation buffer: any ad that would fall to Discarded but is
// still inside its first 14 days from ad_created gets a "Result Awaited"
// grace period instead — no filter has fired yet, but the ad has not had
// enough delivery time for the verdict to be fair.  After day 14 the
// grace period expires and the ad reverts to Discarded on the next
// refresh, matching the definition users expect for that category.
const CT_BUFFER_DAYS = 14;
function _ctCategory(impr, roas, cpncp, cpft, t, adCreated){
  const f1 = impr  >= t.f1;
  const f2 = roas  >= t.f2;
  const f3 = cpncp > 0 && cpncp <= t.f3;
  const f4 = cpft  > 0 && cpft  <= t.f4;
  let cat;
  if      (f1 && (f2 || f3) && f4) cat = 'Incremental Winner';
  else if (f1 && (f2 || f3))       cat = 'Winner';
  else if (f1 && f4)               cat = 'P0 analysis';
  else if (f1)                     cat = 'P1 analysis';
  else if (f2)                     cat = 'P2 analysis';
  else                             cat = 'Discarded';
  if (cat === 'Discarded' && adCreated){
    const created = new Date(adCreated);
    if (!isNaN(created)){
      const daysSince = (Date.now() - created.getTime()) / 86400000;
      if (daysSince < CT_BUFFER_DAYS) cat = 'Result Awaited';
    }
  }
  return {f1_pass:f1, f2_pass:f2, f3_pass:f3, f4_pass:f4, category:cat};
}
/* Fast-path: pre-computed per-ad rollup from results_table. The pipeline's
   results_sync.py writes one row per (account, date_field) with everything
   already aggregated, so the dashboard can render in 1 HTTP fetch instead
   of paging primary_table. Two rows per All-Accounts snapshot exist:
     date_field='delivery' → ads with impressions in the window
     date_field='created'  → ads whose ad_created_date is in the window
   Returns null when no usable cache exists or its window+field don't
   match the requested one. */
async function fetchPrimaryFromCache(dateFrom, dateTo, dateField){
  if (!SUPABASE_URL || !SUPABASE_ANON) return null;
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   Prefer:'count=none'};
  dbStat.innerHTML = 'Loading cached results_table <span class="spinner"></span>';
  // Only 'delivery' and 'created' variants are precomputed. Other field
  // selectors (first_seen, result) fall back to the live aggregator.
  const cacheField = (dateField === 'created') ? 'created' : 'delivery';
  // Extra columns: pre-computed Overview – Performance totals (populated by
  // results_sync.py). Reading them here avoids the extra RPC round-trip when
  // the user is on the standard "last 30 days" window.
  const url = SUPABASE_URL +
    '/rest/v1/results_table?account_name=eq.All%20Accounts' +
    '&date_field=eq.' + cacheField +
    '&select=ads_json,data_date_from,data_date_to,date_field,computed_at,' +
    'total_spend,total_impr,total_conv_value,total_video_plays,total_thru_plays,' +
    'total_out_clicks,total_post_eng,hook_rate,hold_rate,thruplay_rate,' +
    'outbound_ctr,engagement_rate,ct_roas' +
    '&order=computed_at.desc&limit=1';
  const r = await fetch(url, {headers});
  if (!r.ok) return null;
  const j = await r.json();
  if (!Array.isArray(j) || !j.length) return null;
  const row = j[0];
  // Cache window must be a ±1-day match to the requested window. The
  // pipeline runs in IST (`date.today()` in results_sync.py) while
  // client browsers can be anywhere; allowing 1-day drift means the
  // cache still hits for clients whose local "today" is off by a
  // timezone boundary from the pipeline's snapshot.
  const _driftDays = (a, b) => {
    if (!a || !b) return 999;
    const da = new Date(a+'T00:00:00Z'), db = new Date(b+'T00:00:00Z');
    return Math.abs((da - db) / 86400000);
  };
  if (dateFrom && _driftDays(row.data_date_from, dateFrom) > 1) return null;
  if (dateTo   && _driftDays(row.data_date_to,   dateTo)   > 1) return null;
  // Stash pre-computed Overview totals so renderOverviewPerf paints instantly
  // without hitting the RPC.  Keyed on the row's window so a stale snapshot
  // doesn't leak into a subsequent custom-window fetch.
  window._ctOverviewSnapshot = {
    fromDate:      row.data_date_from,
    toDate:        row.data_date_to,
    computedAt:    row.computed_at,
    dateField:     row.date_field,
    spend:         +row.total_spend        || 0,
    impressions:   +row.total_impr         || 0,
    convValue:     +row.total_conv_value   || 0,
    threeSecPlays: +row.total_video_plays  || 0,
    thruPlays:     +row.total_thru_plays   || 0,
    outboundClk:   +row.total_out_clicks   || 0,
    postEng:       +row.total_post_eng     || 0,
    hookRate:      +row.hook_rate          || 0,
    holdRate:      +row.hold_rate          || 0,
    thruplayRate:  +row.thruplay_rate      || 0,
    outboundCtr:   +row.outbound_ctr       || 0,
    engagementRate:+row.engagement_rate    || 0,
    ctRoas:        +row.ct_roas            || 0,
  };
  const ads = row.ads_json || [];
  // Map compact cache keys → the same row shape fetchPrimaryAggregated emits
  const t = (typeof aeReadThresholds === 'function')
            ? aeReadThresholds()
            : {f1:50000, f2:3, f3:525, f4:12};
  const out = [];
  for (const a of ads){
    const impr  = +a.impr  || 0;
    const spend = +a.spend || 0;
    const conv  = +a.convV || 0;
    const reach = +a.reach || 0;
    const ftewv = +a.ftewv || 0;
    const ncp   = +a.ncp   || 0;
    const roas  = spend > 0 ? conv / spend : 0;
    const cpft  = (+a.cpf || 0) || (ftewv > 0 ? spend / ftewv : 0);
    const cpncp = (+a.cpn || 0) || (ncp   > 0 ? spend / ncp   : 0);
    const cat = _ctCategory(impr, roas, cpncp, cpft, t, a.adCreated || null);
    out.push({
      ad_id:        a.adId || '',
      ad_name:      a.adName || '',
      account_name: a.acct || '',
      campaign_name:a.campName || '',
      adset_id:     '',       // not in cache; ok for KPI/funnel uses
      adset_name:   '',
      ad_status:    a.adStatus || '',
      ad_created:   a.adCreated || null,
      preview_link: a.preview || '',
      ad_link:      a.adLink || '',
      impressions:  impr, reach: reach,
      frequency:    reach > 0 ? impr / reach : 0,
      amount_spent: spend, conv_value: conv,
      purchases:    0,        // not in cache; rarely used in CT view
      ftewv_count:  ftewv, ncp_count: ncp,
      roas_ma:      roas, cost_per_ftewv: cpft, cost_per_ncp: cpncp,
      ...cat
    });
  }
  dbStat.innerHTML = 'Loaded <span class="mono">'+fmtInt(out.length)+
                     '</span> ads (results_table cache · ' +
                     (row.date_field || 'delivery') + ' · ' +
                     row.data_date_from + ' → ' + row.data_date_to + ')';
  return out;
}

async function fetchPrimaryAggregated(dateFrom, dateTo){
  if (!SUPABASE_URL || !SUPABASE_ANON){
    dbStat.textContent = 'Missing ?supabaseUrl & ?supabaseAnon';
    return [];
  }
  // Old dashboard aggregates daily rows in the SELECTED date range, not the
  // ad's lifetime. Push the same date filter to the SQL query so categories,
  // spend and KPIs reflect the window (default = last 30 days incl. today).
  const dateFilter =
    (dateFrom ? '&date=gte.'+dateFrom : '') +
    (dateTo   ? '&date=lte.'+dateTo   : '');
  const label = (dateFrom || dateTo) ? 'primary_table '+(dateFrom||'…')+'→'+(dateTo||'…')
                                     : 'primary_table';
  dbStat.innerHTML = 'Loading '+label+' <span class="spinner"></span>';
  const cols = [
    'account_name','date','ad_name','ad_id','adset_id','adset_name','campaign_name','ad_status',
    'impressions','amount_spent_inr','reach','outbound_clicks','conversion_value','purchases',
    'ad_created_date','ftewv_count','cost_per_ftewv','ncp_count','cost_per_ncp',
    'preview_link','ad_link'
    // (Overview – Performance now reads its totals from the
    //  get_overview_perf_totals RPC, so we no longer need to page
    //  three_sec_video_plays/thruplays/etc. per-ad.)
  ].join(',');
  const headers = {apikey:SUPABASE_ANON,Authorization:'Bearer '+SUPABASE_ANON,Prefer:'count=none'};
  const BATCH = 1000;
  const map = Object.create(null);
  let total = 0;
  for (const acctName of ['Raho Saadaa','Third Ad Account - SD','Fourth Ad Account - SD']){
    let offset = 0;
    while (true){
      const url = SUPABASE_URL+'/rest/v1/primary_table?account_name=eq.'+
        encodeURIComponent(acctName)+'&select='+cols+
        dateFilter+
        '&order=date.desc&limit='+BATCH+'&offset='+offset;
      const r = await fetch(url,{headers});
      if (!r.ok){ dbStat.textContent = 'primary_table HTTP '+r.status; break; }
      const chunk = await r.json();
      if (!Array.isArray(chunk) || !chunk.length) break;
      for (const row of chunk){
        const key = row.ad_id || row.ad_name;
        if (!key) continue;
        let m = map[key];
        if (!m){
          m = map[key] = {
            ad_id:row.ad_id||'', ad_name:row.ad_name||'',
            account_name:row.account_name||'', campaign_name:row.campaign_name||'',
            adset_id:row.adset_id||'', adset_name:row.adset_name||'',
            ad_status:row.ad_status||'', ad_created:row.ad_created_date||null,
            preview_link:row.preview_link||'', ad_link:row.ad_link||'',
            _impr:0, _spend:0, _conv:0, _purch:0, _reach:0,
            _ftewv:0, _ncp:0, _outClicks:0,
            _cpft_sheet:[], _cpncp_sheet:[]
          };
        }
        // Most-active status wins (matches old dashboard line 1620-1622)
        const st = (row.ad_status||'').toUpperCase();
        if (row.ad_status && (!m.ad_status || st === 'ACTIVE')) m.ad_status = row.ad_status;
        // Most-recent created date stays (rows are date.desc-ordered)
        if (row.ad_created_date && !m.ad_created) m.ad_created = row.ad_created_date;
        if (row.preview_link && !m.preview_link) m.preview_link = row.preview_link;
        if (row.ad_link      && !m.ad_link)      m.ad_link      = row.ad_link;
        // Sums
        const imp = +row.impressions || 0;
        if (imp > 0) m._impr += imp;
        m._spend  += +row.amount_spent_inr  || 0;
        m._conv   += +row.conversion_value  || 0;
        m._purch  += +row.purchases         || 0;
        m._reach  += +row.reach             || 0;
        m._ftewv  += +row.ftewv_count       || 0;
        m._ncp    += +row.ncp_count         || 0;
        m._outClicks += +row.outbound_clicks|| 0;
        const cpft = +row.cost_per_ftewv;
        const cpncp = +row.cost_per_ncp;
        if (cpft  && isFinite(cpft))  m._cpft_sheet.push(cpft);
        if (cpncp && isFinite(cpncp)) m._cpncp_sheet.push(cpncp);
      }
      total += chunk.length;
      if (chunk.length < BATCH) break;
      offset += BATCH;
    }
  }
  // Derive per-ad metrics and apply F1-F4 with current threshold inputs
  const t = (typeof aeReadThresholds === 'function')
            ? aeReadThresholds()
            : {f1:50000, f2:3, f3:525, f4:12};
  const out = [];
  for (const k in map){
    const m = map[k];
    // OLD dashboard drops ads with zero impressions UNLESS their status is "alive"
    if (m._impr <= 0 && !CT_ACTIVE_STATUSES.has((m.ad_status||'').toUpperCase())) continue;
    const roas  = m._spend > 0 ? m._conv  / m._spend : 0;
    const cpft  = m._ftewv > 0 ? m._spend / m._ftewv
                  : (m._cpft_sheet.length ? m._cpft_sheet.reduce((a,b)=>a+b,0)/m._cpft_sheet.length : 0);
    const cpncp = m._ncp   > 0 ? m._spend / m._ncp
                  : (m._cpncp_sheet.length ? m._cpncp_sheet.reduce((a,b)=>a+b,0)/m._cpncp_sheet.length : 0);
    const cat = _ctCategory(m._impr, roas, cpncp, cpft, t, m.ad_created || null);
    out.push({
      ad_id:m.ad_id, ad_name:m.ad_name, account_name:m.account_name,
      campaign_name:m.campaign_name, adset_id:m.adset_id, adset_name:m.adset_name,
      ad_status:m.ad_status, ad_created:m.ad_created,
      preview_link:m.preview_link, ad_link:m.ad_link,
      impressions:m._impr, reach:m._reach,
      frequency: m._reach > 0 ? m._impr / m._reach : 0,
      amount_spent:m._spend, conv_value:m._conv, purchases:m._purch,
      ftewv_count:m._ftewv, ncp_count:m._ncp,
      roas_ma:roas, cost_per_ftewv:cpft, cost_per_ncp:cpncp,
      ...cat
    });
  }
  dbStat.innerHTML = 'Loaded <span class="mono">'+fmtInt(out.length)+'</span> ads (primary_table)';
  return out;
}
/* Re-applies categories to primaryAds using the current threshold inputs.
   Mirrors aeApplyCurrentThresholds() but for the primary-table-derived set. */
function ctApplyCurrentThresholds(){
  const t = (typeof aeReadThresholds === 'function')
            ? aeReadThresholds()
            : {f1:50000, f2:3, f3:525, f4:12};
  for (const r of primaryAds){
    const cat = _ctCategory(+r.impressions || 0, +r.roas_ma || 0,
                            +r.cost_per_ncp || 0, +r.cost_per_ftewv || 0, t,
                            r.ad_created || null);
    r.f1_pass = cat.f1_pass; r.f2_pass = cat.f2_pass;
    r.f3_pass = cat.f3_pass; r.f4_pass = cat.f4_pass;
    r.category = cat.category;
  }
}

function filtered(rows){
  let r = rows.slice();
  if (state.acct)     r = r.filter(x => (x.account_name||'') === state.acct);
  if (state.status)   r = r.filter(x => (x.ad_status||'').toUpperCase() === state.status);
  if (state.campaign) r = r.filter(x => (x.campaign_name||'') === state.campaign);
  if (state.tier)     r = r.filter(x => (x.category||'') === state.tier);
  if (state.content)  r = r.filter(x => detectCtype(x.ad_name) === state.content);
  // state.dateFrom/dateTo are intentionally NOT applied client-side here
  // anymore — Creative Testing's primary_table fetch already filters daily
  // rows by `date` at the SQL layer, so ANY ad with activity in the window
  // is included regardless of when it was originally created. (Old
  // dashboard behaved the same: an ad created in 2023 still running today
  // appears in the "Last 30d" view.)
  // Exclude Copy — hide ads whose name contains "copy" (case-insensitive).
  // ON by default, matching the old dashboard.
  if (state.exclCopy){
    r = r.filter(x => !IS_COPY_RE.test(String(x.ad_name || '')));
  }
  // Ad-date filter — the top range filters ads by one of four date fields
  // (dateField selector next to the presets). 'delivery' is a no-op here
  // because it's already enforced by the primary_table server-side date
  // filter at fetch time (only ads with impressions in the window get
  // aggregated). The remaining three are client-side filters on the ad's
  // own timestamp fields, enriched onto primaryAds from ae_table_view.
  const _fieldKey = state.dateField === 'created'    ? 'ad_created'
                  : state.dateField === 'first_seen' ? 'first_seen_date'
                  : state.dateField === 'result'     ? 'date_of_result'
                  : null;
  if (_fieldKey && (state.dateFrom || state.dateTo)){
    const df = state.dateFrom ? new Date(state.dateFrom+'T00:00:00') : null;
    const dt = state.dateTo   ? new Date(state.dateTo  +'T23:59:59') : null;
    r = r.filter(x => {
      const v = x[_fieldKey];
      if (!v) return false;
      const d = new Date(v);
      if (df && d < df) return false;
      if (dt && d > dt) return false;
      return true;
    });
  }
  // Multi-Filter replaces the old simple search bar — chainable Field x Op x
  // Value rules, AND-merged. Empty rules pass-through.
  if (Array.isArray(ctRules) && ctRules.length){
    r = r.filter(ctMfMatchAll);
  }
  return r;
}

/* ── Date presets ──────────────────────────────────────────────────── */
function applyDatePreset(p){
  const today = new Date(); today.setHours(0,0,0,0);
  const iso = d => d.toISOString().slice(0,10);
  const fromInp = document.getElementById('fDateFrom');
  const toInp   = document.getElementById('fDateTo');
  let from=null, to=null;
  if (p === 'lifetime')      { from = ''; to = ''; }
  else if (p === 'custom')   { /* leave whatever's in the inputs */ from = fromInp.value; to = toInp.value; }
  else {
    const t = new Date(today);
    if (p === 'last7')      { from = new Date(t); from.setDate(t.getDate()-6); to = t; }
    else if (p === 'last30'){ from = new Date(t); from.setDate(t.getDate()-29); to = t; }
    else if (p === 'last90'){ from = new Date(t); from.setDate(t.getDate()-89); to = t; }
    else if (p === 'thisMonth'){ from = new Date(t.getFullYear(),t.getMonth(),1); to = t; }
    else if (p === 'lastMonth'){
      from = new Date(t.getFullYear(),t.getMonth()-1,1);
      to   = new Date(t.getFullYear(),t.getMonth(),0);
    }
    from = iso(from); to = iso(to);
  }
  fromInp.value = from || '';
  toInp.value   = to   || '';
  state.dateFrom = fromInp.value;
  state.dateTo   = toInp.value;
}

function populateCampaignDropdown(rows){
  const sel = document.getElementById('fCampaign');
  if (!sel) return;
  const seen = new Set();
  const opts = ['<option value="">All campaigns</option>'];
  for (const r of rows){
    const c = r.campaign_name || '';
    if (c && !seen.has(c)) { seen.add(c); opts.push('<option>'+c.replace(/</g,'&lt;')+'</option>'); }
  }
  sel.innerHTML = opts.join('');
}

function renderKpis(rows){
  const map = {'Winner':'winner','Incremental Winner':'incr','P0 analysis':'pri',
               'P1 analysis':'a1','P2 analysis':'a2',
               'Result Awaited':'ra','Discarded':'dis'};
  const buckets = Object.fromEntries(Object.keys(map).map(k=>[k,{n:0,sp:0}]));
  for (const r of rows){
    const c = r.category || 'Discarded';
    if (!buckets[c]) continue;
    buckets[c].n += 1;
    buckets[c].sp += (+r.amount_spent || 0);
  }
  for (const [c, suf] of Object.entries(map)){
    document.getElementById('kp-'+suf).textContent = fmtInt(buckets[c]?.n);
    document.getElementById('kp-'+suf+'-sp').textContent = fmtRs(buckets[c]?.sp);
  }
}

// Landing-page taxonomy (Product in Focus / Product Mix).  Codes are matched
// with a leading token boundary (start-of-name or [-+_ ]) so shorthand like
// "IHP" doesn't collide with "HP".  Order matters — first match wins.
function classifyProduct(adName){
  const s = (adName||'').toUpperCase();
  const has = (code) => new RegExp('(^|[-+_ ])' + code + '($|[-+_ ])').test(s);
  if (has('CLP'))                                    return 'Collection page';
  if (has('PDP') || has('VRP') || has('PP'))          return 'Product page';
  if (has('CTG') || has('CAT') || has('CATEGORY'))    return 'Category page';
  if (has('HP')  || has('HOME'))                      return 'Home page';
  return 'Others';
}
// Manual per-ad ctype overrides. Populated from public.ad_ctype_overrides
// once at load; the Reclassify Others modal writes new entries here and to
// the DB. When an ad has an override, it wins over marker detection so
// the team can send a mis-named or unmarked ad to whichever Creative
// Focus bucket they intended.
let ctypeOverrideByAdId = Object.create(null);
async function fetchCtypeOverrides(){
  if (!SUPABASE_URL || !SUPABASE_ANON) return {};
  const url = SUPABASE_URL + '/rest/v1/ad_ctype_overrides?select=ad_id,ctype';
  const r = await fetch(url, {headers:{apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON}});
  if (!r.ok) return {};
  const rows = await r.json();
  const out = Object.create(null);
  if (Array.isArray(rows)) for (const row of rows){
    if (row.ad_id) out[row.ad_id] = row.ctype;
  }
  return out;
}

/* Manual ad_id → asset_id mapping. Populated from an external Google
   Sheet (import script pending). The Ads Analyse + Ad Intelligence
   tables both stamp the asset_id column from this map at render time. */
let assetIdByAdId = Object.create(null);
async function fetchAssetIds(){
  if (!SUPABASE_URL || !SUPABASE_ANON) return {};
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   Prefer:'count=none'};
  const out = Object.create(null);
  let offset = 0, BATCH = 5000;
  while (true){
    const url = SUPABASE_URL + '/rest/v1/ad_asset_ids?select=ad_id,asset_id'
              + '&limit=' + BATCH + '&offset=' + offset;
    const r = await fetch(url, {headers});
    if (!r.ok) break;
    const rows = await r.json();
    if (!Array.isArray(rows) || !rows.length) break;
    for (const row of rows){
      if (row.ad_id && row.asset_id) out[String(row.ad_id)] = String(row.asset_id);
    }
    if (rows.length < BATCH) break;
    offset += BATCH;
  }
  return out;
}

/* Persist a manual asset_id edit to public.ad_asset_ids. Empty string
   clears the row (DELETE); otherwise upsert. Returns true on success.

   Called by the inline editor on the Asset ID cells in both Ads Analyse
   and Ad Intelligence. Also updates the client-side assetIdByAdId map
   so subsequent renders show the fresh value without a page reload. */
async function persistAssetId(adId, assetId){
  if (!adId || !SUPABASE_URL || !SUPABASE_ANON) return false;
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   'Content-Type':'application/json',
                   Prefer:'resolution=merge-duplicates,return=minimal'};
  const clean = (assetId || '').trim();
  try {
    if (!clean){
      const r = await fetch(SUPABASE_URL + '/rest/v1/ad_asset_ids?ad_id=eq.' + encodeURIComponent(adId),
                            {method:'DELETE', headers});
      if (!r.ok) return false;
      delete assetIdByAdId[adId];
    } else {
      const r = await fetch(SUPABASE_URL + '/rest/v1/ad_asset_ids?on_conflict=ad_id',
                            {method:'POST', headers,
                             body: JSON.stringify([{ad_id: String(adId),
                                                    asset_id: clean,
                                                    source_url: 'inline-edit'}])});
      if (!r.ok) return false;
      assetIdByAdId[adId] = clean;
    }
    return true;
  } catch (e){ return false; }
}

/* Wire a <td> so a single click swaps its text for an <input>. The
   input auto-selects, saves on Enter / blur, cancels on Escape.
   Callers pass the ad_id and an onSaved(newVal, td) hook to refresh
   any denormalised row state (e.g. r.asset_id) or re-render neighbours.
   Called from the Ads Analyse click handler and the Ad Intelligence
   click handler; nothing else needs to know about the mechanics. */
function installAssetIdCellEditor(td, adId, opts){
  if (!td || td.dataset.assetEditorBound === '1') return;
  td.dataset.assetEditorBound = '1';
  td.addEventListener('click', e => {
    if (!window.assetEditMode) return;         // gated behind Edit Asset ID toggle
    if (td.querySelector('input')) return;    // already editing
    e.stopPropagation();                       // don't trigger row-level nav
    const current = (assetIdByAdId[adId] || '').trim();
    const originalHtml = td.innerHTML;
    td.innerHTML = '';
    const input = document.createElement('input');
    input.type = 'text';
    input.value = current;
    input.placeholder = 'Asset ID';
    input.style.cssText = 'width:100%;box-sizing:border-box;padding:3px 6px;'+
                         'font-family:JetBrains Mono,monospace;font-size:11.5px;'+
                         'border:1px solid var(--accent-yellow);border-radius:4px;'+
                         'background:var(--bg-white);color:var(--text-primary);outline:none';
    td.appendChild(input);
    input.focus(); input.select();
    let done = false;
    const finish = async (save) => {
      if (done) return;
      done = true;
      if (!save){
        td.innerHTML = originalHtml;
        return;
      }
      const val = input.value;
      td.innerHTML = '<span style="color:var(--text-tertiary)">saving…</span>';
      const ok = await persistAssetId(adId, val);
      if (!ok){
        td.innerHTML = '<span style="color:var(--error-text)" title="save failed — try again">✗ '+
                        (val || '—').replace(/</g,'&lt;')+'</span>';
        return;
      }
      const cleaned = (val || '').trim();
      td.textContent = cleaned || '—';
      if (opts && typeof opts.onSaved === 'function') opts.onSaved(cleaned, td);
    };
    input.addEventListener('keydown', ev => {
      if (ev.key === 'Enter'){ ev.preventDefault(); finish(true); }
      else if (ev.key === 'Escape'){ ev.preventDefault(); finish(false); }
    });
    input.addEventListener('blur', () => finish(true));
  });
}

/* Global toggle: flip asset-id editability on/off across BOTH tables
   (Ads Analyse + Ad Intelligence). Buttons in each toolbar call this;
   the flag lives on window so every editor sees the same state. */
window.assetEditMode = false;
function setAssetEditMode(on){
  window.assetEditMode = !!on;
  document.body.classList.toggle('asset-edit-on', window.assetEditMode);
  ['aeEditAssetBtn','aiEditAssetBtn'].forEach(id => {
    const b = document.getElementById(id);
    if (!b) return;
    b.classList.toggle('active', window.assetEditMode);
    b.textContent = window.assetEditMode ? '✓ Editing Asset ID' : '✎ Edit Asset ID';
    b.title = window.assetEditMode
      ? 'Editing is ON — click any Asset ID cell to edit. Click again to lock.'
      : 'Turn on to click-edit Asset ID cells';
  });
  ['aeEditAssetBadge','aiEditAssetBadge'].forEach(id => {
    const b = document.getElementById(id);
    if (!b) return;
    b.classList.toggle('active', window.assetEditMode);
    b.title = window.assetEditMode
      ? 'Editing ON — click a cell to edit. Click badge again to lock.'
      : 'Click to enable Asset ID editing';
  });
  document.querySelectorAll('.ae-asset-cell, .ai-asset-cell').forEach(td => {
    td.style.cursor = window.assetEditMode ? 'text' : '';
    td.title = window.assetEditMode ? 'Click to edit asset ID' : '';
  });
}
// Wire the in-header pencil badges as soon as the DOM is ready — they live in
// static markup so we can bind unconditionally. Stops propagation so the click
// doesn't also fire the column-sort handler on the surrounding <th>.
document.addEventListener('DOMContentLoaded', () => {
  ['aeEditAssetBadge','aiEditAssetBadge'].forEach(id => {
    const b = document.getElementById(id);
    if (!b) return;
    b.addEventListener('click', e => {
      e.stopPropagation();
      setAssetEditMode(!window.assetEditMode);
    });
  });
});

// Creative Focus is limited to the three canonical types (IFAD / GAD / VID);
// everything else (BST, ADB, UGC, BR, Brand, ...) falls into Others so it can
// be redirected to the Product-in-Focus taxonomy above.
//
// Uses the same marker set as detectCtype() so the Creative Focus counts
// don't drift from the main CT filter — earlier this function only
// recognised a standalone "VID" token, so ads named with the OSP/CPL/USP/
// CSR/ITE convention (and even the older VRP/NNC/VIDEO/IGP set) landed
// in OTHER and undercounted the VID bucket.
//
// If the ad has a manual override (stored in ad_ctype_overrides), it wins
// so a name-less ad the team reclassified stays where they put it.
function classifyCreative(adName, adId){
  if (adId && ctypeOverrideByAdId[adId]) return ctypeOverrideByAdId[adId];
  const s = (adName || '').toUpperCase();
  if (/IFAD/.test(s)) return 'IFAD';
  if (/GAD/.test(s))  return 'GAD';
  if (/(^|[^A-Z])VID([^A-Z]|$)/.test(s)
      || s.includes('VRP') || s.includes('NNC') || s.includes('VIDEO')
      || s.includes('IGP') || s.includes('NO-ID')
      || s.includes('OSP') || s.includes('CPL') || s.includes('USP')
      || s.includes('CSR') || s.includes('ITE')) return 'VID';
  return 'OTHER';
}
function renderFocusStrips(rows){
  const p = {'Home page':0,'Category page':0,'Collection page':0,'Product page':0,'Others':0};
  const c = {IFAD:0,GAD:0,VID:0,OTHER:0};
  for (const r of rows){
    p[classifyProduct(r.ad_name)] += 1;
    c[classifyCreative(r.ad_name, r.ad_id)] += 1;
  }
  const prodKeys = ['Home page','Category page','Collection page','Product page','Others'];
  document.querySelectorAll('#prodStrip .focus-pill').forEach((el, i) => {
    const k = prodKeys[i];
    el.querySelector('b').textContent = fmtInt(p[k] || 0);
    el.dataset.btype = 'product'; el.dataset.bval = k;
    el.classList.toggle('disabled', (p[k]||0) === 0);
  });
  // Creative strip: IFAD, GAD, VID, then the OTHER pill which opens the
  // reclassify modal instead of a bucket drill.
  const creativeKeys = ['IFAD','GAD','VID','OTHER'];
  const creativeLabels = ['IFAD','GAD','VID','Others'];
  document.querySelectorAll('#creativeStrip .focus-pill').forEach((el, i) => {
    const k = creativeKeys[i];
    el.querySelector('b').textContent = fmtInt(c[k] || 0);
    el.dataset.btype = 'creative'; el.dataset.bval = k;
    el.dataset.blabel = creativeLabels[i];
    el.classList.toggle('disabled', (c[k]||0) === 0);
  });
  return {p,c};
}

/* ── Charts (Chart.js) — Saadaa palette ────────────────────────────── */
Chart.defaults.color = '#494640';
Chart.defaults.borderColor = '#E7E2D2';
Chart.defaults.font.family = "'Inter',sans-serif";
let charts = {prod:null, trend:null, creative:null, modal:null};

const SAADAA_PALETTE = [
  '#F0C61E', // accent-yellow
  '#C9A882', // accent-sand
  '#3B6FD4', // info / indigo
  '#7B4FBF', // purple
  '#B54F7A', // pink
  '#3D9E6B', // success-mid
  '#D9922A', // warning-mid
  '#9A9384'  // tertiary
];

function donutChart(canvas, labels, data, colors){
  return new Chart(canvas.getContext('2d'), {
    type:'doughnut',
    data:{labels, datasets:[{data, backgroundColor:colors, borderColor:'#FFFFFF', borderWidth:2}]},
    options:{responsive:true, maintainAspectRatio:false, cutout:'68%',
      plugins:{legend:{display:true,position:'right',labels:{boxWidth:10,font:{size:11},color:'#494640'}}}}
  });
}
function renderProdChart(p){
  if (charts.prod) charts.prod.destroy();
  const colors = [SAADAA_PALETTE[0], SAADAA_PALETTE[1], SAADAA_PALETTE[2],
                  SAADAA_PALETTE[3], SAADAA_PALETTE[7]];
  const data = [p['Home page'],p['Category page'],p['Collection page'],
                p['Product page'],p['Others']];
  charts.prod = donutChart(document.getElementById('chProd'),
                           ['Home','Category','Collection','Product','Others'],
                           data, colors);
  document.getElementById('prodTotalCnt').textContent = fmtInt(data.reduce((a,b)=>a+b,0));
}
function renderCreativeChart(c){
  if (charts.creative) charts.creative.destroy();
  const data = ['IFAD','GAD','VID'].map(k=>c[k]||0);
  charts.creative = donutChart(document.getElementById('chCreative'),
                               ['IFAD','GAD','VID'],
                               data, SAADAA_PALETTE);
  document.getElementById('creativeTotalCnt').textContent = fmtInt(data.reduce((a,b)=>a+b,0));
}
function renderTrendChart(rows){
  const buckets = {};
  for (const r of rows){
    if (!r.ad_created) continue;
    const d = new Date(r.ad_created); if (isNaN(d.getTime())) continue;
    const monday = new Date(d); monday.setDate(d.getDate() - d.getDay());
    const key = monday.toISOString().slice(0,10);
    buckets[key] = (buckets[key]||0) + 1;
  }
  const labels = Object.keys(buckets).sort();
  const data = labels.map(k=>buckets[k]);
  if (charts.trend) charts.trend.destroy();
  charts.trend = new Chart(document.getElementById('chTrend').getContext('2d'), {
    type:'line',
    data:{labels, datasets:[{label:'New creatives', data,
      borderColor:'#F0C61E', backgroundColor:'rgba(240,198,30,0.12)',
      fill:true, tension:.35, pointRadius:2, pointBackgroundColor:'#F0C61E'}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
      scales:{y:{grid:{color:'#EFEAE0'}, ticks:{color:'#6E695E'}},
              x:{grid:{display:false}, ticks:{color:'#6E695E', maxTicksLimit:8}}}}
  });
  document.getElementById('chTrendCount').textContent = fmtInt(data.reduce((a,b)=>a+b,0));
}

/* ── Modal (Winners popup) ─────────────────────────────────────────── */
let modalState = {category:null, topN:20, metrics:{spend:true, roas:true, ftewv:false, ncp:false}};
function openModal(category){
  modalState.category = category;
  document.getElementById('modalTitle').textContent = 'Top performing — '+category;
  document.getElementById('modalSub').textContent  = 'spend + ROAS for "'+category+'"';
  document.getElementById('modal').classList.add('show');
  renderModal();
}
function closeModal(){document.getElementById('modal').classList.remove('show')}
document.getElementById('modalClose').onclick = closeModal;
document.getElementById('modal').addEventListener('click', e=>{ if(e.target.id==='modal') closeModal(); });

document.getElementById('topNToggle').addEventListener('click', e=>{
  const c = e.target.closest('.chip'); if(!c) return;
  document.querySelectorAll('#topNToggle .chip').forEach(x=>x.classList.remove('active'));
  c.classList.add('active'); modalState.topN = +c.dataset.n; renderModal();
});
document.getElementById('metricToggle').addEventListener('click', e=>{
  const c = e.target.closest('.chip'); if(!c) return;
  c.classList.toggle('active');
  modalState.metrics[c.dataset.m] = c.classList.contains('active');
  renderModal();
});

function renderModal(){
  const rows = filtered(primaryAds).filter(r => (r.category||'')===modalState.category);
  rows.sort((a,b)=>(b.amount_spent||0)-(a.amount_spent||0));
  const top = rows.slice(0, modalState.topN);

  if (charts.modal) charts.modal.destroy();
  const ctx = document.getElementById('modalChart').getContext('2d');
  const ds = [];
  if (modalState.metrics.spend) ds.push({label:'Spend (₹)', backgroundColor:'#F0C61E', data:top.map(r=>+r.amount_spent||0), yAxisID:'y'});
  if (modalState.metrics.roas)  ds.push({label:'ROAS',      backgroundColor:'#3B6FD4', data:top.map(r=>+r.roas_ma||0),     yAxisID:'y1'});
  if (modalState.metrics.ftewv) ds.push({label:'FTEWV',     backgroundColor:'#7B4FBF', data:top.map(r=>+r.ftewv_count||0), yAxisID:'y'});
  if (modalState.metrics.ncp)   ds.push({label:'NCP',       backgroundColor:'#D9922A', data:top.map(r=>+r.ncp_count||0),   yAxisID:'y'});
  charts.modal = new Chart(ctx,{
    type:'bar',
    data:{labels: top.map((r,i)=>'#'+(i+1)), datasets:ds},
    options:{
      responsive:true, maintainAspectRatio:false,
      plugins:{legend:{labels:{boxWidth:12,font:{size:11},color:'#494640'}},
               tooltip:{callbacks:{title:(items)=>{
                 const idx = items[0].dataIndex; return (top[idx]?.ad_name || '').slice(0,80);
               }}}},
      scales:{
        y :{position:'left',  grid:{color:'#EFEAE0'}, ticks:{callback:v=>fmtInt(v), color:'#6E695E'}},
        y1:{position:'right', grid:{display:false},   ticks:{callback:v=>v.toFixed(1)+'x', color:'#6E695E'}},
        x :{grid:{display:false}, ticks:{maxRotation:0, autoSkip:false, color:'#6E695E'}}
      }
    }
  });

  const tb = document.querySelector('#modalTbl tbody'); tb.innerHTML='';
  if (!top.length){
    tb.innerHTML = '<tr><td colspan="8" class="empty">No ads in this category for current filter</td></tr>';
    return;
  }
  for (const r of top){
    const tr = document.createElement('tr'); tr.style.cursor='pointer';
    tr.onclick = ()=>openDrawer(r);
    const name = (r.ad_name||'—').replace(/"/g,'&quot;');
    const thumbUrl = thumbUrlOf(r.ad_id ? thumbsByAdId[r.ad_id] : null);
    const thumbCell = thumbUrl
      ? '<td class="thumb-cell"><img class="ae-thumb" src="'+thumbUrl+'" loading="lazy" alt=""></td>'
      : '<td class="thumb-cell"><div class="ae-thumb-placeholder" title="No thumbnail">—</div></td>';
    tr.innerHTML =
      thumbCell+
      '<td style="max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+name+'">'+name+'</td>'+
      '<td class="mono">'+fmtRs(r.amount_spent)+'</td>'+
      '<td class="mono">'+fmtRoas(r.roas_ma)+'</td>'+
      '<td class="mono">'+fmtInt(r.ftewv_count)+'</td>'+
      '<td class="mono">'+fmtInt(r.ncp_count)+'</td>'+
      '<td class="mono">'+fmtInt(r.shopify_orders)+'</td>'+
      '<td><span class="badge '+badgeCls(r.category)+'">'+(r.category||'—')+'</span></td>';
    tb.appendChild(tr);
  }
}
function badgeCls(c){
  return ({'Winner':'win','Incremental Winner':'incr','P0 analysis':'pri',
           'P1 analysis':'a1','P2 analysis':'a2',
           'Result Awaited':'ra','Discarded':'disc'})[c] || '';
}

/* ── Bucket modal (Product / Creative focus drill-down) ───────────── */
let bucketState = {type:null, val:null, label:null, sort:'spend', limit:15,
                   funnelCtype:'', funnelCat:''};
charts.bucketTrend = null; charts.bucketSpend = null;

function bucketRows(){
  const rows = filtered(primaryAds);
  if (bucketState.type === 'product')
    return rows.filter(r => classifyProduct(r.ad_name) === bucketState.val);
  if (bucketState.type === 'creative')
    return rows.filter(r => classifyCreative(r.ad_name, r.ad_id) === bucketState.val);
  if (bucketState.type === 'funnel'){
    // Funnel cell click — filter by (creative type × category). Empty
    // funnelCtype means "all creative types". funnelCat '__F4__' means
    // "F4-passing ads of this ctype".
    return rows.filter(r => {
      if (bucketState.funnelCtype && detectCtype(r.ad_name) !== bucketState.funnelCtype) return false;
      if (bucketState.funnelCat === '__F4__') return !!r.f4_pass;
      if (bucketState.funnelCat) return (r.category || 'Discarded') === bucketState.funnelCat;
      return true;   // both empty = grand-total click
    });
  }
  if (bucketState.type === 'landing'){
    // Landing-Page Focus row click — filter to ads pointing at this URL.
    const url = bucketState.landingUrl || bucketState.val;
    return rows.filter(r => (r.ad_link || '') === url);
  }
  return [];
}

function openBucketModal(btype, bval, blabel){
  bucketState.type = btype;
  bucketState.val  = bval;
  bucketState.label = blabel || bval;
  let header;
  if (btype === 'product')      header = 'Product in Focus';
  else if (btype === 'creative') header = 'Creative Focus';
  else if (btype === 'funnel')   header = 'Funnel cell';
  else                            header = 'Bucket';
  document.getElementById('bucketTitle').textContent = header + ' — ' + bucketState.label;
  const total = bucketRows().length;
  document.getElementById('bucketSub').textContent = fmtInt(total)+' ads in current filter';
  document.getElementById('bucketModal').style.display = 'flex';
  document.getElementById('bucketModal').setAttribute('aria-hidden','false');
  renderBucket();
}
function openFunnelCell(ctype, cat){
  bucketState.funnelCtype = ctype || '';
  bucketState.funnelCat   = cat   || '';
  // Build a friendly label: "IFAD × Winner", "All × Winner", "IFAD × F4 pass", "Grand total".
  let lbl;
  if (cat === '__F4__'){
    lbl = (ctype || 'All') + ' × F4 pass';
  } else if (!ctype && !cat){
    lbl = 'Grand total';
  } else {
    lbl = (ctype || 'All') + ' × ' + (cat || 'all categories');
  }
  openBucketModal('funnel', cat || ctype || 'all', lbl);
}
function closeBucketModal(){
  document.getElementById('bucketModal').style.display = 'none';
  document.getElementById('bucketModal').setAttribute('aria-hidden','true');
}

document.getElementById('bucketClose').onclick = closeBucketModal;
document.getElementById('bucketModal').addEventListener('click', e=>{
  if (e.target.id === 'bucketModal') closeBucketModal();
});
document.getElementById('bucketSortToggle').addEventListener('click', e=>{
  const c = e.target.closest('.chip'); if(!c) return;
  document.querySelectorAll('#bucketSortToggle .chip').forEach(x=>x.classList.remove('active'));
  c.classList.add('active'); bucketState.sort = c.dataset.s; renderBucket();
});
document.getElementById('bucketLimitToggle').addEventListener('click', e=>{
  const c = e.target.closest('.chip'); if(!c) return;
  document.querySelectorAll('#bucketLimitToggle .chip').forEach(x=>x.classList.remove('active'));
  c.classList.add('active'); bucketState.limit = +c.dataset.n; renderBucket();
});

function renderBucket(){
  const all = bucketRows();
  // Trend (line) — creatives added per week
  const buckets = {};
  for (const r of all){
    if (!r.ad_created) continue;
    const d = new Date(r.ad_created); if (isNaN(d.getTime())) continue;
    const m = new Date(d); m.setDate(d.getDate() - d.getDay());
    const k = m.toISOString().slice(0,10);
    buckets[k] = (buckets[k]||0)+1;
  }
  const tLabels = Object.keys(buckets).sort();
  const tData   = tLabels.map(k=>buckets[k]);
  if (charts.bucketTrend) charts.bucketTrend.destroy();
  charts.bucketTrend = new Chart(document.getElementById('bucketTrendChart').getContext('2d'),{
    type:'line',
    data:{labels:tLabels, datasets:[{label:'New creatives', data:tData,
      borderColor:'#F0C61E', backgroundColor:'rgba(240,198,30,0.14)',
      fill:true, tension:.35, pointRadius:2, pointBackgroundColor:'#F0C61E'}]},
    options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
      scales:{y:{grid:{color:'#EFEAE0'}, ticks:{color:'#6E695E'}},
              x:{grid:{display:false}, ticks:{color:'#6E695E', maxTicksLimit:8}}}}
  });
  document.getElementById('bucketTrendCount').textContent = fmtInt(tData.reduce((a,b)=>a+b,0));

  // Sort + slice
  const sorted = all.slice().sort((a,b)=>{
    if (bucketState.sort === 'spend')   return (+b.amount_spent||0) - (+a.amount_spent||0);
    if (bucketState.sort === 'roas')    return (+b.roas_ma||0)      - (+a.roas_ma||0);
    if (bucketState.sort === 'ftewv')   return (+b.ftewv_count||0)  - (+a.ftewv_count||0);
    if (bucketState.sort === 'created') return new Date(b.ad_created||0) - new Date(a.ad_created||0);
    return 0;
  });
  const top = (bucketState.limit > 0) ? sorted.slice(0, bucketState.limit) : sorted;

  // Spend bar (top N by spend)
  const barTop = sorted.slice(0, 15);
  if (charts.bucketSpend) charts.bucketSpend.destroy();
  charts.bucketSpend = new Chart(document.getElementById('bucketSpendChart').getContext('2d'),{
    type:'bar',
    data:{labels: barTop.map((r,i)=>'#'+(i+1)),
      datasets:[
        {label:'Spend (₹)', backgroundColor:'#F0C61E',
         data: barTop.map(r=>+r.amount_spent||0), yAxisID:'y'},
        {label:'ROAS', backgroundColor:'#3B6FD4',
         data: barTop.map(r=>+r.roas_ma||0), yAxisID:'y1'}
      ]},
    options:{responsive:true, maintainAspectRatio:false,
      plugins:{legend:{labels:{boxWidth:12,font:{size:11},color:'#494640'}},
        tooltip:{callbacks:{title:(items)=>(barTop[items[0].dataIndex]?.ad_name||'').slice(0,80)}}},
      scales:{
        y :{position:'left',  grid:{color:'#EFEAE0'}, ticks:{callback:v=>fmtInt(v),color:'#6E695E'}},
        y1:{position:'right', grid:{display:false},   ticks:{callback:v=>v.toFixed(1)+'x',color:'#6E695E'}},
        x :{grid:{display:false}, ticks:{color:'#6E695E', maxRotation:0}}
      }}
  });
  const sumSpend = all.reduce((a,b)=>a+(+b.amount_spent||0),0);
  document.getElementById('bucketSpendTotal').textContent = '₹'+fmtInt(sumSpend);

  // Table
  const tb = document.querySelector('#bucketTbl tbody'); tb.innerHTML='';
  if (!top.length){
    tb.innerHTML = '<tr><td colspan="10" class="empty">No ads in this bucket for current filter.</td></tr>';
    return;
  }
  // primaryAds (drives this drill) is built from the results_table cache
  // and doesn't carry shopify_orders. Enrich by looking up each ad in
  // allAds (ae_table_view), which does have shopify_orders wired in.
  const aeByAdId = {};
  for (const a of allAds){ if (a.ad_id) aeByAdId[a.ad_id] = a; }
  const linkCellDrill = (r) => {
    const lp = r.ad_link || '';       // destination URL the click-through lands on
    const pv = r.preview_link || '';  // Meta ad preview URL
    const parts = [];
    if (lp) parts.push('<a href="'+encodeURI(lp)+'" target="_blank" rel="noopener" class="drill-link" title="'+lp.replace(/"/g,'&quot;')+'">↗ Landing</a>');
    if (pv) parts.push('<a href="'+encodeURI(pv)+'" target="_blank" rel="noopener" class="drill-link mute" title="Ad preview">◈ Preview</a>');
    return parts.length ? parts.join(' ') : '<span class="mute">—</span>';
  };
  // Show the destination URL in a compact host + path form beneath the ad
  // name, e.g. saadaa.in/collections/whisper-hook. Strips protocol, drops
  // query strings & UTM tags, and trims long paths to 60 chars.
  const shortUrl = (url) => {
    if (!url) return '';
    try {
      const u = new URL(url);
      let path = (u.pathname || '').replace(/\/+$/, '');
      if (path.length > 60) path = path.slice(0, 57) + '…';
      return (u.hostname || '') + path;
    } catch { return url.slice(0, 80); }
  };
  for (const r of top){
    const tr = document.createElement('tr'); tr.style.cursor='pointer';
    tr.onclick = (e) => { if (e.target.closest('a')) return; openDrawer(r); };
    const name = (r.ad_name||'—').replace(/"/g,'&quot;');
    const created = r.ad_created ? new Date(r.ad_created).toISOString().slice(0,10) : '—';
    const thumbUrl = thumbUrlOf(r.ad_id ? thumbsByAdId[r.ad_id] : null);
    const thumbCell = thumbUrl
      ? '<td class="thumb-cell"><img class="ae-thumb" src="'+thumbUrl+'" loading="lazy" alt=""></td>'
      : '<td class="thumb-cell"><div class="ae-thumb-placeholder" title="No thumbnail">—</div></td>';
    // shopify_orders lives on ae_table_view (allAds), not on the
    // results_table cache. Look it up by ad_id; fall back to 0.
    const shop = aeByAdId[r.ad_id];
    const shopOrders = shop ? (+shop.shopify_orders || 0) : 0;
    // Ad name + destination URL beneath. The URL is the actual page the
    // click-through lands on (e.g. saadaa.in/collections/whisper-hook).
    const dest = shortUrl(r.ad_link || r.preview_link || '');
    const nameCell = '<td class="drill-name-cell" title="'+name+'">'+
        '<div class="drill-ad-name">'+name+'</div>'+
        (dest ? '<div class="drill-ad-url" title="'+(r.ad_link||'').replace(/"/g,'&quot;')+'">↗ '+dest+'</div>' : '')+
      '</td>';
    tr.innerHTML =
      thumbCell+
      nameCell+
      '<td class="mono" style="color:var(--text-secondary)">'+created+'</td>'+
      '<td class="mono">'+fmtRs(r.amount_spent)+'</td>'+
      '<td class="mono">'+fmtRoas(r.roas_ma)+'</td>'+
      '<td class="mono">'+fmtInt(r.ftewv_count)+'</td>'+
      '<td class="mono">'+fmtInt(r.ncp_count)+'</td>'+
      '<td class="mono">'+(shopOrders ? fmtInt(shopOrders) : '<span class="mute">0</span>')+'</td>'+
      '<td><span class="badge '+badgeCls(r.category)+'">'+(r.category||'—')+'</span></td>'+
      '<td class="drill-links-cell">'+linkCellDrill(r)+'</td>';
    tb.appendChild(tr);
  }
}

/* Click handlers on Product + Creative focus pills */
function bindFocusStrip(stripId){
  document.getElementById(stripId).addEventListener('click', e=>{
    const pill = e.target.closest('.focus-pill'); if(!pill) return;
    if (pill.classList.contains('disabled')) return;
    // Creative Focus Others pill routes to the reclassify modal instead
    // of the standard bucket drill — that's the whole point of having
    // it visible in the strip.
    if (stripId === 'creativeStrip' && pill.dataset.bval === 'OTHER'){
      openReclassifyModal(); return;
    }
    openBucketModal(pill.dataset.btype, pill.dataset.bval, pill.dataset.blabel);
  });
}
bindFocusStrip('prodStrip');
bindFocusStrip('creativeStrip');

/* ────────────────────────────────────────────────────────────────────
   Reclassify Others modal — lets the user assign a specific
   IFAD/GAD/VID bucket to any ad currently classified as OTHER, and
   persists the assignment in public.ad_ctype_overrides so the strip
   counts follow it on every subsequent render.
   ──────────────────────────────────────────────────────────────────── */
function openReclassifyModal(){
  const modal = document.getElementById('otherReclassifyModal');
  if (!modal) return;
  const rows = filtered(primaryAds).filter(r =>
    classifyCreative(r.ad_name, r.ad_id) === 'OTHER');
  const body = document.getElementById('otherReclassifyBody');
  const status = document.getElementById('otherReclassifyStatus');
  status.textContent = fmtInt(rows.length) + ' ad(s) currently in Others · pick a bucket to reclassify';
  if (!rows.length){
    body.innerHTML = '<tr><td colspan="3" style="padding:24px;text-align:center;color:var(--text-tertiary)">No Others in the current filter. Everything already classified as IFAD, GAD, or VID.</td></tr>';
  } else {
    // Sort by ad_name so recurring name patterns cluster together.
    rows.sort((a,b) => (a.ad_name||'').localeCompare(b.ad_name||''));
    body.innerHTML = rows.map(r => {
      const current = ctypeOverrideByAdId[r.ad_id] || '';
      const opt = (v, l) => '<option value="'+v+'"'+(v===current?' selected':'')+'>'+l+'</option>';
      return '<tr data-ad-id="'+(r.ad_id||'').replace(/"/g,'&quot;')+'">'+
        '<td style="padding:8px 12px;font-size:12px;color:var(--text-mid);max-width:520px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+(r.ad_name||'').replace(/"/g,'&quot;')+'">'+(r.ad_name||'—')+'</td>'+
        '<td style="padding:8px 12px;font-size:12px;color:var(--text-tertiary)">'+(r.account_name||'—')+'</td>'+
        '<td style="padding:8px 12px">'+
          '<select class="fg-select reclassify-sel" data-ad-id="'+(r.ad_id||'').replace(/"/g,'&quot;')+'" style="width:100%;padding:6px 10px;font-size:12px">'+
            opt('', '— Auto (marker) —') +
            opt('IFAD', 'IFAD') +
            opt('GAD',  'GAD')  +
            opt('VID',  'VID')  +
          '</select>'+
        '</td>'+
      '</tr>';
    }).join('');
  }
  modal.style.display = 'flex';
  modal.setAttribute('aria-hidden', 'false');
}
function closeReclassifyModal(){
  const modal = document.getElementById('otherReclassifyModal');
  if (!modal) return;
  modal.style.display = 'none';
  modal.setAttribute('aria-hidden', 'true');
}
async function _persistOverride(adId, ctype){
  if (!SUPABASE_URL || !SUPABASE_ANON || !adId) return false;
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   'Content-Type':'application/json',
                   Prefer:'resolution=merge-duplicates,return=minimal'};
  if (!ctype){
    // Empty selection → clear the override (fall back to marker detection).
    const r = await fetch(SUPABASE_URL + '/rest/v1/ad_ctype_overrides?ad_id=eq.' + encodeURIComponent(adId),
                          {method:'DELETE', headers});
    return r.ok;
  }
  const r = await fetch(SUPABASE_URL + '/rest/v1/ad_ctype_overrides?on_conflict=ad_id',
                        {method:'POST', headers,
                         body: JSON.stringify([{ad_id: adId, ctype: ctype}])});
  return r.ok;
}
document.getElementById('otherReclassifyClose')?.addEventListener('click', closeReclassifyModal);
document.getElementById('otherReclassifyModal')?.addEventListener('click', e => {
  if (e.target.id === 'otherReclassifyModal') closeReclassifyModal();
});
document.addEventListener('keydown', e => {
  const modal = document.getElementById('otherReclassifyModal');
  if (e.key === 'Escape' && modal && modal.style.display === 'flex') closeReclassifyModal();
});
// Delegate change events on the dropdowns so we don't have to re-bind
// every time the modal re-renders.
document.getElementById('otherReclassifyBody')?.addEventListener('change', async e => {
  const sel = e.target.closest('.reclassify-sel'); if (!sel) return;
  const adId = sel.dataset.adId; const val = sel.value;
  const status = document.getElementById('otherReclassifyStatus');
  sel.disabled = true;
  const ok = await _persistOverride(adId, val);
  sel.disabled = false;
  if (!ok){
    status.textContent = 'Save failed — try again';
    return;
  }
  if (val) ctypeOverrideByAdId[adId] = val;
  else delete ctypeOverrideByAdId[adId];
  // Re-render the strip so the counts move immediately; the row itself
  // no longer belongs to Others when a non-empty value is picked, so we
  // strike it out to make that visible without closing the modal.
  const tr = sel.closest('tr');
  if (tr && val){ tr.style.opacity = '0.5'; tr.querySelector('td:first-child').style.textDecoration = 'line-through'; }
  else if (tr){ tr.style.opacity = ''; tr.querySelector('td:first-child').style.textDecoration = ''; }
  status.textContent = 'Saved · ' + (val ? ('now ' + val) : 'reset to Auto');
  rerender();
});

/* Creative Type Funnel cells — click any cell to open the bucket modal
   pre-filtered by that (creative_type × category) intersection. */
document.getElementById('funnelBody').addEventListener('click', e => {
  const cell = e.target.closest('.fk-clickable');
  if (!cell) return;
  const ctype = cell.dataset.fnlCt || '';
  const cat   = cell.dataset.fnlCat || '';
  // Only open if the cell actually has ads in it
  const n = parseInt((cell.querySelector('.n') || {}).textContent || '0', 10);
  if (!n) return;
  openFunnelCell(ctype, cat);
});

/* Chart info-button toggles — show a small popover with what each chart shows */
document.querySelectorAll('.ch-info').forEach(btn => {
  btn.addEventListener('click', e => {
    e.stopPropagation();
    const key = btn.dataset.ci;
    const pop = document.getElementById('ci-pop-' + key);
    if (!pop) return;
    const isOpen = pop.classList.contains('open');
    document.querySelectorAll('.ch-info-pop').forEach(p => p.classList.remove('open'));
    if (!isOpen) pop.classList.add('open');
  });
});
document.addEventListener('click', e => {
  if (!e.target.closest('.ch-info-pop') && !e.target.closest('.ch-info')){
    document.querySelectorAll('.ch-info-pop').forEach(p => p.classList.remove('open'));
  }
});

/* ── Drawer (preview + metrics) ────────────────────────────────────── */
/* Given an Instagram post/reel permalink, return the iframe-embeddable URL.
 * Instagram's own /embed/ endpoint accepts:
 *   https://www.instagram.com/p/{shortcode}/embed/
 *   https://www.instagram.com/reel/{shortcode}/embed/
 *   https://www.instagram.com/tv/{shortcode}/embed/
 * and sends the right X-Frame-Options so iframe rendering works. */
function _iframeUrlForIg(url){
  if (!url) return '';
  const m = url.match(/instagram\.com\/(p|reel|tv)\/([^\/?#]+)/i);
  if (!m) return '';
  return `https://www.instagram.com/${m[1]}/${m[2]}/embed/captioned/`;
}
/* Given a Facebook page-post URL, wrap in the FB post plugin which supports
 * iframe embedding. Requires the URL to be a public page post. */
function _iframeUrlForFb(url){
  if (!url) return '';
  return 'https://www.facebook.com/plugins/post.php?href=' +
         encodeURIComponent(url) + '&show_text=true&width=500';
}

function openDrawer(r){
  const drawer = document.getElementById('drawer');
  // Populate the right-column meta panel
  document.getElementById('drAdName').textContent = r.ad_name || 'Untitled ad';
  document.getElementById('drSub').textContent =
    (r.account_name||'—') + ' · ' + (r.adset_name || '—') + ' · ' + (r.ad_id || '');
  // Category pill next to the header
  const badge = document.getElementById('drCatBadge');
  if (badge){
    const cls = (typeof CAT_CLASS !== 'undefined' && CAT_CLASS[r.category]) || 'cat-disc';
    badge.className = 'cat-badge ' + cls;
    badge.textContent = r.category || '—';
  }
  // Metric grid — 3-across, 2 rows, matches the screenshot layout
  const cells = [
    ['SPEND',       fmtRs(r.amount_spent)],
    ['ROAS',        fmtRoas(r.roas_ma)],
    ['IMPRESSIONS', fmtInt(r.impressions)],
    ['FTEWV',       fmtInt(r.ftewv_count)],
    ['NCP',         fmtInt(r.ncp_count)],
    ['ORDERS',      fmtInt(r.shopify_orders)],
  ];
  document.getElementById('drMetrics').innerHTML = cells.map(
    ([l,v]) => '<div class="dr-metric"><div class="lbl">'+l+'</div>'+
               '<div class="val mono">'+v+'</div></div>'
  ).join('');
  // Ad created line
  const created = r.ad_created ? new Date(r.ad_created).toLocaleDateString('en-IN',
    {day:'numeric', month:'short', year:'numeric'}) : '—';
  document.getElementById('drCreatedLine').textContent = 'Ad created ' + created;

  // LEFT column: preference order —
  //   1. video_source_url  → native <video>  (playable, best UX)
  //   2. Instagram permalink → iframe embed  (live post, requires click to open on IG for playback)
  //   3. FB page-post permalink → iframe FB plugin
  //   4. Static image thumbnail
  //   5. No preview → Ads Library link
  // For grouped rows (adset/campaign) the row's ad_id is "N ad_ids"; use
  // the representative ad_id captured in aeGroupBy so the preview surfaces
  // (video/iframe/thumbnail) render from an actual constituent ad.
  const _lookupAdId = r._repAdId || r.ad_id;
  const entry = _lookupAdId ? thumbsByAdId[_lookupAdId] : null;
  const videoUrl  = entry && entry.v  ? entry.v  : '';
  const igLink    = entry && entry.ig ? entry.ig : '';
  const fbLink    = entry && entry.fb ? entry.fb : '';
  const staticUrl = previewUrlOf(entry);
  const posterUrl = staticUrl;   // use image as the video poster frame
  const video    = document.getElementById('drVideo');
  const iframe   = document.getElementById('drIframe');
  const staticWr = document.getElementById('drStaticWrap');
  const staticIm = document.getElementById('drImg');
  const noPv     = document.getElementById('drNoPreview');
  const foot     = document.getElementById('drPreviewFoot');
  const igOpen   = document.getElementById('drIgLink');
  const fbOpen   = document.getElementById('drFbLink');

  // Reset every preview surface
  video.pause(); video.style.display = 'none';
  video.removeAttribute('src'); video.removeAttribute('poster');
  iframe.style.display = 'none'; iframe.removeAttribute('src');
  staticWr.style.display = 'none'; staticIm.removeAttribute('src');
  noPv.style.display = 'none';
  igOpen.style.display = 'none'; fbOpen.style.display = 'none';
  foot.textContent = '';

  const igEmbed = _iframeUrlForIg(igLink);
  const fbEmbed = fbLink ? _iframeUrlForFb(fbLink) : '';

  if (videoUrl){
    // Native HTML5 <video> — the click-to-play experience matches
    // Instagram/Facebook's own preview and works for dark-post ads too.
    if (posterUrl) video.setAttribute('poster', posterUrl);
    video.src = videoUrl;
    video.load();  // reset any lingering internal state
    video.style.display = 'block';
    const age = entry && entry.vt ? new Date(entry.vt) : null;
    foot.textContent = 'Playable · Meta CDN mp4' +
      (age ? ' · cached ' + age.toLocaleDateString() : '') +
      (igLink ? ' · ↗ IG post available' : '');
    if (igLink){ igOpen.href = igLink; igOpen.style.display = 'inline'; }
    if (fbLink){ fbOpen.href = fbLink; fbOpen.style.display = 'inline'; }
  } else if (igEmbed){
    iframe.src = igEmbed;
    iframe.style.display = 'block';
    foot.textContent = 'Live embed · Instagram · click the post to play on ig.com';
    igOpen.href = igLink; igOpen.style.display = 'inline';
  } else if (fbEmbed){
    iframe.src = fbEmbed;
    iframe.style.display = 'block';
    foot.textContent = 'Live embed · Facebook · click ↗ to open on facebook.com';
    fbOpen.href = fbLink; fbOpen.style.display = 'inline';
  } else if (staticUrl){
    staticIm.src = staticUrl;
    staticWr.style.display = 'block';
    foot.textContent = 'Static thumbnail (video CDN URL not cached — run fetch_ad_thumbnails.py to refresh)';
  } else {
    noPv.style.display = 'block';
    document.getElementById('drOpenLib').href =
      'https://www.facebook.com/ads/library/?id=' + encodeURIComponent(r.ad_id || '');
  }

  // Footer buttons
  document.getElementById('drFooterLanding').href =
    r.ad_link || 'https://www.facebook.com/ads/library/?id=' + encodeURIComponent(r.ad_id || '');
  document.getElementById('drPreviewLink').href =
    r.preview_link || r.ad_link ||
    'https://www.facebook.com/ads/library/?id=' + encodeURIComponent(r.ad_id || '');

  drawer.classList.add('open');
  drawer.setAttribute('aria-hidden','false');
}
function closeDrawer(){
  const drawer = document.getElementById('drawer');
  drawer.classList.remove('open');
  drawer.setAttribute('aria-hidden','true');
  // Stop the video mid-playback (browsers keep decoding it otherwise)
  const video = document.getElementById('drVideo');
  if (video){ video.pause(); video.removeAttribute('src'); video.load(); }
  // Unload the iframe so it stops fetching in the background
  const iframe = document.getElementById('drIframe');
  if (iframe){ iframe.removeAttribute('src'); iframe.style.display = 'none'; }
  const img = document.getElementById('drImg');
  if (img){ img.removeAttribute('src'); }
}
document.getElementById('drClose').onclick       = closeDrawer;
document.getElementById('drFooterClose').onclick = closeDrawer;

/* ── Funnel breakdown (Creative Type × Category) ───────────────────── */
// Icons rendered next to each creative-type row in the funnel.  User-supplied
// line-art PNGs pre-processed with transparent backgrounds in assets/icons/.
// Kept as <img> refs (not inline SVG) because the source files are already
// small (<10KB each after crop+alpha) and the browser caches them once.
const CTYPE_ICONS = {
  'IFAD':      '<img src="assets/icons/ifad.png" alt="" class="ct-icon">',
  'Graphic AD':'<img src="assets/icons/gad.png"  alt="" class="ct-icon">',
  'VID':       '<img src="assets/icons/vid.png"  alt="" class="ct-icon">',
  'STATIC':    '<img src="assets/icons/gad.png"  alt="" class="ct-icon">',
};
const FUNNEL_SUB  = ['Incremental Winner','Winner','P0 analysis','P1 analysis','P2 analysis','Result Awaited','Discarded'];
const FUNNEL_SUB_SHORT = ['Inc. Winner','Winner','P0','P1','P2','Awaited','Discarded'];
// One class per FUNNEL_SUB slot — must stay in lockstep with FUNNEL_SUB order.
// (Missing col-ra for Result Awaited was the reason the funnel grid over-
// flowed into a broken second row after the buffer feature landed.)
const FUNNEL_COL_CLS  = ['col-incr','col-win','col-pri','col-a1','col-a2','col-ra','col-disc'];
function renderFunnel(rows){
  const body = document.getElementById('funnelBody'); if (!body) return;
  const ctypes = ['IFAD','Graphic AD','VID','STATIC'];
  const counts = {};
  for (const ct of ctypes) counts[ct] = {total:0, f4:0, ...Object.fromEntries(FUNNEL_SUB.map(s=>[s,0]))};
  for (const r of rows){
    const ct = detectCtype(r.ad_name);
    if (!counts[ct]) continue;
    const cat = r.category || 'Discarded';
    counts[ct].total += 1;
    if (FUNNEL_SUB.includes(cat)) counts[ct][cat] += 1;
    if (r.f4_pass) counts[ct].f4 += 1;
  }
  const active = ctypes.filter(ct => counts[ct].total > 0);
  if (!active.length){
    body.innerHTML = '<div class="empty">No ads match the current filter.</div>';
    document.getElementById('funnelTotal').textContent = '—';
    return;
  }
  const maxPerCat = {};
  for (const s of FUNNEL_SUB) maxPerCat[s] = Math.max(1, ...active.map(ct=>counts[ct][s]));
  const maxF4 = Math.max(1, ...active.map(ct=>counts[ct].f4));
  // Grid columns: creative (180) · total (70) · 7 sub-cat columns · F4 (130)
  // The 7th sub-cat is Result Awaited, added when the 14-day buffer landed.
  const cols  = '180px 70px repeat(7,1fr) 130px';
  let html = '';
  // Master row — column spans must add up to 10 (matches the 10 grid columns).
  //   creative+total(2) + Winner(2) + P0(1) + P1/P2(2) + Awaited(1) + Discarded(1) + F4(1) = 10
  html += `<div class="fk-row fk-master" style="grid-template-columns:${cols}">
    <div style="grid-column:span 2"></div>
    <div class="fk-master-cell win"  style="grid-column:span 2">Winner</div>
    <div class="fk-master-cell pri"  style="grid-column:span 1">P0 analysis</div>
    <div class="fk-master-cell anl"  style="grid-column:span 2">P1 / P2 analysis</div>
    <div class="fk-master-cell awa"  style="grid-column:span 1">Awaited</div>
    <div class="fk-master-cell dis"  style="grid-column:span 1">Discarded</div>
    <div class="fk-master-cell f4">F4 Quality</div>
  </div>`;
  // Sub-header row
  html += `<div class="fk-row fk-sub" style="grid-template-columns:${cols}">
    <div class="fk-sub-cell first">Creative Type</div>
    <div class="fk-sub-cell">Total</div>
    ${FUNNEL_SUB_SHORT.map(s=>`<div class="fk-sub-cell">${s}</div>`).join('')}
    <div class="fk-sub-cell" style="border-left:2px solid var(--border-primary)">F4 ✓</div>
  </div>`;
  // Data rows — every metric cell is clickable, opens the bucket modal
  // pre-filtered by (creative type × category).
  for (const ct of active){
    const row = counts[ct];
    // The Total cell drops the category filter; the F4 cell sets a special
    // "f4" pseudo-category that bucketRows knows about.
    html += `<div class="fk-row fk-data" style="grid-template-columns:${cols}">
      <div class="fk-ctype"><span class="icon">${CTYPE_ICONS[ct]||'📌'}</span>
        <div><div class="nm">${ct}</div><div class="ttl">${row.total} ads</div></div></div>
      <div class="fk-total fk-clickable" data-fnl-ct="${ct}" data-fnl-cat="" title="View all ${row.total} ${ct} ads"><div class="n">${row.total}</div></div>
      ${FUNNEL_SUB.map((s,i)=>{
        const n = row[s]||0; const pct = row.total ? Math.round(n/row.total*100) : 0;
        const barPct = (n/maxPerCat[s]*100).toFixed(1);
        const clickable = n > 0 ? 'fk-clickable' : '';
        const title = n > 0 ? `View ${n} ${ct} × ${s}` : '';
        return `<div class="fk-cell ${FUNNEL_COL_CLS[i]} ${clickable}" data-fnl-ct="${ct}" data-fnl-cat="${s}" title="${title}">
          <div><span class="n">${n}</span><span class="p">${pct}%</span></div>
          <div class="bar"><div class="bar-fill" style="width:${barPct}%"></div></div>
        </div>`;
      }).join('')}
      <div class="fk-cell col-f4 f4 ${row.f4>0?'fk-clickable':''}" data-fnl-ct="${ct}" data-fnl-cat="__F4__" title="${row.f4>0?'View '+row.f4+' '+ct+' ads passing F4':''}">
        <div><span class="n">${row.f4}</span><span class="p">${row.total?Math.round(row.f4/row.total*100):0}%</span></div>
        <div class="bar"><div class="bar-fill" style="width:${(row.f4/maxF4*100).toFixed(1)}%"></div></div>
      </div>
    </div>`;
  }
  // Grand total row — also clickable; ctype="" means "all creative types"
  const grand = {total:0, f4:0, ...Object.fromEntries(FUNNEL_SUB.map(s=>[s,0]))};
  for (const ct of active){
    grand.total += counts[ct].total; grand.f4 += counts[ct].f4;
    for (const s of FUNNEL_SUB) grand[s] += counts[ct][s];
  }
  html += `<div class="fk-row fk-grand fk-data" style="grid-template-columns:${cols}">
    <div class="fk-ctype"><div><div class="nm">Grand Total</div></div></div>
    <div class="fk-total fk-clickable" data-fnl-ct="" data-fnl-cat=""><div class="n">${grand.total}</div></div>
    ${FUNNEL_SUB.map((s,i)=>{
      const n=grand[s]; const pct = grand.total?Math.round(n/grand.total*100):0;
      const clickable = n > 0 ? 'fk-clickable' : '';
      return `<div class="fk-cell ${FUNNEL_COL_CLS[i]} ${clickable}" data-fnl-ct="" data-fnl-cat="${s}"><div><span class="n">${n}</span><span class="p">${pct}%</span></div></div>`;
    }).join('')}
    <div class="fk-cell col-f4 f4 ${grand.f4>0?'fk-clickable':''}" data-fnl-ct="" data-fnl-cat="__F4__"><div><span class="n">${grand.f4}</span><span class="p">${grand.total?Math.round(grand.f4/grand.total*100):0}%</span></div></div>
  </div>`;
  body.innerHTML = html;
  document.getElementById('funnelTotal').textContent = fmtInt(grand.total) + ' ads';
}

/* Overview – Performance KPI block.
   Two paint paths:
     (a) FAST — read the pre-computed snapshot from results_table (populated
         by results_sync.py). Instant, no round trip. Used when the current
         filter window matches the snapshot's window (± 1 day drift).
     (b) SLOW — call get_overview_perf_totals RPC for custom windows.
   The date-range tag on the header shows which window the numbers cover. */
let _opDebounce = null;
let _opLastKey  = '';

function _opFmtDateRange(fromISO, toISO){
  if (!fromISO || !toISO) return '';
  const days = Math.round(
    (new Date(toISO + 'T00:00:00Z') - new Date(fromISO + 'T00:00:00Z')) / 86400000
  ) + 1;
  // Human-friendly labels for the standard windows.  Falls back to the
  // literal range for anything unusual.
  let label = '';
  if (days === 30) label = 'LAST 30 DAYS';
  else if (days === 7)  label = 'LAST 7 DAYS';
  else if (days === 90) label = 'LAST 90 DAYS';
  else label = days + '-DAY WINDOW';
  return label + ' · ' + fromISO + ' → ' + toISO;
}

function _opWindowMatchesSnapshot(fromISO, toISO){
  const snap = window._ctOverviewSnapshot; if (!snap) return false;
  const drift = (a, b) => {
    if (!a || !b) return 999;
    return Math.abs((new Date(a + 'T00:00:00Z') - new Date(b + 'T00:00:00Z')) / 86400000);
  };
  return drift(snap.fromDate, fromISO) <= 1 && drift(snap.toDate, toISO) <= 1;
}

function _opBadgeText(rangeLabel){
  const excl = state.exclCopy ? 'EXCL' : 'INCL';
  return 'SUM & AVG · ' + (rangeLabel ? rangeLabel + ' · ' : '') + excl + '. COPY';
}

function renderOverviewPerf(_rows){
  const host = document.getElementById('overviewPerf');
  if (!host) return;
  clearTimeout(_opDebounce);
  _opDebounce = setTimeout(_opFire, 180);
}

async function _opFire(){
  const setTxt = (id, v) => {
    const el = document.getElementById(id); if (el) el.textContent = v;
  };
  // Resolve the requested window.  If the CT view has no explicit range set,
  // the pipeline's default (last 30 days ending today) applies.
  const today = new Date().toISOString().slice(0, 10);
  const from = state.dateFrom || (() => {
    const d = new Date(); d.setUTCDate(d.getUTCDate() - 29);
    return d.toISOString().slice(0, 10);
  })();
  const to    = state.dateTo || today;
  const excl  = !!state.exclCopy;
  const key   = from + '|' + to + '|' + (excl ? '1' : '0');
  _opLastKey  = key;
  const badge = document.getElementById('opBadge');

  // ── FAST PATH — pre-computed snapshot from results_table ──
  // Only usable when (a) window matches, (b) Excl. Copy toggle is ON (that's
  // how the snapshot is computed).  Falls through to the RPC otherwise.
  const snap = window._ctOverviewSnapshot;
  if (snap && excl && _opWindowMatchesSnapshot(from, to)){
    if (badge) badge.textContent = _opBadgeText(_opFmtDateRange(snap.fromDate, snap.toDate));
    setTxt('opSpend',    '₹' + fmtInt(snap.spend));
    setTxt('opImpr',     fmtInt(snap.impressions));
    setTxt('opHook',     snap.hookRate       .toFixed(2) + '%');
    setTxt('opCtr',      snap.outboundCtr    .toFixed(2) + '%');
    setTxt('opEng',      snap.engagementRate .toFixed(2) + '%');
    setTxt('opThruplay', snap.thruplayRate   .toFixed(2) + '%');
    setTxt('opHold',     snap.holdRate       .toFixed(2) + '%');
    setTxt('opRoas',     snap.ctRoas         .toFixed(2));
    return;
  }

  // ── SLOW PATH — RPC against primary_table ──
  if (badge) badge.textContent = _opBadgeText(_opFmtDateRange(from, to));
  ['opSpend','opImpr','opHook','opCtr','opEng','opThruplay','opHold','opRoas']
    .forEach(id => setTxt(id, '…'));
  try {
    const url = SUPABASE_URL + '/rest/v1/rpc/get_overview_perf_totals';
    const r = await fetch(url, {
      method: 'POST',
      headers: {
        apikey: SUPABASE_ANON,
        Authorization: 'Bearer ' + SUPABASE_ANON,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ p_from: from, p_to: to, p_excl_copy: excl }),
    });
    if (_opLastKey !== key) return;
    if (!r.ok){
      const bodyText = await r.text().catch(() => '');
      throw new Error('RPC HTTP ' + r.status + ' — ' + bodyText.slice(0, 200));
    }
    const arr = await r.json();
    const t = Array.isArray(arr) ? (arr[0] || {}) : (arr || {});
    const impr    = +t.total_impressions      || 0;
    const spend   = +t.total_spend            || 0;
    const conv    = +t.total_conv_value       || 0;
    const threeS  = +t.total_three_sec        || 0;
    const thrupl  = +t.total_thruplays        || 0;
    const outClk  = +t.total_outbound_clicks  || 0;
    const postEng = +t.total_post_engagements || 0;
    const _pct    = (num, den) => den > 0 ? ((num / den) * 100) : null;
    const _fmtPct = v => v == null ? '—' : v.toFixed(2) + '%';
    setTxt('opSpend',    '₹' + fmtInt(spend));
    setTxt('opImpr',     fmtInt(impr));
    setTxt('opHook',     _fmtPct(_pct(threeS,  impr)));
    setTxt('opCtr',      _fmtPct(_pct(outClk,  impr)));
    setTxt('opEng',      _fmtPct(_pct(postEng, impr)));
    setTxt('opThruplay', _fmtPct(_pct(thrupl,  impr)));
    setTxt('opHold',     _fmtPct(_pct(thrupl,  threeS)));
    setTxt('opRoas',     spend > 0 ? (conv / spend).toFixed(2) : '—');
  } catch (e){
    ['opSpend','opImpr','opHook','opCtr','opEng','opThruplay','opHold','opRoas']
      .forEach(id => setTxt(id, '—'));
    console.warn('[Overview KPI] RPC failed:', e);
  }
}

// Kick one initial paint on DOM ready.  The snapshot won't be loaded yet at
// this point so this call is basically an RPC fallback; subsequent rerender()
// calls (after primary data loads) hit the snapshot fast-path.
document.addEventListener('DOMContentLoaded', () => {
  try { renderOverviewPerf(); } catch(_){}
});

/* ── Wire-up ───────────────────────────────────────────────────────── */
/* Pull the first uppercase product-code-ish token from an ad name. */
const _LP_CODE_RE = /[A-Z][A-Z0-9_]{1,7}/;
function lpExtractCode(adName){
  if (!adName) return '—';
  // Skip leading layout prefixes that aren't product codes
  const stripped = String(adName)
    .replace(/^(CLP|CTP|PDP)[-_+\s]+/i, '')
    .replace(/^(P\d+)[-_+\s]+/i, '');
  const m = stripped.match(_LP_CODE_RE);
  return m ? m[0].toUpperCase() : '—';
}
/* Friendly slug from URL: last meaningful path segment */
function lpUrlSlug(url){
  try{
    const u = new URL(url);
    const parts = u.pathname.split('/').filter(Boolean);
    return parts[parts.length - 1] || u.hostname;
  }catch(e){ return url; }
}

/* Normalise a full ad_link URL down to just the pathname so it can be
   matched against Shopify's landing_page_path (which is the raw path only,
   no querystring or host). Falls back to the raw string when URL parsing
   fails. Home = "/". */
function lpNormaliseToPath(url){
  if (!url) return '';
  try {
    const u = new URL(url);
    return u.pathname || '/';
  } catch (e){
    // ad_link occasionally comes in as a bare path or malformed URL.
    const q = String(url).indexOf('?');
    return (q >= 0 ? String(url).slice(0, q) : String(url)) || '';
  }
}

/* Cache of last-fetched sessions rollup so switching filters doesn't
   re-RPC on every rerender. Key: `${from}|${to}`; value: {path -> row}. */
let _lpSessionsCache = { key: '', map: null };
async function fetchLandingPageSessions(from, to){
  if (!from || !to) return null;
  const key = from + '|' + to;
  if (_lpSessionsCache.key === key && _lpSessionsCache.map) return _lpSessionsCache.map;
  try {
    const r = await fetch(SHOPIFY_URL + '/rest/v1/rpc/get_sessions_by_lp', {
      method: 'POST',
      headers: {
        'apikey':        SHOPIFY_ANON,
        'Authorization': 'Bearer ' + SHOPIFY_ANON,
        'Content-Type':  'application/json',
        // PostgREST caps RPC arrays at 1000 rows by default. The RPC is
        // hard-limited to 5000 server-side (top landing pages by
        // sessions); tell PostgREST to send all of them.
        'Range':         '0-4999',
      },
      body: JSON.stringify({from_date: from, to_date: to}),
    });
    if (!r.ok) return null;
    const rows = await r.json();
    if (!Array.isArray(rows)) return null;
    const map = Object.create(null);
    for (const row of rows){
      if (row && row.landing_page_path) map[row.landing_page_path] = row;
    }
    _lpSessionsCache = { key, map };
    return map;
  } catch (e){
    return null;
  }
}
function renderLandingPageFocus(rows){
  const host = document.getElementById('landingPageFocus');
  if (!host) return;
  const byUrl = {};
  for (const r of rows){
    const url = (r.ad_link || '').trim();
    if (!url) continue;
    if (!byUrl[url]) byUrl[url] = {url, count:0, codes:{}, spend:0};
    const b = byUrl[url];
    b.count += 1;
    b.spend += (+r.amount_spent || 0);
    const code = lpExtractCode(r.ad_name);
    b.codes[code] = (b.codes[code] || 0) + 1;
  }
  const list = Object.values(byUrl).map(b => {
    const top = Object.entries(b.codes).sort((a,b) => b[1] - a[1])[0];
    return {...b, code: top ? top[0] : lpUrlSlug(b.url), path: lpNormaliseToPath(b.url)};
  }).sort((a,b) => b.count - a.count || b.spend - a.spend);
  const total = list.reduce((s,b) => s + b.count, 0);
  document.getElementById('lpFocusTotal').textContent =
    fmtInt(list.length) + ' landing pages · ' + fmtInt(total) + ' ads';
  if (!list.length){
    host.innerHTML = '<div class="empty" style="padding:24px;text-align:center;color:var(--text-tertiary);font-size:12.5px">No ads have a landing-page URL in the current filter.</div>';
    return;
  }
  // Try the cached rollup synchronously so first paint already carries the
  // Sessions column filled. If it's stale (window changed), we still paint
  // with dashes and the async fetch below will re-render when it lands.
  const winKey = (state.dateFrom || '') + '|' + (state.dateTo || '');
  const cachedMap = (_lpSessionsCache.key === winKey) ? _lpSessionsCache.map : null;
  const top = list.slice(0, 40);   // cap visible rows
  const rowHtml = (b) => {
    const s = cachedMap ? cachedMap[b.path] : null;
    const sess = s ? fmtInt(s.sessions) : '—';
    // CVR: sessions_that_reached_checkout / sessions.  Blank when we have
    // no session data to avoid implying 0% conversion.
    let cvr = '—';
    if (s && s.sessions > 0 && s.sessions_that_reached_checkout != null){
      cvr = ((s.sessions_that_reached_checkout / s.sessions) * 100).toFixed(2) + '%';
    }
    return '<tr data-lp-url="'+b.url.replace(/"/g,'&quot;')+'">'+
      '<td><span class="lp-code" title="most common product token across ads at this URL">'+b.code+'</span></td>'+
      '<td><a class="lp-link" href="'+b.url+'" target="_blank" rel="noopener">'+b.url+'</a></td>'+
      '<td class="num">'+fmtRs(b.spend)+'</td>'+
      '<td class="num">'+fmtInt(b.count)+'</td>'+
      '<td class="num" data-lp-sess>'+sess+'</td>'+
      '<td class="num" data-lp-cvr>'+cvr+'</td>'+
    '</tr>';
  };
  host.innerHTML = '<table class="lp-tbl">'+
    '<thead><tr>'+
      '<th style="width:90px">Product</th>'+
      '<th>Landing Page</th>'+
      '<th class="num" style="width:110px">Spend</th>'+
      '<th class="num" style="width:80px">Ads</th>'+
      '<th class="num" style="width:110px" title="Sum of Shopify sessions on this landing-page path in the current window">Sessions</th>'+
      '<th class="num" style="width:80px" title="Sessions that reached checkout / total sessions">Checkout %</th>'+
    '</tr></thead>'+
    '<tbody>'+ top.map(rowHtml).join('') +'</tbody></table>';
  // Row click → bucket modal filtered to that URL (ignore clicks on the link itself)
  host.querySelectorAll('tr[data-lp-url]').forEach(tr => {
    tr.addEventListener('click', e => {
      if (e.target.closest('a')) return;
      openLandingPageBucket(tr.dataset.lpUrl);
    });
  });
  // Async overlay — if we didn't already paint with a cached map (or if the
  // date window changed), refresh the Sessions/Checkout% columns in place
  // once the RPC comes back. Only touches the two data-lp-* cells so any
  // sort/scroll state on the table is preserved.
  if (!cachedMap && state.dateFrom && state.dateTo){
    fetchLandingPageSessions(state.dateFrom, state.dateTo).then(map => {
      if (!map) return;
      host.querySelectorAll('tr[data-lp-url]').forEach(tr => {
        const url  = tr.dataset.lpUrl || '';
        const path = lpNormaliseToPath(url);
        const s = map[path];
        const sc = tr.querySelector('[data-lp-sess]');
        const cc = tr.querySelector('[data-lp-cvr]');
        if (sc) sc.textContent = s ? fmtInt(s.sessions) : '—';
        if (cc){
          if (s && s.sessions > 0 && s.sessions_that_reached_checkout != null){
            cc.textContent = ((s.sessions_that_reached_checkout / s.sessions) * 100).toFixed(2) + '%';
          } else {
            cc.textContent = '—';
          }
        }
      });
    });
  }
}
function openLandingPageBucket(url){
  bucketState.landingUrl = url;
  const slug = lpUrlSlug(url);
  openBucketModal('landing', url, slug);
}
function rerender(){
  const rows = filtered(primaryAds);
  renderKpis(rows);
  renderFunnel(rows);
  renderOverviewPerf(rows);
  const {p,c} = renderFocusStrips(rows);
  renderProdChart(p);
  renderCreativeChart(c);
  renderTrendChart(rows);
  renderLandingPageFocus(rows);
  // Ads Analyse depends on allAds (ae_table_view). If it loaded after
  // the user already navigated to the AE view, renderAE would never
  // fire (the sidebar handler bailed on the empty-allAds guard). Fire
  // it here so late-arriving data always paints.
  if (typeof renderAE === 'function' && (allAds || []).length){
    try { renderAE(); } catch(_){}
  }
  // Same guarantee for Lifecycle — depends on allAds + freqLifecycleByAdId.
  if (typeof renderLifecycle === 'function' && (allAds || []).length){
    try { renderLifecycle(); } catch(_){}
  }
}

document.getElementById('kpiRow').addEventListener('click', e=>{
  const k = e.target.closest('.kpi'); if(!k) return;
  openModal(k.dataset.filter);
});
function bindFilter(id, key, isDate){
  const el = document.getElementById(id); if(!el) return;
  el.addEventListener('change', ()=>{
    state[key] = el.value;
    if (isDate){
      // mark preset as "Custom"
      document.querySelectorAll('#presetRow .preset').forEach(x=>x.classList.remove('active'));
      const custom = document.querySelector('#presetRow .preset[data-p="custom"]');
      if (custom) custom.classList.add('active');
    }
    rerender();
  });
}
bindFilter('fAccount','acct');
bindFilter('fStatus','status');
bindFilter('fCampaign','campaign');
bindFilter('fContent','content');
bindFilter('fTier','tier');
bindFilter('fDateFrom','dateFrom',true);
bindFilter('fDateTo','dateTo',true);

/* Enrich primaryAds with the per-ad date fields (first_seen_date,
   date_of_result) that live on ae_table_view. primaryAds already carries
   ad_created from primary_table; the other two are needed by the new
   top-range field selector (first-seen / result modes). Called after any
   primaryAds/allAds refresh — no-op if allAds is empty. */
function enrichPrimaryDates(){
  if (!Array.isArray(allAds) || !allAds.length || !Array.isArray(primaryAds)) return;
  const byId = {};
  for (const a of allAds){
    if (a && a.ad_id) byId[a.ad_id] = a;
  }
  for (const p of primaryAds){
    const a = byId[p.ad_id];
    if (!a) continue;
    if (!p.first_seen_date && a.first_seen_date) p.first_seen_date = a.first_seen_date;
    if (!p.date_of_result  && a.date_of_result ) p.date_of_result  = a.date_of_result;
    // ad_created fallback for cache-path rows that came through without it
    if (!p.ad_created && a.ad_created) p.ad_created = a.ad_created;
  }
}

/* Date preset chips — change the date window AND re-fetch primary_table
   so Creative Testing aggregates only the daily rows in the new window
   (matches the OLD dashboard's behaviour). */
async function refreshPrimaryForRange(){
  // Try the precomputed cache first — hits when the (dateFrom, dateTo,
  // dateField) triple matches a results_table snapshot (i.e. the last-30d
  // window with either the 'delivery' or 'created' variant). Field-selector
  // changes benefit here: swapping Delivery ↔ Created no longer forces a
  // 2-3s live aggregation because the pipeline precomputes both.
  let rows = null;
  try {
    rows = await fetchPrimaryFromCache(state.dateFrom || '', state.dateTo || '', state.dateField);
  } catch (_){ rows = null; }
  if (!rows || !rows.length){
    rows = await fetchPrimaryAggregated(state.dateFrom || '', state.dateTo || '');
  }
  primaryAds = rows;
  enrichPrimaryDates();
  populateCampaignDropdown(primaryAds);
  rerender();
}
document.getElementById('presetRow').addEventListener('click', async e=>{
  const b = e.target.closest('.preset'); if(!b) return;
  document.querySelectorAll('#presetRow .preset').forEach(x=>x.classList.remove('active'));
  b.classList.add('active');
  applyDatePreset(b.dataset.p);
  await refreshPrimaryForRange();
});
// Manual date-input edits should also re-fetch
['fDateFrom','fDateTo'].forEach(id => {
  document.getElementById(id).addEventListener('change', () => {
    // debounce in case both inputs change in quick succession
    clearTimeout(window._ctDateDb);
    window._ctDateDb = setTimeout(refreshPrimaryForRange, 220);
  });
});

/* Date-field selector — picks which per-ad date the top range filters by.
   When 'delivery' (default), the range is passed to primary_table at fetch
   time and no further client filter runs. Switching to created/first_seen/
   result triggers a re-fetch of the full window; the client-side filter in
   filtered() then narrows to ads whose selected date falls in that range. */
document.getElementById('fDateField').addEventListener('change', async e => {
  state.dateField = e.target.value || 'delivery';
  // Delivery mode changes the fetch semantics — the window becomes a
  // primary_table date filter. Other modes want the widest window fetched
  // so the client-side filter has all candidates to narrow from. We
  // always re-fetch on mode-change to keep the KPI aggregates consistent.
  await refreshPrimaryForRange();
});

/* Reset Filters — clears the top date range, resets the field selector
   back to the dashboard default (created + last 30d), and un-forces the
   CT-format toggle (Excl. copy stays on because it's the sane default
   for the CT dashboard). */
document.getElementById('fCreatedClear').addEventListener('click', async () => {
  document.getElementById('fDateFrom').value = '';
  document.getElementById('fDateTo').value   = '';
  document.getElementById('fDateField').value = 'created';
  state.dateFrom = ''; state.dateTo = ''; state.dateField = 'created';
  // Snap preset chips back to "Last 30d" default
  document.querySelectorAll('#presetRow .preset').forEach(x => x.classList.remove('active'));
  const def = document.querySelector('#presetRow .preset[data-p="last30"]');
  if (def){ def.classList.add('active'); applyDatePreset('last30'); }
  await refreshPrimaryForRange();
});

/* Excl. copy + CT Format toggles — pure client-side filters on primaryAds */
function _ctToggleSync(btnId, on, labels){
  const btn = document.getElementById(btnId);
  if (!btn) return;
  btn.classList.toggle('on', on);
  const lbl = btn.querySelector('.ct-toggle-lbl');
  if (lbl && labels) lbl.textContent = on ? labels.on : labels.off;
}
// Initial UI state (defaults set in `state` above)
_ctToggleSync('fExclCopy', state.exclCopy, {on:'Excl. copy', off:'Incl. copy'});
document.getElementById('fExclCopy').addEventListener('click', () => {
  state.exclCopy = !state.exclCopy;
  _ctToggleSync('fExclCopy', state.exclCopy, {on:'Excl. copy', off:'Incl. copy'});
  rerender();
});

/* (Old single-input search bar removed — replaced by the Multi-Filter
   row above the preset chips. ctRules/ctMfRender drive filtering now.) */

/* Definitions modal */
function openDefModal(){
  document.getElementById('defModal').style.display='flex';
  document.getElementById('defModal').setAttribute('aria-hidden','false');
}
function closeDefModal(){
  document.getElementById('defModal').style.display='none';
  document.getElementById('defModal').setAttribute('aria-hidden','true');
}
document.getElementById('btnDef').onclick = openDefModal;
document.getElementById('defClose').onclick = closeDefModal;
document.getElementById('defModal').addEventListener('click', e=>{
  if (e.target.id === 'defModal') closeDefModal();
});
document.addEventListener('keydown', e=>{
  if (e.key === 'Escape') { closeDefModal(); closeBucketModal(); closeModal(); closeDrawer(); }
});

document.getElementById('btnRefresh').onclick = async ()=>{
  // Creative Testing reads primary_table for the current date window
  // (default = last 30d). Ads Analyse reads ae_table_view (lifetime).
  let freshReach;
  [primaryAds, allAds, thumbsByAdId, freshReach] = await Promise.all([
    fetchPrimaryAggregated(state.dateFrom || '', state.dateTo || ''),
    fetchAds(),
    fetchThumbnails(),
    fetchReachRecent()
  ]);
  reachRecentByAdId = freshReach;
  for (const r of allAds){
    const rr = reachRecentByAdId[r.ad_id];
    if (rr){
      r.previous_reach                   = rr.previous_reach;
      r.latest_reach                     = rr.latest_reach;
      r.incremental_reach                = rr.incremental_reach;
      r.cost_per_incremental_reach       = rr.cost_per_incremental_reach;
      r.cost_per_1000_incremental_reach  = rr.cost_per_1000_incremental_reach;
      r.latest_spend                     = rr.latest_spend;
    }
  }
  enrichPrimaryDates();
  aeApplyCurrentThresholds();
  rerender();
};
document.getElementById('btnExport').onclick = () => {
  // Creative Testing doesn't render a single ad-level table on this
  // view (it's KPI + bucket cards), so there's no <thead> to mirror.
  // We synthesise a stable header set and reuse the exporter with an
  // ad-hoc "table" — a hidden fragment lives only long enough for the
  // helper to read its headers, so filters + escaping stay consistent
  // across every section.
  const rows = filtered(primaryAds);
  const cols = [
    ['ad_id',           'Ad ID'          ],
    ['ad_name',         'Ad Name'        ],
    ['asset_id',        'Asset ID'       ],
    ['account_name',    'Account'        ],
    ['category',        'Category'       ],
    ['amount_spent',    'Spend'          ],
    ['roas_ma',         'ROAS'           ],
    ['ftewv_count',     'FTEWV'          ],
    ['ncp_count',       'NCP'            ],
    ['shopify_orders',  'Shop Orders'    ],
    ['shopify_sales',   'Shop Sales'     ],
    ['shopify_top_tier','Shop Top Tier'  ],
  ];
  const tmp = document.createElement('table');
  tmp.style.display = 'none';
  tmp.id = '_ctExportTmp_' + Date.now();
  tmp.innerHTML = '<thead><tr>' +
    cols.map(([k,l]) => '<th data-sort="'+k+'">'+l+'</th>').join('') +
    '</tr></thead>';
  document.body.appendChild(tmp);
  try {
    exportVisibleTableCsv('#' + tmp.id, rows, {
      filenamePrefix: 'creative_testing_v2',
      deriveRow: r => ({ ...r, asset_id: assetIdByAdId[r.ad_id] || '' }),
    });
  } finally {
    tmp.remove();
  }
};

/* ────────────────────────────────────────────────────────────────────
   VIEW SWITCHER — sidebar nav between Testing / Lifecycle / AE / Inventory
   ──────────────────────────────────────────────────────────────────── */
const VIEW_LOADED = {testing:true, lifecycle:false, ae:false, adintel:false, inventory:false, hreach:false, ireach:false};
// Historic mode per view — the CURRENT Ads Analyse / Ad Intelligence entries
// scope data to date >= 2025-01-01, the Historic ones show lifetime.  The
// sidebar click below flips this based on the item's data-historic attribute
// so a single view can serve both use-cases without duplicated renderers.
const HISTORIC_CUTOFF = '2025-01-01';
let historicMode = {ae:false, adintel:false};
let _lastLoadedHistoric = {ae:null, adintel:null};   // last loaded mode per view
document.querySelectorAll('.sb-item').forEach(it => {
  it.addEventListener('click', () => {
    const v = it.dataset.view;
    const isHistoric = it.dataset.historic === '1';
    // Active shortcut — set Group By + Status=ACTIVE on view-ae before render.
    // data-active-preset = "campaign" | "adset" | "ad"  (defaults to "ad")
    const activePreset = it.dataset.activePreset;
    if (v === 'ae' && activePreset){
      const gb = document.getElementById('aeGroupBy');
      const st = document.getElementById('aeStatus');
      if (gb) gb.value = activePreset;
      if (st) st.value = 'ACTIVE';
      // Reflect the preset on the pill toggle bar so it lights up correctly.
      document.querySelectorAll('#aeLevelToggle .ae-level-pill').forEach(p => {
        const on = p.dataset.level === activePreset;
        p.classList.toggle('active', on);
        p.setAttribute('aria-selected', on ? 'true' : 'false');
      });
      // aeSelectedCat drives category-filter KPI clicks; clear it so the
      // pre-set doesn't inherit a stale category from a prior view.
      try { aeSelectedCat = ''; } catch(_) {}
      const cat = document.getElementById('aeCategory'); if (cat) cat.value = '';
      try { aePage = 0; } catch(_) {}
      // Kick off the RPC rollup fetch so the first render at adset/campaign
      // level shows authoritative Shopify numbers.  Fire-and-forget — the
      // group-by function falls back to summed values if the RPC is still
      // resolving; a subsequent render will pick up the authoritative data.
      if (activePreset === 'adset' || activePreset === 'campaign'){
        try { fetchAeShopifyRollups(); } catch(_){}
      }
    } else if (v === 'ae' && !isHistoric){
      // Regular "Ads Analyse" — reset filters that the Active shortcut may
      // have left stuck. Users expect this button to be a "clean slate";
      // without the reset, a stale Status=ACTIVE causes ~6.7× fewer rows to
      // show than the delivery count in the footer suggests.
      const st = document.getElementById('aeStatus');
      const gb = document.getElementById('aeGroupBy');
      if (st && st.value === 'ACTIVE') st.value = '';
      if (gb && gb.value !== 'ad')     gb.value = 'ad';
      document.querySelectorAll('#aeLevelToggle .ae-level-pill').forEach(p => {
        const on = p.dataset.level === 'ad';
        p.classList.toggle('active', on);
        p.setAttribute('aria-selected', on ? 'true' : 'false');
      });
    }
    // Flip mode BEFORE the view renders so it reads the right flag.
    if (v in historicMode){
      // When the mode actually flips for AE, invalidate the reach cache so
      // the RPC gets re-called with the new mode's default window
      // (post-2025 vs pre-2025). Without this the stale key sticks.
      if (v === 'ae' && historicMode.ae !== isHistoric){
        _aeWindowReachKey = '';
      }
      historicMode[v] = isHistoric;
    }
    document.querySelectorAll('.sb-item').forEach(s => s.classList.remove('active'));
    it.classList.add('active');
    document.querySelectorAll('.view').forEach(vv => vv.style.display = 'none');
    const target = document.getElementById('view-' + v);
    if (target) { target.style.display = 'block'; }
    // Toggle landing-page mode (sidebar hidden, main goes full width).
    document.body.classList.toggle('on-home', v === 'home');
    // Toggle the "Historic (Lifetime)" banner on the view's page header
    const banner = target?.querySelector('.historic-banner');
    if (banner) banner.style.display = isHistoric ? 'inline-flex' : 'none';
    // Lazy-load per-view data
    if (v === 'lifecycle' && allAds.length) renderLifecycle();
    if (v === 'ae'        && allAds.length) {
      // Kick off group-level Shopify RPCs so the level toggle has
      // authoritative data ready the moment the user flips a pill.
      // No-op after the first call for a given window; safe fire-and-forget.
      try { fetchAeShopifyRollups().then(() => renderAE()); } catch(_){}
      renderAE();
    }
    if (v === 'adintel'){
      // If historic-mode changed since last load, re-fetch with the new window.
      const modeChanged = _lastLoadedHistoric.adintel !== null &&
                          _lastLoadedHistoric.adintel !== isHistoric;
      if (!VIEW_LOADED.adintel){
        initAdIntel();
        aiApplyHistoricPreset(isHistoric);
        aiReloadOrders();
        VIEW_LOADED.adintel = true;
      } else if (modeChanged){
        aiApplyHistoricPreset(isHistoric);
        aiReloadOrders();
      }
      _lastLoadedHistoric.adintel = isHistoric;
    }
    if (v === 'inventory' && !VIEW_LOADED.inventory) { loadInventory(); VIEW_LOADED.inventory = true; }
    if (v === 'hreach' && !VIEW_LOADED.hreach) {
      // First open — seed the inputs to a small window (last 7d) so the
      // auto-fetch is snappy. Users can widen to 30/90d manually via the
      // preset chips. A 30d union of primary+backfill can be 300k+ rows.
      _hreachApplyPreset('last7');
      // Mirror the active state on the preset chip
      document.querySelectorAll('#view-hreach .preset-row .preset').forEach(b => b.classList.remove('active'));
      const chip = document.querySelector('#view-hreach .preset[data-p="last7"]');
      if (chip) chip.classList.add('active');
      _hreachApply();
      VIEW_LOADED.hreach = true;
    }
    if (v === 'ireach' && !VIEW_LOADED.ireach) {
      // Same "seed to last-7d for a snappy first open" behaviour as hreach —
      // user can widen to 30d/90d/YTD via the preset chips.
      _ireachApplyPreset('last7');
      document.querySelectorAll('#view-ireach .preset-row .preset').forEach(b => b.classList.remove('active'));
      const chip = document.querySelector('#view-ireach .preset[data-p="last7"]');
      if (chip) chip.classList.add('active');
      _ireachApply();
      VIEW_LOADED.ireach = true;
    }
    if (v === 'landing'){
      // Always refresh sync-status on entry — backfill is running in the
      // background so the numbers move minute-by-minute.  Cheap probe.
      try { loadLandingPageStatus(); } catch(_){}
      try { loadLandingPageTable(); } catch(_){}
    }
    // Auto-close the mobile sidebar after selection
    if (window.innerWidth <= 900) document.getElementById('sidebar').classList.remove('open');
  });
});

/* Landing-page cards — clicking any card is equivalent to clicking its
   sidebar item. We find the matching sidebar item by matching data-view,
   data-historic, and data-active-preset, then dispatch a click so ALL the
   existing sidebar-handler logic runs (historic-mode flip, Active preset,
   lazy-load, mobile sidebar auto-close, banner toggle, etc.). */
document.querySelectorAll('.home-card').forEach(card => {
  card.addEventListener('click', () => {
    const view    = card.dataset.targetView;
    const preset  = card.dataset.activePreset || '';
    const histo   = card.dataset.historic === '1' ? '1' : '';
    if (!view) return;
    // Find the sidebar item whose (data-view, preset, historic) match.
    const items = document.querySelectorAll('.sb-item');
    let picked = null;
    for (const it of items){
      const sameView    = it.dataset.view === view;
      const samePreset  = (it.dataset.activePreset || '') === preset;
      const sameHistoric= (it.dataset.historic === '1' ? '1' : '') === histo;
      if (sameView && samePreset && sameHistoric){ picked = it; break; }
    }
    if (picked) picked.click();
  });
});

/* Hamburger toggle — mobile / tablet only (see @media 900px in CSS).
   Also close when clicking outside the sidebar on those viewports. */
document.getElementById('sbToggle')?.addEventListener('click', e => {
  e.stopPropagation();
  document.getElementById('sidebar').classList.toggle('open');
});
document.addEventListener('click', e => {
  if (window.innerWidth > 900) return;
  const sb = document.getElementById('sidebar');
  const tog = document.getElementById('sbToggle');
  if (sb.classList.contains('open') && !sb.contains(e.target) && e.target !== tog){
    sb.classList.remove('open');
  }
});

/* ────────────────────────────────────────────────────────────────────
   CREATIVE LIFECYCLE — Frequency Buckets + 14-Day Buffer Resolver
   Mirrors the logic in index.html's fatiguePanel.
   ──────────────────────────────────────────────────────────────────── */

// Effective frequency per ad: prefer ltv_frequency (Meta dedup'd), else
// fall back to impressions/reach. Anything > 3 is excluded (matches index.html).
function effFreq(a){
  const ltv = +a.ltv_frequency || 0;
  if (ltv > 0) return ltv;
  const imp = +a.impressions || 0, reach = +a.reach || 0;
  return reach > 0 ? imp / reach : 0;
}

let freqActiveBucket = null;
let freqStatusFilter = '';       // '', 'ACTIVE', 'PAUSED', 'ARCHIVED' — applies to Frequency section
let freqSearch       = '';       // free-text multi-token AND-search for the Frequency section
let freqDrillSort    = {key:'amount_spent', dir:'desc'};  // default: highest-spend first
let lifeBufferStatus = '';
let lifeBufferSearch = '';
let lifeSection      = 'freq';   // 'freq' (frequency distribution) | 'buffer' (14-day)

/* Normalise Meta's ad_status into the three chip buckets. Everything that
   isn't ACTIVE or PAUSED (archived, deleted, disapproved, etc.) is
   grouped under ARCHIVED so the chip row stays a clean three-way split. */
function freqStatusOf(r){
  const s = (r.ad_status || '').toUpperCase();
  if (s === 'ACTIVE') return 'ACTIVE';
  if (s.includes('PAUSED')) return 'PAUSED';
  return 'ARCHIVED';
}

/* Filter ae_table_view rows by ad_created against the Lifecycle DRP window. */
function lifeFilterByDate(rows){
  const f = document.getElementById('lifeDateFrom')?.value || '';
  const t = document.getElementById('lifeDateTo')?.value   || '';
  if (!f && !t) return rows;
  const from = f ? new Date(f+'T00:00:00') : null;
  const to   = t ? new Date(t+'T23:59:59') : null;
  return rows.filter(a => {
    if (!a.ad_created) return false;
    const d = new Date(a.ad_created);
    if (from && d < from) return false;
    if (to   && d > to)   return false;
    return true;
  });
}
// Fetches summary_table.refreshed_at once per session and caches it. Every
// row shares the same refreshed_at (whole-table UPSERT stamps NOW() on the
// refreshed_at column for all rows), so a single value is authoritative.
let _summaryRefreshedAt = null;
async function _fetchSummaryRefreshedAt(){
  if (_summaryRefreshedAt !== null) return _summaryRefreshedAt;
  if (!SUPABASE_URL || !SUPABASE_ANON){ _summaryRefreshedAt = ''; return ''; }
  try {
    const r = await fetch(SUPABASE_URL +
      '/rest/v1/summary_table?select=refreshed_at&order=refreshed_at.desc&limit=1',
      {headers:{apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                Prefer:'count=none'}});
    if (!r.ok) throw new Error('HTTP '+r.status);
    const j = await r.json();
    _summaryRefreshedAt = (Array.isArray(j) && j[0]?.refreshed_at) || '';
  } catch { _summaryRefreshedAt = ''; }
  return _summaryRefreshedAt;
}
// Fetches verdict-history columns from summary_table for every ad and
// merges them onto allAds by ad_id. Called from renderLifecycle so the
// 14-day buffer resolver has status_at / prev_status / prev_status_at
// available for its "current verdict changed on" and "last known verdict"
// cells. Cached module-level so subsequent renders don't re-hit the API.
let _summaryHistoryMerged = false;
async function _mergeSummaryHistoryIntoAllAds(){
  if (_summaryHistoryMerged) return;
  if (!SUPABASE_URL || !SUPABASE_ANON) return;
  if (!Array.isArray(allAds) || !allAds.length) return;
  try {
    const cols = 'ad_id,status,status_at,prev_status,prev_status_at,refreshed_at';
    const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                     Prefer:'count=none'};
    let out = [], offset = 0, BATCH = 5000;
    while (true){
      const r = await fetch(SUPABASE_URL +
        '/rest/v1/summary_table?select=' + cols +
        '&order=ad_id.asc&limit=' + BATCH + '&offset=' + offset,
        {headers});
      if (!r.ok) break;
      const j = await r.json();
      if (!Array.isArray(j) || !j.length) break;
      out = out.concat(j);
      if (j.length < BATCH) break;
      offset += BATCH;
    }
    const byId = {};
    for (const row of out) byId[row.ad_id] = row;
    for (const a of allAds){
      const h = byId[a.ad_id];
      if (!h) continue;
      a.summary_status       = h.status;
      a.summary_status_at    = h.status_at;
      a.summary_prev_status  = h.prev_status;
      a.summary_prev_at      = h.prev_status_at;
    }
    _summaryHistoryMerged = true;
  } catch { /* best effort — buffer resolver renders without history */ }
}
// Human-readable relative-time formatter: "3 h ago", "2 d ago", "just now".
function _relTime(iso){
  if (!iso) return '';
  const d = new Date(iso); if (isNaN(d)) return '';
  const secs = Math.max(0, Math.floor((Date.now() - d.getTime()) / 1000));
  if (secs < 60)    return 'just now';
  if (secs < 3600)  return Math.floor(secs/60) + ' m ago';
  if (secs < 86400) return Math.floor(secs/3600) + ' h ago';
  return Math.floor(secs/86400) + ' d ago';
}

function renderLifecycle(){
  /* Apply the Lifecycle date-range filter (ad_created window) once up front,
     so both the frequency buckets and the 14-day buffer read the same set. */
  const lifeRowsAll = lifeFilterByDate(allAds);
  /* Frequency-section status filter (chip row above the buckets). Applies
     to KPIs, bucket counts, AND the drill so the numbers stay coherent
     with what the user sees. The 14-day buffer below still reads
     lifeRowsAll so it isn't gated by this chip. */
  let lifeRows = freqStatusFilter
    ? lifeRowsAll.filter(r => freqStatusOf(r) === freqStatusFilter)
    : lifeRowsAll;
  /* Multi-token AND search over name / campaign / ad_id / account / category */
  const tokens = (freqSearch || '').toLowerCase().split(/\s+/).filter(Boolean);
  if (tokens.length){
    lifeRows = lifeRows.filter(r => {
      const hay = ((r.ad_name||'')+' '+(r.campaign_name||'')+' '+(r.ad_id||'')+' '+
                   (r.account_name||'')+' '+(r.category||'')+' '+(r.adset_name||'')).toLowerCase();
      return tokens.every(t => hay.includes(t));
    });
  }
  const hintEl = document.getElementById('lifeFreqSearchHint');
  if (hintEl) hintEl.textContent = tokens.length
    ? fmtInt(lifeRows.length) + ' / ' + fmtInt(lifeRowsAll.length) + ' matched'
    : '';

  /* ── Lifetime totals across ALL ads in scope (post-status filter) ── */
  const totalSpend = lifeRows.reduce((s,r) => s + (+r.amount_spent  || 0), 0);
  const totalReach = lifeRows.reduce((s,r) => s + (+r.reach         || 0), 0);
  const totalImpr  = lifeRows.reduce((s,r) => s + (+r.impressions   || 0), 0);
  document.getElementById('freqTotAds'  ).textContent = fmtInt(lifeRows.length);
  document.getElementById('freqTotSpend').textContent = fmtRs(totalSpend);
  document.getElementById('freqTotReach').textContent = fmtInt(totalReach);
  document.getElementById('freqTotImpr' ).textContent = fmtInt(totalImpr);

  /* ── 6 buckets keyed by max_cum_freq. b0 = never served (no lifecycle row) ── */
  const buckets = {b0:[], b1:[], b2:[], b3:[], b4:[], b5:[]};
  for (const r of lifeRows){
    const fx = freqLifecycleByAdId[r.ad_id];
    if (!fx){ buckets.b0.push(r); continue; }   // ad has no served days → <1×
    const m = +fx.max_cum_freq || 0;
    if      (m >= 3.0) buckets.b5.push(r);
    else if (m >= 2.5) buckets.b4.push(r);
    else if (m >= 2.0) buckets.b3.push(r);
    else if (m >= 1.5) buckets.b2.push(r);
    else if (m >= 1.0) buckets.b1.push(r);
    else               buckets.b0.push(r);      // edge-case: served but stayed <1
  }
  const sumF = (rows, k) => rows.reduce((s,r) => s + (+r[k] || 0), 0);
  ['b0','b1','b2','b3','b4','b5'].forEach(k => {
    const rows = buckets[k];
    document.getElementById('freq'+k.toUpperCase()+'Count').textContent = fmtInt(rows.length);
    document.getElementById('freq'+k.toUpperCase()+'Spend').textContent = fmtRs (sumF(rows,'amount_spent'));
    document.getElementById('freq'+k.toUpperCase()+'Reach').textContent = fmtInt(sumF(rows,'reach'));
  });
  document.getElementById('lifeFreqTot').textContent =
    fmtInt(lifeRows.length) + ' ads in lifecycle';

  /* ── Render drill table for active bucket ─────────────────────────── */
  if (freqActiveBucket && buckets[freqActiveBucket]) {
    document.getElementById('freqDrill').style.display = 'block';
    // Which crossing date qualifies each ad for the CURRENT bucket
    const enteredKey = {b0:null, b1:'d_1', b2:'d_1_5', b3:'d_2', b4:'d_2_5', b5:'d_3'}[freqActiveBucket];
    const enteredLbl = {b0:'—', b1:'1×', b2:'1.5×', b3:'2×', b4:'2.5×', b5:'3×'}[freqActiveBucket];
    document.getElementById('freqDrillEnteredHdr').textContent =
      'Entered ' + (enteredLbl === '—' ? 'bucket' : enteredLbl);
    // Sort rows by freqDrillSort. Crossing-date columns pull from the
    // fx lookup, everything else lives directly on the ae row.
    const FX_DATE_KEYS = new Set(['d_1','d_1_5','d_2','d_2_5','d_3']);
    const FX_NUM_KEYS  = new Set(['max_cum_freq']);
    const sortVal = r => {
      const k = freqDrillSort.key;
      const fx = freqLifecycleByAdId[r.ad_id] || {};
      if (k === 'entered')       return enteredKey ? (fx[enteredKey] || '') : '';
      if (FX_DATE_KEYS.has(k))   return fx[k] || '';
      if (FX_NUM_KEYS.has(k))    return +fx[k] || 0;
      if (k === 'amount_spent' || k === 'reach') return +r[k] || 0;
      return (r[k] || '').toString().toLowerCase();
    };
    const dir = freqDrillSort.dir === 'asc' ? 1 : -1;
    const rows = buckets[freqActiveBucket].slice().sort((a,b) => {
      const va = sortVal(a), vb = sortVal(b);
      // Nulls/empties always land at the bottom regardless of dir
      const aE = (va === '' || va == null), bE = (vb === '' || vb == null);
      if (aE && bE) return 0;
      if (aE) return 1;
      if (bE) return -1;
      if (va < vb) return -1 * dir;
      if (va > vb) return  1 * dir;
      return 0;
    }).slice(0, 300);
    // Reflect sort state on the headers
    document.querySelectorAll('#freqDrillTable th.sortable').forEach(th => {
      th.classList.remove('sort-asc','sort-desc');
      if (th.dataset.sort === freqDrillSort.key){
        th.classList.add(freqDrillSort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
      }
    });
    // Impressions / reach / spend on the day the ad crossed this threshold.
    // The date lives in the "Entered N×" column and the individual sort
    // headers, so we don't repeat it inside every cell.
    const cx = (d, imp, reach, spend) => {
      if (!d) return '<td class="fx-cell fx-empty">—</td>';
      return '<td class="fx-cell">'+
        '<span class="fx-metric">'+fmtInt(imp)+' impr</span><br>'+
        '<span class="fx-metric">'+fmtInt(reach)+' reach</span><br>'+
        '<span class="fx-metric">'+fmtRs(spend)+' spend</span>'+
      '</td>';
    };
    document.getElementById('freqDrillBody').innerHTML = rows.map(r => {
      const fx = freqLifecycleByAdId[r.ad_id] || {};
      const stCls = (r.ad_status||'').toUpperCase() === 'ACTIVE' ? 's-active'
                  : (r.ad_status||'').toUpperCase().includes('PAUSED') ? 's-draft' : 's-archived';
      const enteredDate = enteredKey ? (fx[enteredKey] || '—') : '—';
      return '<tr>'+
        '<td style="max-width:280px"><div style="font-weight:600;color:var(--text-primary);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+
          (r.ad_name||'—')+'</div>'+
          '<div style="font-size:10px;color:var(--text-tertiary)">'+(r.campaign_name||'')+'</div></td>'+
        '<td><span class="status-pill '+stCls+'">'+(r.ad_status||'—')+'</span></td>'+
        '<td class="num">'+(fx.max_cum_freq != null ? (+fx.max_cum_freq).toFixed(2)+'×' : '—')+'</td>'+
        '<td class="mono" style="font-size:11px">'+enteredDate+'</td>'+
        cx(fx.d_1,   fx.imp_at_1,   fx.reach_at_1,   fx.spend_at_1)+
        cx(fx.d_1_5, fx.imp_at_1_5, fx.reach_at_1_5, fx.spend_at_1_5)+
        cx(fx.d_2,   fx.imp_at_2,   fx.reach_at_2,   fx.spend_at_2)+
        cx(fx.d_2_5, fx.imp_at_2_5, fx.reach_at_2_5, fx.spend_at_2_5)+
        cx(fx.d_3,   fx.imp_at_3,   fx.reach_at_3,   fx.spend_at_3)+
        '<td class="num">'+fmtRs(r.amount_spent)+'</td>'+
        '<td class="num">'+fmtInt(r.reach)+'</td>'+
      '</tr>';
    }).join('');
  } else {
    document.getElementById('freqDrill').style.display = 'none';
  }

  /* ── 14-Day buffer ────────────────────────────────────────────────── */
  const today = new Date(); today.setHours(0,0,0,0);
  let bufRows = lifeRowsAll.filter(a => {
    if (!a.ad_created) return false;
    const days = Math.floor((today - new Date(a.ad_created)) / 86400000);
    return days >= 14;
  });

  // Status filter pills
  if (lifeBufferStatus) bufRows = bufRows.filter(r => r.category === lifeBufferStatus);
  if (lifeBufferSearch) {
    const q = lifeBufferSearch.toLowerCase().trim();
    bufRows = bufRows.filter(r => ((r.ad_name||'')+' '+(r.campaign_name||'')+' '+(r.ad_id||'')).toLowerCase().includes(q));
  }
  // "Drifted only" toggle — surface ads whose live-computed verdict has moved
  // away from the DB-recorded current status (e.g. Winner → Discarded). Uses
  // summary_status (fetched from summary_table.status) when available,
  // falling back to db_category (the ae_table_view baked-in value).
  const driftEl = document.getElementById('lifeBufferDriftOnly');
  if (driftEl && driftEl.checked){
    bufRows = bufRows.filter(r => {
      const baseline = r.summary_status || r.db_category;
      return baseline && baseline !== r.category;
    });
  }
  bufRows.sort((a,b) => (+b.amount_spent || 0) - (+a.amount_spent || 0));

  const tierClass = c => ({
    'Winner':'winner','Incremental Winner':'incwinner','P0 analysis':'priority',
    'P1 analysis':'analyze1','P2 analysis':'analyze2',
    'Result Awaited':'ra','Discarded':'discarded'
  })[c] || 'discarded';
  const passCell = v => v ? '<span class="pass-yes">✓</span>' : '<span class="pass-no">✗</span>';

  // Populate the "Tag last updated" header chip — asynchronous, no re-render
  // needed since the chip is a plain DOM update.
  _fetchSummaryRefreshedAt().then(ts => {
    const chip  = document.getElementById('lifeBufferUpdated');
    const chipV = document.getElementById('lifeBufferUpdatedVal');
    if (!chip || !chipV) return;
    if (!ts){ chip.style.display = 'none'; return; }
    const d = new Date(ts);
    const iso = isNaN(d) ? ts : d.toISOString().replace('T',' ').slice(0,16);
    chipV.textContent = _relTime(ts) + ' · ' + iso + ' UTC';
    chip.style.display = 'inline-flex';
  });
  // Merge summary_table verdict history (status_at, prev_status, prev_status_at)
  // onto allAds — fires once per session then re-renders so the fresh history
  // cells appear on the next paint. renderLifecycle() is idempotent so re-render
  // is cheap.
  _mergeSummaryHistoryIntoAllAds().then(() => {
    if (_summaryHistoryMerged && !window._bufReRendered){
      window._bufReRendered = true;
      renderLifecycle();
    }
  });

  const isoDate = ts => {
    if (!ts) return '—';
    const d = new Date(ts); if (isNaN(d)) return '—';
    return d.toISOString().slice(0,10);
  };

  document.getElementById('lifeBufferBody').innerHTML = bufRows.slice(0, 500).map(r => {
    const stCls = (r.ad_status||'').toUpperCase() === 'ACTIVE' ? 's-active'
                : (r.ad_status||'').toUpperCase().includes('PAUSED') ? 's-draft' : 's-archived';
    // Verdict-history columns come from summary_table via
    // _mergeSummaryHistoryIntoAllAds(); fallback to live client-computed
    // r.category when the DB history hasn't been fetched yet or the ad has
    // no summary_table row (rare — brand-new ads).
    const currentVerdict = r.summary_status || r.category || '—';
    const prevVerdict    = r.summary_prev_status || null;
    const statusAt       = r.summary_status_at   || null;
    const prevAt         = r.summary_prev_at     || null;
    // ALWAYS show the current-verdict date (status_at is the moment the ad
    // was tagged with its current verdict — this is meaningful even when
    // no prior transition exists). When prev_status_at is null or equals
    // status_at (first record, ad has never transitioned), tag the cell
    // with a subtle "unchanged" chip AFTER the date so the timeline stays
    // legible.
    const noTransition = !prevAt || statusAt === prevAt;
    const currentSince = statusAt
      ? '<span class="mono buf-timeago" title="' + statusAt + '">' + isoDate(statusAt) +
        '<div class="buf-rel">' + _relTime(statusAt) + '</div></span>' +
        (noTransition ? '<span class="buf-unchanged" title="This ad has never transitioned into another verdict">unchanged</span>' : '')
      : '<span style="color:var(--text-tertiary)">—</span>';
    const lastKnown = prevVerdict
      ? '<span class="tier-badge ' + tierClass(prevVerdict) + '">' + prevVerdict + '</span>'
      : '<span style="color:var(--text-tertiary)">—</span>';
    const lastOn = prevAt
      ? '<span class="mono buf-timeago" title="' + prevAt + '">' + isoDate(prevAt) + '<div class="buf-rel">' + _relTime(prevAt) + '</div></span>'
      : '<span style="color:var(--text-tertiary)">—</span>';
    // 3-filter model: F1 = impressions, F2 = ROAS OR Cost/NCP (either f2_pass
    // or f3_pass), F3 = Cost/FTEWV (was f4_pass).
    const f2Group = r.f2_pass || r.f3_pass;
    return '<tr>'+
      '<td style="max-width:260px"><div style="font-weight:600;color:var(--text-primary);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+
        (r.ad_name||'—')+'</div>'+
        '<div style="font-size:10px;color:var(--text-tertiary)">'+(r.campaign_name||'')+'</div></td>'+
      '<td style="font-family:JetBrains Mono;font-size:11px">'+(r.ad_created||'—')+'</td>'+
      '<td><span class="status-pill '+stCls+'">'+(r.ad_status||'—')+'</span></td>'+
      '<td><span class="tier-badge '+tierClass(currentVerdict)+'">'+currentVerdict+'</span></td>'+
      '<td>'+currentSince+'</td>'+
      '<td>'+lastKnown+'</td>'+
      '<td>'+lastOn+'</td>'+
      '<td style="text-align:center">'+passCell(r.f1_pass)+'</td>'+
      '<td style="text-align:center">'+passCell(f2Group)+'</td>'+
      '<td style="text-align:center">'+passCell(r.f4_pass)+'</td>'+
      '<td class="num">'+fmtInt(r.impressions)+'</td>'+
      '<td class="num">'+fmtRs(r.amount_spent)+'</td>'+
      '<td class="num">'+fmtRoas(r.roas_ma)+'</td>'+
      '<td class="num">'+fmtInt(r.ncp_count)+'</td>'+
      '<td class="num">'+fmtInt(r.ftewv_count)+'</td>'+
    '</tr>';
  }).join('');
  document.getElementById('lifeBufferTot').textContent = fmtInt(bufRows.length) + ' ads (showing top 500)';
}

// Section toggle (Frequency / Buffer)
document.getElementById('lifeSectionToggle').addEventListener('click', e => {
  const btn = e.target.closest('.lt-btn'); if (!btn) return;
  document.querySelectorAll('#lifeSectionToggle .lt-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  lifeSection = btn.dataset.sec;
  document.getElementById('lifeFreqSection'  ).style.display = lifeSection === 'freq'   ? 'block' : 'none';
  document.getElementById('lifeBufferSection').style.display = lifeSection === 'buffer' ? 'block' : 'none';
});

// Wire up bucket click + 14-day buffer filters
document.getElementById('lifeFreqBuckets').addEventListener('click', e => {
  const card = e.target.closest('.freq-bucket'); if (!card) return;
  const k = card.dataset.b;
  if (freqActiveBucket === k) { freqActiveBucket = null; card.classList.remove('active'); }
  else {
    freqActiveBucket = k;
    document.querySelectorAll('.freq-bucket').forEach(b => b.classList.remove('active'));
    card.classList.add('active');
  }
  renderLifecycle();
});
document.getElementById('lifeFreqStatusRow').addEventListener('click', e => {
  const btn = e.target.closest('.preset'); if (!btn) return;
  document.querySelectorAll('#lifeFreqStatusRow .preset').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  freqStatusFilter = btn.dataset.s || '';
  renderLifecycle();
});
let freqSearchDb = null;
document.getElementById('lifeFreqSearch').addEventListener('input', e => {
  clearTimeout(freqSearchDb);
  freqSearchDb = setTimeout(() => { freqSearch = e.target.value; renderLifecycle(); }, 180);
});
// Sortable drill headers: click a column to sort by it; clicking the same
// column toggles direction. Date columns default to ascending on first
// click (chronological), numerics default to descending (top-value first).
document.getElementById('freqDrillTable').addEventListener('click', e => {
  const th = e.target.closest('th.sortable'); if (!th) return;
  const k = th.dataset.sort;
  if (freqDrillSort.key === k){
    freqDrillSort.dir = freqDrillSort.dir === 'asc' ? 'desc' : 'asc';
  } else {
    freqDrillSort.key = k;
    // Sensible first-click default per column family
    const dateFirst = ['entered','d_1','d_1_5','d_2','d_2_5','d_3'];
    freqDrillSort.dir = dateFirst.includes(k) ? 'asc' : 'desc';
  }
  renderLifecycle();
});
document.getElementById('lifeBufferStatusRow').addEventListener('click', e => {
  const btn = e.target.closest('.preset'); if (!btn) return;
  document.querySelectorAll('#lifeBufferStatusRow .preset').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  lifeBufferStatus = btn.dataset.s;
  renderLifecycle();
});
let lifeSearchDb = null;
document.getElementById('lifeBufferSearch').addEventListener('input', e => {
  clearTimeout(lifeSearchDb);
  lifeSearchDb = setTimeout(() => { lifeBufferSearch = e.target.value; renderLifecycle(); }, 200);
});
// Drifted-only checkbox — narrows the buffer table to rows where the DB-baked
// Marked-as no longer matches the live Verdict (metric-drift snapshot).
document.getElementById('lifeBufferDriftOnly').addEventListener('change', () => {
  renderLifecycle();
});
document.getElementById('lifeRefresh').onclick = async () => {
  const [freshAllAds, freshFreq] = await Promise.all([fetchAds(), fetchFreqLifecycle()]);
  allAds = freshAllAds;
  freqLifecycleByAdId = freshFreq;
  renderLifecycle();
};
document.getElementById('lifeExport').onclick = () => {
  // Same 14-day cutoff the view uses; column set is driven by the
  // visible <thead> of #lifeBufferTbl so hidden columns stay out.
  const today = new Date(); today.setHours(0,0,0,0);
  const rows = allAds.filter(a => {
    if (!a.ad_created) return false;
    return Math.floor((today - new Date(a.ad_created)) / 86400000) >= 14;
  });
  exportVisibleTableCsv('#lifeBufferTbl', rows, { filenamePrefix: 'lifecycle_14day_buffer' });
};

/* ────────────────────────────────────────────────────────────────────
   ADS ANALYSE — native v2 redesign, same ae_table_view logic as the old AE
   Computes f1/f2/f3/f4 client-side from current threshold inputs.
   Recategorises into Winner / Inc.Winner / Priority / Analyze1/2 / Discarded
   using the same rules as refresh_ae_table.py:
       f1 (Min Imp) AND (f2 OR f3) AND f4  → Incremental Winner
       f1 AND (f2 OR f3)                   → Winner
       f1 AND f4                           → P0 analysis
       f1                                  → P1 analysis
       f2                                  → P2 analysis
       else                                → Discarded
   ──────────────────────────────────────────────────────────────────── */
let aeSelectedCat = '';
let aeSortKey  = 'amount_spent';
let aeSortDir  = 'desc';
let aePage     = 0;             // 0-based page index for the AE table
let aeRules    = [];            // TEXT rules (Name/Status/Category/…) — inline Multi-Filter card
let aeRulesNum = [];            // NUMERIC rules (Spend/ROAS/Impressions/…) — right-side Inspector drawer
// Set<ad_id> of ads that delivered inside the current date window.
// null = not fetched yet / no active window; empty Set = fetched, no matches.
let aeDeliverySet = null;
// Per-ad windowed metrics (impressions, reach, spend, ...) for the current
// AE date range. Populated by fetchAeWindowMetrics() whenever the range
// changes. When set, renderAE prefers these over the lifetime totals on
// each row so the numbers reflect the selected dates.
let aeWindowMetricsByAdId = {};
let _aeWindowMetricsKey    = '';
// Per-ad windowed Shopify metrics — same idea but sourced from
// shopify_ad_attribution filtered by order_created_at inside the AE date
// range.  Overlayed onto rows in aeApplyWindow so the Shopify orders/sales
// columns move in step with the date picker.
let aeWindowShopifyByAdId = {};
let _aeWindowShopifyKey    = '';
// Group-level Shopify rollups — populated from get_shopify_by_adset /
// get_shopify_by_campaign RPCs. Keyed by adset_id / campaign_name so
// aeGroupBy() can overlay authoritative totals (which include orders
// resolved at the group scope, not just those pinned to individual ads).
let aeShopifyByAdset    = {};
let aeShopifyByCampaign = {};
let _aeShopifyRollupKey = '';
// Per-ad windowed reach snapshot — pulled from primary_table + backfill_table
// so the Prev/Latest/Incr reach columns follow the AE date picker rather than
// showing the fixed latest/previous-day snapshot from ae_reach_recent.  Keys
// on "from|to"; recomputed on every date-range change.
let aeWindowReachByAdId = {};
let _aeWindowReachKey    = '';
async function fetchAeWindowMetrics(){
  const from = document.getElementById('aeDateFrom').value || '';
  const to   = document.getElementById('aeDateTo').value   || '';
  const key  = from + '|' + to;
  if (!from || !to){
    aeWindowMetricsByAdId = {}; _aeWindowMetricsKey = ''; return;
  }
  if (key === _aeWindowMetricsKey) return;  // already fetched for this range
  _aeWindowMetricsKey = key;
  if (!SUPABASE_URL || !SUPABASE_ANON){ aeWindowMetricsByAdId = {}; return; }
  try {
    const r = await fetch(SUPABASE_URL + '/rest/v1/rpc/get_ae_metrics_by_window', {
      method:'POST',
      headers:{apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
               'Content-Type':'application/json'},
      body: JSON.stringify({from_date: from, to_date: to})
    });
    if (!r.ok){
      console.warn('[fetchAeWindowMetrics] HTTP', r.status,
                   await r.text().catch(()=>''));
      aeWindowMetricsByAdId = {}; return;
    }
    const rows = await r.json();
    const out = {};
    for (const row of rows){
      if (!row.ad_id) continue;
      const impr  = +row.impressions || 0;
      const reach = +row.reach_sum   || 0;
      const spend = +row.spend       || 0;
      const conv  = +row.conv_value  || 0;
      const ftewv = +row.ftewv       || 0;
      const ncp   = +row.ncp         || 0;
      out[row.ad_id] = {
        days_active:     +row.days_active || 0,
        impressions:     impr,
        reach:           reach,
        reach_peak:      +row.reach_peak || 0,
        amount_spent:    spend,
        conv_value:      conv,
        purchases:       +row.purchases  || 0,
        link_clicks_raw: +row.link_clicks || 0,
        ftewv_count:     ftewv,
        ncp_count:       ncp,
        // Derived
        frequency:       reach > 0 ? impr / reach : 0,
        cost_per_1000:   impr  > 0 ? spend / impr * 1000 : 0,
        ctr_pct:         impr  > 0 ? (+row.link_clicks || 0) / impr * 100 : 0,
        roas_ma:         spend > 0 ? conv / spend : 0,
        cost_per_ftewv:  ftewv > 0 ? spend / ftewv : 0,
        cost_per_ncp:    ncp   > 0 ? spend / ncp   : 0,
      };
    }
    aeWindowMetricsByAdId = out;
  } catch (e){
    console.warn('[fetchAeWindowMetrics] network error', e);
    aeWindowMetricsByAdId = {};
  }
}
// Fetch windowed Shopify metrics — pulls attribution rows in the AE date
// range and aggregates by ad_id (orders count + sum(total_price)). Paginates
// through shopify_ad_attribution because a wide window can exceed the
// PostgREST anon row cap; sum happens client-side into aeWindowShopifyByAdId.
async function fetchAeWindowShopify(){
  const from = document.getElementById('aeDateFrom').value || '';
  const to   = document.getElementById('aeDateTo').value   || '';
  const key  = from + '|' + to;
  if (!from || !to){
    aeWindowShopifyByAdId = {}; _aeWindowShopifyKey = ''; return;
  }
  if (key === _aeWindowShopifyKey) return;
  _aeWindowShopifyKey = key;
  if (!SUPABASE_URL || !SUPABASE_ANON){ aeWindowShopifyByAdId = {}; return; }
  try {
    const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                     Prefer:'count=none'};
    const cols = 'ad_id,total_price';
    let offset = 0, BATCH = 5000;
    const agg = {};
    while (true){
      const url = SUPABASE_URL + '/rest/v1/shopify_ad_attribution?select=' + cols +
                  '&order_created_at=gte.' + from + 'T00:00:00' +
                  '&order_created_at=lte.' + to   + 'T23:59:59' +
                  '&has_match=eq.true&ad_id=not.is.null' +
                  '&limit=' + BATCH + '&offset=' + offset;
      const r = await fetch(url, {headers});
      if (!r.ok) break;
      const chunk = await r.json();
      if (!Array.isArray(chunk) || !chunk.length) break;
      for (const row of chunk){
        const id = row.ad_id; if (!id) continue;
        let a = agg[id];
        if (!a){ a = agg[id] = {orders:0, sales:0}; }
        a.orders += 1;
        a.sales  += (+row.total_price || 0);
      }
      if (chunk.length < BATCH) break;
      offset += BATCH;
      // Cap to prevent runaway if the window is enormous (500k+ orders).
      if (offset > 500000) break;
    }
    aeWindowShopifyByAdId = agg;
  } catch (e){
    console.warn('[fetchAeWindowShopify] network error', e);
    aeWindowShopifyByAdId = {};
  }
}
// Group-level Shopify rollups. Called on:
//   - date-range change (via fetchAeWindowShopify's caller chain)
//   - level-toggle click (when switching to adset/campaign for the first time)
//   - AE view first-open
// Uses RPCs get_shopify_by_adset / get_shopify_by_campaign so orders resolved
// at Step 3 (adset) / Step 4 (campaign) scopes are captured even when they
// didn't pin to a specific ad_id in the current AE partition.
async function fetchAeShopifyRollups(){
  const from = document.getElementById('aeDateFrom').value || '';
  const to   = document.getElementById('aeDateTo').value   || '';
  const key  = from + '|' + to;
  if (key === _aeShopifyRollupKey) return;
  _aeShopifyRollupKey = key;
  if (!SUPABASE_URL || !SUPABASE_ANON){
    aeShopifyByAdset = {}; aeShopifyByCampaign = {}; return;
  }
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   'Content-Type':'application/json', Prefer:'count=none'};
  const body = JSON.stringify({
    from_date: from || '1970-01-01',
    to_date:   to   || '2100-01-01',
  });
  const nextAdset = {}, nextCamp = {};
  try {
    const [ra, rc] = await Promise.all([
      fetch(SUPABASE_URL + '/rest/v1/rpc/get_shopify_by_adset',    {method:'POST', headers, body}),
      fetch(SUPABASE_URL + '/rest/v1/rpc/get_shopify_by_campaign', {method:'POST', headers, body}),
    ]);
    if (ra.ok){
      const j = await ra.json();
      if (Array.isArray(j)){
        for (const row of j){
          if (!row.adset_id) continue;
          nextAdset[row.adset_id] = {orders: +row.orders || 0, sales: +row.sales || 0};
        }
      }
    } else console.warn('[fetchAeShopifyRollups] adset RPC', ra.status);
    if (rc.ok){
      const j = await rc.json();
      if (Array.isArray(j)){
        for (const row of j){
          if (!row.campaign_name) continue;
          nextCamp[row.campaign_name] = {orders: +row.orders || 0, sales: +row.sales || 0};
        }
      }
    } else console.warn('[fetchAeShopifyRollups] campaign RPC', rc.status);
  } catch(e){
    console.warn('[fetchAeShopifyRollups] network error', e);
  }
  aeShopifyByAdset    = nextAdset;
  aeShopifyByCampaign = nextCamp;
}
// Windowed reach fetch — calls the get_reach_by_window RPC (same one the
// removed Incremental Reach Analysis modal used) so the per-ad numbers here
// are bit-for-bit identical to what that modal showed. Server aggregation
// runs against ae_daily_agg_mat with a lifted statement_timeout and
// returns:
//   reach_first  = reach on the earliest day the ad had reach > 0 in window
//   reach_last   = reach on the latest   day the ad had reach > 0 in window
//   reach_incr   = GREATEST(reach_last − reach_first, 0) — capped, never neg
//   reach_sum    = sum of daily reach across the window (for reach_peak too)
//   spend_sum    = sum of spend across the window
async function fetchAeWindowReach(){
  // If no explicit date range is set, feed the RPC the mode's default
  // window so the Prev/Latest/Incr columns describe meaningful numbers.
  //   Ads Analyse (current mode)  → HISTORIC_CUTOFF (2025-01-01) → today
  //   Historic Ads Analysis        → 2023-01-01 → 2024-12-31
  // Explicit date-picker values always override the default.
  const inpFrom = document.getElementById('aeDateFrom').value || '';
  const inpTo   = document.getElementById('aeDateTo').value   || '';
  const isHist  = historicMode.ae;
  const _todayIso    = new Date().toISOString().slice(0,10);
  const _defaultFrom = isHist ? '2023-01-01' : HISTORIC_CUTOFF;
  const _defaultTo   = isHist
    ? (function(){ const c = new Date(HISTORIC_CUTOFF); c.setDate(c.getDate()-1);
                   return c.toISOString().slice(0,10); })()   // 2024-12-31
    : _todayIso;
  const from = inpFrom || _defaultFrom;
  const to   = inpTo   || _defaultTo;
  const key  = from + '|' + to;
  if (key === _aeWindowReachKey) return;
  _aeWindowReachKey = key;
  if (!SUPABASE_URL || !SUPABASE_ANON){ aeWindowReachByAdId = {}; return; }
  try {
    const r = await fetch(SUPABASE_URL + '/rest/v1/rpc/get_reach_by_window', {
      method: 'POST',
      headers: {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                'Content-Type':'application/json'},
      body: JSON.stringify({from_date: from, to_date: to}),
    });
    if (!r.ok){
      console.warn('[fetchAeWindowReach] RPC HTTP', r.status,
                   await r.text().catch(()=>''));
      aeWindowReachByAdId = {}; return;
    }
    const rows = await r.json();
    const out = {};
    for (const row of rows){
      if (!row.ad_id) continue;
      out[row.ad_id] = {
        reach_first: +row.reach_first || 0,
        reach_last:  +row.reach_last  || 0,
        reach_incr:  +row.reach_incr  || 0,   // already GREATEST(..., 0) by RPC
        reach_sum:   +row.reach_sum   || 0,
        reach_peak:  +row.reach_peak  || 0,
        spend_sum:   +row.spend_sum   || 0,
        days_active: +row.days_active || 0,
      };
    }
    aeWindowReachByAdId = out;
  } catch (e){
    console.warn('[fetchAeWindowReach] network error', e);
    aeWindowReachByAdId = {};
  }
}
// Cache: window key ("from|to") → Set<ad_id> so repeated ranges are instant.
const _aeDeliveryCache = new Map();
async function aeRebuildDeliverySet(){
  const from = document.getElementById('aeDateFrom').value || '';
  const to   = document.getElementById('aeDateTo').value   || '';
  const field = document.getElementById('aeDateField').value;
  // Only meaningful in delivery mode with an active window
  if (field !== '__delivery__' || (!from && !to)){
    aeDeliverySet = null;
    return;
  }
  const key = from + '|' + to;
  if (_aeDeliveryCache.has(key)){
    aeDeliverySet = _aeDeliveryCache.get(key);
    return;
  }
  aeDeliverySet = null;   // clear stale result while we fetch
  if (!SUPABASE_URL || !SUPABASE_ANON) return;
  const dbStatEl = document.getElementById('dbStat');
  if (dbStatEl) dbStatEl.innerHTML = 'Loading delivery ads <span class="spinner"></span>';
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   'Content-Type':'application/json', Prefer:'count=none'};
  // Single RPC call — server-side DISTINCT against the ae_daily_agg_mat mat
  // table. Replaces the 900k-row paginated crawl of primary + backfill that
  // used to blow past anon's 3s statement_timeout for 30+ day windows.
  const ids = new Set();
  try {
    const r = await fetch(SUPABASE_URL + '/rest/v1/rpc/get_delivery_ads', {
      method:'POST', headers,
      body: JSON.stringify({from_date: from || '1970-01-01',
                            to_date:   to   || '2100-01-01'}),
    });
    if (r.ok){
      const j = await r.json();
      if (Array.isArray(j)) for (const row of j){ if (row.ad_id) ids.add(row.ad_id); }
    } else {
      console.warn('[aeRebuildDeliverySet] RPC HTTP', r.status,
                   await r.text().catch(()=>''));
    }
  } catch (e){
    console.warn('[aeRebuildDeliverySet] network error', e);
  }
  _aeDeliveryCache.set(key, ids);
  aeDeliverySet = ids;
  if (dbStatEl) dbStatEl.innerHTML = 'Delivered in window: <span class="mono">'+
    fmtInt(ids.size)+'</span> ads';
}
let ctRules    = [];            // Creative Testing multi-filter — same engine
let aeDailyOpenKey = '';        // cache-key of the currently-open daily-row
const aeDailyCache = {};        // {cacheKey:{state, rows, totals, error}}

/* ─────────────────────────────────────────────────────────────
   DAILY ATTRIBUTION — per-ad daily breakdown from primary_table
   (mirrors the behaviour from ads_analyse_static.html)
   ───────────────────────────────────────────────────────────── */
function aeDailyRange(){
  const from = document.getElementById('aeDateFrom').value || '';
  const to   = document.getElementById('aeDateTo').value   || '';
  if (!from && !to) return {from:'', to:'', selFrom:'', selTo:'', capped:false,
                            label:'all available primary_table dates'};
  let f = from ? new Date(from+'T00:00:00') : null;
  let t = to   ? new Date(to+'T00:00:00')   : null;
  if (f && !t) t = new Date(f);
  if (t && !f) f = new Date(t);
  if (f > t){ const x = f; f = t; t = x; }
  const isoLocal = d => d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+
                        '-'+String(d.getDate()).padStart(2,'0');
  const selFrom = isoLocal(f), selTo = isoLocal(t);
  const days = Math.floor((t - f)/86400000) + 1;
  if (days >= 30){
    const capF = new Date(t); capF.setDate(t.getDate() - 29);
    return {from:isoLocal(capF), to:selTo, selFrom, selTo, capped:true,
            label:'last 30 days of selected range: '+isoLocal(capF)+' to '+selTo+
                  ' (selected '+selFrom+' to '+selTo+')'};
  }
  return {from:selFrom, to:selTo, selFrom, selTo, capped:false,
          label:'selected range: '+selFrom+' to '+selTo};
}
function aeDailyCacheKey(adId){
  const r = aeDailyRange();
  return [adId||'', r.from||'', r.to||'', r.capped?'last30':'selected'].join('|');
}
async function aeFetchDaily(adId){
  if (!SUPABASE_URL || !SUPABASE_ANON) throw new Error('Supabase credentials missing');
  const cols = [
    'date','account_name','campaign_name','adset_id','adset_name','ad_id','ad_name',
    'ad_status','ad_created_date','impressions','reach','amount_spent_inr',
    'purchase_roas','outbound_clicks','inline_link_clicks','purchases',
    'conversion_value','ftewv_count','cost_per_ftewv','ncp_count','cost_per_ncp',
    'preview_link','ad_link'
  ].join(',');
  const range = aeDailyRange();
  const headers = {apikey:SUPABASE_ANON,Authorization:'Bearer '+SUPABASE_ANON,Prefer:'count=none'};
  let offset = 0, BATCH = 1000, out = [];
  while (true){
    let url = SUPABASE_URL+'/rest/v1/primary_table?select='+cols+
              '&ad_id=eq.'+encodeURIComponent(adId)+
              '&order=date.desc&limit='+BATCH+'&offset='+offset;
    if (range.from) url += '&date=gte.'+range.from;
    if (range.to)   url += '&date=lte.'+range.to;
    const r = await fetch(url,{headers});
    if (!r.ok){
      const txt = await r.text().catch(()=> '');
      throw new Error('primary_table HTTP '+r.status+(txt?': '+txt.slice(0,160):''));
    }
    const chunk = await r.json();
    if (!Array.isArray(chunk) || !chunk.length) break;
    out = out.concat(chunk);
    offset += chunk.length;
    if (chunk.length < BATCH) break;
  }
  return out;
}
function _aeNum(v){
  if (v === null || v === undefined || v === '') return 0;
  const n = typeof v === 'number' ? v : Number(String(v).replace(/[,₹Rs%\s]/g,''));
  return isFinite(n) ? n : 0;
}
function _aeMaybe(v){
  if (v === null || v === undefined || v === '') return NaN;
  const n = _aeNum(v); return isFinite(n) ? n : NaN;
}
function _aePush(arr, v){ const n = _aeMaybe(v); if (isFinite(n)) arr.push(n); }
function _aeAvg(a){ return a.length ? a.reduce((s,x)=>s+x,0)/a.length : 0; }
function _aeDiv(a,b){ return b ? a/b : 0; }
function aeBuildDaily(raw){
  const by = new Map();
  raw.forEach(r => {
    const d = (r.date||'').slice(0,10);
    if (!d) return;
    if (!by.has(d)) by.set(d, {date:d, status:'', spend:0, impressions:0, reach:0,
      linkClicks:0, purchases:0, convValue:0, ftewv:0, ncp:0,
      _cf:[], _cn:[], _ro:[]});
    const x = by.get(d);
    if (r.ad_status) x.status = String(r.ad_status);
    x.spend       += _aeNum(r.amount_spent_inr);
    x.impressions += _aeNum(r.impressions);
    x.reach       += _aeNum(r.reach);
    x.linkClicks  += _aeNum(r.inline_link_clicks) || _aeNum(r.outbound_clicks);
    x.purchases   += _aeNum(r.purchases);
    x.convValue   += _aeNum(r.conversion_value);
    x.ftewv       += _aeNum(r.ftewv_count);
    x.ncp         += _aeNum(r.ncp_count);
    _aePush(x._cf, r.cost_per_ftewv);
    _aePush(x._cn, r.cost_per_ncp);
    _aePush(x._ro, r.purchase_roas);
  });
  return Array.from(by.values()).map(r => {
    const freq      = _aeDiv(r.impressions, r.reach);
    const ctr       = _aeDiv(r.linkClicks, r.impressions) * 100;
    const roas      = r.convValue > 0 ? _aeDiv(r.convValue, r.spend) : _aeAvg(r._ro);
    const costFt    = r.ftewv > 0 ? _aeDiv(r.spend, r.ftewv) : _aeAvg(r._cf);
    const costNcp   = r.ncp   > 0 ? _aeDiv(r.spend, r.ncp)   : _aeAvg(r._cn);
    return Object.assign(r, {freq, ctr, roas, costFt, costNcp});
  }).sort((a,b) => b.date.localeCompare(a.date));
}
function aeDailyTotals(rows){
  const t = rows.reduce((a,d) => {
    a.spend       += d.spend;
    a.impressions += d.impressions;
    a.reach       += d.reach;
    a.linkClicks  += d.linkClicks;
    a.purchases   += d.purchases;
    a.convValue   += d.convValue;
    a.ftewv       += d.ftewv;
    a.ncp         += d.ncp;
    return a;
  }, {spend:0, impressions:0, reach:0, linkClicks:0, purchases:0,
      convValue:0, ftewv:0, ncp:0});
  t.freq    = _aeDiv(t.impressions, t.reach);
  t.ctr     = _aeDiv(t.linkClicks, t.impressions) * 100;
  t.roas    = t.convValue > 0 ? _aeDiv(t.convValue, t.spend) : 0;
  t.costFt  = t.ftewv > 0 ? _aeDiv(t.spend, t.ftewv) : 0;
  t.costNcp = t.ncp   > 0 ? _aeDiv(t.spend, t.ncp)   : 0;
  return t;
}
async function aeToggleDaily(adId, cacheKey){
  if (!adId) return;
  if (aeDailyOpenKey === cacheKey){ aeDailyOpenKey = ''; renderAE(); return; }
  aeDailyOpenKey = cacheKey;
  if (!aeDailyCache[cacheKey]){
    aeDailyCache[cacheKey] = {state:'loading', rows:[], totals:null, error:''};
    renderAE();
    try{
      const raw  = await aeFetchDaily(adId);
      const rows = aeBuildDaily(raw);
      aeDailyCache[cacheKey] = {state:'ready', rows, totals:aeDailyTotals(rows), error:''};
    }catch(e){
      aeDailyCache[cacheKey] = {state:'error', rows:[], totals:null,
                                error:(e && e.message) ? e.message : String(e)};
    }
  }
  renderAE();
}
function aeDailyTotalsFor(adId){
  const ck = aeDailyCacheKey(adId);
  const s  = aeDailyCache[ck];
  if (!s || s.state !== 'ready' || !s.totals) return null;
  return s.totals;
}
function aeRenderDailyRowHTML(r, colspan){
  const ck = aeDailyCacheKey(r.ad_id);
  const s  = aeDailyCache[ck] || {state:'loading', rows:[], error:''};
  const range = aeDailyRange();
  let inner = '';
  if (s.state === 'loading'){
    inner = '<div class="ae-daily-note">Loading daily attribution from primary_table…</div>';
  } else if (s.state === 'error'){
    inner = '<div class="ae-daily-note" style="color:var(--error-text)">Failed to load: '+
            (s.error||'unknown error')+'</div>';
  } else if (!s.rows.length){
    inner = '<div class="ae-daily-note">No primary_table daily rows found for this ad and date range.</div>';
  } else {
    const t = s.totals;
    const cell = v => '<td class="num">'+v+'</td>';
    inner =
      '<div class="ae-daily-table-wrap"><table class="ae-daily-table">'+
        '<thead><tr>'+
          '<th>Date</th><th class="num">Spend</th><th class="num">Impr.</th>'+
          '<th class="num">Reach</th><th class="num">Freq.</th>'+
          '<th class="num">Link Clicks</th><th class="num">CTR</th>'+
          '<th class="num">Purchases</th><th class="num">Conv Value</th>'+
          '<th class="num">ROAS</th><th class="num">FTEWV</th>'+
          '<th class="num">Cost/FTEWV</th><th class="num">NCP</th>'+
          '<th class="num">Cost/NCP</th><th>Status</th>'+
        '</tr></thead>'+
        '<tbody>'+ s.rows.map(d => {
          const st = (d.status||'').toUpperCase() === 'ACTIVE' ? 'active' : '';
          return '<tr>'+
            '<td>'+d.date+'</td>'+
            cell(fmtRs(d.spend))+ cell(fmtInt(d.impressions))+ cell(fmtInt(d.reach))+
            cell((d.freq||0).toFixed(2))+ cell(fmtInt(d.linkClicks))+
            cell((d.ctr||0).toFixed(2)+'%')+ cell((d.purchases||0).toFixed(2))+
            cell(fmtRs(d.convValue))+ cell((d.roas||0).toFixed(2))+
            cell(fmtInt(d.ftewv))+ cell(fmtRs(d.costFt))+
            cell(fmtInt(d.ncp))+   cell(fmtRs(d.costNcp))+
            '<td><span class="ae-status '+st+'">'+(d.status||'—')+'</span></td>'+
          '</tr>';
        }).join('') +'</tbody>'+
        '<tfoot><tr>'+
          '<td>Totals</td>'+
          cell(fmtRs(t.spend))+ cell(fmtInt(t.impressions))+ cell(fmtInt(t.reach))+
          cell((t.freq||0).toFixed(2))+ cell(fmtInt(t.linkClicks))+
          cell((t.ctr||0).toFixed(2)+'%')+ cell((t.purchases||0).toFixed(2))+
          cell(fmtRs(t.convValue))+ cell((t.roas||0).toFixed(2))+
          cell(fmtInt(t.ftewv))+ cell(fmtRs(t.costFt))+
          cell(fmtInt(t.ncp))+   cell(fmtRs(t.costNcp))+
          '<td></td>'+
        '</tr></tfoot></table></div>';
  }
  return '<tr class="ae-daily-row"><td colspan="'+colspan+'">'+
    '<div class="ae-daily-panel">'+
      '<div class="ae-daily-head">'+
        '<div>'+
          '<div class="ae-daily-title">Daily Attribution Breakdown</div>'+
          '<div class="ae-daily-sub">'+(r.ad_name||'—')+' · ad_id '+(r.ad_id||'—')+
            ' · '+range.label+'</div>'+
        '</div>'+
        '<button class="btn ghost" type="button" data-ae-daily-close="1" style="padding:5px 12px;font-size:11px">Close</button>'+
      '</div>'+ inner +
    '</div></td></tr>';
}

/* ─────────────────────────────────────────────────────────────
   MULTI-FILTER — chainable rules (Field × Operator × Value)
   ───────────────────────────────────────────────────────────── */
// Text fields (name-like) — use string operators
// Numeric fields (metrics) — use threshold operators.  Marked with numeric:true
// so the UI auto-swaps the operator dropdown when the user picks one.
const AE_MF_FIELDS = [
  // ── Text ─────────────────────────────────────────────────────────
  {key:'name_combined', label:'Name (Ad/Camp)'},
  {key:'ad_name',       label:'Ad Name'},
  {key:'campaign_name', label:'Campaign'},
  {key:'adset_id',      label:'Adset ID'},
  {key:'ad_id',         label:'Ad ID'},
  {key:'category',      label:'Category'},
  {key:'ad_status',     label:'Status'},
  {key:'account_name',  label:'Account'},
  // ── Numeric metrics — pick metric + set threshold ───────────────
  {key:'amount_spent',       label:'Spend',            numeric:true, ph:'e.g. 1000'},
  {key:'roas_ma',            label:'ROAS',             numeric:true, ph:'e.g. 3'},
  {key:'shopify_roas',       label:'Shopify ROAS',     numeric:true, ph:'e.g. 2'},
  {key:'impressions',        label:'Impressions',      numeric:true, ph:'e.g. 50000'},
  {key:'reach',              label:'Reach',            numeric:true, ph:'e.g. 10000'},
  {key:'frequency',          label:'Frequency',        numeric:true, ph:'e.g. 2.5'},
  {key:'purchases',          label:'Meta Purchases',   numeric:true, ph:'e.g. 10'},
  {key:'conv_value',         label:'Meta Conv Value',  numeric:true, ph:'e.g. 100000'},
  {key:'shopify_orders',     label:'Shopify Orders',   numeric:true, ph:'e.g. 5'},
  {key:'shopify_sales',      label:'Shopify Sales',    numeric:true, ph:'e.g. 50000'},
  {key:'ctr_pct',            label:'CTR %',            numeric:true, ph:'e.g. 1.5'},
  {key:'atc_lc_pct',         label:'ATC / LC %',       numeric:true, ph:'e.g. 5'},
  {key:'ci_atc_pct',         label:'CI / ATC %',       numeric:true, ph:'e.g. 10'},
  {key:'checkout_compl_pct', label:'Checkout %',       numeric:true, ph:'e.g. 20'},
  {key:'cost_per_1000',      label:'Cost / 1k',        numeric:true, ph:'e.g. 500'},
  {key:'cpc_link',           label:'CPC Link',         numeric:true, ph:'e.g. 5'},
  {key:'cost_per_ncp',       label:'Cost / NCP',       numeric:true, ph:'e.g. 525'},
  {key:'cost_per_ftewv',     label:'Cost / FTEWV',     numeric:true, ph:'e.g. 12'},
  {key:'ftewv_count',        label:'FTEWV',            numeric:true, ph:'e.g. 100'},
  {key:'ncp_count',          label:'NCP',              numeric:true, ph:'e.g. 50'},
  {key:'atc_count',          label:'ATC',              numeric:true, ph:'e.g. 20'},
  {key:'ci_count',           label:'CI',               numeric:true, ph:'e.g. 10'},
  {key:'link_clicks_raw',    label:'Link Clicks',      numeric:true, ph:'e.g. 500'},
];
const AE_MF_OPS_TEXT = [
  {key:'contains_all',  label:'contains all of',  ph:'Keywords separated by space'},
  {key:'contains_any',  label:'contains any of',  ph:'Any of these keywords'},
  {key:'equals',        label:'equals',           ph:'Exact match (case-insensitive)'},
  {key:'not_contains',  label:'does not contain', ph:'Exclude these keywords'},
];
const AE_MF_OPS_NUMERIC = [
  {key:'gte',      label:'≥',        ph:'e.g. 1000'},
  {key:'gt',       label:'>',        ph:'e.g. 1000'},
  {key:'lte',      label:'≤',        ph:'e.g. 500'},
  {key:'lt',       label:'<',        ph:'e.g. 500'},
  {key:'eq',       label:'=',        ph:'e.g. 3'},
  {key:'neq',      label:'≠',        ph:'e.g. 0'},
  {key:'between',  label:'between',  ph:'min-max, e.g. 1000-5000'},
];
// Combined list (kept for back-compat with any code that reads AE_MF_OPS)
const AE_MF_OPS = AE_MF_OPS_TEXT.concat(AE_MF_OPS_NUMERIC);

function _aeMfFieldMeta(key){ return AE_MF_FIELDS.find(f => f.key === key) || {}; }
function _aeMfIsNumericField(key){ return !!_aeMfFieldMeta(key).numeric; }

function aeMfFieldValue(row, key){
  if (key === 'name_combined') return ((row.ad_name||'') + ' ' + (row.campaign_name||'')).trim();
  return row[key];   // preserve type (number stays number for numeric ops)
}
function aeMfMatch(row, rule){
  const raw = String(rule.value ?? '').trim();
  if (!raw) return true;                             // empty value = ignore rule
  // Numeric field → threshold comparison
  if (_aeMfIsNumericField(rule.field)){
    const rowVal = aeMfFieldValue(row, rule.field);
    const rowNum = typeof rowVal === 'number' ? rowVal : parseFloat(rowVal);
    if (isNaN(rowNum)) return false;                 // no value on this row → excluded
    if (rule.op === 'between'){
      const m = raw.match(/^\s*([-+]?[\d.]+)\s*[-–]\s*([-+]?[\d.]+)\s*$/);
      if (!m) return true;
      const lo = parseFloat(m[1]); const hi = parseFloat(m[2]);
      if (isNaN(lo) || isNaN(hi)) return true;
      return rowNum >= Math.min(lo,hi) && rowNum <= Math.max(lo,hi);
    }
    const n = parseFloat(raw); if (isNaN(n)) return true;
    if (rule.op === 'gte' || rule.op === 'contains_all') return rowNum >= n;  // default when op was text
    if (rule.op === 'gt')  return rowNum >  n;
    if (rule.op === 'lte') return rowNum <= n;
    if (rule.op === 'lt')  return rowNum <  n;
    if (rule.op === 'eq')  return Math.abs(rowNum - n) < 1e-9;
    if (rule.op === 'neq') return Math.abs(rowNum - n) >= 1e-9;
    return true;
  }
  // Text field → substring comparisons (as before)
  const v = String(aeMfFieldValue(row, rule.field) || '').toLowerCase();
  const toks = raw.toLowerCase().split(/\s+/).filter(Boolean);
  if (rule.op === 'contains_all')  return toks.every(t => v.includes(t));
  if (rule.op === 'contains_any')  return toks.some (t => v.includes(t));
  if (rule.op === 'equals')        return v === toks.join(' ');
  if (rule.op === 'not_contains')  return !toks.some(t => v.includes(t));
  return true;
}
function _aeMfOpsFor(fieldKey){
  return _aeMfIsNumericField(fieldKey) ? AE_MF_OPS_NUMERIC : AE_MF_OPS_TEXT;
}
function _aeMfDefaultOp(fieldKey){
  return _aeMfIsNumericField(fieldKey) ? 'gte' : 'contains_all';
}
function _aeMfPlaceholder(fieldKey, opKey){
  const ops = _aeMfOpsFor(fieldKey);
  const op  = ops.find(o => o.key === opKey) || ops[0];
  // For numeric fields, prefer the field-specific placeholder (e.g. "e.g. 1000")
  // unless the op has its own hint (between → "min-max, e.g. 1000-5000")
  if (_aeMfIsNumericField(fieldKey) && op.key !== 'between'){
    return _aeMfFieldMeta(fieldKey).ph || op.ph;
  }
  return op.ph;
}
/* Generic renderer used by both the inline text Multi-Filter and the
   drawer's numeric Filters — differs only in state array, host element,
   and which subset of AE_MF_FIELDS to expose in the field dropdown.
   `mode` is 'text' or 'numeric'. */
function _aeMfRenderPanel(mode){
  const isNum   = mode === 'numeric';
  const state   = isNum ? aeRulesNum : aeRules;
  const hostId  = isNum ? 'aeMfNumRows' : 'aeMfRows';
  const host    = document.getElementById(hostId);
  if (!host) return;
  const defField = isNum ? 'amount_spent' : 'name_combined';
  const defRule  = () => ({field: defField, op: _aeMfDefaultOp(defField), value: ''});
  if (!state.length) state.push(defRule());
  const fieldsForMode = AE_MF_FIELDS.filter(f => !!f.numeric === isNum);
  host.innerHTML = state.map((r, i) => {
    // Coerce field into the correct pool (drop stale field of the wrong type).
    if (!fieldsForMode.some(f => f.key === r.field)) r.field = defField;
    const validOps = _aeMfOpsFor(r.field);
    if (!validOps.some(o => o.key === r.op)) r.op = _aeMfDefaultOp(r.field);
    const fieldOpts = fieldsForMode.map(f =>
      `<option value="${f.key}"${f.key===r.field?' selected':''}>${f.label}</option>`).join('');
    const opOpts = validOps.map(o =>
      `<option value="${o.key}"${o.key===r.op?' selected':''}>${o.label}</option>`).join('');
    const val = String(r.value ?? '').replace(/"/g, '&quot;');
    const ph  = _aeMfPlaceholder(r.field, r.op);
    return `<div class="ae-mfilter-row" data-i="${i}">
      <select class="ae-finput rule-field">${fieldOpts}</select>
      <select class="ae-finput rule-op">${opOpts}</select>
      <input  class="ae-finput rule-value" type="text" placeholder="${ph}" value="${val}">
      <button class="ae-rule-del" type="button" title="Remove this rule">&times;</button>
    </div>`;
  }).join('');
  host.querySelectorAll('.ae-mfilter-row').forEach(div => {
    const i = +div.dataset.i;
    div.querySelector('.rule-field').addEventListener('change', e => {
      state[i].field = e.target.value;
      state[i].op    = _aeMfDefaultOp(e.target.value);
      state[i].value = '';
      _aeMfRenderPanel(mode);
    });
    div.querySelector('.rule-op').addEventListener('change', e => {
      state[i].op = e.target.value;
      div.querySelector('.rule-value').placeholder = _aeMfPlaceholder(state[i].field, e.target.value);
    });
    // LIVE-SEARCH — typing debounces to a re-render (no Apply click needed).
    // Enter forces an immediate re-render. This eliminates the class of bugs
    // where a user typed a second search but forgot / stopped clicking Apply.
    const _valInp = div.querySelector('.rule-value');
    _valInp.addEventListener('input', e => {
      state[i].value = e.target.value;
      clearTimeout(_valInp._aeMfDb);
      _valInp._aeMfDb = setTimeout(() => { aePage = 0; renderAE(); }, 250);
    });
    _valInp.addEventListener('keydown', e => {
      if (e.key === 'Enter'){
        clearTimeout(_valInp._aeMfDb);
        state[i].value = e.target.value;
        aePage = 0; renderAE();
      }
    });
    div.querySelector('.ae-rule-del').addEventListener('click', () => {
      state.splice(i, 1);
      if (!state.length) state.push(defRule());
      _aeMfRenderPanel(mode); aePage = 0; renderAE();
    });
  });
}
function aeMfRender()    { _aeMfRenderPanel('text');    }
function aeMfRenderNum() { _aeMfRenderPanel('numeric'); }

/* ─────────────────────────────────────────────────────────────
   Creative Testing — Multi-Filter (same engine as AE) + F1-F4 thresholds
   ───────────────────────────────────────────────────────────── */
function ctMfRender(){
  const host = document.getElementById('ctMfRows');
  if (!host) return;
  if (!ctRules.length) ctRules.push({field:'name_combined', op:'contains_all', value:''});
  host.innerHTML = ctRules.map((r, i) => {
    const opPh = (AE_MF_OPS.find(o => o.key === r.op) || AE_MF_OPS[0]).ph;
    const fieldOpts = AE_MF_FIELDS.map(f =>
      `<option value="${f.key}"${f.key===r.field?' selected':''}>${f.label}</option>`).join('');
    const opOpts = AE_MF_OPS.map(o =>
      `<option value="${o.key}"${o.key===r.op?' selected':''}>${o.label}</option>`).join('');
    const val = (r.value || '').replace(/"/g, '&quot;');
    return `<div class="ae-mfilter-row" data-i="${i}">
      <select class="fg-select rule-field">${fieldOpts}</select>
      <select class="fg-select rule-op">${opOpts}</select>
      <input  class="fg-input  rule-value" type="text" placeholder="${opPh}" value="${val}">
      <button class="ae-rule-del" type="button" title="Remove this rule">&times;</button>
    </div>`;
  }).join('');
  host.querySelectorAll('.ae-mfilter-row').forEach(div => {
    const i = +div.dataset.i;
    div.querySelector('.rule-field').addEventListener('change', e => { ctRules[i].field = e.target.value; });
    div.querySelector('.rule-op'   ).addEventListener('change', e => {
      ctRules[i].op = e.target.value;
      const ph = (AE_MF_OPS.find(o => o.key === e.target.value) || AE_MF_OPS[0]).ph;
      div.querySelector('.rule-value').placeholder = ph;
    });
    div.querySelector('.rule-value').addEventListener('input',  e => { ctRules[i].value = e.target.value; });
    div.querySelector('.ae-rule-del').addEventListener('click', () => {
      ctRules.splice(i, 1);
      if (!ctRules.length) ctRules.push({field:'name_combined', op:'contains_all', value:''});
      ctMfRender(); rerender();
    });
  });
}
function ctMfMatchAll(row){
  for (const rule of ctRules){
    if (!aeMfMatch(row, rule)) return false;
  }
  return true;
}
/* Push CT thresholds into the AE inputs too, so categorisation stays in
   sync across the two views. */
function ctSyncThresholdsToAE(){
  const ids = [['ctF1','aeF1'],['ctF2','aeF2'],['ctF3','aeF3'],['ctF4','aeF4']];
  for (const [from, to] of ids){
    const a = document.getElementById(from), b = document.getElementById(to);
    if (a && b && b.value !== a.value) b.value = a.value;
  }
}
function ctApplyThresholdInputs(){
  ctSyncThresholdsToAE();
  ctApplyCurrentThresholds();   // re-bucket primaryAds
  aeApplyCurrentThresholds();   // re-bucket allAds (ae_table_view)
  rerender();
  if (typeof renderAE === 'function') renderAE();
}

/* ────────────────────────────────────────────────────────────
   Date Range Picker (DRP) — full popup, ported from
   ads_analyse_static.html. Writes to #aeDateFrom / #aeDateTo
   (the existing hidden inputs the AE filter logic reads).
   ──────────────────────────────────────────────────────────── */
const DRP_MONTHS = ['January','February','March','April','May','June',
                    'July','August','September','October','November','December'];
const DRP_DAYS   = ['Su','Mo','Tu','We','Th','Fr','Sa'];
const drpState = {
  open:false,
  viewYear: new Date().getFullYear(),
  viewMonth: new Date().getMonth() - 1,
  from:null, to:null, hov:null,
  preset:'lifetime',
  selecting:false,
};
if (drpState.viewMonth < 0){ drpState.viewMonth = 11; drpState.viewYear -= 1; }
const drpEl = (id) => document.getElementById(id);

function drpFmtLocal(d){
  return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
}
function drpParseLocal(s){
  const p = String(s||'').split('-').map(Number);
  return new Date(p[0], p[1]-1, p[2], 0, 0, 0, 0);
}
function drpDisplay(d){
  if (!d) return '--/--/----';
  return String(d.getDate()).padStart(2,'0') + '/' + String(d.getMonth()+1).padStart(2,'0') + '/' + d.getFullYear();
}
function drpSetPresetActive(key){
  document.querySelectorAll('#ae-drp-presets .drp-preset').forEach(n => {
    n.classList.toggle('active', n.dataset.key === key);
  });
}
function drpPositionPanel(){
  const btn = drpEl('ae-drp-btn'), panel = drpEl('ae-drp-panel');
  if (!btn || !panel) return;
  const r = btn.getBoundingClientRect();
  const panelW = panel.offsetWidth || 680;
  const panelH = panel.offsetHeight || 380;
  let top = r.bottom + 6;
  let left = r.right - panelW;
  if (top + panelH > window.innerHeight - 8) top = Math.max(8, r.top - panelH - 6);
  if (left < 8) left = 8;
  if (left + panelW > window.innerWidth - 8) left = window.innerWidth - panelW - 8;
  panel.style.top  = top  + 'px';
  panel.style.left = left + 'px';
}
function drpOpen(){
  drpState.open = true;
  drpEl('ae-drp-panel').classList.add('open');
  drpEl('ae-drp-panel').setAttribute('aria-hidden','false');
  drpEl('ae-drp-btn').classList.add('open');
  drpRender();
  drpPositionPanel();
  setTimeout(drpPositionPanel, 0);
}
function drpClose(){
  drpState.open = false;
  drpEl('ae-drp-panel').classList.remove('open');
  drpEl('ae-drp-panel').setAttribute('aria-hidden','true');
  drpEl('ae-drp-btn').classList.remove('open');
}
function drpToggle(){ drpState.open ? drpClose() : drpOpen(); }
function drpNavMonth(dir){
  drpState.viewMonth += dir;
  if (drpState.viewMonth > 11){ drpState.viewMonth = 0;  drpState.viewYear += 1; }
  if (drpState.viewMonth <  0){ drpState.viewMonth = 11; drpState.viewYear -= 1; }
  drpRender();
  drpPositionPanel();
}
function drpCalHTML(year, month){
  const today = new Date(); today.setHours(0,0,0,0);
  const firstDay = new Date(year, month, 1).getDay();
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const daysInPrevMonth = new Date(year, month, 0).getDate();
  let rFrom = drpState.from;
  let rTo   = drpState.selecting && drpState.hov ? drpState.hov : drpState.to;
  if (rFrom && rTo && rFrom > rTo){ const t = rFrom; rFrom = rTo; rTo = t; }
  let html = '<div class="drp-grid">';
  DRP_DAYS.forEach(d => { html += '<div class="drp-dow">' + d + '</div>'; });
  for (let i = 0; i < firstDay; i++){
    html += '<div class="drp-day other-month">' + (daysInPrevMonth - firstDay + 1 + i) + '</div>';
  }
  for (let d = 1; d <= daysInMonth; d++){
    const dt = new Date(year, month, d); dt.setHours(0,0,0,0);
    const iso = drpFmtLocal(dt);
    const isFuture = dt > today;
    let cls = 'drp-day';
    if (dt.getTime() === today.getTime())            cls += ' today';
    if (rFrom && dt.getTime() === rFrom.getTime())   cls += ' range-start';
    if (rTo   && dt.getTime() === rTo.getTime())     cls += ' range-end';
    if (rFrom && rTo && dt > rFrom && dt < rTo)      cls += ' in-range';
    if (isFuture)                                    cls += ' disabled';
    html += '<div class="' + cls + '" data-date="' + iso + '">' + d + '</div>';
  }
  const total = firstDay + daysInMonth;
  const remainder = total % 7 === 0 ? 0 : 7 - (total % 7);
  for (let d = 1; d <= remainder; d++) html += '<div class="drp-day other-month">' + d + '</div>';
  return html + '</div>';
}
function drpRender(){
  let rMonth = drpState.viewMonth + 1;
  let rYear  = drpState.viewYear;
  if (rMonth > 11){ rMonth = 0; rYear += 1; }
  drpEl('ae-drp-month-l').textContent = DRP_MONTHS[drpState.viewMonth] + ' ' + drpState.viewYear;
  drpEl('ae-drp-month-r').textContent = DRP_MONTHS[rMonth]            + ' ' + rYear;
  drpEl('ae-drp-cal-l').innerHTML = drpCalHTML(drpState.viewYear, drpState.viewMonth);
  drpEl('ae-drp-cal-r').innerHTML = drpCalHTML(rYear, rMonth);
  const end = drpState.selecting && drpState.hov ? drpState.hov : drpState.to;
  let a = drpState.from, b = end;
  if (a && b && a > b){ const t = a; a = b; b = t; }
  // If mid-selection (start picked, end not yet), make the footer shout
  // so users don't hit Apply and get a single-day range they didn't want.
  const footerEl = drpEl('ae-drp-footer-range');
  const applyBtn = drpEl('ae-drp-apply');
  if (drpState.selecting && drpState.from && !drpState.to){
    footerEl.textContent = drpDisplay(a) + ' → click END date';
    footerEl.style.color = '#B33A2A';
    footerEl.style.fontWeight = '700';
    if (applyBtn){
      applyBtn.disabled = true;
      applyBtn.style.opacity = '0.4';
      applyBtn.style.cursor = 'not-allowed';
      applyBtn.title = 'Pick an END date first';
    }
  } else {
    footerEl.textContent = drpDisplay(a) + ' - ' + drpDisplay(b);
    footerEl.style.color = ''; footerEl.style.fontWeight = '';
    if (applyBtn){
      applyBtn.disabled = false;
      applyBtn.style.opacity = ''; applyBtn.style.cursor = '';
      applyBtn.title = '';
    }
  }
  drpSetPresetActive(drpState.preset);
}
function drpDayClick(node){
  if (!node || node.classList.contains('other-month') || node.classList.contains('disabled')) return;
  const dt = drpParseLocal(node.dataset.date);
  // If the previous selection is fully complete (both from + to filled
  // and we're not mid-range) or if we have neither, start a fresh
  // range with this click as the new "from".
  const hasCompleted = drpState.from && drpState.to && !drpState.selecting;
  if (!drpState.selecting || hasCompleted){
    drpState.from = dt; drpState.to = null; drpState.hov = null;
    drpState.selecting = true; drpState.preset = 'custom';
  } else {
    drpState.to = dt; drpState.hov = null; drpState.selecting = false;
    if (drpState.from > drpState.to){ const t = drpState.from; drpState.from = drpState.to; drpState.to = t; }
  }
  drpRender();
}
/* Update range-highlight classes IN PLACE on the existing cells instead
 * of calling drpRender (which wipes innerHTML). The reason: on the very
 * next mouseup, click bubbles up to the calendar container's listener —
 * but if innerHTML was just replaced, the day cell is detached and the
 * click never reaches the container. So hover-driven re-render was
 * eating the second click of a range. */
function drpUpdateRangeHighlight(){
  let rFrom = drpState.from;
  let rTo   = drpState.selecting && drpState.hov ? drpState.hov : drpState.to;
  if (rFrom && rTo && rFrom > rTo){ const t = rFrom; rFrom = rTo; rTo = t; }
  const from = rFrom ? rFrom.getTime() : null;
  const to   = rTo   ? rTo.getTime()   : null;
  ['ae-drp-cal-l','ae-drp-cal-r'].forEach(id => {
    const cal = drpEl(id); if (!cal) return;
    cal.querySelectorAll('.drp-day').forEach(cell => {
      cell.classList.remove('range-start','range-end','in-range');
      if (cell.classList.contains('other-month')) return;
      const iso = cell.dataset.date; if (!iso) return;
      const t = drpParseLocal(iso).getTime();
      if (from !== null && t === from) cell.classList.add('range-start');
      if (to   !== null && t === to)   cell.classList.add('range-end');
      if (from !== null && to !== null && t > from && t < to) cell.classList.add('in-range');
    });
  });
  // Also refresh the footer text since the "end" hint changes with hover.
  const footerEl = drpEl('ae-drp-footer-range');
  if (footerEl){
    if (drpState.selecting && drpState.from && !drpState.to){
      footerEl.textContent = drpDisplay(rFrom) + ' → click END date';
    } else {
      footerEl.textContent = drpDisplay(rFrom) + ' - ' + drpDisplay(rTo);
    }
  }
}
function drpDayHover(node){
  if (!drpState.selecting || !node || node.classList.contains('other-month') || node.classList.contains('disabled')) return;
  drpState.hov = drpParseLocal(node.dataset.date);
  // Class-only update — keeps existing DOM nodes attached so the ensuing
  // click event bubbles to the calendar listener.
  drpUpdateRangeHighlight();
}
function drpPreset(key){
  const now = new Date(); now.setHours(0,0,0,0);
  let from = null, to = null;
  if      (key === 'today')      { from = new Date(now); to = new Date(now); }
  else if (key === 'yesterday')  { from = new Date(now); from.setDate(now.getDate()-1); to = new Date(from); }
  else if (key === 'last7')      { from = new Date(now); from.setDate(now.getDate()-6);  to = new Date(now); }
  else if (key === 'last30')     { from = new Date(now); from.setDate(now.getDate()-29); to = new Date(now); }
  else if (key === 'last90')     { from = new Date(now); from.setDate(now.getDate()-89); to = new Date(now); }
  else if (key === 'thisMonth')  { from = new Date(now.getFullYear(), now.getMonth(), 1); to = new Date(now); }
  else if (key === 'lastMonth')  { from = new Date(now.getFullYear(), now.getMonth()-1, 1);
                                   to   = new Date(now.getFullYear(), now.getMonth(),    0); }
  else if (key === 'lifetime')   { from = null; to = null; }
  else if (key === 'custom')     {
    drpState.from = null; drpState.to = null; drpState.hov = null;
    drpState.selecting = false; drpState.preset = 'custom';
    drpRender(); return;
  }
  drpState.from = from; drpState.to = to; drpState.hov = null;
  drpState.selecting = false; drpState.preset = key;
  if (from){ drpState.viewYear = from.getFullYear(); drpState.viewMonth = from.getMonth(); }
  else { const n = new Date(); drpState.viewYear = n.getFullYear(); drpState.viewMonth = n.getMonth() - 1;
         if (drpState.viewMonth < 0){ drpState.viewMonth = 11; drpState.viewYear -= 1; } }
  drpRender();
  drpPositionPanel();
  // Auto-apply for real presets so users don't have to click Apply
  // separately — the picker closes and the table refreshes immediately.
  // Custom mode is skipped (returns early above) so the calendar
  // stays open for the user to pick a range.
  drpApply();
}
function drpUpdateButton(){
  const lbl = drpEl('ae-drp-label');
  const map = {today:'Today', yesterday:'Yesterday', last7:'Last 7 Days', last30:'Last 30 Days',
               thisMonth:'This Month', lastMonth:'Last Month', last90:'Last 90 Days', lifetime:'All time'};
  if (drpState.preset === 'lifetime' || (!drpState.from && !drpState.to)){
    lbl.textContent = 'All time'; lbl.classList.remove('active');
  } else if (drpState.preset !== 'custom'){
    lbl.textContent = map[drpState.preset] || drpState.preset; lbl.classList.add('active');
  } else {
    const a = drpState.from, b = drpState.to || drpState.from;
    lbl.textContent = drpDisplay(a) + (a && b && a.getTime() !== b.getTime() ? ' - ' + drpDisplay(b) : '');
    lbl.classList.add('active');
  }
}
async function drpApply(){
  const fromInp = drpEl('aeDateFrom'), toInp = drpEl('aeDateTo');
  if (drpState.preset === 'lifetime' || (!drpState.from && !drpState.to)){
    fromInp.value = ''; toInp.value = '';
    drpState.from = null; drpState.to = null;
  } else {
    let from = drpState.from, to = drpState.to || drpState.from;
    if (from > to){ const t = from; from = to; to = t; }
    fromInp.value = drpFmtLocal(from); toInp.value = drpFmtLocal(to);
    // Normalise the state so the next picker-open shows the current
    // range as a completed selection (not mid-way through a range).
    drpState.from = from; drpState.to = to;
  }
  // Critical: reset the "picking a range" mode. Without this, single-day
  // Apply leaves drpState.selecting = true and the NEXT day-click gets
  // interpreted as the end of the old range instead of a fresh start.
  drpState.selecting = false; drpState.hov = null;
  drpUpdateButton();
  drpClose();
  // Fetch both in parallel — delivery set gates which ads to show,
  // window metrics gates the values shown in each row's columns.
  await Promise.all([aeRebuildDeliverySet(), fetchAeWindowMetrics(),
                     fetchAeWindowShopify(), fetchAeWindowReach(),
                     fetchAeShopifyRollups()]);
  aePage = 0; renderAE();
}
async function drpClearDateRange(){
  drpEl('aeDateFrom').value = '';
  drpEl('aeDateTo').value   = '';
  drpState.from = null; drpState.to = null; drpState.hov = null;
  drpState.selecting = false; drpState.preset = 'lifetime';
  drpUpdateButton();
  drpRender();
  await Promise.all([aeRebuildDeliverySet(), fetchAeWindowMetrics(),
                     fetchAeWindowShopify(), fetchAeWindowReach(),
                     fetchAeShopifyRollups()]);
  aePage = 0; renderAE();
}
function initDateRangePicker(){
  drpEl('ae-drp-btn'   ).addEventListener('click',  e => { e.stopPropagation(); drpToggle(); });
  drpEl('ae-drp-prev'  ).addEventListener('click',  e => { e.stopPropagation(); drpNavMonth(-1); });
  drpEl('ae-drp-next'  ).addEventListener('click',  e => { e.stopPropagation(); drpNavMonth( 1); });
  drpEl('ae-drp-cancel').addEventListener('click',  e => { e.stopPropagation(); drpClose(); });
  drpEl('ae-drp-apply' ).addEventListener('click',  e => { e.stopPropagation(); drpApply(); });
  document.querySelectorAll('#ae-drp-presets .drp-preset').forEach(node => {
    node.addEventListener('click', e => { e.stopPropagation(); drpPreset(node.dataset.key); });
  });
  ['ae-drp-cal-l','ae-drp-cal-r'].forEach(id => {
    drpEl(id).addEventListener('click',     e => { e.stopPropagation(); drpDayClick(e.target.closest('.drp-day')); });
    drpEl(id).addEventListener('mouseover', e => { drpDayHover(e.target.closest('.drp-day')); });
  });
  document.addEventListener('click', e => {
    const wrap = drpEl('ae-drp-wrap');
    if (drpState.open && wrap && !wrap.contains(e.target)) drpClose();
  });
  window.addEventListener('resize', () => { if (drpState.open) drpPositionPanel(); });
  drpRender();
  drpUpdateButton();
}

/* ────────────────────────────────────────────────────────────
   Lifecycle Date Range Picker — clone of the AE DRP wired to
   the #life-drp-* DOM, #lifeDateFrom / #lifeDateTo hidden inputs,
   and the renderLifecycle() callback.
   ──────────────────────────────────────────────────────────── */
const lifeDrpState = {
  open:false,
  viewYear: new Date().getFullYear(),
  viewMonth: new Date().getMonth() - 1,
  from:null, to:null, hov:null,
  preset:'lifetime',
  selecting:false,
};
if (lifeDrpState.viewMonth < 0){ lifeDrpState.viewMonth = 11; lifeDrpState.viewYear -= 1; }
const lifeDrpEl = (id) => document.getElementById(id);
function lifeDrpSetPresetActive(key){
  document.querySelectorAll('#life-drp-presets .drp-preset').forEach(n => {
    n.classList.toggle('active', n.dataset.key === key);
  });
}
function lifeDrpPositionPanel(){
  const btn = lifeDrpEl('life-drp-btn'), panel = lifeDrpEl('life-drp-panel');
  if (!btn || !panel) return;
  const r = btn.getBoundingClientRect();
  const panelW = panel.offsetWidth || 680;
  const panelH = panel.offsetHeight || 380;
  let top = r.bottom + 6;
  let left = r.right - panelW;
  if (top + panelH > window.innerHeight - 8) top = Math.max(8, r.top - panelH - 6);
  if (left < 8) left = 8;
  if (left + panelW > window.innerWidth - 8) left = window.innerWidth - panelW - 8;
  panel.style.top  = top  + 'px';
  panel.style.left = left + 'px';
}
function lifeDrpOpen(){
  lifeDrpState.open = true;
  lifeDrpEl('life-drp-panel').classList.add('open');
  lifeDrpEl('life-drp-panel').setAttribute('aria-hidden','false');
  lifeDrpEl('life-drp-btn').classList.add('open');
  lifeDrpRender();
  lifeDrpPositionPanel();
  setTimeout(lifeDrpPositionPanel, 0);
}
function lifeDrpClose(){
  lifeDrpState.open = false;
  lifeDrpEl('life-drp-panel').classList.remove('open');
  lifeDrpEl('life-drp-panel').setAttribute('aria-hidden','true');
  lifeDrpEl('life-drp-btn').classList.remove('open');
}
function lifeDrpToggle(){ lifeDrpState.open ? lifeDrpClose() : lifeDrpOpen(); }
function lifeDrpNavMonth(dir){
  lifeDrpState.viewMonth += dir;
  if (lifeDrpState.viewMonth > 11){ lifeDrpState.viewMonth = 0;  lifeDrpState.viewYear += 1; }
  if (lifeDrpState.viewMonth <  0){ lifeDrpState.viewMonth = 11; lifeDrpState.viewYear -= 1; }
  lifeDrpRender();
  lifeDrpPositionPanel();
}
function lifeDrpCalHTML(year, month){
  const today = new Date(); today.setHours(0,0,0,0);
  const firstDay = new Date(year, month, 1).getDay();
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const daysInPrevMonth = new Date(year, month, 0).getDate();
  let rFrom = lifeDrpState.from;
  let rTo   = lifeDrpState.selecting && lifeDrpState.hov ? lifeDrpState.hov : lifeDrpState.to;
  if (rFrom && rTo && rFrom > rTo){ const t = rFrom; rFrom = rTo; rTo = t; }
  let html = '<div class="drp-grid">';
  DRP_DAYS.forEach(d => { html += '<div class="drp-dow">' + d + '</div>'; });
  for (let i = 0; i < firstDay; i++){
    html += '<div class="drp-day other-month">' + (daysInPrevMonth - firstDay + 1 + i) + '</div>';
  }
  for (let d = 1; d <= daysInMonth; d++){
    const dt = new Date(year, month, d); dt.setHours(0,0,0,0);
    const iso = drpFmtLocal(dt);
    const isFuture = dt > today;
    let cls = 'drp-day';
    if (dt.getTime() === today.getTime())            cls += ' today';
    if (rFrom && dt.getTime() === rFrom.getTime())   cls += ' range-start';
    if (rTo   && dt.getTime() === rTo.getTime())     cls += ' range-end';
    if (rFrom && rTo && dt > rFrom && dt < rTo)      cls += ' in-range';
    if (isFuture)                                    cls += ' disabled';
    html += '<div class="' + cls + '" data-date="' + iso + '">' + d + '</div>';
  }
  const total = firstDay + daysInMonth;
  const remainder = total % 7 === 0 ? 0 : 7 - (total % 7);
  for (let d = 1; d <= remainder; d++) html += '<div class="drp-day other-month">' + d + '</div>';
  return html + '</div>';
}
function lifeDrpRender(){
  let rMonth = lifeDrpState.viewMonth + 1;
  let rYear  = lifeDrpState.viewYear;
  if (rMonth > 11){ rMonth = 0; rYear += 1; }
  lifeDrpEl('life-drp-month-l').textContent = DRP_MONTHS[lifeDrpState.viewMonth] + ' ' + lifeDrpState.viewYear;
  lifeDrpEl('life-drp-month-r').textContent = DRP_MONTHS[rMonth]                  + ' ' + rYear;
  lifeDrpEl('life-drp-cal-l').innerHTML = lifeDrpCalHTML(lifeDrpState.viewYear, lifeDrpState.viewMonth);
  lifeDrpEl('life-drp-cal-r').innerHTML = lifeDrpCalHTML(rYear, rMonth);
  const end = lifeDrpState.selecting && lifeDrpState.hov ? lifeDrpState.hov : lifeDrpState.to;
  let a = lifeDrpState.from, b = end;
  if (a && b && a > b){ const t = a; a = b; b = t; }
  // Same mid-selection helper as the AE picker — makes it obvious the
  // second click is required.
  const lFooter = lifeDrpEl('life-drp-footer-range');
  const lApply  = lifeDrpEl('life-drp-apply');
  if (lifeDrpState.selecting && lifeDrpState.from && !lifeDrpState.to){
    lFooter.textContent = drpDisplay(a) + ' → click END date';
    lFooter.style.color = '#B33A2A';
    lFooter.style.fontWeight = '700';
    if (lApply){ lApply.disabled = true; lApply.style.opacity='0.4'; lApply.style.cursor='not-allowed'; }
  } else {
    lFooter.textContent = drpDisplay(a) + ' - ' + drpDisplay(b);
    lFooter.style.color = ''; lFooter.style.fontWeight = '';
    if (lApply){ lApply.disabled = false; lApply.style.opacity=''; lApply.style.cursor=''; }
  }
  lifeDrpSetPresetActive(lifeDrpState.preset);
}
function lifeDrpDayClick(node){
  if (!node || node.classList.contains('other-month') || node.classList.contains('disabled')) return;
  const dt = drpParseLocal(node.dataset.date);
  const hasCompleted = lifeDrpState.from && lifeDrpState.to && !lifeDrpState.selecting;
  if (!lifeDrpState.selecting || hasCompleted){
    lifeDrpState.from = dt; lifeDrpState.to = null; lifeDrpState.hov = null;
    lifeDrpState.selecting = true; lifeDrpState.preset = 'custom';
  } else {
    lifeDrpState.to = dt; lifeDrpState.hov = null; lifeDrpState.selecting = false;
    if (lifeDrpState.from > lifeDrpState.to){ const t = lifeDrpState.from; lifeDrpState.from = lifeDrpState.to; lifeDrpState.to = t; }
  }
  lifeDrpRender();
}
function lifeDrpUpdateRangeHighlight(){
  let rFrom = lifeDrpState.from;
  let rTo   = lifeDrpState.selecting && lifeDrpState.hov ? lifeDrpState.hov : lifeDrpState.to;
  if (rFrom && rTo && rFrom > rTo){ const t = rFrom; rFrom = rTo; rTo = t; }
  const from = rFrom ? rFrom.getTime() : null;
  const to   = rTo   ? rTo.getTime()   : null;
  ['life-drp-cal-l','life-drp-cal-r'].forEach(id => {
    const cal = lifeDrpEl(id); if (!cal) return;
    cal.querySelectorAll('.drp-day').forEach(cell => {
      cell.classList.remove('range-start','range-end','in-range');
      if (cell.classList.contains('other-month')) return;
      const iso = cell.dataset.date; if (!iso) return;
      const t = drpParseLocal(iso).getTime();
      if (from !== null && t === from) cell.classList.add('range-start');
      if (to   !== null && t === to)   cell.classList.add('range-end');
      if (from !== null && to !== null && t > from && t < to) cell.classList.add('in-range');
    });
  });
  const footerEl = lifeDrpEl('life-drp-footer-range');
  if (footerEl){
    if (lifeDrpState.selecting && lifeDrpState.from && !lifeDrpState.to){
      footerEl.textContent = drpDisplay(rFrom) + ' → click END date';
    } else {
      footerEl.textContent = drpDisplay(rFrom) + ' - ' + drpDisplay(rTo);
    }
  }
}
function lifeDrpDayHover(node){
  if (!lifeDrpState.selecting || !node || node.classList.contains('other-month') || node.classList.contains('disabled')) return;
  lifeDrpState.hov = drpParseLocal(node.dataset.date);
  lifeDrpUpdateRangeHighlight();
}
function lifeDrpPreset(key){
  const now = new Date(); now.setHours(0,0,0,0);
  let from = null, to = null;
  if      (key === 'today')      { from = new Date(now); to = new Date(now); }
  else if (key === 'yesterday')  { from = new Date(now); from.setDate(now.getDate()-1); to = new Date(from); }
  else if (key === 'last7')      { from = new Date(now); from.setDate(now.getDate()-6);  to = new Date(now); }
  else if (key === 'last30')     { from = new Date(now); from.setDate(now.getDate()-29); to = new Date(now); }
  else if (key === 'last90')     { from = new Date(now); from.setDate(now.getDate()-89); to = new Date(now); }
  else if (key === 'thisMonth')  { from = new Date(now.getFullYear(), now.getMonth(), 1); to = new Date(now); }
  else if (key === 'lastMonth')  { from = new Date(now.getFullYear(), now.getMonth()-1, 1);
                                   to   = new Date(now.getFullYear(), now.getMonth(),    0); }
  else if (key === 'lifetime')   { from = null; to = null; }
  else if (key === 'custom')     {
    lifeDrpState.from = null; lifeDrpState.to = null; lifeDrpState.hov = null;
    lifeDrpState.selecting = false; lifeDrpState.preset = 'custom';
    lifeDrpRender(); return;
  }
  lifeDrpState.from = from; lifeDrpState.to = to; lifeDrpState.hov = null;
  lifeDrpState.selecting = false; lifeDrpState.preset = key;
  if (from){ lifeDrpState.viewYear = from.getFullYear(); lifeDrpState.viewMonth = from.getMonth(); }
  else { const n = new Date(); lifeDrpState.viewYear = n.getFullYear(); lifeDrpState.viewMonth = n.getMonth() - 1;
         if (lifeDrpState.viewMonth < 0){ lifeDrpState.viewMonth = 11; lifeDrpState.viewYear -= 1; } }
  lifeDrpRender();
  lifeDrpPositionPanel();
  // Auto-apply so the picker closes and the view refreshes on preset click.
  lifeDrpApply();
}
function lifeDrpUpdateButton(){
  const lbl = lifeDrpEl('life-drp-label');
  const map = {today:'Today', yesterday:'Yesterday', last7:'Last 7 Days', last30:'Last 30 Days',
               thisMonth:'This Month', lastMonth:'Last Month', last90:'Last 90 Days', lifetime:'All time'};
  if (lifeDrpState.preset === 'lifetime' || (!lifeDrpState.from && !lifeDrpState.to)){
    lbl.textContent = 'All time'; lbl.classList.remove('active');
  } else if (lifeDrpState.preset !== 'custom'){
    lbl.textContent = map[lifeDrpState.preset] || lifeDrpState.preset; lbl.classList.add('active');
  } else {
    const a = lifeDrpState.from, b = lifeDrpState.to || lifeDrpState.from;
    lbl.textContent = drpDisplay(a) + (a && b && a.getTime() !== b.getTime() ? ' - ' + drpDisplay(b) : '');
    lbl.classList.add('active');
  }
}
function lifeDrpApply(){
  const fromInp = lifeDrpEl('lifeDateFrom'), toInp = lifeDrpEl('lifeDateTo');
  if (lifeDrpState.preset === 'lifetime' || (!lifeDrpState.from && !lifeDrpState.to)){
    fromInp.value = ''; toInp.value = '';
    lifeDrpState.from = null; lifeDrpState.to = null;
  } else {
    let from = lifeDrpState.from, to = lifeDrpState.to || lifeDrpState.from;
    if (from > to){ const t = from; from = to; to = t; }
    fromInp.value = drpFmtLocal(from); toInp.value = drpFmtLocal(to);
    lifeDrpState.from = from; lifeDrpState.to = to;
  }
  lifeDrpState.selecting = false; lifeDrpState.hov = null;
  lifeDrpUpdateButton();
  lifeDrpClose();
  renderLifecycle();
}
function initLifecycleDRP(){
  lifeDrpEl('life-drp-btn'   ).addEventListener('click',  e => { e.stopPropagation(); lifeDrpToggle(); });
  lifeDrpEl('life-drp-prev'  ).addEventListener('click',  e => { e.stopPropagation(); lifeDrpNavMonth(-1); });
  lifeDrpEl('life-drp-next'  ).addEventListener('click',  e => { e.stopPropagation(); lifeDrpNavMonth( 1); });
  lifeDrpEl('life-drp-cancel').addEventListener('click',  e => { e.stopPropagation(); lifeDrpClose(); });
  lifeDrpEl('life-drp-apply' ).addEventListener('click',  e => { e.stopPropagation(); lifeDrpApply(); });
  document.querySelectorAll('#life-drp-presets .drp-preset').forEach(node => {
    node.addEventListener('click', e => { e.stopPropagation(); lifeDrpPreset(node.dataset.key); });
  });
  ['life-drp-cal-l','life-drp-cal-r'].forEach(id => {
    lifeDrpEl(id).addEventListener('click',     e => { e.stopPropagation(); lifeDrpDayClick(e.target.closest('.drp-day')); });
    lifeDrpEl(id).addEventListener('mouseover', e => { lifeDrpDayHover(e.target.closest('.drp-day')); });
  });
  document.addEventListener('click', e => {
    const wrap = lifeDrpEl('life-drp-wrap');
    if (lifeDrpState.open && wrap && !wrap.contains(e.target)) lifeDrpClose();
  });
  window.addEventListener('resize', () => { if (lifeDrpState.open) lifeDrpPositionPanel(); });
  lifeDrpRender();
  lifeDrpUpdateButton();
}

/* ────────────────────────────────────────────────────────────
   Ad Intelligence Date Range Picker — third DRP instance.
   Default preset: last30. Writes to #aiDateFrom / #aiDateTo and
   calls aiReloadOrders() on Apply.
   ──────────────────────────────────────────────────────────── */
const aiDrpState = {
  open:false,
  viewYear: new Date().getFullYear(),
  viewMonth: new Date().getMonth() - 1,
  from:null, to:null, hov:null,
  preset:'last30',
  selecting:false,
};
if (aiDrpState.viewMonth < 0){ aiDrpState.viewMonth = 11; aiDrpState.viewYear -= 1; }
const aiDrpEl = (id) => document.getElementById(id);
function aiDrpSetPresetActive(key){
  document.querySelectorAll('#ai-drp-presets .drp-preset').forEach(n => {
    n.classList.toggle('active', n.dataset.key === key);
  });
}
function aiDrpPositionPanel(){
  const btn = aiDrpEl('ai-drp-btn'), panel = aiDrpEl('ai-drp-panel');
  if (!btn || !panel) return;
  const r = btn.getBoundingClientRect();
  const panelW = panel.offsetWidth || 680;
  const panelH = panel.offsetHeight || 380;
  let top = r.bottom + 6, left = r.right - panelW;
  if (top + panelH > window.innerHeight - 8) top = Math.max(8, r.top - panelH - 6);
  if (left < 8) left = 8;
  if (left + panelW > window.innerWidth - 8) left = window.innerWidth - panelW - 8;
  panel.style.top = top + 'px'; panel.style.left = left + 'px';
}
function aiDrpOpen(){
  aiDrpState.open = true;
  aiDrpEl('ai-drp-panel').classList.add('open');
  aiDrpEl('ai-drp-panel').setAttribute('aria-hidden','false');
  aiDrpEl('ai-drp-btn').classList.add('open');
  aiDrpRender(); aiDrpPositionPanel(); setTimeout(aiDrpPositionPanel, 0);
}
function aiDrpClose(){
  aiDrpState.open = false;
  aiDrpEl('ai-drp-panel').classList.remove('open');
  aiDrpEl('ai-drp-panel').setAttribute('aria-hidden','true');
  aiDrpEl('ai-drp-btn').classList.remove('open');
}
function aiDrpToggle(){ aiDrpState.open ? aiDrpClose() : aiDrpOpen(); }
function aiDrpNavMonth(dir){
  aiDrpState.viewMonth += dir;
  if (aiDrpState.viewMonth > 11){ aiDrpState.viewMonth = 0;  aiDrpState.viewYear += 1; }
  if (aiDrpState.viewMonth <  0){ aiDrpState.viewMonth = 11; aiDrpState.viewYear -= 1; }
  aiDrpRender(); aiDrpPositionPanel();
}
function aiDrpCalHTML(year, month){
  const today = new Date(); today.setHours(0,0,0,0);
  const firstDay = new Date(year, month, 1).getDay();
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const daysInPrevMonth = new Date(year, month, 0).getDate();
  let rFrom = aiDrpState.from;
  let rTo   = aiDrpState.selecting && aiDrpState.hov ? aiDrpState.hov : aiDrpState.to;
  if (rFrom && rTo && rFrom > rTo){ const t = rFrom; rFrom = rTo; rTo = t; }
  let html = '<div class="drp-grid">';
  DRP_DAYS.forEach(d => { html += '<div class="drp-dow">' + d + '</div>'; });
  for (let i = 0; i < firstDay; i++)
    html += '<div class="drp-day other-month">' + (daysInPrevMonth - firstDay + 1 + i) + '</div>';
  for (let d = 1; d <= daysInMonth; d++){
    const dt = new Date(year, month, d); dt.setHours(0,0,0,0);
    const iso = drpFmtLocal(dt);
    const isFuture = dt > today;
    let cls = 'drp-day';
    if (dt.getTime() === today.getTime())            cls += ' today';
    if (rFrom && dt.getTime() === rFrom.getTime())   cls += ' range-start';
    if (rTo   && dt.getTime() === rTo.getTime())     cls += ' range-end';
    if (rFrom && rTo && dt > rFrom && dt < rTo)      cls += ' in-range';
    if (isFuture)                                    cls += ' disabled';
    html += '<div class="' + cls + '" data-date="' + iso + '">' + d + '</div>';
  }
  const total = firstDay + daysInMonth;
  const remainder = total % 7 === 0 ? 0 : 7 - (total % 7);
  for (let d = 1; d <= remainder; d++) html += '<div class="drp-day other-month">' + d + '</div>';
  return html + '</div>';
}
function aiDrpRender(){
  let rMonth = aiDrpState.viewMonth + 1, rYear = aiDrpState.viewYear;
  if (rMonth > 11){ rMonth = 0; rYear += 1; }
  aiDrpEl('ai-drp-month-l').textContent = DRP_MONTHS[aiDrpState.viewMonth] + ' ' + aiDrpState.viewYear;
  aiDrpEl('ai-drp-month-r').textContent = DRP_MONTHS[rMonth]              + ' ' + rYear;
  aiDrpEl('ai-drp-cal-l').innerHTML = aiDrpCalHTML(aiDrpState.viewYear, aiDrpState.viewMonth);
  aiDrpEl('ai-drp-cal-r').innerHTML = aiDrpCalHTML(rYear, rMonth);
  const end = aiDrpState.selecting && aiDrpState.hov ? aiDrpState.hov : aiDrpState.to;
  let a = aiDrpState.from, b = end;
  if (a && b && a > b){ const t = a; a = b; b = t; }
  aiDrpEl('ai-drp-footer-range').textContent = drpDisplay(a) + ' - ' + drpDisplay(b);
  aiDrpSetPresetActive(aiDrpState.preset);
}
function aiDrpDayClick(node){
  if (!node || node.classList.contains('other-month') || node.classList.contains('disabled')) return;
  const dt = drpParseLocal(node.dataset.date);
  if (!aiDrpState.selecting){
    aiDrpState.from = dt; aiDrpState.to = null; aiDrpState.hov = null;
    aiDrpState.selecting = true; aiDrpState.preset = 'custom';
  } else {
    aiDrpState.to = dt; aiDrpState.hov = null; aiDrpState.selecting = false;
    if (aiDrpState.from > aiDrpState.to){ const t = aiDrpState.from; aiDrpState.from = aiDrpState.to; aiDrpState.to = t; }
  }
  aiDrpRender();
}
// Class-only range highlight — same pattern as the AE picker's
// drpUpdateRangeHighlight. Rewriting innerHTML on every mouseover (which
// the previous aiDrpRender() call did) yanked the calendar's DOM out from
// under the user's second click, so the range never completed. Toggling
// classes on the existing cells keeps every click listener alive.
function aiDrpUpdateRangeHighlight(){
  let rFrom = aiDrpState.from;
  let rTo   = aiDrpState.selecting && aiDrpState.hov ? aiDrpState.hov : aiDrpState.to;
  if (rFrom && rTo && rFrom > rTo){ const t = rFrom; rFrom = rTo; rTo = t; }
  const from = rFrom ? rFrom.getTime() : null;
  const to   = rTo   ? rTo.getTime()   : null;
  ['ai-drp-cal-l','ai-drp-cal-r'].forEach(id => {
    const cal = aiDrpEl(id); if (!cal) return;
    cal.querySelectorAll('.drp-day').forEach(cell => {
      cell.classList.remove('range-start','range-end','in-range');
      if (cell.classList.contains('other-month')) return;
      const iso = cell.dataset.date; if (!iso) return;
      const t = drpParseLocal(iso).getTime();
      if (from !== null && t === from) cell.classList.add('range-start');
      if (to   !== null && t === to)   cell.classList.add('range-end');
      if (from !== null && to !== null && t > from && t < to) cell.classList.add('in-range');
    });
  });
  const footerEl = aiDrpEl('ai-drp-footer-range');
  if (footerEl){
    if (aiDrpState.selecting && aiDrpState.from && !aiDrpState.to){
      footerEl.textContent = drpDisplay(rFrom) + ' → click END date';
    } else {
      footerEl.textContent = drpDisplay(rFrom) + ' - ' + drpDisplay(rTo);
    }
  }
}
function aiDrpDayHover(node){
  if (!aiDrpState.selecting || !node || node.classList.contains('other-month') || node.classList.contains('disabled')) return;
  aiDrpState.hov = drpParseLocal(node.dataset.date);
  aiDrpUpdateRangeHighlight();
}
function aiDrpPreset(key){
  const now = new Date(); now.setHours(0,0,0,0);
  let from = null, to = null;
  if      (key === 'today')      { from = new Date(now); to = new Date(now); }
  else if (key === 'yesterday')  { from = new Date(now); from.setDate(now.getDate()-1); to = new Date(from); }
  else if (key === 'last7')      { from = new Date(now); from.setDate(now.getDate()-6);  to = new Date(now); }
  else if (key === 'last15')     { from = new Date(now); from.setDate(now.getDate()-14); to = new Date(now); }
  else if (key === 'last30')     { from = new Date(now); from.setDate(now.getDate()-29); to = new Date(now); }
  else if (key === 'last90')     { from = new Date(now); from.setDate(now.getDate()-89); to = new Date(now); }
  else if (key === 'thisMonth')  { from = new Date(now.getFullYear(), now.getMonth(), 1); to = new Date(now); }
  else if (key === 'lastMonth')  { from = new Date(now.getFullYear(), now.getMonth()-1, 1);
                                   to   = new Date(now.getFullYear(), now.getMonth(),    0); }
  else if (key === 'custom')     {
    aiDrpState.from = null; aiDrpState.to = null; aiDrpState.hov = null;
    aiDrpState.selecting = false; aiDrpState.preset = 'custom';
    aiDrpRender(); return;
  }
  aiDrpState.from = from; aiDrpState.to = to; aiDrpState.hov = null;
  aiDrpState.selecting = false; aiDrpState.preset = key;
  if (from){ aiDrpState.viewYear = from.getFullYear(); aiDrpState.viewMonth = from.getMonth(); }
  aiDrpRender(); aiDrpPositionPanel();
}
function aiDrpUpdateButton(){
  const lbl = aiDrpEl('ai-drp-label');
  const map = {today:'Today', yesterday:'Yesterday', last7:'Last 7 Days', last15:'Last 15 Days',
               last30:'Last 30 Days', thisMonth:'This Month', lastMonth:'Last Month',
               last90:'Last 90 Days', custom:'Custom'};
  if (aiDrpState.preset !== 'custom'){
    lbl.textContent = map[aiDrpState.preset] || aiDrpState.preset; lbl.classList.add('active');
  } else {
    const a = aiDrpState.from, b = aiDrpState.to || aiDrpState.from;
    lbl.textContent = drpDisplay(a) + (a && b && a.getTime() !== b.getTime() ? ' - ' + drpDisplay(b) : '');
    lbl.classList.add('active');
  }
}
function aiDrpApply(){
  const fromInp = aiDrpEl('aiDateFrom'), toInp = aiDrpEl('aiDateTo');
  if (!aiDrpState.from && !aiDrpState.to){ fromInp.value = ''; toInp.value = ''; }
  else {
    let from = aiDrpState.from, to = aiDrpState.to || aiDrpState.from;
    if (from > to){ const t = from; from = to; to = t; }
    fromInp.value = drpFmtLocal(from); toInp.value = drpFmtLocal(to);
  }
  aiDrpUpdateButton(); aiDrpClose();
  aiReloadOrders();
}
function initAiDRP(){
  aiDrpEl('ai-drp-btn'   ).addEventListener('click',  e => { e.stopPropagation(); aiDrpToggle(); });
  aiDrpEl('ai-drp-prev'  ).addEventListener('click',  e => { e.stopPropagation(); aiDrpNavMonth(-1); });
  aiDrpEl('ai-drp-next'  ).addEventListener('click',  e => { e.stopPropagation(); aiDrpNavMonth( 1); });
  aiDrpEl('ai-drp-cancel').addEventListener('click',  e => { e.stopPropagation(); aiDrpClose(); });
  aiDrpEl('ai-drp-apply' ).addEventListener('click',  e => { e.stopPropagation(); aiDrpApply(); });
  document.querySelectorAll('#ai-drp-presets .drp-preset').forEach(node => {
    node.addEventListener('click', e => { e.stopPropagation(); aiDrpPreset(node.dataset.key); });
  });
  ['ai-drp-cal-l','ai-drp-cal-r'].forEach(id => {
    aiDrpEl(id).addEventListener('click',     e => { e.stopPropagation(); aiDrpDayClick(e.target.closest('.drp-day')); });
    aiDrpEl(id).addEventListener('mouseover', e => { aiDrpDayHover(e.target.closest('.drp-day')); });
  });
  document.addEventListener('click', e => {
    const wrap = aiDrpEl('ai-drp-wrap');
    if (aiDrpState.open && wrap && !wrap.contains(e.target)) aiDrpClose();
  });
  window.addEventListener('resize', () => { if (aiDrpState.open) aiDrpPositionPanel(); });
  // Seed the default last-30 window into the hidden inputs
  aiDrpPreset('last30'); aiDrpApply();
}

/* ─────────────────────────────────────────────────────────────
   Ad Intelligence — fetch + render
   ───────────────────────────────────────────────────────────── */
let aiOrders   = [];      // raw rows from shopify_ad_attribution
let aiAdStatusMap = {};   // ad_id → status, populated from allAds when avail
let aiPage     = 0;
let aiSortKey  = 'order_created_at';
let aiSortDir  = 'desc';
let aiLoaded   = false;   // dataset present?
let aiLoading  = false;

async function aiFetchOrders(fromIso, toIso, perfBudgetMs){
  if (!SUPABASE_URL || !SUPABASE_ANON) return [];
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON, Prefer:'count=none'};
  const cols = ['order_id','order_created_at','total_price',
                'utm_source','utm_medium','utm_campaign','utm_content','utm_term',
                'matched_tier','matched_value','has_match',
                'ad_id','ad_name','campaign_name','adset_id',
                // customer_id + customer_num_orders + contact_email were
                // added by shopify_ad_attribution_add_customer_cols
                // migration (2026-07-14) and populated by
                // backfill_customer_info.py.  Show as "—" while the
                // backfill is still catching up on historical rows.
                'customer_id','customer_num_orders','contact_email'].join(',');
  const BATCH = 1000;
  let offset = 0, out = [], pages = 0;
  const t0 = performance.now();
  while (true){
    let url = SUPABASE_URL+'/rest/v1/shopify_ad_attribution?select='+cols+
              '&order=order_created_at.desc&limit='+BATCH+'&offset='+offset;
    if (fromIso) url += '&order_created_at=gte.'+fromIso+'T00:00:00';
    if (toIso)   url += '&order_created_at=lte.'+toIso  +'T23:59:59';
    const r = await fetch(url, {headers});
    if (!r.ok){ break; }
    const chunk = await r.json();
    if (!Array.isArray(chunk) || !chunk.length) break;
    out = out.concat(chunk);
    pages += 1;
    document.getElementById('aiStatus').textContent =
      'Loaded ' + fmtInt(out.length) + ' rows · ' + ((performance.now()-t0)/1000).toFixed(1) + 's';
    if (chunk.length < BATCH) break;
    offset += BATCH;
    // Adaptive abort: if we're past the perf budget, stop early and warn
    if (perfBudgetMs && (performance.now() - t0) > perfBudgetMs) {
      document.getElementById('aiStatus').textContent =
        'Aborted at ' + fmtInt(out.length) + ' rows — slow fetch';
      return null;
    }
  }
  return out;
}

// Applies the Ad Intelligence date range appropriate for the current mode.
// Called by the sidebar routing so switching between Ad Intelligence and
// Historic Ad Intelligence resets the DRP to its meaningful default before
// aiReloadOrders() fires.
function aiApplyHistoricPreset(isHistoric){
  const iso = d => d.toISOString().slice(0,10);
  if (isHistoric){
    // Historic Ad Intelligence: lifetime pre-2025.  shopify_ad_attribution
    // has only ~25 orders in this window today, but the query is honest
    // (the dashboard doesn't secretly clamp to a smaller range).
    const from = new Date('2020-01-01'), to = new Date('2024-12-31');
    document.getElementById('aiDateFrom').value = iso(from);
    document.getElementById('aiDateTo').value   = iso(to);
    aiDrpState.from = from; aiDrpState.to = to; aiDrpState.preset = 'historic';
  } else {
    // Current Ad Intelligence: last 30 days, but never before the cutoff.
    const today = new Date(); today.setHours(0,0,0,0);
    const cutoff = new Date(HISTORIC_CUTOFF + 'T00:00:00');
    let from = new Date(today); from.setDate(today.getDate() - 29);
    if (from < cutoff) from = cutoff;
    document.getElementById('aiDateFrom').value = iso(from);
    document.getElementById('aiDateTo').value   = iso(today);
    aiDrpState.from = from; aiDrpState.to = today; aiDrpState.preset = 'last30';
  }
  if (typeof aiDrpUpdateButton === 'function') aiDrpUpdateButton();
}

async function aiReloadOrders(){
  if (aiLoading) return;
  aiLoading = true;
  // Refresh ad-status map from allAds (live) — used by Active/Inactive filter
  aiAdStatusMap = {};
  for (const a of allAds){ if (a.ad_id) aiAdStatusMap[a.ad_id] = (a.ad_status || '').toUpperCase(); }
  const fromIso = document.getElementById('aiDateFrom').value || '';
  const toIso   = document.getElementById('aiDateTo').value   || '';
  document.getElementById('aiStatus').textContent = 'Loading shopify_ad_attribution …';
  // First attempt: requested window, soft budget 180 s
  const t0 = performance.now();
  let rows = await aiFetchOrders(fromIso, toIso, 180_000);
  if (rows === null){
    // Aborted as too slow — fall back to last 15 days
    const today = new Date(); today.setHours(0,0,0,0);
    const d15 = new Date(today); d15.setDate(today.getDate()-14);
    const iso = d => d.toISOString().slice(0,10);
    const newFrom = iso(d15), newTo = iso(today);
    document.getElementById('aiDateFrom').value = newFrom;
    document.getElementById('aiDateTo').value   = newTo;
    aiDrpState.from = d15; aiDrpState.to = today; aiDrpState.preset = 'last15';
    aiDrpUpdateButton();
    document.getElementById('aiStatus').textContent = 'Window too wide — retrying with Last 15 Days';
    rows = await aiFetchOrders(newFrom, newTo, null) || [];
  }
  aiOrders = rows || [];
  // Stamp asset_id on every row so the existing sort-by-header path
  // (reads a[aiSortKey]) can order by the new Asset ID column.
  for (const r of aiOrders){
    r.asset_id = (r.ad_id && assetIdByAdId[r.ad_id]) || '';
  }
  aiLoaded = true; aiLoading = false;
  document.getElementById('aiStatus').textContent =
    'Loaded ' + fmtInt(aiOrders.length) + ' rows in ' + ((performance.now()-t0)/1000).toFixed(1) + 's';
  // Seed the utm_source multi-select to the current tier mode's channel
  // so the KPI cascade counts and the table row set stay in agreement
  // even before the user touches the Meta/Google toggle. Without this
  // the default Meta mode lets non-Meta rows (kwikengage, direct, etc.)
  // leak into the Unmatched bucket in the table, while the KPI Unmatched
  // card is already meta-scoped — the two disagreed on the same tier.
  if (!aiUtmSourceSel.size){
    for (const r of aiOrders){
      const key = aiSourceKey(r);
      if (aiChannel(key) === aiTierMode) aiUtmSourceSel.add(key);
    }
  }
  // Populate filter dropdowns from data
  aiPopulateFilters();
  aiPage = 0;
  aiRenderChannels();
  // If a drill was already open, refresh its contents against the new dataset
  if (aiOpenChannel) aiRenderChannelDrill(aiOpenChannel);
  aiRenderKpis();
  aiRenderTable();
}

// Selected utm_source values (multi-select). Empty Set = "all sources".
// The sentinel '' in this Set means "utm_source is null/blank".
const aiUtmSourceSel = new Set();

// Build a Map<source, count> from aiOrders including a '' bucket for
// null/blank utm_source rows. Used by the multi-select popover AND by
// the channel drilldown so the two views stay in sync.
function aiBuildSourceCounts(){
  const m = new Map();
  for (const r of aiOrders){
    const key = aiSourceKey(r);
    m.set(key, (m.get(key) || 0) + 1);
  }
  return m;
}

function aiPopulateFilters(){
  const srcCounts = aiBuildSourceCounts();
  const med = new Set();
  for (const r of aiOrders){
    if (r.utm_medium) med.add(r.utm_medium);
  }
  // utm_medium — still a single-select
  const medSel = document.getElementById('aiUtmMedium');
  const curMed = medSel.value;
  medSel.innerHTML = '<option value="">All</option>' +
    Array.from(med).sort().map(v => `<option value="${v}">${v}</option>`).join('');
  if (Array.from(med).includes(curMed)) medSel.value = curMed;
  // utm_source — multi-select
  aiRenderSourceMs(srcCounts);
}

// Render the utm_source multi-select list from a Map<source, count>
function aiRenderSourceMs(srcCounts){
  const list = document.getElementById('aiUtmSourceList');
  const search = (document.getElementById('aiUtmSourceSearch').value || '').toLowerCase();
  // Sort by count desc, then name
  // Friendly labels for the synthetic keys
  const displayOf = key => key === '' ? '(blank)'
                        : key === _AI_HEADLESS ? 'headless_retention'
                        : key === _AI_EXCHANGE ? 'exchange'
                        : key;
  // Sort synthetic keys to the top: (blank), headless_retention, exchange
  const items = Array.from(srcCounts.entries())
    .filter(([n]) => !search || displayOf(n).toLowerCase().includes(search))
    .sort((a,b) => {
      const rank = k => k === '' ? 0 : k === _AI_HEADLESS ? 1 : k === _AI_EXCHANGE ? 2 : 3;
      const ra = rank(a[0]), rb = rank(b[0]);
      if (ra !== rb) return ra - rb;
      return b[1] - a[1] || String(a[0]).localeCompare(String(b[0]));
    });
  list.innerHTML = items.map(([name, count]) => {
    const checked = aiUtmSourceSel.has(name) ? 'checked' : '';
    const displayName = displayOf(name);
    const safe = String(name).replace(/"/g, '&quot;');
    const safeDisp = displayName.replace(/"/g, '&quot;');
    let style = '';
    if (name === '')                 style = ' style="font-style:italic;color:var(--text-tertiary)"';
    else if (name === _AI_HEADLESS)  style = ' style="font-style:italic;color:#8B5CF6"';        // Retention purple
    else if (name === _AI_EXCHANGE)  style = ' style="font-style:italic;color:var(--warning-mid,#D9922A)"'; // Exchange amber
    return '<label class="ms-item">'+
      '<input type="checkbox" data-ms-val="'+safe+'" '+checked+'>'+
      '<span class="ms-item-name" title="'+safeDisp+'"'+style+'>'+displayName+'</span>'+
      '<span class="ms-item-count">'+fmtInt(count)+'</span>'+
    '</label>';
  }).join('') || '<div style="padding:16px;text-align:center;color:var(--text-tertiary);font-size:12px">No matches</div>';
  // Wire per-checkbox change
  list.querySelectorAll('input[type=checkbox]').forEach(cb => {
    cb.addEventListener('change', e => {
      const v = e.target.dataset.msVal;
      if (e.target.checked) aiUtmSourceSel.add(v);
      else                  aiUtmSourceSel.delete(v);
      aiUpdateSourceLabel(srcCounts);
      aiPage = 0; aiRenderTable();
    });
  });
  aiUpdateSourceLabel(srcCounts);
}

function aiUpdateSourceLabel(srcCounts){
  const lbl = document.getElementById('aiUtmSourceLbl');
  const chip = document.getElementById('aiSrcCount');
  const total = srcCounts ? srcCounts.size : 0;
  const n = aiUtmSourceSel.size;
  if (n === 0){
    lbl.textContent = 'All sources' + (total ? ' (' + total + ')' : '');
    lbl.classList.add('placeholder');
    chip.textContent = ''; chip.classList.remove('visible');
  } else if (n === 1){
    lbl.textContent = Array.from(aiUtmSourceSel)[0];
    lbl.classList.remove('placeholder');
    chip.textContent = '1 selected'; chip.classList.add('visible');
  } else {
    lbl.textContent = n + ' sources selected';
    lbl.classList.remove('placeholder');
    chip.textContent = n + ' selected'; chip.classList.add('visible');
  }
}

function aiFiltered(){
  const tier   = document.getElementById('aiTierSel').value;
  const med    = document.getElementById('aiUtmMedium').value;
  const adSt   = document.getElementById('aiAdStatus').value;
  const cam    = (document.getElementById('aiUtmCampaign').value || '').trim().toLowerCase();
  const cnt    = (document.getElementById('aiUtmContent' ).value || '').trim().toLowerCase();
  const trm    = (document.getElementById('aiUtmTerm'    ).value || '').trim().toLowerCase();
  const mv     = (document.getElementById('aiMatchedValue').value|| '').trim().toLowerCase();
  let rows = aiOrders;
  // A row is "matched" if either the Meta cascade or the Google cascade
  // placed it in a non-__none__ bucket.
  const _matched = r => aiStep(r) !== '__none__' || aiGoogleStep(r) !== '__none__';
  if (tier === '__matched__')   rows = rows.filter(_matched);
  else if (tier === '__none__') rows = rows.filter(r => !_matched(r));
  else if (tier === 'G1' || tier === 'G2' || tier === 'G3' || tier === 'G4')
                                rows = rows.filter(r => aiGoogleStep(r) === tier);
  else if (tier)                rows = rows.filter(r => aiStep(r) === tier);
  // Row-level channel filter — takes precedence when set. Used by the
  // Organic (IG) card where the channel is defined by (source, medium,
  // content) together, so a source-based aiUtmSourceSel filter can't
  // express it accurately.
  if (aiChannelFilter){
    rows = rows.filter(r => aiChannel(aiSourceKey(r), r) === aiChannelFilter);
  }
  // Multi-select utm_source — empty Set means "all"
  else if (aiUtmSourceSel.size) rows = rows.filter(r => aiUtmSourceSel.has(aiSourceKey(r)));
  if (med) rows = rows.filter(r => r.utm_medium === med);
  if (cam) rows = rows.filter(r => (r.utm_campaign || '').toLowerCase().includes(cam));
  if (cnt) rows = rows.filter(r => (r.utm_content  || '').toLowerCase().includes(cnt));
  if (trm) rows = rows.filter(r => (r.utm_term     || '').toLowerCase().includes(trm));
  if (mv)  rows = rows.filter(r => (r.matched_value || '').toLowerCase().includes(mv));
  if (adSt){
    rows = rows.filter(r => {
      if (!r.ad_id) return adSt === 'INACTIVE';
      const st = aiAdStatusMap[r.ad_id] || 'INACTIVE';
      return adSt === 'ACTIVE' ? st === 'ACTIVE' : st !== 'ACTIVE';
    });
  }
  // Sort
  rows = rows.slice().sort((a,b) => {
    const av = a[aiSortKey], bv = b[aiSortKey];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    if (typeof av === 'number' && typeof bv === 'number')
      return aiSortDir === 'asc' ? av - bv : bv - av;
    return aiSortDir === 'asc'
      ? String(av).localeCompare(String(bv))
      : String(bv).localeCompare(String(av));
  });
  return rows;
}

/* Normalise legacy tier labels into the canonical Step 1-5 names.
   The DB still contains a few thousand rows from earlier runs that used
   T1_ad_id, T2_ad_name, T3, T0_template — fold them into the equivalent
   modern step so the KPIs are accurate. */
function aiStep(row){
  const t = (row.matched_tier || '').trim();
  if (!t) return '__none__';
  // Google tiers first — has_match may be FALSE for adgroup/campaign-only
  // matches (ad_id intentionally NULL) but the order IS attributed at the
  // adgroup / campaign level, so it should render as matched, not unmatched.
  // G5_google_unpinned = utm hints exist but nothing pinned in gads →
  // effectively unmatched, folds into __none__ so KPI + CSV agree.
  if (t.startsWith('G1')) return 'G1';
  if (t.startsWith('G2')) return 'G2';
  if (t.startsWith('G3')) return 'G3';
  if (t.startsWith('G4')) return 'G4';
  if (t.startsWith('G5')) return '__none__';
  // Meta tiers require has_match true (unchanged legacy behavior)
  if (!row.has_match) return '__none__';
  if (t === 'Step 1' || t === 'T1_ad_id')   return 'Step 1';
  if (t === 'Step 2' || t === 'T2_ad_name') return 'Step 2';
  if (t === 'Step 3' || t === 'T3')         return 'Step 3';
  if (t === 'Step 4')                       return 'Step 4';
  if (t === 'Step 5')                       return 'Step 5';
  return '__none__';
}
function aiTierClass(t){
  if (!t || t === '__none__') return 'none';
  if (t === 'Step 1') return 't1';
  if (t === 'Step 2') return 't2';
  if (t === 'Step 3') return 't3';
  if (t === 'Step 4') return 't3';
  if (t === 'Step 5') return 't4';
  return 'none';
}

/* Classify a raw utm_source (+ optional row for cross-utm checks) into one
   of nine channel buckets. Order matters — the first matching branch wins.
     organic_ig    Meta rows where utm_medium=social AND utm_content=link_in_bio
                    (Instagram bio-link discovery — non-paid, so it's split
                    out of Meta so paid-Meta KPIs stay clean).
     meta          any source starting with "meta" (meta, meta_ads,
                    meta-featuredofferings, metald_1, meta-sitelink, …)
                    plus facebook / fb / instagram / ig / igshopping.
     google        google / gads / google_ads / adwords / any "google*".
     retention     kwikengage/kwikchat/kwikchatbot/any kwik*, plus wa,
                    sagepilot(-ai), email, rcs. Also catches the synthetic
                    key HEADLESS_RETENTION (blank utm_source but non-blank
                    utm_content — retention marker sans source tag).
     brand_collab  fealtyx / brand-collab*
     ai            chatgpt(.com) / openai* / perplexity(.ai) / claude(.ai) /
                    gemini* / bing-chat.
     organic       utm_source explicitly tagged '(direct)' / 'direct' /
                    'none' / '(none)' — the intentional-direct bucket
                    aka Organic (Direct). Truly-blank utm_source falls
                    through to `other` (per team decision — blank rows
                    are un-attributed, not organic).
     loyalty       nector / loyalty*.
     other         everything else (exchange orders, robylon, one-offs). */
const _AI_HEADLESS = '__headless_retention__';   // synthetic source keys
const _AI_EXCHANGE = '__exchange__';
const _AI_META_RE      = /^(meta[\w\-]*|facebook|fb|instagram|ig|igshopping)$/i;
const _AI_GOOGLE_RE    = /^(google[\w\-]*|gads|adwords)$/i;
const _AI_RETENTION_RE = /^(kwik[\w\-]*|wa|sagepilot(?:[-_]?ai)?|email|rcs)$/i;
// Newer sub-buckets split out of the old "Other" pool.
const _AI_BRANDCOLLAB_RE = /^(fealtyx[\w\-]*|brand.?collab[\w\-]*)$/i;
const _AI_AI_RE          = /^(chatgpt(?:\.com)?|openai(?:[\w\-]*)|perplexity(?:\.ai)?|claude(?:\.ai)?|gemini(?:[\w\-]*)|bing.?chat)$/i;
const _AI_LOYALTY_RE     = /^(nector[\w\-]*|loyalty[\w\-]*)$/i;
// Explicit direct markers — only rows carrying one of these utm_source
// values count as Organic (Direct). Truly-blank utm_source (empty string
// after aiSourceKey normalisation) is treated as Other so the Organic
// card only reflects intentional "direct" tagging.
const _AI_DIRECT_MARKERS = new Set(['(direct)', 'direct', 'none', '(none)']);

/* Row → synthetic source key used by the filter, drilldown and multi-select.
      utm_source populated                 -> the raw source string
      utm_source blank + utm_content set   -> __headless_retention__  (Retention)
      utm_source blank + total_price == 0  -> __exchange__            (Other)
      utm_source blank + everything else   -> ''                      (Other)
   Priority for the two synthetic keys is: headless_retention wins over
   exchange, because a headless retention click that happened to be
   worth zero rupees is still marketing-attributable — the free-swap
   should only claim rows that carry no other signal. */
function aiSourceKey(r){
  // Normalise to lower-case so case-variants ("META" / "Meta" / "meta")
  // collapse into a single bucket in the KPI count, drilldown table,
  // multi-select filter and export. Otherwise the drilldown shows
  // three visually-identical "meta" rows and manual summation misses
  // whichever variants the user skims past.
  const s = (r.utm_source || '').trim().toLowerCase();
  if (s) return s;
  const c = (r.utm_content || '').trim();
  if (c) return _AI_HEADLESS;
  const price = +r.total_price;
  if (isFinite(price) && price === 0) return _AI_EXCHANGE;
  return '';
}
// Optional row arg lets us peek at utm_medium + utm_content when the
// source alone is ambiguous (e.g. Meta rows whose utm_medium=social AND
// utm_content=link_in_bio are the Instagram bio-link → Organic (IG),
// not paid Meta).
function aiChannel(srcKey, row){
  const s = (srcKey || '').trim();
  // ── Organic (IG): all three fields must match exactly ──────────
  //   utm_source  = 'ig'
  //   utm_medium  = 'social'
  //   utm_content = 'link_in_bio'
  // Verified against shopify_ad_attribution: the triple matches 789
  // orders (₹12.4 L), which is the entire IG bio-link discovery pool.
  // Requiring the source to be 'ig' too prevents any future paid Meta
  // rows with the same medium/content combo from being misclassified.
  if (row){
    const usrc = (row.utm_source  || '').trim().toLowerCase();
    const um   = (row.utm_medium  || '').trim().toLowerCase();
    const uc   = (row.utm_content || '').trim().toLowerCase();
    if (usrc === 'ig' && um === 'social' && uc === 'link_in_bio') {
      return 'organic_ig';
    }
  }
  if (s === _AI_HEADLESS)       return 'retention';
  if (s === _AI_EXCHANGE)       return 'other';
  // Truly-blank utm_source (no marker at all) → Other. Only explicit
  // "(direct)" / "direct" / "none" markers roll into Organic (Direct).
  if (!s)                       return 'other';
  if (_AI_DIRECT_MARKERS.has(s.toLowerCase()))
                                return 'organic';
  if (_AI_META_RE.test(s))      return 'meta';
  if (_AI_GOOGLE_RE.test(s))    return 'google';
  if (_AI_RETENTION_RE.test(s)) return 'retention';
  if (_AI_BRANDCOLLAB_RE.test(s)) return 'brand_collab';
  if (_AI_AI_RE.test(s))          return 'ai';
  if (_AI_LOYALTY_RE.test(s))     return 'loyalty';
  return 'other';
}

/* Track which channel drilldown is currently open (empty = none). */
let aiOpenChannel = '';
let aiDisplayMode = 'count';   // 'count' | 'pct' — swaps KPI primary value with chip
let aiTierMode    = 'meta';    // 'meta' | 'google' — which tier cascade the KPI row shows
/* Optional row-level channel filter. Set by clicking a channel KPI card
   when the channel is defined by multi-field logic (currently just
   Organic (IG)). '' means no channel filter is active. */
let aiChannelFilter = '';

// Canonical bucket list — must stay in sync with aiChannel + the HTML
// dom ids under #aiChannelRow. Adding a new bucket = 3 edits: aiChannel,
// this list, and the <div class="kpi ai-ch-card" data-channel="…">.
const AI_CHANNELS = ['meta','organic_ig','google','retention',
                     'brand_collab','ai','organic','loyalty','other'];

function aiRenderChannels(){
  const chs = {all:{n:0,sp:0}};
  for (const k of AI_CHANNELS) chs[k] = {n:0, sp:0};
  for (const r of aiOrders){
    // Pass row so Organic (IG) detection can peek at utm_medium + utm_content.
    const c = aiChannel(aiSourceKey(r), r);
    const sp = +r.total_price || 0;
    chs.all.n += 1;      chs.all.sp += sp;
    if (chs[c]){ chs[c].n += 1; chs[c].sp += sp; }
  }
  const total = chs.all.n || 0;
  const set = (id, k) => {
    if (!document.getElementById('aiCh-'+id)) return;
    const n = chs[k]?.n || 0;
    const pctStr = k === 'all' ? '100%'
                 : (total > 0 ? ((n / total * 100).toFixed(1) + '%') : '—');
    const countStr = fmtInt(n);
    const primary   = aiDisplayMode === 'pct' ? pctStr   : countStr;
    const secondary = aiDisplayMode === 'pct' ? countStr : pctStr;
    document.getElementById('aiCh-'+id     ).textContent = primary;
    document.getElementById('aiCh-'+id+'-sp').textContent = fmtRs(chs[k]?.sp || 0);
    const pcEl = document.getElementById('aiCh-'+id+'-pc');
    if (pcEl) pcEl.textContent = secondary;
  };
  set('all','all');
  for (const k of AI_CHANNELS) set(k, k);
  // Reflect currently-open drilldown selection
  document.querySelectorAll('#aiChannelRow .kpi').forEach(card => {
    card.classList.toggle('selected', card.dataset.channel === aiOpenChannel);
  });
}

function aiRenderChannelDrill(channel){
  aiOpenChannel = channel;
  document.querySelectorAll('#aiChannelRow .kpi').forEach(card => {
    card.classList.toggle('selected', card.dataset.channel === channel);
  });
  const panel = document.getElementById('aiChannelDrill');
  const body  = document.getElementById('aiChannelDrillBody');
  const ttl   = document.getElementById('aiChannelDrillTtl');
  const sub   = document.getElementById('aiChannelDrillSub');
  const nice  = {meta:'Meta', organic_ig:'Organic (IG)', google:'Google',
                 retention:'Retention', brand_collab:'Brand Collab',
                 ai:'AI', organic:'Organic (Direct)', loyalty:'Loyalty',
                 other:'Other', all:'All channels'};
  panel.style.display = 'block';
  // For Organic (IG) we group by utm_content (since all rows share
  // utm_source=ig anyway); re-label everything so the header, subtitle
  // and column all agree on what the values actually are.
  const groupField = channel === 'organic_ig' ? 'utm_content' : 'utm_source';
  ttl.textContent = nice[channel] + ' · ' + groupField + ' breakdown';
  // Group rows in this channel by the row-level source key so the two
  // synthetic sources ('' -> "(blank)" and __headless_retention__ ->
  // "headless_retention") appear as first-class entries in the table.
  const groups = new Map();
  let totalOrders = 0, totalSales = 0;
  for (const r of aiOrders){
    const key = aiSourceKey(r);
    const c   = aiChannel(key, r);   // row-aware so Organic (IG) is honored
    if (channel !== 'all' && c !== channel) continue;
    // For Organic (IG) we group on the utm_content marker rather than the
    // (already Meta-classified) source so the drilldown shows "link_in_bio"
    // as a first-class row instead of collapsing everything into "meta".
    const dispKey = c === 'organic_ig'
                    ? ((r.utm_content || '').trim().toLowerCase() || '(blank)')
                    : key;
    if (!groups.has(dispKey)) groups.set(dispKey, {key:dispKey, orders:0, sales:0});
    const g = groups.get(dispKey);
    g.orders += 1;
    g.sales  += +r.total_price || 0;
    totalOrders += 1;
    totalSales  += +r.total_price || 0;
  }
  // Keys are already lower-cased by aiSourceKey() so we just pass through
  const displayOf = k => k === '' ? '(blank)'
                       : k === _AI_HEADLESS ? 'headless_retention'
                       : k === _AI_EXCHANGE ? 'exchange'
                       : String(k);
  const list = Array.from(groups.values())
    .sort((a,b) => b.orders - a.orders);
  sub.textContent = fmtInt(totalOrders) + ' orders · ' + fmtRs(totalSales) +
                    ' · ' + list.length + ' unique ' + groupField + ' values';
  if (!list.length){
    body.innerHTML = '<div class="ai-empty-hint">No orders in this channel for the current window.</div>';
    return;
  }
  const maxOrders = list[0].orders;
  const barCls = channel === 'all' ? 'other' : channel;
  body.innerHTML = '<table class="ai-src-tbl">'+
    '<thead><tr>'+
      '<th style="width:180px">' + groupField + '</th>'+
      '<th class="num" style="width:100px">Orders</th>'+
      '<th class="num" style="width:130px">Sales</th>'+
      '<th class="num" style="width:80px">AOV</th>'+
      '<th>Share of channel</th>'+
    '</tr></thead>'+
    '<tbody>' + list.map(g => {
      const pct  = totalOrders ? (g.orders / totalOrders * 100) : 0;
      const bw   = maxOrders   ? (g.orders / maxOrders  * 100) : 0;
      const aov  = g.orders > 0 ? g.sales / g.orders : 0;
      const safe = String(g.key).replace(/"/g,'&quot;');
      const disp = displayOf(g.key);
      let italic = '';
      if (g.key === '')             italic = ' style="font-style:italic;color:var(--text-tertiary)"';
      else if (g.key === _AI_HEADLESS) italic = ' style="font-style:italic;color:#8B5CF6"';
      else if (g.key === _AI_EXCHANGE) italic = ' style="font-style:italic;color:var(--warning-mid,#D9922A)"';
      return '<tr data-drill-src="'+safe+'">'+
        '<td><span class="src-name"'+italic+'>'+disp+'</span></td>'+
        '<td class="num">'+fmtInt(g.orders)+'</td>'+
        '<td class="num">'+fmtRs(g.sales)+'</td>'+
        '<td class="num">'+fmtRs(aov)+'</td>'+
        '<td><div class="ai-src-bar-cell">'+
          '<div class="bar"><div class="bar-fill '+barCls+'" style="width:'+bw.toFixed(1)+'%"></div></div>'+
          '<span class="ai-src-bar-pct">'+pct.toFixed(1)+'%</span>'+
        '</div></td>'+
      '</tr>';
    }).join('') + '</tbody></table>';
  // Row click behaviour depends on how the drilldown is grouped.
  // For source-based channels: add the source key to aiUtmSourceSel.
  // For Organic (IG) (grouped by utm_content): keep the row-level
  // aiChannelFilter — the utm_content values are the drilldown's
  // key domain, not utm_source, so pushing them into aiUtmSourceSel
  // would filter to zero rows.
  body.querySelectorAll('tr[data-drill-src]').forEach(tr => {
    tr.addEventListener('click', () => {
      if (channel === 'organic_ig'){
        // No-op for the Organic (IG) case — the aiChannelFilter set
        // during the card click already narrows the table correctly.
        return;
      }
      const key = tr.dataset.drillSrc;
      aiUtmSourceSel.clear();
      aiUtmSourceSel.add(key);
      aiRenderSourceMs(aiBuildSourceCounts());
      aiPage = 0; aiRenderTable();
    });
  });
}

function aiCloseChannelDrill(){
  aiOpenChannel = '';
  document.getElementById('aiChannelDrill').style.display = 'none';
  document.querySelectorAll('#aiChannelRow .kpi').forEach(c => c.classList.remove('selected'));
}

/* Google-side tier bucketing: G1 (ad_id), G2 (campaign_id),
   G3 (joint campaign+ad_name), G4 (unique ad_name), Unmatched. */
function aiGoogleStep(row){
  const t = (row.matched_tier || '').trim();
  // Prefix match so DB variants (G1_ad_id, G4_campaign_only, etc.) all fold
  // into the 4 canonical KPI slots. G5_google_unpinned buckets with Gnone.
  if (t.startsWith('G1')) return 'G1';
  if (t.startsWith('G2')) return 'G2';
  if (t.startsWith('G3')) return 'G3';
  if (t.startsWith('G4')) return 'G4';
  return '__none__';
}

function aiRenderKpis(){
  if (aiTierMode === 'google'){
    const buckets = {G1:0,G2:0,G3:0,G4:0,'__none__':0};
    const sales   = {G1:0,G2:0,G3:0,G4:0,'__none__':0};
    // Scope to Google-channel orders only, so the % denominator makes sense
    for (const r of aiOrders){
      // Row-aware so Organic (IG) doesn't spill into Meta / Google tiers.
      if (aiChannel(aiSourceKey(r), r) !== 'google') continue;
      const k = aiGoogleStep(r);
      buckets[k] += 1;
      sales[k]   += (+r.total_price || 0);
    }
    let total = 0; for (const k in buckets) total += buckets[k];
    const set = (id, k) => {
      const n = buckets[k] || 0;
      const pctStr   = total > 0 ? ((n / total * 100).toFixed(1) + '%') : '—';
      const countStr = fmtInt(n);
      const primary   = aiDisplayMode === 'pct' ? pctStr : countStr;
      const secondary = aiDisplayMode === 'pct' ? countStr : pctStr;
      document.getElementById('aiKp-'+id     ).textContent = primary;
      document.getElementById('aiKp-'+id+'-sp').textContent = fmtRs(sales[k] || 0);
      const pcEl = document.getElementById('aiKp-'+id+'-pc');
      if (pcEl) pcEl.textContent = secondary;
    };
    set('G1','G1'); set('G2','G2'); set('G3','G3'); set('G4','G4'); set('Gnone','__none__');
    return;
  }
  const buckets = {'Step 1':0,'Step 2':0,'Step 3':0,'Step 4':0,'Step 5':0,'__none__':0};
  const sales   = {'Step 1':0,'Step 2':0,'Step 3':0,'Step 4':0,'Step 5':0,'__none__':0};
  // Scope to Meta-channel orders only, so the % denominator makes sense.
  // Row-aware call excludes Organic (IG) — those aren't paid Meta orders
  // and shouldn't inflate the Meta tier counts.
  for (const r of aiOrders){
    if (aiChannel(aiSourceKey(r), r) !== 'meta') continue;
    const k = aiStep(r);
    buckets[k] += 1;
    sales[k]   += (+r.total_price || 0);
  }
  let total = 0; for (const k in buckets) total += buckets[k];
  const set = (id, k) => {
    const n = buckets[k] || 0;
    const pctStr   = total > 0 ? ((n / total * 100).toFixed(1) + '%') : '—';
    const countStr = fmtInt(n);
    const primary   = aiDisplayMode === 'pct' ? pctStr : countStr;
    const secondary = aiDisplayMode === 'pct' ? countStr : pctStr;
    document.getElementById('aiKp-'+id     ).textContent = primary;
    document.getElementById('aiKp-'+id+'-sp').textContent = fmtRs(sales[k] || 0);
    const pcEl = document.getElementById('aiKp-'+id+'-pc');
    if (pcEl) pcEl.textContent = secondary;
  };
  set('S1','Step 1'); set('S2','Step 2'); set('S3','Step 3');
  set('S4','Step 4'); set('S5','Step 5'); set('none','__none__');
}

// UTM Analytics — 3-panel viz sitting above the orders table.  Recomputes
// off the currently-filtered aiOrders every time the table re-renders,
// so channel / step / date filters flow through automatically.
const _AI_CHANNEL_ORDER = [
  {key:'meta',        label:'Meta',        color:'#1877F2'},
  {key:'google',      label:'Google',      color:'#34A853'},
  {key:'organic_ig',  label:'Organic (IG)', color:'#E1306C'},
  {key:'retention',   label:'Retention',   color:'#8B5CF6'},
  {key:'brand_collab',label:'Brand Collab',color:'#F59E0B'},
  {key:'ai',          label:'AI',          color:'#10B981'},
  {key:'organic',     label:'Organic (Direct)', color:'#14B8A6'},
  {key:'loyalty',     label:'Loyalty',     color:'#EF4444'},
  {key:'other',       label:'Other',       color:'#9A9384'},
];
let _aiChartTime = null, _aiChartSource = null;

function aiRenderCharts(rows){
  const container = document.getElementById('aiAnalyticsRow');
  if (!container) return;
  if (!rows || !rows.length){
    // Blank state — keep chart canvases visible so DOM sizing stays stable
    if (_aiChartTime)   { _aiChartTime.destroy();   _aiChartTime = null; }
    if (_aiChartSource) { _aiChartSource.destroy(); _aiChartSource = null; }
    document.getElementById('aiTopUtmContent').innerHTML =
      '<div style="padding:24px;text-align:center;color:var(--text-tertiary);font-size:11.5px">No orders in the current filter.</div>';
    return;
  }

  // ── Chart 1: stacked daily orders by channel ────────────────────────
  const byDate = new Map();     // date → { [channelKey]: count }
  const chSet  = new Set();
  for (const r of rows){
    const d = (r.order_created_at || '').slice(0, 10);
    if (!d) continue;
    const src = aiSourceKey(r);
    const ch  = aiChannel(src, r);
    chSet.add(ch);
    if (!byDate.has(d)) byDate.set(d, {});
    const b = byDate.get(d);
    b[ch] = (b[ch] || 0) + 1;
  }
  const dates = Array.from(byDate.keys()).sort();
  // Cap X-axis to a reasonable number of ticks
  const displayDates = dates.length > 60 ? dates.filter((_,i) => i % Math.ceil(dates.length/60) === 0) : dates;
  const datasets = _AI_CHANNEL_ORDER
    .filter(c => chSet.has(c.key))
    .map(c => ({
      label: c.label,
      data:  dates.map(d => (byDate.get(d)?.[c.key]) || 0),
      backgroundColor: c.color,
      borderColor: c.color,
      borderWidth: 0,
      stack: 'orders',
    }));

  const ctxTime = document.getElementById('aiChartTime');
  if (_aiChartTime) _aiChartTime.destroy();
  _aiChartTime = new Chart(ctxTime, {
    type: 'bar',
    data: { labels: dates, datasets },
    options: {
      responsive:true, maintainAspectRatio:false,
      plugins: {
        legend: { position:'bottom', labels:{ boxWidth:10, font:{size:10.5}, padding:8 } },
        tooltip: { mode:'index', intersect:false },
      },
      scales: {
        x: { stacked:true, ticks:{ maxTicksLimit:12, font:{size:10} }, grid:{display:false} },
        y: { stacked:true, ticks:{ font:{size:10} }, grid:{color:'rgba(0,0,0,0.05)'} },
      },
    },
  });

  // ── Chart 2: doughnut of utm_source distribution ────────────────────
  const bySource = new Map();
  for (const r of rows){
    const s = (r.utm_source || '(direct)').toLowerCase().trim() || '(direct)';
    bySource.set(s, (bySource.get(s) || 0) + 1);
  }
  const sortedSrc = Array.from(bySource.entries()).sort((a,b) => b[1] - a[1]);
  const top = sortedSrc.slice(0, 7);
  const rest = sortedSrc.slice(7).reduce((s, [,v]) => s + v, 0);
  if (rest > 0) top.push(['other utm_sources', rest]);
  const srcLabels = top.map(([l]) => l);
  const srcData   = top.map(([,v]) => v);
  const srcColors = ['#1877F2','#34A853','#E1306C','#8B5CF6','#F59E0B','#10B981','#14B8A6','#9A9384'];

  const ctxSrc = document.getElementById('aiChartSource');
  if (_aiChartSource) _aiChartSource.destroy();
  _aiChartSource = new Chart(ctxSrc, {
    type: 'doughnut',
    data: {
      labels: srcLabels,
      datasets: [{ data: srcData, backgroundColor: srcColors, borderWidth: 2, borderColor: '#fff' }],
    },
    options: {
      responsive:true, maintainAspectRatio:false,
      cutout: '60%',
      plugins: {
        legend: { position:'right', labels:{ boxWidth:10, font:{size:10.5}, padding:6 } },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const total = srcData.reduce((s,v) => s+v, 0);
              const v = ctx.parsed;
              const pct = total > 0 ? (v/total*100).toFixed(1) : '0';
              return `${ctx.label}: ${v.toLocaleString('en-IN')} (${pct}%)`;
            }
          }
        }
      },
    },
  });

  // ── Panel 3: top utm_content ─────────────────────────────────────
  const byContent = new Map();
  for (const r of rows){
    const c = (r.utm_content || '(none)').trim() || '(none)';
    const cur = byContent.get(c) || {orders:0, sales:0};
    cur.orders += 1;
    cur.sales  += (+r.total_price || 0);
    byContent.set(c, cur);
  }
  const topContent = Array.from(byContent.entries())
                       .sort((a,b) => b[1].orders - a[1].orders)
                       .slice(0, 12);
  const maxOrd = topContent.reduce((m, [,v]) => Math.max(m, v.orders), 0) || 1;
  document.getElementById('aiTopUtmContent').innerHTML =
    topContent.map(([lbl, v]) => {
      const pct = (v.orders / maxOrd) * 100;
      const short = lbl.length > 48 ? lbl.slice(0, 45) + '…' : lbl;
      return '<div class="ai-topbar" style="--bar-pct:' + pct.toFixed(1) + '%" title="' +
             lbl.replace(/"/g, '&quot;') + '">'+
        '<div class="ai-topbar-lbl">' + short.replace(/</g,'&lt;') + '</div>'+
        '<div class="ai-topbar-val">' + fmtInt(v.orders) +
        '<span class="ai-topbar-sales">' + fmtRs(v.sales) + '</span></div>'+
      '</div>';
    }).join('') || '<div style="padding:20px;text-align:center;color:var(--text-tertiary);font-size:11.5px">No utm_content values to show.</div>';
}

function aiRenderTable(){
  const rows = aiFiltered();
  aiRenderCharts(rows);
  const pageSize = +document.getElementById('aiPageSize').value || 100;
  const totalRows = rows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  if (aiPage >= totalPages) aiPage = totalPages - 1;
  if (aiPage < 0) aiPage = 0;
  const offset = aiPage * pageSize;
  const slice = rows.slice(offset, offset + pageSize);
  const tb = document.querySelector('#aiTbl tbody');
  if (!slice.length){
    tb.innerHTML = '<tr><td colspan="15" style="padding:30px;text-align:center;color:var(--text-tertiary)">No rows match the current filter.</td></tr>';
  } else {
    tb.innerHTML = slice.map(r => {
      const step  = aiStep(r);
      const gstep = aiGoogleStep(r);
      const tier  = step !== '__none__' ? step
                  : gstep !== '__none__' ? gstep
                  : '—';
      const tc    = step !== '__none__' ? aiTierClass(step)
                  : gstep !== '__none__' ? 't1'
                  : 'none';
      const date = (r.order_created_at || '').slice(0,16).replace('T',' ');
      const st   = r.ad_id ? (aiAdStatusMap[r.ad_id] || 'UNKNOWN') : '—';
      const stCls= st === 'ACTIVE' ? 'active' : '';
      return '<tr>'+
        '<td class="id-cell">'+date+'</td>'+
        '<td class="id-cell">'+(r.order_id || '—').replace('gid://shopify/Order/','')+'</td>'+
        // Customer ID + lifetime orders sit right after Order ID so buyer
        // + loyalty read together.  Populated by backfill_customer_info.py
        // (and rebuild_attribution_orders.py for new syncs); "—" while
        // the backfill is still catching up on historical rows.
        '<td class="id-cell" title="'+(r.contact_email || '').replace(/"/g,'&quot;')+'">'+
          (r.customer_id || '—')+
        '</td>'+
        '<td class="num">'+(r.customer_num_orders != null ? fmtInt(r.customer_num_orders) : '—')+'</td>'+
        '<td class="num">'+fmtRs(r.total_price)+'</td>'+
        '<td><span class="ai-tier '+tc+'">'+tier+'</span></td>'+
        '<td>'+(r.utm_source  || '—')+'</td>'+
        '<td>'+(r.utm_medium  || '—')+'</td>'+
        '<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis" title="'+(r.utm_campaign||'').replace(/"/g,'&quot;')+'">'+(r.utm_campaign || '—')+'</td>'+
        '<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis" title="'+(r.utm_content ||'').replace(/"/g,'&quot;')+'">'+(r.utm_content  || '—')+'</td>'+
        '<td style="max-width:160px;overflow:hidden;text-overflow:ellipsis" title="'+(r.utm_term    ||'').replace(/"/g,'&quot;')+'">'+(r.utm_term     || '—')+'</td>'+
        '<td style="max-width:160px;overflow:hidden;text-overflow:ellipsis" title="'+(r.matched_value||'').replace(/"/g,'&quot;')+'">'+(r.matched_value|| '—')+'</td>'+
        '<td class="id-cell">'+(r.ad_id || '—')+'</td>'+
        // Asset ID from manual ad_asset_ids mapping. Blank when the
        // Sheet hasn't been imported for this ad yet.  Editable inline
        // via installAssetIdCellEditor (wired in the delegated handler
        // just after render).
        '<td class="id-cell ai-asset-cell" data-ad-id="'+(r.ad_id||'')+'">'+
          (r.ad_id && assetIdByAdId[r.ad_id] || '—')+
        '</td>'+
        // Ad Name & Campaign columns widened so long ad-copy titles land
        // on a single visible line — the table already sits inside a
        // horizontal-scroll wrapper so the extra width doesn't hurt.
        '<td style="min-width:420px;max-width:520px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+(r.ad_name    ||'').replace(/"/g,'&quot;')+'">'+(r.ad_name    || '—')+'</td>'+
        '<td style="min-width:280px;max-width:360px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+(r.campaign_name||'').replace(/"/g,'&quot;')+'">'+(r.campaign_name|| '—')+'</td>'+
        '<td><span class="ai-status '+stCls+'">'+st+'</span></td>'+
      '</tr>';
    }).join('');
  }
  // Wire the inline asset-id editor on every Asset ID cell in this
  // page's slice. Idempotent — the editor bails if already bound.
  document.querySelectorAll('#aiTbl tbody .ai-asset-cell[data-ad-id]').forEach(td => {
    const adId = td.dataset.adId;
    if (!adId) return;
    installAssetIdCellEditor(td, adId, {
      onSaved: (val) => {
        // Keep the row-level copy in sync so sort by asset_id still works
        const r = aiOrders.find(x => String(x.ad_id) === String(adId));
        if (r) r.asset_id = val;
      }
    });
  });
  document.getElementById('aiRowInfo').textContent  = fmtInt(totalRows) + ' rows';
  document.getElementById('aiPageInfo').textContent = 'Page ' + (aiPage + 1) + ' / ' + totalPages;
  document.getElementById('aiPrevPage').disabled    = aiPage === 0;
  document.getElementById('aiNextPage').disabled    = aiPage >= totalPages - 1;
}

function initAdIntel(){
  initAiDRP();
  // Filter wiring — every change re-renders (no re-fetch unless date changes)
  ['aiTierSel','aiUtmMedium','aiAdStatus'].forEach(id =>
    document.getElementById(id).addEventListener('change', () => { aiPage = 0; aiRenderTable(); }));
  // Edit Asset ID toggle — same global flag as the AE-side button.
  const aiEditBtn = document.getElementById('aiEditAssetBtn');
  if (aiEditBtn) aiEditBtn.onclick = () => setAssetEditMode(!window.assetEditMode);
  // utm_source multi-select popover
  const msBtn  = document.getElementById('aiUtmSourceBtn');
  const msPanel= document.getElementById('aiUtmSourcePanel');
  const msWrap = document.getElementById('aiUtmSourceMs');
  msBtn.addEventListener('click', e => {
    e.stopPropagation();
    const isOpen = msPanel.classList.contains('open');
    msPanel.classList.toggle('open', !isOpen);
    msBtn.classList.toggle('open', !isOpen);
    msPanel.setAttribute('aria-hidden', isOpen ? 'true' : 'false');
    if (!isOpen) setTimeout(() => document.getElementById('aiUtmSourceSearch').focus(), 20);
  });
  document.addEventListener('click', e => {
    if (msPanel.classList.contains('open') && !msWrap.contains(e.target)){
      msPanel.classList.remove('open'); msBtn.classList.remove('open');
      msPanel.setAttribute('aria-hidden','true');
    }
  });
  document.getElementById('aiUtmSourceSearch').addEventListener('input', () => {
    // Re-render the list with the current search term (counts recomputed from aiOrders)
    aiRenderSourceMs(aiBuildSourceCounts());
  });
  document.querySelectorAll('#aiUtmSourcePanel .ms-mini').forEach(btn => {
    btn.addEventListener('click', e => {
      const op = e.target.dataset.msOp;
      const counts = aiBuildSourceCounts();
      if (op === 'all'){
        // "All" = select every source currently visible after search
        const searchQ = (document.getElementById('aiUtmSourceSearch').value || '').toLowerCase();
        for (const [name] of counts){
          const hay = name === '' ? '(blank)' : name.toLowerCase();
          if (!searchQ || hay.includes(searchQ)) aiUtmSourceSel.add(name);
        }
      } else if (op === 'none'){
        aiUtmSourceSel.clear();
      }
      aiRenderSourceMs(counts);
      aiPage = 0; aiRenderTable();
    });
  });
  ['aiUtmCampaign','aiUtmContent','aiUtmTerm','aiMatchedValue'].forEach(id =>
    document.getElementById(id).addEventListener('input', () => {
      clearTimeout(window._aiDb);
      window._aiDb = setTimeout(() => { aiPage = 0; aiRenderTable(); }, 220);
    }));
  document.getElementById('aiPageSize').addEventListener('change', () => { aiPage = 0; aiRenderTable(); });
  document.getElementById('aiPrevPage').addEventListener('click', () => { if (aiPage > 0){ aiPage--; aiRenderTable(); } });
  document.getElementById('aiNextPage').addEventListener('click', () => { aiPage++; aiRenderTable(); });
  // Header sort
  document.querySelectorAll('#aiTbl thead th').forEach(th => {
    th.style.cursor = 'pointer';
    th.addEventListener('click', () => {
      const k = th.dataset.aisort; if (!k) return;
      if (aiSortKey === k) aiSortDir = aiSortDir === 'asc' ? 'desc' : 'asc';
      else { aiSortKey = k; aiSortDir = 'desc'; }
      aiRenderTable();
    });
  });
  // Refresh / Export
  document.getElementById('aiRefresh').onclick = () => { aiLoaded = false; aiReloadOrders(); };
  document.getElementById('aiExport').onclick = () => {
    // Export the filtered order list in visible column order.  Enrich
    // each row with the canonical Step-N label (matching what the UI
    // renders) and the resolved ad_status pulled from aiAdStatusMap,
    // plus the current Asset ID so the "Asset ID" column isn't blank.
    // These fields aren't on the raw DB row.
    const rows = aiFiltered();
    exportVisibleTableCsv('#aiTbl', rows, {
      filenamePrefix: 'ad_intelligence',
      deriveRow: r => {
        const step  = aiStep(r);
        return {
          ...r,
          matched_tier: step === '__none__' ? 'Unmatched' : step,
          ad_status:    r.ad_id ? (aiAdStatusMap[r.ad_id] || 'UNKNOWN') : '',
          asset_id:     r.ad_id ? (assetIdByAdId[r.ad_id] || '') : '',
        };
      },
      // Column labels in the AI table include "Date" / "Order" which
      // don't match the DB field names — map them here.
      getValue: (r, c) => {
        if (c.label === 'Date')  return (r.order_created_at || '').slice(0,16).replace('T',' ');
        if (c.label === 'Order') return (r.order_id || '').replace('gid://shopify/Order/','');
        // undefined = fall through to the default key-based lookup.
      },
    });
  };
  // KPI cards click → filter to that tier (both Meta and Google rows share
  // the same handler; card.dataset.tier carries the tier key already)
  const _tierClick = e => {
    const card = e.target.closest('.kpi'); if (!card) return;
    document.getElementById('aiTierSel').value = card.dataset.tier;
    aiPage = 0; aiRenderTable();
  };
  document.getElementById('aiTierCards'      ).addEventListener('click', _tierClick);
  document.getElementById('aiTierCardsGoogle').addEventListener('click', _tierClick);

  // Meta / Google tier row toggle — swaps the KPI cascade AND scopes the
  // table + source filter to the same channel, so what you see in the
  // table always matches the cascade you're looking at above.
  document.getElementById('aiTierModeToggle').addEventListener('click', e => {
    const btn = e.target.closest('.lt-btn'); if (!btn) return;
    aiTierMode = btn.dataset.mode === 'google' ? 'google' : 'meta';
    document.querySelectorAll('#aiTierModeToggle .lt-btn').forEach(b => {
      b.classList.toggle('active', b.dataset.mode === aiTierMode);
    });
    document.getElementById('aiTierCards'      ).style.display = (aiTierMode === 'meta'  ) ? 'grid' : 'none';
    document.getElementById('aiTierCardsGoogle').style.display = (aiTierMode === 'google') ? 'grid' : 'none';

    // Rescope utm_source multi-select to the mode's channel so the table
    // only shows orders that could plausibly match the visible cascade.
    aiUtmSourceSel.clear();
    for (const r of aiOrders){
      const key = aiSourceKey(r);
      if (aiChannel(key) === aiTierMode) aiUtmSourceSel.add(key);
    }
    // Any prior tier filter is now stale (Meta step selected while
    // switching to Google, or vice versa) — clear it back to "All".
    document.getElementById('aiTierSel').value = '';

    aiRenderSourceMs(aiBuildSourceCounts());
    aiRenderChannels();
    aiRenderKpis();
    aiPage = 0;
    aiRenderTable();
  });
  // Channel KPI cards → apply the channel as a utm_source filter (pushes
  // every source that classifies into that channel into the multi-select)
  // AND open the drilldown below.  Clicking the same card again closes
  // the drilldown and clears the filter (toggle).
  document.getElementById('aiChannelRow').addEventListener('click', e => {
    const card = e.target.closest('.kpi'); if (!card) return;
    const channel = card.dataset.channel;
    if (aiOpenChannel === channel){
      // Same card clicked again → clear both filter modes + close drill
      aiUtmSourceSel.clear();
      aiChannelFilter = '';
      aiCloseChannelDrill();
    } else {
      aiUtmSourceSel.clear();
      aiChannelFilter = '';
      if (channel === 'organic_ig'){
        // Row-level channel filter — the (source, medium, content) triple
        // can't be expressed via aiUtmSourceSel alone since source='ig'
        // by itself would also let paid Meta rows through.
        aiChannelFilter = 'organic_ig';
      } else if (channel !== 'all'){
        // Source-key filter — good enough for channels defined by
        // utm_source alone.
        for (const r of aiOrders){
          const key = aiSourceKey(r);
          if (aiChannel(key, r) === channel) aiUtmSourceSel.add(key);
        }
      }
      aiRenderChannelDrill(channel);
    }
    // Repaint the utm_source multi-select popover so the checkboxes and
    // label reflect the new selection
    aiRenderSourceMs(aiBuildSourceCounts());
    aiPage = 0;
    aiRenderTable();
  });
  document.getElementById('aiChannelDrillClose').addEventListener('click', () => {
    aiUtmSourceSel.clear();
    aiChannelFilter = '';
    aiCloseChannelDrill();
    aiRenderSourceMs(aiBuildSourceCounts());
    aiPage = 0;
    aiRenderTable();
  });
  // # / % toggle — swaps KPI card primary value with the chip
  document.getElementById('aiDisplayToggle').addEventListener('click', e => {
    const btn = e.target.closest('.lt-btn'); if (!btn) return;
    aiDisplayMode = btn.dataset.mode === 'pct' ? 'pct' : 'count';
    document.querySelectorAll('#aiDisplayToggle .lt-btn').forEach(b => {
      b.classList.toggle('active', b.dataset.mode === aiDisplayMode);
    });
    aiRenderChannels();
    aiRenderKpis();
  });
}

/* Group ad rows by ad_name, adset, or campaign — sum numeric fields, keep most-recent ad meta */
function aeGroupBy(rows, key){
  if (key === 'ad') return rows;
  // adset groups on adset_id (stable) + label with adset_name; ad_name / campaign
  // still key off their respective name fields for back-compat.
  const groupKey = (key === 'ad_name')  ? 'ad_name'
                 : (key === 'adset')    ? 'adset_id'
                 :                        'campaign_name';
  const groups = {};
  for (const r of rows){
    const k = (r[groupKey] || '').toString().trim();
    if (!k) continue;
    if (!groups[k]) groups[k] = {
      ...r, _members:1, _adIds:new Set([r.ad_id]),
      impressions:0, reach:0, amount_spent:0,
      ftewv_count:0, ncp_count:0, conv_value:0, purchases:0,
      shopify_orders:0, shopify_sales:0,
    };
    const g = groups[k];
    g._members += 1; if (r.ad_id) g._adIds.add(r.ad_id);
    g.impressions    += (+r.impressions    || 0);
    g.reach          += (+r.reach          || 0);
    g.amount_spent   += (+r.amount_spent   || 0);
    g.ftewv_count    += (+r.ftewv_count    || 0);
    g.ncp_count      += (+r.ncp_count      || 0);
    g.conv_value     += (+r.conv_value     || 0);
    g.purchases      += (+r.purchases      || 0);
    g.shopify_orders += (+r.shopify_orders || 0);
    g.shopify_sales  += (+r.shopify_sales  || 0);
    // Take most-recent created date for the group
    if (r.ad_created && (!g.ad_created || r.ad_created > g.ad_created)) g.ad_created = r.ad_created;
  }
  // Derive frequency, ROAS, cost-per metrics from the aggregated sums.
  // For adset / campaign group levels, OVERLAY shopify_orders + shopify_sales
  // from the authoritative RPC rollups (aeShopifyByAdset / aeShopifyByCampaign)
  // instead of using the sums from constituent ads — those miss orders
  // resolved at the adset/campaign scope (Step 3/4) that didn't pin to an
  // ad_id inside the current AE partition.
  return Object.values(groups).map(g => {
    const cnt = g._adIds.size;
    const label = key === 'ad_name'  ? g.ad_name + (cnt > 1 ? '  (×' + cnt + ' ad_ids)' : '')
                : key === 'adset'    ? (g.adset_name || '(adset)') + (cnt > 1 ? '  (×' + cnt + ' ads)' : '')
                :                      '[Campaign]';
    // Authoritative Shopify overlay (adset/campaign only). Uses RPC rollup
    // when available; falls back to the summed value otherwise (e.g. before
    // the RPC has resolved on first paint).
    let shopOrders = g.shopify_orders;
    let shopSales  = g.shopify_sales;
    if (key === 'adset'){
      const rr = aeShopifyByAdset[g.adset_id];
      if (rr){ shopOrders = rr.orders; shopSales = rr.sales; }
    } else if (key === 'campaign'){
      const rr = aeShopifyByCampaign[g.campaign_name];
      if (rr){ shopOrders = rr.orders; shopSales = rr.sales; }
    }
    // Representative ad_id — pick the first constituent ad that actually has
    // a thumbnail (falls back to the first ad_id if none do). Used for the
    // inline thumbnail cell + drawer preview so grouped rows don't render
    // as empty placeholders.
    let repAdId = null;
    for (const id of g._adIds){
      if (!id) continue;
      if (typeof thumbsByAdId !== 'undefined' && thumbsByAdId[id]){ repAdId = id; break; }
      if (!repAdId) repAdId = id;   // fallback
    }
    const conv = +g.conv_value || 0;
    return {
      ...g,
      ad_name:        label,
      campaign_name:  g.campaign_name || '',
      ad_id:          cnt === 1 ? [...g._adIds][0] : (cnt + ' ad_ids'),
      _repAdId:       repAdId,
      frequency:      g.reach > 0 ? g.impressions / g.reach : 0,
      roas_ma:        g.amount_spent > 0 ? g.conv_value / g.amount_spent : 0,
      cost_per_ftewv: g.ftewv_count  > 0 ? g.amount_spent / g.ftewv_count : 0,
      cost_per_ncp:   g.ncp_count    > 0 ? g.amount_spent / g.ncp_count   : 0,
      shopify_orders: shopOrders,
      shopify_sales:  shopSales,
      shopify_roas:   g.amount_spent > 0 ? shopSales / g.amount_spent : 0,
      meta_shop_diff_pct: conv > 0 ? ((shopSales - conv) / conv) * 100 : null,
    };
  });
}

/* Canonical thresholds. F4=12 is the new working standard (was 25 in old refresh).
   Changes via the Ads Analyse view propagate through aeApplyCurrentThresholds(). */
const AE_DEFAULTS = {f1:50000, f2:3, f3:525, f4:12};

function aeReadThresholds(){
  return {
    f1: +(document.getElementById('aeF1')?.value || AE_DEFAULTS.f1),
    f2: +(document.getElementById('aeF2')?.value || AE_DEFAULTS.f2),
    f3: +(document.getElementById('aeF3')?.value || AE_DEFAULTS.f3),
    f4: +(document.getElementById('aeF4')?.value || AE_DEFAULTS.f4),
  };
}

function aeCategorise(r, t){
  const p1 = (+r.impressions || 0) >= t.f1;
  const p2 = (+r.roas_ma     || 0) >= t.f2;
  const p3 = (+r.cost_per_ncp   || 0) > 0 && (+r.cost_per_ncp   || Infinity) <= t.f3;
  const p4 = (+r.cost_per_ftewv || 0) > 0 && (+r.cost_per_ftewv || Infinity) <= t.f4;
  let cat;
  if      (p1 && (p2 || p3) && p4) cat = 'Incremental Winner';
  else if (p1 && (p2 || p3))       cat = 'Winner';
  else if (p1 && p4)               cat = 'P0 analysis';
  else if (p1)                     cat = 'P1 analysis';
  else if (p2)                     cat = 'P2 analysis';
  else                             cat = 'Discarded';
  // Same 14-day buffer used by CT — ads still within their first two
  // weeks from ad_created show as Result Awaited instead of Discarded.
  if (cat === 'Discarded' && r.ad_created){
    const created = new Date(r.ad_created);
    if (!isNaN(created)){
      const daysSince = (Date.now() - created.getTime()) / 86400000;
      if (daysSince < CT_BUFFER_DAYS) cat = 'Result Awaited';
    }
  }
  return {p1, p2, p3, p4, category: cat};
}

/* Mutate allAds in place — every row's .category is overwritten with the
   freshly-computed value. This makes Creative Testing's KPIs use the same
   thresholds as Ads Analyse. The original DB-baked category is preserved
   under .db_category for the cross-check tool. */
function aeApplyCurrentThresholds(){
  const t = aeReadThresholds();
  for (const r of allAds){
    if (r.db_category === undefined) r.db_category = r.category;   // snapshot once
    const {p1,p2,p3,p4,category} = aeCategorise(r, t);
    r.f1_pass = p1; r.f2_pass = p2; r.f3_pass = p3; r.f4_pass = p4;
    r.category = category;
  }
}

function aeRecategorise(rows){
  const t = aeReadThresholds();
  return rows.map(r => ({...r, ...aeCategorise(r, t)}));
}

function aeFiltered(){
  const acct       = document.getElementById('aeAcct').value;
  const status     = document.getElementById('aeStatus').value;
  const groupBy    = document.getElementById('aeGroupBy').value;
  const dateField  = document.getElementById('aeDateField').value || '__delivery__';
  const dFromStr   = document.getElementById('aeDateFrom').value;
  const dToStr     = document.getElementById('aeDateTo').value;
  const dFrom = dFromStr ? new Date(dFromStr + 'T00:00:00') : null;
  const dTo   = dToStr   ? new Date(dToStr   + 'T23:59:59') : null;

  let rows = aeRecategorise(allAds);
  // Historic mode partitions the ad universe by ad_created: Historic Ads
  // Analysis shows ads created before HISTORIC_CUTOFF (lifetime pre-2025),
  // the regular Ads Analyse shows ads created on/after the cutoff. Ads
  // without a created_date are treated as recent (kept in current, hidden
  // in historic).
  const _cutoff = new Date(HISTORIC_CUTOFF + 'T00:00:00');
  rows = rows.filter(r => {
    if (!r.ad_created) return !historicMode.ae;
    const d = new Date(r.ad_created);
    return historicMode.ae ? d < _cutoff : d >= _cutoff;
  });
  if (acct)   rows = rows.filter(r => (r.account_name || '') === acct);
  if (status) rows = rows.filter(r => (r.ad_status || '').toUpperCase() === status);
  // Show every category (including Discarded) by default; a KPI card click
  // narrows to just that category via aeSelectedCat.
  if (aeSelectedCat)  rows = rows.filter(r => r.category === aeSelectedCat);
  // Date-range filter
  if (dFrom || dTo){
    if (dateField === '__delivery__'){
      // Match "ad delivered in this window" — an ad_id is included iff it
      // had at least one impressioned daily row in primary_table or
      // backfill_table inside the window. The Set is pre-fetched on
      // every date-range change; while it's loading we pass through so
      // the user isn't looking at an empty table.
      if (aeDeliverySet){
        rows = rows.filter(r => aeDeliverySet.has(r.ad_id));
      }
    } else {
      // Legacy per-field filter (Ad Created / First Seen / etc.)
      rows = rows.filter(r => {
        const v = r[dateField]; if (!v) return false;
        const d = new Date(v);  if (isNaN(d)) return false;
        if (dFrom && d < dFrom) return false;
        if (dTo   && d > dTo  ) return false;
        return true;
      });
    }
  }
  // Text Multi-Filter (inline card) — always safe to apply pre-window
  // because name/status/category don't change with the date window.
  // Numeric filters (drawer) are DEFERRED to renderAE / CSV export, so
  // they run AFTER aeApplyWindow against on-screen (windowed) values.
  const textRules = aeRules.filter(r => String(r.value ?? '').trim());
  if (textRules.length){
    rows = rows.filter(r => textRules.every(rule => aeMfMatch(r, rule)));
  }
  // Optional Group By
  rows = aeGroupBy(rows, groupBy);
  // NOTE: sort is intentionally NOT applied here anymore.  It's now the
  // caller's job to sort AFTER any windowed-metric overrides (aeApplyWindow),
  // otherwise the sort order is based on lifetime values but the display
  // shows windowed values — looks jumbled to the user.  See _aeSortRows().
  return rows;
}

// Category rank for the Category column — sorts by real priority (best →
// worst) instead of alphabetical.  Anything not in the map falls to the end.
const AE_CATEGORY_RANK = {
  'Incremental Winner': 0,
  'Winner':             1,
  'P0 analysis':        2,
  'P1 analysis':        3,
  'P2 analysis':        4,
  'Result Awaited':     5,
  'Discarded':          6,
};

// Robust sort for the AE table.  Handles PostgREST numeric-strings, dates,
// null/undefined sinking, booleans, category ranks, and mixed-type columns.
function _aeSortRows(rows, key, dir){
  if (!key) return rows;
  const DATE_KEYS = new Set(['ad_created','first_seen_date','date_of_result',
                             'reporting_ends','reporting_starts','date_target_imp_achieved']);
  const d = dir === 'asc' ? 1 : -1;
  rows.sort((a, b) => {
    let av = a[key], bv = b[key];
    // 1. Nulls always sink
    const aNull = av === null || av === undefined || av === '';
    const bNull = bv === null || bv === undefined || bv === '';
    if (aNull && bNull) return 0;
    if (aNull) return 1;
    if (bNull) return -1;
    // 2. Category column — rank-based (Incremental Winner first, Discarded last)
    if (key === 'category'){
      const ar = AE_CATEGORY_RANK[av] ?? 99;
      const br = AE_CATEGORY_RANK[bv] ?? 99;
      return d * (ar - br);
    }
    // 3. Date columns — parse to ms epoch
    if (DATE_KEYS.has(key)){
      const ad = new Date(av).getTime();
      const bd = new Date(bv).getTime();
      const an = isNaN(ad) ? -Infinity : ad;
      const bn = isNaN(bd) ? -Infinity : bd;
      return d * (an - bn);
    }
    // 4. Booleans — false < true (numeric coerce)
    if (typeof av === 'boolean' || typeof bv === 'boolean'){
      return d * ((av ? 1 : 0) - (bv ? 1 : 0));
    }
    // 5. Numeric coercion — handles "5.75" string same as 5.75 number
    const an = typeof av === 'number' ? av : parseFloat(av);
    const bn = typeof bv === 'number' ? bv : parseFloat(bv);
    const aNum = !isNaN(an), bNum = !isNaN(bn);
    if (aNum && bNum) return d * (an - bn);
    // 6. Mixed numeric + non-numeric → non-numeric sinks
    if (aNum) return -1;
    if (bNum) return 1;
    // 7. Both non-numeric — locale-aware string compare
    return d * String(av).localeCompare(String(bv));
  });
  return rows;
}

const CAT_CLASS = {
  'Incremental Winner':'cat-iw',
  'Winner':'cat-winner',
  'P0 analysis':'cat-priority',
  'P1 analysis':'cat-a1',
  'P2 analysis':'cat-a2',
  'Result Awaited':'cat-ra',
  'Discarded':'cat-disc',
};

/* Overlay windowed metrics onto an ae row when a date range is set.
 * Falls through to the row's lifetime values if the ad had no delivery
 * in the window. Category and F-flags stay lifetime-based on purpose —
 * they're the ad's overall verdict, not "was this ad a winner on Jun 29?" */
function aeApplyWindow(r){
  const w  = aeWindowMetricsByAdId[r.ad_id];
  const s  = aeWindowShopifyByAdId[r.ad_id];
  const rr = aeWindowReachByAdId[r.ad_id];
  if (!w && !s && !rr && !_aeWindowReachKey) return r;
  const out = Object.assign({}, r);
  if (w){
    Object.assign(out, {
      impressions:     w.impressions,
      reach:           w.reach,
      amount_spent:    w.amount_spent,
      frequency:       w.frequency,
      cost_per_1000:   w.cost_per_1000,
      ctr_pct:         w.ctr_pct,
      roas_ma:         w.roas_ma,
      cost_per_ftewv:  w.cost_per_ftewv,
      cost_per_ncp:    w.cost_per_ncp,
      conv_value:      w.conv_value,
      purchases:       w.purchases,
      link_clicks_raw: w.link_clicks_raw,
      ftewv_count:     w.ftewv_count,
      ncp_count:       w.ncp_count,
      _isWindowed:     true,
    });
  }
  // Shopify overlay: replace the lifetime shopify_orders / shopify_sales
  // with the windowed aggregate. When the window has no matched orders for
  // this ad, show 0 (not lifetime) so users see the honest windowed answer.
  if (aeWindowShopifyKeyIsActive()){
    out.shopify_orders = s ? s.orders : 0;
    out.shopify_sales  = s ? s.sales  : 0;
    // shopify_roas re-derives from the (possibly windowed) spend + sales
    const spend = +out.amount_spent || 0;
    out.shopify_roas = spend > 0 ? (out.shopify_sales / spend) : null;
  }
  // % gap between Meta-attributed conversion value and Shopify actual sales.
  // Negative = Meta over-reports (view-through attribution inflation).
  // Positive = Shopify sales exceed Meta's attribution (usually late-window
  // orders or non-Meta assist). Null when Meta reported 0.
  {
    const _conv = +out.conv_value    || 0;
    const _shop = +out.shopify_sales || 0;
    out.meta_shop_diff_pct = _conv > 0 ? ((_shop - _conv) / _conv) * 100 : null;
  }
  // Reach overlay — reads from get_reach_by_window RPC. Aligns with the
  // reference incremental-logic sheet: Incr Reach for a window = SUM of
  // daily incremental reach across days = SUM(daily_reach) = rr.reach_sum.
  // (Old first-vs-last snapshot delta didn't represent user coverage — an
  // ad whose daily reach was 1,000 on day 1 AND day 30 would show incr=0
  // even though it reached ~30k people cumulatively over the window.)
  //   Prev Reach     = reach_first    (daily reach on first delivering day)
  //   Latest Reach   = reach_last     (daily reach on last  delivering day)
  //   Incr. Reach    = reach_sum      (sum of daily reach across window)
  //   Cost / 1k Incr = spend_sum × 1000 / reach_sum
  if (_aeWindowReachKey && rr){
    out.previous_reach    = rr.reach_first;
    out.latest_reach      = rr.reach_last;
    out.incremental_reach = Math.max(0, rr.reach_sum || 0);
    out.cost_per_1000_incremental_reach = rr.reach_sum > 0
      ? (rr.spend_sum * 1000) / rr.reach_sum
      : null;
  }
  // If `rr` is missing (ad has no reach in the current window, OR the
  // fetch hasn't resolved yet on the first paint) fall through and keep
  // out.previous_reach / latest_reach / incremental_reach from the
  // ae_reach_recent merge that Object.assign copied at line 4981.
  // Clamp the fallback incremental_reach at 0 — the base view can emit
  // negatives when Meta's per-day unique reach dips, but users read a
  // negative as "reach was taken back," which never happens.
  if (typeof out.incremental_reach === 'number' && out.incremental_reach < 0){
    out.incremental_reach = 0;
    out.cost_per_1000_incremental_reach = null;
  }
  return out;
}
// True when a real date range is active in AE and the windowed Shopify
// fetch has completed. Used by aeApplyWindow to know whether to overlay
// windowed Shopify totals or leave the lifetime values alone.
function aeWindowShopifyKeyIsActive(){
  return !!_aeWindowShopifyKey;
}
/* ─────────────────────────────────────────────────────────────
   HISTORIC INCREMENTAL REACH  (Ads Analyse → Historic Reach tab)
   Pulls daily rows from primary_table + backfill_table, aggregates
   reach at the campaign/adset level PER DAY, and computes incremental
   reach on the aggregated series. Rationale for group-first aggregation:
   two ads in one campaign (A: 100→10, B: 10→10) would net to -90 at
   the ad level, cancelling out B's steady contribution. Group-first
   preserves it — campaign D1 = 110, D-last = 20, incr = -90 correctly
   reflects the campaign's aggregate user coverage instead of averaging
   over ad-level noise.
   ───────────────────────────────────────────────────────────── */
let hreachState = {
  groupBy:'campaign_name', from:'', to:'',
  search:'', sortKey:'incr_reach', sortDir:'desc',
  rows:[],
};

function _hreachApplyPreset(p){
  const today = new Date(); today.setHours(0,0,0,0);
  const iso = d => d.toISOString().slice(0,10);
  let from = null;
  if (p === 'last7')      { from = new Date(today); from.setDate(today.getDate()-6); }
  else if (p === 'last30'){ from = new Date(today); from.setDate(today.getDate()-29); }
  else if (p === 'last90'){ from = new Date(today); from.setDate(today.getDate()-89); }
  else return;
  document.getElementById('hreachDateFrom').value = iso(from);
  document.getElementById('hreachDateTo').value   = iso(today);
}

async function _hreachFetch(from, to){
  // Pull daily rows for the window. primary_table covers the recent
  // 15-day sync window; backfill_table has the older historic depth.
  // We union both server-side via two parallel range queries and merge
  // client-side (same ad/date rows can exist in both; primary wins).
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   Prefer:'count=none'};
  const cols = 'ad_id,date,campaign_name,adset_name,reach,amount_spent_inr';
  const fetchAll = async (tbl) => {
    let out = [], offset = 0, BATCH = 5000;
    while (true){
      const url = SUPABASE_URL + '/rest/v1/' + tbl + '?select=' + cols +
                  '&date=gte.' + from + '&date=lte.' + to +
                  '&order=date.asc&limit=' + BATCH + '&offset=' + offset;
      const r = await fetch(url, {headers});
      if (!r.ok) throw new Error(tbl + ' HTTP ' + r.status);
      const j = await r.json();
      if (!Array.isArray(j) || !j.length) break;
      out = out.concat(j);
      if (j.length < BATCH) break;
      offset += BATCH;
    }
    return out;
  };
  const [prim, back] = await Promise.all([
    fetchAll('primary_table').catch(() => []),
    fetchAll('backfill_table').catch(() => []),
  ]);
  // Merge: primary_table takes precedence for overlapping (ad_id, date)
  // because it's the freshest sync.
  const key = r => (r.ad_id || '') + '|' + (r.date || '');
  const map = new Map();
  for (const r of back) map.set(key(r), r);
  for (const r of prim) map.set(key(r), r);
  return Array.from(map.values());
}

function _hreachAggregate(rows, groupBy){
  // Two-level aggregation:
  //   1. Sum reach + spend by (group_key, date) — this is where the
  //      "attribution to steady ads" comes from.  Every ad in the group
  //      contributes its daily reach to the group's daily total.
  //   2. From those per-day totals, extract first-day, last-day, peak,
  //      total-reach-sum, total-spend, day count, unique ads.
  const perDay = new Map();          // {grp|date -> {reach, spend, ads:Set}}
  const perGrpAds = new Map();       // {grp -> Set<ad_id>}
  for (const r of rows){
    const grp = (r[groupBy] || '(none)').trim() || '(none)';
    const d = (r.date || '').slice(0,10);
    if (!d) continue;
    const k = grp + '|' + d;
    let bucket = perDay.get(k);
    if (!bucket){ bucket = {reach:0, spend:0, ads:new Set()}; perDay.set(k, bucket); }
    bucket.reach += (+r.reach || 0);
    bucket.spend += (+r.amount_spent_inr || 0);
    if (r.ad_id) bucket.ads.add(r.ad_id);
    let ga = perGrpAds.get(grp);
    if (!ga){ ga = new Set(); perGrpAds.set(grp, ga); }
    if (r.ad_id) ga.add(r.ad_id);
  }
  // Rebucket into per-group timelines.
  const byGrp = new Map();
  for (const [k, v] of perDay.entries()){
    const [grp, d] = k.split('|');
    let series = byGrp.get(grp);
    if (!series){ series = []; byGrp.set(grp, series); }
    series.push({date:d, reach:v.reach, spend:v.spend});
  }
  const out = [];
  for (const [grp, series] of byGrp.entries()){
    series.sort((a,b) => a.date < b.date ? -1 : 1);
    const first = series[0], last = series[series.length-1];
    let peak = 0, totalReach = 0, totalSpend = 0;
    for (const s of series){
      if (s.reach > peak) peak = s.reach;
      totalReach += s.reach;
      totalSpend += s.spend;
    }
    // Reach is a cumulative unique count and can never regress — clamp
    // the (last - first) delta at 0 so a mid-window dip in Meta's daily
    // unique-reach number doesn't render as a nonsensical negative.
    const incr = Math.max(0, (last?.reach || 0) - (first?.reach || 0));
    const cpk  = incr > 0 ? (totalSpend / incr) * 1000 : null;
    out.push({
      grp, n_ads:(perGrpAds.get(grp)?.size || 0),
      days: series.length,
      first_reach: first?.reach || 0,
      last_reach : last?.reach  || 0,
      peak_reach : peak,
      total_reach: totalReach,
      spend      : totalSpend,
      incr_reach : incr,
      cpk        : cpk,
    });
  }
  return out;
}

function _hreachRender(){
  const q = (hreachState.search || '').trim().toLowerCase();
  let rows = hreachState.rows;
  if (q) rows = rows.filter(r => r.grp.toLowerCase().includes(q));
  const dir = hreachState.sortDir === 'asc' ? 1 : -1;
  const k = hreachState.sortKey;
  rows = rows.slice().sort((a,b) => {
    const av = a[k], bv = b[k];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    if (typeof av === 'string') return dir * av.localeCompare(bv);
    return dir * (av - bv);
  });
  const body = document.getElementById('hreachBody');
  if (!rows.length){
    body.innerHTML = '<tr><td colspan="10" style="padding:32px;text-align:center;color:var(--text-tertiary)">No groups match the current filter.</td></tr>';
    return;
  }
  body.innerHTML = rows.slice(0, 1000).map(r => {
    // incr_reach is clamped ≥ 0 upstream — always render as a positive delta
    return '<tr>'+
      '<td style="max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+r.grp.replace(/"/g,'&quot;')+'">'+r.grp+'</td>'+
      '<td class="num">'+fmtInt(r.n_ads)+'</td>'+
      '<td class="num">'+fmtInt(r.days)+'</td>'+
      '<td class="num">'+fmtInt(r.first_reach)+'</td>'+
      '<td class="num">'+fmtInt(r.last_reach)+'</td>'+
      '<td class="num">'+fmtInt(r.peak_reach)+'</td>'+
      '<td class="num" style="color:var(--success-text);font-weight:700">'+
        '+'+fmtInt(r.incr_reach)+'</td>'+
      '<td class="num">'+fmtInt(r.total_reach)+'</td>'+
      '<td class="num">'+fmtRs(r.spend)+'</td>'+
      '<td class="num">'+(r.cpk != null ? fmtRs(r.cpk) : '—')+'</td>'+
    '</tr>';
  }).join('');
}

async function _hreachApply(){
  const from = document.getElementById('hreachDateFrom').value;
  const to   = document.getElementById('hreachDateTo').value;
  if (!from || !to){
    document.getElementById('hreachStatus').textContent = 'Pick a date range first.';
    return;
  }
  hreachState.from = from; hreachState.to = to;
  const status = document.getElementById('hreachStatus');
  status.innerHTML = 'Fetching daily rows from primary_table + backfill_table <span class="spinner"></span>';
  const t0 = performance.now();
  try {
    const rows = await _hreachFetch(from, to);
    const agg  = _hreachAggregate(rows, hreachState.groupBy);
    hreachState.rows = agg;
    const dt = ((performance.now()-t0)/1000).toFixed(1);
    status.innerHTML = 'Aggregated <b>'+fmtInt(rows.length)+'</b> daily rows into <b>'+
      fmtInt(agg.length)+'</b> '+
      (hreachState.groupBy === 'campaign_name' ? 'campaigns' : 'adsets') +
      ' · '+dt+'s';
    _hreachRender();
  } catch (e){
    status.textContent = 'Error: ' + (e.message || e);
    hreachState.rows = [];
    _hreachRender();
  }
}

// The Historic Reach view auto-loads with the default (last-30d, Campaign)
// window the first time the user opens it — wire that from the sidebar's
// view-switch below. No manual "Apply" is required for the first open.
document.getElementById('hreachGroupBy').addEventListener('click', e => {
  const btn = e.target.closest('.lt-btn'); if (!btn) return;
  document.querySelectorAll('#hreachGroupBy .lt-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  hreachState.groupBy = btn.dataset.g;
  // Re-aggregate the last fetched daily rows instead of hitting the network
  // again — grouping is a pure client transformation.
  if (hreachState.rows.length){
    // We don't have the raw daily rows cached at the group level, so re-fetch.
    // Cost is small; the user changes group-by rarely mid-window.
    _hreachApply();
  }
});
document.querySelector('#view-hreach .preset-row').addEventListener('click', e => {
  const btn = e.target.closest('.preset'); if (!btn) return;
  document.querySelectorAll('#view-hreach .preset-row .preset').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  _hreachApplyPreset(btn.dataset.p);
});
document.getElementById('hreachApply').addEventListener('click', _hreachApply);
document.getElementById('hreachSearch').addEventListener('input', e => {
  hreachState.search = e.target.value;
  _hreachRender();
});
document.querySelectorAll('#hreachTbl thead th').forEach(th => {
  th.addEventListener('click', () => {
    const k = th.dataset.hrsort; if (!k) return;
    if (hreachState.sortKey === k) hreachState.sortDir = hreachState.sortDir === 'asc' ? 'desc' : 'asc';
    else { hreachState.sortKey = k; hreachState.sortDir = 'desc'; }
    _hreachRender();
  });
});
_hreachApplyPreset('last30');   // seed the inputs

/* ────────────────────────────────────────────────────────────────────
   INCREMENTAL ANALYSIS (post-2025) — Campaign + Adset reach deltas
   scoped to date >= HISTORIC_CUTOFF. Fetches primary_table +
   backfill_table once, dedups on (ad_id, date) with primary winning
   (matches the ae_daily_agg_mat logic verified via SQL audit), then
   aggregates the same daily rows into BOTH campaign-level and
   adset-level tables simultaneously.
   ──────────────────────────────────────────────────────────────────── */
let ireachState = {
  from:'', to:'',
  search:'',
  status:'',                    // '' | 'ACTIVE' | 'PAUSED' | ... — filters groups by ad_status
  scope:'camp',                 // 'account' | 'camp' | 'adset' — which table is visible
  account: { sortKey:'incr_reach', sortDir:'desc', rows:[] },
  camp:    { sortKey:'incr_reach', sortDir:'desc', rows:[] },
  adset:   { sortKey:'incr_reach', sortDir:'desc', rows:[] },
  audit: null,
};

function _ireachApplyPreset(p){
  const today = new Date(); today.setHours(0,0,0,0);
  const iso = d => d.getFullYear() + '-' +
                   String(d.getMonth()+1).padStart(2,'0') + '-' +
                   String(d.getDate()).padStart(2,'0');
  const cutoff = new Date(HISTORIC_CUTOFF + 'T00:00:00');
  let from = null;
  if (p === 'last7')       { from = new Date(today); from.setDate(today.getDate()-6); }
  else if (p === 'last30') { from = new Date(today); from.setDate(today.getDate()-29); }
  else if (p === 'last90') { from = new Date(today); from.setDate(today.getDate()-89); }
  else if (p === 'ytd')    { from = cutoff; }
  else return;
  if (from < cutoff) from = cutoff;
  document.getElementById('ireachDateFrom').value = iso(from);
  document.getElementById('ireachDateTo').value   = iso(today);
}

// Fetch pre-aggregated campaign / adset daily rows from the server-side
// matviews (public.ireach_campaign_daily / public.ireach_adset_daily).
// These are ~18k / ~54k rows total — tiny compared to the ~450k raw
// daily-ad rows the old client-side dedup path pulled. Server-side
// already handles the (ad_id, date) primary-wins dedup, so no audit
// counter to surface any more.
async function _ireachFetch(from, to){
  // Uses the sheet-model RPC get_ireach_incremental_analysis. Returns per-entity
  // rows already aggregated over the window:
  //   incr_reach = MAX(0, cum_at_end - cum_at_start_prev)
  //   cum_at_end = Meta cumulative unique reach at window end (from origin)
  //   cum_at_start_prev = same, at (window_start - 1). Diff = new users in window.
  //   spend / cost_per_1k_incr aggregated from the per-day *_daily tables.
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   'Content-Type':'application/json', Prefer:'count=none'};
  const call = async (level) => {
    const url = SUPABASE_URL + '/rest/v1/rpc/get_ireach_incremental_analysis';
    const body = JSON.stringify({from_date: from, to_date: to, level_arg: level});
    const r = await fetch(url, {method:'POST', headers, body});
    if (!r.ok) throw new Error('ireach RPC ' + level + ' HTTP ' + r.status);
    const j = await r.json();
    return Array.isArray(j) ? j : [];
  };
  const [account, camp, adset] = await Promise.all([
    call('account').catch(() => []),
    call('campaign').catch(() => []),
    call('adset').catch(() => []),
  ]);
  return {
    account, camp, adset,
    audit: {
      account_rows: account.length,
      camp_rows: camp.length,
      adset_rows: adset.length,
    },
  };
}

// Aggregate pre-aggregated daily group rows into a per-group summary.
// Consolidates by (grp, date) with SUM first — safe no-op for camp/adset
// scopes (one row per (grp, date) already) and correct for account scope
// (many campaigns per (account, date) → sum daily reach across them).
function _ireachAggregateFromDaily(rows, grpCol){
  // Phase 1: consolidate → one entry per (grp, date), summing metrics.
  const byGrpDate = new Map();
  for (const r of rows){
    const grp = ((r[grpCol] || '(none)') + '').trim() || '(none)';
    const d   = (r.date || '').slice(0, 10);
    if (!d) continue;
    const key = grp + '|' + d;
    let e = byGrpDate.get(key);
    if (!e){ e = {grp, date:d, reach:0, spend:0}; byGrpDate.set(key, e); }
    e.reach += +r.reach_daily || 0;
    e.spend += +r.spend_daily || 0;
  }
  // Phase 2: rebuild the per-group time series.
  const byGrp = new Map();
  for (const e of byGrpDate.values()){
    let series = byGrp.get(e.grp);
    if (!series){ series = []; byGrp.set(e.grp, series); }
    series.push({ date: e.date, reach: e.reach, spend: e.spend });
  }
  const out = [];
  for (const [grp, series] of byGrp.entries()){
    series.sort((a,b) => a.date < b.date ? -1 : 1);
    const first = series[0], last = series[series.length-1];
    let peak = 0, totalReach = 0, totalSpend = 0;
    for (const s of series){
      if (s.reach > peak) peak = s.reach;
      totalReach += s.reach;
      totalSpend += s.spend;
    }
    // Incremental reach per user's sheet model:
    //   cumulative[D]   = SUM(daily_reach) from origin to D
    //   incremental[D]  = cumulative[D] - cumulative[D-1] = daily_reach[D]
    //   window incr     = SUM(daily incrs) = SUM(daily_reach in window)
    // So incr_reach = totalReach (SUM of daily reach across the window),
    // and cost/1k = spend * 1000 / SUM(reach).
    const incr = totalReach;
    const cpk  = incr > 0 ? (totalSpend / incr) * 1000 : null;
    out.push({
      grp,
      n_ads: null,   // group-level insights don't expose ad count
      days:  series.length,
      first_reach: first?.reach || 0,
      last_reach : last?.reach  || 0,
      peak_reach : peak,
      total_reach: totalReach,
      spend      : totalSpend,
      incr_reach : incr,
      cpk        : cpk,
    });
  }
  return out;
}

// Render either the camp or adset table. Both share this fn — the
// scope arg is 'camp' or 'adset' and picks the state slot + DOM ids.
function _ireachRenderScope(scope){
  const st  = ireachState[scope];
  const q   = (ireachState.search || '').trim().toLowerCase();
  const dir = st.sortDir === 'asc' ? 1 : -1;
  const k   = st.sortKey;
  let rows = st.rows;
  if (q) rows = rows.filter(r => r.grp.toLowerCase().includes(q));
  // Ad Status filter — keeps only groups that have at least one ad in
  // allAds matching the selected status. Silently passes through when
  // allAds is empty (dashboard still warming up).
  const statusSel = (ireachState.status || '').toUpperCase();
  if (statusSel){
    const sets = _ireachStatusSets()[statusSel];
    if (sets){
      const setKey = scope === 'account' ? sets.accounts
                   : scope === 'camp'    ? sets.camps
                   :                       sets.adsets;
      rows = rows.filter(r => setKey.has(r.grp));
    }
  }
  rows = rows.slice().sort((a,b) => {
    const av = a[k], bv = b[k];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    if (typeof av === 'string') return dir * av.localeCompare(bv);
    return dir * (av - bv);
  });
  const bodyId  = scope === 'account' ? 'ireachBodyAccount'
                : scope === 'camp'    ? 'ireachBodyCamp'
                :                       'ireachBodyAdset';
  const countId = scope === 'account' ? 'ireachAccountCount'
                : scope === 'camp'    ? 'ireachCampCount'
                :                       'ireachAdsetCount';
  const noun    = scope === 'account' ? ' accounts'
                : scope === 'camp'    ? ' campaigns'
                :                       ' adsets';
  const body = document.getElementById(bodyId);
  const cntEl = document.getElementById(countId);
  if (cntEl) cntEl.textContent = fmtInt(rows.length) + noun;
  if (!rows.length){
    body.innerHTML = '<tr><td colspan="8" style="padding:24px;text-align:center;color:var(--text-tertiary)">No groups match the current filter.</td></tr>';
    return;
  }
  // No pagination — render every row so nothing is silently dropped.
  // 261 adsets × ~1KB HTML each is still ~260KB, well under any DOM
  // limits, and lets the user Ctrl-F any group name.
  body.innerHTML = rows.map(r => {
    // Sheet model:
    //   base_reach = cumulative(window_start − 1)
    //   cum_reach  = cumulative(window_end)
    //   incr_reach = MAX(0, cum_reach − base_reach)
    // cpk        = spend × 1000 / incr_reach
    return '<tr>'+
      '<td style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+r.grp.replace(/"/g,'&quot;')+'">'+r.grp+'</td>'+
      '<td class="num">'+fmtInt(r.n_ads)+'</td>'+
      '<td class="num">'+fmtInt(r.days)+'</td>'+
      '<td class="num">'+fmtInt(r.base_reach)+'</td>'+
      '<td class="num">'+fmtInt(r.cum_reach)+'</td>'+
      '<td class="num" style="color:var(--success-text);font-weight:700">'+
        '+'+fmtInt(r.incr_reach)+'</td>'+
      '<td class="num">'+fmtRs(r.spend)+'</td>'+
      '<td class="num">'+(r.cpk != null ? fmtRs(r.cpk) : '—')+'</td>'+
    '</tr>';
  }).join('');
}
function _ireachSetScope(scope){
  if (scope !== 'camp' && scope !== 'adset' && scope !== 'account') return;
  ireachState.scope = scope;
  // Show only the selected table
  const acc   = document.getElementById('ireachSecAccount');
  const camp  = document.getElementById('ireachSecCamp');
  const adset = document.getElementById('ireachSecAdset');
  if (acc)   acc.style.display   = scope === 'account' ? '' : 'none';
  if (camp)  camp.style.display  = scope === 'camp'    ? '' : 'none';
  if (adset) adset.style.display = scope === 'adset'   ? '' : 'none';
  // Sync the toggle buttons
  document.querySelectorAll('#ireachScope .lt-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.scope === scope));
}
function _ireachRender(){
  // Render all three tables (cheap; ~few hundred rows total) so switching
  // the scope toggle is instant — no re-render round-trip needed.
  _ireachRenderScope('account');
  _ireachRenderScope('camp');
  _ireachRenderScope('adset');
  _ireachRenderAudit();
  _ireachSetScope(ireachState.scope);
}
// Build sets of {accounts, camps, adsets} keyed by ad_status from allAds,
// so the status filter can decide which groups pass. Cached per allAds
// snapshot to keep _ireachRenderScope O(rows).
let _ireachStatusSetsCache = { v: null, byStatus: null };
function _ireachStatusSets(){
  const v = allAds && allAds.length ? allAds.length : 0;
  if (_ireachStatusSetsCache.v === v && _ireachStatusSetsCache.byStatus){
    return _ireachStatusSetsCache.byStatus;
  }
  const by = {};
  if (allAds && allAds.length){
    for (const a of allAds){
      const s = (a.ad_status || '').toUpperCase();
      if (!s) continue;
      let entry = by[s];
      if (!entry){ entry = by[s] = { accounts:new Set(), camps:new Set(), adsets:new Set() }; }
      if (a.account_name)  entry.accounts.add(a.account_name);
      if (a.campaign_name) entry.camps.add(a.campaign_name);
      if (a.adset_name)    entry.adsets.add(a.adset_name);
    }
  }
  _ireachStatusSetsCache = { v, byStatus: by };
  return by;
}
function _ireachRenderAudit(){
  const a = ireachState.audit;
  const el = document.getElementById('ireachAudit');
  if (!a || !el){ if (el) el.style.display = 'none'; return; }
  el.style.display = '';
  el.innerHTML =
    '<span>Source · <b>ireach_cumulative_daily</b> (Meta unique reach, growing time_range from ORIGIN=2025-01-01) via '+
    '<b>get_ireach_incremental_analysis()</b> RPC · returned '+
      fmtInt(a.account_rows)+' accounts, '+
      fmtInt(a.camp_rows)+' campaigns, '+
      fmtInt(a.adset_rows)+' adsets · spend joined from ireach_*_daily</span>';
}

async function _ireachApply(){
  let from = document.getElementById('ireachDateFrom').value;
  const to = document.getElementById('ireachDateTo').value;
  if (!from || !to){
    document.getElementById('ireachStatus').textContent = 'Pick a date range first.';
    return;
  }
  if (from < HISTORIC_CUTOFF){
    from = HISTORIC_CUTOFF;
    document.getElementById('ireachDateFrom').value = from;
  }
  ireachState.from = from; ireachState.to = to;
  const status = document.getElementById('ireachStatus');
  status.innerHTML = 'Fetching from ireach_campaign_daily + ireach_adset_daily <span class="spinner"></span>';
  const t0 = performance.now();
  try {
    const { account, camp, adset, audit } = await _ireachFetch(from, to);
    ireachState.audit = audit;
    // RPC rows are per-entity; adapt to the shape the renderer expects.
    // The 'grp' key is what the sort/search/status-filter code targets.
    const _shape = rows => rows.map(r => ({
      grp:              r.entity_name || '(unnamed)',
      entity_id:        r.entity_id,
      account_name:     r.account_name,
      n_ads:            null,                          // group-level Meta insights don't expose ad count
      days:             r.n_days || 0,
      base_reach:       +r.cum_at_start_prev || 0,     // sheet's "Base Reach D1+D2"
      cum_reach:        +r.cum_at_end || 0,            // sheet's "Cumulative Reach D1+D2+D3"
      incr_reach:       +r.incr_reach || 0,            // MAX(0, cum − base)
      spend:            +r.spend || 0,
      cpk:              r.cost_per_1k_incr == null ? null : +r.cost_per_1k_incr,
    }));
    ireachState.account.rows = _shape(account);
    ireachState.camp.rows    = _shape(camp);
    ireachState.adset.rows   = _shape(adset);
    const dt = ((performance.now()-t0)/1000).toFixed(1);
    status.innerHTML = 'Loaded <b>' +
      fmtInt(ireachState.account.rows.length)+'</b> accounts, <b>'+
      fmtInt(ireachState.camp.rows.length)+'</b> campaigns and <b>'+
      fmtInt(ireachState.adset.rows.length)+'</b> adsets · sheet model (cum(end)−cum(start−1)) · '+dt+'s';
    _ireachRender();
  } catch (e){
    status.textContent = 'Error: ' + (e.message || e);
    ireachState.camp.rows = ireachState.adset.rows = ireachState.account.rows = [];
    ireachState.audit = null;
    _ireachRender();
  }
}

document.querySelector('#view-ireach .preset-row').addEventListener('click', e => {
  const btn = e.target.closest('.preset'); if (!btn) return;
  document.querySelectorAll('#view-ireach .preset-row .preset').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  _ireachApplyPreset(btn.dataset.p);
});
// Account / Campaign / Adset toggle — flips visibility, no re-fetch (all
// three tables were populated during the same Apply pass).
document.getElementById('ireachScope')?.addEventListener('click', e => {
  const btn = e.target.closest('.lt-btn'); if (!btn) return;
  _ireachSetScope(btn.dataset.scope);
});
// Ad Status filter — applies client-side to all three scope tables.
document.getElementById('ireachAdStatus')?.addEventListener('change', e => {
  ireachState.status = e.target.value || '';
  _ireachRender();
});
document.getElementById('ireachApply').addEventListener('click', _ireachApply);
// Column-definitions modal for Incremental Analysis. Uses the same
// modal-card shell as #defModal so ESC-to-close and backdrop-click-to-close
// stay consistent with the rest of the dashboard's dialogs.
(function(){
  const modal = document.getElementById('ireachInfoModal');
  if (!modal) return;
  const open  = () => { modal.style.display = 'flex'; modal.setAttribute('aria-hidden','false'); };
  const close = () => { modal.style.display = 'none'; modal.setAttribute('aria-hidden','true'); };
  document.getElementById('ireachInfo')?.addEventListener('click', open);
  document.getElementById('ireachInfoClose')?.addEventListener('click', close);
  modal.addEventListener('click', e => { if (e.target === modal) close(); });
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape' && modal.style.display === 'flex') close();
  });
})();
document.getElementById('ireachSearch').addEventListener('input', e => {
  ireachState.search = e.target.value;
  _ireachRender();
});
// Sort click handler — one per table. Each <th> carries data-ir-tbl
// (camp/adset) + data-irsort (which key). Toggling asc/desc is scoped
// to that table's state so sorting Adset by Reach D1 doesn't disturb
// the Campaign table's current sort.
['ireachTblAccount','ireachTblCamp','ireachTblAdset'].forEach(tblId => {
  const tbl = document.getElementById(tblId);
  if (!tbl) return;
  tbl.querySelectorAll('thead th').forEach(th => {
    th.addEventListener('click', () => {
      const key   = th.dataset.irsort; if (!key) return;
      const scope = th.dataset.irTbl || 'camp';
      const st = ireachState[scope];
      if (st.sortKey === key) st.sortDir = st.sortDir === 'asc' ? 'desc' : 'asc';
      else { st.sortKey = key; st.sortDir = 'desc'; }
      _ireachRenderScope(scope);
    });
  });
});
_ireachApplyPreset('last30');   // seed the inputs on first script eval

function renderAE(){
  if (!allAds.length){
    // Data hasn't landed yet. Instead of silently bailing (and
    // leaving the KPIs stuck on "—"), show a loading state. rerender()
    // will call renderAE again once the ae_table_view fetch resolves.
    const footer = document.getElementById('aeFooter');
    if (footer) footer.textContent = 'Loading ae_table_view — filters will apply once the 15k-row fetch completes';
    return;
  }
  // Lazy-trigger the windowed reach fetch once — with no explicit range
  // it hits the RPC with the ad-lifetime bounds so the Prev/Latest/Incr
  // columns describe the ad's entire life instead of the last 2 days
  // from ae_reach_recent's snapshot. Fires once; the fetch cache-keys on
  // "from|to" so identical no-window renders don't re-hit the RPC.
  if (!_aeWindowReachKey){
    fetchAeWindowReach().then(() => renderAE()).catch(()=>{});
  }
  // Trigger overlay when ANY of the three windowed maps is loaded — metrics,
  // Shopify, or reach. Missing the reach check meant that when the user had
  // no explicit window but the lifetime-reach fallback populated
  // aeWindowReachByAdId, aeApplyWindow was skipped and the Prev/Latest/Incr
  // columns fell through to ae_reach_recent's stale 2-day snapshot.
  const hasWindow = Object.keys(aeWindowMetricsByAdId).length > 0
                    || aeWindowShopifyKeyIsActive()
                    || !!_aeWindowReachKey;
  // Apply window overrides FIRST, then run numeric Filters (drawer) against
  // the windowed values, then sort — so both filter and sort operate on the
  // values the user actually sees in the table.
  const _numRules = aeRulesNum.filter(rl => String(rl.value ?? '').trim());
  let _windowed = aeFiltered().map(r => hasWindow ? aeApplyWindow(r) : r);
  if (_numRules.length){
    _windowed = _windowed.filter(r => _numRules.every(rule => aeMfMatch(r, rule)));
  }
  const rows = _aeSortRows(_windowed, aeSortKey, aeSortDir);

  // KPIs honour the date range + account + status + multi-filter (but NOT
  // category / discarded toggle — so clicking a card still shows the count
  // for every category at once).
  const acct       = document.getElementById('aeAcct').value;
  const status     = document.getElementById('aeStatus').value;
  const dateField  = document.getElementById('aeDateField').value || '__delivery__';
  const dFromStr   = document.getElementById('aeDateFrom').value;
  const dToStr     = document.getElementById('aeDateTo').value;
  const dFrom = dFromStr ? new Date(dFromStr + 'T00:00:00') : null;
  const dTo   = dToStr   ? new Date(dToStr   + 'T23:59:59') : null;
  let cats = aeRecategorise(allAds);
  // Apply the SAME historic partition the table uses (line 5065 in aeFiltered)
  // — without this, KPI counts include ads outside the current/historic split
  // and disagree with the table row count (e.g. Winner KPI 1,132 vs table 722).
  const _catCutoff = new Date(HISTORIC_CUTOFF + 'T00:00:00');
  cats = cats.filter(r => {
    if (!r.ad_created) return !historicMode.ae;
    const d = new Date(r.ad_created);
    return historicMode.ae ? d < _catCutoff : d >= _catCutoff;
  });
  if (acct)   cats = cats.filter(r => (r.account_name || '') === acct);
  if (status) cats = cats.filter(r => (r.ad_status || '').toUpperCase() === status);
  if (dFrom || dTo){
    if (dateField === '__delivery__'){
      if (aeDeliverySet){
        cats = cats.filter(r => aeDeliverySet.has(r.ad_id));
      }
    } else {
      cats = cats.filter(r => {
        const v = r[dateField]; if (!v) return false;
        const d = new Date(v);  if (isNaN(d)) return false;
        if (dFrom && d < dFrom) return false;
        if (dTo   && d > dTo  ) return false;
        return true;
      });
    }
  }
  // Text rules (inline card) → apply on lifetime cats — name/status don't
  // move with the window.  Numeric rules (drawer) → overlay windowed metrics
  // first so "Spend ≥ X" matches the on-screen windowed value.
  const activeText = aeRules   .filter(rl => String(rl.value ?? '').trim());
  const activeNum  = aeRulesNum.filter(rl => String(rl.value ?? '').trim());
  if (activeText.length){
    cats = cats.filter(r => activeText.every(rule => aeMfMatch(r, rule)));
  }
  if (activeNum.length){
    cats = cats.map(r => hasWindow ? aeApplyWindow(r) : r)
               .filter(r => activeNum.every(rule => aeMfMatch(r, rule)));
  }
  // KPI card spend totals reflect window metrics when a range is set,
  // so "Winner spend: ₹5.9 Cr" becomes "Winner spend on 29/06: ₹X" the
  // moment the picker's date is picked.
  const hasWindowKPI = Object.keys(aeWindowMetricsByAdId).length > 0;
  const totalsByCat = {};
  cats.forEach(r => {
    const k = r.category;
    if (!totalsByCat[k]) totalsByCat[k] = {n:0, sp:0};
    totalsByCat[k].n  += 1;
    const spend = hasWindowKPI && aeWindowMetricsByAdId[r.ad_id]
      ? aeWindowMetricsByAdId[r.ad_id].amount_spent
      : (+r.amount_spent || 0);
    totalsByCat[k].sp += spend;
  });
  const setKPI = (id, name) => {
    const x = totalsByCat[name] || {n:0, sp:0};
    document.getElementById('aeKp-' + id     ).textContent = fmtInt(x.n);
    document.getElementById('aeKp-' + id + '-sp').textContent = fmtRs(x.sp);
  };
  setKPI('iw', 'Incremental Winner');
  setKPI('w',  'Winner');
  setKPI('pr', 'P0 analysis');
  setKPI('a1', 'P1 analysis');
  setKPI('a2', 'P2 analysis');
  setKPI('ra', 'Result Awaited');   // created-date-anchored; delivery date filter doesn't move it
  setKPI('d',  'Discarded');
  // Total row — sum of ALL KPI cards (Discarded is no longer hidden by
  // default, so the total matches the table row count directly).
  const _kpiTotalN = document.getElementById('aeKpiTotalN');
  if (_kpiTotalN){
    if (aeSelectedCat){
      const x = totalsByCat[aeSelectedCat] || {n:0};
      _kpiTotalN.textContent = fmtInt(x.n);
    } else {
      let n = 0;
      for (const v of Object.values(totalsByCat)) n += v.n;
      _kpiTotalN.textContent = fmtInt(n);
    }
  }

  // Reflect selected card visually
  document.querySelectorAll('#aeCats .kpi').forEach(card => {
    card.classList.toggle('cat-selected', card.dataset.cat === aeSelectedCat);
  });

  // Pagination
  const pageSize = +document.getElementById('aePageSize').value || 100;
  const totalRows = rows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  if (aePage >= totalPages) aePage = totalPages - 1;
  if (aePage < 0) aePage = 0;
  const offset = aePage * pageSize;
  const slice = rows.slice(offset, offset + pageSize);

  const tbody = document.querySelector('#aeMain tbody');
  const COLSPAN = document.querySelectorAll('#aeMain thead th').length;
  const dCell = v => '<td class="id-cell">' + (v ? String(v).slice(0,10) : '—') + '</td>';
  const fCell = v => {
    const truthy = v === true || v === 'Y' || v === 'y' || v === 1 || v === '1' || v === 'true';
    return '<td><span class="ae-flag '+(truthy?'y':'n')+'">'+(truthy?'Y':'N')+'</span></td>';
  };
  const numCell = (txt) => '<td class="num">' + txt + '</td>';
  const linkCell = (href, label) => href
    ? '<td><a class="ae-link" href="'+href+'" target="_blank" rel="noopener">'+label+'</a></td>'
    : '<td class="id-cell">—</td>';
  // Landing-page cell: strip protocol + query string, badge the URL type
  // (collection / product / custom page / homepage / other), show the
  // host + short path as monospace clickable text.
  const aeLandingCell = (url) => {
    if (!url) return '<td class="id-cell">—</td>';
    let host = '', path = '', badge = 'other', badgeLbl = 'Other';
    try {
      const u = new URL(url);
      host = u.hostname; path = u.pathname.replace(/\/+$/, '');
      if      (path.startsWith('/collections/')) { badge = 'coll';    badgeLbl = 'Collection'; }
      else if (path.startsWith('/products/'))    { badge = 'prod';    badgeLbl = 'Product'; }
      else if (path.startsWith('/pages/'))       { badge = 'page';    badgeLbl = 'Page'; }
      else if (path === '' || path === '/')      { badge = 'home';    badgeLbl = 'Homepage'; }
      else if (path.startsWith('/blogs/'))       { badge = 'blog';    badgeLbl = 'Blog'; }
    } catch { path = url.slice(0, 60); }
    let shortPath = path.length > 50 ? path.slice(0, 47) + '…' : path;
    return '<td class="ae-landing" title="'+url.replace(/"/g,'&quot;')+'">'+
             '<span class="lp-badge lp-'+badge+'">'+badgeLbl+'</span>'+
             '<a class="lp-url" href="'+url+'" target="_blank" rel="noopener">'+
               (host || '')+'<span class="lp-path">'+shortPath+'</span></a>'+
           '</td>';
  };
  const fmtPct = v => (v == null || v === '') ? '—' : (+v).toFixed(2) + '%';
  const fmtNum2 = v => (v == null || v === '') ? '—' : (+v).toFixed(2);
  const fmtNum3 = v => (v == null || v === '') ? '—' : (+v).toFixed(3);
  tbody.innerHTML = slice.map(r => {
    const cls = CAT_CLASS[r.category] || 'cat-disc';
    const status = (r.ad_status||'').toUpperCase();
    const statusCls = status === 'ACTIVE' ? 'active' : '';
    // Daily attribution: button + (optionally) override metric cells with daily totals
    const ck = aeDailyCacheKey(r.ad_id);
    const dailyState = aeDailyCache[ck];
    const isOpen = aeDailyOpenKey === ck;
    const dTot   = aeDailyTotalsFor(r.ad_id);
    const dailyBtn = r.ad_id
      ? '<button class="ae-attrib-btn '+(isOpen?'open':'')+'" type="button" data-ae-daily-ad="'+r.ad_id+'" data-ae-daily-ck="'+ck+'">'+
          (dailyState && dailyState.state === 'loading' && isOpen ? 'Loading…' : isOpen ? 'Hide daily' : 'Daily')+
        '</button>'
      : '<button class="ae-attrib-btn" type="button" disabled title="Ad-level only">—</button>';
    // When daily totals exist for this ad, override the metric cells with daily-derived values
    const impVal     = dTot ? fmtInt(dTot.impressions)        : fmtInt(r.impressions);
    const reachVal   = dTot ? fmtInt(dTot.reach)              : fmtInt(r.reach);
    const freqVal    = dTot ? (dTot.freq||0).toFixed(2)       : fmtNum2(r.frequency);
    const spendVal   = dTot ? fmtRs(dTot.spend)               : fmtRs(r.amount_spent);
    const cpcVal     = dTot ? fmtRs(dTot.linkClicks ? dTot.spend/dTot.linkClicks : 0) : fmtRs(r.cpc_link);
    const ctrVal     = dTot ? (dTot.ctr||0).toFixed(2)+'%'    : fmtPct(r.ctr_pct);
    const linkClkVal = dTot ? fmtInt(dTot.linkClicks)         : fmtInt(r.link_clicks_raw);
    const purchVal   = dTot ? (dTot.purchases||0).toFixed(2)  : fmtInt(r.purchases);
    const convVal    = dTot ? fmtRs(dTot.convValue)           : fmtRs(r.conv_value);
    const roasVal    = dTot ? (dTot.roas||0).toFixed(2)       : fmtRoas(r.roas_ma);
    const ftVal      = dTot ? fmtInt(dTot.ftewv)              : fmtInt(r.ftewv_count);
    const cFtVal     = dTot ? fmtRs(dTot.costFt)              : fmtRs(r.cost_per_ftewv);
    const ncpVal     = dTot ? fmtInt(dTot.ncp)                : fmtInt(r.ncp_count);
    const cNcpVal    = dTot ? fmtRs(dTot.costNcp)             : fmtRs(r.cost_per_ncp);
    // Thumbnail cell — Meta Graph creative.thumbnail_url, clickable to open drawer.
    // For grouped rows (adset/campaign) the ad_id is overridden to "N ad_ids",
    // so use _repAdId (representative ad chosen in aeGroupBy) for the lookup.
    const _thumbAdId = r._repAdId || r.ad_id;
    const thumbUrl = thumbUrlOf(_thumbAdId ? thumbsByAdId[_thumbAdId] : null);
    const thumbCell = thumbUrl
      ? '<td class="thumb-cell"><img class="ae-thumb" src="'+thumbUrl+'" loading="lazy" alt="" data-ae-thumb-ad="'+_thumbAdId+'" title="Click to preview"></td>'
      : '<td class="thumb-cell"><div class="ae-thumb-placeholder" title="No thumbnail">—</div></td>';
    const main = '<tr>'+
      thumbCell+
      '<td class="ad-cell" title="'+(r.ad_name||'')+'"><span style="font-weight:600;color:var(--text-primary)">'+(r.ad_name||'—')+'</span></td>'+
      '<td class="id-cell">'+(r.ad_id||'—')+'</td>'+
      // Asset ID from manual ad_asset_ids mapping. Editable inline —
      // click the cell, type + Enter to save (Escape to cancel).
      '<td class="id-cell ae-asset-cell" data-ad-id="'+(r.ad_id||'')+'">'+
        ((r.ad_id && assetIdByAdId[r.ad_id]) || '—')+
      '</td>'+
      '<td class="camp-cell" title="'+(r.campaign_name||'')+'">'+(r.campaign_name||'—')+'</td>'+
      '<td class="id-cell">'+(r.adset_id||'—')+'</td>'+
      '<td>'+dailyBtn+'</td>'+
      '<td>'+(r.account_name||'—')+'</td>'+
      dCell(r.ad_created)+
      dCell(r.first_seen_date)+
      dCell(r.date_target_imp_achieved)+
      dCell(r.date_of_result)+
      numCell(r.days_to_result == null ? '—' : fmtInt(r.days_to_result))+
      numCell(r.days_to_target_f1 == null ? '—' : fmtInt(r.days_to_target_f1))+
      '<td><span class="cat-badge '+cls+'">'+(r.category||'—')+'</span></td>'+
      fCell(r.f1_pass)+
      fCell(r.f2_pass)+
      fCell(r.f3_pass)+
      fCell(r.f4_pass)+
      '<td><span class="ae-status '+statusCls+'">'+(r.ad_status||'—')+'</span></td>'+
      numCell(impVal)+
      numCell(reachVal)+
      numCell(fmtNum2(r.reach_weight_pct))+
      numCell(fmtInt(r.previous_reach))+
      numCell(fmtInt(r.latest_reach))+
      numCell(fmtInt(r.incremental_reach))+
      numCell(fmtRs(r.cost_per_1000_incremental_reach))+
      numCell(freqVal)+
      numCell(spendVal)+
      numCell(fmtRs(r.cost_per_1000))+
      numCell(cpcVal)+
      numCell(ctrVal)+
      numCell(linkClkVal)+
      numCell(fmtInt(r.atc_count))+
      numCell(fmtPct(r.atc_lc_pct))+
      numCell(fmtInt(r.ci_count))+
      numCell(fmtPct(r.ci_atc_pct))+
      numCell(fmtPct(r.checkout_compl_pct))+
      numCell(fmtPct(r.cr_link_clicks_pct))+
      numCell(purchVal)+
      numCell(convVal)+
      numCell(roasVal)+
      numCell(fmtInt(r.shopify_orders))+
      numCell(fmtRs(r.shopify_sales))+
      numCell(r.shopify_roas == null ? '—' : (+r.shopify_roas).toFixed(2))+
      numCell(r.meta_shop_diff_pct == null ? '—' : (r.meta_shop_diff_pct >= 0 ? '+' : '') + r.meta_shop_diff_pct.toFixed(1) + '%')+
      numCell(cFtVal)+
      numCell(ftVal)+
      numCell(fmtNum2(r.pct_reach_ftewv))+
      numCell(cNcpVal)+
      numCell(ncpVal)+
      numCell(fmtRs(r.profit_efficiency))+
      numCell(fmtPct(r.contrib_margin_pct))+
      numCell(fmtNum3(r.blended_eff))+
      numCell(fmtNum3(r.delivery_eff))+
      numCell(fmtNum3(r.sales_spend_eff))+
      numCell(fmtNum3(r.cpr_eff))+
      numCell(fmtNum3(r.ftv_contrib_eff))+
      numCell(fmtNum3(r.ftev_volume))+
      numCell(fmtNum3(r.ncp_cost_eff))+
      numCell(fmtNum3(r.roas_eff))+
      numCell(fmtNum3(r.profit_vol_eff))+
      numCell(fmtInt(r.ltv_reach))+
      numCell(fmtNum2(r.ltv_frequency))+
      numCell(fmtInt(r.engagement_count))+
      linkCell(r.preview_link, '▸ Preview')+
      linkCell(r.ad_link,      '▸ Open')+
      aeLandingCell(r.ad_link)+
    '</tr>';
    // Append the expanded daily-row directly below the parent if open
    return isOpen ? (main + aeRenderDailyRowHTML(r, COLSPAN)) : main;
  }).join('');
  // Wire daily-attribution button clicks (re-wired on every render)
  tbody.querySelectorAll('[data-ae-daily-ad]').forEach(btn => {
    btn.addEventListener('click', () => aeToggleDaily(btn.dataset.aeDailyAd, btn.dataset.aeDailyCk));
  });
  tbody.querySelectorAll('[data-ae-daily-close]').forEach(btn => {
    btn.addEventListener('click', () => { aeDailyOpenKey = ''; renderAE(); });
  });
  // Thumbnail click → open the preview drawer for that ad
  tbody.querySelectorAll('[data-ae-thumb-ad]').forEach(img => {
    img.addEventListener('click', () => {
      const ad = allAds.find(x => x.ad_id === img.dataset.aeThumbAd);
      if (ad && typeof openDrawer === 'function') openDrawer(ad);
    });
  });

  // Header sort indicators
  document.querySelectorAll('#aeMain thead th').forEach(th => {
    th.classList.remove('sort-asc','sort-desc');
    if (th.dataset.sort === aeSortKey) th.classList.add(aeSortDir === 'asc' ? 'sort-asc' : 'sort-desc');
  });

  const hiddenNote = '';   // Discarded no longer hidden by default
  // Diagnostic strip: shows which date-field mode is active + delivery-set size
  const dfEl = document.getElementById('aeDateField');
  const dfVal = dfEl ? dfEl.value : '';
  const dfLbl = dfEl && dfEl.selectedOptions[0] ? dfEl.selectedOptions[0].text : dfVal;
  let modeNote = '';
  if (dFromStr || dToStr){
    modeNote = ' · date field: ' + dfLbl;
    if (dfVal === '__delivery__'){
      modeNote += (aeDeliverySet ? (' · delivery set ' + fmtInt(aeDeliverySet.size)) : ' · delivery set loading…');
    }
    modeNote += Object.keys(aeWindowMetricsByAdId).length
      ? ' · window metrics ✓'
      : ' · window metrics loading…';
  }
  const catNote = aeSelectedCat ? (' · category=' + aeSelectedCat) : '';
  // Surface any non-default filter so the user can see WHY the row count
  // is lower than the delivery-set headline (e.g., status=ACTIVE dropping
  // 6,955 → 1,002 was invisible until this footer.
  const acctVal   = document.getElementById('aeAcct')?.value    || '';
  const statusVal = document.getElementById('aeStatus')?.value  || '';
  const groupVal  = document.getElementById('aeGroupBy')?.value || 'ad';
  const activeFilters = [];
  if (acctVal)             activeFilters.push('account=' + acctVal);
  if (statusVal)           activeFilters.push('status=' + statusVal);
  if (groupVal !== 'ad')   activeFilters.push('group=' + groupVal);
  const filterNote = activeFilters.length ? ' · [' + activeFilters.join(' · ') + ']' : '';
  // Build the drop-off cascade so the raw delivery-set headline (e.g., 8,548)
  // and the visible-table row count can be reconciled at a glance.
  let cascadeNote = '';
  if (dFromStr || dToStr){
    const raw = aeDeliverySet ? aeDeliverySet.size : allAds.length;
    const parts = ['delivered ' + fmtInt(raw)];
    if (totalRows !== raw){
      parts.push('post-partition+filters ' + fmtInt(totalRows));
    }
    parts.push('shown ' + fmtInt(slice.length));
    cascadeNote = ' · ' + parts.join(' → ');
  }
  document.getElementById('aeFooter').textContent =
    'Showing ' + fmtInt(slice.length) + ' of ' + fmtInt(totalRows) + ' filtered ' +
    '(' + fmtInt(allAds.length) + ' total in ae_table_view' + hiddenNote + ')'+
    modeNote + catNote + filterNote + cascadeNote;

  // Pagination footer
  document.getElementById('aeRowInfo').textContent   = fmtInt(totalRows) + ' rows';
  document.getElementById('aePageInfo').textContent  = 'Page ' + (aePage + 1) + ' / ' + totalPages;
  document.getElementById('aePrevPage').disabled     = aePage === 0;
  document.getElementById('aeNextPage').disabled     = aePage >= totalPages - 1;

  // Bind the inline Asset ID editor on every visible AE row.  Idempotent
  // — installAssetIdCellEditor bails on cells it already owns.
  document.querySelectorAll('#aeMain tbody .ae-asset-cell[data-ad-id]').forEach(td => {
    const adId = td.dataset.adId;
    if (!adId) return;
    installAssetIdCellEditor(td, adId, {
      onSaved: (val) => {
        // Update the allAds cache so header-driven sort by asset_id
        // reflects the new value on the next re-sort/render.
        const r = allAds.find(x => String(x.ad_id) === String(adId));
        if (r) r.asset_id = val;
      }
    });
  });
}

/* Wiring */
['aeF1','aeF2','aeF3','aeF4'].forEach(id =>
  document.getElementById(id).addEventListener('input', () =>
    clearTimeout(window._aeDb) || (window._aeDb = setTimeout(() => {
      // recompute categories on every row, then refresh both views.
      // Creative Testing uses primaryAds (primary_table); Ads Analyse uses
      // allAds (ae_table_view) — re-categorise BOTH so they stay in sync
      // with the threshold inputs. Also mirror values to the CT inputs.
      const ids = [['aeF1','ctF1'],['aeF2','ctF2'],['aeF3','ctF3'],['aeF4','ctF4']];
      for (const [from, to] of ids){
        const a = document.getElementById(from), b = document.getElementById(to);
        if (a && b && b.value !== a.value) b.value = a.value;
      }
      aeApplyCurrentThresholds();
      ctApplyCurrentThresholds();
      renderAE();
      if (typeof rerender === 'function') rerender();   // refreshes Creative Testing KPIs
    }, 250))));
// Creative Testing thresholds + multi-filter wiring
['ctF1','ctF2','ctF3','ctF4'].forEach(id => {
  const el = document.getElementById(id); if (!el) return;
  el.addEventListener('input', () =>
    clearTimeout(window._ctDb) || (window._ctDb = setTimeout(ctApplyThresholdInputs, 250)));
});
const _ctResetBtn = document.getElementById('ctResetThresh');
if (_ctResetBtn) _ctResetBtn.addEventListener('click', () => {
  const defaults = {ctF1:50000, ctF2:3, ctF3:525, ctF4:12};
  for (const [id, v] of Object.entries(defaults)){
    const el = document.getElementById(id); if (el) el.value = v;
  }
  ctApplyThresholdInputs();
});
const _ctMfApply = document.getElementById('ctMfApply');
if (_ctMfApply) _ctMfApply.addEventListener('click', () => rerender());
const _ctMfClear = document.getElementById('ctMfClear');
if (_ctMfClear) _ctMfClear.addEventListener('click', () => {
  ctRules = [{field:'name_combined', op:'contains_all', value:''}];
  ctMfRender(); rerender();
});
const _ctMfAdd = document.getElementById('ctMfAdd');
if (_ctMfAdd) _ctMfAdd.addEventListener('click', () => {
  ctRules.push({field:'name_combined', op:'contains_all', value:''});
  ctMfRender();
});
// Render the empty CT multi-filter row on initial load
ctMfRender();
['aeAcct','aeStatus','aeGroupBy'].forEach(id =>
  document.getElementById(id).addEventListener('change', () => { aePage = 0; renderAE(); }));
// aeDateField change → possibly rebuild the delivery Set (only meaningful
// when switching to __delivery__ with an active window)
document.getElementById('aeDateField').addEventListener('change', async () => {
  await aeRebuildDeliverySet();
  aePage = 0; renderAE();
});
// ── Level toggle (Ad / Adset / Campaign) — mirrors + drives the Group By
// dropdown. Also kicks off the RPC rollup fetch when switching to a group
// level for the first time (fetchAeShopifyRollups is a no-op if the key
// already matches). Keeps the pill row and the dropdown in sync both ways.
(function initAeLevelToggle(){
  const bar = document.getElementById('aeLevelToggle');
  const gb  = document.getElementById('aeGroupBy');
  if (!bar || !gb) return;
  function _sync(fromDropdown){
    const level = gb.value || 'ad';
    bar.querySelectorAll('.ae-level-pill').forEach(p => {
      const on = p.dataset.level === level;
      p.classList.toggle('active', on);
      p.setAttribute('aria-selected', on ? 'true' : 'false');
    });
  }
  bar.addEventListener('click', async e => {
    const p = e.target.closest('.ae-level-pill'); if (!p) return;
    const level = p.dataset.level; if (!level || gb.value === level) return;
    gb.value = level;
    _sync(false);
    // Kick off the RPC rollup fetch if we're going to a group level and it
    // hasn't been fetched yet for the current window. Safe to await —
    // resolves to no-op when already cached.
    if (level === 'adset' || level === 'campaign'){
      await fetchAeShopifyRollups();
    }
    aePage = 0; renderAE();
  });
  gb.addEventListener('change', _sync);
  // Initial sync on load
  _sync(true);
})();
document.getElementById('aePageSize').addEventListener('change', () => { aePage = 0; renderAE(); });

/* Column visibility picker — Shopify-style. State lives in localStorage
   under 'aeHiddenCols' as an array of column labels (the visible header
   text). We hide by injecting CSS nth-child rules that target both the
   <th> and matching <td> in every row, so nothing in the renderer needs
   to change. */
(function initAeColpicker(){
  const KEY = 'aeHiddenCols_v1';
  const wrap    = document.getElementById('aeColpickWrap');
  const btn     = document.getElementById('aeColpickBtn');
  const panel   = document.getElementById('aeColpickPanel');
  const listEl  = document.getElementById('aeColpickList');
  const search  = document.getElementById('aeColpickSearch');
  const styleEl = document.getElementById('aeColHideStyle');
  const cntEl   = document.getElementById('aeColpickCount');
  if (!wrap || !btn || !panel || !styleEl) return;

  const loadHidden = () => {
    try { return new Set(JSON.parse(localStorage.getItem(KEY) || '[]')); }
    catch(_) { return new Set(); }
  };
  const saveHidden = (set) =>
    localStorage.setItem(KEY, JSON.stringify(Array.from(set)));

  function _collectCols(){
    // Return [{idx, label}] for every <th> in the AE table, 1-based to
    // line up with CSS nth-child indexing.
    const ths = document.querySelectorAll('#aeMain thead th');
    return Array.from(ths).map((th, i) => ({
      idx: i + 1,
      // Strip any nested widget text (e.g. the ✎ badge in the Asset ID
      // header) — use the first text node so we get "Asset ID" not
      // "Asset ID ✎".
      label: (th.childNodes[0]?.nodeValue || th.textContent || '').trim()
             || ('col ' + (i + 1)),
    }));
  }

  function _applyHide(){
    const cols = _collectCols();
    const hidden = loadHidden();
    // Build one rule per hidden col — table structure lines up thead↔tbody.
    const rules = [];
    for (const c of cols){
      if (hidden.has(c.label)){
        rules.push(
          '#aeMain thead th:nth-child(' + c.idx + '),' +
          '#aeMain tbody td:nth-child(' + c.idx + ')' +
          '{display:none !important}'
        );
      }
    }
    styleEl.textContent = rules.join('\n');
    if (cntEl){
      const visibleCount = cols.length - hidden.size;
      cntEl.textContent = '(' + visibleCount + '/' + cols.length + ')';
    }
  }

  function _renderList(){
    const cols = _collectCols();
    const hidden = loadHidden();
    const q = (search.value || '').trim().toLowerCase();
    const shown = cols.filter(c => !q || c.label.toLowerCase().includes(q));
    if (!shown.length){
      listEl.innerHTML = '<div class="ae-colpick-empty">No columns match.</div>';
      return;
    }
    listEl.innerHTML = shown.map(c => {
      const checked = hidden.has(c.label) ? '' : 'checked';
      const safe    = c.label.replace(/</g, '&lt;');
      return '<label class="ae-colpick-item">' +
        '<input type="checkbox" data-col-label="' + safe.replace(/"/g,'&quot;') + '" ' + checked + '>' +
        '<span class="lbl" title="' + safe.replace(/"/g,'&quot;') + '">' + safe + '</span>' +
      '</label>';
    }).join('');
    listEl.querySelectorAll('input[type="checkbox"]').forEach(cb => {
      cb.addEventListener('change', () => {
        const label = cb.dataset.colLabel;
        const set = loadHidden();
        if (cb.checked) set.delete(label);
        else            set.add(label);
        saveHidden(set);
        _applyHide();
      });
    });
  }

  btn.addEventListener('click', e => {
    e.stopPropagation();
    const open = panel.classList.toggle('open');
    panel.setAttribute('aria-hidden', open ? 'false' : 'true');
    if (open){ _renderList(); setTimeout(() => search.focus(), 30); }
  });
  document.addEventListener('click', e => {
    if (!panel.classList.contains('open')) return;
    if (wrap.contains(e.target)) return;
    panel.classList.remove('open'); panel.setAttribute('aria-hidden','true');
  });
  search.addEventListener('input', _renderList);

  document.getElementById('aeColpickAll').addEventListener('click', () => {
    saveHidden(new Set()); _applyHide(); _renderList();
  });
  document.getElementById('aeColpickNone').addEventListener('click', () => {
    const cols = _collectCols();
    saveHidden(new Set(cols.map(c => c.label))); _applyHide(); _renderList();
  });
  document.getElementById('aeColpickReset').addEventListener('click', () => {
    localStorage.removeItem(KEY); _applyHide(); _renderList();
  });

  // Apply on page load. The CSS rules are document-scoped so they hit
  // every current + future <td>/<th> in the table — no re-hook needed
  // for subsequent renderAE() calls.
  _applyHide();

  // Expose the internals so the Inspector drawer's Metrics section can
  // reuse the same state (single source of truth: localStorage 'aeHiddenCols_v1').
  window._aeColpick = {
    collectCols: _collectCols,
    loadHidden,
    saveHidden,
    applyHide: _applyHide,
    renderList: _renderList,
    KEY,
  };
})();

/* ─────────────────────────────────────────────────────────────
   Inspector drawer — right-side Shopify-Analytics-style panel.
   Sections: Metrics (column visibility) + Filters (multi-filter rules).
   Filters live inside this drawer via #aeMfRows (moved from the inline card).
   ───────────────────────────────────────────────────────────── */
(function initAeInspector(){
  const btn      = document.getElementById('aeInspBtn');
  const drawer   = document.getElementById('aeInspector');
  const closeBtn = document.getElementById('aeInspClose');
  const backdrop = document.getElementById('aeInspBackdrop');
  if (!btn || !drawer) return;

  // ── Metrics section (mirror the ▤ Columns picker) ──
  const listEl  = document.getElementById('aeInspMetricsList');
  const search  = document.getElementById('aeInspMetricsSearch');
  const cntEl   = document.getElementById('aeInspMetricsCount');

  function renderMetrics(){
    const api = window._aeColpick;
    if (!api || !listEl) return;
    const cols = api.collectCols();
    const hidden = api.loadHidden();
    if (cntEl) cntEl.textContent = (cols.length - hidden.size) + ' / ' + cols.length + ' shown';
    const q = (search?.value || '').trim().toLowerCase();
    const shown = cols.filter(c => !q || c.label.toLowerCase().includes(q));
    if (!shown.length){
      listEl.innerHTML = '<div class="ae-insp-metric-empty">No metrics match.</div>';
      return;
    }
    listEl.innerHTML = shown.map(c => {
      const isOn = !hidden.has(c.label);
      const safe = c.label.replace(/</g, '&lt;').replace(/"/g,'&quot;');
      return '<label class="ae-insp-metric-item' + (isOn ? '' : ' check-off') + '">' +
        '<input type="checkbox" data-col-label="' + safe + '" ' + (isOn ? 'checked' : '') + '>' +
        '<span class="lbl" title="' + safe + '">' + safe + '</span>' +
      '</label>';
    }).join('');
    listEl.querySelectorAll('input[type="checkbox"]').forEach(cb => {
      cb.addEventListener('change', () => {
        const api = window._aeColpick; if (!api) return;
        const label = cb.dataset.colLabel;
        const set = api.loadHidden();
        if (cb.checked) set.delete(label);
        else            set.add(label);
        api.saveHidden(set); api.applyHide();
        // Repaint just this row's dimmed state — cheaper than re-rendering the list.
        cb.closest('.ae-insp-metric-item')?.classList.toggle('check-off', !cb.checked);
        if (cntEl){
          const cols = api.collectCols();
          cntEl.textContent = (cols.length - set.size) + ' / ' + cols.length + ' shown';
        }
        // Keep the legacy ▤ Columns panel in sync if it happens to be open.
        try { api.renderList(); } catch(_){}
      });
    });
  }
  if (search) search.addEventListener('input', renderMetrics);

  document.getElementById('aeInspMetricsAll')?.addEventListener('click', () => {
    const api = window._aeColpick; if (!api) return;
    api.saveHidden(new Set()); api.applyHide(); renderMetrics();
  });
  document.getElementById('aeInspMetricsNone')?.addEventListener('click', () => {
    const api = window._aeColpick; if (!api) return;
    const cols = api.collectCols();
    api.saveHidden(new Set(cols.map(c => c.label))); api.applyHide(); renderMetrics();
  });
  document.getElementById('aeInspMetricsReset')?.addEventListener('click', () => {
    const api = window._aeColpick; if (!api) return;
    localStorage.removeItem(api.KEY); api.applyHide(); renderMetrics();
  });

  // ── Open / close ──
  function open(){
    drawer.classList.add('open'); drawer.setAttribute('aria-hidden','false');
    backdrop?.classList.add('open'); backdrop?.setAttribute('aria-hidden','false');
    renderMetrics();
    // Re-render numeric filter rules — drawer's Filters section.
    if (typeof aeMfRenderNum === 'function') aeMfRenderNum();
    setTimeout(() => search?.focus(), 40);
  }
  function close(){
    drawer.classList.remove('open'); drawer.setAttribute('aria-hidden','true');
    backdrop?.classList.remove('open'); backdrop?.setAttribute('aria-hidden','true');
  }
  btn.addEventListener('click', () => {
    drawer.classList.contains('open') ? close() : open();
  });
  closeBtn?.addEventListener('click', close);
  backdrop?.addEventListener('click', close);
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape' && drawer.classList.contains('open')) close();
  });
})();
// Manual date-input edits rebuild the delivery Set too (debounced)
['aeDateFrom','aeDateTo'].forEach(id =>
  document.getElementById(id).addEventListener('change', async () => {
    clearTimeout(window._aeDeliveryDb);
    window._aeDeliveryDb = setTimeout(async () => {
      await Promise.all([aeRebuildDeliverySet(), fetchAeWindowMetrics(),
                     fetchAeWindowShopify(), fetchAeWindowReach(),
                     fetchAeShopifyRollups()]);
      aePage = 0; renderAE();
    }, 220);
  }));
// Category dropdown drives the same filter as clicking a KPI card.
document.getElementById('aeCategory').addEventListener('change', e => {
  aeSelectedCat = e.target.value || '';
  aePage = 0; renderAE();
});

// Clear buttons
document.getElementById('aeClearDates').onclick = () => drpClearDateRange();
// Edit Asset ID toggle — same flag for AE + AI. See setAssetEditMode.
// aeEditAssetBtn was removed from the AE actions row — asset-id editing is now
// only wired through the ✎ badge in the Asset ID column header. Guarded so the
// bootstrap doesn't crash if the element is missing.
document.getElementById('aeEditAssetBtn')?.addEventListener('click',
  () => setAssetEditMode(!window.assetEditMode));
document.getElementById('aeClearFilters').onclick = async () => {
  document.getElementById('aeAcct').value      = '';
  document.getElementById('aeStatus').value    = '';
  document.getElementById('aeGroupBy').value   = 'ad';
  document.getElementById('aeDateField').value = '__delivery__';  // new default
  document.getElementById('aeCategory').value  = '';
  await drpClearDateRange();   // also clears aeDateFrom/aeDateTo + button label
  aeSelectedCat = '';
  aeRules = []; aeRulesNum = [];
  aeMfRender(); aeMfRenderNum();
  await aeRebuildDeliverySet();
  aePage = 0; renderAE();
};

// Pagination buttons
document.getElementById('aePrevPage').onclick = () => { if (aePage > 0) { aePage--; renderAE(); } };
document.getElementById('aeNextPage').onclick = () => { aePage++; renderAE(); };

// Multi-Filter buttons — inline card (TEXT rules)
document.getElementById('aeMfAdd').onclick = () => {
  aeRules.push({field:'name_combined', op:'contains_all', value:''});
  aeMfRender();
};
document.getElementById('aeMfApply').onclick = () => { aePage = 0; renderAE(); };
document.getElementById('aeMfClear').onclick = () => {
  aeRules = [];
  aeMfRender();
  aePage = 0; renderAE();
};
// Inspector drawer — NUMERIC filter buttons
document.getElementById('aeMfNumAdd')?.addEventListener('click', () => {
  aeRulesNum.push({field:'amount_spent', op:'gte', value:''});
  aeMfRenderNum();
});
document.getElementById('aeMfNumApply')?.addEventListener('click', () => {
  aePage = 0; renderAE();
});
document.getElementById('aeMfNumClear')?.addEventListener('click', () => {
  aeRulesNum = [];
  aeMfRenderNum();
  aePage = 0; renderAE();
});

// Render the initial empty rule on page load — both panels
aeMfRender();
aeMfRenderNum();
// Init the date-range pickers once the DOM is wired
initDateRangePicker();
initLifecycleDRP();
document.getElementById('aeCats').addEventListener('click', e => {
  const card = e.target.closest('.kpi'); if (!card) return;
  const c = card.dataset.cat;
  aeSelectedCat = (aeSelectedCat === c) ? '' : c;
  // Mirror to the dropdown so the user sees the same state in both places
  const dd = document.getElementById('aeCategory');
  if (dd) dd.value = aeSelectedCat;
  aePage = 0;
  renderAE();
});
document.querySelectorAll('#aeMain thead th').forEach(th => {
  th.addEventListener('click', () => {
    const k = th.dataset.sort;
    // Guard: Preview and Attribution columns have no data-sort; ignoring the
    // click prevents aeSortKey from being set to undefined (which silently
    // broke every subsequent sort).
    if (!k) return;
    if (aeSortKey === k) aeSortDir = (aeSortDir === 'asc') ? 'desc' : 'asc';
    else { aeSortKey = k; aeSortDir = 'desc'; }
    renderAE();
  });
});
document.getElementById('aeBtnRefresh').onclick = async () => {
  // Refresh both sources + thumbnails + reach-recent. Creative Testing uses
  // the current date window (default last 30d); Ads Analyse uses lifetime.
  let freshReach;
  [primaryAds, allAds, thumbsByAdId, freshReach] = await Promise.all([
    fetchPrimaryAggregated(state.dateFrom || '', state.dateTo || ''),
    fetchAds(),
    fetchThumbnails(),
    fetchReachRecent()
  ]);
  reachRecentByAdId = freshReach;
  for (const r of allAds){
    const rr = reachRecentByAdId[r.ad_id];
    if (rr){
      r.previous_reach                   = rr.previous_reach;
      r.latest_reach                     = rr.latest_reach;
      r.incremental_reach                = rr.incremental_reach;
      r.cost_per_incremental_reach       = rr.cost_per_incremental_reach;
      r.cost_per_1000_incremental_reach  = rr.cost_per_1000_incremental_reach;
      r.latest_spend                     = rr.latest_spend;
    }
  }
  enrichPrimaryDates();
  aeApplyCurrentThresholds();
  rerender();
  renderAE();
};
document.getElementById('aeBtnExport').onclick = () => {
  // Exports exactly what the table currently shows: filter-selected rows,
  // in the sort order they're displayed, and only the columns the user
  // hasn't hidden via the ▤ Columns picker.  Enriches each row with the
  // computed asset_id, reach snapshot, and shopify metrics that the
  // renderer merges in but that aren't stored on the raw ae_table_view
  // row, so those columns don't come out blank.
  // Sort matches the on-screen order: same window-then-numeric-filter-then-sort
  // pipeline as renderAE, so CSV export mirrors what the user is looking at.
  const hasWindow = Object.keys(aeWindowMetricsByAdId).length > 0
                    || aeWindowShopifyKeyIsActive()
                    || !!_aeWindowReachKey;
  const _numRulesCsv = aeRulesNum.filter(rl => String(rl.value ?? '').trim());
  let _windowedCsv = aeFiltered().map(r => hasWindow ? aeApplyWindow(r) : r);
  if (_numRulesCsv.length){
    _windowedCsv = _windowedCsv.filter(r => _numRulesCsv.every(rule => aeMfMatch(r, rule)));
  }
  const rows = _aeSortRows(_windowedCsv, aeSortKey, aeSortDir);
  exportVisibleTableCsv('#aeMain', rows, {
    filenamePrefix: 'ads_analyse',
    deriveRow: r => ({
      ...r,
      asset_id:              assetIdByAdId[r.ad_id] || '',
      previous_reach:        (aeWindowReachByAdId[r.ad_id] || {}).reach_first ?? r.previous_reach,
      latest_reach:          (aeWindowReachByAdId[r.ad_id] || {}).reach_last  ?? r.latest_reach,
      incremental_reach:     (aeWindowReachByAdId[r.ad_id] || {}).reach_sum   ?? r.incremental_reach,
    }),
  });
};
/* Incremental Reach group-by modal was removed — the per-ad reach
   columns (Prev / Latest / Incr / Cost per 1k) now render directly in
   the AE table from the fields ae_reach_recent already merges onto
   each row. The aggregated Campaign / Adset group-by view lives in
   the sidebar under Historic > Historic Reach. */


/* ────────────────────────────────────────────────────────────────────
   INVENTORY — Shopify products via Admin GraphQL
   Credentials via URL params:
     ?shopDomain=<shop>.myshopify.com&shopifyToken=<admin_token>
     [&shopifyApiVer=2025-10]
   ──────────────────────────────────────────────────────────────────── */
// Shopify credentials resolve in this order:
//   1. localStorage (set via the Inventory config panel — persistent)
//   2. URL params (?shopDomain=&shopifyToken=&shopifyApiVer= — one-off)
// Kept as `let` so the config panel can rewrite them without a reload.
function _readShopifyCfg(){
  let cfg = {};
  try { cfg = JSON.parse(localStorage.getItem('shopify.cfg') || '{}') || {}; }
  catch { cfg = {}; }
  return {
    domain:  cfg.domain || params.get('shopDomain')    || '',
    token:   cfg.token  || params.get('shopifyToken')  || '',
    apiVer:  cfg.apiVer || params.get('shopifyApiVer') || '2025-10',
  };
}
let SHOPIFY_DOMAIN  = _readShopifyCfg().domain;
let SHOPIFY_TOKEN   = _readShopifyCfg().token;
let SHOPIFY_API_VER = _readShopifyCfg().apiVer;
let invProducts = [];
// Multi-facet filter state — every dropdown is client-side.
let invState = {
  status:'', search:'', excludeNoImg:false,
  lifecycle:'',     // '', 'npd', 'active', 'discontinued'
  stock:'',         // '', 'in_stock', 'low_stock', 'out_of_stock'
  category:'',      // free-string exact match
  type:'',          // free-string exact match on product_type
};

// Discontinued / NPD constants shared with the reference project. NPD =
// created ≤45 days ago; anything else with a "discontinued" tag is
// pushed into the third bucket. Everything else is Active.
const INV_NPD_MAX_DAYS  = 45;
const INV_DISC_TAGS     = new Set(['discontinued','disc','discontinue','disc.']);
const INV_LOW_BREAK_MAX = 10;   // ≤10 units → low
const INV_SIZE_ORDER_TABLE = ['XS','S','M','L','XL','2XL','3XL','4XL','5XL'];

function invLifecycle(p){
  const tags = (p.tags || []).map(t => (t || '').toLowerCase().trim());
  if (tags.some(t => INV_DISC_TAGS.has(t))) return 'discontinued';
  const t = p.createdAt ? Date.parse(p.createdAt) : NaN;
  if (isFinite(t)){
    const days = (Date.now() - t) / 86400000;
    if (days <= INV_NPD_MAX_DAYS) return 'npd';
  }
  return 'active';
}
function invStockStatus(p){
  // Derived from sizes_json — the JSON blob populated by
  // sync_shopify_products.py. When no size variants exist we fall back
  // to totalInventory so bedsheets etc. still classify sensibly.
  const sz = p.sizes || null;
  if (sz && typeof sz === 'object' && Object.keys(sz).length){
    const vals = Object.values(sz).map(v => Number(v) || 0);
    const zeros = vals.filter(v => v === 0).length;
    const total = vals.reduce((a,b) => a + b, 0);
    if (zeros === vals.length)        return 'out_of_stock';
    if (zeros >= vals.length / 2)     return 'broken_stock';   // mostly zeroes
    if (total <= INV_LOW_BREAK_MAX)   return 'low_stock';
    return 'in_stock';
  }
  const tot = Number(p.totalInventory);
  if (!isFinite(tot) || tot <= 0)      return 'out_of_stock';
  if (tot <= INV_LOW_BREAK_MAX)        return 'low_stock';
  return 'in_stock';
}

const INV_QUERY = `
  query InvPage($after: String) {
    products(first: 100, after: $after) {
      edges {
        cursor
        node {
          id
          title
          handle
          status
          vendor
          productType
          createdAt
          totalInventory
          variantsCount { count }
          featuredImage { url(transform: {maxWidth: 96, maxHeight: 96}) }
          priceRangeV2 { minVariantPrice { amount } maxVariantPrice { amount } }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;

async function shopifyGraphQL(query, variables){
  const url = 'https://' + SHOPIFY_DOMAIN + '/admin/api/' + SHOPIFY_API_VER + '/graphql.json';
  const r = await fetch(url, {
    method:'POST',
    headers:{'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type':'application/json'},
    body: JSON.stringify({query, variables})
  });
  if (!r.ok) throw new Error('Shopify HTTP ' + r.status);
  const j = await r.json();
  if (j.errors) throw new Error(JSON.stringify(j.errors).slice(0, 200));
  return j.data;
}

/* ───────────────────────────────────────────────────────────────
   Landing Page Analysis — two master-table modes:
     'pages' → public.landing_page_analysis_30d (one row per page)
     'ads'   → public.landing_page_ad_breakdown_30d (one row per ad)
   Drill-down is the same in both modes — it always opens on the
   selected page (in 'ads' mode we use the clicked ad's landing_page_path).
   ─────────────────────────────────────────────────────────────── */
const LP_ROLLUP_TABLE = 'landing_page_analysis_30d';
let _lpRows       = [];       // rows for 'pages' master
let _lpAdsMaster  = [];       // rows for 'ads' master (all pages)
let _lpViewMode   = 'pages';  // 'pages' | 'ads'

async function loadLandingPageStatus(){
  // The heavy status panel is gone — Session-data-sync is finished. Just
  // keep this shim so the sidebar click handler doesn't error out.
  return;
}

async function loadLandingPageTable(){
  const bodyP = document.getElementById('lpTableBody');
  const sub   = document.getElementById('lpTableSub');
  if (!bodyP) return;
  bodyP.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:14px;color:var(--text-tertiary)">loading…</td></tr>';
  const hdrs = { apikey: SUPABASE_ANON, Authorization: 'Bearer ' + SUPABASE_ANON };
  try {
    // Fetch BOTH masters in parallel — small tables, one round trip each.
    const [pR, aR] = await Promise.all([
      fetch(SUPABASE_URL + '/rest/v1/' + LP_ROLLUP_TABLE +
            '?select=*&order=sessions.desc&limit=5000', { headers: hdrs }),
      fetch(SUPABASE_URL + '/rest/v1/' + LP_AD_TABLE +
            '?select=*&order=spend.desc&limit=5000', { headers: hdrs }),
    ]);
    if (!pR.ok) throw new Error(LP_ROLLUP_TABLE + ' HTTP ' + pR.status);
    if (!aR.ok) throw new Error(LP_AD_TABLE + ' HTTP ' + aR.status);
    _lpRows      = await pR.json();
    _lpAdsMaster = await aR.json();
    if (!Array.isArray(_lpRows) || !_lpRows.length){
      bodyP.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:14px;' +
        'color:var(--text-tertiary)">Rollup is empty. Click ↻ Recompute to seed it.</td></tr>';
      return;
    }
    _lpRenderTable();
    _lpRenderAdsMasterTable();
    const first = _lpRows[0];
    if (first && sub){
      sub.textContent = 'window ' + first.window_from + ' → ' + first.window_to +
                        ' · joined with primary_table.ad_link';
    }
    _lpRenderKpis();
  } catch (e){
    bodyP.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:14px;' +
      'color:var(--error-text, #b94a3d)">Error: ' + (e?.message || e) + '</td></tr>';
  }
}

/* Render the Ads master table (Mode B).  Same filter/search/sort UI as
   the Pages master — but the sort key is remapped to ad-level columns. */
function _lpRenderAdsMasterTable(){
  const body   = document.getElementById('lpTableAdsBody');
  const foot   = document.getElementById('lpTableFooter');
  const qEl    = document.getElementById('lpSearch');
  const sortEl = document.getElementById('lpSort');
  if (!body) return;
  // Map the shared sort dropdown values to ad-level fields.
  const _adSortMap = {
    sessions:         'spend',           // sessions unavailable per ad → default to spend
    ad_spend:         'spend',
    atc_rate:         'shopify_roas',    // repurpose to Shop ROAS in ad view
    bounce_rate:      'meta_roas',
    cost_per_session: 'roas_gap_pct',
  };
  const q     = ((qEl?.value || '')).trim().toLowerCase();
  const sortK = _adSortMap[sortEl?.value] || 'spend';
  let rows = _lpAdsMaster.slice();
  if (q) rows = rows.filter(r =>
    (r.ad_name || '').toLowerCase().includes(q) ||
    (r.landing_page_path || '').toLowerCase().includes(q));
  rows.sort((a, b) => (+b[sortK] || 0) - (+a[sortK] || 0));
  const TOPN  = 250;
  const shown = rows.slice(0, TOPN);
  const _fmtRs   = v => '₹' + fmtInt(+v || 0);
  const _fmtRoas = v => (+v || 0).toFixed(2);
  const _pill = st => {
    const s = (st || '').toUpperCase();
    const cls = s === 'ACTIVE' ? 'active' : (s ? 'paused' : '');
    return '<span class="lp-status ' + cls + '">' + (s || '—') + '</span>';
  };
  body.innerHTML = shown.map(r => {
    const gap    = r.roas_gap_pct;
    const gapCls = gap == null ? '' : (+gap < -15 ? 'lp-gap-neg' : (+gap > 15 ? 'lp-gap-pos' : ''));
    const gapTxt = gap == null ? '—' : ((+gap > 0 ? '+' : '') + (+gap).toFixed(1) + '%');
    const thumb  = r.preview_link
      ? '<img src="' + r.preview_link.replace(/"/g, '&quot;') + '" alt=""' +
        ' style="width:40px;height:40px;object-fit:cover;border-radius:5px" onerror="this.style.visibility=\'hidden\'">'
      : '';
    const adName = (r.ad_name || '').replace(/</g, '&lt;');
    const path   = (r.landing_page_path || '').replace(/</g, '&lt;');
    return '<tr data-lp-path="' + (r.landing_page_path || '').replace(/"/g, '&quot;') + '">' +
      '<td>' + thumb + '</td>' +
      '<td class="mono" style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"' +
        ' title="' + adName.replace(/"/g,'&quot;') + '">' + adName + '</td>' +
      '<td class="mono" style="max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"' +
        ' title="' + path.replace(/"/g,'&quot;') + '">' + path + '</td>' +
      '<td class="mono" style="text-align:right">' + _pill(r.ad_status) + '</td>' +
      '<td class="mono" style="text-align:right">' + fmtInt(r.impressions) + '</td>' +
      '<td class="mono" style="text-align:right">' + _fmtRs(r.spend) + '</td>' +
      '<td class="mono" style="text-align:right">' + _fmtRoas(r.meta_roas) + '</td>' +
      '<td class="mono" style="text-align:right">' + fmtInt(r.shopify_orders) + '</td>' +
      '<td class="mono" style="text-align:right">' + _fmtRoas(r.shopify_roas) + '</td>' +
      '<td class="mono ' + gapCls + '" style="text-align:right">' + gapTxt + '</td>' +
      '</tr>';
  }).join('') || '<tr><td colspan="10" style="text-align:center;padding:14px;color:var(--text-tertiary)">No ads match.</td></tr>';
  if (foot && _lpViewMode === 'ads'){
    foot.textContent = 'Showing ' + fmtInt(shown.length) + ' of ' + fmtInt(rows.length) +
                       ' ads (from ' + fmtInt(_lpAdsMaster.length) + ') · sort=' + sortK;
  }
}

/* Toggle: swap the master table + retarget the sort/search UI. */
function _lpSetViewMode(mode){
  if (mode !== 'ads' && mode !== 'pages') return;
  _lpViewMode = mode;
  document.querySelectorAll('.lp-viewtog-btn').forEach(b => {
    const on = b.dataset.lpView === mode;
    b.classList.toggle('active', on);
    b.setAttribute('aria-selected', on ? 'true' : 'false');
  });
  const pagesWrap = document.getElementById('lpTableWrapPages');
  const adsWrap   = document.getElementById('lpTableWrapAds');
  if (pagesWrap) pagesWrap.style.display = (mode === 'pages') ? '' : 'none';
  if (adsWrap)   adsWrap  .style.display = (mode === 'ads')   ? '' : 'none';
  // Re-title + retune search placeholder
  const title = document.getElementById('lpTableTitle');
  const q     = document.getElementById('lpSearch');
  if (title){
    title.innerHTML = (mode === 'ads' ? 'Ads' : 'Landing pages') +
      ' <span class="h-sub" id="lpTableSub">last 30 days · ' +
      (mode === 'ads' ? 'ad-first view · Meta vs Shopify ROAS'
                     : 'sessions × ad-spend join') + '</span>';
  }
  if (q) q.placeholder = (mode === 'ads') ? 'filter ad name or path…' : 'filter path…';
  // Force re-render the visible master with the current search/sort state.
  if (mode === 'ads') _lpRenderAdsMasterTable();
  else                _lpRenderTable();
}

// Wire the view toggle pills.
document.querySelectorAll('.lp-viewtog-btn').forEach(btn => {
  btn.addEventListener('click', () => _lpSetViewMode(btn.dataset.lpView));
});

function _lpRenderTable(){
  const body   = document.getElementById('lpTableBody');
  const foot   = document.getElementById('lpTableFooter');
  const qEl    = document.getElementById('lpSearch');
  const sortEl = document.getElementById('lpSort');
  if (!body) return;
  const q     = ((qEl?.value || '')).trim().toLowerCase();
  const sortK = sortEl?.value || 'sessions';
  let rows = _lpRows.slice();
  if (q) rows = rows.filter(r => (r.landing_page_path || '').toLowerCase().includes(q));
  rows.sort((a, b) => (+b[sortK] || 0) - (+a[sortK] || 0));
  const TOPN  = 250;
  const shown = rows.slice(0, TOPN);
  const _fmtPct = v => (v == null || v === '') ? '—' : (+v).toFixed(2) + '%';
  const _fmtRs  = v => '₹' + fmtInt(+v || 0);
  // Make rows clickable so the drill-down opens for the picked path.
  const tblEl = document.getElementById('lpTable');
  if (tblEl && !tblEl.classList.contains('lp-tbl-clickable'))
    tblEl.classList.add('lp-tbl-clickable');
  body.innerHTML = shown.map(r => (
    '<tr data-lp-path="' + (r.landing_page_path || '').replace(/"/g, '&quot;') + '">' +
    '<td class="mono" style="max-width:460px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"' +
      ' title="' + (r.landing_page_path || '').replace(/"/g, '&quot;') + '">' + r.landing_page_path + '</td>' +
    '<td class="mono" style="text-align:right">' + fmtInt(r.sessions) + '</td>' +
    '<td class="mono" style="text-align:right">' + fmtInt(r.visitors) + '</td>' +
    '<td class="mono" style="text-align:right">' + _fmtRs(r.ad_spend) + '</td>' +
    '<td class="mono" style="text-align:right">' + fmtInt(r.distinct_ads) + '</td>' +
    '<td class="mono" style="text-align:right">' + _fmtRs(r.cost_per_session) + '</td>' +
    '<td class="mono" style="text-align:right">' + _fmtPct(r.atc_rate) + '</td>' +
    '<td class="mono" style="text-align:right">' + _fmtPct(r.checkout_rate) + '</td>' +
    '<td class="mono" style="text-align:right">' + _fmtPct(r.bounce_rate) + '</td>' +
    '</tr>'
  )).join('') || '<tr><td colspan="9" style="text-align:center;padding:14px;color:var(--text-tertiary)">No pages match.</td></tr>';
  if (foot){
    foot.textContent = 'Showing ' + fmtInt(shown.length) + ' of ' + fmtInt(rows.length) +
                       ' pages (filtered from ' + fmtInt(_lpRows.length) + ') · sort=' + sortK;
  }
}

function _lpRenderKpis(){
  const rows = _lpRows;
  if (!rows.length) return;
  let sess = 0, spend = 0, atcW = 0, bncW = 0;
  for (const r of rows){
    const s = +r.sessions || 0;
    sess  += s;
    spend += +r.ad_spend || 0;
    atcW  += ((+r.atc_rate    || 0) * s);
    bncW  += ((+r.bounce_rate || 0) * s);
  }
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  set('lpKpSess',    fmtInt(sess));
  set('lpKpSpend',   '₹' + fmtInt(spend));
  set('lpKpCPS',     sess > 0 ? '₹' + (spend / sess).toFixed(2) : '—');
  set('lpKpATC',     sess > 0 ? (atcW / sess).toFixed(2) + '%' : '—');
  set('lpKpBounce',  sess > 0 ? (bncW / sess).toFixed(2) + '%' : '—');
  // Snapshot info — window + computed_at (all rows share the same snapshot).
  const first = rows[0];
  set('lpKpSnap', first?.window_from + ' → ' + first?.window_to);
  const el = document.getElementById('lpKpSnapSub');
  if (el && first?.computed_at){
    const d = new Date(first.computed_at);
    el.textContent = 'computed ' + d.toISOString().slice(0, 16).replace('T', ' ') + 'Z';
  }
}

async function _lpRecompute(){
  const btn = document.getElementById('lpRefreshCompute');
  if (btn){ btn.disabled = true; btn.textContent = '⏳ Recomputing…'; }
  try {
    const url = SUPABASE_URL + '/rest/v1/rpc/refresh_landing_page_analysis_30d';
    const r = await fetch(url, {
      method: 'POST',
      headers: { apikey: SUPABASE_ANON, Authorization: 'Bearer ' + SUPABASE_ANON,
                 'Content-Type': 'application/json' },
      body: JSON.stringify({ p_days: 30 }),
    });
    if (!r.ok) throw new Error('RPC HTTP ' + r.status);
    await loadLandingPageTable();
  } catch (e){
    alert('Recompute failed: ' + (e?.message || e));
  } finally {
    if (btn){ btn.disabled = false; btn.textContent = '↻ Recompute'; }
  }
}

// Search + sort dispatch to whichever master is currently visible.
function _lpMasterRerender(){
  if (_lpViewMode === 'ads') _lpRenderAdsMasterTable();
  else                       _lpRenderTable();
}
document.getElementById('lpRefresh')?.addEventListener('click', loadLandingPageTable);
document.getElementById('lpRefreshCompute')?.addEventListener('click', _lpRecompute);
document.getElementById('lpSearch')?.addEventListener('input',
  () => { clearTimeout(window._lpDb); window._lpDb = setTimeout(_lpMasterRerender, 120); });
document.getElementById('lpSort')?.addEventListener('change', _lpMasterRerender);
document.getElementById('lpExport')?.addEventListener('click', () => {
  const rows = Array.from(document.querySelectorAll('#lpTable tbody tr'));
  const csv = ['landing_page_path,sessions,visitors,ad_spend,distinct_ads,' +
               'cost_per_session,atc_pct,checkout_pct,bounce_pct'];
  for (const r of rows){
    const c = r.querySelectorAll('td');
    if (c.length < 9) continue;
    csv.push('"' + c[0].textContent.replace(/"/g, '""') + '",' +
             [c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8]]
               .map(x => x.textContent.replace(/[,₹]/g, '')).join(','));
  }
  const blob = new Blob([csv.join('\n')], {type:'text/csv'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'landing_page_analysis_' + new Date().toISOString().slice(0,10) + '.csv';
  a.click();
});

/* ═════════════════════════════════════════════════════════════════
   Landing Page × Ads DRILL-DOWN
   Fires when the user clicks a row in the master landing-page table.
   Fetches every ad whose destination URL resolves to that path from
   public.landing_page_ad_breakdown_30d, renders KPIs + 2 charts + table.
   ═════════════════════════════════════════════════════════════════ */
const LP_AD_TABLE = 'landing_page_ad_breakdown_30d';
let _lpAdRows       = [];              // last-loaded ad rows for selected path
let _lpAdFilter     = 'all';
let _lpSelectedPath = null;
let _lpChartBars    = null;            // Chart.js instances (destroy before re-init)
let _lpChartBubble  = null;

// Delegated click handler for BOTH master tables — the tbody re-renders on
// every filter, so listen at each table root.  Clicking an ad-first row
// opens the drill-down on that ad's landing_page_path (all sibling ads on
// the same page show up in the detail, sorted by spend).
function _lpMasterClick(e){
  const tr = e.target.closest('tr[data-lp-path]');
  if (!tr) return;
  const path = tr.getAttribute('data-lp-path') || '';
  document.querySelectorAll('#lpTable tbody tr.selected, #lpTableAds tbody tr.selected')
    .forEach(x => x.classList.remove('selected'));
  tr.classList.add('selected');
  _lpSelectedPath = path;
  _lpOpenDetail(path);
}
document.getElementById('lpTable')?.addEventListener('click', _lpMasterClick);
document.getElementById('lpTableAds')?.addEventListener('click', _lpMasterClick);

function _lpCloseDetail(){
  const card     = document.getElementById('lpDetailCard');
  const backdrop = document.getElementById('lpDetailBackdrop');
  if (card)     card.style.display = 'none';
  if (backdrop){ backdrop.classList.remove('open'); backdrop.setAttribute('aria-hidden','true'); }
  document.body.classList.remove('lp-modal-open');
  document.querySelectorAll('#lpTable tbody tr.selected, #lpTableAds tbody tr.selected')
    .forEach(x => x.classList.remove('selected'));
  _lpSelectedPath = null;
}
document.getElementById('lpDetailClose')?.addEventListener('click', _lpCloseDetail);
document.getElementById('lpDetailBackdrop')?.addEventListener('click', _lpCloseDetail);
document.addEventListener('keydown', e => {
  if (e.key === 'Escape'){
    const card = document.getElementById('lpDetailCard');
    if (card && card.style.display !== 'none') _lpCloseDetail();
  }
});

document.querySelectorAll('.lp-pill').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.lp-pill').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    _lpAdFilter = btn.dataset.lpFilter || 'all';
    _lpRenderDetail();
  });
});

async function _lpOpenDetail(path){
  const card     = document.getElementById('lpDetailCard');
  const backdrop = document.getElementById('lpDetailBackdrop');
  const pathEl   = document.getElementById('lpDetailPath');
  const winEl    = document.getElementById('lpDetailWindow');
  if (!card) return;
  card.style.display = 'block';
  if (backdrop){ backdrop.classList.add('open'); backdrop.setAttribute('aria-hidden','false'); }
  document.body.classList.add('lp-modal-open');
  card.scrollTop = 0;   // start at the modal's own top on each open
  if (pathEl) pathEl.innerHTML = 'path: <code>' + path.replace(/</g, '&lt;') + '</code>';
  const body = document.getElementById('lpAdTableBody');
  if (body) body.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:14px;color:var(--text-tertiary)">loading…</td></tr>';
  try {
    const hdrs = { apikey: SUPABASE_ANON, Authorization: 'Bearer ' + SUPABASE_ANON };
    const url  = SUPABASE_URL + '/rest/v1/' + LP_AD_TABLE +
                 '?select=*&landing_page_path=eq.' + encodeURIComponent(path) +
                 '&order=spend.desc&limit=500';
    const r = await fetch(url, {headers: hdrs});
    if (!r.ok) throw new Error(LP_AD_TABLE + ' HTTP ' + r.status);
    _lpAdRows = await r.json();
    if (winEl && _lpAdRows[0]){
      winEl.textContent = _lpAdRows[0].window_from + ' → ' + _lpAdRows[0].window_to;
    }
    _lpAdFilter = 'all';
    document.querySelectorAll('.lp-pill').forEach(b => b.classList.toggle('active', b.dataset.lpFilter === 'all'));
    _lpRenderDetail();
  } catch (e){
    if (body) body.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:14px;' +
      'color:var(--error-text, #b94a3d)">Error: ' + (e?.message || e) + '</td></tr>';
  }
}

function _lpApplyFilter(rows){
  if (_lpAdFilter === 'active')  return rows.filter(r => (r.ad_status || '').toUpperCase() === 'ACTIVE');
  if (_lpAdFilter === 'paused')  return rows.filter(r => (r.ad_status || '').toUpperCase() !== 'ACTIVE');
  if (_lpAdFilter === 'winner')  return rows.filter(r => (+r.shopify_roas || 0) >= 2);
  if (_lpAdFilter === 'loser')   return rows.filter(r => (+r.shopify_roas || 0) <  1 && (+r.spend || 0) > 0);
  if (_lpAdFilter === 'gap')     return rows.filter(r => Math.abs(+r.roas_gap_pct || 0) > 40);
  return rows;
}

function _lpRenderDetail(){
  const rows = _lpApplyFilter(_lpAdRows);
  _lpRenderDetailKpis(_lpAdRows);   // KPIs based on FULL page, not the filter
  _lpRenderDetailTable(rows);
  _lpRenderDetailCharts(rows);
  const cntEl = document.getElementById('lpDetailCount');
  if (cntEl) cntEl.textContent = fmtInt(rows.length) + ' of ' + fmtInt(_lpAdRows.length) + ' ads';
}

function _lpRenderDetailKpis(rows){
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  if (!rows.length){
    ['lpDkSess','lpDkSpend','lpDkMROAS','lpDkSROAS','lpDkGap','lpDkAds'].forEach(id => set(id, '—'));
    return;
  }
  let spend = 0, conv = 0, shopSales = 0, shopOrders = 0, active = 0;
  for (const r of rows){
    spend      += +r.spend         || 0;
    conv       += +r.conv_value    || 0;
    shopSales  += +r.shopify_sales || 0;
    shopOrders += +r.shopify_orders|| 0;
    if ((r.ad_status || '').toUpperCase() === 'ACTIVE') active++;
  }
  const paused = rows.length - active;
  const metaRoas = spend > 0 ? (conv     / spend) : 0;
  const shopRoas = spend > 0 ? (shopSales / spend) : 0;
  const gap = conv > 0 ? ((shopSales - conv) / conv * 100) : 0;
  const page = rows[0];
  set('lpDkSess',    fmtInt(page.page_sessions));
  set('lpDkSessSub', 'ATC ' + (+page.page_atc_rate || 0).toFixed(1) + '% · Bounce ' + (+page.page_bounce_rate || 0).toFixed(1) + '%');
  set('lpDkSpend',   '₹' + fmtInt(spend));
  set('lpDkSpendSub', 'cost/session ₹' + (+page.page_cost_per_sess || 0).toFixed(2));
  set('lpDkMROAS',   metaRoas.toFixed(2));
  set('lpDkSROAS',   shopRoas.toFixed(2));
  set('lpDkSROASub', fmtInt(shopOrders) + ' orders · ₹' + fmtInt(shopSales));
  const gapEl = document.getElementById('lpDkGap');
  if (gapEl){
    gapEl.textContent = (gap > 0 ? '+' : '') + gap.toFixed(1) + '%';
    gapEl.style.color = gap < -20 ? 'var(--error-text,#b94a3d)' :
                        (gap >  20 ? 'var(--success-text)'    : '');
  }
  set('lpDkAds',     fmtInt(rows.length));
  set('lpDkAdsSub',  'active ' + active + ' · paused ' + paused);
}

function _lpRenderDetailTable(rows){
  const body = document.getElementById('lpAdTableBody');
  const foot = document.getElementById('lpAdTableFoot');
  if (!body) return;
  if (!rows.length){
    body.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:14px;color:var(--text-tertiary)">No ads match the filter.</td></tr>';
    if (foot) foot.textContent = '—';
    return;
  }
  const _fmtRs   = v => '₹' + fmtInt(+v || 0);
  const _fmtRoas = v => (+v || 0).toFixed(2);
  const _statusPill = st => {
    const s = (st || '').toUpperCase();
    const cls = s === 'ACTIVE' ? 'active' : (s ? 'paused' : '');
    return '<span class="lp-status ' + cls + '">' + (s || '—') + '</span>';
  };
  body.innerHTML = rows.slice(0, 200).map(r => {
    const gap = r.roas_gap_pct;
    const gapCls = gap == null ? '' : (+gap < -15 ? 'lp-gap-neg' : (+gap > 15 ? 'lp-gap-pos' : ''));
    const gapTxt = gap == null ? '—' : ((+gap > 0 ? '+' : '') + (+gap).toFixed(1) + '%');
    const thumb = r.preview_link
      ? '<img src="' + r.preview_link.replace(/"/g, '&quot;') + '" alt="" onerror="this.style.visibility=\'hidden\'">'
      : '';
    const adName = (r.ad_name || '').replace(/</g, '&lt;');
    const sub    = ((r.campaign_name || '—') + ' · ' + (r.adset_name || '—')).replace(/</g, '&lt;');
    return '<tr>' +
      '<td>' + thumb + '</td>' +
      '<td><div class="lp-adname" title="' + adName.replace(/"/g,'&quot;') + '">' + adName +
           ' &nbsp;' + _statusPill(r.ad_status) +
           '<span class="sub">' + sub + '</span></div></td>' +
      '<td class="mono" style="text-align:right">' + fmtInt(r.impressions) + '</td>' +
      '<td class="mono" style="text-align:right">' + _fmtRs(r.spend) + '</td>' +
      '<td class="mono" style="text-align:right">' + _fmtRoas(r.meta_roas) + '</td>' +
      '<td class="mono" style="text-align:right">' + fmtInt(r.shopify_orders) + '</td>' +
      '<td class="mono" style="text-align:right">' + _fmtRs(r.shopify_sales) + '</td>' +
      '<td class="mono" style="text-align:right">' + _fmtRoas(r.shopify_roas) + '</td>' +
      '<td class="mono ' + gapCls + '" style="text-align:right">' + gapTxt + '</td>' +
      '</tr>';
  }).join('');
  if (foot){
    foot.textContent = 'Showing ' + fmtInt(Math.min(200, rows.length)) + ' of ' + fmtInt(rows.length) +
                       ' ads · filter=' + _lpAdFilter;
  }
}

/* Sync ad-table scroll container height to the chart column's natural
   bottom.  Called after the charts render — the two Chart.js canvases have
   fixed heights, so the col height is stable within a paint. */
function _lpSyncTableHeight(){
  const chartCol = document.querySelector('.funnel-card.lp-modal .lp-chart-col');
  const scroll   = document.querySelector('.funnel-card.lp-modal .lp-adtable-scroll');
  if (!chartCol || !scroll) return;
  const h = chartCol.getBoundingClientRect().height;
  if (h > 100) scroll.style.maxHeight = h + 'px';   // guard against pre-render 0
}
// Re-sync on modal resize (viewport rotate, dev-tools toggle, etc.)
window.addEventListener('resize', () => {
  const card = document.getElementById('lpDetailCard');
  if (card && card.style.display !== 'none') _lpSyncTableHeight();
});

function _lpRenderDetailCharts(rows){
  if (typeof Chart === 'undefined') return;
  // ── Bar chart: Meta vs Shopify ROAS for top 12 by spend ──
  const barCanvas = document.getElementById('lpChartBars');
  if (barCanvas){
    const top = rows.slice().sort((a,b) => (+b.spend||0) - (+a.spend||0)).slice(0, 12);
    const labels = top.map((r,i) => {
      const nm = r.ad_name || r.ad_id || '';
      return nm.length > 34 ? nm.slice(0, 32) + '…' : nm;
    });
    if (_lpChartBars) _lpChartBars.destroy();
    _lpChartBars = new Chart(barCanvas.getContext('2d'), {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label: 'Meta ROAS',    data: top.map(r => +r.meta_roas   || 0),
            backgroundColor: '#3B6FD4' },
          { label: 'Shopify ROAS', data: top.map(r => +r.shopify_roas|| 0),
            backgroundColor: '#C0603A' },
        ],
      },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top', align: 'end',
                    labels: {boxWidth: 10, boxHeight: 10, font: {size: 10, family: 'Space Grotesk'}} },
          tooltip:{ callbacks: {
            afterBody(items){
              const r = top[items[0].dataIndex]; if (!r) return '';
              return ['spend ₹' + fmtInt(r.spend),
                      'shop orders ' + fmtInt(r.shopify_orders),
                      'shop sales ₹' + fmtInt(r.shopify_sales),
                      (r.roas_gap_pct == null ? '' : 'gap ' + (+r.roas_gap_pct).toFixed(1) + '%')].filter(Boolean);
            }
          }}
        },
        scales: {
          x: { grid: {color: 'rgba(0,0,0,0.05)'}, ticks: {font: {size: 10}} },
          y: { grid: {display: false}, ticks: {font: {size: 10}} },
        },
      },
    });
  }
  // ── Bubble chart: Spend × Shopify ROAS, size = orders ──
  const bubCanvas = document.getElementById('lpChartBubble');
  if (bubCanvas){
    const _colour = st => (st || '').toUpperCase() === 'ACTIVE' ? '#3D9E6B' : '#B57514';
    const _size = o => Math.max(4, Math.min(28, Math.sqrt(+o || 0) * 1.5));
    const pts = rows.map(r => ({
      x: +r.spend        || 0,
      y: +r.shopify_roas || 0,
      r: _size(r.shopify_orders),
      _ad: r,
      backgroundColor: _colour(r.ad_status) + 'AA',
      borderColor:     _colour(r.ad_status),
    }));
    if (_lpChartBubble) _lpChartBubble.destroy();
    _lpChartBubble = new Chart(bubCanvas.getContext('2d'), {
      type: 'bubble',
      data: { datasets: [{ label: 'ads', data: pts,
                           backgroundColor: pts.map(p => p.backgroundColor),
                           borderColor:     pts.map(p => p.borderColor) }] },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: {
            label(item){
              const p = item.raw?._ad; if (!p) return '';
              return [
                (p.ad_name || p.ad_id || '').slice(0, 60),
                (p.ad_status || '') + ' · ' + p.campaign_name,
                'spend ₹' + fmtInt(p.spend) + ' · shop ROAS ' + (+p.shopify_roas || 0).toFixed(2),
                'orders ' + fmtInt(p.shopify_orders) + ' · gap ' +
                  (p.roas_gap_pct == null ? '—' : (+p.roas_gap_pct).toFixed(1) + '%'),
              ];
            }
          }}
        },
        scales: {
          x: { title: {display:true, text:'Ad Spend (₹)', font:{size:10}},
               grid: {color:'rgba(0,0,0,0.05)'},
               ticks: {font:{size:10}, callback: v => '₹' + fmtInt(v)} },
          y: { title: {display:true, text:'Shopify ROAS', font:{size:10}},
               grid: {color:'rgba(0,0,0,0.05)'},
               ticks: {font:{size:10}} },
        },
      },
    });
  }
  // Charts are now instantiated. Chart.js sizes canvases synchronously on
  // creation, but the first layout is queued for the next frame — sync
  // after paint so getBoundingClientRect() reads the settled height.
  requestAnimationFrame(() => _lpSyncTableHeight());
}

async function loadInventory(){
  // Shopify Admin GraphQL doesn't send CORS headers so the browser can't
  // hit it directly — we read from the public.shopify_products cache in
  // Supabase instead. That table is refreshed by
  // backend/sync_shopify_products.py (run any time / on a cron). The
  // Shopify config panel is still available so anyone can PIN their own
  // shop's cache (future — right now the pipeline only serves the
  // configured shop).
  const footer = document.getElementById('invFooter');
  if (!SUPABASE_URL || !SUPABASE_ANON){
    footer.innerHTML = '⚠️ Supabase URL/key missing — inventory cache unreachable.';
    return;
  }
  footer.innerHTML = 'Loading products from cache <span class="spinner"></span>';
  invProducts = [];
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   Prefer:'count=none'};
  const cols = 'id,title,status,vendor,product_type,handle,image_url,tags,'+
               'variant_count,total_inventory,price_min,price_max,'+
               'created_at_shop,updated_at_shop,synced_at,sizes_json';
  let offset = 0, BATCH = 1000, lastSynced = null;
  try {
    while (true){
      const url = SUPABASE_URL + '/rest/v1/shopify_products?select=' + cols +
                  '&order=updated_at_shop.desc&limit=' + BATCH + '&offset=' + offset;
      const r = await fetch(url, {headers});
      if (!r.ok){
        const txt = await r.text().catch(() => '');
        throw new Error('HTTP ' + r.status + ' — ' + txt.slice(0, 120));
      }
      const j = await r.json();
      if (!Array.isArray(j) || !j.length) break;
      // Adapt to the shape the renderer expects (matches the old GraphQL result)
      for (const p of j){
        invProducts.push({
          id:              p.id,
          title:           p.title,
          status:          p.status,
          vendor:          p.vendor,
          productType:     p.product_type,
          handle:          p.handle,
          createdAt:       p.created_at_shop,
          updatedAt:       p.updated_at_shop,
          totalInventory:  p.total_inventory,
          featuredImage:   p.image_url ? {url: p.image_url} : null,
          priceRangeV2: {
            minVariantPrice: {amount: p.price_min},
            maxVariantPrice: {amount: p.price_max},
          },
          variantsCount: {count: p.variant_count},
          // Tags used by invLifecycle() to detect discontinued products.
          tags: Array.isArray(p.tags) ? p.tags : (p.tags ? [p.tags] : []),
          // Per-size stock breakdown (populated by sync_shopify_products.py
          // from Shopify GraphQL variants.selectedOptions). Missing on
          // pre-migration rows — render code treats null/{} identically.
          sizes: (p.sizes_json && typeof p.sizes_json === 'object') ? p.sizes_json : null,
        });
        if (p.synced_at && (!lastSynced || p.synced_at > lastSynced)) lastSynced = p.synced_at;
      }
      footer.innerHTML = 'Loading… ' + invProducts.length.toLocaleString() +
                          ' products fetched <span class="spinner"></span>';
      if (j.length < BATCH) break;
      offset += BATCH;
    }
  } catch (err){
    footer.innerHTML = '❌ Inventory cache fetch failed: ' + String(err.message || err) +
      '<br><span style="font-size:11px;color:var(--text-tertiary)">The cache is populated by ' +
      '<code>backend/sync_shopify_products.py</code> — run it to refresh.</span>';
    return;
  }
  const lastTxt = lastSynced
    ? ' · last synced ' + new Date(lastSynced).toLocaleString()
    : '';
  footer.textContent = 'Loaded ' + invProducts.length.toLocaleString() +
    ' products from cache' + lastTxt;

  // Overlay DoQ metrics from product_doq_daily. This is a small side-
  // fetch (~6k rows) that maps to invProducts by product_id — matched
  // to the latest date_day so we always show the freshest snapshot.
  try {
    const doqUrl = SUPABASE_URL + '/rest/v1/product_doq_daily' +
      '?select=product_id,date_day,daily_quantity,doq_30,doq_45,oos_days_30' +
      '&order=date_day.desc&limit=20000';
    const dr = await fetch(doqUrl, {headers});
    if (dr.ok){
      const drows = await dr.json();
      // First occurrence wins because we sorted by date_day desc — that
      // is the freshest snapshot per product.
      const byProd = {};
      for (const d of drows){
        if (!byProd[d.product_id]) byProd[d.product_id] = d;
      }
      for (const p of invProducts){
        const d = byProd[p.id];
        p.doq_daily = d?.daily_quantity ?? null;
        p.doq_30    = d?.doq_30         ?? null;
        p.doq_45    = d?.doq_45         ?? null;
        p.oos_30    = d?.oos_days_30    ?? null;
      }
    }
  } catch (e){ /* DoQ overlay is best-effort — table still works without it */ }

  invPopulateFilterOptions();
  renderInventoryKpis();
  renderInventoryTable();
}

function _invLcSet(prefix, items){
  // items = list of products already classified into one lifecycle bucket
  const t = items.length;
  const inS  = items.filter(p => invStockStatus(p) === 'in_stock').length;
  const lowS = items.filter(p => { const s = invStockStatus(p);
                                    return s === 'low_stock' || s === 'broken_stock'; }).length;
  const outS = items.filter(p => invStockStatus(p) === 'out_of_stock').length;
  const inPct  = t ? Math.round(inS  / t * 100) : 0;
  const lowPct = t ? Math.round(lowS / t * 100) : 0;
  const outPct = t ? Math.round(outS / t * 100) : 0;
  document.getElementById(prefix + 'Cnt').textContent = fmtInt(t);
  document.getElementById(prefix + 'In').style.width  = inPct  + '%';
  document.getElementById(prefix + 'Low').style.width = lowPct + '%';
  document.getElementById(prefix + 'Out').style.width = outPct + '%';
  document.getElementById(prefix + 'InPct').textContent  = inPct  + '%';
  document.getElementById(prefix + 'OutPct').textContent = outPct + '%';
}

function renderInventoryKpis(){
  // All KPIs computed on the CURRENT FILTER'd set so numbers stay in
  // sync with the visible table. Lifecycle summary cards use the full
  // dataset — that's the "big picture" panel at the top.
  const filt = invFiltered();
  document.getElementById('invKpTotal').textContent = fmtInt(filt.length);
  document.getElementById('invKpNpd'  ).textContent = fmtInt(filt.filter(p => invLifecycle(p) === 'npd').length);
  document.getElementById('invKpIn'   ).textContent = fmtInt(filt.filter(p => invStockStatus(p) === 'in_stock').length);
  const lowSet = filt.filter(p => { const s = invStockStatus(p);
                                    return s === 'low_stock' || s === 'broken_stock'; });
  document.getElementById('invKpLow'  ).textContent = fmtInt(lowSet.length);
  document.getElementById('invKpOOS'  ).textContent = fmtInt(filt.filter(p => invStockStatus(p) === 'out_of_stock').length);

  // Lifecycle summary cards use the FULL dataset (before filters) so
  // the top-of-page tally always shows the total shape of the catalogue.
  const npd  = invProducts.filter(p => invLifecycle(p) === 'npd');
  const act  = invProducts.filter(p => invLifecycle(p) === 'active');
  const disc = invProducts.filter(p => invLifecycle(p) === 'discontinued');
  _invLcSet('invLcNpd',  npd);
  _invLcSet('invLcAct',  act);
  _invLcSet('invLcDisc', disc);
}

function invFiltered(){
  let rows = invProducts.slice();
  if (invState.status)    rows = rows.filter(p => (p.status||'').toUpperCase() === invState.status);
  if (invState.category)  rows = rows.filter(p => (p.productType||'').trim() === invState.category);
  if (invState.type)      rows = rows.filter(p => (p.productType||'').trim() === invState.type);
  if (invState.lifecycle) rows = rows.filter(p => invLifecycle(p) === invState.lifecycle);
  if (invState.stock){
    if (invState.stock === 'low_stock'){
      rows = rows.filter(p => { const s = invStockStatus(p);
                                return s === 'low_stock' || s === 'broken_stock'; });
    } else {
      rows = rows.filter(p => invStockStatus(p) === invState.stock);
    }
  }
  if (invState.excludeNoImg) rows = rows.filter(p => p.featuredImage?.url);
  if (invState.search){
    const q = invState.search.toLowerCase().trim();
    rows = rows.filter(p => ((p.title||'')+' '+(p.handle||'')+' '+(p.vendor||'')+' '+(p.productType||'')).toLowerCase().includes(q));
  }
  return rows;
}

// One <td> per canonical size. Colour by qty vs LOW_BREAK_MAX.  Products
// that don't carry a given size get a muted "—".
function invSizeCells(sizes){
  return INV_SIZE_ORDER_TABLE.map(s => {
    const has = sizes && Object.prototype.hasOwnProperty.call(sizes, s);
    if (!has) return '<td class="num inv-sz-cell inv-sz-none">—</td>';
    const q  = Number(sizes[s]) || 0;
    const cls = q === 0 ? 'inv-sz-out' :
                q <= INV_LOW_BREAK_MAX ? 'inv-sz-low' : 'inv-sz-in';
    return '<td class="num inv-sz-cell '+cls+'" title="'+s+' · '+q.toLocaleString('en-IN')+' units">'+
             fmtInt(q) +
           '</td>';
  }).join('');
}

// Populate the Category + Type dropdowns from the loaded data. Called
// once on load and again after any Refresh.
function invPopulateFilterOptions(){
  const cats  = new Set();
  const types = new Set();
  for (const p of invProducts){
    if (p.productType) cats.add(p.productType);
    if (p.productType) types.add(p.productType);
  }
  const catSel  = document.getElementById('invCategory');
  const typeSel = document.getElementById('invType');
  if (catSel){
    const cur = catSel.value;
    catSel.innerHTML = '<option value="">All Categories</option>' +
      [...cats].sort().map(c => '<option value="'+c.replace(/"/g,'&quot;')+'">'+c+'</option>').join('');
    catSel.value = cur;
  }
  if (typeSel){
    const cur = typeSel.value;
    typeSel.innerHTML = '<option value="">All Types</option>' +
      [...types].sort().map(c => '<option value="'+c.replace(/"/g,'&quot;')+'">'+c+'</option>').join('');
    typeSel.value = cur;
  }
}

const INV_LC_BADGE = {
  npd:          { cls:'lc-npd-pill',  label:'🌱 NPD' },
  active:       { cls:'lc-act-pill',  label:'Active' },
  discontinued: { cls:'lc-disc-pill', label:'Discontinued' },
};
const INV_STOCK_BADGE = {
  in_stock:     { cls:'ss-in',    label:'In stock' },
  low_stock:    { cls:'ss-low',   label:'Low' },
  broken_stock: { cls:'ss-low',   label:'Broken' },
  out_of_stock: { cls:'ss-out',   label:'Out' },
};

function renderInventoryTable(){
  const rows = invFiltered();
  const tbody = document.querySelector('#invTbl tbody');
  tbody.innerHTML = rows.map(p => {
    const img = p.featuredImage?.url
      ? '<img class="thumb" loading="lazy" src="' + p.featuredImage.url + '">'
      : '<div class="thumb-ph">no img</div>';
    const minP = +p.priceRangeV2?.minVariantPrice?.amount || 0;
    const maxP = +p.priceRangeV2?.maxVariantPrice?.amount || 0;
    const priceTxt = minP === maxP ? fmtRs(minP) : fmtRs(minP) + ' – ' + fmtRs(maxP);
    const dq  = p.doq_daily != null ? fmtInt(p.doq_daily) : '—';
    const d30 = p.doq_30    != null ? (+p.doq_30).toFixed(2) : '—';
    const d45 = p.doq_45    != null ? (+p.doq_45).toFixed(2) : '—';
    const oo  = p.oos_30    != null ? fmtInt(p.oos_30) : '—';
    const lc  = invLifecycle(p);
    const ss  = invStockStatus(p);
    const lcB = INV_LC_BADGE[lc] || INV_LC_BADGE.active;
    const ssB = INV_STOCK_BADGE[ss] || INV_STOCK_BADGE.in_stock;
    const daysOld = p.createdAt ? Math.floor((Date.now() - Date.parse(p.createdAt)) / 86400000) : null;
    const lcSub = (lc === 'npd' && daysOld != null) ? ' <span class="lc-sub">'+daysOld+'d</span>' : '';
    return '<tr>'+
      '<td>'+img+'</td>'+
      '<td class="title-cell">'+
        '<div class="title">'+ (p.title || '—') +'</div>'+
        '<div class="handle">'+ (p.handle || '') +'</div>'+
      '</td>'+
      '<td><span class="lc-pill '+lcB.cls+'">'+lcB.label+'</span>'+lcSub+'</td>'+
      '<td>'+(p.productType || '—')+'</td>'+
      '<td><span class="ss-pill '+ssB.cls+'">'+ssB.label+'</span></td>'+
      '<td class="num">'+fmtInt(p.totalInventory)+'</td>'+
      '<td class="num">'+dq +'</td>'+
      '<td class="num">'+d30+'</td>'+
      '<td class="num">'+d45+'</td>'+
      '<td class="num">'+oo +'</td>'+
      invSizeCells(p.sizes) +
      '<td class="num">'+priceTxt+'</td>'+
    '</tr>';
  }).join('');
  const tot = invProducts.length;
  const footer = document.getElementById('invFooter');
  if (footer){
    footer.textContent = 'Showing ' + rows.length.toLocaleString() +
                          ' of ' + tot.toLocaleString() + ' products';
  }
}

// Filter change wiring — every dropdown just updates state + re-renders.
function _invBindSelect(id, key){
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener('change', () => { invState[key] = el.value; renderInventoryKpis(); renderInventoryTable(); });
}
_invBindSelect('invLifecycle', 'lifecycle');
_invBindSelect('invStock',     'stock');
_invBindSelect('invCategory',  'category');
_invBindSelect('invStatus',    'status');
_invBindSelect('invType',      'type');

document.getElementById('invResetFilters')?.addEventListener('click', () => {
  invState.lifecycle = invState.stock = invState.category = invState.status = invState.type = '';
  invState.search = ''; invState.excludeNoImg = false;
  ['invLifecycle','invStock','invCategory','invStatus','invType'].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = '';
  });
  const s = document.getElementById('invSearch'); if (s) s.value = '';
  const c = document.getElementById('invExcludeNoImg'); if (c) c.checked = false;
  renderInventoryKpis(); renderInventoryTable();
});

let invSearchDb = null;
document.getElementById('invSearch').addEventListener('input', e => {
  clearTimeout(invSearchDb);
  invSearchDb = setTimeout(() => { invState.search = e.target.value; renderInventoryKpis(); renderInventoryTable(); }, 200);
});
document.getElementById('invExcludeNoImg').addEventListener('change', e => {
  invState.excludeNoImg = e.target.checked;
  renderInventoryKpis(); renderInventoryTable();
});
document.getElementById('invRefresh').onclick = () => { VIEW_LOADED.inventory = false; loadInventory(); VIEW_LOADED.inventory = true; };

// ── Shopify config panel ─────────────────────────────────────────────
// Small toolbar-panel that reads/writes shopify.cfg in localStorage.
// Token stored as a plain string — this is a private dashboard and
// the same token would otherwise sit in the URL address bar, so
// localStorage is no worse. Cleared via the "Clear" button.
function _invConfigPopulate(){
  const cfg = _readShopifyCfg();
  document.getElementById('invCfgDomain').value = cfg.domain;
  document.getElementById('invCfgToken' ).value = cfg.token;
  document.getElementById('invCfgApiVer').value = cfg.apiVer;
  const st = document.getElementById('invCfgStatus');
  if (cfg.domain && cfg.token){
    st.textContent = '✓ credentials stored — shop=' + cfg.domain + ' · api ' + cfg.apiVer;
    st.style.color = 'var(--success-text, #2E7755)';
  } else {
    st.textContent = 'No credentials stored. Enter them above to fetch inventory.';
    st.style.color = 'var(--text-tertiary)';
  }
}
document.getElementById('invBtnConfig').onclick = () => {
  const p = document.getElementById('invConfigPanel');
  const open = p.style.display === 'none';
  p.style.display = open ? 'block' : 'none';
  if (open) _invConfigPopulate();
};
document.getElementById('invCfgSave').onclick = () => {
  const domain = (document.getElementById('invCfgDomain').value || '').trim();
  const token  = (document.getElementById('invCfgToken' ).value || '').trim();
  const apiVer = (document.getElementById('invCfgApiVer').value || '2025-10').trim();
  const st = document.getElementById('invCfgStatus');
  if (!/^[a-z0-9\-]+\.myshopify\.com$/i.test(domain)){
    st.textContent = '✗ domain must look like your-shop.myshopify.com';
    st.style.color = '#B33A2A'; return;
  }
  if (!token.startsWith('shpat_') && !token.startsWith('shpca_') && token.length < 20){
    st.textContent = '✗ token doesn\'t look like an admin API token (shpat_...)';
    st.style.color = '#B33A2A'; return;
  }
  try {
    localStorage.setItem('shopify.cfg', JSON.stringify({domain, token, apiVer}));
  } catch (e){
    st.textContent = '✗ could not write to localStorage: ' + e.message;
    st.style.color = '#B33A2A'; return;
  }
  SHOPIFY_DOMAIN  = domain;
  SHOPIFY_TOKEN   = token;
  SHOPIFY_API_VER = apiVer;
  st.textContent = '✓ credentials stored — the browser reads inventory from ' +
                   'the Supabase cache. Run backend/sync_shopify_products.py to refresh the cache.';
  st.style.color = 'var(--success-text, #2E7755)';
  VIEW_LOADED.inventory = false;
  loadInventory();
  VIEW_LOADED.inventory = true;
};
document.getElementById('invCfgClear').onclick = () => {
  localStorage.removeItem('shopify.cfg');
  SHOPIFY_DOMAIN = ''; SHOPIFY_TOKEN = '';
  document.getElementById('invCfgDomain').value = '';
  document.getElementById('invCfgToken' ).value = '';
  const st = document.getElementById('invCfgStatus');
  st.textContent = '✓ credentials cleared from this browser';
  st.style.color = 'var(--text-tertiary)';
};
document.getElementById('invExport').onclick = () => {
  const rows = invFiltered();
  if (!rows.length) return;
  // Match whatever the Inventory table is currently showing — every
  // filter facet (status / lifecycle / stock / category / type) is
  // already baked into invFiltered().  Column order + labels come from
  // the visible <thead>.
  exportVisibleTableCsv('#invTbl', rows, { filenamePrefix: 'inventory' });
};

(async ()=>{
  // Default Creative Testing date window = last 30 days INCLUDING today,
  // matching the OLD dashboard's behaviour. Set the dates first so
  // fetchPrimaryAggregated() / cache lookup uses the right window.
  applyDatePreset('last30');

  // Try the pre-computed cache (results_table.ads_json) first — one HTTP
  // fetch of a single JSONB blob instead of paging 260k+ rows from
  // primary_table. Two variants are precomputed: date_field='delivery'
  // and 'created'; pass the current field-selector so we hit the right
  // one. The dashboard defaults to state.dateField='created' so the
  // 'created' variant matters on every fresh load.
  const cachedPromise = fetchPrimaryFromCache(state.dateFrom || '', state.dateTo || '', state.dateField)
    .catch(() => null);

  // Kick off ae_table_view + thumbnails + reach-recent + freq-lifecycle
  // + ctype-overrides in parallel. Overrides is a small table (< dozens of
  // rows expected) so it never gates the load.
  const [cached, freshAllAds, freshThumbs, freshReach, freshFreq, freshOverrides, freshAssetIds] = await Promise.all([
    cachedPromise, fetchAds(), fetchThumbnails(), fetchReachRecent(), fetchFreqLifecycle(),
    fetchCtypeOverrides(), fetchAssetIds()
  ]);
  allAds = freshAllAds; thumbsByAdId = freshThumbs;
  reachRecentByAdId = freshReach;
  freqLifecycleByAdId = freshFreq;
  ctypeOverrideByAdId = freshOverrides || {};
  assetIdByAdId       = freshAssetIds || {};
  // Merge the reach-recent snapshot + manual asset_id mapping into each
  // ae row by ad_id so renderAE can display them inline and the header
  // sort by data-sort="asset_id" hits r.asset_id directly.
  for (const r of allAds){
    const rr = reachRecentByAdId[r.ad_id];
    if (rr){
      r.previous_reach                   = rr.previous_reach;
      r.latest_reach                     = rr.latest_reach;
      r.incremental_reach                = rr.incremental_reach;
      r.cost_per_incremental_reach       = rr.cost_per_incremental_reach;
      r.cost_per_1000_incremental_reach  = rr.cost_per_1000_incremental_reach;
      r.latest_spend                     = rr.latest_spend;
    }
    r.asset_id = (r.ad_id && assetIdByAdId[r.ad_id]) || '';
  }

  if (cached && cached.length){
    primaryAds = cached;            // fast path — already aggregated server-side
  } else {
    // Slow path: live aggregation from primary_table
    primaryAds = await fetchPrimaryAggregated(state.dateFrom || '', state.dateTo || '');
  }
  enrichPrimaryDates();
  // Refresh ae_table_view rows' category with the current working thresholds
  // (the DB column may have been baked with an older F4 value).
  aeApplyCurrentThresholds();
  // Campaign filter dropdown is shared by Creative Testing — populate from
  // the primary-derived set so its options match the visible KPI counts.
  populateCampaignDropdown(primaryAds);
  rerender();
})();
