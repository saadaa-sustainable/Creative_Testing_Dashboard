/* ============================================================
   v2 — data + interactions
   ============================================================ */
const params      = new URLSearchParams(window.location.search);
const SUPABASE_URL  = params.get('supabaseUrl')  || '';
const SUPABASE_ANON = params.get('supabaseAnon') || '';
const dbStat = document.getElementById('dbStat');

const fmtInt = n => (n==null||isNaN(n)) ? '—' : Math.round(+n).toLocaleString('en-IN');
const fmtRs  = n => (n==null||isNaN(n)) ? '—' : '₹' + fmtInt(n);
const fmtRoas= n => (n==null||isNaN(n)) ? '—' : (+n).toFixed(2)+'x';
// Global 2-decimal formatter — used by the Incremental Reach modal and
// anywhere else outside renderAE()'s local scope.
const fmtNum2 = v => (v==null||v===''||isNaN(+v)) ? '—' : (+v).toLocaleString('en-IN',{minimumFractionDigits:2, maximumFractionDigits:2});

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
             ctFormat:false,                 // only ads with name starting CLP-/CTP-
             search:'', searchMode:'contains'};
const CT_FORMAT_RE = /^(CLP-|CTP-)/i;
const IS_COPY_RE   = /copy/i;

function detectCtype(name){
  const n = (name||'').toUpperCase();
  if(n.includes('IFAD'))  return 'IFAD';
  if(n.includes('GAD'))   return 'Graphic AD';
  if(n.includes('VRP')||n.includes('NNC')||n.includes('VIDEO')||n.includes('IGP')||n.includes('NO-ID')||/^VID-AD/.test(n)) return 'VID';
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
   applied (50000 / 3.2 / 525 / 12).  Returns per-ad rows shaped like
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
   results_sync.py writes this row once per accounts_group with everything
   already aggregated, so the dashboard can render in 1 HTTP fetch instead
   of paging primary_table. Returns null when no usable cache exists or its
   window doesn't match the requested one. */
async function fetchPrimaryFromCache(dateFrom, dateTo){
  if (!SUPABASE_URL || !SUPABASE_ANON) return null;
  const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                   Prefer:'count=none'};
  dbStat.innerHTML = 'Loading cached results_table <span class="spinner"></span>';
  const url = SUPABASE_URL +
    '/rest/v1/results_table?account_name=eq.All%20Accounts' +
    '&select=ads_json,data_date_from,data_date_to,computed_at' +
    '&order=computed_at.desc&limit=1';
  const r = await fetch(url, {headers});
  if (!r.ok) return null;
  const j = await r.json();
  if (!Array.isArray(j) || !j.length) return null;
  const row = j[0];
  // Cache window must match (or be a 1-day superset of) the requested window
  // for us to trust it.
  if (dateFrom && row.data_date_from !== dateFrom) return null;
  if (dateTo   && row.data_date_to   !== dateTo  ) return null;
  const ads = row.ads_json || [];
  // Map compact cache keys → the same row shape fetchPrimaryAggregated emits
  const t = (typeof aeReadThresholds === 'function')
            ? aeReadThresholds()
            : {f1:50000, f2:3.2, f3:525, f4:12};
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
            : {f1:50000, f2:3.2, f3:525, f4:12};
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
            : {f1:50000, f2:3.2, f3:525, f4:12};
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
  // CT Format — restrict to ads whose name starts with "CLP-" or "CTP-"
  // (the Creative-Testing-sheet naming convention).
  if (state.ctFormat){
    r = r.filter(x => CT_FORMAT_RE.test(String(x.ad_name || '')));
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
// Creative Focus is limited to the three canonical types (IFAD / GAD / VID);
// everything else (BST, ADB, UGC, BR, Brand, ...) falls into Others so it can
// be redirected to the Product-in-Focus taxonomy above.
function classifyCreative(adName){
  const s = adName || '';
  if (/(^|[^A-Za-z])VID([^A-Za-z]|$)/.test(s)) return 'VID';
  if (/IFAD/i.test(s)) return 'IFAD';
  if (/GAD/i.test(s))  return 'GAD';
  return 'OTHER';
}
function renderFocusStrips(rows){
  const p = {'Home page':0,'Category page':0,'Collection page':0,'Product page':0,'Others':0};
  const c = {IFAD:0,GAD:0,VID:0,OTHER:0};
  for (const r of rows){
    p[classifyProduct(r.ad_name)] += 1;
    c[classifyCreative(r.ad_name)] += 1;
  }
  const prodKeys = ['Home page','Category page','Collection page','Product page','Others'];
  document.querySelectorAll('#prodStrip .focus-pill').forEach((el, i) => {
    const k = prodKeys[i];
    el.querySelector('b').textContent = fmtInt(p[k] || 0);
    el.dataset.btype = 'product'; el.dataset.bval = k;
    el.classList.toggle('disabled', (p[k]||0) === 0);
  });
  const creativeKeys = ['IFAD','GAD','VID'];
  const creativeLabels = ['IFAD','GAD','VID'];
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
    return rows.filter(r => classifyCreative(r.ad_name) === bucketState.val);
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
    openBucketModal(pill.dataset.btype, pill.dataset.bval, pill.dataset.blabel);
  });
}
bindFocusStrip('prodStrip');
bindFocusStrip('creativeStrip');

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
  const entry = r.ad_id ? thumbsByAdId[r.ad_id] : null;
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
    return {...b, code: top ? top[0] : lpUrlSlug(b.url)};
  }).sort((a,b) => b.count - a.count || b.spend - a.spend);
  const total = list.reduce((s,b) => s + b.count, 0);
  document.getElementById('lpFocusTotal').textContent =
    fmtInt(list.length) + ' landing pages · ' + fmtInt(total) + ' ads';
  if (!list.length){
    host.innerHTML = '<div class="empty" style="padding:24px;text-align:center;color:var(--text-tertiary);font-size:12.5px">No ads have a landing-page URL in the current filter.</div>';
    return;
  }
  const top = list.slice(0, 40);   // cap visible rows
  host.innerHTML = '<table class="lp-tbl">'+
    '<thead><tr>'+
      '<th style="width:90px">Product</th>'+
      '<th>Landing Page</th>'+
      '<th class="num" style="width:110px">Spend</th>'+
      '<th class="num" style="width:80px">Ads</th>'+
    '</tr></thead>'+
    '<tbody>'+
      top.map(b => '<tr data-lp-url="'+b.url.replace(/"/g,'&quot;')+'">'+
        '<td><span class="lp-code" title="most common product token across ads at this URL">'+b.code+'</span></td>'+
        '<td><a class="lp-link" href="'+b.url+'" target="_blank" rel="noopener">'+b.url+'</a></td>'+
        '<td class="num">'+fmtRs(b.spend)+'</td>'+
        '<td class="num">'+fmtInt(b.count)+'</td>'+
      '</tr>').join('')+
    '</tbody></table>';
  // Row click → bucket modal filtered to that URL (ignore clicks on the link itself)
  host.querySelectorAll('tr[data-lp-url]').forEach(tr => {
    tr.addEventListener('click', e => {
      if (e.target.closest('a')) return;
      openLandingPageBucket(tr.dataset.lpUrl);
    });
  });
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
  primaryAds = await fetchPrimaryAggregated(state.dateFrom || '', state.dateTo || '');
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
_ctToggleSync('fCtFormat', state.ctFormat, {on:'CT Format only', off:'CT Format'});
document.getElementById('fExclCopy').addEventListener('click', () => {
  state.exclCopy = !state.exclCopy;
  _ctToggleSync('fExclCopy', state.exclCopy, {on:'Excl. copy', off:'Incl. copy'});
  rerender();
});
document.getElementById('fCtFormat').addEventListener('click', () => {
  state.ctFormat = !state.ctFormat;
  _ctToggleSync('fCtFormat', state.ctFormat, {on:'CT Format only', off:'CT Format'});
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
document.getElementById('btnExport').onclick = ()=>{
  const rows = filtered(primaryAds);
  const cols = ['ad_id','ad_name','account_name','category','amount_spent','roas_ma',
                'ftewv_count','ncp_count','shopify_orders','shopify_sales','shopify_top_tier'];
  const lines = [cols.join(',')];
  for (const r of rows) lines.push(cols.map(c=>JSON.stringify(r[c]??'')).join(','));
  const blob = new Blob([lines.join('\n')],{type:'text/csv'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'creative_testing_v2_'+(new Date().toISOString().slice(0,10))+'.csv';
  a.click();
};

/* ────────────────────────────────────────────────────────────────────
   VIEW SWITCHER — sidebar nav between Testing / Lifecycle / AE / Inventory
   ──────────────────────────────────────────────────────────────────── */
const VIEW_LOADED = {testing:true, lifecycle:false, ae:false, adintel:false, inventory:false, hreach:false};
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
    // Flip mode BEFORE the view renders so it reads the right flag.
    if (v in historicMode) historicMode[v] = isHistoric;
    document.querySelectorAll('.sb-item').forEach(s => s.classList.remove('active'));
    it.classList.add('active');
    document.querySelectorAll('.view').forEach(vv => vv.style.display = 'none');
    const target = document.getElementById('view-' + v);
    if (target) { target.style.display = 'block'; }
    // Toggle the "Historic (Lifetime)" banner on the view's page header
    const banner = target?.querySelector('.historic-banner');
    if (banner) banner.style.display = isHistoric ? 'inline-flex' : 'none';
    // Lazy-load per-view data
    if (v === 'lifecycle' && allAds.length) renderLifecycle();
    if (v === 'ae'        && allAds.length) renderAE();
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
    // Auto-close the mobile sidebar after selection
    if (window.innerWidth <= 900) document.getElementById('sidebar').classList.remove('open');
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
  const today = new Date(); today.setHours(0,0,0,0);
  const rows = allAds.filter(a => {
    if (!a.ad_created) return false;
    return Math.floor((today - new Date(a.ad_created)) / 86400000) >= 14;
  });
  const cols = ['ad_id','ad_name','ad_created','ad_status','category',
                'f1_pass','f2_pass','f3_pass','f4_pass',
                'impressions','reach','frequency','ltv_frequency',
                'amount_spent','roas_ma','ftewv_count','ncp_count'];
  const lines = [cols.join(',')];
  rows.forEach(r => lines.push(cols.map(c => JSON.stringify(r[c] ?? '')).join(',')));
  const blob = new Blob([lines.join('\n')], {type:'text/csv'});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = 'lifecycle_14day_buffer_' + (new Date().toISOString().slice(0,10)) + '.csv'; a.click();
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
let aeRules    = [];            // [{field, op, value}, …] for multi-filter
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
// Windowed reach fetch — pulls daily reach + spend rows for the AE date range
// from primary_table + backfill_table, then per ad_id extracts:
//   prev_reach     = reach on the earliest reporting day in the window
//   latest_reach   = reach on the latest reporting day in the window
//   latest_spend   = spend on that latest day (for the Cost/1k Incr calc)
// The Incremental Reach column is populated with latest_reach (matches the
// materialized-view semantic — "reach the ad delivered on its most recent
// day inside the window").
async function fetchAeWindowReach(){
  const from = document.getElementById('aeDateFrom').value || '';
  const to   = document.getElementById('aeDateTo').value   || '';
  const key  = from + '|' + to;
  if (!from || !to){
    aeWindowReachByAdId = {}; _aeWindowReachKey = ''; return;
  }
  if (key === _aeWindowReachKey) return;
  _aeWindowReachKey = key;
  if (!SUPABASE_URL || !SUPABASE_ANON){ aeWindowReachByAdId = {}; return; }
  try {
    const headers = {apikey:SUPABASE_ANON, Authorization:'Bearer '+SUPABASE_ANON,
                     Prefer:'count=none'};
    const cols = 'ad_id,date,reach,amount_spent_inr';
    const fetchAll = async (tbl) => {
      let out = [], offset = 0, BATCH = 5000;
      while (true){
        const url = SUPABASE_URL + '/rest/v1/' + tbl + '?select=' + cols +
                    '&date=gte.' + from + '&date=lte.' + to +
                    '&reach=not.is.null&reach=gt.0' +
                    '&limit=' + BATCH + '&offset=' + offset;
        const r = await fetch(url, {headers});
        if (!r.ok) break;
        const chunk = await r.json();
        if (!Array.isArray(chunk) || !chunk.length) break;
        out = out.concat(chunk);
        if (chunk.length < BATCH) break;
        offset += BATCH;
        if (offset > 400000) break;
      }
      return out;
    };
    const [prim, back] = await Promise.all([
      fetchAll('primary_table').catch(() => []),
      fetchAll('backfill_table').catch(() => []),
    ]);
    // Merge with primary winning on overlap (freshest sync)
    const bestByKey = new Map();
    for (const r of back) bestByKey.set(r.ad_id + '|' + r.date, r);
    for (const r of prim) bestByKey.set(r.ad_id + '|' + r.date, r);
    // Per-ad reduce — matches the semantics of the old Incremental Reach
    // Analysis modal (group_by = ad_id):
    //   prev_reach    = reach on the FIRST reporting day in window
    //   latest_reach  = reach on the LAST  reporting day in window
    //   total_spend   = SUM(spend) across every reporting day in window
    // The overlay computes incr = latest − prev and CPK = total × 1000 / incr.
    const agg = {};
    for (const r of bestByKey.values()){
      const id = r.ad_id; if (!id || !r.date) continue;
      const d  = r.date.slice(0,10);
      const spend = +r.amount_spent_inr || 0;
      const reach = +r.reach || 0;
      let a = agg[id];
      if (!a){
        a = agg[id] = {
          prev_date: d, prev_reach: reach,
          latest_date: d, latest_reach: reach,
          total_spend: spend,
        };
        continue;
      }
      a.total_spend += spend;
      if (d < a.prev_date){
        a.prev_date = d; a.prev_reach = reach;
      }
      if (d > a.latest_date){
        a.latest_date = d; a.latest_reach = reach;
      }
    }
    aeWindowReachByAdId = agg;
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
const AE_MF_FIELDS = [
  {key:'name_combined', label:'Name (Ad/Camp)'},
  {key:'ad_name',       label:'Ad Name'},
  {key:'campaign_name', label:'Campaign'},
  {key:'adset_id',      label:'Adset ID'},
  {key:'ad_id',         label:'Ad ID'},
  {key:'category',      label:'Category'},
  {key:'ad_status',     label:'Status'},
  {key:'account_name',  label:'Account'},
];
const AE_MF_OPS = [
  {key:'contains_all',  label:'contains all of',  ph:'Keywords separated by space'},
  {key:'contains_any',  label:'contains any of',  ph:'Any of these keywords'},
  {key:'equals',        label:'equals',           ph:'Exact match (case-insensitive)'},
  {key:'not_contains',  label:'does not contain', ph:'Exclude these keywords'},
];
function aeMfFieldValue(row, key){
  if (key === 'name_combined') return ((row.ad_name||'') + ' ' + (row.campaign_name||'')).trim();
  return row[key] || '';
}
function aeMfMatch(row, rule){
  const raw = String(rule.value || '').trim();
  if (!raw) return true;                              // empty value = ignore rule
  const v = String(aeMfFieldValue(row, rule.field)).toLowerCase();
  const toks = raw.toLowerCase().split(/\s+/).filter(Boolean);
  if (rule.op === 'contains_all')  return toks.every(t => v.includes(t));
  if (rule.op === 'contains_any')  return toks.some (t => v.includes(t));
  if (rule.op === 'equals')        return v === toks.join(' ');
  if (rule.op === 'not_contains')  return !toks.some(t => v.includes(t));
  return true;
}
function aeMfRender(){
  const host = document.getElementById('aeMfRows');
  if (!aeRules.length) aeRules.push({field:'name_combined', op:'contains_all', value:''});
  host.innerHTML = aeRules.map((r, i) => {
    const opPh = (AE_MF_OPS.find(o => o.key === r.op) || AE_MF_OPS[0]).ph;
    const fieldOpts = AE_MF_FIELDS.map(f =>
      `<option value="${f.key}"${f.key===r.field?' selected':''}>${f.label}</option>`).join('');
    const opOpts = AE_MF_OPS.map(o =>
      `<option value="${o.key}"${o.key===r.op?' selected':''}>${o.label}</option>`).join('');
    const val = (r.value || '').replace(/"/g, '&quot;');
    return `<div class="ae-mfilter-row" data-i="${i}">
      <select class="ae-finput rule-field">${fieldOpts}</select>
      <select class="ae-finput rule-op">${opOpts}</select>
      <input  class="ae-finput rule-value" type="text" placeholder="${opPh}" value="${val}">
      <button class="ae-rule-del" type="button" title="Remove this rule">&times;</button>
    </div>`;
  }).join('');
  host.querySelectorAll('.ae-mfilter-row').forEach(div => {
    const i = +div.dataset.i;
    div.querySelector('.rule-field').addEventListener('change', e => { aeRules[i].field = e.target.value; });
    div.querySelector('.rule-op'   ).addEventListener('change', e => {
      aeRules[i].op = e.target.value;
      const ph = (AE_MF_OPS.find(o => o.key === e.target.value) || AE_MF_OPS[0]).ph;
      div.querySelector('.rule-value').placeholder = ph;
    });
    div.querySelector('.rule-value').addEventListener('input',  e => { aeRules[i].value = e.target.value; });
    div.querySelector('.ae-rule-del').addEventListener('click', () => {
      aeRules.splice(i, 1);
      if (!aeRules.length) aeRules.push({field:'name_combined', op:'contains_all', value:''});
      aeMfRender(); aePage = 0; renderAE();
    });
  });
}

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
                     fetchAeWindowShopify(), fetchAeWindowReach()]);
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
                     fetchAeWindowShopify(), fetchAeWindowReach()]);
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
                'ad_id','ad_name','campaign_name','adset_id'].join(',');
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
  // Multi-select utm_source — empty Set means "all"
  if (aiUtmSourceSel.size) rows = rows.filter(r => aiUtmSourceSel.has(aiSourceKey(r)));
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
  if (!row.has_match) return '__none__';
  const t = (row.matched_tier || '').trim();
  if (!t) return '__none__';
  if (t === 'Step 1' || t === 'T1_ad_id')   return 'Step 1';
  if (t === 'Step 2' || t === 'T2_ad_name') return 'Step 2';
  if (t === 'Step 3' || t === 'T3')         return 'Step 3';
  if (t === 'Step 4')                       return 'Step 4';
  if (t === 'Step 5')                       return 'Step 5';
  // Unknown / template-only labels — treat as unmatched for KPI bucketing
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

/* Classify a raw utm_source (or synthetic key) into one of the four channels.
   Meta       — any source starting with "meta" (meta, meta_ads,
                 meta-featuredofferings, metald_1, meta-sitelink, …),
                 plus facebook / fb / instagram / ig / igshopping.
   Google     — google / gads / google_ads / adwords / any "google*".
   Retention  — kwikengage/kwikchat/kwikchatbot/any kwik*, plus
                 wa, sagepilot(-ai), email, rcs. Also catches the
                 synthetic key HEADLESS_RETENTION (blank utm_source
                 but a non-blank utm_content — an order that carries
                 a retention marker even though the source tag is
                 missing).
   Other      — everything else (direct, chatgpt.com, robylon,
                 fealtyx, nector, truly-blank orders, etc.). */
const _AI_HEADLESS = '__headless_retention__';   // synthetic source keys
const _AI_EXCHANGE = '__exchange__';
const _AI_META_RE      = /^(meta[\w\-]*|facebook|fb|instagram|ig|igshopping)$/i;
const _AI_GOOGLE_RE    = /^(google[\w\-]*|gads|adwords)$/i;
const _AI_RETENTION_RE = /^(kwik[\w\-]*|wa|sagepilot(?:[-_]?ai)?|email|rcs)$/i;

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
function aiChannel(srcKey){
  const s = (srcKey || '').trim();
  if (s === _AI_HEADLESS)       return 'retention';
  if (s === _AI_EXCHANGE)       return 'other';
  if (!s)                       return 'other';
  if (_AI_META_RE.test(s))      return 'meta';
  if (_AI_GOOGLE_RE.test(s))    return 'google';
  if (_AI_RETENTION_RE.test(s)) return 'retention';
  return 'other';
}

/* Track which channel drilldown is currently open (empty = none). */
let aiOpenChannel = '';
let aiDisplayMode = 'count';   // 'count' | 'pct' — swaps KPI primary value with chip
let aiTierMode    = 'meta';    // 'meta' | 'google' — which tier cascade the KPI row shows

function aiRenderChannels(){
  const chs = {all:{n:0,sp:0}, meta:{n:0,sp:0}, google:{n:0,sp:0},
               retention:{n:0,sp:0}, other:{n:0,sp:0}};
  for (const r of aiOrders){
    const c = aiChannel(aiSourceKey(r));   // uses row-level key so
    const sp = +r.total_price || 0;         // headless_retention counts under Retention
    chs.all.n += 1;      chs.all.sp += sp;
    chs[c].n  += 1;      chs[c].sp  += sp;
  }
  const total = chs.all.n || 0;
  const set = (id, k) => {
    const n = chs[k].n;
    const pctStr = k === 'all' ? '100%'
                 : (total > 0 ? ((n / total * 100).toFixed(1) + '%') : '—');
    const countStr = fmtInt(n);
    // Depending on display mode, swap which value is primary (big) vs
    // secondary (chip).
    const primary   = aiDisplayMode === 'pct' ? pctStr : countStr;
    const secondary = aiDisplayMode === 'pct' ? countStr : pctStr;
    document.getElementById('aiCh-'+id     ).textContent = primary;
    document.getElementById('aiCh-'+id+'-sp').textContent = fmtRs(chs[k].sp);
    const pcEl = document.getElementById('aiCh-'+id+'-pc');
    if (pcEl) pcEl.textContent = secondary;
  };
  set('all','all'); set('meta','meta'); set('google','google');
  set('retention','retention'); set('other','other');
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
  const nice  = {meta:'Meta', google:'Google', retention:'Retention',
                 other:'Other', all:'All channels'};
  panel.style.display = 'block';
  ttl.textContent = nice[channel] + ' · utm_source breakdown';
  // Group rows in this channel by the row-level source key so the two
  // synthetic sources ('' -> "(blank)" and __headless_retention__ ->
  // "headless_retention") appear as first-class entries in the table.
  const groups = new Map();
  let totalOrders = 0, totalSales = 0;
  for (const r of aiOrders){
    const key = aiSourceKey(r);
    const c   = aiChannel(key);
    if (channel !== 'all' && c !== channel) continue;
    if (!groups.has(key)) groups.set(key, {key:key, orders:0, sales:0});
    const g = groups.get(key);
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
                    ' · ' + list.length + ' unique utm_source values';
  if (!list.length){
    body.innerHTML = '<div class="ai-empty-hint">No orders in this channel for the current window.</div>';
    return;
  }
  const maxOrders = list[0].orders;
  const barCls = channel === 'all' ? 'other' : channel;
  body.innerHTML = '<table class="ai-src-tbl">'+
    '<thead><tr>'+
      '<th style="width:180px">utm_source</th>'+
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
  // Row click → set utm_source filter to just this value and close the drill
  body.querySelectorAll('tr[data-drill-src]').forEach(tr => {
    tr.addEventListener('click', () => {
      // data-drill-src carries the exact synthetic key already, so we
      // can just add it to the Set — no translation needed.
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
  if (t === 'G1' || t === 'G2' || t === 'G3' || t === 'G4') return t;
  return '__none__';
}

function aiRenderKpis(){
  if (aiTierMode === 'google'){
    const buckets = {G1:0,G2:0,G3:0,G4:0,'__none__':0};
    const sales   = {G1:0,G2:0,G3:0,G4:0,'__none__':0};
    // Scope to Google-channel orders only, so the % denominator makes sense
    for (const r of aiOrders){
      if (aiChannel(aiSourceKey(r)) !== 'google') continue;
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
  // Scope to Meta-channel orders only, so the % denominator makes sense
  for (const r of aiOrders){
    if (aiChannel(aiSourceKey(r)) !== 'meta') continue;
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

function aiRenderTable(){
  const rows = aiFiltered();
  const pageSize = +document.getElementById('aiPageSize').value || 100;
  const totalRows = rows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  if (aiPage >= totalPages) aiPage = totalPages - 1;
  if (aiPage < 0) aiPage = 0;
  const offset = aiPage * pageSize;
  const slice = rows.slice(offset, offset + pageSize);
  const tb = document.querySelector('#aiTbl tbody');
  if (!slice.length){
    tb.innerHTML = '<tr><td colspan="14" style="padding:30px;text-align:center;color:var(--text-tertiary)">No rows match the current filter.</td></tr>';
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
        '<td class="num">'+fmtRs(r.total_price)+'</td>'+
        '<td><span class="ai-tier '+tc+'">'+tier+'</span></td>'+
        '<td>'+(r.utm_source  || '—')+'</td>'+
        '<td>'+(r.utm_medium  || '—')+'</td>'+
        '<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis" title="'+(r.utm_campaign||'').replace(/"/g,'&quot;')+'">'+(r.utm_campaign || '—')+'</td>'+
        '<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis" title="'+(r.utm_content ||'').replace(/"/g,'&quot;')+'">'+(r.utm_content  || '—')+'</td>'+
        '<td style="max-width:160px;overflow:hidden;text-overflow:ellipsis" title="'+(r.utm_term    ||'').replace(/"/g,'&quot;')+'">'+(r.utm_term     || '—')+'</td>'+
        '<td style="max-width:160px;overflow:hidden;text-overflow:ellipsis" title="'+(r.matched_value||'').replace(/"/g,'&quot;')+'">'+(r.matched_value|| '—')+'</td>'+
        '<td class="id-cell">'+(r.ad_id || '—')+'</td>'+
        '<td style="max-width:240px;overflow:hidden;text-overflow:ellipsis" title="'+(r.ad_name    ||'').replace(/"/g,'&quot;')+'">'+(r.ad_name    || '—')+'</td>'+
        '<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis" title="'+(r.campaign_name||'').replace(/"/g,'&quot;')+'">'+(r.campaign_name|| '—')+'</td>'+
        '<td><span class="ai-status '+stCls+'">'+st+'</span></td>'+
      '</tr>';
    }).join('');
  }
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
    const rows = aiFiltered();
    // Column order for the CSV. matched_tier is written as the CANONICAL
    // Step N label (via aiStep) so legacy DB values (T1_ad_id, T2_ad_name,
    // T3, T0_template) come out normalised — matching what the UI shows.
    // The step-wise breakdown is the sole source of truth for matching, so
    // the raw DB value is not exported.
    const cols = ['order_created_at','order_id','total_price','matched_tier',
                  'utm_source','utm_medium','utm_campaign','utm_content','utm_term',
                  'matched_value','ad_id','ad_name','campaign_name','ad_status'];
    const csv = [cols.join(',')];
    for (const r of rows){
      const step = aiStep(r);
      const stepLabel = step === '__none__' ? 'Unmatched' : step;
      const status = r.ad_id ? (aiAdStatusMap[r.ad_id] || 'UNKNOWN') : '';
      const rowOut = {
        ...r,
        matched_tier: stepLabel,   // Step 1 / Step 2 / Step 3 / Step 4 / Step 5 / Unmatched
        ad_status:    status,
      };
      csv.push(cols.map(c => JSON.stringify(rowOut[c] ?? '')).join(','));
    }
    const blob = new Blob([csv.join('\n')], {type:'text/csv'});
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'ad_intelligence_' + new Date().toISOString().slice(0,10) + '.csv';
    a.click();
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
      // Same card clicked again → clear the filter + close drilldown
      aiUtmSourceSel.clear();
      aiCloseChannelDrill();
    } else {
      // Set filter to every source key that maps to this channel.
      //   ''                       -> truly-blank rows (under Other)
      //   '__headless_retention__' -> blank source + non-blank content
      //                               (under Retention)
      // Both sentinels get added when their channel is clicked so
      // those orders survive the filter.
      aiUtmSourceSel.clear();
      if (channel !== 'all'){
        for (const r of aiOrders){
          const key = aiSourceKey(r);
          if (aiChannel(key) === channel) aiUtmSourceSel.add(key);
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

/* Group ad rows by ad_name or campaign — sum numeric fields, keep most-recent ad meta */
function aeGroupBy(rows, key){
  if (key === 'ad') return rows;
  const groupKey = (key === 'ad_name') ? 'ad_name' : 'campaign_name';
  const groups = {};
  for (const r of rows){
    const k = (r[groupKey] || '').trim();
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
  // Derive frequency, ROAS, cost-per metrics from the aggregated sums
  return Object.values(groups).map(g => {
    const cnt = g._adIds.size;
    return {
      ...g,
      ad_name: key === 'ad_name' ? g.ad_name + (cnt > 1 ? '  (×' + cnt + ' ad_ids)' : '') : '[Campaign]',
      campaign_name: g.campaign_name || '',
      ad_id: cnt === 1 ? [...g._adIds][0] : (cnt + ' ad_ids'),
      frequency: g.reach > 0 ? g.impressions / g.reach : 0,
      roas_ma: g.amount_spent > 0 ? g.conv_value / g.amount_spent : 0,
      cost_per_ftewv: g.ftewv_count > 0 ? g.amount_spent / g.ftewv_count : 0,
      cost_per_ncp:   g.ncp_count   > 0 ? g.amount_spent / g.ncp_count   : 0,
      shopify_roas:   g.amount_spent > 0 ? g.shopify_sales / g.amount_spent : 0,
    };
  });
}

/* Canonical thresholds. F4=12 is the new working standard (was 25 in old refresh).
   Changes via the Ads Analyse view propagate through aeApplyCurrentThresholds(). */
const AE_DEFAULTS = {f1:50000, f2:3.2, f3:525, f4:12};

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
  const showDisc   = document.getElementById('aeShowDiscarded').checked;
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
  // Hide Discarded by default — matches old dashboard's implicit filter.
  if (!showDisc && !aeSelectedCat) rows = rows.filter(r => r.category !== 'Discarded');
  if (aeSelectedCat) rows = rows.filter(r => r.category === aeSelectedCat);
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
  // Multi-Filter rules — every rule with a non-empty value must pass (AND)
  const activeRules = aeRules.filter(r => (r.value || '').trim());
  if (activeRules.length){
    rows = rows.filter(r => activeRules.every(rule => aeMfMatch(r, rule)));
  }
  // Optional Group By
  rows = aeGroupBy(rows, groupBy);
  // sort
  rows.sort((a, b) => {
    let av = a[aeSortKey], bv = b[aeSortKey];
    if (aeSortKey === 'ad_created') { av = new Date(av || 0); bv = new Date(bv || 0); }
    else if (typeof av === 'string' || typeof bv === 'string'){
      av = (av || '').toString(); bv = (bv || '').toString();
      return aeSortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av);
    } else {
      av = +av || 0; bv = +bv || 0;
    }
    return aeSortDir === 'asc' ? (av - bv) : (bv - av);
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
  // Reach overlay — mirrors the old Incremental Reach Analysis modal so the
  // columns behave exactly the way they did before the refactor:
  //   Prev Reach     = reach on the earliest reporting day in the window
  //   Latest Reach   = reach on the latest reporting day in the window
  //   Incr. Reach    = Latest − Prev  (positive = growing, negative = falling)
  //   Cost / 1k Incr = SUM(spend across the whole window) × 1000 / Incr Reach
  // Ads with no reporting days in the window render "—" instead of the
  // stale ae_reach_recent snapshot.
  if (_aeWindowReachKey){
    if (rr){
      out.previous_reach    = rr.prev_reach;
      out.latest_reach      = rr.latest_reach;
      const incr = (rr.latest_reach || 0) - (rr.prev_reach || 0);
      out.incremental_reach = incr;
      out.cost_per_1000_incremental_reach = incr > 0
        ? (rr.total_spend * 1000) / incr
        : null;
    } else {
      out.previous_reach = null;
      out.latest_reach   = null;
      out.incremental_reach = null;
      out.cost_per_1000_incremental_reach = null;
    }
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
    const incr = (last?.reach || 0) - (first?.reach || 0);
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
    const neg = r.incr_reach < 0;
    return '<tr>'+
      '<td style="max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+r.grp.replace(/"/g,'&quot;')+'">'+r.grp+'</td>'+
      '<td class="num">'+fmtInt(r.n_ads)+'</td>'+
      '<td class="num">'+fmtInt(r.days)+'</td>'+
      '<td class="num">'+fmtInt(r.first_reach)+'</td>'+
      '<td class="num">'+fmtInt(r.last_reach)+'</td>'+
      '<td class="num">'+fmtInt(r.peak_reach)+'</td>'+
      '<td class="num" style="color:'+(neg?'var(--error-text)':'var(--success-text)')+';font-weight:700">'+
        (neg?'':'+')+fmtInt(r.incr_reach)+'</td>'+
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

function renderAE(){
  if (!allAds.length){
    // Data hasn't landed yet. Instead of silently bailing (and
    // leaving the KPIs stuck on "—"), show a loading state. rerender()
    // will call renderAE again once the ae_table_view fetch resolves.
    const footer = document.getElementById('aeFooter');
    if (footer) footer.textContent = 'Loading ae_table_view — filters will apply once the 15k-row fetch completes';
    return;
  }
  // Trigger overlay when either metric set OR shopify aggregate is loaded —
  // the Shopify overlay is enough on its own to justify calling aeApplyWindow,
  // since it zeroes-out shopify_orders / shopify_sales for ads with no matched
  // orders in the window (otherwise they'd render lifetime totals).
  const hasWindow = Object.keys(aeWindowMetricsByAdId).length > 0
                    || aeWindowShopifyKeyIsActive();
  const rows = aeFiltered().map(r => hasWindow ? aeApplyWindow(r) : r);

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
  const activeRules = aeRules.filter(rl => (rl.value || '').trim());
  if (activeRules.length){
    cats = cats.filter(r => activeRules.every(rule => aeMfMatch(r, rule)));
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
  setKPI('d',  'Discarded');

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
    // Thumbnail cell — Meta Graph creative.thumbnail_url, clickable to open drawer
    const thumbUrl = thumbUrlOf(r.ad_id ? thumbsByAdId[r.ad_id] : null);
    const thumbCell = thumbUrl
      ? '<td class="thumb-cell"><img class="ae-thumb" src="'+thumbUrl+'" loading="lazy" alt="" data-ae-thumb-ad="'+r.ad_id+'" title="Click to preview"></td>'
      : '<td class="thumb-cell"><div class="ae-thumb-placeholder" title="No thumbnail">—</div></td>';
    const main = '<tr>'+
      thumbCell+
      '<td class="ad-cell" title="'+(r.ad_name||'')+'"><span style="font-weight:600;color:var(--text-primary)">'+(r.ad_name||'—')+'</span></td>'+
      '<td class="id-cell">'+(r.ad_id||'—')+'</td>'+
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

  const showDisc = document.getElementById('aeShowDiscarded').checked;
  const hiddenNote = (!showDisc && !aeSelectedCat) ? ' · Discarded hidden' : '';
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
  document.getElementById('aeFooter').textContent =
    'Showing ' + fmtInt(slice.length) + ' of ' + fmtInt(totalRows) + ' filtered ' +
    '(' + fmtInt(allAds.length) + ' total in ae_table_view' + hiddenNote + ')'+
    modeNote + catNote;

  // Pagination footer
  document.getElementById('aeRowInfo').textContent   = fmtInt(totalRows) + ' rows';
  document.getElementById('aePageInfo').textContent  = 'Page ' + (aePage + 1) + ' / ' + totalPages;
  document.getElementById('aePrevPage').disabled     = aePage === 0;
  document.getElementById('aeNextPage').disabled     = aePage >= totalPages - 1;
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
  const defaults = {ctF1:50000, ctF2:3.2, ctF3:525, ctF4:12};
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
['aeAcct','aeShowDiscarded','aeStatus','aeGroupBy'].forEach(id =>
  document.getElementById(id).addEventListener('change', () => { aePage = 0; renderAE(); }));
// aeDateField change → possibly rebuild the delivery Set (only meaningful
// when switching to __delivery__ with an active window)
document.getElementById('aeDateField').addEventListener('change', async () => {
  await aeRebuildDeliverySet();
  aePage = 0; renderAE();
});
document.getElementById('aePageSize').addEventListener('change', () => { aePage = 0; renderAE(); });
// Manual date-input edits rebuild the delivery Set too (debounced)
['aeDateFrom','aeDateTo'].forEach(id =>
  document.getElementById(id).addEventListener('change', async () => {
    clearTimeout(window._aeDeliveryDb);
    window._aeDeliveryDb = setTimeout(async () => {
      await Promise.all([aeRebuildDeliverySet(), fetchAeWindowMetrics(),
                     fetchAeWindowShopify(), fetchAeWindowReach()]);
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
document.getElementById('aeClearFilters').onclick = async () => {
  document.getElementById('aeAcct').value      = '';
  document.getElementById('aeStatus').value    = '';
  document.getElementById('aeGroupBy').value   = 'ad';
  document.getElementById('aeDateField').value = '__delivery__';  // new default
  document.getElementById('aeCategory').value  = '';
  await drpClearDateRange();   // also clears aeDateFrom/aeDateTo + button label
  document.getElementById('aeShowDiscarded').checked = false;
  aeSelectedCat = '';
  aeRules = [];
  aeMfRender();
  await aeRebuildDeliverySet();
  aePage = 0; renderAE();
};

// Pagination buttons
document.getElementById('aePrevPage').onclick = () => { if (aePage > 0) { aePage--; renderAE(); } };
document.getElementById('aeNextPage').onclick = () => { aePage++; renderAE(); };

// Multi-Filter buttons
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

// Render the initial empty rule on page load
aeMfRender();
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
  const rows = aeFiltered();
  const cols = ['ad_id','ad_name','account_name','campaign_name','adset_id','adset_name',
                'ad_created','ad_status','category',
                'impressions','reach','frequency','amount_spent','roas_ma',
                'ftewv_count','cost_per_ftewv','ncp_count','cost_per_ncp',
                'shopify_orders','shopify_sales','shopify_roas'];
  const lines = [cols.join(',')];
  rows.forEach(r => lines.push(cols.map(c => JSON.stringify(r[c] ?? '')).join(',')));
  const blob = new Blob([lines.join('\n')], {type:'text/csv'});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = 'ads_analyse_' + (new Date().toISOString().slice(0,10)) + '.csv';
  a.click();
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
let invState = {status:'', search:'', excludeOOS:false, excludeNoImg:false};

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
  const cols = 'id,title,status,vendor,product_type,handle,image_url,'+
               'variant_count,total_inventory,price_min,price_max,'+
               'created_at_shop,updated_at_shop,synced_at';
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

  renderInventoryKpis();
  renderInventoryTable();
}

function renderInventoryKpis(){
  const tot   = invProducts.length;
  const act   = invProducts.filter(p => (p.status||'').toUpperCase() === 'ACTIVE').length;
  const draft = invProducts.filter(p => (p.status||'').toUpperCase() === 'DRAFT').length;
  const arch  = invProducts.filter(p => (p.status||'').toUpperCase() === 'ARCHIVED').length;
  const variants = invProducts.reduce((s,p) => s + (p.variantsCount?.count || 0), 0);
  const oos   = invProducts.filter(p => (p.totalInventory || 0) <= 0).length;
  document.getElementById('invKpTotal' ).textContent = fmtInt(tot);
  document.getElementById('invKpActive').textContent = fmtInt(act);
  document.getElementById('invKpDraft' ).textContent = fmtInt(draft);
  document.getElementById('invKpArch'  ).textContent = fmtInt(arch);
  document.getElementById('invKpVar'   ).textContent = fmtInt(variants);
  document.getElementById('invKpOOS'   ).textContent = fmtInt(oos);
}

function invFiltered(){
  let rows = invProducts.slice();
  if (invState.status) rows = rows.filter(p => (p.status||'').toUpperCase() === invState.status);
  if (invState.excludeOOS) rows = rows.filter(p => (p.totalInventory || 0) > 0);
  if (invState.excludeNoImg) rows = rows.filter(p => p.featuredImage?.url);
  if (invState.search){
    const q = invState.search.toLowerCase().trim();
    rows = rows.filter(p => ((p.title||'')+' '+(p.handle||'')+' '+(p.vendor||'')+' '+(p.productType||'')).toLowerCase().includes(q));
  }
  return rows;
}

function renderInventoryTable(){
  const rows = invFiltered();
  const tbody = document.querySelector('#invTbl tbody');
  tbody.innerHTML = rows.map(p => {
    const st = (p.status || '').toUpperCase();
    const cls = st === 'ACTIVE' ? 's-active' : st === 'DRAFT' ? 's-draft' : 's-archived';
    const img = p.featuredImage?.url
      ? '<img class="thumb" loading="lazy" src="' + p.featuredImage.url + '">'
      : '<div class="thumb-ph">no img</div>';
    const minP = +p.priceRangeV2?.minVariantPrice?.amount || 0;
    const maxP = +p.priceRangeV2?.maxVariantPrice?.amount || 0;
    const priceTxt = minP === maxP ? fmtRs(minP) : fmtRs(minP) + ' – ' + fmtRs(maxP);
    // DoQ / OOS cells — "—" when the product had no SKU-tag match in
    // product_doq_daily (mostly bedsheets / no SKU-shaped tags).
    const dq  = p.doq_daily != null ? fmtInt(p.doq_daily) : '—';
    const d30 = p.doq_30    != null ? (+p.doq_30).toFixed(2) : '—';
    const d45 = p.doq_45    != null ? (+p.doq_45).toFixed(2) : '—';
    const oo  = p.oos_30    != null ? fmtInt(p.oos_30) : '—';
    return '<tr>'+
      '<td>'+img+'</td>'+
      '<td class="title-cell">'+
        '<div class="title">'+ (p.title || '—') +'</div>'+
        '<div class="handle">'+ (p.handle || '') +'</div>'+
      '</td>'+
      '<td><span class="status-pill '+cls+'">'+st+'</span></td>'+
      '<td>'+(p.vendor || '—')+'</td>'+
      '<td>'+(p.productType || '—')+'</td>'+
      '<td class="num">'+fmtInt(p.variantsCount?.count)+'</td>'+
      '<td class="num">'+fmtInt(p.totalInventory)+'</td>'+
      '<td class="num">'+dq +'</td>'+
      '<td class="num">'+d30+'</td>'+
      '<td class="num">'+d45+'</td>'+
      '<td class="num">'+oo +'</td>'+
      '<td class="num">'+priceTxt+'</td>'+
      '<td style="font-family:JetBrains Mono;font-size:11px">'+(p.createdAt ? p.createdAt.slice(0,10) : '—')+'</td>'+
    '</tr>';
  }).join('');
  const tot = invProducts.length;
  if (rows.length !== tot){
    document.getElementById('invFooter').textContent =
      'Showing ' + rows.length.toLocaleString() + ' of ' + tot.toLocaleString() + ' products';
  }
}

document.getElementById('invStatusRow').addEventListener('click', e => {
  const btn = e.target.closest('.preset'); if (!btn) return;
  document.querySelectorAll('#invStatusRow .preset').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  invState.status = btn.dataset.s;
  renderInventoryTable();
});
let invSearchDb = null;
document.getElementById('invSearch').addEventListener('input', e => {
  clearTimeout(invSearchDb);
  invSearchDb = setTimeout(() => { invState.search = e.target.value; renderInventoryTable(); }, 200);
});
document.getElementById('invExcludeOOS' ).addEventListener('change', e => { invState.excludeOOS  = e.target.checked; renderInventoryTable(); });
document.getElementById('invExcludeNoImg').addEventListener('change', e => { invState.excludeNoImg = e.target.checked; renderInventoryTable(); });
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
  const cols = ['id','title','handle','status','vendor','productType','totalInventory','createdAt'];
  const lines = [cols.join(',')];
  rows.forEach(p => lines.push(cols.map(c => JSON.stringify(p[c] ?? '')).join(',')));
  const blob = new Blob([lines.join('\n')], {type:'text/csv'});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = 'inventory_' + (new Date().toISOString().slice(0,10)) + '.csv'; a.click();
};

(async ()=>{
  // Default Creative Testing date window = last 30 days INCLUDING today,
  // matching the OLD dashboard's behaviour. Set the dates first so
  // fetchPrimaryAggregated() / cache lookup uses the right window.
  applyDatePreset('last30');

  // Try the pre-computed cache (results_table.ads_json) first — one HTTP
  // fetch of a single JSONB blob instead of paging 260k+ rows from
  // primary_table. The cache is refreshed by results_sync.py in the
  // backend pipeline; if its window doesn't match the requested one
  // (e.g. user has already clicked a different preset before this code
  // runs) we fall back to the live aggregator.
  const cachedPromise = fetchPrimaryFromCache(state.dateFrom || '', state.dateTo || '')
    .catch(() => null);

  // Kick off ae_table_view + thumbnails + reach-recent + freq-lifecycle in parallel
  const [cached, freshAllAds, freshThumbs, freshReach, freshFreq] = await Promise.all([
    cachedPromise, fetchAds(), fetchThumbnails(), fetchReachRecent(), fetchFreqLifecycle()
  ]);
  allAds = freshAllAds; thumbsByAdId = freshThumbs;
  reachRecentByAdId = freshReach;
  freqLifecycleByAdId = freshFreq;
  // Merge the reach-recent snapshot into each ae row by ad_id so renderAE
  // can display incremental_reach / cost_per_incremental_reach inline.
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
