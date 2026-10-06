"""fetch_ad_edit_log.py — mirror Meta's Activity Log (ad edit history) into
public.ad_edit_log.

WHY THIS IS ACCOUNT-WIDE AND NOT PER-AD
---------------------------------------
Meta exposes edit history ONLY at /act_{account}/activities. Probed 2026-09-18:

  * /{ad_id}/activities              -> error #100, no such edge
  * ?filtering=[{field:object_id...}] -> ACCEPTED BUT SILENTLY IGNORED
        (asked for one ad, got 25 rows of unrelated ad ids back)

So there is no way to ask Meta for one ad's history. Looping per-ad would
re-walk the entire account log once per ad — ~15k ads x 155 pages. Instead
this walks the account log ONCE, stores every row keyed by object_id, and
then ANY ad's edit log is a local SQL query:

    SELECT * FROM public.ad_edit_log WHERE ad_id = '...' ORDER BY event_time;

VOLUME (measured on Raho Saadaa, category=AD)
    ~183 rows/day, 500 rows/page  ->  ~1 page per 3 days
    14 months ~ 77k rows ~ 155 pages ~ a few minutes

NOTES
  * Ads appear as object_type 'ADGROUP' in the activity log, not 'AD'.
  * category=AD is a real server-side filter and roughly halves volume by
    dropping CAMPAIGN rows. Pass --category ALL to keep everything.
  * Meta returns no activity id, so the primary key is a content hash of
    (account_id, object_id, event_time, event_type, extra_data). Re-running
    any window is therefore idempotent.
  * Retention: verified back to at least 2025-01-09. Older windows return
    empty rather than erroring, so --since can be set generously.

USAGE
  python fetch_ad_edit_log.py --ad-id 120229618619190422 --max-range
  python fetch_ad_edit_log.py --since 2025-01-01 --until 2026-09-18
  python fetch_ad_edit_log.py --account 1136644150469466 --since 2026-09-01
  python fetch_ad_edit_log.py --ad-id 1202296... --no-store   # preview only
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

TOK = (os.environ.get("META_ACCESS_TOKEN") or "").strip()
if not TOK: sys.exit("Missing META_ACCESS_TOKEN in .env")
VER = (os.environ.get("META_API_VERSION") or "v22.0").strip()
API = f"https://graph.facebook.com/{VER}"
DB_URL = (os.environ.get("SUPABASE_DB_URL") or "").strip()
if not DB_URL: sys.exit("Missing SUPABASE_DB_URL in .env")

ACCOUNTS = [
    (os.environ.get("ACCOUNT_1_ID", "1136644150469466"), os.environ.get("ACCOUNT_1_NAME", "Raho Saadaa")),
    (os.environ.get("ACCOUNT_2_ID", "1349767139294217"), os.environ.get("ACCOUNT_2_NAME", "Fourth Ad Account - SD")),
    (os.environ.get("ACCOUNT_3_ID", "264868699479122"),  os.environ.get("ACCOUNT_3_NAME", "Third Ad Account - SD")),
]

# Earliest activity Meta still serves. Probed 2025-01-09 OK; going further
# back just returns empty pages, so this is a safe floor for --max-range.
RETENTION_FLOOR = date(2024, 1, 1)

FIELDS = ("event_time,event_type,translated_event_type,object_id,object_name,"
          "object_type,actor_id,actor_name,extra_data,application_id,"
          "application_name,date_time_in_timezone")

PROGRESS = ".ad_edit_log.progress.json"
LOG_FILE = "logs/ad_edit_log.log"

DDL = """
CREATE TABLE IF NOT EXISTS public.ad_edit_log (
  row_hash              text PRIMARY KEY,
  account_id            text NOT NULL,
  account_name          text,
  ad_id                 text,
  object_name           text,
  object_type           text,
  event_time            timestamptz NOT NULL,
  event_type            text,
  translated_event_type text,
  actor_id              text,
  actor_name            text,
  extra_data            jsonb,
  application_name      text,
  date_time_in_timezone text,
  fetched_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ad_edit_log_ad_time_idx  ON public.ad_edit_log (ad_id, event_time DESC);
CREATE INDEX IF NOT EXISTS ad_edit_log_time_idx     ON public.ad_edit_log (event_time DESC);
CREATE INDEX IF NOT EXISTS ad_edit_log_evt_idx      ON public.ad_edit_log (event_type);

ALTER TABLE public.ad_edit_log
  ADD COLUMN IF NOT EXISTS adset_id      text,
  ADD COLUMN IF NOT EXISTS campaign_name text;
CREATE INDEX IF NOT EXISTS ad_edit_log_adset_idx    ON public.ad_edit_log (adset_id);
CREATE INDEX IF NOT EXISTS ad_edit_log_campaign_idx ON public.ad_edit_log (campaign_name);
"""

# Backfill adset_id / campaign_name for rows that arrived without them.
#
# Two passes, because ~2% of ads in the activity log never delivered and so
# never reach ae_table_view at all:
#   1. ae_table_view — authoritative, covers ~98% of ads.
#   2. the create_ad payload itself. Meta's legacy schema calls an ADSET a
#      "campaign", so extra_data->'campaign_id' on a create_ad event holds the
#      ADSET id, NOT the campaign id (verified 300/300 against ae_table_view).
#      Mapping that key to campaign_id would be silently wrong for every row.
#      The campaign NAME is then borrowed from any sibling ad in that adset.
MAP_SQL = """
WITH m AS (
  SELECT DISTINCT ON (ad_id) ad_id, adset_id, campaign_name
  FROM public.ae_table_view
  WHERE ad_id IS NOT NULL
  ORDER BY ad_id, reporting_ends DESC NULLS LAST
)
UPDATE public.ad_edit_log l
SET adset_id = m.adset_id, campaign_name = m.campaign_name
FROM m
WHERE l.ad_id = m.ad_id
  AND (l.adset_id IS NULL OR l.campaign_name IS NULL);
"""

MAP_FALLBACK_SQL = """
WITH payload AS (
  SELECT DISTINCT ON (l.ad_id)
         l.ad_id, l.extra_data->'campaign_id'->>'new' AS adset_id
  FROM public.ad_edit_log l
  WHERE l.event_type = 'create_ad' AND l.extra_data ? 'campaign_id'
    AND l.adset_id IS NULL
  ORDER BY l.ad_id, l.event_time
),
adset_camp AS (
  SELECT DISTINCT ON (adset_id) adset_id, campaign_name
  FROM public.ae_table_view
  WHERE adset_id IS NOT NULL AND campaign_name IS NOT NULL
  ORDER BY adset_id, reporting_ends DESC NULLS LAST
)
UPDATE public.ad_edit_log l
SET adset_id = p.adset_id, campaign_name = ac.campaign_name
FROM payload p
LEFT JOIN adset_camp ac ON ac.adset_id = p.adset_id
WHERE l.ad_id = p.ad_id AND l.adset_id IS NULL;
"""


def backfill_mapping(conn) -> tuple[int, int]:
    """Fill adset_id / campaign_name on any rows missing them."""
    with conn.cursor() as cur:
        cur.execute(MAP_SQL)
        n1 = cur.rowcount
        cur.execute(MAP_FALLBACK_SQL)
        n2 = cur.rowcount
    conn.commit()
    return max(0, n1), max(0, n2)

_TOK_RE = re.compile(r"(?:EAA[A-Za-z0-9]{30,}|IGQ[\w\-]{20,}|eyJ[\w\-.]{40,})")


def scrub(s) -> str:
    s = _TOK_RE.sub("<REDACTED>", str(s or ""))
    return re.sub(r"(access_token=)[^&\s\"]+", r"\1<REDACTED>", s)


def log(*a) -> None:
    msg = " ".join(scrub(str(x)) for x in a)
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        os.makedirs("logs", exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8", errors="backslashreplace") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ── Throttle handling ────────────────────────────────────────────────
# Mirrors fetch_reach_incr.py: read Meta's own usage headers and back off
# BEFORE hitting a hard block, rather than reacting to a 429 after the fact.
def _sleep_if_throttled(headers) -> int:
    max_pct = 0
    try:
        for k in ("x-app-usage", "x-business-use-case-usage", "x-ad-account-usage"):
            v = headers.get(k)
            if not v:
                continue
            j = json.loads(v) if isinstance(v, str) else v

            def walk(x):
                if isinstance(x, dict):
                    for _, vv in x.items(): yield from walk(vv)
                elif isinstance(x, list):
                    for vv in x: yield from walk(vv)
                elif isinstance(x, (int, float)) and 0 <= x <= 100:
                    yield x
            for n in walk(j):
                if n > max_pct: max_pct = n
    except Exception:
        pass
    if   max_pct >= 95: log(f"  [throttle {max_pct}%] sleep 300s"); time.sleep(300)
    elif max_pct >= 90: log(f"  [throttle {max_pct}%] sleep 60s");  time.sleep(60)
    elif max_pct >= 80: log(f"  [throttle {max_pct}%] sleep 15s");  time.sleep(15)
    return max_pct


def _get(url, params, retries=6):
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=60)
        except requests.RequestException as e:
            log(f"    [net {attempt}] {type(e).__name__}: {str(e)[:120]} — sleep {delay}s")
            time.sleep(delay); delay = min(delay * 2, 120); continue
        if r.status_code == 200:
            j = r.json()
            _sleep_if_throttled(r.headers)
            return j, None
        _sleep_if_throttled(r.headers)
        try: err = (r.json().get("error") or {})
        except Exception: err = {}
        msg, code = err.get("message") or r.text[:200], err.get("code")
        if r.status_code in (429, 500, 502, 503, 504) or code in (4, 17, 32, 613) \
           or "too many calls" in str(msg).lower() or "reduce the amount of data" in str(msg).lower():
            log(f"    [throttle {attempt}] {scrub(msg)[:120]} — sleep {delay}s")
            time.sleep(delay); delay = min(delay * 2, 300); continue
        return None, scrub(msg)[:300]
    return None, "exhausted retries"


def _row_hash(acct_id, r) -> str:
    raw = "|".join([acct_id, str(r.get("object_id") or ""), str(r.get("event_time") or ""),
                    str(r.get("event_type") or ""), str(r.get("extra_data") or "")])
    return hashlib.md5(raw.encode("utf-8", "replace")).hexdigest()


def fetch_window(acct_id, acct_name, since, until, category, page_cap=0):
    """Page the activity log for one account over [since, until]."""
    url = f"{API}/act_{acct_id}/activities"
    params = {"fields": FIELDS, "since": since.isoformat(), "until": until.isoformat(),
              "limit": 500, "access_token": TOK}
    if category and category.upper() != "ALL":
        params["category"] = category.upper()

    out, pages = [], 0
    while True:
        j, err = _get(url, params)
        if err:
            log(f"    [!] {since}..{until}: {err}")
            return out, err
        out.extend(j.get("data", []) or [])
        pages += 1
        nxt = (j.get("paging") or {}).get("next")
        if not nxt: break
        if page_cap and pages >= page_cap:
            log(f"    [!] page cap {page_cap} hit — window truncated"); break
        url, params = nxt, None
        time.sleep(0.2)          # gentle pacing between pages
    return out, None


def store(conn, acct_id, acct_name, rows):
    if not rows: return 0
    payload = []
    for r in rows:
        ed = r.get("extra_data")
        if isinstance(ed, str):
            try: ed = json.loads(ed)
            except Exception: ed = {"_raw": ed}
        payload.append((
            _row_hash(acct_id, r), acct_id, acct_name,
            r.get("object_id"), r.get("object_name"), r.get("object_type"),
            r.get("event_time"), r.get("event_type"), r.get("translated_event_type"),
            r.get("actor_id"), r.get("actor_name"),
            json.dumps(ed, ensure_ascii=False) if ed is not None else None,
            r.get("application_name"), r.get("date_time_in_timezone"),
        ))
    # Batch explicitly and sum rowcount per batch. execute_values(page_size=N)
    # splits internally but leaves cur.rowcount reflecting only the LAST
    # statement, so a 1451-row window reported "451 new" (= the final partial
    # batch) even though all 1451 were inserted. Counting per batch is the
    # difference between a trustworthy "new rows" number and a scary-looking
    # one that sends you hunting a dedup bug that was never there.
    SQL = """
        INSERT INTO public.ad_edit_log
          (row_hash, account_id, account_name, ad_id, object_name, object_type,
           event_time, event_type, translated_event_type, actor_id, actor_name,
           extra_data, application_name, date_time_in_timezone)
        VALUES %s
        ON CONFLICT (row_hash) DO NOTHING"""
    TPL = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)"
    BATCH = 500
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(payload), BATCH):
            execute_values(cur, SQL, payload[i:i + BATCH], template=TPL,
                           page_size=BATCH)
            total += max(0, cur.rowcount)
    conn.commit()
    return total


def _load_prog():
    try:
        with open(PROGRESS, encoding="utf-8") as f: return json.load(f)
    except Exception: return {}


def _save_prog(p):
    try:
        with open(PROGRESS, "w", encoding="utf-8") as f: json.dump(p, f, indent=2)
    except Exception: pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ad-id", help="print this ad's edit log after fetching")
    ap.add_argument("--account", help="limit to one ad account id")
    ap.add_argument("--since")
    ap.add_argument("--until")
    ap.add_argument("--max-range", action="store_true",
                    help=f"walk back to the retention floor ({RETENTION_FLOOR})")
    ap.add_argument("--category", default="AD", help="AD | CAMPAIGN | AD_SET | ALL (default AD)")
    ap.add_argument("--chunk-days", type=int, default=30)
    ap.add_argument("--page-cap", type=int, default=0, help="max pages per window, 0 = unlimited")
    ap.add_argument("--no-store", action="store_true", help="fetch + report, skip DB writes")
    ap.add_argument("--reset", action="store_true", help="ignore saved progress")
    args = ap.parse_args()

    until = date.fromisoformat(args.until) if args.until else date.today()
    if args.max_range:
        since = RETENTION_FLOOR
    elif args.since:
        since = date.fromisoformat(args.since)
    else:
        since = until - timedelta(days=30)

    accts = [a for a in ACCOUNTS if not args.account or a[0] == args.account]
    if not accts: return print(f"no account matches {args.account!r}") or 2

    conn = None
    if not args.no_store:
        conn = psycopg2.connect(DB_URL, connect_timeout=30)
        with conn.cursor() as cur: cur.execute(DDL)
        conn.commit()

    prog = {} if args.reset else _load_prog()
    done = set(prog.get("windows", []))

    log(f"=== ad edit log: {since} → {until}  category={args.category}  "
        f"accounts={len(accts)}  chunk={args.chunk_days}d ===")

    grand_fetched = grand_new = 0
    for acct_id, acct_name in accts:
        log(f"[{acct_name}] ({acct_id})")
        cur_start, acct_fetched, acct_new = since, 0, 0
        while cur_start <= until:
            cur_end = min(cur_start + timedelta(days=args.chunk_days - 1), until)
            # Meta's activities edge returns ZERO rows when since == until,
            # even on days that demonstrably have events (verified 2026-09-21:
            # 2026-08-10 -> 2026-08-10 gave 0, while 2026-08-09 -> 2026-08-12
            # gave the 6 events that day really has). A one-day window is
            # exactly what a nightly incremental produces, so without this the
            # catch-up run would silently fetch nothing and look successful.
            if cur_end <= cur_start:
                cur_end = cur_start + timedelta(days=1)
            key = f"{acct_id}|{cur_start}|{cur_end}|{args.category}"
            if key in done:
                cur_start = cur_end + timedelta(days=1); continue

            rows, err = fetch_window(acct_id, acct_name, cur_start, cur_end,
                                     args.category, args.page_cap)
            acct_fetched += len(rows)
            n_new = 0
            if rows and conn is not None:
                # Reconnect if the pooler dropped us during a long page-walk.
                try:
                    with conn.cursor() as c: c.execute("SELECT 1")
                except Exception:
                    log("  [db] reconnecting"); conn = psycopg2.connect(DB_URL, connect_timeout=30)
                n_new = store(conn, acct_id, acct_name, rows)
            acct_new += n_new
            log(f"  {cur_start} → {cur_end}: {len(rows):5d} rows, {n_new:5d} new")

            if not err:
                done.add(key); prog["windows"] = sorted(done); _save_prog(prog)
            cur_start = cur_end + timedelta(days=1)

        log(f"[{acct_name}] fetched={acct_fetched:,}  new={acct_new:,}")
        grand_fetched += acct_fetched; grand_new += acct_new

    log(f"=== TOTAL fetched={grand_fetched:,}  new_rows={grand_new:,} ===")

    if conn is not None:
        n1, n2 = backfill_mapping(conn)
        log(f"=== mapped adset/campaign: {n1:,} from ae_table_view, "
            f"{n2:,} from create_ad payload ===")

    if args.ad_id and conn is not None:
        with conn.cursor() as cur:
            cur.execute("""SELECT event_time, event_type, translated_event_type,
                                  actor_name, object_name, extra_data
                           FROM public.ad_edit_log
                           WHERE ad_id = %s ORDER BY event_time DESC""", (args.ad_id,))
            hist = cur.fetchall()
        log(f"\n=== edit log for ad {args.ad_id} — {len(hist)} events ===")
        for t, et, tt, actor, oname, ed in hist:
            extra = json.dumps(ed, ensure_ascii=False)[:90] if ed else ""
            log(f"  {t:%Y-%m-%d %H:%M}  {(tt or et or ''):38.38s} {(actor or '-'):18.18s} {extra}")

    if conn is not None: conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
