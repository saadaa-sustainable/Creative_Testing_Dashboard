"""fetch_creatorhub_posts.py — mirror posts from the saadaa-creatorhub
Supabase project into Meta_ads_data as public.content_influencer_posts,
so the dashboard's Untested → Influencer tab can query it via the
normal SUPABASE_URL / SUPABASE_ANON path (no extra keys, no RLS quirks).

Source : CREATOR_HUB_URL / CREATOR_HUB_SERVICE_KEY   (xynyvbagcudjrzklwnqp)
Target : SUPABASE_DB_URL                             (rtkohjfzyzhizkebdsuy)

Only 22 out of 76 posts columns are mirrored — the "relevant" ones for
Untested tracking. Same rule as content_asset_register:

Tested rule (computed here, not on dashboard):
    tested  = nomenclature (case-insensitive) appears inside any
              primary_table.ad_name
    Also stores matched_ad_id + matched_ad_name for surfacing in the UI.

Idempotent — CREATE IF NOT EXISTS, UPSERT by post_id.

USAGE
  python fetch_creatorhub_posts.py                # full mirror + tested compute
  python fetch_creatorhub_posts.py --dry-run      # fetch only, no writes
"""
from __future__ import annotations
import os, sys, json, argparse, urllib.request, urllib.error
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(Path(__file__).parent / ".env", override=True)

CH_URL = (os.environ.get("CREATOR_HUB_URL") or "").rstrip("/")
CH_KEY = (os.environ.get("CREATOR_HUB_SERVICE_KEY")
          or os.environ.get("CREATOR_HUB_ACCESS") or "")
DB_URL = os.environ.get("SUPABASE_DB_URL", "").strip()

if not (CH_URL and CH_KEY):
    sys.exit("[fatal] set CREATOR_HUB_URL + CREATOR_HUB_SERVICE_KEY in .env "
             "(or CREATOR_HUB_ACCESS if the anon key can SELECT public.posts).")
if not DB_URL:
    sys.exit("[fatal] SUPABASE_DB_URL missing")

# Columns to mirror — same set the dashboard's _utNormalizeInfluencer reads.
COLS = [
    "id", "post_id", "post_id_short", "username", "nomenclature",
    "content_type", "deliverable_type", "deliverable_role", "collab_type",
    "campaign_id", "post_date", "created_at", "updated_at",
    "workflow_status", "partnership_status",
    "ads_status", "ads_results", "ads_usage_rights",
    "post_link", "download_link", "post_thumbnail",
]

TYPES = {
    "id":                    "bigint",
    "post_id":               "text primary key",
    "post_id_short":         "text",
    "username":              "text",
    "nomenclature":          "text",
    "content_type":          "text",
    "deliverable_type":      "text",
    "deliverable_role":      "text",
    "collab_type":           "text",
    "campaign_id":           "text",
    "post_date":             "date",
    "created_at":            "timestamptz",
    "updated_at":            "timestamptz",
    "workflow_status":       "text",
    "partnership_status":    "text",
    "ads_status":            "text",
    "ads_results":           "text",
    "ads_usage_rights":      "text",
    "post_link":             "text",
    "download_link":         "text",
    "post_thumbnail":        "text",
}

DDL = f"""
create table if not exists public.content_influencer_posts (
    {', '.join(f'{c} {TYPES[c]}' for c in COLS)},
    computed_is_tested boolean default false,
    matched_ad_id      text,
    matched_ad_name    text,
    mirrored_at        timestamptz not null default now()
);
create index if not exists content_influencer_posts_username_idx
    on public.content_influencer_posts (username);
create index if not exists content_influencer_posts_post_date_idx
    on public.content_influencer_posts (post_date desc);
create index if not exists content_influencer_posts_nomenclature_idx
    on public.content_influencer_posts (nomenclature);
"""


def http_get(path, extra=None, timeout=30):
    hdrs = {"apikey": CH_KEY, "Authorization": f"Bearer {CH_KEY}",
            "Accept": "application/json"}
    if extra: hdrs.update(extra)
    req = urllib.request.Request(f"{CH_URL}{path}", headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _paginate(path, select_cols, extra_qs=""):
    """Paginate a PostgREST endpoint via Range headers."""
    all_rows = []
    page = 0
    PAGE = 1000
    while True:
        lo, hi = page * PAGE, (page + 1) * PAGE - 1
        st, hdrs, b = http_get(
            f"{path}?select={select_cols}{extra_qs}",
            extra={"Range-Unit": "items", "Range": f"{lo}-{hi}",
                   "Prefer": "count=exact"},
        )
        if st not in (200, 206):
            raise RuntimeError(f"fetch failed status={st} body={b[:300]}")
        rows = json.loads(b)
        cr = hdrs.get("Content-Range", "")
        all_rows.extend(rows)
        print(f"  [posts page {page}] fetched={len(rows)}  total={len(all_rows)}  {cr}", flush=True)
        if len(rows) < PAGE: break
        page += 1
    return all_rows


def fetch_all():
    # Only posts with a public link are useful for Untested tracking — a post
    # without a link can't be tested as a creative. Matches dashboard filter.
    rows = _paginate(
        "/rest/v1/posts",
        ",".join(COLS),
        extra_qs="&post_link=not.is.null&order=post_date.desc.nullslast",
    )
    print(f"[fetched] posts (with link): {len(rows)}")
    return rows


def _compute_tested(cur):
    """Substring-match post_id_short against primary_table.ad_name in memory
    (same in-memory strategy as fetch_content_asset_register — SQL cross-join
    hits Supabase's 30s statement timeout).

    Needle = post_id_short + '-' (ad names always append '-<username>' after
    the post_id, so this boundary prevents SIF-11686-P1 matching SIF-11686-P10)
    """
    cur.execute("""
      select post_id, coalesce(post_id_short, nomenclature) as needle
        from public.content_influencer_posts
       where coalesce(post_id_short, nomenclature) is not null
         and length(coalesce(post_id_short, nomenclature)) >= 4
    """)
    posts = cur.fetchall()
    print(f"[compute] {len(posts)} posts to match")

    cur.execute("select ad_id::text, ad_name from public.primary_table "
                "where ad_name is not null")
    ads = [(aid, name, name.lower()) for aid, name in cur.fetchall()]
    print(f"[compute] scanning {len(ads):,} ad_names")

    matches = {}
    for post_id, needle_raw in posts:
        needle = needle_raw.lower() + '-'
        for aid, name, name_lc in ads:
            if needle in name_lc:
                matches[post_id] = (aid, name)
                break

    tested = len(matches)
    untested = len(posts) - tested
    print(f"[compute] tested={tested}  untested={untested}")

    payload_hit  = [(aid, name, key) for key, (aid, name) in matches.items()]
    payload_miss = [(key,) for key, _ in posts if key not in matches]
    execute_values(
        cur,
        """update public.content_influencer_posts c set
              computed_is_tested = true,
              matched_ad_id      = v.ad_id,
              matched_ad_name    = v.ad_name
             from (values %s) as v(ad_id, ad_name, post_id)
            where c.post_id = v.post_id""",
        payload_hit, page_size=500,
    )
    execute_values(
        cur,
        """update public.content_influencer_posts c set
              computed_is_tested = false,
              matched_ad_id      = null,
              matched_ad_name    = null
             from (values %s) as v(post_id)
            where c.post_id = v.post_id""",
        payload_miss, page_size=500,
    )


def upsert(rows):
    if not rows:
        print("[upsert] no rows")
        return 0
    conn = psycopg2.connect(DB_URL); conn.autocommit = False
    cur = conn.cursor()
    cur.execute(DDL); conn.commit()
    print("[ddl] table + indexes ready")

    payload = [tuple(r.get(c) for c in COLS) for r in rows
               if r.get("post_id")]
    print(f"[upsert] rows with post_id: {len(payload):,}")
    if not payload:
        return 0
    updates = ", ".join(f"{c} = excluded.{c}" for c in COLS if c != "post_id")
    execute_values(
        cur,
        f"""insert into public.content_influencer_posts
              ({', '.join(COLS)}, mirrored_at)
             values %s
             on conflict (post_id) do update set
                mirrored_at = now(),
                {updates}""",
        payload,
        template="(" + ", ".join(["%s"] * len(COLS)) + ", now())",
        page_size=500,
    )
    print(f"[upsert] committed {len(payload):,} rows")

    _compute_tested(cur)
    conn.commit()
    cur.close(); conn.close()
    return len(payload)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print(f"\n[creatorhub → content_influencer_posts mirror]")
    print(f"  source: {CH_URL}")
    print(f"  target: {DB_URL[:50]}...\n")

    rows = fetch_all()
    if args.dry_run:
        print(f"[dry-run] would upsert {len(rows)} rows"); return
    n = upsert(rows)
    print(f"\n[done] mirrored {n} influencer posts")


if __name__ == "__main__":
    main()
