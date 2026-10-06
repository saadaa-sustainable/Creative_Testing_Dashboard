"""build_brand_post_links.py — assemble every known post link for the
saadaa_women / saadaa_men Instagram grids, for a preview gallery.

WHY THIS IS ASSEMBLED RATHER THAN FETCHED
-----------------------------------------
saadaa_women shows 1,196 posts and saadaa_men 496 on instagram.com, but the
Graph API will not enumerate them:

    /{ig_id}/media          women -> (#10) no permission ; men -> 13 posts, no cursor
    /{ig_id}/tags           (#10) on ALL THREE brand accounts, including the one
                            we own — the tagged-media edge needs App Review
    business_discovery      women -> 0 media ; men -> 13, no cursor

The reason is Instagram Collab: a collab post is OWNED by the creator who
posted it and merely DISPLAYED on the brand's grid. media_count counts what
is displayed; the media edge returns only what is owned. So the brand grids
are mostly other people's media, and no brand-side endpoint can list them.

The links therefore come from records we already hold:
    1. CreatorHub posts + historic_posts  -> post_link logged by the team
    2. ig_tagged_posts.csv                -> caption-verified brand tags from
                                             business_discovery on creators

ATTRIBUTION CONFIDENCE (the `source` column)
    caption   - the post's own caption names the handle. Highest confidence.
                NOTE the API strips '@', so captions read "saadaa_women", not
                "@saadaa_women" — match bare handles or you match nothing.
    gender    - inferred from the creator's recorded gender
                (Female -> women, Male -> men). A reasonable proxy, not proof.
    unknown   - link is known but neither method resolved it.

USAGE
    python build_brand_post_links.py
    python build_brand_post_links.py --xlsx      # also write a 2-tab workbook
"""
from __future__ import annotations
import argparse, csv, json, os, re, sys, urllib.parse, urllib.request
from collections import Counter

from dotenv import load_dotenv

try: sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception: pass

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

CH_URL = (os.environ.get("CREATOR_HUB_URL") or "").rstrip("/")
CH_KEY = (os.environ.get("CREATOR_HUB_ACCESS") or "").strip()
if not (CH_URL and CH_KEY): sys.exit("Missing CREATOR_HUB_URL / CREATOR_HUB_ACCESS in .env")

TAGGED_CSV = "ig_tagged_posts.csv"


def ch_get(table: str, select: str, extra: dict | None = None):
    """Page a CreatorHub PostgREST table fully (anon role caps at 1000/req)."""
    out, offset = [], 0
    while True:
        params = {"select": select, "limit": 1000, "offset": offset}
        if extra: params.update(extra)
        url = f"{CH_URL}/rest/v1/{table}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={
            "apikey": CH_KEY, "Authorization": f"Bearer {CH_KEY}", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=90) as r:
            chunk = json.loads(r.read())
        out += chunk
        if len(chunk) < 1000: break
        offset += 1000
    return out


def norm_link(u: str | None) -> str | None:
    """Canonical form so /p/ vs /reel/ duplicates and ?igsh= tails collapse."""
    if not u: return None
    u = u.strip().split("?")[0].rstrip("/")
    m = re.search(r"instagram\.com/(?:p|reel|tv)/([A-Za-z0-9_-]+)", u)
    return f"https://www.instagram.com/p/{m.group(1)}" if m else (u or None)


def shortcode(u: str | None) -> str | None:
    if not u: return None
    m = re.search(r"instagram\.com/(?:p|reel|tv)/([A-Za-z0-9_-]+)", u)
    return m.group(1) if m else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="brand_post_links.csv")
    ap.add_argument("--xlsx", action="store_true")
    args = ap.parse_args()

    print("pulling CreatorHub …", flush=True)
    posts    = ch_get("posts", "post_link,post_date,username,content_type,nomenclature",
                      {"post_link": "not.is.null"})
    historic = ch_get("historic_posts", "post_link,post_date,username,content_type,nomenclature,gender",
                      {"post_link": "not.is.null"})
    creators = ch_get("creators", "username,gender")
    print(f"  posts={len(posts)}  historic_posts={len(historic)}  creators={len(creators)}")

    cg = {(c.get("username") or "").strip().lstrip("@").lower(): (c.get("gender") or "").strip()
          for c in creators if c.get("username")}

    # caption-verified attribution from the business_discovery scan
    cap_brand, cap_meta = {}, {}
    if os.path.exists(TAGGED_CSV):
        for r in csv.DictReader(open(TAGGED_CSV, encoding="utf-8-sig")):
            k = norm_link(r["permalink"])
            if not k: continue
            cap_brand[k] = [b for b in r["brands_tagged"].split("|")
                            if b in ("saadaa_women", "saadaa_men", "saadaadesigns")]
            cap_meta[k] = r
        print(f"  caption-verified from {TAGGED_CSV}: {len(cap_brand)}")

    rows: dict[str, dict] = {}
    for src, lst in (("posts", posts), ("historic_posts", historic)):
        for p in lst:
            k = norm_link(p.get("post_link"))
            if not k: continue
            g = (p.get("gender") or "").strip() or cg.get(
                (p.get("username") or "").strip().lstrip("@").lower(), "")
            prev = rows.get(k)
            if prev and prev.get("post_date") and not p.get("post_date"):
                continue
            rows[k] = {
                "post_link": k,
                "shortcode": shortcode(k),
                "post_date": p.get("post_date") or (prev or {}).get("post_date") or "",
                "creator": (p.get("username") or (prev or {}).get("creator") or "").lstrip("@"),
                "content_type": p.get("content_type") or (prev or {}).get("content_type") or "",
                "creator_gender": g or (prev or {}).get("creator_gender") or "",
                "record_source": src,
            }

    # Overlay caption truth, and add any caption-verified links CreatorHub lacks
    for k, brands in cap_brand.items():
        r = cap_meta[k]
        base = rows.get(k, {"post_link": k, "shortcode": shortcode(k),
                            "post_date": r.get("post_date", ""), "creator": r.get("creator", ""),
                            "content_type": "", "creator_gender": "", "record_source": "tagged_scan"})
        base["caption_brands"] = "|".join(brands)
        base["like_count"] = r.get("like_count", "")
        base["comments_count"] = r.get("comments_count", "")
        base["media_product_type"] = r.get("media_product_type", "")
        base["post_date"] = base.get("post_date") or r.get("post_date", "")
        rows[k] = base

    out = []
    for k, r in rows.items():
        cb = r.get("caption_brands", "")
        if "saadaa_women" in cb and "saadaa_men" in cb: brand, how = "both", "caption"
        elif "saadaa_women" in cb:                      brand, how = "saadaa_women", "caption"
        elif "saadaa_men" in cb:                        brand, how = "saadaa_men", "caption"
        else:
            g = (r.get("creator_gender") or "").lower()
            if   g == "female": brand, how = "saadaa_women", "gender"
            elif g == "male":   brand, how = "saadaa_men", "gender"
            elif g:             brand, how = "unclassified", "gender"
            else:               brand, how = "unclassified", "unknown"
        out.append({
            "brand_account": brand, "attribution": how,
            "post_link": r["post_link"], "shortcode": r["shortcode"],
            "post_date": r.get("post_date", ""), "creator": r.get("creator", ""),
            "content_type": r.get("content_type", ""),
            "media_product_type": r.get("media_product_type", ""),
            "like_count": r.get("like_count", ""), "comments_count": r.get("comments_count", ""),
            "creator_gender": r.get("creator_gender", ""), "record_source": r.get("record_source", ""),
        })

    out.sort(key=lambda r: (r["brand_account"], r["post_date"] or ""), reverse=True)
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)

    print(f"\n{'brand':16s} {'links':>7s}   attribution breakdown")
    print("-" * 62)
    for b, n in Counter(r["brand_account"] for r in out).most_common():
        how = Counter(r["attribution"] for r in out if r["brand_account"] == b)
        print(f"{b:16s} {n:>7d}   {dict(how)}")
    print(f"\nTOTAL distinct links: {len(out)}  ->  {args.out}")

    if args.xlsx:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
        wb = Workbook(); wb.remove(wb.active)
        for tab, sel in (("saadaa_women", ("saadaa_women", "both")),
                         ("saadaa_men", ("saadaa_men", "both")),
                         ("unclassified", ("unclassified",))):
            sub = [r for r in out if r["brand_account"] in sel]
            ws = wb.create_sheet(tab); hdr = list(out[0].keys()); ws.append(hdr)
            for c in range(1, len(hdr) + 1):
                cell = ws.cell(row=1, column=c)
                cell.fill = PatternFill("solid", fgColor="1F3864")
                cell.font = Font(name="Arial", bold=True, color="FFFFFF", size=10)
                cell.alignment = Alignment(horizontal="center")
            for r in sub: ws.append([r[h] for h in hdr])
            for row in ws.iter_rows(min_row=2):
                for cell in row: cell.font = Font(name="Arial", size=10)
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = f"A1:{get_column_letter(len(hdr))}{ws.max_row}"
            for i, h in enumerate(hdr, 1):
                ws.column_dimensions[get_column_letter(i)].width = \
                    {"post_link": 48, "creator": 24, "shortcode": 14}.get(h, max(12, len(h) + 2))
            print(f"  tab '{tab}': {len(sub)} rows")
        wb.save("brand_post_links.xlsx"); print("wrote brand_post_links.xlsx")
    return 0


if __name__ == "__main__":
    sys.exit(main())
