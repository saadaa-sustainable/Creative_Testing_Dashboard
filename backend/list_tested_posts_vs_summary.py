"""
list_tested_posts_vs_summary.py - for every POST ID in Influencer sheet,
record its Excel Status / Result and how many summary_table ads it
matches (with the same digit-boundary guard as SubsheetMatch.gs).

Output:
  tested_posts_vs_summary.csv      (every row, all statuses)
  tested_posts_only.csv            (filtered to Excel Status = Tested)

Console: counts.
"""
import os, csv, openpyxl, psycopg2
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"].strip()
XLSX   = r"C:\Users\Saadaa\Downloads\CTP unique ids.xlsx"

# ── 1) Load summary_table ad_names once ──────────────────────────────────────
conn = psycopg2.connect(DB_URL); cur = conn.cursor()
cur.execute("SELECT ad_id, ad_name, status FROM summary_table WHERE ad_name IS NOT NULL")
ads = cur.fetchall()
print(f"summary_table ads loaded: {len(ads):,}")
ad_names_lower = [(aid, (nm or "").lower(), status) for aid, nm, status in ads]
conn.close()

def boundary_match_count(name_set_lower, post_id):
    """Return list of (ad_id, ad_name) matching post_id with digit-boundary guard."""
    needle = post_id.lower()
    L = len(needle)
    hits = []
    for aid, hay, st in name_set_lower:
        pos = 0
        while True:
            p = hay.find(needle, pos)
            if p < 0: break
            nxt = hay[p + L] if (p + L) < len(hay) else ""
            if not nxt.isdigit():
                hits.append((aid, hay, st)); break
            pos = p + 1
    return hits

# ── 2) Walk Influencer sheet ─────────────────────────────────────────────────
wb = openpyxl.load_workbook(XLSX, data_only=True, read_only=True)
ws = wb["Influencer"]
rows = list(ws.iter_rows(values_only=True))
headers = rows[0]
post_col = next(i for i,h in enumerate(headers) if str(h or "").strip().lower()=="post id")
sif_col  = next(i for i,h in enumerate(headers) if str(h or "").strip().lower()=="sif-id")
stat_col = next(i for i,h in enumerate(headers) if str(h or "").strip().lower()=="status")
res_col  = next(i for i,h in enumerate(headers) if str(h or "").strip().lower()=="result")

OUT_ALL   = "tested_posts_vs_summary.csv"
OUT_TEST  = "tested_posts_only.csv"

n_rows = 0
n_tested = 0; n_pending = 0; n_other = 0
n_tested_with_match = 0
n_tested_without_match = 0
status_match_breakdown = defaultdict(lambda: {"with":0,"without":0})

with open(OUT_ALL, "w", newline="", encoding="utf-8") as f_all, \
     open(OUT_TEST,"w", newline="", encoding="utf-8") as f_test:
    w_all  = csv.writer(f_all);  w_all.writerow(
        ["sif_id","post_id","excel_status","excel_result","summary_match_count","sample_ad_id","sample_ad_name"])
    w_test = csv.writer(f_test); w_test.writerow(
        ["post_id","excel_result","summary_match_count","sample_ad_id","sample_ad_name"])

    for r in rows[1:]:
        if not r: continue
        post = r[post_col] if post_col < len(r) else None
        if not post: continue
        post = str(post).strip()
        if not post: continue
        sif    = (str(r[sif_col]).strip()   if sif_col<len(r)  and r[sif_col]  else "")
        status = (str(r[stat_col]).strip()  if stat_col<len(r) and r[stat_col] else "")
        result = (str(r[res_col]).strip()   if res_col<len(r)  and r[res_col]  else "")

        n_rows += 1
        hits = boundary_match_count(ad_names_lower, post)
        match_count = len(hits)
        sample = hits[0] if hits else (None, None, None)
        w_all.writerow([sif, post, status, result, match_count, sample[0] or "", sample[1] or ""])

        bkt = "with" if hits else "without"
        status_match_breakdown[status or "(blank)"][bkt] += 1

        s_l = status.lower()
        if s_l == "tested":
            n_tested += 1
            if hits:
                n_tested_with_match += 1
                w_test.writerow([post, result, match_count, sample[0] or "", sample[1] or ""])
            else:
                n_tested_without_match += 1
                w_test.writerow([post, result, 0, "", ""])
        elif s_l == "pending": n_pending += 1
        else: n_other += 1
wb.close()

# ── 3) Summary console ──────────────────────────────────────────────────────
print("\n========== Influencer rows vs summary_table ==========")
print(f"  total rows scanned         : {n_rows:,}")
print(f"  Tested                     : {n_tested:,}")
print(f"  Pending                    : {n_pending:,}")
print(f"  Other / blank              : {n_other:,}")
print()
print(f"Of {n_tested} Tested POST IDs:")
print(f"  matched in summary_table   : {n_tested_with_match}")
print(f"  NOT matched in summary_table: {n_tested_without_match}")
print()
print("Per-status: matched vs not matched in summary_table")
print(f"  {'status':<14}{'with_match':>12}{'without_match':>16}")
for st, d in sorted(status_match_breakdown.items()):
    print(f"  {st:<14}{d['with']:>12}{d['without']:>16}")
print(f"\n[csv] full breakdown -> {OUT_ALL}")
print(f"[csv] only Tested    -> {OUT_TEST}")

# ── 4) Direct spot-check on SIF-371-P2 as requested ─────────────────────────
print("\n[spot check] SIF-371-P2")
hits = boundary_match_count(ad_names_lower, "SIF-371-P2")
print(f"  matches in summary_table : {len(hits)}")
for aid, name, st in hits[:5]:
    print(f"    ad_id={aid}  status={st}  ad_name={name[:80]}")
# Also lookup excel status for SIF-371-P2 specifically
print("  Excel rows for SIF-371-P2:")
wb = openpyxl.load_workbook(XLSX, data_only=True, read_only=True)
for r in list(wb["Influencer"].iter_rows(values_only=True))[1:]:
    if not r: continue
    if post_col<len(r) and r[post_col] and str(r[post_col]).strip()=="SIF-371-P2":
        print(f"    Status={r[stat_col]!r}  Result={r[res_col]!r}  SIF={r[sif_col]!r}")
wb.close()
