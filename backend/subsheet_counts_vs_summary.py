"""
subsheet_counts_vs_summary.py - apply the corrected SubsheetMatch.gs logic
(with Priority/Analyze1/Analyze2 aliases) to every subsheet in
CTP-Asset_Sheet_v1, comparing what the script WOULD write to what the
sheet CURRENTLY has.

For each subsheet, report:
  - row counts
  - Status: currently shown in sheet (Tested / Pending / blank)
  - Status: what the corrected script would write (Tested / Pending)
  - Result distribution (Tested rows broken down by best-rank label)
  - Mismatches (rows where current vs script disagree)
"""
import os, openpyxl, psycopg2
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"].strip()
XLSX   = r"C:\Users\Saadaa\Downloads\CTP unique ids.xlsx"

# Subsheet config — (sheet_name, key_header)
SUBSHEETS = [
    ("Video",                              "Asset ID"),
    ("Influencer",                         "POST ID"),
    ("Graphic",                            "Requisition ID"),
    ("Deprecated Completed File",          "Requisition ID"),
    ("Deprecated Planning",                "Requisition ID"),
    ("Deprecated Unplanned Edited Con",    "Nomenclature"),  # tab name in this workbook
]

# Corrected rank table (with DB-name aliases)
RANK = {
    "incremental winner": 5,
    "winner":             4,
    "iteration":          3,
    "priority":           3,   # DB alias
    "analyse":            2,
    "analyze 1":          2,   # DB alias
    "analyze 2":          2,   # DB alias
    "discarded":          1,
}
LABEL = {5: "Incremental Winner", 4: "Winner", 3: "Iteration", 2: "Analyse", 1: "Discarded"}

# ── 1) Load summary_table ad_names + statuses ───────────────────────────────
print("[load] summary_table ad_names ...")
conn = psycopg2.connect(DB_URL); cur = conn.cursor()
cur.execute("SELECT ad_name, status FROM summary_table WHERE ad_name IS NOT NULL")
ads = []
for name, status in cur.fetchall():
    rank = RANK.get((status or "").strip().lower(), 0)
    ads.append((name.lower(), rank))
conn.close()
print(f"  {len(ads):,} ads loaded")

def best_rank(needle):
    """Apps-Script logic: digit-boundary guard, best rank wins."""
    needle = needle.lower()
    L = len(needle)
    best = 0
    for hay, rank in ads:
        if rank <= best: continue
        pos = 0
        while True:
            p = hay.find(needle, pos)
            if p < 0: break
            nxt = hay[p + L] if (p + L) < len(hay) else ""
            if not nxt.isdigit():
                if rank > best: best = rank
                break
            pos = p + 1
        if best == 5: return 5
    return best

# ── 2) Walk each subsheet ───────────────────────────────────────────────────
wb = openpyxl.load_workbook(XLSX, data_only=True, read_only=True)
sheets_lower = {s.lower(): s for s in wb.sheetnames}

def find_sheet(name):
    return wb[name] if name in wb.sheetnames else (
        wb[sheets_lower[name.lower()]] if name.lower() in sheets_lower else None
    )

def header_index(headers, target):
    t = target.strip().lower()
    for i, h in enumerate(headers):
        if str(h or "").strip().lower() == t: return i
    return -1

print(f"\n{'='*94}")
print(f"  Per-subsheet matching counts (corrected Apps Script logic vs current Excel Status)")
print(f"{'='*94}")

grand = {"rows":0, "current_tested":0, "current_pending":0, "script_tested":0, "script_pending":0}

for sheet_name, key_hdr in SUBSHEETS:
    ws = find_sheet(sheet_name)
    if ws is None:
        print(f"\n[skip] sheet not found: {sheet_name}")
        continue
    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 2:
        print(f"\n[skip] {sheet_name}: empty"); continue
    headers = rows[0]
    key_col = header_index(headers, key_hdr)
    st_col  = header_index(headers, "Status")
    if key_col < 0 or st_col < 0:
        print(f"\n[skip] {sheet_name}: missing column ({key_hdr} or Status)"); continue

    total = current_t = current_p = current_other = 0
    script_t = script_p = 0
    by_label = defaultdict(int)
    mismatches_tested_to_pending = 0  # currently Tested but script would say Pending
    mismatches_pending_to_tested = 0  # currently Pending but script would say Tested

    for r in rows[1:]:
        if not r: continue
        key_val = r[key_col] if key_col < len(r) else None
        if not key_val: continue
        key = str(key_val).strip()
        if not key: continue
        total += 1

        current = (str(r[st_col]).strip().lower() if st_col < len(r) and r[st_col] else "")
        if current == "tested":      current_t += 1
        elif current == "pending":   current_p += 1
        else:                        current_other += 1

        rank = best_rank(key)
        if rank > 0:
            script_t += 1; by_label[LABEL[rank]] += 1
            if current == "pending": mismatches_pending_to_tested += 1
        else:
            script_p += 1
            if current == "tested":  mismatches_tested_to_pending += 1

    print(f"\n— {sheet_name} (key column: '{key_hdr}', non-empty rows: {total:,}) —")
    print(f"  current Excel: Tested={current_t:>5}  Pending={current_p:>5}  other/blank={current_other:>3}")
    print(f"  script would : Tested={script_t:>5}  Pending={script_p:>5}")
    print(f"  Tested result breakdown (script):")
    for lbl in ["Incremental Winner","Winner","Iteration","Analyse","Discarded"]:
        if by_label[lbl] or True:
            print(f"     {lbl:<22} {by_label[lbl]:>5}")
    if mismatches_pending_to_tested:
        print(f"  >> {mismatches_pending_to_tested} rows currently Pending would flip to Tested under corrected logic")
    if mismatches_tested_to_pending:
        print(f"  >> {mismatches_tested_to_pending} rows currently Tested would flip to Pending under corrected logic")

    grand["rows"]          += total
    grand["current_tested"]+= current_t
    grand["current_pending"]+= current_p
    grand["script_tested"] += script_t
    grand["script_pending"]+= script_p

wb.close()

print(f"\n{'='*94}")
print(f"  GRAND TOTAL across all subsheets")
print(f"{'='*94}")
print(f"  Total non-empty rows                : {grand['rows']:,}")
print(f"  Currently Tested in Excel           : {grand['current_tested']:,}")
print(f"  Currently Pending in Excel          : {grand['current_pending']:,}")
print(f"  Script would write Tested           : {grand['script_tested']:,}")
print(f"  Script would write Pending          : {grand['script_pending']:,}")
print(f"  Net change if you run corrected script: {grand['script_tested'] - grand['current_tested']:+,} Tested")
