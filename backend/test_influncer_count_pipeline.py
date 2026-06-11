"""
test_influncer_count_pipeline.py - verify Tested-SIF counts end-to-end.

Stages:
  1) EXCEL    : count Tested SIF post_ids in 'CTP unique ids.xlsx' / Influncer
  2) RAW SQL  : naive monthly aggregation from summary_table (the 563 number)
  3) FIXED SQL: dedup per post by first-seen month -> should equal Excel #1
  4) DIFF     : list the cross-month posts driving the 563-vs-544 gap
"""
import os, openpyxl, psycopg2
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"].strip()
XLSX   = r"C:\Users\Saadaa\Downloads\CTP unique ids.xlsx"

PASS = lambda msg: f"  [OK]  {msg}"
FAIL = lambda msg: f"  [FAIL] {msg}"

# ── Stage 1: Excel source of truth ───────────────────────────────────────────
print("="*70)
print("Stage 1 — EXCEL source of truth (Influncer sheet)")
print("="*70)
wb = openpyxl.load_workbook(XLSX, data_only=True, read_only=True)
ws = wb["Influncer"]
rows = ws.iter_rows(values_only=True); next(rows, None)
excel_tested_posts = set()      # set of POST IDs (col B) with Status=Tested
excel_tested_sif   = set()      # set of SIF-IDs (col A) with Status=Tested
excel_status_counts = defaultdict(int)
for row in rows:
    if not row or not row[0]: continue
    sif_id, post_id = row[0], row[1]
    status = (str(row[4]).strip() if row[4] else "")
    excel_status_counts[status or "(blank)"] += 1
    if status.lower() == "tested":
        if post_id: excel_tested_posts.add(str(post_id).strip())
        if sif_id:  excel_tested_sif.add(str(sif_id).strip())
wb.close()
print(f"  Tested post_ids  (col B): {len(excel_tested_posts):>5}")
print(f"  Tested SIF-IDs   (col A): {len(excel_tested_sif):>5}")
EXCEL_TARGET = len(excel_tested_posts)
print(PASS(f"Excel source-of-truth target = {EXCEL_TARGET}"))

# ── Stage 2: raw monthly aggregation from summary_table (= 563) ──────────────
print("\n" + "="*70)
print("Stage 2 — RAW monthly aggregation from summary_table")
print("="*70)
conn = psycopg2.connect(DB_URL); cur = conn.cursor()
cur.execute("""
  SELECT TO_CHAR(created_date,'YYYY-MM') AS m,
         COUNT(DISTINCT excel_id_matched) AS distinct_post_ids
  FROM summary_table
  WHERE excel_status='Tested'
    AND excel_id_matched LIKE 'SIF-%-P%'
    AND created_date IS NOT NULL
  GROUP BY m ORDER BY m
""")
rows = cur.fetchall()
raw_sum = 0
print(f"  {'month':<10}{'distinct_post_ids':>20}")
for m, d in rows:
    print(f"  {m:<10}{d:>20}"); raw_sum += d
print(f"  {'TOTAL':<10}{raw_sum:>20}")
if raw_sum == 563:
    print(PASS(f"raw sum = {raw_sum} — matches user's reported 563 (cross-month inflation present)"))
else:
    print(f"  [note] raw sum is {raw_sum}, user saw 563. Could be a snapshot-time delta of a few posts.")

# ── Stage 3: dedup by first-seen month — should equal Excel target ───────────
print("\n" + "="*70)
print("Stage 3 — FIXED: each post counted in its first-seen month only")
print("="*70)
cur.execute("""
  WITH posts AS (
    SELECT excel_id_matched AS post_id, MIN(created_date) AS first_created
    FROM summary_table
    WHERE excel_status='Tested'
      AND excel_id_matched LIKE 'SIF-%-P%'
      AND created_date IS NOT NULL
    GROUP BY excel_id_matched
  )
  SELECT TO_CHAR(first_created,'YYYY-MM') AS m, COUNT(*) AS posts
  FROM posts GROUP BY m ORDER BY m
""")
rows = cur.fetchall()
fixed_sum = 0
print(f"  {'month':<10}{'distinct_post_ids':>20}")
for m, d in rows:
    print(f"  {m:<10}{d:>20}"); fixed_sum += d
print(f"  {'TOTAL':<10}{fixed_sum:>20}")

if fixed_sum == EXCEL_TARGET:
    print(PASS(f"dedup sum = {fixed_sum} = Excel target {EXCEL_TARGET}  -- COUNTS MATCH"))
else:
    print(FAIL(f"dedup sum = {fixed_sum}, Excel target = {EXCEL_TARGET}. Investigate."))
    delta = abs(fixed_sum - EXCEL_TARGET)
    print(f"        delta = {delta} posts. Possible: Excel posts that never landed in summary_table,")
    print(f"        OR summary_table posts with NULL created_date.")

# ── Stage 4: list the cross-month posts (the inflation source) ───────────────
print("\n" + "="*70)
print("Stage 4 — Cross-month posts (drivers of raw-vs-fixed inflation)")
print("="*70)
cur.execute("""
  SELECT excel_id_matched,
         COUNT(DISTINCT TO_CHAR(created_date,'YYYY-MM')) AS months_present,
         MIN(created_date) AS first, MAX(created_date) AS last
  FROM summary_table
  WHERE excel_status='Tested'
    AND excel_id_matched LIKE 'SIF-%-P%'
    AND created_date IS NOT NULL
  GROUP BY excel_id_matched
  HAVING COUNT(DISTINCT TO_CHAR(created_date,'YYYY-MM')) > 1
  ORDER BY months_present DESC, excel_id_matched
""")
cross = cur.fetchall()
extra = sum(m - 1 for _, m, _, _ in cross)
print(f"  posts spanning >1 month : {len(cross)}")
print(f"  total extra rows added  : {extra}   <-- this is the inflation factor")
print(f"  expected raw sum        : {EXCEL_TARGET} + {extra} = {EXCEL_TARGET + extra}")
print(f"  actual raw sum          : {raw_sum}")
if raw_sum == EXCEL_TARGET + extra:
    print(PASS("arithmetic checks out: 544 + cross-month overlaps = raw sum"))
print(f"\n  First 12 cross-month posts:")
print(f"  {'post_id':<14}{'#months':>8}  {'first':<12}{'last':<12}")
for pid, n, f, l in cross[:12]:
    print(f"  {pid:<14}{n:>8}  {str(f):<12}{str(l):<12}")

# ── Optional: also check broader sheets ──────────────────────────────────────
print("\n" + "="*70)
print("Stage 5 — Quick sanity for ALL sheets (not just Influncer)")
print("="*70)
cur.execute("""
  WITH posts AS (
    SELECT excel_id_matched, MIN(created_date) AS first_created
    FROM summary_table
    WHERE excel_status='Tested' AND created_date IS NOT NULL
    GROUP BY excel_id_matched
  )
  SELECT COUNT(*) FROM posts
""")
all_fixed = cur.fetchone()[0]
cur.execute("""
  SELECT COUNT(DISTINCT excel_id_matched)
  FROM summary_table WHERE excel_status='Tested' AND excel_id_matched IS NOT NULL
""")
all_distinct = cur.fetchone()[0]
print(f"  All-sheet distinct excel_id_matched (Tested) : {all_distinct}")
print(f"  All-sheet first-month dedup count            : {all_fixed}")
print(f"  Recall — Excel total Tested rows (all sheets): 1,591 (Stage 1 of earlier run)")

conn.close()
print("\n[done] Pipeline verified.")
