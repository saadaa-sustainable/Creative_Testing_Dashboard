import sys, io, os, psycopg2
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="backslashreplace")
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"].strip()); cur = conn.cursor()

def hdr(t): print(f"\n{'-'*78}\n  {t}\n{'-'*78}")

# Mirror of dashboard JS getAdCategory()
def js_get_category(imp, ct_roas, cpn, cpf, t_imp, t_roas, t_cpn, t_fte):
    f1 = imp >= t_imp
    f2 = ct_roas >= t_roas
    f3 = cpn > 0 and cpn <= t_cpn
    f4 = cpf > 0 and cpf <= t_fte
    if f1 and f2 and f3 and f4: return "Incremental Winner"
    if f1 and f2 and f3:        return "Winner"
    if f1 and f4:               return "Priority"
    if f1:                      return "Analyze 1"
    if f2:                      return "Analyze 2"
    return "Discarded"

# ────────────────────────────────────────────────────────────
hdr("TEST A: getAdCategory() — JS logic vs SQL category (every ad)")
cur.execute("""SELECT ad_id, impressions, amount_spent, conv_value,
                      cost_per_ncp, cost_per_ftewv, category
               FROM ae_view_full""")
rows = cur.fetchall()
T_IMP, T_ROAS, T_CPN, T_FTE = 50000, 3.2, 525, 25
mismatches = []
counts = {}
for ad_id, imp, spend, conv, cpn, cpf, sql_cat in rows:
    imp = int(imp or 0); spend = float(spend or 0); conv = float(conv or 0)
    cpn = float(cpn or 0); cpf = float(cpf or 0)
    ct_roas = (conv/spend) if spend > 0 else 0
    js_cat = js_get_category(imp, ct_roas, cpn, cpf, T_IMP, T_ROAS, T_CPN, T_FTE)
    counts[js_cat] = counts.get(js_cat, 0) + 1
    if js_cat != sql_cat:
        mismatches.append((ad_id, sql_cat, js_cat, imp, spend, conv, cpn, cpf))
print(f"  total ads checked     : {len(rows):,}")
print(f"  mismatches (JS vs SQL): {len(mismatches):,}")
if mismatches:
    print("  first 5 mismatches:")
    for m in mismatches[:5]:
        print(f"    ad_id={m[0]}  SQL={m[1]}  JS={m[2]}  imp={m[3]}  spend={m[4]:.0f}  conv={m[5]:.0f}  cpn={m[6]:.2f}  cpf={m[7]:.2f}")
else:
    print("  PASS — JS getAdCategory() exactly reproduces SQL CASE")

# ────────────────────────────────────────────────────────────
hdr("TEST B: F-flag boolean values vs JS recompute (every ad)")
cur.execute("""SELECT ad_id, impressions, amount_spent, conv_value,
                      cost_per_ncp, cost_per_ftewv, f1_pass, f2_pass, f3_pass, f4_pass
               FROM ae_view_full""")
fdiff = {"f1":0,"f2":0,"f3":0,"f4":0}
for ad_id, imp, spend, conv, cpn, cpf, f1, f2, f3, f4 in cur.fetchall():
    imp = int(imp or 0); spend = float(spend or 0); conv = float(conv or 0)
    cpn = float(cpn or 0); cpf = float(cpf or 0)
    js_f1 = imp >= 50000
    js_f2 = spend > 0 and conv/spend >= 3.2
    js_f3 = cpn > 0 and cpn <= 525
    js_f4 = cpf > 0 and cpf <= 25
    if bool(f1) != js_f1: fdiff["f1"] += 1
    if bool(f2) != js_f2: fdiff["f2"] += 1
    if bool(f3) != js_f3: fdiff["f3"] += 1
    if bool(f4) != js_f4: fdiff["f4"] += 1
for k,v in fdiff.items():
    print(f'  {k} mismatches: {v}  {"PASS" if v==0 else "FAIL"}')

# ────────────────────────────────────────────────────────────
hdr("TEST C: AE bucket counts (chips at top of AE panel)")
cur.execute("SELECT category, COUNT(*) FROM ae_view_full GROUP BY 1 ORDER BY 2 DESC")
for c, n in cur.fetchall(): print(f"  {c:<22} {n:>6,}")

# ────────────────────────────────────────────────────────────
hdr("TEST D: Per-account totals must sum to grand total")
cur.execute("""SELECT account_name, COUNT(*), SUM(impressions), ROUND(SUM(amount_spent),0),
                      SUM(shopify_orders), ROUND(SUM(shopify_sales),0)
               FROM ae_view_full GROUP BY 1""")
gt = {"n":0,"i":0,"s":0,"o":0,"ss":0}
for a, n, i, s, o, ss in cur.fetchall():
    print(f"  {a:<28} {n:>6,} ads  imp={int(i or 0):>13,}  spend=Rs {float(s or 0):>13,.0f}  shop_o={int(o or 0):>7,}  shop_s=Rs {float(ss or 0):>13,.0f}")
    gt["n"]+=n; gt["i"]+=int(i or 0); gt["s"]+=float(s or 0); gt["o"]+=int(o or 0); gt["ss"]+=float(ss or 0)
cur.execute("SELECT COUNT(*), SUM(impressions), ROUND(SUM(amount_spent),0), SUM(shopify_orders), ROUND(SUM(shopify_sales),0) FROM ae_view_full")
n, i, s, o, ss = cur.fetchone()
print(f"  {'SUMMED':<28} {gt['n']:>6,} ads  imp={gt['i']:>13,}  spend=Rs {gt['s']:>13,.0f}  shop_o={gt['o']:>7,}  shop_s=Rs {gt['ss']:>13,.0f}")
print(f"  {'GRAND TOTAL':<28} {n:>6,} ads  imp={int(i or 0):>13,}  spend=Rs {float(s or 0):>13,.0f}  shop_o={int(o or 0):>7,}  shop_s=Rs {float(ss or 0):>13,.0f}")
ok = gt["n"]==n and gt["i"]==int(i) and abs(gt["s"]-float(s))<1 and gt["o"]==int(o or 0) and abs(gt["ss"]-float(ss or 0))<1
print(f'  -> per-account == grand total: {"PASS" if ok else "FAIL"}')

# ────────────────────────────────────────────────────────────
hdr("TEST E: date_of_result math")
cur.execute("""SELECT COUNT(*) FROM ae_view_full
                WHERE date_target_imp_achieved IS NOT NULL
                  AND days_to_result IS NOT NULL AND days_to_target_f1 IS NOT NULL
                  AND days_to_result <> days_to_target_f1""")
d = cur.fetchone()[0]
print(f'  F1-hit ads where days_to_result != days_to_target_f1: {d}  {"PASS" if d==0 else "FAIL"}')
cur.execute("""SELECT COUNT(*) FROM ae_view_full
                WHERE date_target_imp_achieved IS NULL
                  AND first_seen_date IS NOT NULL
                  AND days_to_result <> 14""")
d = cur.fetchone()[0]
print(f'  F1-not-hit ads where days_to_result != 14: {d}  {"PASS" if d==0 else "FAIL"}')

# ────────────────────────────────────────────────────────────
hdr("TEST F: F1 boundary check")
cur.execute("SELECT COUNT(*) FROM ae_view_full WHERE impressions = 50000")
print(f"  ads exactly at impressions=50000: {cur.fetchone()[0]}")
cur.execute("SELECT COUNT(*) FROM ae_view_full WHERE impressions = 49999 AND f1_pass")
n_off = cur.fetchone()[0]
print(f'  ads at 49,999 imp with f1_pass=true (should be 0): {n_off}  {"PASS" if n_off==0 else "FAIL"}')
cur.execute("SELECT COUNT(*) FROM ae_view_full WHERE impressions > 49999 AND impressions < 50001 AND NOT f1_pass")
n_off = cur.fetchone()[0]
print(f'  ads at 50,000 imp with f1_pass=false (should be 0): {n_off}  {"PASS" if n_off==0 else "FAIL"}')

# ────────────────────────────────────────────────────────────
hdr("TEST G: Cross-table referential integrity")
cur.execute("SELECT COUNT(DISTINCT ad_id) FROM ae_view_full WHERE ad_id NOT IN (SELECT ad_id FROM summary_table)")
n1 = cur.fetchone()[0]
print(f'  ae_view_full ads missing from summary_table: {n1}  {"PASS" if n1==0 else "FAIL"}')
cur.execute("SELECT COUNT(DISTINCT ad_id) FROM summary_table WHERE ad_id NOT IN (SELECT ad_id FROM ae_view_full)")
n2 = cur.fetchone()[0]
print(f'  summary_table ads missing from ae_view_full: {n2}  {"PASS" if n2==0 else "FAIL"}')

# ────────────────────────────────────────────────────────────
hdr("TEST H: Modal-data spot check (top 3 by shopify_sales)")
cur.execute("""SELECT ad_id, ad_name, shopify_orders, shopify_sales
                FROM ae_view_full WHERE shopify_sales IS NOT NULL
                ORDER BY shopify_sales DESC LIMIT 3""")
for ad_id, name, o, s in cur.fetchall():
    cur.execute("""SELECT COUNT(*), ROUND(SUM(total_price),2)
                    FROM shopify_ad_attribution
                    WHERE has_match AND ad_id = %s""", (ad_id,))
    r_o, r_s = cur.fetchone()
    ok_o = (o == r_o)
    ok_s = (abs(float(s)-float(r_s or 0)) < 0.01)
    print(f'  {name[:50]:<52}  o:{o:>5} (raw:{r_o:>5}) {"OK" if ok_o else "DIFF"}   s:Rs{float(s):>11,.0f} (raw:{float(r_s or 0):>11,.0f}) {"OK" if ok_s else "DIFF"}')

# ────────────────────────────────────────────────────────────
hdr("TEST I: Lifecycle panel — ad_results")
cur.execute("""SELECT result_status, COUNT(*), ROUND(AVG(impressions_14d),0)
                FROM ad_results GROUP BY 1 ORDER BY 2 DESC""")
for r, n, avg in cur.fetchall():
    print(f"  {str(r):<22} n={n:>5,}  avg(impressions_14d)={int(avg or 0):>9,}")

# ────────────────────────────────────────────────────────────
hdr("TEST J: results_table snapshot")
cur.execute("""SELECT account_name, computed_at,
                       CASE WHEN ads_json IS NULL THEN 0
                            ELSE jsonb_array_length(ads_json::jsonb) END
                FROM results_table ORDER BY computed_at DESC""")
for r in cur.fetchall(): print(f"  {r[0]:<28} computed_at={r[1]}  ads={r[2]}")

# ────────────────────────────────────────────────────────────
hdr("TEST K: Excel CTP match consistency (summary_table)")
cur.execute("""SELECT
  COUNT(*) FILTER (WHERE excel_id_matched IS NOT NULL AND excel_status IS NULL),
  COUNT(*) FILTER (WHERE excel_status IS NOT NULL AND excel_id_matched IS NULL),
  COUNT(*) FILTER (WHERE excel_result IS NOT NULL AND excel_status IS NULL)
FROM summary_table""")
a, b, c = cur.fetchone()
print(f'  matched_id without status (should be 0): {a}  {"PASS" if a==0 else "FAIL"}')
print(f'  status without matched_id (should be 0): {b}  {"PASS" if b==0 else "FAIL"}')
print(f'  result without status (should be 0): {c}  {"PASS" if c==0 else "FAIL"}')

# ────────────────────────────────────────────────────────────
hdr("TEST L: F4 threshold change verification (was 12, now 25)")
cur.execute("""SELECT COUNT(*) FROM ae_view_full
               WHERE cost_per_ftewv > 0 AND cost_per_ftewv <= 25 AND NOT f4_pass""")
n1 = cur.fetchone()[0]
print(f'  ads with cost/FTEWV in [0,25] but f4_pass=false (should be 0): {n1}  {"PASS" if n1==0 else "FAIL"}')
cur.execute("""SELECT COUNT(*) FROM ae_view_full
               WHERE cost_per_ftewv > 25 AND f4_pass""")
n2 = cur.fetchone()[0]
print(f'  ads with cost/FTEWV > 25 but f4_pass=true (should be 0): {n2}  {"PASS" if n2==0 else "FAIL"}')

# ────────────────────────────────────────────────────────────
hdr("TEST M: ad_status counts match between summary_table (primary-latest) and Meta direct")
print("  Note: only summary_table.ad_status is primary-table-latest. ae_view_full uses backfill-latest.")
cur.execute("""SELECT ad_status, COUNT(*) FROM summary_table WHERE ad_status IS NOT NULL GROUP BY 1 ORDER BY 2 DESC""")
for s, n in cur.fetchall(): print(f"  {s:<22} {n:>6,}")

# ────────────────────────────────────────────────────────────
hdr("TEST N: F-flag sums equal category sums (logical consistency)")
cur.execute("SELECT COUNT(*) FILTER (WHERE f1_pass AND f2_pass AND f3_pass AND f4_pass), COUNT(*) FILTER (WHERE category='Incremental Winner') FROM ae_view_full")
a, b = cur.fetchone()
print(f'  IW: f1234 sum={a}  category sum={b}  {"PASS" if a==b else "FAIL"}')
cur.execute("SELECT COUNT(*) FILTER (WHERE f1_pass AND f2_pass AND f3_pass AND NOT f4_pass), COUNT(*) FILTER (WHERE category='Winner') FROM ae_view_full")
a, b = cur.fetchone()
print(f'  Winner: f123-notf4 sum={a}  category sum={b}  {"PASS" if a==b else "FAIL"}')
cur.execute("SELECT COUNT(*) FILTER (WHERE f1_pass AND NOT (f2_pass AND f3_pass) AND f4_pass), COUNT(*) FILTER (WHERE category='Priority') FROM ae_view_full")
a, b = cur.fetchone()
print(f'  Priority: f1-not(f2&f3)-f4 sum={a}  category sum={b}  {"PASS" if a==b else "FAIL"}')

conn.close()
print("\n" + "="*78)
print("  REGRESSION SWEEP COMPLETE")
print("="*78)
