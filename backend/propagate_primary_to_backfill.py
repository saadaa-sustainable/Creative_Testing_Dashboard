"""
propagate_primary_to_backfill.py - mirror the latest values from
primary_table into backfill_table for any (account_name, ad_id, date)
where both tables have data, and INSERT new rows that don't exist yet.

Use this right after running `primary_sync.py hourly|daily` to keep
backfill_table caught up with the freshest numbers Meta has returned.
"""
import os, sys, psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ["SUPABASE_DB_URL"].strip()

# Sync window — last N days (matches primary_sync hourly = 3, daily = 15)
WINDOW_DAYS = 15

def main():
    conn = psycopg2.connect(DB_URL); conn.autocommit = False
    cur  = conn.cursor()

    # Backfill_table can now have 350K+ rows (paused-ad placeholders included);
    # the two big DML statements below regularly exceed Supabase's default 60s
    # statement_timeout. Lift it for this session — they are still bounded by
    # the WHERE-window so won't run forever.
    cur.execute("SET statement_timeout = '30min'")
    cur.execute("SET lock_timeout = '5min'")

    # ── 1) UPDATE matching rows
    print(f"[run] UPDATE backfill_table rows from primary_table (last {WINDOW_DAYS} days) ...")
    cur.execute(f"""
      UPDATE backfill_table b
      SET impressions      = p.impressions,
          reach            = p.reach,
          frequency        = p.frequency,
          amount_spent_inr = p.amount_spent_inr,
          ad_status        = p.ad_status,
          purchase_roas    = p.purchase_roas,
          outbound_clicks  = p.outbound_clicks,
          thruplays        = p.thruplays,
          three_sec_video_plays = p.three_sec_video_plays,
          post_engagements = p.post_engagements,
          conversion_value = p.conversion_value,
          video_play_time  = p.video_play_time,
          ftewv_count      = p.ftewv_count,
          cost_per_ftewv   = p.cost_per_ftewv,
          ncp_count        = p.ncp_count,
          cost_per_ncp     = p.cost_per_ncp,
          ltv_reach        = p.ltv_reach,
          ltv_frequency    = p.ltv_frequency,
          campaign_name    = p.campaign_name,
          campaign_id      = p.campaign_id,
          ad_created_date  = COALESCE(p.ad_created_date, b.ad_created_date)
      FROM primary_table p
      WHERE p.account_name = b.account_name
        AND p.ad_id        = b.ad_id
        AND p.date         = b.date
        AND p.date >= CURRENT_DATE - INTERVAL '{WINDOW_DAYS} days'
    """)
    print(f"   [ok] backfill_table rows refreshed: {cur.rowcount:,}")

    # ── 2) INSERT new rows (primary_table has them, backfill_table doesn't)
    print(f"[run] INSERT missing rows from primary_table into backfill_table ...")
    cur.execute(f"""
      INSERT INTO backfill_table (
        account_name, date, ad_id, ad_name, campaign_name, campaign_id,
        ad_status, ad_created_date, impressions, reach, frequency,
        amount_spent_inr, purchase_roas, outbound_clicks, thruplays,
        three_sec_video_plays, post_engagements, conversion_value,
        video_play_time, ftewv_count, cost_per_ftewv, ncp_count, cost_per_ncp,
        ltv_reach, ltv_frequency
      )
      SELECT
        p.account_name, p.date, p.ad_id, p.ad_name, p.campaign_name, p.campaign_id,
        p.ad_status, p.ad_created_date, p.impressions, p.reach, p.frequency,
        p.amount_spent_inr, p.purchase_roas, p.outbound_clicks, p.thruplays,
        p.three_sec_video_plays, p.post_engagements, p.conversion_value,
        p.video_play_time, p.ftewv_count, p.cost_per_ftewv, p.ncp_count, p.cost_per_ncp,
        p.ltv_reach, p.ltv_frequency
      FROM primary_table p
      WHERE p.date >= CURRENT_DATE - INTERVAL '{WINDOW_DAYS} days'
        AND NOT EXISTS (
          SELECT 1 FROM backfill_table b
          WHERE b.account_name=p.account_name AND b.ad_id=p.ad_id AND b.date=p.date
        )
    """)
    print(f"   [ok] backfill_table rows inserted (new): {cur.rowcount:,}")

    conn.commit(); print("[commit]")

    # ── 3) Coverage stats
    cur.execute("""
      SELECT
        MAX(date) AS max_date,
        COUNT(*) FILTER (WHERE date = CURRENT_DATE)         AS today_rows,
        COUNT(*) FILTER (WHERE date = CURRENT_DATE - 1)     AS yest_rows,
        COUNT(*) FILTER (WHERE date >= CURRENT_DATE - 7)    AS last7_rows
      FROM primary_table
    """)
    p_max, p_today, p_yest, p_7 = cur.fetchone()
    cur.execute("""
      SELECT
        MAX(date) AS max_date,
        COUNT(*) FILTER (WHERE date = CURRENT_DATE)         AS today_rows,
        COUNT(*) FILTER (WHERE date = CURRENT_DATE - 1)     AS yest_rows,
        COUNT(*) FILTER (WHERE date >= CURRENT_DATE - 7)    AS last7_rows
      FROM backfill_table
    """)
    b_max, b_today, b_yest, b_7 = cur.fetchone()

    print(f"\n[stats] latest data coverage")
    print(f"  primary_table  max_date={p_max}  today={p_today:,}  yest={p_yest:,}  last7={p_7:,}")
    print(f"  backfill_table max_date={b_max}  today={b_today:,}  yest={b_yest:,}  last7={b_7:,}")

    cur.close(); conn.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
