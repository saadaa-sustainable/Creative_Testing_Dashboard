"""Create or validate the attribution tables used by this project."""

from __future__ import annotations

import argparse
from typing import Iterable

import psycopg2

from supabase_config import get_supabase_connection, load_supabase_settings


SHOPIFY_ATTR_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS shopify_ad_attribution (
    order_id          TEXT PRIMARY KEY,
    order_created_at  TIMESTAMPTZ,
    ordered_item      TEXT,
    total_price       NUMERIC,
    utm_campaign      TEXT,
    utm_content       TEXT,
    utm_medium        TEXT,
    utm_source        TEXT,
    utm_term          TEXT,
    ad_id             TEXT,
    ad_name           TEXT,
    campaign_name     TEXT,
    adset_id          TEXT,
    has_match         BOOLEAN,
    matched_value     TEXT,
    matched_tier      TEXT,
    last_synced_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sao_has_match ON shopify_ad_attribution (has_match);
CREATE INDEX IF NOT EXISTS idx_sao_ad_id     ON shopify_ad_attribution (ad_id) WHERE ad_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sao_created   ON shopify_ad_attribution (order_created_at DESC);
CREATE INDEX IF NOT EXISTS idx_sao_utm_source ON shopify_ad_attribution (utm_source);
"""

AE_RAW_VIEW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ae_raw_view (
    account_name           TEXT,
    campaign_name          TEXT,
    adset_id               TEXT,
    adset_name             TEXT,
    ad_id                  TEXT PRIMARY KEY,
    ad_name                TEXT,
    ad_created             DATE,
    reporting_starts       DATE,
    reporting_ends         DATE,
    date_target_imp_achieved DATE,
    date_of_result         DATE,
    days_to_result         INTEGER,
    days_to_target_f1      INTEGER,
    ad_status              TEXT,
    f1_pass                BOOLEAN,
    f2_pass                BOOLEAN,
    f3_pass                BOOLEAN,
    f4_pass                BOOLEAN,
    impressions            BIGINT,
    reach                  BIGINT,
    cost_per_1000          NUMERIC,
    frequency              NUMERIC,
    amount_spent           NUMERIC,
    cpc_link               NUMERIC,
    ctr_pct                NUMERIC,
    checkout_compl_pct     NUMERIC,
    cr_link_clicks_pct     NUMERIC,
    atc_lc_pct             NUMERIC,
    ci_atc_pct             NUMERIC,
    roas_ma                NUMERIC,
    cost_per_ftewv         NUMERIC,
    ftewv_count            BIGINT,
    cost_per_ncp           NUMERIC,
    ncp_count              BIGINT,
    ltv_reach              NUMERIC,
    ltv_frequency          NUMERIC,
    engagement_count       BIGINT,
    preview_link           TEXT,
    ad_link                TEXT,
    refreshed_at           TIMESTAMPTZ DEFAULT NOW(),
    conv_value             NUMERIC,
    purchases              NUMERIC,
    link_clicks_raw        BIGINT,
    ci_count               NUMERIC,
    atc_count              NUMERIC,
    source                 TEXT,
    first_seen_date        DATE
);
"""


def ensure_column(cur, table_name: str, column_name: str, column_sql: str) -> None:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
        )
        """,
        (table_name, column_name),
    )
    exists = cur.fetchone()[0]
    if not exists:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SHOPIFY_ATTR_TABLE_SQL)
        cur.execute(AE_RAW_VIEW_TABLE_SQL)

        ensure_column(cur, "shopify_ad_attribution", "order_created_at", "TIMESTAMPTZ")
        ensure_column(cur, "shopify_ad_attribution", "ordered_item", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "total_price", "NUMERIC")
        ensure_column(cur, "shopify_ad_attribution", "utm_campaign", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "utm_content", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "utm_medium", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "utm_source", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "utm_term", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "ad_id", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "ad_name", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "campaign_name", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "adset_id", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "has_match", "BOOLEAN")
        ensure_column(cur, "shopify_ad_attribution", "matched_value", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "matched_tier", "TEXT")
        ensure_column(cur, "shopify_ad_attribution", "last_synced_at", "TIMESTAMPTZ DEFAULT NOW()")

        ensure_column(cur, "ae_raw_view", "account_name", "TEXT")
        ensure_column(cur, "ae_raw_view", "campaign_name", "TEXT")
        ensure_column(cur, "ae_raw_view", "adset_id", "TEXT")
        ensure_column(cur, "ae_raw_view", "adset_name", "TEXT")
        ensure_column(cur, "ae_raw_view", "ad_id", "TEXT")
        ensure_column(cur, "ae_raw_view", "ad_name", "TEXT")
        ensure_column(cur, "ae_raw_view", "ad_created", "DATE")
        ensure_column(cur, "ae_raw_view", "reporting_starts", "DATE")
        ensure_column(cur, "ae_raw_view", "reporting_ends", "DATE")
        ensure_column(cur, "ae_raw_view", "date_target_imp_achieved", "DATE")
        ensure_column(cur, "ae_raw_view", "date_of_result", "DATE")
        ensure_column(cur, "ae_raw_view", "days_to_result", "INTEGER")
        ensure_column(cur, "ae_raw_view", "days_to_target_f1", "INTEGER")
        ensure_column(cur, "ae_raw_view", "ad_status", "TEXT")
        ensure_column(cur, "ae_raw_view", "f1_pass", "BOOLEAN")
        ensure_column(cur, "ae_raw_view", "f2_pass", "BOOLEAN")
        ensure_column(cur, "ae_raw_view", "f3_pass", "BOOLEAN")
        ensure_column(cur, "ae_raw_view", "f4_pass", "BOOLEAN")
        ensure_column(cur, "ae_raw_view", "impressions", "BIGINT")
        ensure_column(cur, "ae_raw_view", "reach", "BIGINT")
        ensure_column(cur, "ae_raw_view", "cost_per_1000", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "frequency", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "amount_spent", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "cpc_link", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "ctr_pct", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "checkout_compl_pct", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "cr_link_clicks_pct", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "atc_lc_pct", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "ci_atc_pct", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "roas_ma", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "cost_per_ftewv", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "ftewv_count", "BIGINT")
        ensure_column(cur, "ae_raw_view", "cost_per_ncp", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "ncp_count", "BIGINT")
        ensure_column(cur, "ae_raw_view", "ltv_reach", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "ltv_frequency", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "engagement_count", "BIGINT")
        ensure_column(cur, "ae_raw_view", "preview_link", "TEXT")
        ensure_column(cur, "ae_raw_view", "ad_link", "TEXT")
        ensure_column(cur, "ae_raw_view", "refreshed_at", "TIMESTAMPTZ DEFAULT NOW()")
        ensure_column(cur, "ae_raw_view", "conv_value", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "purchases", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "link_clicks_raw", "BIGINT")
        ensure_column(cur, "ae_raw_view", "ci_count", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "atc_count", "NUMERIC")
        ensure_column(cur, "ae_raw_view", "source", "TEXT")
        ensure_column(cur, "ae_raw_view", "first_seen_date", "DATE")

    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Create or validate the Supabase attribution schema")
    parser.add_argument("--env", dest="env_path", default=None, help="Optional path to a .env file")
    args = parser.parse_args()

    settings = load_supabase_settings(env_path=args.env_path)
    print(f"Using env file: {settings['env_file'] or '(environment)'}")
    conn = get_supabase_connection(env_path=args.env_path)
    try:
        ensure_schema(conn)
        print("Supabase schema is ready for shopify_ad_attribution and ae_raw_view")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
