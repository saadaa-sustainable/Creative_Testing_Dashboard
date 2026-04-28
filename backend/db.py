"""
db.py — Database connection utilities for Supabase/PostgreSQL
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Create a new PostgreSQL connection using SUPABASE_DB_URL from .env"""
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise ValueError("SUPABASE_DB_URL not found in .env")
    return psycopg2.connect(db_url)


def test_connection():
    """Test the database connection — returns True if successful, False otherwise"""
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False