"""Helpers for loading Supabase settings from .env and opening PostgreSQL connections."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv


def resolve_env_path(env_path: Optional[os.PathLike[str] | str] = None) -> Optional[Path]:
    """Return an existing env file path or None if none is found."""
    if env_path is not None:
        path = Path(env_path).expanduser().resolve()
        if path.exists():
            return path
        return None

    candidates = [
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent.parent / ".env",
        Path.cwd() / ".env",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def load_supabase_settings(env_path: Optional[os.PathLike[str] | str] = None) -> Dict[str, str]:
    """Load Supabase-related settings from a .env file and environment variables."""
    env_file = resolve_env_path(env_path)
    if env_file is not None:
        for key in list(os.environ):
            if key.startswith("SUPABASE_"):
                os.environ.pop(key, None)
        load_dotenv(dotenv_path=env_file, override=True)

    db_url = (
        os.getenv("SUPABASE_DB_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
        or ""
    ).strip()
    if not db_url:
        raise ValueError(
            "SUPABASE_DB_URL is required. Add it to your .env file or export it in the shell."
        )

    return {
        "db_url": db_url,
        "supabase_url": (
            os.getenv("SUPABASE_URL")
            or os.getenv("SUPABASE_PROJECT_URL")
            or ""
        ).strip(),
        "anon_key": (
            os.getenv("SUPABASE_ANON_KEY")
            or os.getenv("SUPABASE_ANON")
            or ""
        ).strip(),
        "service_role_key": (
            os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_SERVICE_ROLE")
            or ""
        ).strip(),
        "env_file": str(env_file) if env_file is not None else "",
    }


def get_supabase_connection(env_path: Optional[os.PathLike[str] | str] = None) -> Any:
    """Open a PostgreSQL connection using the Supabase DB URL from .env."""
    import psycopg2

    settings = load_supabase_settings(env_path=env_path)
    conn = psycopg2.connect(settings["db_url"], connect_timeout=30)
    conn.autocommit = False
    return conn
