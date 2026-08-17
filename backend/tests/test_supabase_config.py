import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from supabase_config import load_supabase_settings


class SupabaseConfigTests(unittest.TestCase):
    def test_loads_values_from_dotenv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text(
                "SUPABASE_DB_URL=postgresql://user:pass@host:5432/postgres\n"
                "SUPABASE_URL=https://demo.supabase.co\n"
                "SUPABASE_ANON_KEY=demo-anon-key\n",
                encoding="utf-8",
            )

            settings = load_supabase_settings(env_path=env_path)

            self.assertEqual(settings["db_url"], "postgresql://user:pass@host:5432/postgres")
            self.assertEqual(settings["supabase_url"], "https://demo.supabase.co")
            self.assertEqual(settings["anon_key"], "demo-anon-key")

    def test_rejects_missing_db_url(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text(
                "SUPABASE_URL=https://demo.supabase.co\n",
                encoding="utf-8",
            )

            with self.assertRaises(ValueError):
                load_supabase_settings(env_path=env_path)


if __name__ == "__main__":
    unittest.main()
