from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import load_environment


def main() -> None:
    load_environment()

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "social_attribution_engine")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    print("DB config (effective):")
    print(f"- host: {host}")
    print(f"- port: {port}")
    print(f"- db: {db}")
    print(f"- user: {user}")
    print("- password: [set]" if password else "- password: [missing]")

    dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    try:
        conn = psycopg2.connect(dsn)
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            _ = cur.fetchone()
        conn.close()
    except Exception as exc:
        raise SystemExit(f"DB connectivity check failed: {exc}")

    print("DB connectivity check passed.")


if __name__ == "__main__":
    main()
