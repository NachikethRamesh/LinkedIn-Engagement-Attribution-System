from pathlib import Path

from app.db import get_connection


def main() -> None:
    schema_path = Path(__file__).resolve().parent.parent / "db" / "schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE;")
            cur.execute("CREATE SCHEMA public;")
            cur.execute(schema_sql)
        conn.commit()

    print("Database reset complete (schema dropped and recreated).")


if __name__ == "__main__":
    main()
