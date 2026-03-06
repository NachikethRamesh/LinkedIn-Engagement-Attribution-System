from pathlib import Path

from app.db import get_connection


def main() -> None:
    schema_path = Path(__file__).resolve().parent.parent / "db" / "schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()

    print("Schema initialized successfully.")


if __name__ == "__main__":
    main()