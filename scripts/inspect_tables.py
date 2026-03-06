from app.db import get_connection

SOURCE_TABLES = [
    "posts",
    "social_events",
    "accounts",
    "contacts",
    "website_events",
    "opportunities",
]

DERIVED_TABLES = [
    "account_intent_scores",
    "opportunity_influence",
]


def main() -> None:
    counts = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table in SOURCE_TABLES + DERIVED_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                counts[table] = count
                print(f"{table}: {count}")

    print("\nvalidation:")
    source_ok = all(counts[table] > 0 for table in SOURCE_TABLES)
    derived_ok = all(counts[table] == 0 for table in DERIVED_TABLES)
    print(f"- source tables populated: {'PASS' if source_ok else 'FAIL'}")
    print(f"- derived tables empty: {'PASS' if derived_ok else 'FAIL'}")

    if not (source_ok and derived_ok):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
