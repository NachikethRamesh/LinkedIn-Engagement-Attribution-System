import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM social_events;")
            total_social_events = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM social_event_matches;")
            total_matches = cur.fetchone()[0]

            cur.execute(
                """
                SELECT match_type, COUNT(*)
                FROM social_event_matches
                GROUP BY match_type
                ORDER BY COUNT(*) DESC, match_type
                """
            )
            by_type = cur.fetchall()

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events se
                LEFT JOIN social_event_matches sem ON sem.social_event_id = se.id
                WHERE sem.social_event_id IS NULL
                """
            )
            no_match_rows = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events
                WHERE COALESCE((metadata_json->>'aggregated_import')::boolean, false) = true
                """
            )
            aggregate_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_event_matches sem
                JOIN social_events se ON se.id = sem.social_event_id
                WHERE COALESCE((se.metadata_json->>'aggregated_import')::boolean, false) = true
                  AND sem.matched_contact_id IS NOT NULL
                """
            )
            bad_aggregate_contact_matches = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT social_event_id
                    FROM social_event_matches
                    GROUP BY social_event_id
                    HAVING COUNT(*) > 1
                ) dup
                """
            )
            duplicate_match_rows = cur.fetchone()[0]

    print(f"total social_events: {total_social_events}")
    print(f"total social_event_matches: {total_matches}")
    print("counts by match_type:")
    for match_type, count in by_type:
        print(f"- {match_type}: {count}")
    print(f"events with no match row: {no_match_rows}")
    print(f"aggregate imports: {aggregate_count}")
    print(f"aggregate imports incorrectly contact-matched: {bad_aggregate_contact_matches}")
    print(f"duplicate social_event_id rows in social_event_matches: {duplicate_match_rows}")


if __name__ == "__main__":
    main()