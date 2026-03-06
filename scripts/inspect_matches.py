import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM social_event_matches;")
            total = cur.fetchone()[0]
            print(f"social_event_matches total: {total}")

            cur.execute(
                """
                SELECT match_type, COUNT(*)
                FROM social_event_matches
                GROUP BY match_type
                ORDER BY COUNT(*) DESC, match_type
                """
            )
            rows = cur.fetchall()
            print("\nmatch type counts:")
            for match_type, count in rows:
                print(f"- {match_type}: {count}")

            cur.execute(
                """
                SELECT sem.social_event_id, sem.match_type, sem.match_confidence, sem.match_reason,
                       sem.matched_contact_id, sem.matched_account_id
                FROM social_event_matches sem
                ORDER BY sem.id DESC
                LIMIT 20
                """
            )
            samples = cur.fetchall()
            print("\nrecent samples:")
            for sample in samples:
                print(
                    f"- social_event_id={sample[0]} type={sample[1]} confidence={sample[2]} "
                    f"contact={sample[4]} account={sample[5]} reason={sample[3]}"
                )


if __name__ == "__main__":
    main()
