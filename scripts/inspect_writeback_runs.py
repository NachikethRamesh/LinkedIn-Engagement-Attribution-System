import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM writeback_runs;")
            total_runs = cur.fetchone()[0]

            cur.execute(
                """
                SELECT status, COUNT(*)
                FROM writeback_runs
                GROUP BY status
                ORDER BY COUNT(*) DESC, status
                """
            )
            by_status = cur.fetchall()

            cur.execute(
                """
                SELECT target_type, COUNT(*)
                FROM writeback_runs
                GROUP BY target_type
                ORDER BY COUNT(*) DESC, target_type
                """
            )
            by_target = cur.fetchall()

            cur.execute(
                """
                SELECT writeback_run_id, target_type, status, started_at, duration_ms
                FROM writeback_runs
                ORDER BY started_at DESC
                LIMIT 10
                """
            )
            recent_runs = cur.fetchall()

            cur.execute(
                """
                SELECT status, COUNT(*)
                FROM writeback_records
                GROUP BY status
                ORDER BY COUNT(*) DESC, status
                """
            )
            record_status = cur.fetchall()

    print(f"total writeback_runs: {total_runs}")
    print("\nruns by status:")
    for status, count in by_status:
        print(f"- {status}: {count}")

    print("\nruns by target:")
    for target, count in by_target:
        print(f"- {target}: {count}")

    print("\nwriteback_records by status:")
    for status, count in record_status:
        print(f"- {status}: {count}")

    print("\nlatest runs:")
    for run_id, target_type, status, started_at, duration_ms in recent_runs:
        print(
            f"- run_id={run_id} target={target_type} status={status} "
            f"started={started_at.isoformat()} duration_ms={duration_ms}"
        )


if __name__ == "__main__":
    main()
