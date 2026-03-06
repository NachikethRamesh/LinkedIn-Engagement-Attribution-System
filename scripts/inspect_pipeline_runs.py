import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pipeline_runs;")
            total_runs = cur.fetchone()[0]

            cur.execute(
                """
                SELECT status, COUNT(*)
                FROM pipeline_runs
                GROUP BY status
                ORDER BY COUNT(*) DESC, status
                """
            )
            by_status = cur.fetchall()

            cur.execute(
                """
                SELECT job_name, ROUND(AVG(duration_ms)::numeric, 1)
                FROM pipeline_runs
                WHERE duration_ms IS NOT NULL
                GROUP BY job_name
                ORDER BY job_name
                """
            )
            avg_duration = cur.fetchall()

            cur.execute(
                """
                SELECT run_id, job_name, started_at, duration_ms, LEFT(error_message, 220)
                FROM pipeline_runs
                WHERE status = 'failed'
                ORDER BY started_at DESC
                LIMIT 10
                """
            )
            latest_failed = cur.fetchall()

            cur.execute(
                """
                SELECT run_id, job_name, started_at, duration_ms, output_metrics_json
                FROM pipeline_runs
                WHERE status = 'success'
                ORDER BY started_at DESC
                LIMIT 10
                """
            )
            latest_success = cur.fetchall()

    print(f"total pipeline_runs: {total_runs}")
    print("\ncounts by status:")
    for status, count in by_status:
        print(f"- {status}: {count}")

    print("\navg duration ms by job:")
    for job, avg_ms in avg_duration:
        print(f"- {job}: {avg_ms}")

    print("\nlatest failed runs:")
    if not latest_failed:
        print("- none")
    for run_id, job_name, started_at, duration_ms, error_message in latest_failed:
        print(
            f"- run_id={run_id} job={job_name} started={started_at.isoformat()} "
            f"duration_ms={duration_ms} error={error_message}"
        )

    print("\nlatest successful runs:")
    if not latest_success:
        print("- none")
    for run_id, job_name, started_at, duration_ms, metrics in latest_success:
        metrics = metrics if isinstance(metrics, dict) else {}
        if job_name == "full_pipeline":
            summary = f"stages={list((metrics.get('stages') or {}).keys())}"
        else:
            summary = f"metrics_keys={list(metrics.keys())[:5]}"
        print(
            f"- run_id={run_id} job={job_name} started={started_at.isoformat()} "
            f"duration_ms={duration_ms} {summary}"
        )


if __name__ == "__main__":
    main()