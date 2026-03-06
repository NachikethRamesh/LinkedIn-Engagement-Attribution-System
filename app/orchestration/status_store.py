from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from psycopg2.extras import Json

from app.db import get_connection
from app.orchestration.types import RunRecord


class PipelineRunStore:
    def create_run(
        self,
        run_id: str,
        job_name: str,
        stage_name: str | None,
        trigger_source: str,
        input_params: dict[str, Any],
    ) -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_runs (
                        run_id,
                        job_name,
                        stage_name,
                        status,
                        started_at,
                        trigger_source,
                        input_params_json,
                        output_metrics_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id,
                        job_name,
                        stage_name,
                        "running",
                        datetime.now(UTC),
                        trigger_source,
                        Json(input_params),
                        Json({}),
                    ),
                )
            conn.commit()

    def complete_run(self, run_id: str, status: str, output_metrics: dict[str, Any], error_message: str | None) -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline_runs
                    SET
                        status = %s,
                        completed_at = %s,
                        duration_ms = EXTRACT(EPOCH FROM (%s - started_at)) * 1000,
                        output_metrics_json = %s,
                        error_message = %s
                    WHERE run_id = %s
                    """,
                    (
                        status,
                        datetime.now(UTC),
                        datetime.now(UTC),
                        Json(output_metrics),
                        error_message,
                        run_id,
                    ),
                )
            conn.commit()

    def get_run(self, run_id: str) -> RunRecord | None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        run_id,
                        job_name,
                        stage_name,
                        status,
                        started_at,
                        completed_at,
                        duration_ms,
                        trigger_source,
                        input_params_json,
                        output_metrics_json,
                        error_message
                    FROM pipeline_runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()

        if row is None:
            return None

        return RunRecord(
            run_id=row[0],
            job_name=row[1],
            stage_name=row[2],
            status=row[3],
            started_at=row[4],
            completed_at=row[5],
            duration_ms=int(row[6]) if row[6] is not None else None,
            trigger_source=row[7],
            input_params_json=row[8] if isinstance(row[8], dict) else {},
            output_metrics_json=row[9] if isinstance(row[9], dict) else {},
            error_message=row[10],
        )

    def list_runs(self, limit: int = 100) -> list[RunRecord]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        run_id,
                        job_name,
                        stage_name,
                        status,
                        started_at,
                        completed_at,
                        duration_ms,
                        trigger_source,
                        input_params_json,
                        output_metrics_json,
                        error_message
                    FROM pipeline_runs
                    ORDER BY started_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()

        return [
            RunRecord(
                run_id=row[0],
                job_name=row[1],
                stage_name=row[2],
                status=row[3],
                started_at=row[4],
                completed_at=row[5],
                duration_ms=int(row[6]) if row[6] is not None else None,
                trigger_source=row[7],
                input_params_json=row[8] if isinstance(row[8], dict) else {},
                output_metrics_json=row[9] if isinstance(row[9], dict) else {},
                error_message=row[10],
            )
            for row in rows
        ]