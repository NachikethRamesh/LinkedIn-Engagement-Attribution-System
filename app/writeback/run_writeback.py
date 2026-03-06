from __future__ import annotations

import argparse
import json
import traceback
import uuid
from datetime import UTC, datetime
from typing import Any

from psycopg2.extras import Json

from app.config import load_environment
from app.db import get_connection
from app.integrations_config import (
    get_writeback_auth_headers,
    get_writeback_endpoint,
    summarize_integration_requirements,
)
from app.writeback.adapters.clay import ClayWritebackAdapter
from app.writeback.adapters.crm import CRMWritebackAdapter
from app.writeback.adapters.exa import ExaWritebackAdapter
from app.writeback.adapters.webhook_generic import WebhookGenericWritebackAdapter
from app.writeback.payloads import build_payload
from app.writeback.selector import WritebackSelector
from app.writeback.types import DeliveryResult, SelectedEntity, TargetType

ADAPTERS = {
    "crm": CRMWritebackAdapter(),
    "clay": ClayWritebackAdapter(),
    "exa": ExaWritebackAdapter(),
    "webhook_generic": WebhookGenericWritebackAdapter(),
}


class WritebackService:
    def __init__(self) -> None:
        self.selector = WritebackSelector()

    def run(self, target_type: TargetType, params: dict[str, Any], trigger_source: str = "manual") -> dict[str, Any]:
        load_environment()
        # Simulation delivery mode is intentionally disabled.
        if bool(params.get("simulate_local", False)):
            raise ValueError("simulate_local is disabled. Configure real endpoint_url values instead.")
        writeback_run_id = str(uuid.uuid4())
        started_at = datetime.now(UTC)
        status = "success"
        error_message: str | None = None

        metrics: dict[str, Any] = {
            "selected_count": 0,
            "sent_count": 0,
            "success_count": 0,
            "failed_count": 0,
            "skipped_count": 0,
            "dry_run_record_count": 0,
            "replay_skipped_count": 0,
            "dry_run": bool(params.get("dry_run", False)),
            "target_type": target_type,
            "selection_mode": params.get("selection_mode"),
            "record_status_counts": {},
            "selection_preview": [],
        }

        with get_connection() as conn:
            with conn.cursor() as cur:
                self._create_run(
                    cur=cur,
                    writeback_run_id=writeback_run_id,
                    target_type=target_type,
                    trigger_source=trigger_source,
                    params=params,
                )
                conn.commit()

            try:
                selected = self.selector.select(target_type=target_type, params=params)
                metrics["selected_count"] = len(selected)
                metrics["selection_preview"] = [
                    {
                        "entity_type": s.entity_type,
                        "entity_id": s.entity_id,
                        "selection_bucket": s.selection_bucket,
                        "selection_reason": s.selection_reason,
                    }
                    for s in selected[:5]
                ]
                endpoint_url = get_writeback_endpoint(target_type=target_type, explicit_endpoint=params.get("endpoint_url"))
                auth_headers = get_writeback_auth_headers(target_type=target_type)
                metrics["integration_warnings"] = summarize_integration_requirements(
                    target_type=target_type,
                    endpoint_url=endpoint_url,
                    simulate_local=bool(params.get("simulate_local", False)),
                )

                with conn.cursor() as cur:
                    for item in selected:
                        payload = build_payload(item)
                        record_id = self._create_record(
                            cur=cur,
                            writeback_run_id=writeback_run_id,
                            item=item,
                            payload=payload,
                        )

                        if bool(params.get("dry_run", False)):
                            self._update_record(
                                cur=cur,
                                record_id=record_id,
                                status="skipped",
                                response_json={"dry_run": True, "payload_preview": payload, "replay_check_performed": False},
                                error_message=None,
                            )
                            metrics["skipped_count"] += 1
                            metrics["dry_run_record_count"] += 1
                            continue

                        if bool(params.get("skip_if_previously_successful", True)) and self._was_previously_successful(
                            cur=cur, target_type=target_type, entity_type=item.entity_type, entity_id=item.entity_id
                        ):
                            self._update_record(
                                cur=cur,
                                record_id=record_id,
                                status="skipped",
                                response_json={"replay_protection": True},
                                error_message="Skipped due to prior successful writeback for target/entity",
                            )
                            metrics["skipped_count"] += 1
                            metrics["replay_skipped_count"] += 1
                            continue

                        self._update_record(cur=cur, record_id=record_id, status="sent", response_json={"dispatched": True}, error_message=None)
                        metrics["sent_count"] += 1

                        delivery = self._deliver(
                            target_type=target_type,
                            payload=payload,
                            endpoint_url=endpoint_url,
                            timeout_seconds=int(params.get("timeout_seconds", 15)),
                            writeback_run_id=writeback_run_id,
                            simulate_local=bool(params.get("simulate_local", False)),
                            auth_headers=auth_headers,
                        )
                        self._update_record(
                            cur=cur,
                            record_id=record_id,
                            status=delivery.status,
                            response_json=delivery.response_json,
                            error_message=delivery.error_message,
                            external_key=delivery.external_key,
                        )

                        if delivery.status == "success":
                            metrics["success_count"] += 1
                        elif delivery.status == "skipped":
                            metrics["skipped_count"] += 1
                        else:
                            metrics["failed_count"] += 1

                    metrics["record_status_counts"] = self._status_counts(cur=cur, writeback_run_id=writeback_run_id)
                    status = self._derive_run_status(metrics)
                    self._complete_run(
                        cur=cur,
                        writeback_run_id=writeback_run_id,
                        status=status,
                        metrics=metrics,
                        error_message=None,
                        started_at=started_at,
                    )
                    conn.commit()
            except Exception as exc:
                error_message = f"{exc.__class__.__name__}: {exc}"
                metrics["traceback"] = traceback.format_exc()
                status = "failed"
                with conn.cursor() as cur:
                    self._complete_run(
                        cur=cur,
                        writeback_run_id=writeback_run_id,
                        status=status,
                        metrics=metrics,
                        error_message=error_message,
                        started_at=started_at,
                    )
                    conn.commit()

        record = self.get_run(writeback_run_id)
        if record is None:
            raise RuntimeError("Failed to load writeback run after completion")
        return record

    def list_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        writeback_run_id,
                        target_type,
                        status,
                        started_at,
                        completed_at,
                        duration_ms,
                        trigger_source,
                        selection_params_json,
                        result_metrics_json,
                        error_message
                    FROM writeback_runs
                    ORDER BY started_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()

        return [self._serialize_run_row(row) for row in rows]

    def get_run(self, writeback_run_id: str) -> dict[str, Any] | None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        writeback_run_id,
                        target_type,
                        status,
                        started_at,
                        completed_at,
                        duration_ms,
                        trigger_source,
                        selection_params_json,
                        result_metrics_json,
                        error_message
                    FROM writeback_runs
                    WHERE writeback_run_id = %s
                    """,
                    (writeback_run_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        payload = self._serialize_run_row(row)
        payload["records"] = self._list_records(writeback_run_id=writeback_run_id)
        return payload

    def _deliver(
        self,
        target_type: TargetType,
        payload: dict[str, Any],
        endpoint_url: str | None,
        timeout_seconds: int,
        writeback_run_id: str,
        simulate_local: bool,
        auth_headers: dict[str, str] | None,
    ) -> DeliveryResult:
        adapter = ADAPTERS[target_type]
        return adapter.deliver(
            payload=payload,
            endpoint_url=endpoint_url,
            timeout_seconds=timeout_seconds,
            writeback_run_id=writeback_run_id,
            simulate_local=simulate_local,
            auth_headers=auth_headers,
        )

    def _create_run(self, cur, writeback_run_id: str, target_type: TargetType, trigger_source: str, params: dict[str, Any]) -> None:
        cur.execute(
            """
            INSERT INTO writeback_runs (
                writeback_run_id,
                target_type,
                status,
                started_at,
                trigger_source,
                selection_params_json,
                result_metrics_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (writeback_run_id, target_type, "running", datetime.now(UTC), trigger_source, Json(params), Json({})),
        )

    def _complete_run(
        self,
        cur,
        writeback_run_id: str,
        status: str,
        metrics: dict[str, Any],
        error_message: str | None,
        started_at: datetime,
    ) -> None:
        completed_at = datetime.now(UTC)
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)
        cur.execute(
            """
            UPDATE writeback_runs
            SET
                status = %s,
                completed_at = %s,
                duration_ms = %s,
                result_metrics_json = %s,
                error_message = %s
            WHERE writeback_run_id = %s
            """,
            (status, completed_at, duration_ms, Json(metrics), error_message, writeback_run_id),
        )

    def _create_record(self, cur, writeback_run_id: str, item: SelectedEntity, payload: dict[str, Any]) -> int:
        cur.execute(
            """
            INSERT INTO writeback_records (
                writeback_run_id,
                entity_type,
                entity_id,
                target_type,
                payload_json,
                status,
                response_json,
                error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (writeback_run_id, item.entity_type, item.entity_id, item.target_type, Json(payload), "pending", Json({}), None),
        )
        return int(cur.fetchone()[0])

    def _update_record(
        self,
        cur,
        record_id: int,
        status: str,
        response_json: dict[str, Any] | None,
        error_message: str | None,
        external_key: str | None = None,
    ) -> None:
        cur.execute(
            """
            UPDATE writeback_records
            SET
                status = %s,
                response_json = %s,
                error_message = %s,
                external_key = COALESCE(%s, external_key),
                updated_at = %s
            WHERE id = %s
            """,
            (status, Json(response_json or {}), error_message, external_key, datetime.now(UTC), record_id),
        )

    def _status_counts(self, cur, writeback_run_id: str) -> dict[str, int]:
        cur.execute(
            """
            SELECT status, COUNT(*)
            FROM writeback_records
            WHERE writeback_run_id = %s
            GROUP BY status
            """,
            (writeback_run_id,),
        )
        return {row[0]: int(row[1]) for row in cur.fetchall()}

    def _was_previously_successful(self, cur, target_type: str, entity_type: str, entity_id: int) -> bool:
        cur.execute(
            """
            SELECT 1
            FROM writeback_records
            WHERE target_type = %s
              AND entity_type = %s
              AND entity_id = %s
              AND status = 'success'
            LIMIT 1
            """,
            (target_type, entity_type, entity_id),
        )
        return cur.fetchone() is not None

    def _derive_run_status(self, metrics: dict[str, Any]) -> str:
        failed = int(metrics.get("failed_count", 0))
        success = int(metrics.get("success_count", 0))
        skipped = int(metrics.get("skipped_count", 0))
        selected = int(metrics.get("selected_count", 0))
        dry_run = bool(metrics.get("dry_run", False))

        if failed > 0 and success == 0:
            return "failed"
        if failed > 0 and success > 0:
            return "partial_success"
        if selected == 0:
            return "success"
        if dry_run and failed == 0:
            return "success"
        if failed == 0 and success == 0 and skipped > 0:
            return "partial_success"
        if success > 0 and skipped > 0:
            return "partial_success"
        return "success"

    def _serialize_run_row(self, row) -> dict[str, Any]:
        return {
            "writeback_run_id": row[0],
            "target_type": row[1],
            "status": row[2],
            "started_at": row[3].isoformat() if row[3] else None,
            "completed_at": row[4].isoformat() if row[4] else None,
            "duration_ms": int(row[5]) if row[5] is not None else None,
            "trigger_source": row[6],
            "selection_params_json": row[7] if isinstance(row[7], dict) else {},
            "result_metrics_json": row[8] if isinstance(row[8], dict) else {},
            "error_message": row[9],
        }

    def _list_records(self, writeback_run_id: str) -> list[dict[str, Any]]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        entity_type,
                        entity_id,
                        target_type,
                        external_key,
                        status,
                        response_json,
                        error_message,
                        created_at,
                        updated_at
                    FROM writeback_records
                    WHERE writeback_run_id = %s
                    ORDER BY id
                    """,
                    (writeback_run_id,),
                )
                rows = cur.fetchall()

        payload = []
        for row in rows:
            payload.append(
                {
                    "id": int(row[0]),
                    "entity_type": row[1],
                    "entity_id": int(row[2]),
                    "target_type": row[3],
                    "external_key": row[4],
                    "status": row[5],
                    "response_json": row[6] if isinstance(row[6], dict) else {},
                    "error_message": row[7],
                    "created_at": row[8].isoformat() if row[8] else None,
                    "updated_at": row[9].isoformat() if row[9] else None,
                }
            )
        return payload


def parse_target_type(value: str) -> TargetType:
    normalized = value.strip().lower()
    allowed = {"crm", "clay", "exa", "webhook_generic"}
    if normalized not in allowed:
        raise ValueError(f"target_type must be one of {sorted(allowed)}")
    return normalized  # type: ignore[return-value]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic writeback handoff for CRM/Clay/Exa/webhook targets.")
    parser.add_argument("--target-type", required=True, choices=["crm", "clay", "exa", "webhook_generic"])
    parser.add_argument(
        "--selection-mode",
        choices=[
            "high_intent_accounts",
            "socially_influenced_opportunities",
            "low_confidence_promising_accounts",
            "unresolved_account_candidates",
        ],
        help="Deterministic selection mode. Defaults based on target type.",
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--min-intent-score", type=float)
    parser.add_argument("--min-intent-confidence", type=float)
    parser.add_argument("--max-intent-confidence", type=float)
    parser.add_argument("--score-window", choices=["rolling_7d", "rolling_14d", "rolling_30d"])
    parser.add_argument("--min-influence-band", choices=["none", "weak", "medium", "strong"])
    parser.add_argument("--min-influence-score", type=float)
    parser.add_argument("--min-contributing-events", type=int)
    parser.add_argument("--min-strong-signals", type=int)
    parser.add_argument("--min-recent-signals", type=int)
    parser.add_argument("--recent-days", type=int)
    parser.add_argument("--weak-match-confidence-threshold", type=float)
    parser.add_argument("--include-generic-candidates", action="store_true")
    parser.add_argument("--endpoint-url", help="Optional explicit endpoint URL override.")
    parser.add_argument("--timeout-seconds", type=int, default=15)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--simulate-local",
        action="store_true",
        help="Deprecated/disabled: local simulation delivery mode is blocked.",
    )
    parser.add_argument("--no-replay-skip", action="store_true", help="Disable skip_if_previously_successful.")
    parser.add_argument("--trigger-source", default="manual")
    args = parser.parse_args()

    params: dict[str, Any] = {
        "selection_mode": args.selection_mode,
        "limit": args.limit,
        "endpoint_url": args.endpoint_url,
        "timeout_seconds": args.timeout_seconds,
        "dry_run": args.dry_run,
        "simulate_local": args.simulate_local,
        "skip_if_previously_successful": not args.no_replay_skip,
    }
    optional = {
        "min_intent_score": args.min_intent_score,
        "min_intent_confidence": args.min_intent_confidence,
        "max_intent_confidence": args.max_intent_confidence,
        "score_window": args.score_window,
        "min_influence_band": args.min_influence_band,
        "min_influence_score": args.min_influence_score,
        "min_contributing_events": args.min_contributing_events,
        "min_strong_signals": args.min_strong_signals,
        "min_recent_signals": args.min_recent_signals,
        "recent_days": args.recent_days,
        "weak_match_confidence_threshold": args.weak_match_confidence_threshold,
        "include_generic_candidates": args.include_generic_candidates,
    }
    for key, value in optional.items():
        if value is not None:
            params[key] = value

    service = WritebackService()
    result = service.run(target_type=parse_target_type(args.target_type), params=params, trigger_source=args.trigger_source)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
