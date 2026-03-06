import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT writeback_run_id, target_type, status, started_at, duration_ms, result_metrics_json, error_message
                FROM writeback_runs
                ORDER BY started_at DESC
                LIMIT 12
                """
            )
            recent_runs = cur.fetchall()

            cur.execute(
                """
                SELECT target_type, status, COUNT(*)
                FROM writeback_runs
                GROUP BY target_type, status
                ORDER BY target_type, status
                """
            )
            by_target_status = cur.fetchall()

            cur.execute(
                """
                SELECT writeback_run_id, target_type, started_at, error_message
                FROM writeback_runs
                WHERE status IN ('failed', 'partial_success')
                ORDER BY started_at DESC
                LIMIT 8
                """
            )
            recent_failures = cur.fetchall()

            cur.execute(
                """
                SELECT target_type, COUNT(*)
                FROM enrichment_results
                GROUP BY target_type
                ORDER BY COUNT(*) DESC, target_type
                """
            )
            enrichment_counts = cur.fetchall()

            cur.execute(
                """
                SELECT
                    writeback_run_id,
                    status,
                    started_at,
                    result_metrics_json,
                    error_message
                FROM writeback_runs
                WHERE target_type = 'exa'
                  AND selection_params_json->>'selection_mode' = 'unresolved_account_candidates'
                ORDER BY started_at DESC
                LIMIT 8
                """
            )
            unresolved_exa_runs = cur.fetchall()

            cur.execute(
                """
                SELECT
                    wr.writeback_run_id,
                    wr.entity_id,
                    wr.status,
                    wr.payload_json,
                    wr.error_message,
                    wr.created_at
                FROM writeback_records wr
                JOIN writeback_runs r ON r.writeback_run_id = wr.writeback_run_id
                WHERE wr.target_type = 'exa'
                  AND wr.entity_type = 'unresolved_account_candidate'
                  AND r.selection_params_json->>'selection_mode' = 'unresolved_account_candidates'
                ORDER BY wr.created_at DESC
                LIMIT 10
                """
            )
            unresolved_payloads = cur.fetchall()

            cur.execute(
                """
                SELECT
                    entity_id,
                    enrichment_type,
                    source_run_id,
                    received_at,
                    normalized_data_json
                FROM enrichment_results
                WHERE target_type = 'exa'
                  AND entity_type = 'unresolved_account_candidate'
                ORDER BY received_at DESC, id DESC
                LIMIT 10
                """
            )
            unresolved_enrichment = cur.fetchall()

            cur.execute(
                """
                SELECT wr.target_type, wr.entity_type, wr.entity_id, wr.payload_json
                FROM writeback_records wr
                JOIN writeback_runs r ON r.writeback_run_id = wr.writeback_run_id
                WHERE wr.status IN ('success', 'skipped')
                ORDER BY wr.created_at DESC
                LIMIT 12
                """
            )
            payload_samples = cur.fetchall()

    print("recent writeback runs:")
    for run_id, target, status, started_at, duration_ms, metrics, error in recent_runs:
        metrics = metrics if isinstance(metrics, dict) else {}
        print(
            f"- run_id={run_id} target={target} status={status} "
            f"started={started_at.isoformat()} duration_ms={duration_ms} "
            f"selected={metrics.get('selected_count')} success={metrics.get('success_count')} "
            f"failed={metrics.get('failed_count')} skipped={metrics.get('skipped_count')}"
        )
        if error:
            print(f"  error={error}")

    print("\ncounts by target/status:")
    for target, status, count in by_target_status:
        print(f"- {target} | {status}: {count}")

    print("\nrecent dry-runs:")
    for run_id, target, status, started_at, duration_ms, metrics, _ in recent_runs:
        metrics = metrics if isinstance(metrics, dict) else {}
        if metrics.get("dry_run"):
            print(
                f"- run_id={run_id} target={target} status={status} "
                f"started={started_at.isoformat()} duration_ms={duration_ms} "
                f"dry_run_records={metrics.get('dry_run_record_count')}"
            )

    print("\nrecent failures/partials:")
    if not recent_failures:
        print("- none")
    for run_id, target, started_at, error in recent_failures:
        print(f"- run_id={run_id} target={target} started={started_at.isoformat()} error={error}")

    print("\nenrichment result counts by target_type:")
    for target, count in enrichment_counts:
        print(f"- {target}: {count}")

    print("\nunresolved-candidate exa runs:")
    if not unresolved_exa_runs:
        print("- none")
    for run_id, status, started_at, metrics, error in unresolved_exa_runs:
        metrics = metrics if isinstance(metrics, dict) else {}
        print(
            f"- run_id={run_id} status={status} started={started_at.isoformat()} "
            f"selected={metrics.get('selected_count')} success={metrics.get('success_count')} "
            f"failed={metrics.get('failed_count')} skipped={metrics.get('skipped_count')}"
        )
        if error:
            print(f"  error={error}")

    print("\nrecent unresolved-candidate payload summaries (exa):")
    if not unresolved_payloads:
        print("- none")
    for run_id, entity_id, status, payload, error, created_at in unresolved_payloads[:6]:
        payload = payload if isinstance(payload, dict) else {}
        print(
            f"- run_id={run_id} candidate_id={entity_id} status={status} "
            f"created={created_at.isoformat()} normalized={payload.get('candidate_company_name_normalized')} "
            f"strongest={payload.get('strongest_signal_type')} recent={payload.get('recent_signal_count')}"
        )
        if error:
            print(f"  error={error}")

    print("\nrecent unresolved-candidate enrichment ingests (exa):")
    if not unresolved_enrichment:
        print("- none")
    for entity_id, enrichment_type, source_run_id, received_at, normalized in unresolved_enrichment[:6]:
        normalized = normalized if isinstance(normalized, dict) else {}
        print(
            f"- candidate_id={entity_id} received={received_at.isoformat()} type={enrichment_type} "
            f"source_run_id={source_run_id} likely_company={normalized.get('likely_company_name')} "
            f"likely_domain={normalized.get('likely_domain')}"
        )

    print("\nsample payload summaries by target_type:")
    grouped: dict[str, list[dict]] = {}
    for target, entity_type, entity_id, payload in payload_samples:
        grouped.setdefault(target, [])
        if len(grouped[target]) >= 2:
            continue
        grouped[target].append(
            {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "keys": sorted(list((payload or {}).keys()))[:8],
            }
        )
    for target, samples in grouped.items():
        print(f"- {target}: {json.dumps(samples)}")


if __name__ == "__main__":
    main()
