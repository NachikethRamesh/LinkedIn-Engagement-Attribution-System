from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection
from app.linkedin_ingestion.org_post_resolver import resolve_org_post_identifier
from app.linkedin_ingestion.url_ingestion import OrganizationPostURLIngestionService
from app.orchestration.job_runner import JobRunner


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str


def check_url_normalization() -> CheckResult:
    variants = [
        "https://www.linkedin.com/posts/<REDACTED_POST>?trk=foo",
        "https://www.linkedin.com/posts/<REDACTED_POST>/",
        "https://m.linkedin.com/posts/placeholder-company_demo-post-12345/?utm_source=test",
        "https://linkedin.com/posts/placeholder-company_demo-post-12345#fragment",
    ]
    normalized = []
    for url in variants:
        resolved = resolve_org_post_identifier(url, simulation_mode=True)
        normalized.append(resolved.normalized_url)

    unique = sorted(set(normalized))
    return CheckResult(
        name="url_normalization",
        passed=len(unique) == 1,
        details=f"normalized_values={unique}",
    )


def check_invalid_url_rejected() -> CheckResult:
    try:
        resolve_org_post_identifier("https://example.com/not-linkedin", simulation_mode=False)
    except ValueError as exc:
        pass
    else:
        return CheckResult("invalid_url_rejected", False, "expected ValueError for non-LinkedIn URL")

    try:
        resolve_org_post_identifier("https://www.linkedin.com/company/placeholder-company", simulation_mode=True)
    except ValueError as exc:
        return CheckResult("invalid_url_rejected", True, str(exc))
    return CheckResult("invalid_url_rejected", False, "expected ValueError for unsupported LinkedIn URL path")


def _event_count_for_org_import() -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events
                WHERE metadata_json->>'source_name' = 'linkedin_org_api'
                  AND metadata_json->>'import_mode' = 'url_ingestion'
                """
            )
            return int(cur.fetchone()[0])


def check_ingestion_db(post_url: str) -> list[CheckResult]:
    checks: list[CheckResult] = []
    service = OrganizationPostURLIngestionService()

    before = _event_count_for_org_import()
    _ = service.ingest(post_url=post_url, simulation_mode=True)
    after_first = _event_count_for_org_import()
    _ = service.ingest(post_url=post_url, simulation_mode=True)
    after_second = _event_count_for_org_import()

    checks.append(
        CheckResult(
            "dedupe_same_url_reimport",
            after_second == after_first,
            f"before={before}, after_first={after_first}, after_second={after_second}",
        )
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events
                WHERE metadata_json->>'source_name' = 'linkedin_org_api'
                  AND metadata_json->>'import_mode' = 'url_ingestion'
                  AND COALESCE((metadata_json->>'aggregated_import')::boolean, false) = true
                """
            )
            aggregated = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events
                WHERE metadata_json->>'source_name' = 'linkedin_org_api'
                  AND metadata_json->>'import_mode' = 'url_ingestion'
                  AND COALESCE((metadata_json->>'aggregated_import')::boolean, false) = true
                  AND actor_name IS NOT NULL
                """
            )
            aggregated_with_actor = int(cur.fetchone()[0])

            checks.append(
                CheckResult(
                    "aggregate_actor_safety",
                    aggregated_with_actor == 0,
                    f"aggregated_events={aggregated}, aggregated_with_actor_name={aggregated_with_actor}",
                )
            )

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events
                WHERE metadata_json->>'source_name' = 'linkedin_org_api'
                  AND metadata_json->>'import_mode' = 'url_ingestion'
                  AND metadata_json ? 'original_url'
                  AND metadata_json ? 'normalized_url'
                  AND metadata_json ? 'resolution_mode'
                  AND metadata_json ? 'resolved_org_post_identifier'
                """
            )
            provenance_count = int(cur.fetchone()[0])

            checks.append(
                CheckResult(
                    "provenance_fields_present",
                    provenance_count == after_second,
                    f"events={after_second}, with_provenance={provenance_count}",
                )
            )

            cur.execute(
                """
                SELECT COUNT(*)
                FROM imports_log
                WHERE source_name = 'linkedin_org_api'
                  AND import_mode = 'url_ingestion'
                """
            )
            imports_rows = int(cur.fetchone()[0])
            checks.append(
                CheckResult(
                    "imports_log_written",
                    imports_rows > 0,
                    f"imports_log_rows={imports_rows}",
                )
            )

    return checks


def check_orchestration(post_url: str, run_pipeline: bool) -> CheckResult:
    runner = JobRunner()
    record = runner.run_job(
        "linkedin_ingestion_org_url",
        params={
            "post_url": post_url,
            "simulation_mode": True,
            "run_pipeline": run_pipeline,
            "rebuild_downstream": False,
            "window_days": 30,
        },
        trigger_source="manual",
    )
    metrics = record.output_metrics_json
    if record.status != "success":
        return CheckResult("orchestration_job", False, f"status={record.status}, error={record.error_message}")

    has_core = all(
        key in metrics
        for key in (
            "normalized_url",
            "resolved_org_post_identifier",
            "rows_read",
            "events_inserted",
        )
    )
    if not has_core:
        return CheckResult("orchestration_job", False, f"missing expected metrics keys: {metrics.keys()}")

    if run_pipeline:
        downstream = metrics.get("downstream", {})
        stage_order = downstream.get("stage_order")
        expected = ["identity_resolution", "intent_scoring", "opportunity_attribution"]
        return CheckResult(
            "orchestration_job",
            stage_order == expected,
            f"stage_order={stage_order}",
        )

    return CheckResult("orchestration_job", True, "run_pipeline=false path succeeded")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify org-post URL ingestion behavior.")
    parser.add_argument(
        "--post-url",
        default="https://www.linkedin.com/posts/<REDACTED_POST>?trk=public_post",
    )
    parser.add_argument("--run-db", action="store_true", help="Run DB-backed checks.")
    parser.add_argument("--run-pipeline", action="store_true", help="Run orchestration check with downstream stages.")
    args = parser.parse_args()

    checks: list[CheckResult] = [check_url_normalization(), check_invalid_url_rejected()]
    if args.run_db:
        checks.extend(check_ingestion_db(args.post_url))
        checks.append(check_orchestration(args.post_url, run_pipeline=args.run_pipeline))

    failed = [check for check in checks if not check.passed]
    for check in checks:
        status = "PASS" if check.passed else "FAIL"
        print(f"[{status}] {check.name}: {check.details}")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
