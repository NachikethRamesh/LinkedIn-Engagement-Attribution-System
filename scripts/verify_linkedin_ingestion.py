from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection
from app.linkedin_ingestion.base import LinkedInIngestionService
from app.linkedin_ingestion.csv_adapter import CSVLinkedInAdapter
from app.linkedin_ingestion.validator import normalize_linkedin_post_url


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str


def check_url_normalization() -> CheckResult:
    variants = [
        "https://www.linkedin.com/posts/<REDACTED_POST>",
        "https://www.linkedin.com/posts/<REDACTED_POST>/",
        "https://www.linkedin.com/posts/<REDACTED_POST>?trk=public_post_share-update_update-text",
        "https://m.linkedin.com/posts/placeholder-company_abc123/?utm_source=test",
        "https://linkedin.com/posts/placeholder-company_abc123#fragment",
    ]
    normalized = {normalize_linkedin_post_url(url) for url in variants}

    if len(normalized) == 1 and None not in normalized:
        canonical = next(iter(normalized))
        return CheckResult("url_normalization", True, f"canonical={canonical}")
    return CheckResult("url_normalization", False, f"normalized_variants={sorted(str(value) for value in normalized)}")


def _count_shield_events() -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events
                WHERE metadata_json->>'source_name' = 'shield'
                  AND metadata_json->>'import_mode' = 'csv'
                """
            )
            return int(cur.fetchone()[0])


def check_dedupe_and_metadata(shield_file: str) -> list[CheckResult]:
    results: list[CheckResult] = []

    service = LinkedInIngestionService()
    before = _count_shield_events()

    batch = CSVLinkedInAdapter(file_path=shield_file, source_name="shield").collect()
    service.ingest_batch(batch=batch, source_name="shield", filename=shield_file, import_mode="csv")
    after_first = _count_shield_events()

    batch = CSVLinkedInAdapter(file_path=shield_file, source_name="shield").collect()
    service.ingest_batch(batch=batch, source_name="shield", filename=shield_file, import_mode="csv")
    after_second = _count_shield_events()

    second_delta = after_second - after_first
    results.append(
        CheckResult(
            "dedupe_same_file_reimport",
            second_delta == 0,
            f"before={before}, after_first={after_first}, after_second={after_second}, second_delta={second_delta}",
        )
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events
                WHERE metadata_json->>'source_name' = 'shield'
                  AND metadata_json->>'import_mode' = 'csv'
                  AND COALESCE((metadata_json->>'aggregated_import')::boolean, false) = true
                """
            )
            aggregated_count = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events
                WHERE metadata_json->>'source_name' = 'shield'
                  AND metadata_json->>'import_mode' = 'csv'
                  AND COALESCE((metadata_json->>'aggregated_import')::boolean, false) = true
                  AND actor_name IS NOT NULL
                """
            )
            aggregated_with_actor = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events
                WHERE metadata_json->>'source_name' = 'shield'
                  AND metadata_json->>'import_mode' = 'csv'
                  AND COALESCE((metadata_json->>'aggregated_import')::boolean, false) = true
                  AND metadata_json ? 'source_metric_count'
                  AND metadata_json ? 'original_columns'
                  AND metadata_json ? 'actor_origin'
                """
            )
            aggregated_with_required_metadata = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_events
                WHERE metadata_json->>'source_name' = 'shield'
                  AND metadata_json->>'import_mode' = 'csv'
                  AND COALESCE((metadata_json->>'aggregated_import')::boolean, false) = true
                  AND metadata_json->>'actor_origin' = 'aggregate_unknown'
                """
            )
            aggregated_with_origin = int(cur.fetchone()[0])

            results.append(
                CheckResult(
                    "aggregate_actor_absent",
                    aggregated_with_actor == 0,
                    f"aggregated_count={aggregated_count}, aggregated_with_actor={aggregated_with_actor}",
                )
            )
            results.append(
                CheckResult(
                    "aggregate_required_metadata",
                    aggregated_with_required_metadata == aggregated_count,
                    f"aggregated_count={aggregated_count}, with_required_metadata={aggregated_with_required_metadata}",
                )
            )
            results.append(
                CheckResult(
                    "aggregate_actor_origin",
                    aggregated_with_origin == aggregated_count,
                    f"aggregated_count={aggregated_count}, with_actor_origin=aggregate_unknown:{aggregated_with_origin}",
                )
            )

            cur.execute(
                """
                SELECT source_name, filename, import_mode, imported_at, row_count, success_count, skip_count, warning_count, notes
                FROM imports_log
                WHERE source_name = 'shield'
                ORDER BY imported_at DESC
                LIMIT 1
                """
            )
            latest_log = cur.fetchone()

            complete = latest_log is not None and all(value is not None for value in latest_log)
            results.append(
                CheckResult(
                    "imports_log_completeness",
                    complete,
                    f"latest_log={latest_log}",
                )
            )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify LinkedIn ingestion hardening checks.")
    parser.add_argument("--run-db", action="store_true", help="Run DB-backed verification checks.")
    parser.add_argument("--shield-file", default="data/shield_sample.csv", help="Shield CSV path for reimport checks.")
    args = parser.parse_args()

    checks: list[CheckResult] = [check_url_normalization()]

    if args.run_db:
        checks.extend(check_dedupe_and_metadata(args.shield_file))

    failed = [check for check in checks if not check.passed]
    for check in checks:
        status = "PASS" if check.passed else "FAIL"
        print(f"[{status}] {check.name}: {check.details}")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
