from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.orchestration.job_runner import JobRunner


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _validate_run_shape(payload: dict[str, Any]) -> None:
    required = [
        "run_id",
        "job_name",
        "status",
        "started_at",
        "completed_at",
        "duration_ms",
        "trigger_source",
        "input_params_json",
        "output_metrics_json",
        "error_message",
    ]
    for key in required:
        assert_true(key in payload, f"missing key '{key}' in run payload")


def verify_stage_jobs(runner: JobRunner) -> None:
    jobs = [
        ("linkedin_ingestion_mock", {"posts": 3, "events": 20}),
        ("identity_resolution", {"rebuild": True}),
        ("intent_scoring", {"rebuild": True}),
        ("opportunity_attribution", {"rebuild": True, "window_days": 30}),
    ]

    for job_name, payload in jobs:
        record = runner.run_job(job_name=job_name, params=payload, trigger_source="manual")
        assert_true(record.status == "success", f"{job_name} failed")
        assert_true(bool(record.output_metrics_json), f"{job_name} missing output metrics")
        assert_true(record.started_at is not None, f"{job_name} missing started_at")
        assert_true(record.completed_at is not None, f"{job_name} missing completed_at")
        assert_true(record.duration_ms is not None and record.duration_ms >= 0, f"{job_name} invalid duration_ms")


def verify_full_pipeline(runner: JobRunner) -> str:
    record = runner.run_job(
        job_name="full_pipeline",
        params={"source": "mock", "posts": 5, "events": 40, "rebuild": True, "window_days": 30},
        trigger_source="manual",
    )
    assert_true(record.status == "success", "full_pipeline failed")
    assert_true(record.started_at is not None, "full_pipeline missing started_at")
    assert_true(record.completed_at is not None, "full_pipeline missing completed_at")
    assert_true(record.duration_ms is not None and record.duration_ms >= 0, "full_pipeline invalid duration")

    stages = record.output_metrics_json.get("stages", {})
    expected = {"linkedin_ingestion_mock", "identity_resolution", "intent_scoring", "opportunity_attribution"}
    assert_true(expected.issubset(stages.keys()), "full_pipeline missing stage metrics")
    assert_true(
        record.output_metrics_json.get("stage_order")
        == ["linkedin_ingestion_mock", "identity_resolution", "intent_scoring", "opportunity_attribution"],
        "full_pipeline stage order missing or incorrect",
    )
    return record.run_id


def verify_failure_logging(runner: JobRunner, simulate_failure: bool) -> None:
    if not simulate_failure:
        return

    record = runner.run_job(
        job_name="linkedin_ingestion_csv",
        params={"source": "shield"},
        trigger_source="manual",
    )
    assert_true(record.status == "failed", "expected failure run to be marked failed")
    assert_true(record.error_message is not None and len(record.error_message) > 0, "failed run missing error_message")
    assert_true(record.completed_at is not None, "failed run missing completed_at")
    assert_true(record.duration_ms is not None and record.duration_ms >= 0, "failed run invalid duration_ms")
    assert_true(
        isinstance(record.output_metrics_json, dict) and "error_type" in record.output_metrics_json,
        "failed run output_metrics_json missing error_type",
    )


def verify_runs_table_surface(runner: JobRunner, latest_run_id: str) -> None:
    runs = runner.list_runs(limit=20)
    assert_true(len(runs) > 0, "expected pipeline_runs records")
    sample = runs[0]
    assert_true(sample.run_id is not None, "run_id missing")
    assert_true(sample.job_name is not None, "job_name missing")
    assert_true(sample.status in {"running", "success", "failed", "queued"}, "invalid run status")
    assert_true(any(r.run_id == latest_run_id for r in runs), "latest full pipeline run not found in list-runs")


def verify_list_runs_cli(latest_run_id: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "app.orchestration.pipeline", "list-runs", "--limit", "20"],
        capture_output=True,
        text=True,
        check=True,
    )
    stdout = result.stdout.strip()
    assert_true(len(stdout) > 0, "list-runs CLI produced empty output")
    assert_true(latest_run_id in stdout, "list-runs CLI output did not include latest run_id")
    assert_true("status=" in stdout and "duration_ms=" in stdout, "list-runs CLI output missing status/duration fields")


def verify_api_health(api_base_url: str) -> None:
    url = api_base_url.rstrip("/") + "/health"
    request = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(request, timeout=5) as response:
        body = response.read().decode("utf-8")
        parsed = json.loads(body)
        assert_true(parsed.get("status") == "ok", "health endpoint did not return status=ok")
        assert_true(parsed.get("db") == "ok", "health endpoint did not report db=ok")


def verify_api_job_endpoints(api_base_url: str) -> None:
    base = api_base_url.rstrip("/")

    payload = json.dumps({"source": "mock", "posts": 4, "events": 20, "rebuild": False, "window_days": 30}).encode("utf-8")
    request = urllib.request.Request(
        url=base + "/jobs/full-pipeline",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        run_payload = json.loads(response.read().decode("utf-8"))
        _validate_run_shape(run_payload)
        run_id = run_payload["run_id"]
        assert_true(run_payload["status"] in {"success", "failed"}, "unexpected status from POST full-pipeline")

    get_one = urllib.request.Request(url=base + f"/jobs/{run_id}", method="GET")
    with urllib.request.urlopen(get_one, timeout=15) as response:
        one_payload = json.loads(response.read().decode("utf-8"))
        _validate_run_shape(one_payload)
        assert_true(one_payload["run_id"] == run_id, "GET /jobs/{run_id} returned mismatched run_id")

    get_many = urllib.request.Request(url=base + "/jobs?limit=10", method="GET")
    with urllib.request.urlopen(get_many, timeout=15) as response:
        many_payload = json.loads(response.read().decode("utf-8"))
        assert_true(isinstance(many_payload, list), "GET /jobs did not return list payload")
        assert_true(len(many_payload) > 0, "GET /jobs returned empty list unexpectedly")
        _validate_run_shape(many_payload[0])


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify orchestration runner and optional API health endpoint.")
    parser.add_argument("--api-base-url", help="Optional API base URL for health check (e.g., http://127.0.0.1:8000)")
    parser.add_argument("--simulate-failure", action="store_true", help="Execute a controlled failing run for audit checks.")
    parser.add_argument("--print-snapshot", action="store_true", help="Print latest runs snapshot after checks.")
    args = parser.parse_args()

    runner = JobRunner()

    verify_stage_jobs(runner)
    print("[PASS] stage_jobs")

    latest_run_id = verify_full_pipeline(runner)
    print("[PASS] full_pipeline")

    verify_failure_logging(runner, simulate_failure=args.simulate_failure)
    if args.simulate_failure:
        print("[PASS] failure_logging")

    verify_runs_table_surface(runner, latest_run_id=latest_run_id)
    print("[PASS] runs_table_surface")
    verify_list_runs_cli(latest_run_id=latest_run_id)
    print("[PASS] list_runs_cli")

    if args.api_base_url:
        try:
            verify_api_health(args.api_base_url)
            print("[PASS] api_health")
            verify_api_job_endpoints(args.api_base_url)
            print("[PASS] api_job_polling")
        except (urllib.error.URLError, TimeoutError, AssertionError) as exc:
            raise SystemExit(f"API health verification failed: {exc}")

    if args.print_snapshot:
        runs = runner.list_runs(limit=5)
        print("snapshot:")
        for run in runs:
            print(
                f"- run_id={run.run_id} job={run.job_name} status={run.status} "
                f"started_at={run.started_at.isoformat()} duration_ms={run.duration_ms}"
            )


if __name__ == "__main__":
    main()
