from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection
from app.writeback.ingest_enrichment import EnrichmentIngestionService
from app.writeback.payloads import build_payload
from app.writeback.run_writeback import WritebackService
from app.writeback.selector import WritebackSelector


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def verify_selector_quality(selector: WritebackSelector) -> None:
    high = selector.select("crm", {"selection_mode": "high_intent_accounts", "limit": 5})
    influenced = selector.select("crm", {"selection_mode": "socially_influenced_opportunities", "limit": 5})
    low_conf = selector.select("clay", {"selection_mode": "low_confidence_promising_accounts", "limit": 5})
    unresolved = selector.select(
        "exa",
        {
            "selection_mode": "unresolved_account_candidates",
            "limit": 5,
            "min_contributing_events": 2,
            "min_strong_signals": 0,
            "min_recent_signals": 0,
            "recent_days": 365,
        },
    )

    assert_true(len(unresolved) > 0, "unresolved_account_candidates returned zero rows")
    assert_true(all(s.selection_reason for s in high + influenced + low_conf + unresolved), "selector reason missing")

    payload_samples = []
    if high:
        payload_samples.append(build_payload(high[0]))
    if low_conf:
        payload_samples.append(build_payload(low_conf[0]))
    if influenced:
        payload_samples.append(build_payload(influenced[0]))
    if unresolved:
        payload_samples.append(build_payload(unresolved[0]))
    for payload in payload_samples:
        assert_true("payload_version" in payload or payload.get("target_type") == "webhook_generic", "payload_version missing")
        assert_true("source_system" in payload, "payload missing source_system")
    if unresolved:
        unresolved_payload = build_payload(unresolved[0])
        assert_true(
            unresolved_payload.get("entity_type") == "unresolved_account_candidate",
            "unresolved payload missing unresolved entity type",
        )
        assert_true(
            "candidate_company_name_raw" in unresolved_payload,
            "unresolved payload missing candidate_company_name_raw",
        )
    print("[PASS] selector_quality_and_payload_sanity")


def verify_dry_run(writeback: WritebackService) -> dict[str, Any]:
    dry = writeback.run(
        target_type="crm",
        trigger_source="manual",
        params={
            "selection_mode": "high_intent_accounts",
            "limit": 5,
            "dry_run": True,
            "skip_if_previously_successful": True,
        },
    )
    records = dry.get("records", [])
    assert_true(dry["result_metrics_json"].get("dry_run") is True, "dry_run metric not set")
    assert_true(all(r["status"] == "skipped" for r in records), "dry-run should only produce skipped records")
    assert_true(
        all((r.get("response_json") or {}).get("dry_run") is True for r in records),
        "dry-run records missing dry_run response marker",
    )
    print("[PASS] dry_run_safety")
    return dry


def verify_replay(writeback: WritebackService) -> dict[str, Any]:
    run_one = writeback.run(
        target_type="exa",
        trigger_source="manual",
        params={
            "selection_mode": "unresolved_account_candidates",
            "limit": 3,
            "dry_run": False,
            "skip_if_previously_successful": True,
            "min_contributing_events": 2,
            "min_strong_signals": 0,
            "min_recent_signals": 0,
            "recent_days": 365,
        },
    )
    run_two = writeback.run(
        target_type="exa",
        trigger_source="manual",
        params={
            "selection_mode": "unresolved_account_candidates",
            "limit": 3,
            "dry_run": False,
            "skip_if_previously_successful": True,
            "min_contributing_events": 2,
            "min_strong_signals": 0,
            "min_recent_signals": 0,
            "recent_days": 365,
        },
    )

    one_success = sum(1 for r in run_one.get("records", []) if r["status"] == "success")
    two_replay_skips = sum(
        1
        for r in run_two.get("records", [])
        if r["status"] == "skipped"
        and "prior successful writeback" in str(r.get("error_message") or "")
    )

    assert_true(one_success >= 0, "run one success count invalid")
    assert_true(two_replay_skips >= one_success, "replay protection did not skip previously successful entities")
    print("[PASS] replay_idempotency")
    return run_two


def verify_outbound_once(writeback: WritebackService) -> None:
    run_one = writeback.run(
        target_type="exa",
        trigger_source="manual",
        params={
            "selection_mode": "unresolved_account_candidates",
            "limit": 3,
            "dry_run": False,
            "skip_if_previously_successful": True,
            "min_contributing_events": 2,
            "min_strong_signals": 0,
            "min_recent_signals": 0,
            "recent_days": 365,
        },
    )
    statuses = {r["status"] for r in run_one.get("records", [])}
    assert_true(statuses.issubset({"success", "failed", "skipped"}), "unexpected record status in outbound run")
    print("[PASS] outbound_record_status_behavior")


def verify_failure_path(writeback: WritebackService) -> None:
    fail = writeback.run(
        target_type="exa",
        trigger_source="manual",
        params={
            "selection_mode": "unresolved_account_candidates",
            "limit": 3,
            "dry_run": False,
            "skip_if_previously_successful": False,
            "endpoint_url": "http://127.0.0.1:1/unreachable",
            "timeout_seconds": 1,
            "min_contributing_events": 2,
            "min_strong_signals": 0,
            "min_recent_signals": 0,
            "recent_days": 365,
        },
    )
    failed_records = [r for r in fail.get("records", []) if r["status"] == "failed"]
    assert_true(len(failed_records) >= 1 or fail["status"] in {"failed", "partial_success"}, "failure path not captured")
    print("[PASS] failure_path_logging")


def verify_enrichment_ingestion() -> None:
    enrichment = EnrichmentIngestionService()
    sample_file = ROOT / "data" / "sample_clay_enrichment_result.json"
    unresolved_file = ROOT / "data" / "sample_exa_unresolved_candidate_result.json"
    payload = json.loads(sample_file.read_text(encoding="utf-8"))
    unresolved_payload = json.loads(unresolved_file.read_text(encoding="utf-8"))
    parsed = enrichment.parse_payload(payload)
    parsed_unresolved = enrichment.parse_payload(unresolved_payload)

    first = enrichment.ingest(parsed, trigger_source="manual")
    second = enrichment.ingest(parsed, trigger_source="manual")
    unresolved_insert = enrichment.ingest(parsed_unresolved, trigger_source="manual")

    assert_true(first["results_received"] >= 1, "no enrichment rows received")
    assert_true(
        first["inserted"] + first["skipped_duplicates"] >= 1,
        "enrichment first ingestion should insert or dedupe-skip",
    )
    assert_true(second["skipped_duplicates"] >= 1, "enrichment dedupe not observed on second ingestion")
    assert_true(
        unresolved_insert["inserted"] + unresolved_insert["skipped_duplicates"] >= 1,
        "unresolved candidate enrichment row should insert or dedupe-skip",
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM enrichment_results;")
            total = cur.fetchone()[0]
            cur.execute(
                """
                SELECT COUNT(*)
                FROM account_enrichment_summary
                """
            )
            summarized = cur.fetchone()[0]
    assert_true(total >= 1, "enrichment_results unexpectedly empty")
    assert_true(summarized >= 1, "account_enrichment_summary unexpectedly empty")
    print("[PASS] enrichment_ingest_and_dedupe")


def verify_unresolved_exa_simulation(writeback: WritebackService) -> None:
    run = writeback.run(
        target_type="exa",
        trigger_source="manual",
        params={
            "selection_mode": "unresolved_account_candidates",
            "limit": 3,
            "dry_run": False,
            "simulate_local": True,
            "skip_if_previously_successful": False,
            "min_contributing_events": 2,
            "min_strong_signals": 1,
            "min_recent_signals": 1,
            "recent_days": 365,
        },
    )
    records = run.get("records", [])
    assert_true(len(records) > 0, "expected unresolved Exa simulation records")
    assert_true(
        any((r.get("response_json") or {}).get("delivery_mode") == "simulated_local" for r in records),
        "expected simulated_local response in unresolved Exa flow",
    )
    print("[PASS] unresolved_exa_simulation")


def print_snapshot(writeback: WritebackService) -> None:
    runs = writeback.list_runs(limit=5)
    print("snapshot:")
    for run in runs:
        metrics = run.get("result_metrics_json", {})
        print(
            f"- run_id={run['writeback_run_id']} target={run['target_type']} status={run['status']} "
            f"selected={metrics.get('selected_count')} success={metrics.get('success_count')} "
            f"failed={metrics.get('failed_count')} skipped={metrics.get('skipped_count')}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify writeback/enrichment behavior for Step 8.5 hardening.")
    parser.add_argument("--dry-run-only", action="store_true", help="Only execute selector + dry-run safety checks.")
    parser.add_argument("--simulate-replay", action="store_true", help="Explicitly run replay/idempotency checks.")
    parser.add_argument("--print-snapshot", action="store_true", help="Print compact run snapshot at the end.")
    args = parser.parse_args()

    selector = WritebackSelector()
    verify_selector_quality(selector)

    writeback = WritebackService()
    verify_dry_run(writeback)

    if not args.dry_run_only:
        if args.simulate_replay:
            verify_replay(writeback)
        else:
            verify_outbound_once(writeback)
        verify_failure_path(writeback)
        verify_unresolved_exa_simulation(writeback)
        verify_enrichment_ingestion()

    if args.print_snapshot:
        print_snapshot(writeback)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(f"writeback verification failed: {exc}")
