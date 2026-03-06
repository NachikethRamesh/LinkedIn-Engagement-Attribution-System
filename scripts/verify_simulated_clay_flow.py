from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.crm_sync.load_crm_csv import CRMSyncService
from app.db import get_connection
from app.writeback.ingest_enrichment import EnrichmentIngestionService
from app.writeback.run_writeback import WritebackService


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    crm = CRMSyncService()
    sync_summary = crm.run(
        accounts_file=str(ROOT / "data" / "crm_accounts_sample.csv"),
        contacts_file=str(ROOT / "data" / "crm_contacts_sample.csv"),
    )
    assert_true(sync_summary["accounts_rows_read"] > 0, "CRM accounts CSV not loaded")
    assert_true(sync_summary["contacts_rows_read"] > 0, "CRM contacts CSV not loaded")
    print("[PASS] crm_csv_sync")

    writeback = WritebackService()
    run = writeback.run(
        target_type="clay",
        trigger_source="manual",
        params={
            "selection_mode": "low_confidence_promising_accounts",
            "limit": 5,
            "simulate_local": True,
            "dry_run": False,
            "skip_if_previously_successful": False,
        },
    )
    assert_true(run["status"] in {"success", "partial_success"}, "simulated clay writeback failed")
    records = run.get("records", [])
    assert_true(len(records) > 0, "simulated clay run produced no records")

    generated = []
    for r in records:
        response_json = r.get("response_json") or {}
        assert_true(response_json.get("delivery_mode") in {"simulated_local", "stub", None}, "unexpected delivery mode")
        inbound = response_json.get("generated_inbound_result_file")
        outbound = response_json.get("outbound_file")
        if inbound:
            generated.append(inbound)
            assert_true(Path(inbound).exists(), f"missing generated inbound file {inbound}")
        if outbound:
            assert_true(Path(outbound).exists(), f"missing generated outbound file {outbound}")
    assert_true(len(generated) > 0, "no generated inbound simulated clay files found")
    print("[PASS] simulated_clay_artifacts")

    ingest = EnrichmentIngestionService()
    inserted_total = 0
    duplicate_total = 0
    for file_path in generated:
        payload = json.loads(Path(file_path).read_text(encoding="utf-8"))
        parsed = ingest.parse_payload(payload)
        first = ingest.ingest(parsed, trigger_source="verify_simulated_clay_flow")
        second = ingest.ingest(parsed, trigger_source="verify_simulated_clay_flow")
        inserted_total += first["inserted"]
        duplicate_total += second["skipped_duplicates"]

    assert_true(inserted_total > 0, "no enrichment results inserted from generated files")
    assert_true(duplicate_total > 0, "dedupe behavior not observed on repeated ingestion")
    print("[PASS] enrichment_ingest_and_dedupe")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM enrichment_results
                WHERE target_type = 'clay'
                """
            )
            clay_count = cur.fetchone()[0]
    assert_true(clay_count > 0, "expected clay enrichment rows missing")
    print("[PASS] clay_rows_present")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(f"simulated clay flow verification failed: {exc}")
