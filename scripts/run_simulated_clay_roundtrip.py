from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.writeback.ingest_enrichment import EnrichmentIngestionService
from app.writeback.run_writeback import WritebackService


def main() -> None:
    parser = argparse.ArgumentParser(description="Run simulated Clay round-trip: select -> outbound artifact -> inbound artifact -> ingest.")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--skip-ingest-generated", action="store_true", help="Only generate outbound/inbound artifacts; skip DB ingestion step.")
    parser.add_argument("--enable-replay-skip", action="store_true", help="Enable replay-skip against previously successful Clay writes.")
    args = parser.parse_args()

    writeback = WritebackService()
    run = writeback.run(
        target_type="clay",
        trigger_source="manual",
        params={
            "selection_mode": "low_confidence_promising_accounts",
            "limit": args.limit,
            "dry_run": False,
            "simulate_local": True,
            "skip_if_previously_successful": bool(args.enable_replay_skip),
        },
    )

    generated_files: list[str] = []
    for record in run.get("records", []):
        response_json = record.get("response_json") or {}
        inbound_file = response_json.get("generated_inbound_result_file")
        if isinstance(inbound_file, str):
            generated_files.append(inbound_file)

    ingest_summary = {"results_received": 0, "inserted": 0, "skipped_duplicates": 0}
    if (not args.skip_ingest_generated) and generated_files:
        service = EnrichmentIngestionService()
        for file_path in generated_files:
            payload = json.loads(Path(file_path).read_text(encoding="utf-8"))
            parsed = service.parse_payload(payload)
            summary = service.ingest(parsed, trigger_source="simulated_clay_roundtrip")
            ingest_summary["results_received"] += summary["results_received"]
            ingest_summary["inserted"] += summary["inserted"]
            ingest_summary["skipped_duplicates"] += summary["skipped_duplicates"]

    print(
        json.dumps(
            {
                "writeback_run_id": run["writeback_run_id"],
                "writeback_status": run["status"],
                "generated_inbound_result_files": generated_files,
                "ingest_summary": ingest_summary,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
