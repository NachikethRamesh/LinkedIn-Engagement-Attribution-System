from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from psycopg2.extras import Json

from app.db import get_connection
from app.writeback.types import EnrichmentResultInput

ALLOWED_TARGETS = {"crm", "clay", "exa", "webhook_generic"}
ALLOWED_ENTITY_TYPES = {"account", "opportunity", "contact", "unresolved_account_candidate"}


class EnrichmentIngestionService:
    def ingest(
        self,
        results: list[EnrichmentResultInput],
        trigger_source: str = "manual",
    ) -> dict[str, Any]:
        inserted = 0
        skipped = 0

        with get_connection() as conn:
            with conn.cursor() as cur:
                for result in results:
                    dedupe_key = self._dedupe_key(result)
                    cur.execute(
                        """
                        INSERT INTO enrichment_results (
                            target_type,
                            entity_type,
                            entity_id,
                            received_at,
                            enrichment_type,
                            normalized_data_json,
                            source_run_id,
                            notes,
                            dedupe_key
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (dedupe_key) DO NOTHING
                        """,
                        (
                            result.target_type,
                            result.entity_type,
                            result.entity_id,
                            datetime.now(UTC),
                            result.enrichment_type,
                            Json(result.normalized_data_json),
                            result.source_run_id,
                            self._append_trigger_note(result.notes, trigger_source),
                            dedupe_key,
                        ),
                    )
                    if cur.rowcount == 1:
                        inserted += 1
                    else:
                        skipped += 1

            conn.commit()

        return {"results_received": len(results), "inserted": inserted, "skipped_duplicates": skipped}

    def parse_payload(self, payload: Any) -> list[EnrichmentResultInput]:
        if isinstance(payload, dict) and "results" in payload:
            payload = payload["results"]
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            raise ValueError("Input must be an object, list, or object with 'results' list")

        parsed: list[EnrichmentResultInput] = []
        for index, row in enumerate(payload, start=1):
            if not isinstance(row, dict):
                raise ValueError(f"Result row #{index} must be a JSON object")

            target_type = str(row.get("target_type", "")).strip().lower()
            entity_type = str(row.get("entity_type", "")).strip().lower()
            enrichment_type = str(row.get("enrichment_type", "")).strip()
            entity_id = row.get("entity_id")
            normalized_data_json = row.get("normalized_data_json")

            if target_type not in ALLOWED_TARGETS:
                raise ValueError(f"Row #{index}: invalid target_type '{target_type}'")
            if entity_type not in ALLOWED_ENTITY_TYPES:
                raise ValueError(f"Row #{index}: invalid entity_type '{entity_type}'")
            if not enrichment_type:
                raise ValueError(f"Row #{index}: enrichment_type is required")
            if not isinstance(entity_id, int):
                raise ValueError(f"Row #{index}: entity_id must be an integer")
            if not isinstance(normalized_data_json, dict):
                raise ValueError(f"Row #{index}: normalized_data_json must be an object")

            parsed.append(
                EnrichmentResultInput(
                    target_type=target_type,  # type: ignore[arg-type]
                    entity_type=entity_type,  # type: ignore[arg-type]
                    entity_id=entity_id,
                    enrichment_type=enrichment_type,
                    normalized_data_json=normalized_data_json,
                    source_run_id=row.get("source_run_id"),
                    notes=row.get("notes"),
                )
            )
        return parsed

    def _dedupe_key(self, result: EnrichmentResultInput) -> str:
        canonical = {
            "target_type": result.target_type,
            "entity_type": result.entity_type,
            "entity_id": result.entity_id,
            "enrichment_type": result.enrichment_type,
            "normalized_data_json": result.normalized_data_json,
            "source_run_id": result.source_run_id,
        }
        raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _append_trigger_note(self, notes: str | None, trigger_source: str) -> str:
        base = notes.strip() if isinstance(notes, str) else ""
        trigger_note = f"trigger_source={trigger_source}"
        return f"{base}; {trigger_note}".strip("; ")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest normalized enrichment results back into local Postgres.")
    parser.add_argument("--file", required=True, help="Path to JSON file containing one or more normalized enrichment rows.")
    parser.add_argument("--trigger-source", default="manual")
    args = parser.parse_args()

    payload = json.loads(Path(args.file).read_text(encoding="utf-8"))
    service = EnrichmentIngestionService()
    parsed = service.parse_payload(payload)
    summary = service.ingest(parsed, trigger_source=args.trigger_source)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
