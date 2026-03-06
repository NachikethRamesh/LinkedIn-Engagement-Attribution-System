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
            print("== Pipeline Overview ==")
            cur.execute(
                """
                SELECT run_id, job_name, status, started_at, duration_ms, error_message
                FROM v_demo_pipeline_summary
                LIMIT 10
                """
            )
            for run_id, job_name, status, started_at, duration_ms, error_message in cur.fetchall():
                print(
                    f"- {started_at.isoformat()} run_id={run_id} job={job_name} status={status} "
                    f"duration_ms={duration_ms} error={error_message or 'none'}"
                )

            print("\n== Account Intent Overview ==")
            cur.execute(
                """
                SELECT
                    company_name,
                    score_window,
                    score,
                    confidence,
                    score_reason,
                    enrichment_result_count,
                    last_enriched_at
                FROM v_demo_account_summary
                WHERE score_window = 'rolling_30d'
                ORDER BY score DESC, confidence DESC
                LIMIT 10
                """
            )
            for row in cur.fetchall():
                print(
                    f"- {row[0]} [{row[1]}] score={row[2]} conf={row[3]} "
                    f"enrichments={row[5] or 0} last_enriched={row[6].isoformat() if row[6] else 'none'} "
                    f"reason={row[4]}"
                )

            print("\n== Opportunity Influence Overview ==")
            cur.execute(
                """
                SELECT
                    opportunity_name,
                    company_name,
                    influence_band,
                    influence_score,
                    confidence,
                    notes
                FROM v_demo_opportunity_summary
                WHERE influence_score IS NOT NULL
                ORDER BY influence_score DESC, confidence DESC
                LIMIT 10
                """
            )
            for row in cur.fetchall():
                print(f"- {row[0]} ({row[1]}) band={row[2]} score={row[3]} conf={row[4]} notes={row[5]}")

            print("\n== Writeback Overview ==")
            cur.execute(
                """
                SELECT writeback_run_id, target_type, status, started_at, duration_ms, record_count, error_message
                FROM v_demo_writeback_summary
                LIMIT 10
                """
            )
            for row in cur.fetchall():
                print(
                    f"- run_id={row[0]} target={row[1]} status={row[2]} started={row[3].isoformat()} "
                    f"duration_ms={row[4]} records={row[5]} error={row[6] or 'none'}"
                )

            cur.execute(
                """
                SELECT target_type, entity_type, entity_id, payload_json
                FROM writeback_records
                WHERE status IN ('success', 'skipped')
                ORDER BY created_at DESC
                LIMIT 5
                """
            )
            payload_rows = cur.fetchall()
            print("Recent payload samples:")
            for target, entity_type, entity_id, payload_json in payload_rows:
                payload_json = payload_json if isinstance(payload_json, dict) else {}
                print(
                    f"- target={target} entity={entity_type}:{entity_id} "
                    f"keys={json.dumps(sorted(list(payload_json.keys()))[:10])}"
                )

            print("\n== Enrichment Overview ==")
            cur.execute(
                """
                SELECT target_type, COUNT(*)
                FROM enrichment_results
                GROUP BY target_type
                ORDER BY COUNT(*) DESC, target_type
                """
            )
            for target, count in cur.fetchall():
                print(f"- {target}: {count}")

            cur.execute(
                """
                SELECT target_type, entity_type, entity_id, enrichment_type, received_at, normalized_data_json
                FROM enrichment_results
                ORDER BY received_at DESC
                LIMIT 5
                """
            )
            for target, entity_type, entity_id, enrichment_type, received_at, normalized_data in cur.fetchall():
                normalized_data = normalized_data if isinstance(normalized_data, dict) else {}
                print(
                    f"- {received_at.isoformat()} target={target} entity={entity_type}:{entity_id} "
                    f"type={enrichment_type} keys={json.dumps(sorted(list(normalized_data.keys()))[:8])}"
                )


if __name__ == "__main__":
    main()
