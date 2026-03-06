from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from app.linkedin_ingestion.base import LinkedInAdapter
from app.linkedin_ingestion.comment_ai import CommentAnalysisService
from app.linkedin_ingestion.normalizer import normalize_csv_row
from app.linkedin_ingestion.types import AdapterBatch, ImportWarning, NormalizedPost, NormalizedSocialEvent
from app.linkedin_ingestion.validator import (
    build_original_columns,
    ensure_post_url,
    resolve_actor_origin,
    validate_event_type,
)


class CSVLinkedInAdapter(LinkedInAdapter):
    def __init__(
        self,
        file_path: str,
        source_name: str,
        mapping_override: dict[str, list[str]] | None = None,
        delimiter: str = ",",
    ) -> None:
        self.file_path = Path(file_path)
        self.source_name = source_name
        self.mapping_override = mapping_override
        self.delimiter = delimiter

    def collect(self) -> AdapterBatch:
        posts: list[NormalizedPost] = []
        events: list[NormalizedSocialEvent] = []
        warnings: list[ImportWarning] = []
        row_count = 0
        skipped_rows = 0
        comment_ai = CommentAnalysisService()

        with self.file_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file, delimiter=self.delimiter)
            for index, row in enumerate(reader, start=2):
                row_count += 1
                row_id = str(index)

                post_payload, normalized_events, row_warnings = normalize_csv_row(
                    row=row,
                    source_name=self.source_name,
                    row_id=row_id,
                    mapping_override=self.mapping_override,
                )

                for warning in row_warnings:
                    warnings.append(ImportWarning(row_id=row_id, message=warning))

                post_url = ensure_post_url(post_payload.get("post_url"))
                if post_url is None:
                    skipped_rows += 1
                    warnings.append(ImportWarning(row_id=row_id, message="skipped row: invalid or missing post URL"))
                    continue

                created_at = post_payload.get("created_at")
                posts.append(
                    NormalizedPost(
                        post_url=post_url,
                        author_name=post_payload.get("author_name") or "Unknown Author",
                        topic=post_payload.get("topic") or "LinkedIn Post",
                        cta_url=post_payload.get("cta_url"),
                        created_at=created_at,
                        source_name=self.source_name,
                        raw_payload_json={"row": row, "row_id": row_id},
                    )
                )

                original_columns = build_original_columns(row)
                for event in normalized_events:
                    event_type = validate_event_type(event.get("event_type"))
                    if event_type is None:
                        warnings.append(ImportWarning(row_id=row_id, message="skipped event: unsupported event type"))
                        continue

                    event_timestamp = event.get("event_timestamp") or created_at
                    aggregated_import = bool(event.get("aggregated_import", False))
                    actor_name = event.get("actor_name")
                    actor_linkedin_url = event.get("actor_linkedin_url")
                    actor_company_raw = event.get("actor_company_raw")
                    if aggregated_import:
                        actor_name = None
                        actor_linkedin_url = None

                    actor_origin = resolve_actor_origin(
                        source_name=self.source_name,
                        aggregated_import=aggregated_import,
                        actor_name=actor_name,
                        actor_linkedin_url=actor_linkedin_url,
                    )
                    comment_text = event.get("comment_text")
                    metadata_json: dict[str, Any] = {
                        "source_name": self.source_name,
                        "import_mode": "csv",
                        "raw_row_id": row_id,
                        "aggregated_import": aggregated_import,
                        "source_metric_count": event.get("source_metric_count"),
                        "original_columns": original_columns,
                        "actor_origin": actor_origin,
                        "raw_payload": row,
                    }
                    if event_type == "post_comment" and not aggregated_import and comment_text:
                        metadata_json["comment_text"] = comment_text
                        try:
                            comment_analysis = comment_ai.analyze(comment_text)
                            metadata_json["comment_analysis"] = {
                                "sentiment": comment_analysis.sentiment,
                                "intent": comment_analysis.intent,
                                "confidence": comment_analysis.confidence,
                                "summary": comment_analysis.summary,
                                "source": comment_analysis.source,
                            }
                        except Exception:
                            warnings.append(
                                ImportWarning(row_id=row_id, message="comment analysis failed; continuing without analysis")
                            )

                    events.append(
                        NormalizedSocialEvent(
                            post_url=post_url,
                            actor_name=actor_name,
                            actor_linkedin_url=actor_linkedin_url,
                            actor_company_raw=actor_company_raw,
                            event_type=event_type,
                            event_timestamp=event_timestamp,
                            metadata_json=metadata_json,
                            source_name=self.source_name,
                            import_mode="csv",
                            aggregated_import=aggregated_import,
                        )
                    )

        return AdapterBatch(
            posts=posts,
            events=events,
            row_count=row_count,
            skipped_rows=skipped_rows,
            warnings=warnings,
        )
