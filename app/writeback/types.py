from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

TargetType = Literal["crm", "clay", "exa", "webhook_generic"]
EntityType = Literal["account", "opportunity", "contact", "unresolved_account_candidate"]
WritebackRecordStatus = Literal["pending", "sent", "success", "failed", "skipped"]
WritebackRunStatus = Literal["queued", "running", "success", "partial_success", "failed"]


@dataclass(slots=True)
class SelectedEntity:
    entity_type: EntityType
    entity_id: int
    target_type: TargetType
    selection_bucket: str
    selection_reason: str
    data: dict[str, Any]


@dataclass(slots=True)
class DeliveryResult:
    status: WritebackRecordStatus
    response_json: dict[str, Any]
    error_message: str | None = None
    external_key: str | None = None


@dataclass(slots=True)
class WritebackRunRecord:
    writeback_run_id: str
    target_type: TargetType
    status: str
    started_at: datetime
    completed_at: datetime | None
    duration_ms: int | None
    trigger_source: str
    selection_params_json: dict[str, Any]
    result_metrics_json: dict[str, Any]
    error_message: str | None


@dataclass(slots=True)
class EnrichmentResultInput:
    target_type: TargetType
    entity_type: EntityType
    entity_id: int
    enrichment_type: str
    normalized_data_json: dict[str, Any]
    source_run_id: str | None = None
    notes: str | None = None
