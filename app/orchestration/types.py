from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

JobName = Literal[
    "linkedin_ingestion_csv",
    "linkedin_ingestion_mock",
    "linkedin_ingestion_org_url",
    "identity_resolution",
    "intent_scoring",
    "opportunity_attribution",
    "full_pipeline",
]

RunStatus = Literal["queued", "running", "success", "failed"]


@dataclass(slots=True)
class RunRecord:
    run_id: str
    job_name: str
    stage_name: str | None
    status: str
    started_at: datetime
    completed_at: datetime | None
    duration_ms: int | None
    trigger_source: str
    input_params_json: dict[str, Any]
    output_metrics_json: dict[str, Any]
    error_message: str | None
