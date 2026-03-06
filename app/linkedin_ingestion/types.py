from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

CanonicalEventType = Literal[
    "post_impression",
    "post_like",
    "post_comment",
    "post_repost",
    "post_link_click",
]

CANONICAL_EVENT_TYPES: tuple[str, ...] = (
    "post_impression",
    "post_like",
    "post_comment",
    "post_repost",
    "post_link_click",
)


@dataclass(slots=True)
class NormalizedPost:
    post_url: str
    author_name: str
    topic: str
    cta_url: str | None
    created_at: datetime
    source_name: str
    raw_payload_json: dict[str, Any]
    platform: str = "linkedin"
    workspace_id: str | None = None
    tenant_id: str | None = None
    sync_job_id: str | None = None
    platform_post_id: str | None = None
    platform_post_urn: str | None = None
    organization_id: str | None = None


@dataclass(slots=True)
class NormalizedSocialEvent:
    post_url: str
    actor_name: str | None
    actor_linkedin_url: str | None
    actor_company_raw: str | None
    event_type: CanonicalEventType
    event_timestamp: datetime
    metadata_json: dict[str, Any]
    source_name: str
    import_mode: str
    aggregated_import: bool = False
    platform: str = "linkedin"
    workspace_id: str | None = None
    tenant_id: str | None = None
    sync_job_id: str | None = None
    platform_object_type: str = "post"
    platform_object_id: str | None = None
    parent_platform_object_id: str | None = None
    event_external_id: str | None = None
    actor_external_id: str | None = None
    actor_urn: str | None = None
    actor_headline: str | None = None
    actor_title: str | None = None
    actor_company: str | None = None
    availability_status: str = "actor_resolved"
    raw_payload_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ImportWarning:
    row_id: str
    message: str


@dataclass(slots=True)
class ImportStats:
    source_name: str
    filename: str
    import_mode: str
    row_count: int = 0
    success_count: int = 0
    skip_count: int = 0
    warning_count: int = 0
    posts_created: int = 0
    posts_updated: int = 0
    events_inserted: int = 0
    warnings: list[ImportWarning] = field(default_factory=list)

    def add_warning(self, row_id: str, message: str) -> None:
        self.warning_count += 1
        self.warnings.append(ImportWarning(row_id=row_id, message=message))


@dataclass(slots=True)
class AdapterBatch:
    posts: list[NormalizedPost]
    events: list[NormalizedSocialEvent]
    row_count: int
    skipped_rows: int
    warnings: list[ImportWarning]
