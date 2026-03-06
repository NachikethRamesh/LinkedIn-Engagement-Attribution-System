from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

MatchType = Literal[
    "exact_contact_linkedin_url",
    "exact_contact_name_and_account",
    "exact_account_name",
    "normalized_account_name",
    "inferred_from_actor_company",
    "inferred_from_website_domain",
    "unresolved",
    "skipped_aggregate_import",
]


@dataclass(slots=True)
class SocialEventRecord:
    id: int
    actor_name: str | None
    actor_linkedin_url: str | None
    actor_company_raw: str | None
    metadata_json: dict[str, Any]


@dataclass(slots=True)
class MatchResult:
    social_event_id: int
    matched_contact_id: int | None
    matched_account_id: int | None
    match_type: MatchType
    match_confidence: float
    match_reason: str
    matched_on_fields_json: dict[str, Any]
    created_at: datetime