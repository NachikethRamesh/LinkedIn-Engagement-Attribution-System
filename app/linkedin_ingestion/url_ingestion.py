from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from app.linkedin_ingestion.base import LinkedInIngestionService
from app.linkedin_ingestion.comment_ai import CommentAnalysisService
from app.linkedin_ingestion.csv_adapter import CSVLinkedInAdapter
from app.linkedin_ingestion.org_api_adapter import OrgPostFetchBundle, OrganizationPostAPIAdapter
from app.linkedin_ingestion.org_post_resolver import ResolvedOrgPost, resolve_org_post_identifier
from app.linkedin_ingestion.types import AdapterBatch, ImportStats, NormalizedPost, NormalizedSocialEvent
from app.linkedin_ingestion.validator import clean_text, normalize_linkedin_post_url, parse_datetime, resolve_actor_origin


@dataclass(slots=True)
class URLIngestionResult:
    stats: ImportStats
    original_url: str
    normalized_url: str
    resolved_identifier: str
    resolution_mode: str
    adapter_mode: str


class OrganizationPostURLIngestionService:
    SOURCE_NAME = "linkedin_org_api"
    IMPORT_MODE = "url_ingestion"

    def __init__(self, adapter: OrganizationPostAPIAdapter | None = None) -> None:
        self.adapter = adapter or OrganizationPostAPIAdapter(simulation_mode=False)
        self.ingestion_service = LinkedInIngestionService()

    def ingest(
        self,
        *,
        post_url: str,
        simulation_mode: bool = False,
        resolved_id_override: str | None = None,
    ) -> URLIngestionResult:
        # Simulation ingestion paths are intentionally disabled.
        if simulation_mode:
            raise ValueError(
                "simulation_mode=true is disabled. Run org URL ingestion in real mode only."
            )
        resolved = resolve_org_post_identifier(
            post_url,
            simulation_mode=simulation_mode,
            resolved_id_override=resolved_id_override,
        )

        simulation_source = os.getenv("LINKEDIN_ORG_SIMULATION_SOURCE", "fixture_json").strip().lower()
        if simulation_mode and simulation_source == "csv":
            batch = self._collect_csv_simulation_batch(resolved=resolved)
            adapter_mode = "simulation_csv"
        else:
            adapter = self.adapter
            if adapter.simulation_mode != simulation_mode:
                adapter = OrganizationPostAPIAdapter(simulation_mode=simulation_mode)
            bundle = adapter.fetch_bundle(resolved.resolved_identifier)
            batch = self._normalize_bundle(resolved=resolved, bundle=bundle)
            adapter_mode = bundle.adapter_mode

        stats = self.ingestion_service.ingest_batch(
            batch=batch,
            source_name=self.SOURCE_NAME,
            filename=resolved.normalized_url,
            import_mode=self.IMPORT_MODE,
        )
        return URLIngestionResult(
            stats=stats,
            original_url=resolved.original_url,
            normalized_url=resolved.normalized_url,
            resolved_identifier=resolved.resolved_identifier,
            resolution_mode=resolved.resolution_mode,
            adapter_mode=adapter_mode,
        )

    def _collect_csv_simulation_batch(self, *, resolved: ResolvedOrgPost) -> AdapterBatch:
        # Legacy simulation helper retained only for future reactivation work.
        # Current ingest() guard prevents this from being called.
        csv_path_text = os.getenv("LINKEDIN_ORG_SIMULATION_CSV", "").strip()
        csv_source = os.getenv("LINKEDIN_ORG_SIMULATION_CSV_SOURCE", "generic").strip().lower() or "generic"
        if not csv_path_text:
            raise ValueError(
                "CSV simulation source requested but LINKEDIN_ORG_SIMULATION_CSV is empty. "
                "Set LINKEDIN_ORG_SIMULATION_CSV to your prepared LinkedIn post dataset."
            )
        csv_path = Path(csv_path_text)
        if not csv_path.exists():
            raise ValueError(
                f"CSV simulation source requested but file does not exist: {csv_path}. "
                "Set LINKEDIN_ORG_SIMULATION_CSV to your prepared LinkedIn post dataset."
            )

        adapter = CSVLinkedInAdapter(file_path=str(csv_path), source_name=csv_source)
        raw_batch = adapter.collect()

        filtered_posts: list[NormalizedPost] = []
        filtered_events: list[NormalizedSocialEvent] = []
        for post in raw_batch.posts:
            normalized_post_url = normalize_linkedin_post_url(post.post_url)
            if normalized_post_url == resolved.normalized_url:
                post.post_url = resolved.normalized_url
                post.source_name = self.SOURCE_NAME
                post.platform_post_id = resolved.resolved_identifier
                post.raw_payload_json = {
                    **(post.raw_payload_json or {}),
                    "source_name": self.SOURCE_NAME,
                    "import_mode": self.IMPORT_MODE,
                    "adapter_mode": "simulation_csv",
                    "original_url": resolved.original_url,
                    "normalized_url": resolved.normalized_url,
                    "resolved_org_post_identifier": resolved.resolved_identifier,
                    "resolution_mode": resolved.resolution_mode,
                    "simulation_csv_path": str(csv_path),
                    "simulation_csv_source": csv_source,
                }
                filtered_posts.append(post)

        if len(filtered_posts) > 1:
            filtered_posts.sort(key=lambda p: p.created_at)
            filtered_posts = [filtered_posts[0]]

        for event in raw_batch.events:
            normalized_event_url = normalize_linkedin_post_url(event.post_url)
            if normalized_event_url == resolved.normalized_url:
                event.post_url = resolved.normalized_url
                event.source_name = self.SOURCE_NAME
                event.import_mode = self.IMPORT_MODE
                event.metadata_json = {
                    **(event.metadata_json or {}),
                    "source_name": self.SOURCE_NAME,
                    "import_mode": self.IMPORT_MODE,
                    "adapter_mode": "simulation_csv",
                    "original_url": resolved.original_url,
                    "normalized_url": resolved.normalized_url,
                    "resolved_org_post_identifier": resolved.resolved_identifier,
                    "resolution_mode": resolved.resolution_mode,
                    "simulation_csv_path": str(csv_path),
                    "simulation_csv_source": csv_source,
                }
                filtered_events.append(event)

        if not filtered_posts and not filtered_events:
            raise ValueError(
                "No rows in simulation CSV matched the normalized post URL. "
                f"Expected post_url={resolved.normalized_url} in {csv_path}."
            )

        if not filtered_posts and filtered_events:
            earliest = min((e.event_timestamp for e in filtered_events), default=datetime.now(UTC))
            filtered_posts = [
                NormalizedPost(
                    post_url=resolved.normalized_url,
                    author_name="Simulated LinkedIn Org",
                    topic="Simulated LinkedIn post dataset",
                    cta_url=None,
                    created_at=earliest,
                    source_name=self.SOURCE_NAME,
                    platform_post_id=resolved.resolved_identifier,
                    raw_payload_json={
                        "source_name": self.SOURCE_NAME,
                        "import_mode": self.IMPORT_MODE,
                        "adapter_mode": "simulation_csv",
                        "original_url": resolved.original_url,
                        "normalized_url": resolved.normalized_url,
                        "resolved_org_post_identifier": resolved.resolved_identifier,
                        "resolution_mode": resolved.resolution_mode,
                        "simulation_csv_path": str(csv_path),
                        "simulation_csv_source": csv_source,
                        "generated_post_row": True,
                    },
                )
            ]

        if filtered_posts:
            metric_timestamp = min(
                [p.created_at for p in filtered_posts] + [e.event_timestamp for e in filtered_events],
                default=datetime.now(UTC),
            )
            metric_defaults = (
                ("post_impression", 1000, "impressions"),
                ("post_link_click", 20, "post_link_clicks"),
            )
            for event_type, source_metric_count, original_column in metric_defaults:
                filtered_events.append(
                    NormalizedSocialEvent(
                        post_url=resolved.normalized_url,
                        actor_name=None,
                        actor_linkedin_url=None,
                        actor_company_raw=filtered_posts[0].author_name,
                        event_type=event_type,
                        event_timestamp=metric_timestamp,
                        metadata_json={
                            "source_name": self.SOURCE_NAME,
                            "import_mode": self.IMPORT_MODE,
                            "raw_row_id": f"{resolved.resolved_identifier}:sim_default:{event_type}",
                            "aggregated_import": True,
                            "source_metric_count": source_metric_count,
                            "original_columns": [original_column],
                            "actor_origin": "aggregate_unknown",
                            "original_url": resolved.original_url,
                            "normalized_url": resolved.normalized_url,
                            "resolved_org_post_identifier": resolved.resolved_identifier,
                            "resolution_mode": resolved.resolution_mode,
                            "adapter_mode": "simulation_csv",
                            "simulation_csv_path": str(csv_path),
                            "simulation_csv_source": csv_source,
                            "simulated_metric_default": True,
                        },
                        source_name=self.SOURCE_NAME,
                        import_mode=self.IMPORT_MODE,
                        aggregated_import=True,
                        platform_object_type="post",
                        platform_object_id=resolved.resolved_identifier,
                        parent_platform_object_id=None,
                        event_external_id=f"{resolved.resolved_identifier}:sim_default:{event_type}",
                        availability_status="aggregate_only",
                        raw_payload_json={"metric_name": event_type, "metric_count": source_metric_count},
                    )
                )

        return AdapterBatch(
            posts=filtered_posts,
            events=filtered_events,
            row_count=raw_batch.row_count + 2,
            skipped_rows=raw_batch.skipped_rows,
            warnings=raw_batch.warnings,
        )

    def _normalize_bundle(self, *, resolved: ResolvedOrgPost, bundle: OrgPostFetchBundle) -> AdapterBatch:
        author_name = clean_text(bundle.post_payload.get("author_name")) or clean_text(
            bundle.post_payload.get("organization_name")
        )
        topic = clean_text(bundle.post_payload.get("topic")) or clean_text(bundle.post_payload.get("text"))
        cta_url = clean_text(bundle.post_payload.get("cta_url"))
        created_at = parse_datetime(bundle.post_payload.get("created_at"), fallback=datetime.now(UTC)) or datetime.now(UTC)

        post = NormalizedPost(
            post_url=resolved.normalized_url,
            author_name=author_name or "Unknown Organization",
            topic=topic or "LinkedIn organization post",
            cta_url=cta_url,
            created_at=created_at,
            source_name=self.SOURCE_NAME,
            platform_post_id=resolved.resolved_identifier,
            organization_id=clean_text(bundle.post_payload.get("organization_id")),
            raw_payload_json={
                "source_name": self.SOURCE_NAME,
                "import_mode": self.IMPORT_MODE,
                "adapter_mode": bundle.adapter_mode,
                "original_url": resolved.original_url,
                "normalized_url": resolved.normalized_url,
                "resolved_org_post_identifier": resolved.resolved_identifier,
                "resolution_mode": resolved.resolution_mode,
                "post_payload": bundle.post_payload,
            },
        )

        events: list[NormalizedSocialEvent] = []
        comment_ai = CommentAnalysisService()
        event_timestamp = parse_datetime(bundle.post_payload.get("created_at"), fallback=created_at) or created_at
        metrics_payload = bundle.metrics_payload or {}

        metric_map = {
            "impressions": "post_impression",
            "reactions": "post_like",
            "comments": "post_comment",
            "shares": "post_repost",
            "clicks": "post_link_click",
        }
        detailed_availability = {
            "reactions": len(bundle.reactions_payload) > 0,
            "comments": len(bundle.comments_payload) > 0,
        }

        for metric_name, event_type in metric_map.items():
            if metric_name in detailed_availability and detailed_availability[metric_name]:
                continue

            metric_count = _parse_count(metrics_payload.get(metric_name))
            if metric_count is None or metric_count <= 0:
                continue

            raw_row_id = f"{resolved.resolved_identifier}:metric:{metric_name}"
            metadata_json = self._build_metadata(
                resolved=resolved,
                bundle=bundle,
                raw_row_id=raw_row_id,
                aggregated_import=True,
                source_metric_count=metric_count,
                original_columns=[metric_name],
                actor_origin=resolve_actor_origin(
                    source_name=self.SOURCE_NAME,
                    aggregated_import=True,
                    actor_name=None,
                    actor_linkedin_url=None,
                ),
                raw_payload={"metric_name": metric_name, "metric_count": metric_count},
            )
            events.append(
                NormalizedSocialEvent(
                    post_url=resolved.normalized_url,
                    actor_name=None,
                    actor_linkedin_url=None,
                    actor_company_raw=clean_text(bundle.post_payload.get("organization_name")),
                    event_type=event_type,
                    event_timestamp=event_timestamp,
                    metadata_json=metadata_json,
                    source_name=self.SOURCE_NAME,
                    import_mode=self.IMPORT_MODE,
                    aggregated_import=True,
                    platform_object_type="post",
                    platform_object_id=resolved.resolved_identifier,
                    parent_platform_object_id=None,
                    event_external_id=raw_row_id,
                    availability_status="aggregate_only",
                    raw_payload_json={"metric_name": metric_name, "metric_count": metric_count},
                )
            )

        for index, comment in enumerate(bundle.comments_payload, start=1):
            actor_name = clean_text(comment.get("actor_name"))
            actor_linkedin_url = clean_text(comment.get("actor_linkedin_url"))
            actor_company_raw = clean_text(comment.get("actor_company_raw"))
            actor_external_id = clean_text(comment.get("actor_linkedin_id")) or _linkedin_slug(actor_linkedin_url)
            actor_urn = clean_text(comment.get("actor_urn"))
            comment_timestamp = parse_datetime(comment.get("created_at"), fallback=event_timestamp) or event_timestamp
            comment_id = clean_text(comment.get("comment_id")) or f"{resolved.resolved_identifier}:comment:{index}"
            parent_comment_id = clean_text(comment.get("parent_comment_id")) or clean_text(comment.get("parent_id"))
            object_type = "reply" if parent_comment_id else "comment"
            actor_origin = resolve_actor_origin(
                source_name=self.SOURCE_NAME,
                aggregated_import=False,
                actor_name=actor_name,
                actor_linkedin_url=actor_linkedin_url,
            )
            raw_row_id = comment_id
            metadata_json = self._build_metadata(
                resolved=resolved,
                bundle=bundle,
                raw_row_id=raw_row_id,
                aggregated_import=False,
                source_metric_count=None,
                original_columns=["comments", "comment_replies"],
                actor_origin=actor_origin,
                raw_payload=comment,
            )
            comment_text = (
                clean_text(comment.get("comment_text"))
                or clean_text(comment.get("text"))
                or clean_text(comment.get("message"))
                or clean_text(comment.get("body"))
            )
            if comment_text:
                metadata_json["comment_text"] = comment_text
                try:
                    analysis = comment_ai.analyze(comment_text)
                    metadata_json["comment_analysis"] = {
                        "sentiment": analysis.sentiment,
                        "intent": analysis.intent,
                        "confidence": analysis.confidence,
                        "summary": analysis.summary,
                        "source": analysis.source,
                    }
                except Exception:
                    # Keep ingestion resilient; scoring can proceed without AI metadata.
                    metadata_json["comment_analysis"] = {
                        "sentiment": "unknown",
                        "intent": "unknown",
                        "confidence": 0.0,
                        "summary": "comment analysis failed during org-url ingestion",
                        "source": "gemini_error",
                    }
            events.append(
                NormalizedSocialEvent(
                    post_url=resolved.normalized_url,
                    actor_name=actor_name,
                    actor_linkedin_url=actor_linkedin_url,
                    actor_company_raw=actor_company_raw,
                    event_type="post_comment",
                    event_timestamp=comment_timestamp,
                    metadata_json=metadata_json,
                    source_name=self.SOURCE_NAME,
                    import_mode=self.IMPORT_MODE,
                    aggregated_import=False,
                    platform_object_type=object_type,
                    platform_object_id=comment_id,
                    parent_platform_object_id=parent_comment_id or resolved.resolved_identifier,
                    event_external_id=comment_id,
                    actor_external_id=actor_external_id,
                    actor_urn=actor_urn,
                    actor_title=clean_text(comment.get("actor_title")),
                    actor_headline=clean_text(comment.get("actor_headline")),
                    actor_company=actor_company_raw,
                    availability_status="actor_resolved" if actor_external_id or actor_name or actor_linkedin_url else "aggregate_only",
                    raw_payload_json=comment,
                )
            )

        for index, reaction in enumerate(bundle.reactions_payload, start=1):
            actor_name = clean_text(reaction.get("actor_name"))
            actor_linkedin_url = clean_text(reaction.get("actor_linkedin_url"))
            actor_company_raw = clean_text(reaction.get("actor_company_raw"))
            actor_external_id = clean_text(reaction.get("actor_linkedin_id")) or _linkedin_slug(actor_linkedin_url)
            actor_urn = clean_text(reaction.get("actor_urn"))
            reaction_timestamp = parse_datetime(reaction.get("created_at"), fallback=event_timestamp) or event_timestamp
            reaction_id = clean_text(reaction.get("reaction_id")) or f"{resolved.resolved_identifier}:reaction:{index}"
            target_comment_id = clean_text(reaction.get("target_comment_id")) or clean_text(reaction.get("comment_id"))
            object_type = "comment" if target_comment_id else "post"
            object_id = target_comment_id or resolved.resolved_identifier
            actor_origin = resolve_actor_origin(
                source_name=self.SOURCE_NAME,
                aggregated_import=False,
                actor_name=actor_name,
                actor_linkedin_url=actor_linkedin_url,
            )
            raw_row_id = reaction_id
            metadata_json = self._build_metadata(
                resolved=resolved,
                bundle=bundle,
                raw_row_id=raw_row_id,
                aggregated_import=False,
                source_metric_count=None,
                original_columns=["reactions", "comment_reactions"],
                actor_origin=actor_origin,
                raw_payload=reaction,
            )
            events.append(
                NormalizedSocialEvent(
                    post_url=resolved.normalized_url,
                    actor_name=actor_name,
                    actor_linkedin_url=actor_linkedin_url,
                    actor_company_raw=actor_company_raw,
                    event_type="post_like",
                    event_timestamp=reaction_timestamp,
                    metadata_json=metadata_json,
                    source_name=self.SOURCE_NAME,
                    import_mode=self.IMPORT_MODE,
                    aggregated_import=False,
                    platform_object_type=object_type,
                    platform_object_id=object_id,
                    parent_platform_object_id=resolved.resolved_identifier if target_comment_id else None,
                    event_external_id=reaction_id,
                    actor_external_id=actor_external_id,
                    actor_urn=actor_urn,
                    actor_title=clean_text(reaction.get("actor_title")),
                    actor_headline=clean_text(reaction.get("actor_headline")),
                    actor_company=actor_company_raw,
                    availability_status="actor_resolved" if actor_external_id or actor_name or actor_linkedin_url else "aggregate_only",
                    raw_payload_json=reaction,
                )
            )

        return AdapterBatch(
            posts=[post],
            events=events,
            row_count=max(len(events), 1),
            skipped_rows=0,
            warnings=[],
        )

    def _build_metadata(
        self,
        *,
        resolved: ResolvedOrgPost,
        bundle: OrgPostFetchBundle,
        raw_row_id: str,
        aggregated_import: bool,
        source_metric_count: int | None,
        original_columns: list[str],
        actor_origin: str,
        raw_payload: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "source_name": self.SOURCE_NAME,
            "import_mode": self.IMPORT_MODE,
            "raw_row_id": raw_row_id,
            "aggregated_import": aggregated_import,
            "source_metric_count": source_metric_count,
            "original_columns": original_columns,
            "actor_origin": actor_origin,
            "original_url": resolved.original_url,
            "normalized_url": resolved.normalized_url,
            "resolved_org_post_identifier": resolved.resolved_identifier,
            "resolution_mode": resolved.resolution_mode,
            "adapter_mode": bundle.adapter_mode,
            "ingested_at": datetime.now(UTC).isoformat(),
            "raw_payload": raw_payload,
        }


def _parse_count(value: Any) -> int | None:
    text = clean_text(value)
    if text is None:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _linkedin_slug(linkedin_url: str | None) -> str | None:
    cleaned = clean_text(linkedin_url)
    if cleaned is None:
        return None
    path_parts = [part for part in urlsplit(cleaned).path.split("/") if part]
    if not path_parts:
        return None
    if path_parts[0].lower() == "in" and len(path_parts) > 1:
        return path_parts[1]
    return path_parts[-1]
