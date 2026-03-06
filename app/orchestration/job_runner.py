from __future__ import annotations

import traceback
import uuid
from dataclasses import asdict
import os
from typing import Any

from app.crm_sync.load_crm_csv import CRMSyncService
from app.identity_resolution.matcher import IdentityResolutionService
from app.intent_scoring.scorer import IntentScoringService
from app.linkedin_ingestion.base import LinkedInIngestionService
from app.linkedin_ingestion.csv_adapter import CSVLinkedInAdapter
from app.linkedin_ingestion.mock_adapter import MockLinkedInAdapter
from app.linkedin_ingestion.url_ingestion import OrganizationPostURLIngestionService
from app.opportunity_attribution.attributor import OpportunityAttributionService
from app.opportunity_attribution.config import DEFAULT_WINDOW_DAYS
from app.orchestration.status_store import PipelineRunStore
from app.orchestration.types import RunRecord


class JobRunner:
    def __init__(self) -> None:
        self.store = PipelineRunStore()

    def run_job(
        self,
        job_name: str,
        params: dict[str, Any] | None = None,
        trigger_source: str = "manual",
    ) -> RunRecord:
        payload = params or {}
        run_id = str(uuid.uuid4())
        stage_name = None if job_name == "full_pipeline" else job_name

        self.store.create_run(
            run_id=run_id,
            job_name=job_name,
            stage_name=stage_name,
            trigger_source=trigger_source,
            input_params=payload,
        )

        output_metrics: dict[str, Any] = {}
        error_message: str | None = None
        status = "success"

        try:
            if job_name == "linkedin_ingestion_csv":
                output_metrics = self._run_linkedin_csv(payload)
            elif job_name == "linkedin_ingestion_mock":
                output_metrics = self._run_linkedin_mock(payload)
            elif job_name == "identity_resolution":
                output_metrics = self._run_identity_resolution(payload)
            elif job_name == "intent_scoring":
                output_metrics = self._run_intent_scoring(payload)
            elif job_name == "opportunity_attribution":
                output_metrics = self._run_opportunity_attribution(payload)
            elif job_name == "linkedin_ingestion_org_url":
                output_metrics = self._run_linkedin_org_url(payload)
            elif job_name == "full_pipeline":
                output_metrics = self._run_full_pipeline(payload)
            else:
                raise ValueError(f"Unsupported job_name: {job_name}")
        except Exception as exc:
            status = "failed"
            error_message = f"{exc.__class__.__name__}: {exc}"
            output_metrics = {
                "partial_metrics": output_metrics,
                "error_type": exc.__class__.__name__,
                "error_detail": str(exc),
                "traceback": traceback.format_exc(),
            }

        self.store.complete_run(
            run_id=run_id,
            status=status,
            output_metrics=output_metrics,
            error_message=error_message,
        )

        record = self.store.get_run(run_id)
        if record is None:
            raise RuntimeError("Failed to fetch run record after completion")
        return record

    def get_run(self, run_id: str) -> RunRecord | None:
        return self.store.get_run(run_id)

    def list_runs(self, limit: int = 100) -> list[RunRecord]:
        return self.store.list_runs(limit=limit)

    def _run_linkedin_csv(self, params: dict[str, Any]) -> dict[str, Any]:
        source = str(params.get("source", "shield"))
        file_path = str(params.get("file"))
        if not file_path:
            raise ValueError("'file' is required for linkedin_ingestion_csv")

        delimiter = str(params.get("delimiter", ","))
        mapping_override = params.get("mapping_override")

        adapter = CSVLinkedInAdapter(
            file_path=file_path,
            source_name=source,
            mapping_override=mapping_override,
            delimiter=delimiter,
        )
        batch = adapter.collect()
        service = LinkedInIngestionService()
        stats = service.ingest_batch(
            batch=batch,
            source_name=source,
            filename=file_path,
            import_mode="csv",
        )

        return {
            "rows_read": stats.row_count,
            "rows_successful": stats.success_count,
            "posts_created": stats.posts_created,
            "posts_updated": stats.posts_updated,
            "events_inserted": stats.events_inserted,
            "rows_skipped": stats.skip_count,
            "warnings": stats.warning_count,
        }

    def _run_linkedin_mock(self, params: dict[str, Any]) -> dict[str, Any]:
        posts = int(params.get("posts", 20))
        events = int(params.get("events", 250))

        adapter = MockLinkedInAdapter(posts=posts, events=events)
        batch = adapter.collect()
        service = LinkedInIngestionService()
        stats = service.ingest_batch(
            batch=batch,
            source_name="mock",
            filename=f"generated_posts_{posts}_events_{events}",
            import_mode="mock",
        )

        return {
            "rows_read": stats.row_count,
            "rows_successful": stats.success_count,
            "posts_created": stats.posts_created,
            "posts_updated": stats.posts_updated,
            "events_inserted": stats.events_inserted,
            "rows_skipped": stats.skip_count,
            "warnings": stats.warning_count,
        }

    def _run_identity_resolution(self, params: dict[str, Any]) -> dict[str, Any]:
        rebuild = bool(params.get("rebuild", False))
        crm_sync_enabled = self._bool_param(
            params.get("crm_sync_enabled"),
            default=self._bool_param(os.getenv("DEMO_CRM_SYNC_ENABLED"), default=False),
        )
        crm_accounts_csv = str(
            params.get("crm_accounts_file")
            or os.getenv("DEMO_CRM_ACCOUNTS_CSV", "")
        ).strip()
        crm_contacts_csv = str(
            params.get("crm_contacts_file")
            or os.getenv("DEMO_CRM_CONTACTS_CSV", "")
        ).strip()

        crm_sync_summary: dict[str, Any] | None = None
        if crm_sync_enabled:
            if crm_accounts_csv and crm_contacts_csv and os.path.exists(crm_accounts_csv) and os.path.exists(crm_contacts_csv):
                crm_sync_summary = CRMSyncService().run(
                    accounts_file=crm_accounts_csv,
                    contacts_file=crm_contacts_csv,
                )
            else:
                crm_sync_summary = {
                    "skipped": True,
                    "reason": "CRM sync requested but CSV paths are not configured or files are missing",
                    "crm_accounts_file": crm_accounts_csv,
                    "crm_contacts_file": crm_contacts_csv,
                }

        service = IdentityResolutionService()
        result = service.run(rebuild=rebuild)
        if crm_sync_summary is not None:
            result["crm_sync"] = crm_sync_summary
        return result

    def _run_intent_scoring(self, params: dict[str, Any]) -> dict[str, Any]:
        rebuild = bool(params.get("rebuild", False))
        service = IntentScoringService()
        return service.run(rebuild=rebuild)

    def _run_opportunity_attribution(self, params: dict[str, Any]) -> dict[str, Any]:
        rebuild = bool(params.get("rebuild", False))
        window_days = int(params.get("window_days", DEFAULT_WINDOW_DAYS))
        service = OpportunityAttributionService()
        return service.run(rebuild=rebuild, window_days=window_days)

    def _run_linkedin_org_url(self, params: dict[str, Any]) -> dict[str, Any]:
        post_url = str(params.get("post_url", "")).strip()
        if not post_url:
            raise ValueError("'post_url' is required for linkedin_ingestion_org_url")

        simulation_mode = bool(params.get("simulation_mode", False))
        # Simulation mode is intentionally disabled in this branch.
        if simulation_mode:
            raise ValueError("linkedin_ingestion_org_url simulation_mode is disabled; use real mode.")
        resolved_id_override = params.get("resolved_id_override")
        run_pipeline = bool(params.get("run_pipeline", False))
        rebuild_downstream = bool(params.get("rebuild_downstream", False))
        window_days = int(params.get("window_days", DEFAULT_WINDOW_DAYS))

        ingestion = OrganizationPostURLIngestionService()
        ingest_result = ingestion.ingest(
            post_url=post_url,
            simulation_mode=simulation_mode,
            resolved_id_override=str(resolved_id_override) if resolved_id_override else None,
        )
        output: dict[str, Any] = {
            "source_name": "linkedin_org_api",
            "import_mode": "url_ingestion",
            "original_url": ingest_result.original_url,
            "normalized_url": ingest_result.normalized_url,
            "resolved_org_post_identifier": ingest_result.resolved_identifier,
            "resolution_mode": ingest_result.resolution_mode,
            "adapter_mode": ingest_result.adapter_mode,
            "rows_read": ingest_result.stats.row_count,
            "rows_successful": ingest_result.stats.success_count,
            "posts_created": ingest_result.stats.posts_created,
            "posts_updated": ingest_result.stats.posts_updated,
            "events_inserted": ingest_result.stats.events_inserted,
            "rows_skipped": ingest_result.stats.skip_count,
            "warnings": ingest_result.stats.warning_count,
            "run_pipeline": run_pipeline,
        }

        if run_pipeline:
            stage_order = ["identity_resolution", "intent_scoring", "opportunity_attribution"]
            stages = {
                "identity_resolution": self._run_identity_resolution({"rebuild": rebuild_downstream}),
                "intent_scoring": self._run_intent_scoring({"rebuild": rebuild_downstream}),
                "opportunity_attribution": self._run_opportunity_attribution(
                    {"rebuild": rebuild_downstream, "window_days": window_days}
                ),
            }
            output["downstream"] = {
                "rebuild_downstream": rebuild_downstream,
                "window_days": window_days,
                "stage_order": stage_order,
                "stages": stages,
            }

        return output

    def _run_full_pipeline(self, params: dict[str, Any]) -> dict[str, Any]:
        source = str(params.get("source", "mock"))
        rebuild = bool(params.get("rebuild", False))
        window_days = int(params.get("window_days", DEFAULT_WINDOW_DAYS))

        stage_metrics: dict[str, Any] = {}
        stage_order: list[str] = []

        if source == "mock":
            stage_order.append("linkedin_ingestion_mock")
            stage_metrics["linkedin_ingestion_mock"] = self._run_linkedin_mock(
                {
                    "posts": int(params.get("posts", 20)),
                    "events": int(params.get("events", 250)),
                }
            )
        elif source in {"shield_csv", "sprout_csv", "generic_csv"}:
            file_path = params.get("file")
            if not file_path:
                raise ValueError("'file' is required for CSV full pipeline sources")
            stage_order.append("linkedin_ingestion_csv")
            stage_metrics["linkedin_ingestion_csv"] = self._run_linkedin_csv(
                {
                    "source": source.replace("_csv", ""),
                    "file": str(file_path),
                    "delimiter": str(params.get("delimiter", ",")),
                    "mapping_override": params.get("mapping_override"),
                }
            )
        else:
            raise ValueError("source must be one of: mock, shield_csv, sprout_csv, generic_csv")

        stage_order.append("identity_resolution")
        stage_metrics["identity_resolution"] = self._run_identity_resolution({"rebuild": rebuild})
        stage_order.append("intent_scoring")
        stage_metrics["intent_scoring"] = self._run_intent_scoring({"rebuild": rebuild})
        stage_order.append("opportunity_attribution")
        stage_metrics["opportunity_attribution"] = self._run_opportunity_attribution(
            {"rebuild": rebuild, "window_days": window_days}
        )

        return {
            "source": source,
            "rebuild": rebuild,
            "window_days": window_days,
            "stage_order": stage_order,
            "stages": stage_metrics,
        }

    def _bool_param(self, value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return default


def run_record_to_dict(record: RunRecord) -> dict[str, Any]:
    payload = asdict(record)
    payload["started_at"] = record.started_at.isoformat()
    payload["completed_at"] = record.completed_at.isoformat() if record.completed_at else None
    return payload
