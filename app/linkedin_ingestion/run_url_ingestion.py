from __future__ import annotations

import argparse
import json
from typing import Any

from app.identity_resolution.matcher import IdentityResolutionService
from app.intent_scoring.scorer import IntentScoringService
from app.linkedin_ingestion.url_ingestion import OrganizationPostURLIngestionService
from app.opportunity_attribution.attributor import OpportunityAttributionService
from app.opportunity_attribution.config import DEFAULT_WINDOW_DAYS


def _run_downstream(rebuild: bool, window_days: int) -> dict[str, Any]:
    stage_metrics: dict[str, Any] = {}
    stage_order: list[str] = []

    stage_order.append("identity_resolution")
    stage_metrics["identity_resolution"] = IdentityResolutionService().run(rebuild=rebuild)
    stage_order.append("intent_scoring")
    stage_metrics["intent_scoring"] = IntentScoringService().run(rebuild=rebuild)
    stage_order.append("opportunity_attribution")
    stage_metrics["opportunity_attribution"] = OpportunityAttributionService().run(
        rebuild=rebuild, window_days=window_days
    )
    return {"stage_order": stage_order, "stages": stage_metrics}


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest a LinkedIn organization post by URL.")
    parser.add_argument("--post-url", required=True, help="LinkedIn organization post URL.")
    parser.add_argument(
        "--simulation-mode",
        action="store_true",
        help="Deprecated/disabled: simulation mode is intentionally blocked.",
    )
    parser.add_argument("--resolved-id-override", help="Optional org-post identifier override for local tests.")
    parser.add_argument("--run-pipeline", action="store_true", help="Run matching/scoring/attribution after ingestion.")
    parser.add_argument("--rebuild-downstream", action="store_true", help="Rebuild downstream derived tables.")
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    args = parser.parse_args()

    service = OrganizationPostURLIngestionService()
    result = service.ingest(
        post_url=args.post_url,
        simulation_mode=args.simulation_mode,
        resolved_id_override=args.resolved_id_override,
    )

    payload: dict[str, Any] = {
        "source_name": "linkedin_org_api",
        "import_mode": "url_ingestion",
        "original_url": result.original_url,
        "normalized_url": result.normalized_url,
        "resolved_org_post_identifier": result.resolved_identifier,
        "resolution_mode": result.resolution_mode,
        "adapter_mode": result.adapter_mode,
        "rows_read": result.stats.row_count,
        "rows_successful": result.stats.success_count,
        "posts_created": result.stats.posts_created,
        "posts_updated": result.stats.posts_updated,
        "events_inserted": result.stats.events_inserted,
        "rows_skipped": result.stats.skip_count,
        "warnings": result.stats.warning_count,
    }

    if args.run_pipeline:
        payload["downstream"] = _run_downstream(rebuild=args.rebuild_downstream, window_days=args.window_days)

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
