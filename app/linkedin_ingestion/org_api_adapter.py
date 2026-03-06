from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.integrations_config import get_linkedin_credentials
from app.linkedin_ingestion.validator import clean_text, parse_datetime


@dataclass(slots=True)
class OrgPostFetchBundle:
    post_payload: dict[str, Any]
    metrics_payload: dict[str, Any]
    comments_payload: list[dict[str, Any]]
    reactions_payload: list[dict[str, Any]]
    adapter_mode: str


class OrganizationPostAPIAdapter:
    """
    Official organization/community management API adapter interface.

    In local development without OAuth credentials, enable simulation_mode to
    use deterministic fixture payloads.

    TODO (real mode):
    - Implement OAuth authorization and token refresh.
    - Configure org/community endpoint URLs and API versions.
    - Add permissions/scope checks.
    - Add pagination for comments/reactions.
    - Add rate-limit retries/backoff.
    """

    def __init__(
        self,
        *,
        simulation_mode: bool = False,
        access_token: str | None = None,
        organization_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        post_fixture: str = "data/linkedin_org_post_mock_response.json",
        comments_fixture: str = "data/linkedin_org_post_comments_mock.json",
        reactions_fixture: str = "data/linkedin_org_post_reactions_mock.json",
    ) -> None:
        # Simulation fixtures were removed intentionally. Keep this guard to prevent
        # accidental re-enablement of local fixture mode.
        if simulation_mode:
            raise ValueError(
                "LinkedIn org-url simulation mode is disabled. Use real mode credentials and API access."
            )
        creds = get_linkedin_credentials()
        self.simulation_mode = simulation_mode
        self.access_token = access_token or creds.access_token
        self.organization_id = organization_id or creds.organization_id
        self.client_id = client_id or creds.client_id
        self.client_secret = client_secret or creds.client_secret
        self.post_fixture = Path(post_fixture)
        self.comments_fixture = Path(comments_fixture)
        self.reactions_fixture = Path(reactions_fixture)

    def fetch_post_metadata(self, resolved_identifier: str) -> dict[str, Any]:
        self._assert_real_mode_configured()
        raise NotImplementedError("Real LinkedIn organization post metadata API calls are not wired yet.")

    def fetch_social_metadata(self, resolved_identifier: str) -> dict[str, Any]:
        self._assert_real_mode_configured()
        raise NotImplementedError("Real LinkedIn organization post social metadata API calls are not wired yet.")

    def fetch_comments(self, resolved_identifier: str) -> list[dict[str, Any]]:
        self._assert_real_mode_configured()
        raise NotImplementedError("Real LinkedIn organization post comments API calls are not wired yet.")

    def fetch_reactions(self, resolved_identifier: str) -> list[dict[str, Any]]:
        self._assert_real_mode_configured()
        raise NotImplementedError("Real LinkedIn organization post reactions API calls are not wired yet.")

    def fetch_bundle(self, resolved_identifier: str) -> OrgPostFetchBundle:
        post_payload = self.fetch_post_metadata(resolved_identifier)
        metrics_payload = self.fetch_social_metadata(resolved_identifier)
        comments_payload = self.fetch_comments(resolved_identifier)
        reactions_payload = self.fetch_reactions(resolved_identifier)
        return OrgPostFetchBundle(
            post_payload=post_payload,
            metrics_payload=metrics_payload,
            comments_payload=comments_payload,
            reactions_payload=reactions_payload,
            adapter_mode="real",
        )

    def normalize_post_fields(
        self,
        *,
        bundle: OrgPostFetchBundle,
        normalized_url: str,
    ) -> tuple[str, str, str | None, datetime]:
        author_name = clean_text(bundle.post_payload.get("author_name")) or clean_text(
            bundle.post_payload.get("organization_name")
        )
        topic = clean_text(bundle.post_payload.get("topic")) or clean_text(bundle.post_payload.get("text"))
        cta_url = clean_text(bundle.post_payload.get("cta_url"))
        created_at = parse_datetime(bundle.post_payload.get("created_at"), fallback=datetime.now(UTC))

        return (
            author_name or "Unknown Organization",
            topic or "LinkedIn organization post",
            cta_url,
            created_at or datetime.now(UTC),
        )

    def _load_json(self, path: Path) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    def _assert_real_mode_configured(self) -> None:
        missing: list[str] = []
        if not self.organization_id:
            missing.append("LINKEDIN_ORGANIZATION_ID")
        if not self.access_token:
            missing.append("LINKEDIN_ACCESS_TOKEN")
        if missing:
            raise ValueError(
                "LinkedIn org API real mode requested but required credentials are missing: "
                + ", ".join(missing)
            )
