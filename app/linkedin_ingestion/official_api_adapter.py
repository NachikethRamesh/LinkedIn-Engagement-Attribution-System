from __future__ import annotations

from app.linkedin_ingestion.base import LinkedInAdapter
from app.integrations_config import get_linkedin_credentials
from app.linkedin_ingestion.types import AdapterBatch


class OfficialLinkedInAPIAdapter(LinkedInAdapter):
    """
    Placeholder adapter for future official LinkedIn API integration.

    TODO:
    - Implement OAuth authorization code flow and token refresh handling.
    - Define required LinkedIn product permissions/scopes for organization and member analytics.
    - Configure endpoint URLs per API product/version and environment.
    - Add rate limit handling, retries, and backoff policies.
    - Implement pagination/cursor traversal for large event streams.
    - Map raw API responses into canonical normalized records.
    """

    def __init__(self, organization_id: str, access_token: str | None = None) -> None:
        creds = get_linkedin_credentials()
        self.organization_id = organization_id or creds.organization_id
        self.access_token = access_token or creds.access_token

    def collect(self) -> AdapterBatch:
        raise NotImplementedError(
            "Official LinkedIn API adapter is a scaffold only. "
            "Implement OAuth + endpoint clients before use."
        )

    def fetch_posts(self):
        """TODO: call API endpoint(s) and return raw post payloads."""
        raise NotImplementedError

    def fetch_engagement_events(self):
        """TODO: call API endpoint(s) and return raw engagement payloads."""
        raise NotImplementedError

    def normalize_posts(self, raw_posts):
        """TODO: transform official API post payloads into canonical NormalizedPost records."""
        raise NotImplementedError

    def normalize_events(self, raw_events):
        """TODO: transform official API event payloads into canonical NormalizedSocialEvent records."""
        raise NotImplementedError
