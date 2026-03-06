from __future__ import annotations

from typing import Any

from app.writeback.adapters.base import BaseWritebackAdapter
from app.writeback.types import DeliveryResult


class CRMWritebackAdapter(BaseWritebackAdapter):
    target_type = "crm"

    def deliver(
        self,
        payload: dict[str, Any],
        endpoint_url: str | None,
        timeout_seconds: int = 15,
        **kwargs: Any,
    ) -> DeliveryResult:
        auth_headers = kwargs.get("auth_headers")
        if not endpoint_url:
            return DeliveryResult(
                status="success",
                response_json={
                    "delivery_mode": "stub",
                    "target_type": self.target_type,
                    "message": "No CRM endpoint configured; recorded deterministic stub success",
                },
            )
        return self._post_json_with_headers(
            endpoint_url=endpoint_url,
            payload=payload,
            timeout_seconds=timeout_seconds,
            headers=auth_headers if isinstance(auth_headers, dict) else None,
        )
