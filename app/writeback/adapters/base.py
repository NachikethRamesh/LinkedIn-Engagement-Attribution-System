from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from app.writeback.types import DeliveryResult


class BaseWritebackAdapter:
    target_type: str = "base"

    def deliver(
        self,
        payload: dict[str, Any],
        endpoint_url: str | None,
        timeout_seconds: int = 15,
        **kwargs: Any,
    ) -> DeliveryResult:
        raise NotImplementedError

    def _post_json(self, endpoint_url: str, payload: dict[str, Any], timeout_seconds: int) -> DeliveryResult:
        return self._post_json_with_headers(
            endpoint_url=endpoint_url,
            payload=payload,
            timeout_seconds=timeout_seconds,
            headers=None,
        )

    def _post_json_with_headers(
        self,
        *,
        endpoint_url: str,
        payload: dict[str, Any],
        timeout_seconds: int,
        headers: dict[str, str] | None,
    ) -> DeliveryResult:
        body = json.dumps(payload).encode("utf-8")
        request_headers = {"Content-Type": "application/json"}
        if headers:
            request_headers.update(headers)
        req = urllib.request.Request(
            url=endpoint_url,
            data=body,
            method="POST",
            headers=request_headers,
        )

        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                status_code = int(resp.getcode())
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            return DeliveryResult(
                status="failed",
                response_json={"status_code": int(exc.code), "body": error_body},
                error_message=f"HTTPError {exc.code}",
            )
        except urllib.error.URLError as exc:
            return DeliveryResult(
                status="failed",
                response_json={"error": str(exc.reason)},
                error_message=f"URLError: {exc.reason}",
            )
        except Exception as exc:  # pragma: no cover - defensive
            return DeliveryResult(
                status="failed",
                response_json={"error": str(exc)},
                error_message=f"Delivery exception: {exc}",
            )

        parsed_body: Any
        try:
            parsed_body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed_body = {"raw_body": raw}

        if 200 <= status_code < 300:
            return DeliveryResult(
                status="success",
                response_json={"status_code": status_code, "body": parsed_body},
            )

        return DeliveryResult(
            status="failed",
            response_json={"status_code": status_code, "body": parsed_body},
            error_message=f"Unexpected HTTP status {status_code}",
        )
