from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.config import load_environment


@dataclass(frozen=True, slots=True)
class LinkedInCredentials:
    organization_id: str | None
    client_id: str | None
    client_secret: str | None
    access_token: str | None


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if value and value.strip():
            return value.strip()
    return None


def endpoint_env_var(target_type: str) -> str:
    return {
        "crm": "WRITEBACK_CRM_URL",
        "clay": "WRITEBACK_CLAY_URL",
        "exa": "WRITEBACK_EXA_URL",
        "webhook_generic": "WRITEBACK_WEBHOOK_GENERIC_URL",
    }[target_type]


def get_writeback_endpoint(target_type: str, explicit_endpoint: str | None = None) -> str | None:
    load_environment()
    if explicit_endpoint and explicit_endpoint.strip():
        return explicit_endpoint.strip()
    return _first_non_empty(os.getenv(endpoint_env_var(target_type)))


def get_writeback_auth_headers(target_type: str) -> dict[str, str]:
    """
    Optional auth header material for writeback adapters.

    No header values are logged. Empty dict means unauthenticated delivery.
    """
    load_environment()
    headers: dict[str, str] = {}

    if target_type == "crm":
        api_key = _first_non_empty(os.getenv("CRM_API_KEY"), os.getenv("WRITEBACK_CRM_API_KEY"))
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
    elif target_type == "clay":
        api_key = _first_non_empty(os.getenv("CLAY_API_KEY"), os.getenv("WRITEBACK_CLAY_API_KEY"))
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
    elif target_type == "exa":
        api_key = _first_non_empty(
            os.getenv("EXA_API_KEY"),
            os.getenv("WRITEBACK_EXA_API_KEY"),
        )
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
    elif target_type == "webhook_generic":
        secret = _first_non_empty(os.getenv("WEBHOOK_GENERIC_SECRET"))
        header_name = _first_non_empty(os.getenv("WEBHOOK_GENERIC_SECRET_HEADER"), "X-Webhook-Secret")
        if secret and header_name:
            headers[header_name] = secret

    return headers


def get_linkedin_credentials() -> LinkedInCredentials:
    load_environment()
    return LinkedInCredentials(
        organization_id=_first_non_empty(os.getenv("LINKEDIN_ORGANIZATION_ID")),
        client_id=_first_non_empty(os.getenv("LINKEDIN_CLIENT_ID")),
        client_secret=_first_non_empty(os.getenv("LINKEDIN_CLIENT_SECRET")),
        access_token=_first_non_empty(os.getenv("LINKEDIN_ACCESS_TOKEN")),
    )


def summarize_integration_requirements(*, target_type: str, endpoint_url: str | None, simulate_local: bool) -> list[str]:
    """
    Returns warning strings only. Does not raise or hard-fail.
    """
    warnings: list[str] = []
    if simulate_local:
        return warnings

    if target_type in {"crm", "clay", "exa"} and endpoint_url:
        headers = get_writeback_auth_headers(target_type)
        if "Authorization" not in headers:
            expected = {
                "crm": "CRM_API_KEY",
                "clay": "CLAY_API_KEY",
                "exa": "EXA_API_KEY",
            }[target_type]
            warnings.append(
                f"{target_type} endpoint configured without auth header; set {expected} (optional for stub/local endpoints)."
            )

    if target_type == "webhook_generic" and endpoint_url:
        if not _first_non_empty(os.getenv("WEBHOOK_GENERIC_SECRET")):
            warnings.append(
                "webhook_generic endpoint configured without WEBHOOK_GENERIC_SECRET (optional but recommended)."
            )

    return warnings


def mask_secret(value: str | None) -> str:
    if not value:
        return "missing"
    if len(value) <= 6:
        return "***set***"
    return f"{value[:2]}***{value[-2:]}"


def collect_env_presence() -> dict[str, Any]:
    load_environment()
    creds = get_linkedin_credentials()
    frontend_env_file = Path(__file__).resolve().parent.parent / "frontend" / ".env"
    frontend_env_text = frontend_env_file.read_text(encoding="utf-8") if frontend_env_file.exists() else ""
    frontend_has_api_base = bool(os.getenv("VITE_API_BASE_URL")) or ("VITE_API_BASE_URL=" in frontend_env_text)

    return {
        "db": {
            "POSTGRES_HOST": bool(os.getenv("POSTGRES_HOST")),
            "POSTGRES_PORT": bool(os.getenv("POSTGRES_PORT")),
            "POSTGRES_DB": bool(os.getenv("POSTGRES_DB")),
            "POSTGRES_USER": bool(os.getenv("POSTGRES_USER")),
            "POSTGRES_PASSWORD": bool(os.getenv("POSTGRES_PASSWORD")),
        },
        "linkedin": {
            "LINKEDIN_ORGANIZATION_ID": bool(creds.organization_id),
            "LINKEDIN_CLIENT_ID": bool(creds.client_id),
            "LINKEDIN_CLIENT_SECRET": bool(creds.client_secret),
            "LINKEDIN_ACCESS_TOKEN": bool(creds.access_token),
        },
        "writeback_urls": {
            "WRITEBACK_CRM_URL": bool(os.getenv("WRITEBACK_CRM_URL")),
            "WRITEBACK_CLAY_URL": bool(os.getenv("WRITEBACK_CLAY_URL")),
            "WRITEBACK_EXA_URL": bool(os.getenv("WRITEBACK_EXA_URL")),
            "WRITEBACK_WEBHOOK_GENERIC_URL": bool(os.getenv("WRITEBACK_WEBHOOK_GENERIC_URL")),
        },
        "writeback_auth": {
            "CRM_API_KEY": bool(_first_non_empty(os.getenv("CRM_API_KEY"), os.getenv("WRITEBACK_CRM_API_KEY"))),
            "CLAY_API_KEY": bool(_first_non_empty(os.getenv("CLAY_API_KEY"), os.getenv("WRITEBACK_CLAY_API_KEY"))),
            "EXA_API_KEY": bool(_first_non_empty(os.getenv("EXA_API_KEY"), os.getenv("WRITEBACK_EXA_API_KEY"))),
            "WEBHOOK_GENERIC_SECRET": bool(os.getenv("WEBHOOK_GENERIC_SECRET")),
        },
        "frontend": {"VITE_API_BASE_URL": frontend_has_api_base},
    }
