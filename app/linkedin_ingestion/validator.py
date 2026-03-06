from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from app.linkedin_ingestion.types import CANONICAL_EVENT_TYPES

SENTINEL_TIMESTAMP = datetime(1970, 1, 1, tzinfo=UTC)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_datetime(value: Any, fallback: datetime | None = None) -> datetime | None:
    if value is None:
        return fallback
    text = str(value).strip()
    if not text:
        return fallback

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for pattern in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y",
        ):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
        else:
            return fallback

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def validate_event_type(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in CANONICAL_EVENT_TYPES:
        return normalized
    return None


def ensure_post_url(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = normalize_linkedin_post_url(value)
    if normalized is None:
        return None
    return normalized


def build_original_columns(row: dict[str, Any]) -> list[str]:
    return [key for key, value in row.items() if clean_text(value) is not None]


def normalize_linkedin_post_url(value: str | None) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    if not text.lower().startswith(("http://", "https://")):
        return None

    split = urlsplit(text.strip())
    host = split.netloc.lower()
    if host.startswith("m.linkedin.com"):
        host = "www.linkedin.com"
    elif host == "linkedin.com":
        host = "www.linkedin.com"
    elif host.startswith("www.linkedin.com"):
        host = "www.linkedin.com"

    if "linkedin.com" not in host:
        return None

    path = split.path or ""
    while "//" in path:
        path = path.replace("//", "/")
    path = path.rstrip("/")
    if not path:
        return None

    return urlunsplit(("https", host, path, "", ""))


def resolve_actor_origin(
    source_name: str,
    aggregated_import: bool,
    actor_name: str | None,
    actor_linkedin_url: str | None,
) -> str:
    if source_name == "mock":
        return "mock_generated"
    if aggregated_import:
        return "aggregate_unknown"
    if clean_text(actor_name) is not None or clean_text(actor_linkedin_url) is not None:
        return "known"
    return "aggregate_unknown"
