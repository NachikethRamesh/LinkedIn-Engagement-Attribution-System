from __future__ import annotations

import re
import hashlib
from dataclasses import dataclass
from urllib.parse import urlsplit

from app.linkedin_ingestion.validator import normalize_linkedin_post_url


@dataclass(slots=True)
class ResolvedOrgPost:
    original_url: str
    normalized_url: str
    resolved_identifier: str
    resolution_mode: str


URN_PATTERN = re.compile(r"urn:li:(?:activity|share):([A-Za-z0-9:_-]+)", re.IGNORECASE)


def _is_supported_org_post_path(path: str) -> bool:
    lowered = path.lower()
    return lowered.startswith("/posts/") or lowered.startswith("/feed/update/")


def _extract_identifier_from_path(path: str) -> str | None:
    urn_match = URN_PATTERN.search(path)
    if urn_match:
        return f"urn:li:activity:{urn_match.group(1)}"

    lowered = path.lower()
    if not lowered.startswith("/posts/"):
        return None

    parts = [part for part in path.split("/") if part]
    if len(parts) < 2 or parts[0].lower() != "posts":
        return None

    slug = parts[1].strip()
    if not slug:
        return None
    return f"posts:{slug}"


def resolve_org_post_identifier(
    post_url: str,
    *,
    simulation_mode: bool = False,
    resolved_id_override: str | None = None,
) -> ResolvedOrgPost:
    normalized_url = normalize_linkedin_post_url(post_url)
    if normalized_url is None:
        raise ValueError("Invalid LinkedIn URL. Expected an absolute LinkedIn post URL.")

    split = urlsplit(normalized_url)
    if not _is_supported_org_post_path(split.path):
        raise ValueError(
            "Unsupported LinkedIn URL path. Expected organization post URL patterns like '/posts/...' or '/feed/update/...'."
        )

    identifier = resolved_id_override
    resolution_mode = "override" if resolved_id_override else "real"

    if identifier is None:
        identifier = _extract_identifier_from_path(split.path)

    if identifier is None:
        if not simulation_mode:
            raise ValueError(
                "Could not resolve organization post identifier from URL path. "
                "Use --simulation-mode or provide --resolved-id-override."
            )
        identifier = f"sim:{hashlib.sha1(normalized_url.encode('utf-8')).hexdigest()[:16]}"
        resolution_mode = "mock"

    return ResolvedOrgPost(
        original_url=post_url,
        normalized_url=normalized_url,
        resolved_identifier=identifier,
        resolution_mode=resolution_mode,
    )
