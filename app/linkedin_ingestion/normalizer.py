from __future__ import annotations

from typing import Any

from app.linkedin_ingestion.validator import SENTINEL_TIMESTAMP, clean_text, parse_datetime


def _normalize_header(value: str) -> str:
    lowered = value.strip().lower()
    for char in (" ", "-", "/", "."):
        lowered = lowered.replace(char, "_")
    while "__" in lowered:
        lowered = lowered.replace("__", "_")
    return lowered


COMMON_ALIASES: dict[str, list[str]] = {
    "post_url": ["post_url", "post_link", "url", "message_url", "linkedin_post_url", "permalink"],
    "author_name": ["author_name", "author", "published_by", "creator", "owner"],
    "topic": ["topic", "theme", "subject", "post_topic", "content_pillar"],
    "cta_url": ["cta_url", "destination_url", "website_url", "link_url", "target_url"],
    "created_at": ["created_at", "post_date", "published_at", "publish_date", "post_created_at"],
    "event_type": ["event_type", "engagement_type", "activity_type", "interaction_type", "event"],
    "event_timestamp": ["event_timestamp", "event_time", "engaged_at", "activity_time", "timestamp", "time"],
    "actor_name": ["actor_name", "engaged_user", "person_name", "actor", "user_name"],
    "actor_linkedin_url": ["actor_linkedin_url", "engaged_user_url", "person_url", "actor_url", "profile_url"],
    "actor_company_raw": ["actor_company_raw", "company", "organization", "employer", "account_name"],
    "comment_text": ["comment_text", "comment", "comment_body", "comment_message", "message_text", "text"],
    "metric_count": ["metric_count", "count", "engagement_count", "total"],
}

PRESET_ALIASES: dict[str, dict[str, list[str]]] = {
    "shield": {
        "post_url": ["post_link"],
        "author_name": ["author"],
        "topic": ["topic"],
        "cta_url": ["cta_url"],
        "created_at": ["post_date"],
    },
    "sprout": {
        "post_url": ["message_url"],
        "author_name": ["published_by"],
        "topic": ["theme"],
        "cta_url": ["destination_url"],
        "created_at": ["published_at"],
        "event_type": ["engagement_type"],
        "event_timestamp": ["event_time"],
        "actor_name": ["engaged_user"],
        "actor_linkedin_url": ["engaged_user_url"],
        "actor_company_raw": ["company"],
    },
    "generic": {},
}

METRIC_COLUMN_ALIASES: dict[str, list[str]] = {
    "post_impression": ["impressions", "post_impressions", "impression_count", "views"],
    "post_like": ["likes", "like_count", "reactions", "reaction_count"],
    "post_comment": ["comments", "comment_count"],
    "post_repost": ["reposts", "shares", "share_count", "repost_count"],
    "post_link_click": ["post_link_clicks", "website_clicks", "link_clicks", "clicks", "website_click_count"],
}

EVENT_TYPE_SYNONYMS: dict[str, str] = {
    "impression": "post_impression",
    "post_impression": "post_impression",
    "view": "post_impression",
    "like": "post_like",
    "reaction": "post_like",
    "post_like": "post_like",
    "comment": "post_comment",
    "post_comment": "post_comment",
    "repost": "post_repost",
    "share": "post_repost",
    "post_repost": "post_repost",
    "post_link_click": "post_link_click",
    "link_click": "post_link_click",
    "website_click": "post_link_click",
    "website click": "post_link_click",
    "post link click": "post_link_click",
}


def map_event_type(value: str | None) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    key = text.lower().replace("-", "_").replace("/", "_")
    return EVENT_TYPE_SYNONYMS.get(key)


def parse_int(value: Any) -> int | None:
    text = clean_text(value)
    if text is None:
        return None
    cleaned = text.replace(",", "")
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def normalize_csv_row(
    row: dict[str, Any],
    source_name: str,
    row_id: str,
    mapping_override: dict[str, list[str]] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    warnings: list[str] = []

    normalized_row = {_normalize_header(key): value for key, value in row.items()}
    preset = PRESET_ALIASES.get(source_name, PRESET_ALIASES["generic"])

    def get_field(field_name: str) -> str | None:
        alias_candidates: list[str] = []
        if mapping_override and field_name in mapping_override:
            alias_candidates.extend(mapping_override[field_name])
        alias_candidates.extend(preset.get(field_name, []))
        alias_candidates.extend(COMMON_ALIASES.get(field_name, []))

        for alias in alias_candidates:
            value = normalized_row.get(_normalize_header(alias))
            text = clean_text(value)
            if text is not None:
                return text
        return None

    post_url = get_field("post_url")
    if post_url is None:
        warnings.append(f"row {row_id}: missing post URL")

    created_at = parse_datetime(get_field("created_at"))
    if created_at is None:
        warnings.append(f"row {row_id}: invalid created_at, using sentinel timestamp")
        created_at = SENTINEL_TIMESTAMP

    post_payload: dict[str, Any] = {
        "post_url": post_url,
        "author_name": get_field("author_name") or "Unknown Author",
        "topic": get_field("topic") or "LinkedIn Post",
        "cta_url": get_field("cta_url"),
        "created_at": created_at,
        "raw_row_id": row_id,
    }

    event_rows: list[dict[str, Any]] = []

    event_type = map_event_type(get_field("event_type"))
    base_event_timestamp = parse_datetime(get_field("event_timestamp"), fallback=created_at)
    actor_name = get_field("actor_name")
    actor_linkedin_url = get_field("actor_linkedin_url")
    actor_company_raw = get_field("actor_company_raw")
    comment_text = get_field("comment_text")

    explicit_metric_count = parse_int(get_field("metric_count"))
    if event_type is not None:
        event_rows.append(
            {
                "event_type": event_type,
                "event_timestamp": base_event_timestamp,
                "actor_name": actor_name,
                "actor_linkedin_url": actor_linkedin_url,
                "actor_company_raw": actor_company_raw,
                "comment_text": comment_text,
                "aggregated_import": explicit_metric_count is not None,
                "source_metric_count": explicit_metric_count,
            }
        )

    metric_rows_added = 0
    for metric_event_type, aliases in METRIC_COLUMN_ALIASES.items():
        metric_value = None
        for alias in aliases:
            metric_value = parse_int(normalized_row.get(_normalize_header(alias)))
            if metric_value is not None:
                break

        if metric_value is None or metric_value <= 0:
            continue

        metric_rows_added += 1
        event_rows.append(
            {
                "event_type": metric_event_type,
                "event_timestamp": base_event_timestamp,
                "actor_name": None,
                "actor_linkedin_url": None,
                "actor_company_raw": actor_company_raw,
                "comment_text": None,
                "aggregated_import": True,
                "source_metric_count": metric_value,
            }
        )

    if event_type is None and metric_rows_added == 0:
        warnings.append(f"row {row_id}: no recognizable event columns")

    return post_payload, event_rows, warnings
