from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC
from pathlib import Path

from psycopg2.extras import Json, execute_values

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection
from app.linkedin_ingestion.validator import clean_text, normalize_linkedin_post_url


def _hash(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill normalized actor-level social tables from existing posts/social_events.")
    parser.add_argument("--limit", type=int, default=0, help="Optional max social_event rows to backfill (0 = all).")
    args = parser.parse_args()

    with get_connection() as conn:
        with conn.cursor() as cur:
            limit_clause = "LIMIT %s" if args.limit > 0 else ""
            params = (args.limit,) if args.limit > 0 else ()
            cur.execute(
                f"""
                SELECT
                    se.id,
                    p.post_url,
                    p.author_name,
                    p.topic,
                    p.created_at,
                    se.actor_name,
                    se.actor_linkedin_url,
                    se.actor_company_raw,
                    se.event_type,
                    se.event_timestamp,
                    se.metadata_json
                FROM social_events se
                JOIN posts p ON p.id = se.post_id
                ORDER BY se.id
                {limit_clause}
                """,
                params,
            )
            rows = cur.fetchall()

            if not rows:
                print("no social_events found for backfill")
                return

            post_values = []
            post_keys: list[str] = []
            for row in rows:
                metadata = row[10] if isinstance(row[10], dict) else {}
                post_url = normalize_linkedin_post_url(row[1]) or row[1]
                platform_post_id = clean_text(metadata.get("resolved_org_post_identifier"))
                post_key = _hash("social_post", "linkedin", post_url, platform_post_id or "")
                post_keys.append(post_key)
                post_values.append(
                    (
                        "linkedin",
                        None,
                        None,
                        clean_text(metadata.get("source_name")) or "backfill",
                        "backfill-social-events",
                        platform_post_id,
                        None,
                        post_url,
                        clean_text(row[2]),
                        clean_text(metadata.get("organization_name")) or clean_text(row[2]),
                        clean_text(metadata.get("post_payload", {}).get("text")) if isinstance(metadata.get("post_payload"), dict) else clean_text(row[3]),
                        row[4],
                        Json(metadata),
                        post_key,
                    )
                )

            execute_values(
                cur,
                """
                INSERT INTO social_posts (
                    platform, workspace_id, tenant_id, sync_source, sync_job_id,
                    platform_post_id, platform_post_urn, post_url, author_name,
                    organization_name, text_content, post_created_at, raw_payload_json, dedupe_key
                ) VALUES %s
                ON CONFLICT (dedupe_key) DO NOTHING
                """,
                post_values,
            )
            cur.execute("SELECT id, dedupe_key, post_url FROM social_posts WHERE dedupe_key = ANY(%s)", (post_keys,))
            post_map = {row[2]: int(row[0]) for row in cur.fetchall()}

            actor_values = []
            actor_keys: list[str] = []
            for row in rows:
                actor_name = clean_text(row[5])
                actor_url = clean_text(row[6])
                if not actor_name and not actor_url:
                    continue
                actor_key = _hash("social_actor", "linkedin", "", "", actor_url or "", actor_name or "")
                actor_keys.append(actor_key)
                actor_values.append(
                    (
                        "linkedin",
                        None,
                        None,
                        actor_name,
                        actor_url,
                        None,
                        None,
                        clean_text(row[7]),
                        Json({"source_name": "backfill"}),
                        actor_key,
                    )
                )
            if actor_values:
                execute_values(
                    cur,
                    """
                    INSERT INTO social_engagement_actors (
                        platform, external_actor_id, actor_urn, display_name, profile_url,
                        headline, title, company_name, metadata_json, dedupe_key
                    ) VALUES %s
                    ON CONFLICT (dedupe_key) DO UPDATE SET
                        last_seen_at = NOW()
                    """,
                    actor_values,
                )
            cur.execute(
                "SELECT id, dedupe_key FROM social_engagement_actors WHERE dedupe_key = ANY(%s)",
                (actor_keys if actor_keys else ["_none_"],),
            )
            actor_map = {row[1]: int(row[0]) for row in cur.fetchall()}

            event_values = []
            for row in rows:
                metadata = row[10] if isinstance(row[10], dict) else {}
                post_url = normalize_linkedin_post_url(row[1]) or row[1]
                social_post_id = post_map.get(post_url)
                if not social_post_id:
                    continue
                actor_name = clean_text(row[5])
                actor_url = clean_text(row[6])
                actor_key = _hash("social_actor", "linkedin", "", "", actor_url or "", actor_name or "")
                actor_id = actor_map.get(actor_key)
                object_type = clean_text(metadata.get("platform_object_type")) or "post"
                object_id = (
                    clean_text(metadata.get("platform_object_id"))
                    or clean_text(metadata.get("resolved_org_post_identifier"))
                    or post_url
                )
                if not object_id:
                    continue
                aggregated_import = bool(metadata.get("aggregated_import", False))
                actor_resolution_status = "resolved" if actor_id else ("aggregate_only" if aggregated_import else "unresolved")
                availability_status = (
                    clean_text(metadata.get("availability_status"))
                    or ("actor_resolved" if actor_id else ("aggregate_only" if aggregated_import else "not_exposed"))
                )
                dedupe_key = _hash(
                    "social_engagement_event",
                    "linkedin",
                    clean_text(metadata.get("dedupe_key")) or str(row[0]),
                    row[8],
                    object_id,
                    row[9].astimezone(UTC).isoformat(),
                )
                event_values.append(
                    (
                        "linkedin",
                        None,
                        None,
                        clean_text(metadata.get("source_name")) or "backfill",
                        "backfill-social-events",
                        object_type,
                        object_id,
                        clean_text(metadata.get("parent_platform_object_id")),
                        social_post_id,
                        None,
                        actor_id,
                        actor_resolution_status,
                        row[8],
                        row[9],
                        Json(metadata.get("raw_payload", {})),
                        availability_status,
                        dedupe_key,
                    )
                )

            execute_values(
                cur,
                """
                INSERT INTO social_engagement_events (
                    platform, workspace_id, tenant_id, sync_source, sync_job_id,
                    platform_object_type, platform_object_id, parent_platform_object_id,
                    social_post_id, social_comment_id, actor_id, actor_resolution_status,
                    engagement_type, engagement_timestamp, raw_payload_json, availability_status, dedupe_key
                ) VALUES %s
                ON CONFLICT (dedupe_key) DO NOTHING
                """,
                event_values,
            )
        conn.commit()

    print(
        json.dumps(
            {
                "rows_read": len(rows),
                "social_posts_processed": len(post_values),
                "actors_processed": len(actor_values),
                "engagement_events_processed": len(event_values),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
