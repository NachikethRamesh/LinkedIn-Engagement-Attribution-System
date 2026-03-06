from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection
from app.linkedin_ingestion.url_ingestion import OrganizationPostURLIngestionService


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _run_ingestion_twice() -> None:
    service = OrganizationPostURLIngestionService()
    url = "https://www.linkedin.com/posts/<REDACTED_POST>"
    service.ingest(post_url=url, simulation_mode=True)
    service.ingest(post_url=url, simulation_mode=True)


def main() -> None:
    _run_ingestion_twice()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM social_engagement_events;")
            total_events = int(cur.fetchone()[0])
            _assert(total_events > 0, "no social_engagement_events inserted")

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_engagement_events
                WHERE actor_id IS NOT NULL
                  AND availability_status = 'actor_resolved'
                """
            )
            actor_resolved = int(cur.fetchone()[0])
            _assert(actor_resolved > 0, "no actor-resolved engagement events found")

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_engagement_events
                WHERE availability_status = 'aggregate_only'
                """
            )
            aggregate_only = int(cur.fetchone()[0])
            _assert(aggregate_only > 0, "no aggregate-only events found")

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_comments
                WHERE parent_platform_comment_id IS NOT NULL
                """
            )
            replies = int(cur.fetchone()[0])
            _assert(replies > 0, "no nested comment replies found in social_comments")

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_engagement_events
                WHERE platform_object_type = 'comment'
                  AND engagement_type = 'post_like'
                """
            )
            comment_reactions = int(cur.fetchone()[0])
            _assert(comment_reactions > 0, "no comment-level reaction events found")

            cur.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT dedupe_key, COUNT(*)
                    FROM social_engagement_events
                    GROUP BY dedupe_key
                    HAVING COUNT(*) > 1
                ) d
                """
            )
            duplicate_dedupe_keys = int(cur.fetchone()[0])
            _assert(duplicate_dedupe_keys == 0, "duplicate social_engagement_events dedupe keys found")

            cur.execute(
                """
                SELECT engagement_type, actor_resolution_status, availability_status, COUNT(*)
                FROM social_engagement_events
                GROUP BY engagement_type, actor_resolution_status, availability_status
                ORDER BY engagement_type, actor_resolution_status, availability_status
                """
            )
            breakdown = [
                {
                    "engagement_type": row[0],
                    "actor_resolution_status": row[1],
                    "availability_status": row[2],
                    "count": int(row[3]),
                }
                for row in cur.fetchall()
            ]

    print(
        json.dumps(
            {
                "status": "pass",
                "total_engagement_events": total_events,
                "actor_resolved_events": actor_resolved,
                "aggregate_only_events": aggregate_only,
                "nested_reply_rows": replies,
                "comment_reaction_events": comment_reactions,
                "breakdown": breakdown,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
