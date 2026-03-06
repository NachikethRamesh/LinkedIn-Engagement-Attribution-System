from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path

from psycopg2.extras import Json, execute_values

from app.db import get_connection
from app.linkedin_ingestion.types import AdapterBatch, ImportStats, NormalizedPost, NormalizedSocialEvent
from app.linkedin_ingestion.validator import clean_text, normalize_linkedin_post_url, resolve_actor_origin


class LinkedInAdapter(ABC):
    @abstractmethod
    def collect(self) -> AdapterBatch:
        """Collect normalized posts and events from a source."""


class LinkedInIngestionService:
    def ingest_batch(self, batch: AdapterBatch, source_name: str, filename: str, import_mode: str) -> ImportStats:
        stats = ImportStats(
            source_name=source_name,
            filename=Path(filename).name,
            import_mode=import_mode,
            row_count=batch.row_count,
            skip_count=batch.skipped_rows,
            warnings=list(batch.warnings),
        )

        stats.warning_count = len(stats.warnings)

        with get_connection() as conn:
            with conn.cursor() as cur:
                posts_created, posts_updated, post_id_lookup = self._upsert_posts(cur, batch.posts)
                events_inserted = self._insert_events(cur, batch.events, post_id_lookup)
                actor_events_synced = self._sync_actor_level_social_model(
                    cur=cur,
                    batch=batch,
                    source_name=source_name,
                    filename=filename,
                    import_mode=import_mode,
                )
                success_count = max(batch.row_count - batch.skipped_rows, 0)

                stats.posts_created = posts_created
                stats.posts_updated = posts_updated
                stats.events_inserted = events_inserted
                stats.success_count = success_count

                warning_preview = "; ".join(
                    f"{warning.row_id}:{warning.message}" for warning in stats.warnings[:3]
                )
                notes = (
                    f"rows={batch.row_count}, success={success_count}, skipped={batch.skipped_rows}, "
                    f"warnings={stats.warning_count}, posts_in_batch={len(batch.posts)}, events_in_batch={len(batch.events)}, "
                    f"posts_created={posts_created}, posts_updated={posts_updated}, events_inserted={events_inserted}, "
                    f"actor_events_synced={actor_events_synced}"
                )
                if warning_preview:
                    notes = f"{notes}, warning_preview={warning_preview}"
                self._write_import_log(cur, stats, notes)

            conn.commit()

        return stats

    def _upsert_posts(self, cur, posts: list[NormalizedPost]) -> tuple[int, int, dict[str, int]]:
        if not posts:
            return 0, 0, {}

        deduped_posts: dict[str, NormalizedPost] = {}
        for post in posts:
            normalized_post_url = normalize_linkedin_post_url(post.post_url)
            if normalized_post_url is None:
                continue
            deduped_posts[normalized_post_url] = NormalizedPost(
                post_url=normalized_post_url,
                author_name=post.author_name,
                topic=post.topic,
                cta_url=post.cta_url,
                created_at=post.created_at,
                source_name=post.source_name,
                raw_payload_json=post.raw_payload_json,
            )

        post_urls = list(deduped_posts.keys())
        if not post_urls:
            return 0, 0, {}

        cur.execute("SELECT post_url FROM posts WHERE post_url = ANY(%s);", (post_urls,))
        existing_urls = {row[0] for row in cur.fetchall()}

        values = [
            (
                post.post_url,
                post.author_name,
                post.topic,
                post.cta_url,
                post.created_at,
            )
            for post in deduped_posts.values()
        ]

        query = """
            INSERT INTO posts (post_url, author_name, topic, cta_url, created_at)
            VALUES %s
            ON CONFLICT (post_url) DO UPDATE
            SET
                author_name = EXCLUDED.author_name,
                topic = EXCLUDED.topic,
                cta_url = EXCLUDED.cta_url,
                created_at = LEAST(posts.created_at, EXCLUDED.created_at)
        """
        execute_values(cur, query, values)

        cur.execute("SELECT id, post_url FROM posts WHERE post_url = ANY(%s);", (post_urls,))
        post_id_lookup = {row[1]: row[0] for row in cur.fetchall()}

        posts_updated = len(existing_urls)
        posts_created = len(post_urls) - posts_updated
        return posts_created, posts_updated, post_id_lookup

    def _insert_events(self, cur, events: list[NormalizedSocialEvent], post_id_lookup: dict[str, int]) -> int:
        if not events:
            return 0

        values = []
        for event in events:
            normalized_post_url = normalize_linkedin_post_url(event.post_url)
            if normalized_post_url is None:
                continue

            post_id = post_id_lookup.get(normalized_post_url)
            if post_id is None:
                continue

            import_timestamp = datetime.now(UTC).isoformat()
            metadata = dict(event.metadata_json)
            metadata_actor_origin = str(
                metadata.get(
                    "actor_origin",
                    resolve_actor_origin(
                        source_name=event.source_name,
                        aggregated_import=event.aggregated_import,
                        actor_name=event.actor_name,
                        actor_linkedin_url=event.actor_linkedin_url,
                    ),
                )
            )
            metadata.update(
                {
                    "source_name": event.source_name,
                    "import_mode": event.import_mode,
                    "aggregated_import": event.aggregated_import,
                    "actor_origin": metadata_actor_origin,
                    "import_timestamp": import_timestamp,
                }
            )

            dedupe_key = self._build_dedupe_key(
                post_id=post_id,
                event_type=event.event_type,
                event_timestamp=event.event_timestamp,
                actor_name=event.actor_name,
                actor_linkedin_url=event.actor_linkedin_url,
                source_name=event.source_name,
                import_mode=event.import_mode,
                raw_row_id=str(metadata.get("raw_row_id", "")),
                source_metric_count=str(metadata.get("source_metric_count", "")),
                aggregated_import=event.aggregated_import,
                actor_company_raw=event.actor_company_raw,
                actor_origin=metadata_actor_origin,
            )
            metadata["dedupe_key"] = dedupe_key

            values.append(
                (
                    post_id,
                    event.actor_name,
                    event.actor_linkedin_url,
                    event.actor_company_raw,
                    event.event_type,
                    event.event_timestamp,
                    Json(metadata),
                )
            )

        if not values:
            return 0

        query = """
            INSERT INTO social_events (
                post_id,
                actor_name,
                actor_linkedin_url,
                actor_company_raw,
                event_type,
                event_timestamp,
                metadata_json
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """
        execute_values(cur, query, values)
        return cur.rowcount if cur.rowcount > 0 else 0

    def _write_import_log(self, cur, stats: ImportStats, notes: str) -> None:
        cur.execute(
            """
            INSERT INTO imports_log (
                source_name,
                filename,
                import_mode,
                imported_at,
                row_count,
                success_count,
                skip_count,
                warning_count,
                notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                stats.source_name,
                stats.filename,
                stats.import_mode,
                datetime.now(UTC),
                stats.row_count,
                stats.success_count,
                stats.skip_count,
                stats.warning_count,
                notes,
            ),
        )

    def _build_dedupe_key(
        self,
        post_id: int,
        event_type: str,
        event_timestamp: datetime,
        actor_name: str | None,
        actor_linkedin_url: str | None,
        source_name: str,
        import_mode: str,
        raw_row_id: str,
        source_metric_count: str,
        aggregated_import: bool,
        actor_company_raw: str | None,
        actor_origin: str,
    ) -> str:
        payload = "|".join(
            [
                str(post_id),
                event_type,
                event_timestamp.isoformat(),
                clean_text(actor_name) or "",
                clean_text(actor_linkedin_url) or "",
                clean_text(actor_company_raw) or "",
                source_name,
                import_mode,
                raw_row_id,
                source_metric_count,
                str(aggregated_import),
                actor_origin,
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _sync_actor_level_social_model(
        self,
        *,
        cur,
        batch: AdapterBatch,
        source_name: str,
        filename: str,
        import_mode: str,
    ) -> int:
        if not batch.posts and not batch.events:
            return 0

        sync_job_id = hashlib.sha256(
            f"{source_name}|{Path(filename).name}|{import_mode}|{len(batch.posts)}|{len(batch.events)}".encode("utf-8")
        ).hexdigest()[:16]

        social_posts = self._upsert_social_posts(cur=cur, posts=batch.posts, sync_job_id=sync_job_id, source_name=source_name)
        actor_map = self._upsert_social_actors(cur=cur, events=batch.events)
        comments = self._upsert_social_comments(cur=cur, events=batch.events, social_posts=social_posts, actor_map=actor_map)
        self._upsert_metrics_snapshots(cur=cur, batch=batch, social_posts=social_posts)
        return self._upsert_social_engagement_events(
            cur=cur,
            events=batch.events,
            social_posts=social_posts,
            social_comments=comments,
            actor_map=actor_map,
            sync_job_id=sync_job_id,
            source_name=source_name,
            import_mode=import_mode,
        )

    def _upsert_social_posts(self, *, cur, posts: list[NormalizedPost], sync_job_id: str, source_name: str) -> dict[str, int]:
        if not posts:
            return {}

        values = []
        dedupe_keys: list[str] = []
        for post in posts:
            post_url = normalize_linkedin_post_url(post.post_url)
            if post_url is None:
                continue
            platform_post_id = clean_text(post.platform_post_id) or clean_text(post.raw_payload_json.get("resolved_org_post_identifier"))
            dedupe_key = self._hash(
                "social_post",
                "linkedin",
                post_url,
                platform_post_id or "",
            )
            dedupe_keys.append(dedupe_key)
            values.append(
                (
                    "linkedin",
                    post.workspace_id,
                    post.tenant_id,
                    source_name,
                    post.sync_job_id or sync_job_id,
                    platform_post_id,
                    clean_text(post.platform_post_urn),
                    post_url,
                    clean_text(post.author_name),
                    clean_text(post.raw_payload_json.get("organization_name")) or clean_text(post.author_name),
                    clean_text(post.raw_payload_json.get("text")) or clean_text(post.topic),
                    post.created_at,
                    Json(post.raw_payload_json),
                    dedupe_key,
                )
            )

        if not values:
            return {}

        execute_values(
            cur,
            """
            INSERT INTO social_posts (
                platform,
                workspace_id,
                tenant_id,
                sync_source,
                sync_job_id,
                platform_post_id,
                platform_post_urn,
                post_url,
                author_name,
                organization_name,
                text_content,
                post_created_at,
                raw_payload_json,
                dedupe_key
            ) VALUES %s
            ON CONFLICT (dedupe_key) DO UPDATE SET
                workspace_id = COALESCE(EXCLUDED.workspace_id, social_posts.workspace_id),
                tenant_id = COALESCE(EXCLUDED.tenant_id, social_posts.tenant_id),
                sync_source = EXCLUDED.sync_source,
                sync_job_id = EXCLUDED.sync_job_id,
                platform_post_id = COALESCE(EXCLUDED.platform_post_id, social_posts.platform_post_id),
                platform_post_urn = COALESCE(EXCLUDED.platform_post_urn, social_posts.platform_post_urn),
                author_name = COALESCE(EXCLUDED.author_name, social_posts.author_name),
                organization_name = COALESCE(EXCLUDED.organization_name, social_posts.organization_name),
                text_content = COALESCE(EXCLUDED.text_content, social_posts.text_content),
                post_created_at = LEAST(social_posts.post_created_at, EXCLUDED.post_created_at),
                raw_payload_json = EXCLUDED.raw_payload_json,
                updated_at = NOW()
            """,
            values,
        )
        cur.execute("SELECT id, dedupe_key FROM social_posts WHERE dedupe_key = ANY(%s)", (dedupe_keys,))
        return {row[1]: int(row[0]) for row in cur.fetchall()}

    def _upsert_social_actors(self, *, cur, events: list[NormalizedSocialEvent]) -> dict[str, int]:
        values = []
        actor_keys: list[str] = []
        for event in events:
            has_actor = clean_text(event.actor_name) or clean_text(event.actor_linkedin_url) or clean_text(event.actor_external_id)
            if not has_actor:
                continue
            actor_key = self._hash(
                "social_actor",
                event.platform,
                clean_text(event.actor_external_id) or "",
                clean_text(event.actor_urn) or "",
                clean_text(event.actor_linkedin_url) or "",
                clean_text(event.actor_name) or "",
            )
            actor_keys.append(actor_key)
            values.append(
                (
                    event.platform,
                    clean_text(event.actor_external_id),
                    clean_text(event.actor_urn),
                    clean_text(event.actor_name),
                    clean_text(event.actor_linkedin_url),
                    clean_text(event.actor_headline),
                    clean_text(event.actor_title),
                    clean_text(event.actor_company) or clean_text(event.actor_company_raw),
                    Json({"source_name": event.source_name, "import_mode": event.import_mode}),
                    actor_key,
                )
            )

        if not values:
            return {}

        execute_values(
            cur,
            """
            INSERT INTO social_engagement_actors (
                platform,
                external_actor_id,
                actor_urn,
                display_name,
                profile_url,
                headline,
                title,
                company_name,
                metadata_json,
                dedupe_key
            ) VALUES %s
            ON CONFLICT (dedupe_key) DO UPDATE SET
                display_name = COALESCE(EXCLUDED.display_name, social_engagement_actors.display_name),
                profile_url = COALESCE(EXCLUDED.profile_url, social_engagement_actors.profile_url),
                headline = COALESCE(EXCLUDED.headline, social_engagement_actors.headline),
                title = COALESCE(EXCLUDED.title, social_engagement_actors.title),
                company_name = COALESCE(EXCLUDED.company_name, social_engagement_actors.company_name),
                metadata_json = EXCLUDED.metadata_json,
                last_seen_at = NOW()
            """,
            values,
        )
        cur.execute("SELECT id, dedupe_key FROM social_engagement_actors WHERE dedupe_key = ANY(%s)", (actor_keys,))
        return {row[1]: int(row[0]) for row in cur.fetchall()}

    def _upsert_social_comments(
        self,
        *,
        cur,
        events: list[NormalizedSocialEvent],
        social_posts: dict[str, int],
        actor_map: dict[str, int],
    ) -> dict[str, int]:
        comment_rows = []
        comment_dedupe_keys: list[str] = []
        comment_parent_map: dict[str, str | None] = {}
        for event in events:
            if event.platform_object_type not in {"comment", "reply"} or event.event_type != "post_comment":
                continue
            post_key = self._hash(
                "social_post",
                "linkedin",
                normalize_linkedin_post_url(event.post_url) or "",
                clean_text(event.metadata_json.get("resolved_org_post_identifier")) or clean_text(event.platform_object_id) or "",
            )
            social_post_id = social_posts.get(post_key)
            if social_post_id is None:
                continue
            platform_comment_id = clean_text(event.platform_object_id)
            if platform_comment_id is None:
                continue
            parent_platform_comment_id = (
                clean_text(event.parent_platform_object_id)
                if event.platform_object_type == "reply"
                else None
            )
            actor_key = self._hash(
                "social_actor",
                event.platform,
                clean_text(event.actor_external_id) or "",
                clean_text(event.actor_urn) or "",
                clean_text(event.actor_linkedin_url) or "",
                clean_text(event.actor_name) or "",
            )
            actor_id = actor_map.get(actor_key)
            dedupe_key = self._hash("social_comment", event.platform, str(social_post_id), platform_comment_id)
            comment_dedupe_keys.append(dedupe_key)
            comment_parent_map[dedupe_key] = parent_platform_comment_id
            comment_rows.append(
                (
                    event.platform,
                    social_post_id,
                    platform_comment_id,
                    parent_platform_comment_id,
                    1 if parent_platform_comment_id else 0,
                    actor_id,
                    clean_text(event.raw_payload_json.get("text")) if isinstance(event.raw_payload_json, dict) else None,
                    event.event_timestamp,
                    Json(event.raw_payload_json if isinstance(event.raw_payload_json, dict) else {}),
                    dedupe_key,
                )
            )

        if not comment_rows:
            return {}

        execute_values(
            cur,
            """
            INSERT INTO social_comments (
                platform,
                social_post_id,
                platform_comment_id,
                parent_platform_comment_id,
                depth,
                actor_id,
                comment_text,
                comment_created_at,
                raw_payload_json,
                dedupe_key
            ) VALUES %s
            ON CONFLICT (dedupe_key) DO UPDATE SET
                parent_platform_comment_id = COALESCE(EXCLUDED.parent_platform_comment_id, social_comments.parent_platform_comment_id),
                actor_id = COALESCE(EXCLUDED.actor_id, social_comments.actor_id),
                comment_text = COALESCE(EXCLUDED.comment_text, social_comments.comment_text),
                raw_payload_json = EXCLUDED.raw_payload_json,
                updated_at = NOW()
            """,
            comment_rows,
        )
        cur.execute(
            "SELECT id, dedupe_key, platform_comment_id FROM social_comments WHERE dedupe_key = ANY(%s)",
            (comment_dedupe_keys,),
        )
        rows = cur.fetchall()
        dedupe_to_id = {row[1]: int(row[0]) for row in rows}
        comment_id_by_platform_id = {row[2]: int(row[0]) for row in rows}

        update_rows = []
        for dedupe_key, parent_platform_comment_id in comment_parent_map.items():
            if not parent_platform_comment_id:
                continue
            comment_id = dedupe_to_id.get(dedupe_key)
            parent_id = comment_id_by_platform_id.get(parent_platform_comment_id)
            if comment_id and parent_id:
                update_rows.append((parent_id, comment_id))
        if update_rows:
            execute_values(
                cur,
                """
                UPDATE social_comments AS c
                SET parent_comment_id = v.parent_id
                FROM (VALUES %s) AS v(parent_id, comment_id)
                WHERE c.id = v.comment_id
                """,
                update_rows,
            )
        return comment_id_by_platform_id

    def _upsert_metrics_snapshots(self, *, cur, batch: AdapterBatch, social_posts: dict[str, int]) -> None:
        post_snapshot_values = []
        post_snapshot_keys: list[str] = []
        for post in batch.posts:
            post_url = normalize_linkedin_post_url(post.post_url)
            post_key = self._hash(
                "social_post",
                "linkedin",
                post_url or "",
                clean_text(post.platform_post_id) or clean_text(post.raw_payload_json.get("resolved_org_post_identifier")) or "",
            )
            social_post_id = social_posts.get(post_key)
            if not social_post_id:
                continue
            post_payload = post.raw_payload_json.get("post_payload") if isinstance(post.raw_payload_json, dict) else None
            counts = post_payload.get("counts") if isinstance(post_payload, dict) else None
            if not isinstance(counts, dict):
                continue
            snapshot_key = self._hash("post_metrics_snapshot", str(social_post_id), json.dumps(counts, sort_keys=True))
            post_snapshot_keys.append(snapshot_key)
            post_snapshot_values.append(
                (
                    "linkedin",
                    social_post_id,
                    datetime.now(UTC),
                    self._to_int(counts.get("reactions")),
                    self._to_int(counts.get("comments")),
                    self._to_int(counts.get("shares")),
                    self._to_int(counts.get("impressions")),
                    self._to_int(counts.get("reach")),
                    self._to_int(counts.get("clicks")),
                    Json({"counts": counts}),
                    snapshot_key,
                )
            )
        if post_snapshot_values:
            execute_values(
                cur,
                """
                INSERT INTO social_post_metrics_snapshots (
                    platform,
                    social_post_id,
                    snapshot_timestamp,
                    reaction_count,
                    comment_count,
                    repost_count,
                    impression_count,
                    reach_count,
                    click_count,
                    raw_payload_json,
                    dedupe_key
                ) VALUES %s
                ON CONFLICT (dedupe_key) DO NOTHING
                """,
                post_snapshot_values,
            )

    def _upsert_social_engagement_events(
        self,
        *,
        cur,
        events: list[NormalizedSocialEvent],
        social_posts: dict[str, int],
        social_comments: dict[str, int],
        actor_map: dict[str, int],
        sync_job_id: str,
        source_name: str,
        import_mode: str,
    ) -> int:
        values = []
        for event in events:
            post_url = normalize_linkedin_post_url(event.post_url)
            post_key = self._hash(
                "social_post",
                "linkedin",
                post_url or "",
                clean_text(event.metadata_json.get("resolved_org_post_identifier"))
                or clean_text(event.platform_object_id)
                or "",
            )
            social_post_id = social_posts.get(post_key)
            if social_post_id is None:
                continue

            actor_key = self._hash(
                "social_actor",
                event.platform,
                clean_text(event.actor_external_id) or "",
                clean_text(event.actor_urn) or "",
                clean_text(event.actor_linkedin_url) or "",
                clean_text(event.actor_name) or "",
            )
            actor_id = actor_map.get(actor_key)
            comment_id = social_comments.get(clean_text(event.platform_object_id) or "")
            object_id = clean_text(event.platform_object_id) or clean_text(event.metadata_json.get("resolved_org_post_identifier")) or post_url
            if object_id is None:
                continue

            actor_resolution_status = (
                "resolved"
                if actor_id is not None
                else ("aggregate_only" if event.aggregated_import else "unresolved")
            )
            availability_status = clean_text(event.availability_status) or (
                "aggregate_only" if event.aggregated_import else ("actor_resolved" if actor_id else "not_exposed")
            )
            event_identifier = clean_text(event.event_external_id) or clean_text(event.metadata_json.get("dedupe_key"))
            dedupe_key = self._hash(
                "social_engagement_event",
                event.platform,
                event_identifier or "",
                event.event_type,
                object_id,
                clean_text(event.parent_platform_object_id) or "",
                event.event_timestamp.isoformat(),
                clean_text(event.actor_external_id) or clean_text(event.actor_linkedin_url) or clean_text(event.actor_name) or "",
            )
            values.append(
                (
                    event.platform,
                    event.workspace_id,
                    event.tenant_id,
                    source_name,
                    event.sync_job_id or sync_job_id,
                    clean_text(event.platform_object_type) or "post",
                    object_id,
                    clean_text(event.parent_platform_object_id),
                    social_post_id,
                    comment_id,
                    actor_id,
                    actor_resolution_status,
                    event.event_type,
                    event.event_timestamp,
                    Json(event.raw_payload_json if isinstance(event.raw_payload_json, dict) else {}),
                    availability_status,
                    dedupe_key,
                )
            )

        if not values:
            return 0

        execute_values(
            cur,
            """
            INSERT INTO social_engagement_events (
                platform,
                workspace_id,
                tenant_id,
                sync_source,
                sync_job_id,
                platform_object_type,
                platform_object_id,
                parent_platform_object_id,
                social_post_id,
                social_comment_id,
                actor_id,
                actor_resolution_status,
                engagement_type,
                engagement_timestamp,
                raw_payload_json,
                availability_status,
                dedupe_key
            ) VALUES %s
            ON CONFLICT (dedupe_key) DO UPDATE SET
                social_post_id = COALESCE(EXCLUDED.social_post_id, social_engagement_events.social_post_id),
                social_comment_id = COALESCE(EXCLUDED.social_comment_id, social_engagement_events.social_comment_id),
                actor_id = COALESCE(EXCLUDED.actor_id, social_engagement_events.actor_id),
                actor_resolution_status = EXCLUDED.actor_resolution_status,
                raw_payload_json = EXCLUDED.raw_payload_json,
                availability_status = EXCLUDED.availability_status,
                updated_at = NOW()
            """,
            values,
        )
        return cur.rowcount if cur.rowcount > 0 else 0

    def _hash(self, *parts: str) -> str:
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

    def _to_int(self, value: object) -> int | None:
        text = clean_text(value)
        if text is None:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
