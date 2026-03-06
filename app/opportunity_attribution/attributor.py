from __future__ import annotations

import math
import os
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib import error, request

from psycopg2.extras import Json, execute_values

from app.config import load_environment
from app.db import get_connection
from app.opportunity_attribution.config import (
    AGGREGATED_DAMPENING,
    ALLOWED_WINDOWS,
    DEFAULT_WINDOW_DAYS,
    INFLUENCE_BANDS,
    MAX_AGGREGATED_INTENSITY,
    RECENCY_BUCKETS_30,
    RECENCY_BUCKETS_60,
    STRONG_SIGNAL_TYPES,
)
from app.intent_scoring.config import EVENT_WEIGHTS
from app.linkedin_ingestion.validator import clean_text


@dataclass(slots=True)
class OpportunityRecord:
    id: int
    account_id: int
    opportunity_name: str
    created_at: datetime


@dataclass(slots=True)
class MatchedEvent:
    post_id: int
    matched_contact_id: int | None
    match_confidence: float
    event_type: str
    event_timestamp: datetime
    actor_name: str | None
    actor_linkedin_url: str | None
    metadata_json: dict[str, Any]


@dataclass(slots=True)
class WebsiteEvent:
    event_timestamp: datetime


@dataclass(slots=True)
class AccountFunnelState:
    website_visited: bool
    website_last_visited_at: datetime | None
    outbound_replied: bool
    outbound_replied_at: datetime | None
    sales_process_started: bool
    sales_process_stage: str | None
    sales_process_started_at: datetime | None
    purchased_or_closed_won: bool
    purchased_at: datetime | None


class OpportunityAttributionService:
    def __init__(self) -> None:
        load_environment(override=True)
        self._summary_cache: dict[str, str] = {}
        self._gemini_enabled = (os.getenv("GEMINI_ATTRIBUTION_SUMMARY_ENABLED") or "true").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._gemini_api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
        self._gemini_model = (os.getenv("GEMINI_ATTRIBUTION_MODEL") or os.getenv("GEMINI_COMMENT_MODEL") or "gemini-2.0-flash").strip()
        self._gemini_base_url = (os.getenv("GEMINI_BASE_URL") or "https://generativelanguage.googleapis.com/v1beta").rstrip("/")
        self._gemini_timeout_seconds = int((os.getenv("GEMINI_ATTRIBUTION_TIMEOUT_SECONDS") or "12").strip())

    def run(self, rebuild: bool = False, window_days: int = DEFAULT_WINDOW_DAYS) -> dict[str, int]:
        if window_days not in ALLOWED_WINDOWS:
            raise ValueError(f"window_days must be one of {sorted(ALLOWED_WINDOWS)}")

        with get_connection() as conn:
            with conn.cursor() as cur:
                self._ensure_step4_schema(cur)
                if rebuild:
                    cur.execute("TRUNCATE TABLE opportunity_influence RESTART IDENTITY;")

                bootstrap_created = self._bootstrap_demo_opportunities_for_resolved_accounts(cur)
                opportunities = self._load_target_opportunities(cur)
                self._ensure_demo_funnel_state_for_accounts(
                    cur,
                    [opp.account_id for opp in opportunities],
                )
                rows = [self._attribute_opportunity(cur, opp, window_days) for opp in opportunities]
                written = self._upsert_rows(cur, rows)

            conn.commit()

        return {
            "opportunities_processed": len(rows),
            "rows_written": written,
            "window_days": window_days,
            "bootstrap_created": bootstrap_created,
        }

    def _ensure_step4_schema(self, cur) -> None:
        cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS website_visited BOOLEAN NOT NULL DEFAULT FALSE;")
        cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS website_last_visited_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS outbound_replied BOOLEAN NOT NULL DEFAULT FALSE;")
        cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS outbound_replied_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS sales_process_started BOOLEAN NOT NULL DEFAULT FALSE;")
        cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS sales_process_stage TEXT;")
        cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS sales_process_started_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS purchased_or_closed_won BOOLEAN NOT NULL DEFAULT FALSE;")
        cur.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS purchased_at TIMESTAMPTZ;")

        cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS funnel_path TEXT NOT NULL DEFAULT 'not_yet_engaged';")
        cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS commercial_progression_flag TEXT;")
        cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS opportunity_score NUMERIC(5,2);")
        cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS action_priority TEXT;")
        cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS recommended_next_action TEXT;")
        cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS gemini_summary TEXT;")

    def _bootstrap_demo_opportunities_for_resolved_accounts(self, cur) -> int:
        """
        Local-demo safety net:
        If there are no opportunities at all, create one deterministic demo opportunity
        per resolved account so Step 4 can run end-to-end from the frontend.
        """
        cur.execute("SELECT COUNT(*) FROM opportunities")
        existing_total = int(cur.fetchone()[0])
        if existing_total > 0:
            return 0

        cur.execute(
            """
            SELECT DISTINCT sem.matched_account_id, a.company_name
            FROM social_event_matches sem
            JOIN accounts a ON a.id = sem.matched_account_id
            WHERE sem.matched_account_id IS NOT NULL
              AND sem.match_type NOT IN ('unresolved', 'skipped_aggregate_import')
            ORDER BY sem.matched_account_id
            """
        )
        rows = cur.fetchall()
        if not rows:
            return 0

        now = datetime.now(UTC)
        values: list[tuple[Any, ...]] = []
        for account_id, company_name in rows:
            amount = Decimal(str(25000 + (int(account_id) * 750)))
            values.append(
                (
                    int(account_id),
                    f"{company_name} - Demo Opportunity",
                    "pipeline",
                    amount,
                    now,
                )
            )

        execute_values(
            cur,
            """
            INSERT INTO opportunities (
                account_id,
                opportunity_name,
                stage,
                amount,
                created_at
            ) VALUES %s
            """,
            values,
        )
        return len(values)

    def _load_target_opportunities(self, cur) -> list[OpportunityRecord]:
        cur.execute(
            """
            SELECT o.id, o.account_id, o.opportunity_name, o.created_at
            FROM opportunities o
            LEFT JOIN opportunity_influence oi ON oi.opportunity_id = o.id
            WHERE oi.opportunity_id IS NULL
            ORDER BY o.created_at, o.id
            """
        )
        return [OpportunityRecord(id=r[0], account_id=r[1], opportunity_name=r[2], created_at=r[3]) for r in cur.fetchall()]

    def _attribute_opportunity(self, cur, opportunity: OpportunityRecord, window_days: int) -> tuple[Any, ...]:
        lookback_start = opportunity.created_at - timedelta(days=window_days)

        matched_events = self._load_matched_events(cur, opportunity.account_id, lookback_start, opportunity.created_at)
        website_events = self._load_website_events(cur, opportunity.account_id, lookback_start, opportunity.created_at)
        intent_snapshot = self._load_intent_snapshot(cur, opportunity.account_id, lookback_start, opportunity.created_at)
        funnel_state = self._load_funnel_state(cur, opportunity.account_id)

        social_event_points = 0.0
        match_confidences: list[float] = []
        stakeholder_keys: set[str] = set()
        strong_signal_count = 0
        aggregate_signal_count = 0
        matched_posts: set[int] = set()
        event_type_counts = Counter()
        weighted_by_type = defaultdict(float)
        last_social_touch_at: datetime | None = None

        for event in matched_events:
            event_weight = EVENT_WEIGHTS.get(event.event_type, 0.0)
            if event_weight <= 0:
                continue

            days = max((opportunity.created_at - event.event_timestamp).days, 0)
            recency = self._recency_multiplier(days, window_days)
            if recency <= 0:
                continue

            points = event_weight * recency
            is_aggregated = bool(event.metadata_json.get("aggregated_import", False))
            if is_aggregated:
                aggregate_signal_count += 1
                count = self._safe_int(event.metadata_json.get("source_metric_count"), 1)
                intensity = min(math.log2(max(count, 1) + 1), MAX_AGGREGATED_INTENSITY)
                points = points * intensity * AGGREGATED_DAMPENING
            else:
                stakeholder_key = self._stakeholder_key(event)
                if stakeholder_key:
                    stakeholder_keys.add(stakeholder_key)

            social_event_points += points
            matched_posts.add(event.post_id)
            event_type_counts[event.event_type] += 1
            weighted_by_type[event.event_type] += points
            match_confidences.append(event.match_confidence)

            if event.event_type in STRONG_SIGNAL_TYPES and not is_aggregated:
                strong_signal_count += 1

            if last_social_touch_at is None or event.event_timestamp > last_social_touch_at:
                last_social_touch_at = event.event_timestamp
            comment_analysis = event.metadata_json.get("comment_analysis") if isinstance(event.metadata_json, dict) else None
            if isinstance(comment_analysis, dict):
                sentiment = str(comment_analysis.get("sentiment") or "").lower()
                intent = str(comment_analysis.get("intent") or "").lower()
                conf = self._safe_float(comment_analysis.get("confidence"), 0.0)
                if sentiment == "positive":
                    social_event_points += 0.35 * conf
                elif sentiment == "negative":
                    social_event_points -= 0.25 * conf
                if intent == "high":
                    social_event_points += 0.75 * conf
                elif intent == "medium":
                    social_event_points += 0.30 * conf

        website_points = 0.0
        website_signal_count = 0
        for website_event in website_events:
            days = max((opportunity.created_at - website_event.event_timestamp).days, 0)
            recency = self._recency_multiplier(days, window_days)
            if recency <= 0:
                continue
            website_signal_count += 1
            website_points += 2.0 * recency
        website_points = min(website_points, 12.0)

        unique_stakeholder_count = len(stakeholder_keys)
        stakeholder_bonus = 0.0
        if unique_stakeholder_count >= 3:
            stakeholder_bonus = 8.0
        elif unique_stakeholder_count == 2:
            stakeholder_bonus = 4.0

        sequence_bonus = 0.0
        if matched_events and website_events:
            first_social = min(e.event_timestamp for e in matched_events)
            if any(w.event_timestamp > first_social for w in website_events):
                sequence_bonus = 4.0

        strong_signal_bonus = 0.0
        if strong_signal_count >= 3:
            strong_signal_bonus = 6.0
        elif strong_signal_count >= 1:
            strong_signal_bonus = 2.0

        intent_score_value = float(intent_snapshot["score"]) if intent_snapshot else 0.0
        intent_component = min((intent_score_value / 100.0) * 25.0, 25.0)

        proximity_bonus = 0.0
        days_from_last_social_touch_to_opp: int | None = None
        if last_social_touch_at is not None:
            days_from_last_social_touch_to_opp = max((opportunity.created_at - last_social_touch_at).days, 0)
            if days_from_last_social_touch_to_opp <= 3:
                proximity_bonus = 8.0
            elif days_from_last_social_touch_to_opp <= 7:
                proximity_bonus = 5.0
            elif days_from_last_social_touch_to_opp <= 14:
                proximity_bonus = 2.0

        aggregate_ratio = aggregate_signal_count / max(len(matched_events), 1)
        aggregate_dampening_adjustment = 0.0
        if aggregate_ratio > 0.50:
            aggregate_dampening_adjustment = -6.0
        elif aggregate_ratio > 0.25:
            aggregate_dampening_adjustment = -3.0

        pre_confidence_score = (
            social_event_points
            + website_points
            + stakeholder_bonus
            + strong_signal_bonus
            + sequence_bonus
            + intent_component
            + proximity_bonus
            + aggregate_dampening_adjustment
        )

        influence_score = round(max(0.0, min(100.0, pre_confidence_score)), 2)
        had_website_event = website_signal_count > 0
        website_visited = bool(funnel_state.website_visited) or had_website_event
        progression_flag = self._progression_flag(
            website_visited=website_visited,
            outbound_replied=bool(funnel_state.outbound_replied),
            sales_process_started=bool(funnel_state.sales_process_started),
            purchased_or_closed_won=bool(funnel_state.purchased_or_closed_won),
        )
        funnel_path = "already_engaged" if progression_flag is not None else "not_yet_engaged"

        opportunity_score: float | None = None
        action_priority: str
        recommended_next_action: str
        if funnel_path == "already_engaged":
            influence_score = self._progression_to_score(progression_flag)
            action_priority, recommended_next_action = self._path_a_action(progression_flag)
        else:
            opportunity_score = self._path_b_opportunity_score(
                intent_score_value=intent_score_value,
                social_event_points=social_event_points,
                unique_stakeholder_count=unique_stakeholder_count,
                strong_signal_count=strong_signal_count,
                website_signal_count=website_signal_count,
                days_from_last_social_touch_to_opp=days_from_last_social_touch_to_opp,
                aggregate_ratio=aggregate_ratio,
                account_id=opportunity.account_id,
                cur=cur,
            )
            influence_score = opportunity_score
            action_priority, recommended_next_action = self._path_b_action(opportunity_score)

        influence_band = self._band(influence_score)
        influenced = influence_band != "none"

        confidence = self._confidence(
            match_confidences=match_confidences,
            aggregate_ratio=aggregate_ratio,
            signal_count=len(match_confidences),
            stakeholder_count=unique_stakeholder_count,
            has_intent_snapshot=intent_snapshot is not None,
        )

        strongest_signal_type = None
        if weighted_by_type:
            strongest_signal_type = max(weighted_by_type.items(), key=lambda item: item[1])[0]

        notes = self._notes(
            opportunity_name=opportunity.opportunity_name,
            window_days=window_days,
            unique_stakeholders=unique_stakeholder_count,
            strong_signal_count=strong_signal_count,
            website_signal_count=website_signal_count,
            influence_band=influence_band,
            intent_score=intent_score_value,
            funnel_path=funnel_path,
            progression_flag=progression_flag,
            action_priority=action_priority,
            recommended_next_action=recommended_next_action,
        )
        gemini_summary = self._generate_gemini_summary(
            company_name=opportunity.opportunity_name.replace(" - Demo Opportunity", ""),
            funnel_path=funnel_path,
            progression_flag=progression_flag,
            intent_score=intent_score_value,
            influence_score=influence_score,
            strongest_signal_type=strongest_signal_type,
            unique_stakeholder_count=unique_stakeholder_count,
            action_priority=action_priority,
            recommended_next_action=recommended_next_action,
        )

        breakdown = {
            "lookback_days": window_days,
            "social_event_points": round(social_event_points, 2),
            "website_points": round(website_points, 2),
            "stakeholder_bonus": stakeholder_bonus,
            "strong_signal_bonus": strong_signal_bonus,
            "sequence_bonus": sequence_bonus,
            "intent_score_component": round(intent_component, 2),
            "proximity_bonus": proximity_bonus,
            "aggregate_dampening_adjustment": aggregate_dampening_adjustment,
            "aggregate_signal_count": aggregate_signal_count,
            "aggregate_signal_ratio": round(aggregate_ratio, 4),
            "included_event_counts_by_type": dict(event_type_counts),
            "intent_snapshot": intent_snapshot,
            "funnel_state": {
                "website_visited": website_visited,
                "outbound_replied": bool(funnel_state.outbound_replied),
                "sales_process_started": bool(funnel_state.sales_process_started),
                "sales_process_stage": funnel_state.sales_process_stage,
                "purchased_or_closed_won": bool(funnel_state.purchased_or_closed_won),
            },
            "funnel_path": funnel_path,
            "commercial_progression_flag": progression_flag,
            "opportunity_score": opportunity_score,
            "action_priority": action_priority,
            "recommended_next_action": recommended_next_action,
            "lookback_start": lookback_start.isoformat(),
            "opportunity_created_at": opportunity.created_at.isoformat(),
        }

        return (
            opportunity.id,
            opportunity.account_id,
            Decimal(str(influence_score)),
            influence_band,
            influenced,
            window_days,
            len(matched_events),
            len(matched_posts),
            unique_stakeholder_count,
            website_signal_count,
            Decimal(str(round(intent_score_value, 2))) if intent_snapshot else None,
            strongest_signal_type,
            last_social_touch_at,
            days_from_last_social_touch_to_opp,
            Decimal(str(confidence)),
            funnel_path,
            progression_flag,
            Decimal(str(round(opportunity_score, 2))) if opportunity_score is not None else None,
            action_priority,
            recommended_next_action,
            gemini_summary,
            notes,
            Json(breakdown),
        )

    def _load_matched_events(self, cur, account_id: int, start: datetime, end: datetime) -> list[MatchedEvent]:
        cur.execute(
            """
            SELECT
                se.post_id,
                sem.matched_contact_id,
                sem.match_confidence,
                se.event_type,
                se.event_timestamp,
                se.actor_name,
                se.actor_linkedin_url,
                se.metadata_json
            FROM social_event_matches sem
            JOIN social_events se ON se.id = sem.social_event_id
            WHERE sem.matched_account_id = %s
              AND sem.match_type NOT IN ('unresolved', 'skipped_aggregate_import')
              AND se.event_timestamp >= %s
              AND se.event_timestamp <= %s
            """,
            (account_id, start, end),
        )

        events: list[MatchedEvent] = []
        for row in cur.fetchall():
            metadata = row[7] if isinstance(row[7], dict) else {}
            events.append(
                MatchedEvent(
                    post_id=row[0],
                    matched_contact_id=row[1],
                    match_confidence=float(row[2]),
                    event_type=row[3],
                    event_timestamp=row[4],
                    actor_name=row[5],
                    actor_linkedin_url=row[6],
                    metadata_json=metadata,
                )
            )
        return events

    def _load_website_events(self, cur, account_id: int, start: datetime, end: datetime) -> list[WebsiteEvent]:
        cur.execute(
            """
            SELECT event_timestamp
            FROM website_events
            WHERE account_id = %s
              AND event_timestamp >= %s
              AND event_timestamp <= %s
            """,
            (account_id, start, end),
        )
        return [WebsiteEvent(event_timestamp=r[0]) for r in cur.fetchall()]

    def _load_intent_snapshot(self, cur, account_id: int, start: datetime, end: datetime) -> dict[str, Any] | None:
        cur.execute(
            """
            SELECT score_date, score_window, score, confidence
            FROM account_intent_scores
            WHERE account_id = %s
              AND score_date >= %s::date
              AND score_date <= %s::date
            ORDER BY
              CASE WHEN score_window = 'rolling_30d' THEN 0 ELSE 1 END,
              score_date DESC
            LIMIT 1
            """,
            (account_id, start, end),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "score_date": row[0].isoformat(),
            "score_window": row[1],
            "score": float(row[2]),
            "confidence": float(row[3]),
        }

    def _upsert_rows(self, cur, rows: list[tuple[Any, ...]]) -> int:
        if not rows:
            return 0

        query = """
            INSERT INTO opportunity_influence (
                opportunity_id,
                account_id,
                influence_score,
                influence_band,
                influenced,
                influence_window_days,
                matched_event_count,
                matched_post_count,
                unique_stakeholder_count,
                website_signal_count,
                intent_score_snapshot,
                strongest_signal_type,
                last_social_touch_at,
                days_from_last_social_touch_to_opp,
                confidence,
                funnel_path,
                commercial_progression_flag,
                opportunity_score,
                action_priority,
                recommended_next_action,
                gemini_summary,
                notes,
                score_breakdown_json
            ) VALUES %s
            ON CONFLICT (opportunity_id) DO UPDATE
            SET
                account_id = EXCLUDED.account_id,
                influence_score = EXCLUDED.influence_score,
                influence_band = EXCLUDED.influence_band,
                influenced = EXCLUDED.influenced,
                influence_window_days = EXCLUDED.influence_window_days,
                matched_event_count = EXCLUDED.matched_event_count,
                matched_post_count = EXCLUDED.matched_post_count,
                unique_stakeholder_count = EXCLUDED.unique_stakeholder_count,
                website_signal_count = EXCLUDED.website_signal_count,
                intent_score_snapshot = EXCLUDED.intent_score_snapshot,
                strongest_signal_type = EXCLUDED.strongest_signal_type,
                last_social_touch_at = EXCLUDED.last_social_touch_at,
                days_from_last_social_touch_to_opp = EXCLUDED.days_from_last_social_touch_to_opp,
                confidence = EXCLUDED.confidence,
                funnel_path = EXCLUDED.funnel_path,
                commercial_progression_flag = EXCLUDED.commercial_progression_flag,
                opportunity_score = EXCLUDED.opportunity_score,
                action_priority = EXCLUDED.action_priority,
                recommended_next_action = EXCLUDED.recommended_next_action,
                gemini_summary = EXCLUDED.gemini_summary,
                notes = EXCLUDED.notes,
                score_breakdown_json = EXCLUDED.score_breakdown_json
        """
        execute_values(cur, query, rows)
        return len(rows)

    def _band(self, score: float) -> str:
        for threshold, band in INFLUENCE_BANDS:
            if score >= threshold:
                return band
        return "none"

    def _confidence(
        self,
        match_confidences: list[float],
        aggregate_ratio: float,
        signal_count: int,
        stakeholder_count: int,
        has_intent_snapshot: bool,
    ) -> float:
        if signal_count == 0 or not match_confidences:
            return 0.25

        avg = sum(match_confidences) / len(match_confidences)
        conf = avg

        if stakeholder_count >= 2:
            conf += 0.08
        if signal_count >= 5:
            conf += 0.05
        if has_intent_snapshot:
            conf += 0.04

        if aggregate_ratio > 0.50:
            conf -= 0.15
        elif aggregate_ratio > 0.25:
            conf -= 0.08

        conf = max(0.10, min(0.98, conf))
        return round(conf, 2)

    def _recency_multiplier(self, days_ago: int, window_days: int) -> float:
        buckets = RECENCY_BUCKETS_60 if window_days > 30 else RECENCY_BUCKETS_30
        for max_days, mult in buckets:
            if days_ago <= max_days:
                return mult
        return 0.0

    def _stakeholder_key(self, event: MatchedEvent) -> str | None:
        if event.matched_contact_id is not None:
            return f"contact:{event.matched_contact_id}"

        actor_url = clean_text(event.actor_linkedin_url)
        if actor_url:
            return f"linkedin:{actor_url.lower()}"

        actor_name = clean_text(event.actor_name)
        if actor_name:
            return f"name:{actor_name.lower()}"

        return None

    def _safe_int(self, value: Any, fallback: int) -> int:
        if value is None:
            return fallback
        try:
            parsed = int(value)
            return parsed if parsed > 0 else fallback
        except (TypeError, ValueError):
            return fallback

    def _safe_float(self, value: Any, fallback: float) -> float:
        if value is None:
            return fallback
        try:
            return float(value)
        except (TypeError, ValueError):
            return fallback

    def _load_funnel_state(self, cur, account_id: int) -> AccountFunnelState:
        cur.execute(
            """
            SELECT
                website_visited,
                website_last_visited_at,
                outbound_replied,
                outbound_replied_at,
                sales_process_started,
                sales_process_stage,
                sales_process_started_at,
                purchased_or_closed_won,
                purchased_at
            FROM accounts
            WHERE id = %s
            """,
            (account_id,),
        )
        row = cur.fetchone()
        if not row:
            return AccountFunnelState(False, None, False, None, False, None, None, False, None)
        return AccountFunnelState(
            website_visited=bool(row[0]),
            website_last_visited_at=row[1],
            outbound_replied=bool(row[2]),
            outbound_replied_at=row[3],
            sales_process_started=bool(row[4]),
            sales_process_stage=row[5],
            sales_process_started_at=row[6],
            purchased_or_closed_won=bool(row[7]),
            purchased_at=row[8],
        )

    def _ensure_demo_funnel_state_for_accounts(self, cur, account_ids: list[int]) -> None:
        ids = sorted({int(x) for x in account_ids if x is not None})
        if not ids:
            return

        cur.execute(
            """
            SELECT
                id,
                website_visited,
                outbound_replied,
                sales_process_started,
                purchased_or_closed_won,
                sales_process_stage
            FROM accounts
            WHERE id = ANY(%s)
            """,
            (ids,),
        )
        rows = cur.fetchall()
        now = datetime.now(UTC)
        updates: list[tuple[Any, ...]] = []
        for row in rows:
            account_id = int(row[0])
            has_existing_state = bool(row[1]) or bool(row[2]) or bool(row[3]) or bool(row[4]) or bool(row[5])
            if has_existing_state:
                continue

            mod = account_id % 5
            website_visited = mod in (0, 1, 2, 3)
            outbound_replied = mod in (0, 1, 2)
            sales_process_started = mod in (0, 1)
            purchased = mod == 0
            sales_stage = "proposal" if sales_process_started and not purchased else ("closed_won" if purchased else None)

            website_last = now - timedelta(days=7 + (account_id % 3)) if website_visited else None
            outbound_at = now - timedelta(days=5 + (account_id % 2)) if outbound_replied else None
            sales_started_at = now - timedelta(days=3) if sales_process_started else None
            purchased_at = now - timedelta(days=1) if purchased else None

            updates.append(
                (
                    website_visited,
                    website_last,
                    outbound_replied,
                    outbound_at,
                    sales_process_started,
                    sales_stage,
                    sales_started_at,
                    purchased,
                    purchased_at,
                    account_id,
                )
            )

        if not updates:
            return

        execute_values(
            cur,
            """
            UPDATE accounts AS a
            SET
                website_visited = v.website_visited::boolean,
                website_last_visited_at = v.website_last_visited_at::timestamptz,
                outbound_replied = v.outbound_replied::boolean,
                outbound_replied_at = v.outbound_replied_at::timestamptz,
                sales_process_started = v.sales_process_started::boolean,
                sales_process_stage = v.sales_process_stage,
                sales_process_started_at = v.sales_process_started_at::timestamptz,
                purchased_or_closed_won = v.purchased_or_closed_won::boolean,
                purchased_at = v.purchased_at::timestamptz
            FROM (VALUES %s) AS v(
                website_visited,
                website_last_visited_at,
                outbound_replied,
                outbound_replied_at,
                sales_process_started,
                sales_process_stage,
                sales_process_started_at,
                purchased_or_closed_won,
                purchased_at,
                id
            )
            WHERE a.id = v.id
            """,
            updates,
        )

    def _progression_flag(
        self,
        website_visited: bool,
        outbound_replied: bool,
        sales_process_started: bool,
        purchased_or_closed_won: bool,
    ) -> str | None:
        if purchased_or_closed_won:
            return "Purchased"
        if sales_process_started:
            return "In Sales Process"
        if outbound_replied:
            return "Replied to Outbound"
        if website_visited:
            return "Visited Website"
        return None

    def _progression_to_score(self, progression_flag: str | None) -> float:
        score_map = {
            "Purchased": 95.0,
            "In Sales Process": 84.0,
            "Replied to Outbound": 68.0,
            "Visited Website": 52.0,
        }
        return score_map.get(progression_flag or "", 45.0)

    def _path_a_action(self, progression_flag: str | None) -> tuple[str, str]:
        if progression_flag == "Purchased":
            return ("high", "Customer expansion/cross-sell handoff")
        if progression_flag == "In Sales Process":
            return ("high", "Prioritize sales follow-up with social proof")
        if progression_flag == "Replied to Outbound":
            return ("medium", "AE follow-up within 24h with tailored value props")
        return ("medium", "Nudge to meeting with personalized outreach")

    def _path_b_opportunity_score(
        self,
        intent_score_value: float,
        social_event_points: float,
        unique_stakeholder_count: int,
        strong_signal_count: int,
        website_signal_count: int,
        days_from_last_social_touch_to_opp: int | None,
        aggregate_ratio: float,
        account_id: int,
        cur,
    ) -> float:
        intent_component = min(intent_score_value, 100.0) * 0.45
        engagement_component = min(max(social_event_points, 0.0), 24.0) * 1.2
        stakeholder_component = 10.0 if unique_stakeholder_count >= 3 else (6.0 if unique_stakeholder_count == 2 else 2.0)
        strong_signal_component = 8.0 if strong_signal_count >= 3 else (4.0 if strong_signal_count >= 1 else 0.0)
        website_component = min(float(website_signal_count) * 1.5, 6.0)

        recency_component = 0.0
        if days_from_last_social_touch_to_opp is not None:
            if days_from_last_social_touch_to_opp <= 3:
                recency_component = 12.0
            elif days_from_last_social_touch_to_opp <= 7:
                recency_component = 8.0
            elif days_from_last_social_touch_to_opp <= 14:
                recency_component = 4.0
            else:
                recency_component = 1.0

        cur.execute(
            """
            SELECT COUNT(*)
            FROM accounts
            WHERE id = %s
              AND crm_account_id LIKE 'exa_sim:%%'
            """,
            (account_id,),
        )
        exa_relevance_component = 4.0 if int(cur.fetchone()[0]) > 0 else 0.0

        aggregate_penalty = -6.0 if aggregate_ratio > 0.50 else (-3.0 if aggregate_ratio > 0.25 else 0.0)

        score = (
            intent_component
            + engagement_component
            + stakeholder_component
            + strong_signal_component
            + website_component
            + recency_component
            + exa_relevance_component
            + aggregate_penalty
        )
        return round(max(0.0, min(100.0, score)), 2)

    def _path_b_action(self, opportunity_score: float) -> tuple[str, str]:
        if opportunity_score >= 70:
            return ("high", "Reach out now (personalized outbound email + LinkedIn follow-up)")
        if opportunity_score >= 45:
            return ("medium", "Add to warm outbound sequence")
        if opportunity_score >= 25:
            return ("medium", "Monitor and nurture with targeted content")
        return ("low", "Low priority for now")

    def _generate_gemini_summary(
        self,
        company_name: str,
        funnel_path: str,
        progression_flag: str | None,
        intent_score: float,
        influence_score: float,
        strongest_signal_type: str | None,
        unique_stakeholder_count: int,
        action_priority: str,
        recommended_next_action: str,
    ) -> str:
        fallback = (
            f"{company_name}: {('Path A' if funnel_path == 'already_engaged' else 'Path B')} | "
            f"intent {intent_score:.1f}, score {influence_score:.1f}, strongest {strongest_signal_type or 'n/a'}, "
            f"stakeholders {unique_stakeholder_count}. Next: {recommended_next_action}."
        )
        cache_key = "|".join(
            [
                company_name,
                funnel_path,
                str(progression_flag or ""),
                f"{intent_score:.2f}",
                f"{influence_score:.2f}",
                strongest_signal_type or "",
                str(unique_stakeholder_count),
                action_priority,
                recommended_next_action,
            ]
        )
        if cache_key in self._summary_cache:
            return self._summary_cache[cache_key]

        if not self._gemini_enabled or not self._gemini_api_key:
            self._summary_cache[cache_key] = fallback
            return fallback

        path_label = "Path A (Already Engaged in Funnel)" if funnel_path == "already_engaged" else "Path B (Not Yet Engaged in Funnel)"
        prompt = (
            "Write one concise GTM operator summary (max 35 words). "
            "Do not invent facts.\n"
            f"Account: {company_name}\n"
            f"Path: {path_label}\n"
            f"Progression: {progression_flag or 'none'}\n"
            f"Intent score: {intent_score:.2f}\n"
            f"Opportunity score: {influence_score:.2f}\n"
            f"Strongest signal: {strongest_signal_type or 'unknown'}\n"
            f"Stakeholder breadth: {unique_stakeholder_count}\n"
            f"Priority: {action_priority}\n"
            f"Recommended action: {recommended_next_action}\n"
        )
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.1},
        }
        req = request.Request(
            f"{self._gemini_base_url}/models/{self._gemini_model}:generateContent?key={self._gemini_api_key}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self._gemini_timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
            parsed = json.loads(raw)
            text = str(parsed["candidates"][0]["content"]["parts"][0]["text"]).strip()
            summary = clean_text(text)[:320] if text else fallback
        except (error.URLError, error.HTTPError, KeyError, ValueError, json.JSONDecodeError):
            summary = fallback
        self._summary_cache[cache_key] = summary
        return summary

    def _notes(
        self,
        opportunity_name: str,
        window_days: int,
        unique_stakeholders: int,
        strong_signal_count: int,
        website_signal_count: int,
        influence_band: str,
        intent_score: float,
        funnel_path: str,
        progression_flag: str | None,
        action_priority: str,
        recommended_next_action: str,
    ) -> str:
        return (
            f"{opportunity_name}: {influence_band} influence based on {unique_stakeholders} stakeholders, "
            f"{strong_signal_count} strong social signals, {website_signal_count} website signals, "
            f"intent snapshot {intent_score:.2f} within {window_days}d pre-opportunity window; "
            f"path={funnel_path}, progression={progression_flag or 'none'}, "
            f"priority={action_priority}, action={recommended_next_action}"
        )
