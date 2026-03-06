from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from psycopg2.extras import Json, execute_values

from app.db import get_connection
from app.intent_scoring.config import (
    AGGREGATED_DAMPENING,
    CRM_MATCH_BONUS,
    EVENT_WEIGHTS,
    MAX_AGGREGATED_INTENSITY,
    RECENCY_BUCKETS,
    STAKEHOLDER_BONUS_BY_COUNT,
    STRONG_SIGNAL_TYPES,
    TIER_BONUS,
    WINDOW_DAYS,
)
from app.linkedin_ingestion.comment_ai import CommentAnalysisService
from app.linkedin_ingestion.validator import clean_text


@dataclass(slots=True)
class MatchedSocialSignal:
    account_id: int
    matched_contact_id: int | None
    match_confidence: float
    event_type: str
    event_timestamp: datetime
    actor_name: str | None
    actor_linkedin_url: str | None
    metadata_json: dict[str, Any]


@dataclass(slots=True)
class WebsiteSignal:
    account_id: int
    event_timestamp: datetime


@dataclass(slots=True)
class AccountContext:
    account_id: int
    company_name: str
    domain: str | None
    target_tier: str


@dataclass(slots=True)
class ExaResearchSignal:
    account_id: int
    received_at: datetime


class IntentScoringService:
    def run(self, rebuild: bool = False) -> dict[str, int]:
        score_date = date.today()
        now = datetime.now(UTC)

        with get_connection() as conn:
            with conn.cursor() as cur:
                if rebuild:
                    cur.execute("TRUNCATE TABLE account_intent_scores RESTART IDENTITY;")

                comment_analysis_updated = self._refresh_comment_analysis_for_resolved_comments(cur)
                accounts = self._load_accounts(cur)
                social_signals = self._load_matched_social_signals(cur, now)
                website_signals = self._load_website_signals(cur, now)
                exa_signals = self._load_exa_signals(cur, now, accounts)
                resolved_account_ids = set(social_signals.keys())
                resolved_accounts = [a for a in accounts if a.account_id in resolved_account_ids]
                rows = self._compute_rows(
                    resolved_accounts,
                    social_signals,
                    website_signals,
                    exa_signals,
                    score_date,
                    now,
                )
                inserted_or_updated = self._upsert_scores(cur, rows)

            conn.commit()

        return {
            "rows_computed": len(rows),
            "rows_written": inserted_or_updated,
            "windows": len(WINDOW_DAYS),
            "accounts": len(resolved_accounts),
            "comment_analysis_updated": comment_analysis_updated,
        }

    def _refresh_comment_analysis_for_resolved_comments(self, cur) -> int:
        """
        Re-run Gemini comment analysis at scoring time for resolved comment events.
        This guarantees freshest analysis for Step 3 regardless of ingestion timing.
        """
        cur.execute(
            """
            SELECT DISTINCT
                se.id,
                se.metadata_json
            FROM social_event_matches sem
            JOIN social_events se ON se.id = sem.social_event_id
            WHERE sem.matched_account_id IS NOT NULL
              AND sem.match_type NOT IN ('unresolved', 'skipped_aggregate_import')
              AND se.event_type = 'post_comment'
              AND COALESCE((se.metadata_json->>'aggregated_import')::boolean, false) = false
            ORDER BY se.id
            """
        )
        rows = cur.fetchall()
        if not rows:
            return 0

        analyzer = CommentAnalysisService()
        updates: list[tuple[Json, int]] = []
        for row in rows:
            event_id = int(row[0])
            metadata = row[1] if isinstance(row[1], dict) else {}
            comment_text = clean_text(metadata.get("comment_text"))
            if not comment_text:
                continue

            analysis = analyzer.analyze(comment_text)
            metadata["comment_analysis"] = {
                "sentiment": analysis.sentiment,
                "intent": analysis.intent,
                "confidence": analysis.confidence,
                "summary": analysis.summary,
                "source": analysis.source,
                "analyzed_at": datetime.now(UTC).isoformat(),
            }
            updates.append((Json(metadata), event_id))

        if not updates:
            return 0

        execute_values(
            cur,
            """
            UPDATE social_events AS se
            SET metadata_json = v.metadata_json::jsonb
            FROM (VALUES %s) AS v(metadata_json, id)
            WHERE se.id = v.id
            """,
            updates,
        )
        return len(updates)

    def _load_accounts(self, cur) -> list[AccountContext]:
        cur.execute("SELECT id, company_name, domain, target_tier FROM accounts ORDER BY id;")
        return [AccountContext(account_id=r[0], company_name=r[1], domain=r[2], target_tier=r[3]) for r in cur.fetchall()]

    def _load_matched_social_signals(self, cur, now: datetime) -> dict[int, list[MatchedSocialSignal]]:
        min_time = now - timedelta(days=max(WINDOW_DAYS.values()))
        cur.execute(
            """
            SELECT
                sem.matched_account_id,
                sem.matched_contact_id,
                sem.match_confidence,
                se.event_type,
                se.event_timestamp,
                se.actor_name,
                se.actor_linkedin_url,
                se.metadata_json
            FROM social_event_matches sem
            JOIN social_events se ON se.id = sem.social_event_id
            WHERE sem.matched_account_id IS NOT NULL
              AND sem.match_type NOT IN ('unresolved', 'skipped_aggregate_import')
              AND se.event_timestamp >= %s
            """,
            (min_time,),
        )

        grouped: dict[int, list[MatchedSocialSignal]] = defaultdict(list)
        for row in cur.fetchall():
            metadata = row[7] if isinstance(row[7], dict) else {}
            grouped[row[0]].append(
                MatchedSocialSignal(
                    account_id=row[0],
                    matched_contact_id=row[1],
                    match_confidence=float(row[2]),
                    event_type=row[3],
                    event_timestamp=row[4],
                    actor_name=row[5],
                    actor_linkedin_url=row[6],
                    metadata_json=metadata,
                )
            )
        return grouped

    def _load_website_signals(self, cur, now: datetime) -> dict[int, list[WebsiteSignal]]:
        min_time = now - timedelta(days=max(WINDOW_DAYS.values()))
        cur.execute(
            """
            SELECT account_id, event_timestamp
            FROM website_events
            WHERE account_id IS NOT NULL
              AND event_timestamp >= %s
            """,
            (min_time,),
        )

        grouped: dict[int, list[WebsiteSignal]] = defaultdict(list)
        for row in cur.fetchall():
            grouped[row[0]].append(WebsiteSignal(account_id=row[0], event_timestamp=row[1]))
        return grouped

    def _load_exa_signals(
        self,
        cur,
        now: datetime,
        accounts: list[AccountContext],
    ) -> dict[int, list[ExaResearchSignal]]:
        min_time = now - timedelta(days=max(WINDOW_DAYS.values()))
        cur.execute(
            """
            SELECT normalized_data_json, received_at
            FROM enrichment_results
            WHERE target_type = 'exa'
              AND entity_type = 'unresolved_account_candidate'
              AND received_at >= %s
            ORDER BY received_at DESC
            """,
            (min_time,),
        )
        rows = cur.fetchall()

        account_by_domain: dict[str, int] = {}
        account_by_name: dict[str, int] = {}
        for account in accounts:
            if account.domain:
                account_by_domain[account.domain.strip().lower()] = account.account_id
            normalized_name = self._normalize_company(account.company_name)
            if normalized_name:
                account_by_name[normalized_name] = account.account_id

        grouped: dict[int, list[ExaResearchSignal]] = defaultdict(list)
        for normalized_data, received_at in rows:
            data = normalized_data if isinstance(normalized_data, dict) else {}
            domain = clean_text(data.get("likely_domain"))
            company = clean_text(data.get("likely_company_name"))

            account_id: int | None = None
            if domain:
                account_id = account_by_domain.get(domain.lower())
            if account_id is None and company:
                account_id = account_by_name.get(self._normalize_company(company))
            if account_id is None:
                continue

            grouped[account_id].append(ExaResearchSignal(account_id=account_id, received_at=received_at))

        return grouped

    def _compute_rows(
        self,
        accounts: list[AccountContext],
        social_signals: dict[int, list[MatchedSocialSignal]],
        website_signals: dict[int, list[WebsiteSignal]],
        exa_signals: dict[int, list[ExaResearchSignal]],
        score_date: date,
        now: datetime,
    ) -> list[tuple[Any, ...]]:
        rows: list[tuple[Any, ...]] = []

        for account in accounts:
            account_social = social_signals.get(account.account_id, [])
            account_website = website_signals.get(account.account_id, [])
            account_exa = exa_signals.get(account.account_id, [])

            for window_name, days in WINDOW_DAYS.items():
                window_social = [s for s in account_social if self._days_ago(s.event_timestamp, now) <= days]
                window_website = [w for w in account_website if self._days_ago(w.event_timestamp, now) <= days]
                window_exa = [x for x in account_exa if self._days_ago(x.received_at, now) <= days]

                row = self._score_account_window(
                    account=account,
                    window_name=window_name,
                    score_date=score_date,
                    now=now,
                    social_signals=window_social,
                    website_signals=window_website,
                    exa_signals=window_exa,
                )
                rows.append(row)

        return rows

    def _score_account_window(
        self,
        account: AccountContext,
        window_name: str,
        score_date: date,
        now: datetime,
        social_signals: list[MatchedSocialSignal],
        website_signals: list[WebsiteSignal],
        exa_signals: list[ExaResearchSignal],
    ) -> tuple[Any, ...]:
        included_event_counts_by_type = Counter()
        stakeholder_keys: set[str] = set()
        social_engagement_times: list[datetime] = []
        post_link_click_times_from_social: list[datetime] = []

        base_event_points = 0.0
        comment_ai_bonus = 0.0
        analyzed_comment_count = 0
        aggregate_signal_count = 0
        match_confidences: list[float] = []
        strong_signal_count = 0
        has_crm_match = False

        for signal in social_signals:
            event_weight = EVENT_WEIGHTS.get(signal.event_type, 0.0)
            if event_weight <= 0:
                continue

            days = self._days_ago(signal.event_timestamp, now)
            recency = self._recency_multiplier(days)
            if recency <= 0:
                continue

            is_aggregated = bool(signal.metadata_json.get("aggregated_import", False))
            source_metric_count = signal.metadata_json.get("source_metric_count")

            points = event_weight * recency
            if is_aggregated:
                aggregate_signal_count += 1
                count = self._safe_int(source_metric_count, 1)
                intensity = min(math.log2(max(count, 1) + 1), MAX_AGGREGATED_INTENSITY)
                points = points * intensity * AGGREGATED_DAMPENING
            else:
                has_crm_match = True
                stakeholder_key = self._stakeholder_key(signal)
                if stakeholder_key is not None:
                    stakeholder_keys.add(stakeholder_key)

            base_event_points += points
            included_event_counts_by_type[signal.event_type] += 1
            match_confidences.append(signal.match_confidence)

            if signal.event_type in STRONG_SIGNAL_TYPES and not is_aggregated:
                strong_signal_count += 1

            if signal.event_type != "post_impression":
                social_engagement_times.append(signal.event_timestamp)

            if signal.event_type == "post_link_click":
                post_link_click_times_from_social.append(signal.event_timestamp)

            if signal.event_type == "post_comment" and not is_aggregated:
                analysis = signal.metadata_json.get("comment_analysis") if isinstance(signal.metadata_json, dict) else None
                if isinstance(analysis, dict):
                    sentiment = str(analysis.get("sentiment") or "").lower()
                    intent = str(analysis.get("intent") or "").lower()
                    confidence = self._safe_float(analysis.get("confidence"), 0.0)
                    analyzed_comment_count += 1
                    if sentiment == "positive":
                        comment_ai_bonus += 0.75 * confidence
                    elif sentiment == "negative":
                        comment_ai_bonus -= 0.50 * confidence
                    if intent == "high":
                        comment_ai_bonus += 1.50 * confidence
                    elif intent == "medium":
                        comment_ai_bonus += 0.60 * confidence

        website_signal_count = 0
        website_bonus = 0.0
        for signal in website_signals:
            days = self._days_ago(signal.event_timestamp, now)
            recency = self._recency_multiplier(days)
            if recency <= 0:
                continue
            website_signal_count += 1
            website_bonus += 2.0 * recency

        website_bonus = min(website_bonus, 8.0)

        unique_stakeholder_count = len(stakeholder_keys)
        stakeholder_bonus = 0.0
        if unique_stakeholder_count >= 3:
            stakeholder_bonus = STAKEHOLDER_BONUS_BY_COUNT[3]
        elif unique_stakeholder_count == 2:
            stakeholder_bonus = STAKEHOLDER_BONUS_BY_COUNT[2]

        high_signal_bonus = 0.0
        if strong_signal_count >= 3:
            high_signal_bonus += 5.0

        all_website_times = post_link_click_times_from_social + [w.event_timestamp for w in website_signals]
        if social_engagement_times and all_website_times:
            first_social_time = min(social_engagement_times)
            if any(ts > first_social_time for ts in all_website_times):
                high_signal_bonus += 4.0

        tier_bonus = TIER_BONUS.get((account.target_tier or "").strip().lower(), 0.0)
        exa_research_count = len(exa_signals)
        exa_bonus = min(float(exa_research_count) * 1.5, 6.0)
        crm_match_bonus = CRM_MATCH_BONUS if has_crm_match else 0.0

        comment_ai_bonus = max(-3.0, min(comment_ai_bonus, 8.0))
        total_score = (
            base_event_points
            + stakeholder_bonus
            + high_signal_bonus
            + website_bonus
            + tier_bonus
            + exa_bonus
            + crm_match_bonus
            + comment_ai_bonus
        )
        total_score = round(max(0.0, min(100.0, total_score)), 2)

        contributing_event_count = sum(included_event_counts_by_type.values()) + website_signal_count + exa_research_count
        confidence = self._compute_confidence(
            match_confidences=match_confidences,
            aggregate_signal_count=aggregate_signal_count,
            social_signal_count=sum(included_event_counts_by_type.values()),
            contributing_event_count=contributing_event_count,
            exa_research_count=exa_research_count,
        )

        score_reason = self._build_reason(
            window_name=window_name,
            unique_stakeholder_count=unique_stakeholder_count,
            strong_signal_count=strong_signal_count,
            website_signal_count=website_signal_count,
            exa_research_count=exa_research_count,
            crm_match_bonus=crm_match_bonus,
            analyzed_comment_count=analyzed_comment_count,
            comment_ai_bonus=comment_ai_bonus,
            included_event_counts_by_type=included_event_counts_by_type,
        )

        score_breakdown = {
            "window": window_name,
            "base_event_points": round(base_event_points, 2),
            "stakeholder_bonus": stakeholder_bonus,
            "high_signal_bonus": high_signal_bonus,
            "website_bonus": round(website_bonus, 2),
            "tier_bonus": tier_bonus,
            "exa_research_bonus": round(exa_bonus, 2),
            "crm_match_bonus": round(crm_match_bonus, 2),
            "comment_ai_bonus": round(comment_ai_bonus, 2),
            "analyzed_comment_count": analyzed_comment_count,
            "exa_research_count": exa_research_count,
            "aggregate_signal_count": aggregate_signal_count,
            "included_event_counts_by_type": dict(included_event_counts_by_type),
            "recency_buckets": {
                "0_7_days": 1.0,
                "8_14_days": 0.75,
                "15_30_days": 0.5,
                ">30_days": 0.0,
            },
        }

        return (
            account.account_id,
            score_date,
            Decimal(str(total_score)),
            window_name,
            score_reason,
            Decimal(str(confidence)),
            unique_stakeholder_count,
            strong_signal_count,
            website_signal_count,
            contributing_event_count,
            Json(score_breakdown),
        )

    def _upsert_scores(self, cur, rows: list[tuple[Any, ...]]) -> int:
        if not rows:
            return 0

        query = """
            INSERT INTO account_intent_scores (
                account_id,
                score_date,
                score,
                score_window,
                score_reason,
                confidence,
                unique_stakeholder_count,
                strong_signal_count,
                website_signal_count,
                contributing_event_count,
                score_breakdown_json
            ) VALUES %s
            ON CONFLICT (account_id, score_date, score_window) DO UPDATE
            SET
                score = EXCLUDED.score,
                score_reason = EXCLUDED.score_reason,
                confidence = EXCLUDED.confidence,
                unique_stakeholder_count = EXCLUDED.unique_stakeholder_count,
                strong_signal_count = EXCLUDED.strong_signal_count,
                website_signal_count = EXCLUDED.website_signal_count,
                contributing_event_count = EXCLUDED.contributing_event_count,
                score_breakdown_json = EXCLUDED.score_breakdown_json
        """
        execute_values(cur, query, rows)
        return len(rows)

    def _days_ago(self, timestamp: datetime, now: datetime) -> int:
        return max((now - timestamp).days, 0)

    def _recency_multiplier(self, days_ago: int) -> float:
        for max_days, multiplier in RECENCY_BUCKETS:
            if days_ago <= max_days:
                return multiplier
        return 0.0

    def _stakeholder_key(self, signal: MatchedSocialSignal) -> str | None:
        if signal.matched_contact_id is not None:
            return f"contact:{signal.matched_contact_id}"

        actor_url = clean_text(signal.actor_linkedin_url)
        if actor_url:
            return f"linkedin:{actor_url.lower()}"

        actor_name = clean_text(signal.actor_name)
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

    def _compute_confidence(
        self,
        match_confidences: list[float],
        aggregate_signal_count: int,
        social_signal_count: int,
        contributing_event_count: int,
        exa_research_count: int,
    ) -> float:
        if contributing_event_count == 0:
            return 0.20

        if social_signal_count == 0:
            return 0.55

        avg_match_confidence = sum(match_confidences) / len(match_confidences)
        aggregate_ratio = aggregate_signal_count / max(social_signal_count, 1)

        confidence = avg_match_confidence
        if social_signal_count >= 6:
            confidence += 0.05
        if aggregate_ratio >= 0.50:
            confidence -= 0.15
        elif aggregate_ratio >= 0.25:
            confidence -= 0.08
        if exa_research_count > 0:
            confidence += 0.03

        confidence = max(0.10, min(0.98, confidence))
        return round(confidence, 2)

    def _build_reason(
        self,
        window_name: str,
        unique_stakeholder_count: int,
        strong_signal_count: int,
        website_signal_count: int,
        exa_research_count: int,
        crm_match_bonus: float,
        analyzed_comment_count: int,
        comment_ai_bonus: float,
        included_event_counts_by_type: Counter,
    ) -> str:
        days = WINDOW_DAYS[window_name]
        if not included_event_counts_by_type and website_signal_count == 0:
            return f"No matched social or website signals in last {days}d"

        key_parts: list[str] = [f"{unique_stakeholder_count} stakeholders"]

        if included_event_counts_by_type.get("post_comment", 0):
            key_parts.append(f"{included_event_counts_by_type['post_comment']} comments")
        if included_event_counts_by_type.get("post_repost", 0):
            key_parts.append(f"{included_event_counts_by_type['post_repost']} reposts")
        if included_event_counts_by_type.get("post_link_click", 0):
            key_parts.append(f"{included_event_counts_by_type['post_link_click']} post link clicks")
        if website_signal_count:
            key_parts.append(f"{website_signal_count} website events")
        if exa_research_count:
            key_parts.append(f"{exa_research_count} Exa research signals")
        if crm_match_bonus > 0:
            key_parts.append(f"CRM match bonus +{crm_match_bonus:.1f}")
        if analyzed_comment_count:
            key_parts.append(
                f"{analyzed_comment_count} analyzed comments ({'+' if comment_ai_bonus >= 0 else ''}{comment_ai_bonus:.2f} AI bonus)"
            )
        if strong_signal_count:
            key_parts.append(f"{strong_signal_count} strong signals")

        return f"{'; '.join(key_parts)} in last {days}d"

    def _normalize_company(self, value: str | None) -> str:
        text = clean_text(value)
        if not text:
            return ""
        lowered = text.lower()
        for token in ("inc", "llc", "ltd", "corp", "corporation", "company", "co", "plc"):
            lowered = lowered.replace(f" {token}", "")
        return "".join(ch for ch in lowered if ch.isalnum())
