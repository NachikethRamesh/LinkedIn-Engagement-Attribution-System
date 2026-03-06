from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

from app.db import get_connection
from app.identity_resolution.normalization import normalize_company_name
from app.writeback.types import SelectedEntity, TargetType

BAND_ORDER = {"none": 0, "weak": 1, "medium": 2, "strong": 3}


class WritebackSelector:
    def select(self, target_type: TargetType, params: dict[str, Any]) -> list[SelectedEntity]:
        mode = str(params.get("selection_mode", self._default_mode_for_target(target_type)))
        limit = int(params.get("limit", 100))

        if mode == "high_intent_accounts":
            return self._select_high_intent_accounts(target_type=target_type, params=params, limit=limit)
        if mode == "socially_influenced_opportunities":
            return self._select_influenced_opportunities(target_type=target_type, params=params, limit=limit)
        if mode == "low_confidence_promising_accounts":
            return self._select_low_confidence_promising_accounts(target_type=target_type, params=params, limit=limit)
        if mode == "unresolved_account_candidates":
            return self._select_unresolved_account_candidates(target_type=target_type, params=params, limit=limit)

        raise ValueError(
            "selection_mode must be one of: high_intent_accounts, socially_influenced_opportunities, "
            "low_confidence_promising_accounts, unresolved_account_candidates"
        )

    def _default_mode_for_target(self, target_type: TargetType) -> str:
        if target_type == "crm":
            return "high_intent_accounts"
        if target_type == "exa":
            return "unresolved_account_candidates"
        if target_type == "clay":
            return "low_confidence_promising_accounts"
        return "socially_influenced_opportunities"

    def _select_high_intent_accounts(self, target_type: TargetType, params: dict[str, Any], limit: int) -> list[SelectedEntity]:
        min_score = float(params.get("min_intent_score", 55))
        min_confidence = float(params.get("min_intent_confidence", 0.6))
        score_window = str(params.get("score_window", "rolling_30d"))
        min_contributing_events = int(params.get("min_contributing_events", 3))
        min_unique_stakeholders = int(params.get("min_unique_stakeholders", 1))

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        vis.account_id,
                        a.company_name,
                        a.domain,
                        vis.score,
                        vis.confidence,
                        vis.score_window,
                        vis.score_reason,
                        vis.unique_stakeholder_count,
                        vis.strong_signal_count,
                        vis.website_signal_count,
                        vis.contributing_event_count,
                        (
                            SELECT oi.influence_band
                            FROM opportunity_influence oi
                            WHERE oi.account_id = vis.account_id
                            ORDER BY oi.influence_score DESC, oi.opportunity_id DESC
                            LIMIT 1
                        ) AS latest_influence_band,
                        (
                            SELECT oi.influence_score
                            FROM opportunity_influence oi
                            WHERE oi.account_id = vis.account_id
                            ORDER BY oi.influence_score DESC, oi.opportunity_id DESC
                            LIMIT 1
                        ) AS latest_influence_score
                    FROM v_latest_account_intent_status vis
                    JOIN accounts a ON a.id = vis.account_id
                    WHERE vis.score_window = %s
                      AND vis.score >= %s
                      AND vis.confidence >= %s
                      AND vis.contributing_event_count >= %s
                      AND vis.unique_stakeholder_count >= %s
                    ORDER BY vis.score DESC, vis.confidence DESC, vis.account_id
                    LIMIT %s
                    """,
                    (score_window, min_score, min_confidence, min_contributing_events, min_unique_stakeholders, limit),
                )
                rows = cur.fetchall()

        selected: list[SelectedEntity] = []
        for row in rows:
            selected.append(
                SelectedEntity(
                    entity_type="account",
                    entity_id=row[0],
                    target_type=target_type,
                    selection_bucket="high_intent_accounts",
                    selection_reason=(
                        f"Intent score {float(row[3]):.2f} ({row[5]}, conf {float(row[4]):.2f}), "
                        f"{int(row[7] or 0)} stakeholders, {int(row[10] or 0)} contributing events"
                    ),
                    data={
                        "account_id": row[0],
                        "company_name": row[1],
                        "domain": row[2],
                        "latest_intent_score": float(row[3]),
                        "latest_intent_confidence": float(row[4]),
                        "score_window": row[5],
                        "score_reason": row[6],
                        "unique_stakeholder_count": int(row[7] or 0),
                        "strong_signal_count": int(row[8] or 0),
                        "website_signal_count": int(row[9] or 0),
                        "contributing_event_count": int(row[10] or 0),
                        "latest_influence_band": row[11],
                        "latest_influence_score": float(row[12]) if row[12] is not None else None,
                    },
                )
            )
        return selected

    def _select_influenced_opportunities(self, target_type: TargetType, params: dict[str, Any], limit: int) -> list[SelectedEntity]:
        min_band = str(params.get("min_influence_band", "medium"))
        min_score = float(params.get("min_influence_score", 40))
        min_confidence = float(params.get("min_influence_confidence", 0.45))
        min_rank = BAND_ORDER.get(min_band, BAND_ORDER["medium"])

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        oi.opportunity_id,
                        oi.account_id,
                        o.opportunity_name,
                        o.stage,
                        o.amount,
                        o.created_at,
                        a.company_name,
                        a.domain,
                        oi.influence_score,
                        oi.influence_band,
                        oi.influenced,
                        oi.confidence,
                        oi.notes,
                        oi.unique_stakeholder_count,
                        oi.website_signal_count,
                        oi.matched_event_count
                    FROM opportunity_influence oi
                    JOIN opportunities o ON o.id = oi.opportunity_id
                    JOIN accounts a ON a.id = oi.account_id
                    WHERE oi.influence_score >= %s
                      AND oi.confidence >= %s
                    ORDER BY oi.influence_score DESC, oi.opportunity_id DESC
                    LIMIT %s
                    """,
                    (min_score, min_confidence, limit * 3),
                )
                rows = cur.fetchall()

        selected: list[SelectedEntity] = []
        for row in rows:
            band = str(row[9] or "none")
            influenced = bool(row[10])
            if not influenced and BAND_ORDER.get(band, 0) < min_rank:
                continue

            if BAND_ORDER.get(band, 0) < min_rank:
                continue

            selected.append(
                SelectedEntity(
                    entity_type="opportunity",
                    entity_id=row[0],
                    target_type=target_type,
                    selection_bucket="socially_influenced_opportunities",
                    selection_reason=(
                        f"Influence {float(row[8]):.2f} ({band}, conf {float(row[11]):.2f}), "
                        f"{int(row[13] or 0)} stakeholders, {int(row[15] or 0)} matched events"
                    ),
                    data={
                        "opportunity_id": row[0],
                        "account_id": row[1],
                        "opportunity_name": row[2],
                        "stage": row[3],
                        "amount": float(row[4]),
                        "created_at": row[5].isoformat(),
                        "company_name": row[6],
                        "domain": row[7],
                        "influence_score": float(row[8]),
                        "influence_band": band,
                        "influenced": influenced,
                        "influence_confidence": float(row[11]),
                        "notes": row[12],
                        "unique_stakeholder_count": int(row[13] or 0),
                        "website_signal_count": int(row[14] or 0),
                        "matched_event_count": int(row[15] or 0),
                    },
                )
            )

            if len(selected) >= limit:
                break

        return selected

    def _select_low_confidence_promising_accounts(
        self, target_type: TargetType, params: dict[str, Any], limit: int
    ) -> list[SelectedEntity]:
        min_score = float(params.get("min_intent_score", 50))
        max_confidence = float(params.get("max_intent_confidence", 0.65))
        score_window = str(params.get("score_window", "rolling_30d"))
        min_contributing_events = int(params.get("min_contributing_events", 2))
        min_influence_score = float(params.get("min_influence_score", 30))

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        vis.account_id,
                        a.company_name,
                        a.domain,
                        vis.score,
                        vis.confidence,
                        vis.score_window,
                        vis.score_reason,
                        vis.unique_stakeholder_count,
                        vis.strong_signal_count,
                        vis.website_signal_count,
                        vis.contributing_event_count,
                        (
                            SELECT oi.influence_band
                            FROM opportunity_influence oi
                            WHERE oi.account_id = vis.account_id
                            ORDER BY oi.influence_score DESC, oi.opportunity_id DESC
                            LIMIT 1
                        ) AS latest_influence_band,
                        (
                            SELECT oi.influence_score
                            FROM opportunity_influence oi
                            WHERE oi.account_id = vis.account_id
                            ORDER BY oi.influence_score DESC, oi.opportunity_id DESC
                            LIMIT 1
                        ) AS latest_influence_score
                    FROM v_latest_account_intent_status vis
                    JOIN accounts a ON a.id = vis.account_id
                    WHERE vis.score_window = %s
                      AND vis.score >= %s
                      AND vis.confidence <= %s
                      AND (
                          vis.contributing_event_count >= %s
                          OR (
                              SELECT COALESCE(MAX(oi.influence_score), 0)
                              FROM opportunity_influence oi
                              WHERE oi.account_id = vis.account_id
                          ) >= %s
                      )
                    ORDER BY vis.score DESC, vis.confidence ASC, vis.account_id
                    LIMIT %s
                    """,
                    (score_window, min_score, max_confidence, min_contributing_events, min_influence_score, limit),
                )
                rows = cur.fetchall()

        selected: list[SelectedEntity] = []
        for row in rows:
            weak_reasons = []
            if float(row[4]) <= max_confidence:
                weak_reasons.append(f"Intent confidence {float(row[4]):.2f} <= {max_confidence:.2f}")
            if row[11] in {"weak", "none"}:
                weak_reasons.append(f"Influence band currently {row[11]}")
            if int(row[10] or 0) >= min_contributing_events:
                weak_reasons.append(f"Promising volume: {int(row[10] or 0)} contributing events")

            selected.append(
                SelectedEntity(
                    entity_type="account",
                    entity_id=row[0],
                    target_type=target_type,
                    selection_bucket="low_confidence_promising_accounts",
                    selection_reason="; ".join(weak_reasons) if weak_reasons else "Promising account for enrichment",
                    data={
                        "account_id": row[0],
                        "company_name": row[1],
                        "domain": row[2],
                        "latest_intent_score": float(row[3]),
                        "latest_intent_confidence": float(row[4]),
                        "score_window": row[5],
                        "score_reason": row[6],
                        "unique_stakeholder_count": int(row[7] or 0),
                        "strong_signal_count": int(row[8] or 0),
                        "website_signal_count": int(row[9] or 0),
                        "contributing_event_count": int(row[10] or 0),
                        "latest_influence_band": row[11],
                        "latest_influence_score": float(row[12]) if row[12] is not None else None,
                        "weak_match_reasons": weak_reasons,
                    },
                )
            )

        return selected

    def _select_unresolved_account_candidates(
        self, target_type: TargetType, params: dict[str, Any], limit: int
    ) -> list[SelectedEntity]:
        weak_match_threshold = _float_param(params.get("weak_match_confidence_threshold"), 0.7)
        min_contributing_events = _int_param(params.get("min_contributing_events"), 3)
        min_strong_signals = _int_param(params.get("min_strong_signals"), 1)
        recent_days = _int_param(params.get("recent_days"), 30)
        min_recent_signals = _int_param(params.get("min_recent_signals"), 1)
        include_generic_candidates = bool(params.get("include_generic_candidates", False))
        now_utc = datetime.now(UTC)
        recent_cutoff = now_utc - timedelta(days=recent_days)
        strong_types = {"post_comment", "post_repost", "post_link_click", "comment", "share"}
        event_alias = {
            "reaction": "post_like",
            "comment": "post_comment",
            "share": "post_repost",
            "website_click": "post_link_click",
            "profile_view": "post_link_click",
            "company_page_view": "post_link_click",
        }
        generic_company_tokens = {"unknown", "freelance", "independent consultant", "stealth startup", "future ventures"}

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        se.id,
                        se.actor_name,
                        se.actor_company_raw,
                        se.event_type,
                        se.event_timestamp,
                        sem.match_type,
                        sem.match_confidence,
                        sem.match_reason,
                        se.metadata_json->>'actor_origin'
                    FROM social_events se
                    JOIN social_event_matches sem
                      ON sem.social_event_id = se.id
                    WHERE COALESCE(NULLIF(TRIM(se.actor_company_raw), ''), '') <> ''
                      AND sem.match_type <> 'skipped_aggregate_import'
                      AND (
                        sem.match_type = 'unresolved'
                        OR (
                            sem.matched_contact_id IS NULL
                            AND sem.matched_account_id IS NOT NULL
                            AND sem.match_confidence < %s
                        )
                      )
                    ORDER BY se.event_timestamp DESC, se.id DESC
                    """,
                    (weak_match_threshold,),
                )
                rows = cur.fetchall()

        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            social_event_id = int(row[0])
            actor_name = str(row[1] or "").strip()
            raw_company = str(row[2]).strip()
            raw_event_type = str(row[3])
            event_type = event_alias.get(raw_event_type, raw_event_type)
            event_ts = row[4]
            match_type = str(row[5])
            match_conf = float(row[6])
            match_reason = str(row[7] or "")
            actor_origin = str(row[8] or "")

            normalized = normalize_company_name(raw_company) or raw_company.lower()
            key = normalized
            bucket = grouped.get(key)
            if bucket is None:
                bucket = {
                    "raw_company_samples": {},
                    "actor_name_samples": {},
                    "normalized_company_name": normalized,
                    "source_social_event_ids": [],
                    "event_counts_by_type": {},
                    "strong_signal_count": 0,
                    "recent_signal_count": 0,
                    "weak_match_reasons": {},
                    "max_event_ts": event_ts,
                    "max_match_confidence": match_conf,
                    "actor_origin_counts": {},
                }
                grouped[key] = bucket

            bucket["raw_company_samples"][raw_company] = bucket["raw_company_samples"].get(raw_company, 0) + 1
            if actor_name:
                bucket["actor_name_samples"][actor_name] = bucket["actor_name_samples"].get(actor_name, 0) + 1
            if len(bucket["source_social_event_ids"]) < 30:
                bucket["source_social_event_ids"].append(social_event_id)

            counts = bucket["event_counts_by_type"]
            counts[event_type] = counts.get(event_type, 0) + 1
            if event_type in strong_types:
                bucket["strong_signal_count"] += 1
            if event_ts >= recent_cutoff:
                bucket["recent_signal_count"] += 1
            if match_reason:
                bucket["weak_match_reasons"][match_reason] = bucket["weak_match_reasons"].get(match_reason, 0) + 1
            bucket["max_event_ts"] = max(bucket["max_event_ts"], event_ts)
            bucket["max_match_confidence"] = max(bucket["max_match_confidence"], match_conf)
            bucket["actor_origin_counts"][actor_origin] = bucket["actor_origin_counts"].get(actor_origin, 0) + 1

        candidates: list[SelectedEntity] = []
        sorted_buckets = sorted(
            grouped.values(),
            key=lambda b: (
                b["strong_signal_count"],
                b["recent_signal_count"],
                sum(b["event_counts_by_type"].values()),
                b["max_event_ts"],
            ),
            reverse=True,
        )

        for bucket in sorted_buckets:
            total_events = int(sum(bucket["event_counts_by_type"].values()))
            strong_signal_count = int(bucket["strong_signal_count"])
            recent_signal_count = int(bucket["recent_signal_count"])
            if total_events < min_contributing_events:
                continue
            if strong_signal_count < min_strong_signals:
                continue
            if recent_signal_count < min_recent_signals:
                continue

            raw_company = max(bucket["raw_company_samples"].items(), key=lambda x: x[1])[0]
            normalized_company = str(bucket["normalized_company_name"])
            if not include_generic_candidates and normalized_company in generic_company_tokens:
                continue
            event_counts_by_type = dict(sorted(bucket["event_counts_by_type"].items(), key=lambda x: x[1], reverse=True))
            strongest_signal_type = next(iter(event_counts_by_type.keys()))
            weak_match_reason = (
                max(bucket["weak_match_reasons"].items(), key=lambda x: x[1])[0]
                if bucket["weak_match_reasons"]
                else "Unresolved candidate with no confident account match"
            )
            candidate_id = _stable_candidate_id(normalized_company)

            reason = (
                f"Unresolved company candidate with {total_events} signals in {recent_days}d window, "
                f"{strong_signal_count} strong signals, strongest={strongest_signal_type}. "
                f"Weak-match context: {weak_match_reason}"
            )
            candidates.append(
                SelectedEntity(
                    entity_type="unresolved_account_candidate",
                    entity_id=candidate_id,
                    target_type=target_type,
                    selection_bucket="unresolved_account_candidates",
                    selection_reason=reason,
                    data={
                        "candidate_id": candidate_id,
                        "candidate_company_name_raw": raw_company,
                        "candidate_company_name_normalized": normalized_company,
                        "supporting_signal_summary": event_counts_by_type,
                        "strongest_signal_type": strongest_signal_type,
                        "recent_signal_count": recent_signal_count,
                        "contributing_event_count": total_events,
                        "source_social_event_ids": bucket["source_social_event_ids"],
                        "unresolved_actor_names": [
                            name
                            for name, _ in sorted(
                                bucket["actor_name_samples"].items(),
                                key=lambda item: item[1],
                                reverse=True,
                            )[:30]
                        ],
                        "weak_match_reason": weak_match_reason,
                        "max_match_confidence": round(float(bucket["max_match_confidence"]), 2),
                        "actor_origin_counts": bucket["actor_origin_counts"],
                        "selection_context": {
                            "weak_match_confidence_threshold": weak_match_threshold,
                            "min_contributing_events": min_contributing_events,
                            "min_strong_signals": min_strong_signals,
                            "min_recent_signals": min_recent_signals,
                            "recent_days": recent_days,
                        },
                    },
                )
            )
            if len(candidates) >= limit:
                break

        return candidates


def _stable_candidate_id(normalized_company_name: str) -> int:
    digest = hashlib.sha256(normalized_company_name.encode("utf-8")).hexdigest()
    return int(digest[:15], 16)


def _float_param(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int_param(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
