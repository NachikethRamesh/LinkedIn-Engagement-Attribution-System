from __future__ import annotations

from typing import Any

from app.writeback.types import SelectedEntity


def build_payload(selected: SelectedEntity) -> dict[str, Any]:
    if selected.target_type == "crm":
        return _build_crm_payload(selected)
    if selected.target_type == "clay":
        return _build_clay_payload(selected)
    if selected.target_type == "exa":
        return _build_exa_payload(selected)
    return _build_webhook_generic_payload(selected)


def _build_crm_payload(selected: SelectedEntity) -> dict[str, Any]:
    if selected.entity_type == "account":
        data = selected.data
        return {
            "payload_version": "v1",
            "entity_type": "account",
            "account_id": data["account_id"],
            "company_name": data["company_name"],
            "domain": data.get("domain"),
            "latest_intent_score": data.get("latest_intent_score"),
            "latest_intent_confidence": data.get("latest_intent_confidence"),
            "score_window": data.get("score_window"),
            "score_reason": data.get("score_reason"),
            "latest_influence_band": data.get("latest_influence_band"),
            "latest_influence_score": data.get("latest_influence_score"),
            "recommended_action": _recommended_action(
                score=float(data.get("latest_intent_score") or 0),
                influence_band=str(data.get("latest_influence_band") or "none"),
            ),
            "activation_context": {
                "unique_stakeholder_count": data.get("unique_stakeholder_count"),
                "strong_signal_count": data.get("strong_signal_count"),
                "website_signal_count": data.get("website_signal_count"),
                "contributing_event_count": data.get("contributing_event_count"),
            },
            "source_system": "social-attribution-engine",
            "selection_bucket": selected.selection_bucket,
            "selection_reason": selected.selection_reason,
        }

    data = selected.data
    return {
        "payload_version": "v1",
        "entity_type": "opportunity",
        "opportunity_id": data["opportunity_id"],
        "account_id": data["account_id"],
        "opportunity_name": data["opportunity_name"],
        "stage": data["stage"],
        "amount": data["amount"],
        "created_at": data["created_at"],
        "influence_score": data["influence_score"],
        "influence_band": data["influence_band"],
        "influence_confidence": data["influence_confidence"],
        "notes": data.get("notes"),
        "recommended_action": "Coordinate sales follow-up with social context attached",
        "source_system": "social-attribution-engine",
        "selection_bucket": selected.selection_bucket,
        "selection_reason": selected.selection_reason,
    }


def _build_clay_payload(selected: SelectedEntity) -> dict[str, Any]:
    data = selected.data
    return {
        "payload_version": "v1",
        "entity_type": selected.entity_type,
        "account_id": data.get("account_id", selected.entity_id if selected.entity_type == "account" else data.get("account_id")),
        "company_name": data.get("company_name"),
        "domain": data.get("domain"),
        "enrichment_context": {
            "unique_stakeholder_count": data.get("unique_stakeholder_count"),
            "strong_signal_count": data.get("strong_signal_count"),
            "website_signal_count": data.get("website_signal_count"),
            "contributing_event_count": data.get("contributing_event_count"),
        },
        "weak_match_reasons": data.get("weak_match_reasons", [selected.selection_reason]),
        "latest_intent_score": data.get("latest_intent_score"),
        "latest_influence_score": data.get("latest_influence_score"),
        "enrichment_goal": "Resolve company/contact data and improve account confidence",
        "source_system": "social-attribution-engine",
        "selection_bucket": selected.selection_bucket,
    }


def _build_exa_payload(selected: SelectedEntity) -> dict[str, Any]:
    data = selected.data
    if selected.entity_type == "unresolved_account_candidate":
        return {
            "payload_version": "v1",
            "entity_type": "unresolved_account_candidate",
            "candidate_id": data.get("candidate_id", selected.entity_id),
            "candidate_company_name_raw": data.get("candidate_company_name_raw"),
            "candidate_company_name_normalized": data.get("candidate_company_name_normalized"),
            "unresolved_actor_names": data.get("unresolved_actor_names", []),
            "supporting_signal_summary": data.get("supporting_signal_summary", {}),
            "strongest_signal_type": data.get("strongest_signal_type"),
            "recent_signal_count": data.get("recent_signal_count", 0),
            "contributing_event_count": data.get("contributing_event_count", 0),
            "weak_match_reason": data.get("weak_match_reason"),
            "source_social_event_ids": data.get("source_social_event_ids", []),
            "research_goal": (
                "Identify likely company identity, official domain, recent initiatives, "
                "and GTM-relevant context for unresolved account candidate review."
            ),
            "source_system": "social-attribution-engine",
            "selection_bucket": selected.selection_bucket,
            "selection_reason": selected.selection_reason,
        }

    score = data.get("latest_intent_score")
    influence = data.get("latest_influence_score")
    return {
        "payload_version": "v1",
        "entity_type": selected.entity_type,
        "account_id": data.get("account_id", selected.entity_id if selected.entity_type == "account" else data.get("account_id")),
        "company_name": data.get("company_name"),
        "domain": data.get("domain"),
        "research_goal": "Identify recent company initiatives, hiring, and strategic priorities for outreach context",
        "research_context": {
            "context_summary": (
            f"Intent={score}, Influence={influence}, Selection={selected.selection_bucket}: {selected.selection_reason}"
            ),
            "selection_bucket": selected.selection_bucket,
            "selection_reason": selected.selection_reason,
        },
        "source_system": "social-attribution-engine",
    }


def _build_webhook_generic_payload(selected: SelectedEntity) -> dict[str, Any]:
    return {
        "target_type": selected.target_type,
        "entity_type": selected.entity_type,
        "entity_id": selected.entity_id,
        "selection_bucket": selected.selection_bucket,
        "selection_reason": selected.selection_reason,
        "attributes": selected.data,
        "source_system": "social-attribution-engine",
    }


def _recommended_action(score: float, influence_band: str) -> str:
    if influence_band in {"medium", "strong"} or score >= 70:
        return "Prioritize AE outreach this week"
    if score >= 50:
        return "Add to SDR sequence with social context"
    return "Queue for enrichment follow-up"
