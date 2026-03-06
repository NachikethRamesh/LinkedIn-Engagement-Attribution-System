from __future__ import annotations

import json
import csv
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.writeback.adapters.base import BaseWritebackAdapter
from app.writeback.types import DeliveryResult


class ExaWritebackAdapter(BaseWritebackAdapter):
    target_type = "exa"

    def deliver(
        self,
        payload: dict[str, Any],
        endpoint_url: str | None,
        timeout_seconds: int = 15,
        **kwargs: Any,
    ) -> DeliveryResult:
        auth_headers = kwargs.get("auth_headers")
        simulate_local = bool(kwargs.get("simulate_local", False))
        if simulate_local:
            # Local simulation artifacts are intentionally disabled.
            raise ValueError("Exa simulate_local mode is disabled. Provide a real endpoint_url.")

        if not endpoint_url:
            return DeliveryResult(
                status="success",
                response_json={
                    "delivery_mode": "stub",
                    "target_type": self.target_type,
                    "message": "No Exa endpoint configured; recorded deterministic stub success",
                },
            )
        return self._post_json_with_headers(
            endpoint_url=endpoint_url,
            payload=payload,
            timeout_seconds=timeout_seconds,
            headers=auth_headers if isinstance(auth_headers, dict) else None,
        )

    def _deliver_simulated_local(self, payload: dict[str, Any], writeback_run_id: str) -> DeliveryResult:
        # Legacy simulation helper retained for possible future feature-flag reintroduction.
        root = Path(__file__).resolve().parents[3]
        outbound_dir = root / "data" / "outbound" / "exa_requests"
        inbound_dir = root / "data" / "inbound" / "exa_results"
        outbound_dir.mkdir(parents=True, exist_ok=True)
        inbound_dir.mkdir(parents=True, exist_ok=True)

        candidate_id = payload.get("candidate_id") or payload.get("account_id") or "unknown"
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        slug = f"run_{writeback_run_id}_candidate_{candidate_id}_{ts}"

        outbound_file = outbound_dir / f"{slug}.json"
        outbound_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        normalized_result = self._build_simulated_research_result(
            payload=payload,
            writeback_run_id=writeback_run_id,
            root=root,
        )
        inbound_file = inbound_dir / f"{slug}_result.json"
        inbound_file.write_text(json.dumps({"results": [normalized_result]}, indent=2), encoding="utf-8")

        return DeliveryResult(
            status="success",
            response_json={
                "delivery_mode": "simulated_local",
                "target_type": self.target_type,
                "outbound_file": str(outbound_file),
                "generated_inbound_result_file": str(inbound_file),
                "result_count": 1,
            },
            external_key=f"simulated-exa-{writeback_run_id}-{candidate_id}",
        )

    def _build_simulated_research_result(self, payload: dict[str, Any], writeback_run_id: str, root: Path) -> dict[str, Any]:
        csv_matches = self._lookup_simulated_csv_rows(payload=payload, root=root)
        csv_match = csv_matches[0] if csv_matches else None
        is_unresolved = payload.get("entity_type") == "unresolved_account_candidate"
        if is_unresolved:
            candidate_id = int(payload.get("candidate_id") or 0)
            normalized_name = str(payload.get("candidate_company_name_normalized") or "")
            raw_name = str(payload.get("candidate_company_name_raw") or "")
            likely_company_name = str(csv_match.get("likely_company_name") or raw_name.title()) if csv_match else raw_name.title()
            likely_domain = (
                str(csv_match.get("likely_domain"))
                if csv_match and csv_match.get("likely_domain")
                else normalized_name.replace(" ", "") + ".com"
            )
            if not normalized_name:
                likely_domain = "unknown-domain.com"

            possible_match_hints: dict[str, Any] = {
                "normalized_candidate_name": normalized_name,
                "source_signal_context": payload.get("supporting_signal_summary", {}),
            }
            if csv_match:
                possible_match_hints["csv_unresolved_actor_name"] = csv_match.get("unresolved_actor_name")
                possible_match_hints["csv_possible_match_hints"] = csv_match.get("possible_match_hints")

            recent_initiatives = (
                [part.strip() for part in str(csv_match.get("recent_initiatives", "")).split("|") if part.strip()]
                if csv_match
                else []
            )
            if not recent_initiatives:
                recent_initiatives = [
                    "Hiring in growth and revops roles",
                    "Publishing attribution-related thought leadership",
                ]
            additional_contacts = []
            for extra in csv_matches[1:]:
                additional_contacts.append(
                    {
                        "contact_full_name": str(extra.get("contact_full_name") or "").strip(),
                        "contact_title": str(extra.get("contact_title") or "").strip(),
                        "contact_email": str(extra.get("contact_email") or "").strip().lower(),
                        "contact_linkedin_url": str(extra.get("contact_linkedin_url") or "").strip(),
                    }
                )

            return {
                "target_type": "exa",
                "entity_type": "unresolved_account_candidate",
                "entity_id": candidate_id,
                "enrichment_type": "account_resolution_research",
                "normalized_data_json": {
                    "likely_company_name": likely_company_name,
                    "likely_domain": likely_domain,
                    "industry": str(csv_match.get("industry") or "B2B Technology") if csv_match else "B2B Technology",
                    "company_description": (
                        str(csv_match.get("company_description") or "Simulated Exa research profile for unresolved candidate review.")
                        if csv_match
                        else "Simulated Exa research profile for unresolved candidate review."
                    ),
                    "recent_initiatives": recent_initiatives,
                    "hiring_or_growth_signals": (
                        str(csv_match.get("hiring_or_growth_signals") or "Moderate expansion indicators")
                        if csv_match
                        else "Moderate expansion indicators"
                    ),
                    "confidence_notes": (
                        str(csv_match.get("confidence_notes") or "Simulation output based on unresolved candidate signal summary.")
                        if csv_match
                        else "Simulation output based on unresolved candidate signal summary."
                    ),
                    "possible_match_hints": possible_match_hints,
                    "crm_enrichment": {
                        "company_name": likely_company_name,
                        "domain": likely_domain,
                        "contact_full_name": str(csv_match.get("contact_full_name") or "") if csv_match else "",
                        "contact_title": str(csv_match.get("contact_title") or "") if csv_match else "",
                        "contact_email": str(csv_match.get("contact_email") or "") if csv_match else "",
                        "contact_linkedin_url": str(csv_match.get("contact_linkedin_url") or "") if csv_match else "",
                        "additional_contacts": additional_contacts,
                    },
                    "simulation_source": {
                        "mode": "csv" if csv_match else "default_fixture",
                        "csv_path": self._simulation_csv_path(root),
                    },
                },
                "source_run_id": writeback_run_id,
                "notes": "Generated by simulated Exa adapter for unresolved candidate flow",
            }

        account_id = int(payload.get("account_id") or 0)
        company_name = str(payload.get("company_name") or "Unknown Company")
        domain = str(payload.get("domain") or f"{company_name.lower().replace(' ', '')}.com")
        return {
            "target_type": "exa",
            "entity_type": "account",
            "entity_id": account_id,
            "enrichment_type": "company_research",
            "normalized_data_json": {
                "likely_company_name": company_name,
                "likely_domain": domain,
                "industry": "Technology",
                "company_description": "Simulated Exa research profile for known account.",
                "recent_initiatives": ["Expansion into enterprise segment"],
                "hiring_or_growth_signals": "Steady growth",
                "confidence_notes": "Simulation output based on account context.",
                "possible_match_hints": {"account_id": account_id},
            },
            "source_run_id": writeback_run_id,
            "notes": "Generated by simulated Exa adapter",
        }

    def _simulation_csv_path(self, root: Path) -> str:
        raw = os.getenv("EXA_SIMULATION_CSV_PATH", "")
        if not raw.strip():
            return ""
        path = Path(raw)
        if not path.is_absolute():
            path = root / path
        return str(path)

    def _lookup_simulated_csv_rows(self, payload: dict[str, Any], root: Path) -> list[dict[str, str]]:
        csv_path_value = self._simulation_csv_path(root)
        if not csv_path_value:
            return []
        csv_path = Path(csv_path_value)
        if not csv_path.exists():
            return []

        candidate_norm = str(payload.get("candidate_company_name_normalized") or "").strip().lower()
        actor_names = [str(x).strip().lower() for x in (payload.get("unresolved_actor_names") or []) if str(x).strip()]

        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = list(csv.DictReader(f))

        actor_hits: list[dict[str, str]] = []
        company_hits: list[dict[str, str]] = []
        for row in reader:
            row_actor = str(row.get("unresolved_actor_name") or "").strip().lower()
            row_norm = str(row.get("candidate_company_name_normalized") or "").strip().lower()
            if row_actor and row_actor in actor_names:
                actor_hits.append(row)
            elif row_norm and row_norm == candidate_norm:
                company_hits.append(row)

        # Return actor-specific rows first, then company-level fallback rows.
        return actor_hits + company_hits
