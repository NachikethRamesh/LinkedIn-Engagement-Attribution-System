from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from psycopg2.extras import Json, execute_values

from app.db import get_connection
from app.identity_resolution.normalization import (
    clean_text,
    looks_like_domain,
    normalize_company_name,
    normalize_domain,
    normalize_linkedin_url,
    normalize_person_name,
)
from app.identity_resolution.types import MatchResult, SocialEventRecord

CONFIDENCE_BY_MATCH_TYPE: dict[str, float] = {
    "exact_contact_linkedin_url": 0.95,
    "exact_contact_name_and_account": 0.85,
    "exact_account_name": 0.80,
    "normalized_account_name": 0.75,
    "inferred_from_actor_company": 0.60,
    "inferred_from_website_domain": 0.60,
    "unresolved": 0.00,
    "skipped_aggregate_import": 0.00,
}


@dataclass(slots=True)
class ContactRecord:
    id: int
    account_id: int
    full_name: str
    linkedin_url: str | None


@dataclass(slots=True)
class AccountRecord:
    id: int
    company_name: str
    domain: str | None


@dataclass(slots=True)
class AccountMatchOutcome:
    status: str
    account_id: int | None
    match_type: str | None
    reason: str
    candidate_account_ids: list[int]


class IdentityResolutionService:
    def run(self, rebuild: bool = False) -> dict[str, int]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                if rebuild:
                    cur.execute("TRUNCATE TABLE social_event_matches RESTART IDENTITY;")

                events = self._load_target_events(cur)
                contacts, accounts = self._load_reference_data(cur)

                results: list[MatchResult] = [self._resolve_event(event, contacts, accounts) for event in events]
                self._upsert_results(cur, results)

            conn.commit()

        return self._build_summary(results)

    def _load_target_events(self, cur) -> list[SocialEventRecord]:
        cur.execute(
            """
            SELECT se.id, se.actor_name, se.actor_linkedin_url, se.actor_company_raw, se.metadata_json
            FROM social_events se
            LEFT JOIN social_event_matches sem ON sem.social_event_id = se.id
            WHERE sem.social_event_id IS NULL
            ORDER BY se.id
            """
        )

        events: list[SocialEventRecord] = []
        for row in cur.fetchall():
            metadata = row[4] if isinstance(row[4], dict) else {}
            events.append(
                SocialEventRecord(
                    id=row[0],
                    actor_name=row[1],
                    actor_linkedin_url=row[2],
                    actor_company_raw=row[3],
                    metadata_json=metadata,
                )
            )
        return events

    def _load_reference_data(self, cur) -> tuple[dict[str, list[ContactRecord]], dict[str, Any]]:
        cur.execute(
            """
            SELECT
                c.id AS contact_id,
                a.id AS account_id,
                c.full_name,
                c.linkedin_url,
                a.company_name,
                a.domain
            FROM contacts c
            JOIN accounts a ON a.id = c.account_id
            """
        )
        merged_rows = cur.fetchall()
        contacts = [ContactRecord(id=r[0], account_id=r[1], full_name=r[2], linkedin_url=r[3]) for r in merged_rows]
        unique_accounts: dict[int, AccountRecord] = {}
        for row in merged_rows:
            account_id = int(row[1])
            if account_id not in unique_accounts:
                unique_accounts[account_id] = AccountRecord(
                    id=account_id,
                    company_name=row[4],
                    domain=row[5],
                )
        accounts = list(unique_accounts.values())

        contacts_by_linkedin: dict[str, list[ContactRecord]] = defaultdict(list)
        contacts_by_name_and_account: dict[tuple[str, int], list[ContactRecord]] = defaultdict(list)
        for contact in contacts:
            normalized_url = normalize_linkedin_url(contact.linkedin_url)
            if normalized_url:
                contacts_by_linkedin[normalized_url].append(contact)

            normalized_name = normalize_person_name(contact.full_name)
            if normalized_name:
                contacts_by_name_and_account[(normalized_name, contact.account_id)].append(contact)

        accounts_by_exact_lower: dict[str, list[AccountRecord]] = defaultdict(list)
        accounts_by_normalized: dict[str, list[AccountRecord]] = defaultdict(list)
        accounts_by_domain: dict[str, list[AccountRecord]] = defaultdict(list)
        for account in accounts:
            exact = clean_text(account.company_name)
            if exact:
                accounts_by_exact_lower[exact.lower()].append(account)

            normalized = normalize_company_name(account.company_name)
            if normalized:
                accounts_by_normalized[normalized].append(account)

            domain = normalize_domain(account.domain)
            if domain:
                accounts_by_domain[domain].append(account)

        return contacts_by_linkedin, {
            "contacts_by_name_and_account": contacts_by_name_and_account,
            "accounts_by_exact_lower": accounts_by_exact_lower,
            "accounts_by_normalized": accounts_by_normalized,
            "accounts_by_domain": accounts_by_domain,
        }

    def _resolve_event(self, event: SocialEventRecord, contacts_by_linkedin: dict[str, list[ContactRecord]], refs: dict[str, Any]) -> MatchResult:
        now = datetime.now(UTC)
        metadata = event.metadata_json or {}
        aggregated = bool(metadata.get("aggregated_import", False))

        actor_name_norm = normalize_person_name(event.actor_name)
        actor_company_norm = normalize_company_name(event.actor_company_raw)
        actor_company_exact_lower = clean_text(event.actor_company_raw).lower() if clean_text(event.actor_company_raw) else None
        actor_url_norm = normalize_linkedin_url(event.actor_linkedin_url)

        accounts_by_exact_lower = refs["accounts_by_exact_lower"]
        accounts_by_normalized = refs["accounts_by_normalized"]
        accounts_by_domain = refs["accounts_by_domain"]
        contacts_by_name_and_account = refs["contacts_by_name_and_account"]

        account_match_for_company = self._match_account(
            actor_company_exact_lower,
            actor_company_norm,
            event.actor_company_raw,
            accounts_by_exact_lower,
            accounts_by_normalized,
            accounts_by_domain,
        )

        if aggregated:
            if account_match_for_company.status == "matched" and account_match_for_company.account_id is not None:
                return MatchResult(
                    social_event_id=event.id,
                    matched_contact_id=None,
                    matched_account_id=account_match_for_company.account_id,
                    match_type=account_match_for_company.match_type or "inferred_from_actor_company",
                    match_confidence=self._confidence(account_match_for_company.match_type or "inferred_from_actor_company"),
                    match_reason="Aggregate import matched at account level only",
                    matched_on_fields_json={
                        "aggregated_import": True,
                        "actor_origin": metadata.get("actor_origin"),
                        "source_name": metadata.get("source_name"),
                        "actor_company_raw": event.actor_company_raw,
                        "account_match": {
                            "status": account_match_for_company.status,
                            "account_id": account_match_for_company.account_id,
                            "match_type": account_match_for_company.match_type,
                            "reason": account_match_for_company.reason,
                            "candidate_account_ids": account_match_for_company.candidate_account_ids,
                        },
                    },
                    created_at=now,
                )

            if account_match_for_company.status == "ambiguous":
                return MatchResult(
                    social_event_id=event.id,
                    matched_contact_id=None,
                    matched_account_id=None,
                    match_type="skipped_aggregate_import",
                    match_confidence=self._confidence("skipped_aggregate_import"),
                    match_reason="Aggregate import skipped due to ambiguous account candidates",
                    matched_on_fields_json={
                        "aggregated_import": True,
                        "actor_origin": metadata.get("actor_origin"),
                        "actor_company_raw": event.actor_company_raw,
                        "candidate_account_ids": account_match_for_company.candidate_account_ids,
                    },
                    created_at=now,
                )

            return MatchResult(
                social_event_id=event.id,
                matched_contact_id=None,
                matched_account_id=None,
                match_type="skipped_aggregate_import",
                match_confidence=self._confidence("skipped_aggregate_import"),
                match_reason="Aggregate import skipped for person-level identity resolution",
                matched_on_fields_json={
                    "aggregated_import": True,
                    "actor_origin": metadata.get("actor_origin"),
                },
                created_at=now,
            )

        if actor_url_norm is not None:
            candidates = contacts_by_linkedin.get(actor_url_norm, [])
            if len(candidates) == 1:
                contact = candidates[0]
                return MatchResult(
                    social_event_id=event.id,
                    matched_contact_id=contact.id,
                    matched_account_id=contact.account_id,
                    match_type="exact_contact_linkedin_url",
                    match_confidence=self._confidence("exact_contact_linkedin_url"),
                    match_reason="Matched actor_linkedin_url to contacts.linkedin_url",
                    matched_on_fields_json={
                        "actor_linkedin_url_normalized": actor_url_norm,
                        "contact_id": contact.id,
                        "account_id": contact.account_id,
                    },
                    created_at=now,
                )

            if len(candidates) > 1:
                return MatchResult(
                    social_event_id=event.id,
                    matched_contact_id=None,
                    matched_account_id=None,
                    match_type="unresolved",
                    match_confidence=self._confidence("unresolved"),
                    match_reason="Ambiguous contacts for actor_linkedin_url",
                    matched_on_fields_json={
                        "actor_linkedin_url_normalized": actor_url_norm,
                        "candidate_contact_ids": [c.id for c in candidates],
                    },
                    created_at=now,
                )

        if (
            actor_name_norm
            and account_match_for_company.status == "matched"
            and account_match_for_company.match_type
            in {"exact_account_name", "normalized_account_name", "inferred_from_website_domain"}
            and account_match_for_company.account_id is not None
        ):
            account_id = account_match_for_company.account_id
            candidates = contacts_by_name_and_account.get((actor_name_norm, account_id), [])
            if len(candidates) == 1:
                contact = candidates[0]
                return MatchResult(
                    social_event_id=event.id,
                    matched_contact_id=contact.id,
                    matched_account_id=account_id,
                    match_type="exact_contact_name_and_account",
                    match_confidence=self._confidence("exact_contact_name_and_account"),
                    match_reason="Matched actor_name to contact full_name within matched account",
                    matched_on_fields_json={
                        "actor_name_normalized": actor_name_norm,
                        "account_id": account_id,
                        "contact_id": contact.id,
                    },
                    created_at=now,
                )

            if len(candidates) > 1:
                return MatchResult(
                    social_event_id=event.id,
                    matched_contact_id=None,
                    matched_account_id=None,
                    match_type="unresolved",
                    match_confidence=self._confidence("unresolved"),
                    match_reason="Ambiguous contact candidates for actor_name within matched account",
                    matched_on_fields_json={
                        "actor_name_normalized": actor_name_norm,
                        "account_id": account_id,
                        "candidate_contact_ids": [c.id for c in candidates],
                    },
                    created_at=now,
                )

        if account_match_for_company.status == "ambiguous":
            return MatchResult(
                social_event_id=event.id,
                matched_contact_id=None,
                matched_account_id=None,
                match_type="unresolved",
                match_confidence=self._confidence("unresolved"),
                match_reason="Ambiguous account candidates after normalization",
                matched_on_fields_json={
                    "actor_company_raw": event.actor_company_raw,
                    "candidate_account_ids": account_match_for_company.candidate_account_ids,
                },
                created_at=now,
            )

        if account_match_for_company.status == "matched" and account_match_for_company.account_id is not None:
            # Merged CRM semantics: for person-level events we require deterministic contact identity,
            # not just company/account inference. Keep account inference in metadata for auditability.
            return MatchResult(
                social_event_id=event.id,
                matched_contact_id=None,
                matched_account_id=None,
                match_type="unresolved",
                match_confidence=self._confidence("unresolved"),
                match_reason="Account matched, but no deterministic contact match in merged CRM model",
                matched_on_fields_json={
                    "actor_company_raw": event.actor_company_raw,
                    "account_match": {
                        "status": account_match_for_company.status,
                        "account_id": account_match_for_company.account_id,
                        "match_type": account_match_for_company.match_type,
                        "reason": account_match_for_company.reason,
                        "candidate_account_ids": account_match_for_company.candidate_account_ids,
                    },
                },
                created_at=now,
            )

        return MatchResult(
            social_event_id=event.id,
            matched_contact_id=None,
            matched_account_id=None,
            match_type="unresolved",
            match_confidence=self._confidence("unresolved"),
            match_reason="No deterministic contact or account match found",
            matched_on_fields_json={
                "actor_name_normalized": actor_name_norm,
                "actor_company_normalized": actor_company_norm,
            },
            created_at=now,
        )

    def _match_account(
        self,
        actor_company_exact_lower: str | None,
        actor_company_norm: str | None,
        actor_company_raw: str | None,
        accounts_by_exact_lower,
        accounts_by_normalized,
        accounts_by_domain,
    ) -> AccountMatchOutcome:
        if actor_company_exact_lower:
            exact_matches = accounts_by_exact_lower.get(actor_company_exact_lower, [])
            if len(exact_matches) == 1:
                account = exact_matches[0]
                return AccountMatchOutcome(
                    status="matched",
                    account_id=account.id,
                    match_type="exact_account_name",
                    reason="Matched actor_company_raw exactly to accounts.company_name",
                    candidate_account_ids=[account.id],
                )
            if len(exact_matches) > 1:
                return AccountMatchOutcome(
                    status="ambiguous",
                    account_id=None,
                    match_type=None,
                    reason="Ambiguous exact account name candidates",
                    candidate_account_ids=[account.id for account in exact_matches],
                )

        if actor_company_norm:
            normalized_matches = accounts_by_normalized.get(actor_company_norm, [])
            if len(normalized_matches) == 1:
                account = normalized_matches[0]
                return AccountMatchOutcome(
                    status="matched",
                    account_id=account.id,
                    match_type="normalized_account_name",
                    reason="Matched normalized actor_company_raw to accounts.company_name",
                    candidate_account_ids=[account.id],
                )
            if len(normalized_matches) > 1:
                return AccountMatchOutcome(
                    status="ambiguous",
                    account_id=None,
                    match_type=None,
                    reason="Ambiguous normalized account name candidates",
                    candidate_account_ids=[account.id for account in normalized_matches],
                )

        if looks_like_domain(actor_company_raw):
            domain = normalize_domain(actor_company_raw)
            if domain:
                domain_matches = accounts_by_domain.get(domain, [])
                if len(domain_matches) == 1:
                    account = domain_matches[0]
                    return AccountMatchOutcome(
                        status="matched",
                        account_id=account.id,
                        match_type="inferred_from_website_domain",
                        reason="Matched actor_company_raw domain to accounts.domain",
                        candidate_account_ids=[account.id],
                    )
                if len(domain_matches) > 1:
                    return AccountMatchOutcome(
                        status="ambiguous",
                        account_id=None,
                        match_type=None,
                        reason="Ambiguous account domain candidates",
                        candidate_account_ids=[account.id for account in domain_matches],
                    )

        if actor_company_norm:
            token_matches = []
            for normalized_name, accounts in accounts_by_normalized.items():
                if actor_company_norm in normalized_name or normalized_name in actor_company_norm:
                    token_matches.extend(accounts)

            unique_ids = {account.id for account in token_matches}
            if len(unique_ids) == 1:
                matched_account = next(account for account in token_matches if account.id in unique_ids)
                return AccountMatchOutcome(
                    status="matched",
                    account_id=matched_account.id,
                    match_type="inferred_from_actor_company",
                    reason="Inferred account from normalized actor_company_raw token overlap",
                    candidate_account_ids=[matched_account.id],
                )
            if len(unique_ids) > 1:
                return AccountMatchOutcome(
                    status="ambiguous",
                    account_id=None,
                    match_type=None,
                    reason="Ambiguous token-overlap account candidates",
                    candidate_account_ids=sorted(unique_ids),
                )

        return AccountMatchOutcome(
            status="none",
            account_id=None,
            match_type=None,
            reason="No account match",
            candidate_account_ids=[],
        )

    def _upsert_results(self, cur, results: list[MatchResult]) -> None:
        if not results:
            return

        rows = [
            (
                result.social_event_id,
                result.matched_contact_id,
                result.matched_account_id,
                result.match_type,
                Decimal(str(round(result.match_confidence, 2))),
                result.match_reason,
                Json(result.matched_on_fields_json),
                result.created_at,
            )
            for result in results
        ]

        query = """
            INSERT INTO social_event_matches (
                social_event_id,
                matched_contact_id,
                matched_account_id,
                match_type,
                match_confidence,
                match_reason,
                matched_on_fields_json,
                created_at
            ) VALUES %s
            ON CONFLICT (social_event_id) DO UPDATE
            SET
                matched_contact_id = EXCLUDED.matched_contact_id,
                matched_account_id = EXCLUDED.matched_account_id,
                match_type = EXCLUDED.match_type,
                match_confidence = EXCLUDED.match_confidence,
                match_reason = EXCLUDED.match_reason,
                matched_on_fields_json = EXCLUDED.matched_on_fields_json,
                created_at = EXCLUDED.created_at
        """
        execute_values(cur, query, rows)

    def _build_summary(self, results: list[MatchResult]) -> dict[str, int]:
        summary = {
            "events_processed": 0,
            "contact_matches": 0,
            "account_only_matches": 0,
            "unresolved": 0,
            "skipped_aggregate_imports": 0,
        }

        for result in results:
            if result.match_type == "skipped_aggregate_import":
                summary["skipped_aggregate_imports"] += 1
            elif result.match_type == "unresolved":
                summary["events_processed"] += 1
                summary["unresolved"] += 1
            elif result.matched_contact_id is not None:
                summary["events_processed"] += 1
                summary["contact_matches"] += 1
            elif result.matched_account_id is not None:
                summary["events_processed"] += 1
                summary["account_only_matches"] += 1

        return summary

    def _confidence(self, match_type: str) -> float:
        return CONFIDENCE_BY_MATCH_TYPE[match_type]
