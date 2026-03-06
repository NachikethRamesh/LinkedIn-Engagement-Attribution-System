from __future__ import annotations

import hashlib
from typing import Any


class ExaCRMEnrichmentService:
    """Apply normalized Exa enrichment into CRM source-of-truth tables (accounts/contacts)."""

    def apply(self, cur, results: list[dict[str, Any]]) -> dict[str, int]:
        accounts_created = 0
        accounts_updated = 0
        contacts_created = 0
        contacts_updated = 0

        for result in results:
            if str(result.get("target_type", "")).lower() != "exa":
                continue
            if str(result.get("entity_type", "")).lower() != "unresolved_account_candidate":
                continue

            normalized_data = result.get("normalized_data_json") or {}
            if not isinstance(normalized_data, dict):
                continue

            crm = normalized_data.get("crm_enrichment") or {}
            if not isinstance(crm, dict):
                crm = {}

            company_name = (
                str(crm.get("company_name") or normalized_data.get("likely_company_name") or "").strip()
            )
            domain = str(crm.get("domain") or normalized_data.get("likely_domain") or "").strip().lower() or None
            if not company_name:
                continue

            cur.execute(
                """
                SELECT id, domain
                FROM accounts
                WHERE lower(company_name) = lower(%s)
                LIMIT 1
                """,
                (company_name,),
            )
            account_row = cur.fetchone()
            if account_row is None:
                exa_sim_account_id = f"exa_sim:{_slug(company_name)}:{_short_hash(company_name)}"
                cur.execute(
                    """
                    INSERT INTO accounts (company_name, domain, target_tier, crm_account_id)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                    """,
                    (company_name, domain, "tier_3", exa_sim_account_id),
                )
                account_id = int(cur.fetchone()[0])
                accounts_created += 1
            else:
                account_id = int(account_row[0])
                existing_domain = account_row[1]
                if domain and (existing_domain or "").strip().lower() != domain:
                    cur.execute("UPDATE accounts SET domain = %s WHERE id = %s", (domain, account_id))
                    accounts_updated += 1

            primary_contact = {
                "contact_full_name": crm.get("contact_full_name"),
                "contact_title": crm.get("contact_title"),
                "contact_email": crm.get("contact_email"),
                "contact_linkedin_url": crm.get("contact_linkedin_url"),
            }
            additional_contacts = crm.get("additional_contacts") if isinstance(crm.get("additional_contacts"), list) else []
            all_contacts = [primary_contact] + [c for c in additional_contacts if isinstance(c, dict)]

            for c in all_contacts:
                contact_name = str(c.get("contact_full_name") or "").strip()
                contact_title = str(c.get("contact_title") or "").strip() or None
                contact_email = str(c.get("contact_email") or "").strip().lower() or None
                contact_linkedin_url = str(c.get("contact_linkedin_url") or "").strip() or None

                if not contact_name:
                    continue

                existing_contact_id = None
                if contact_linkedin_url:
                    cur.execute(
                        """
                        SELECT id
                        FROM contacts
                        WHERE lower(linkedin_url) = lower(%s)
                        LIMIT 1
                        """,
                        (contact_linkedin_url,),
                    )
                    row = cur.fetchone()
                    if row is not None:
                        existing_contact_id = int(row[0])

                if existing_contact_id is None and contact_email:
                    cur.execute(
                        """
                        SELECT id
                        FROM contacts
                        WHERE lower(email) = lower(%s)
                        LIMIT 1
                        """,
                        (contact_email,),
                    )
                    row = cur.fetchone()
                    if row is not None:
                        existing_contact_id = int(row[0])

                if existing_contact_id is None:
                    cur.execute(
                        """
                        SELECT id
                        FROM contacts
                        WHERE account_id = %s
                          AND lower(full_name) = lower(%s)
                        LIMIT 1
                        """,
                        (account_id, contact_name),
                    )
                    row = cur.fetchone()
                    if row is not None:
                        existing_contact_id = int(row[0])

                if existing_contact_id is None:
                    exa_sim_contact_id = f"exa_sim:{_slug(contact_name)}:{_short_hash(contact_name + str(account_id))}"
                    cur.execute(
                        """
                        INSERT INTO contacts (account_id, crm_contact_id, full_name, email, linkedin_url, title)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (account_id, exa_sim_contact_id, contact_name, contact_email, contact_linkedin_url, contact_title),
                    )
                    contacts_created += 1
                else:
                    cur.execute(
                        """
                        UPDATE contacts
                        SET
                            account_id = %s,
                            full_name = COALESCE(NULLIF(%s, ''), full_name),
                            email = COALESCE(%s, email),
                            linkedin_url = COALESCE(%s, linkedin_url),
                            title = COALESCE(%s, title)
                        WHERE id = %s
                        """,
                        (
                            account_id,
                            contact_name,
                            contact_email,
                            contact_linkedin_url,
                            contact_title,
                            existing_contact_id,
                        ),
                    )
                    contacts_updated += 1

        return {
            "accounts_created": accounts_created,
            "accounts_updated": accounts_updated,
            "contacts_created": contacts_created,
            "contacts_updated": contacts_updated,
        }


def _slug(value: str) -> str:
    return "-".join(value.lower().strip().split())[:48]


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]
