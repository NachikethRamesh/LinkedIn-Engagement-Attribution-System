from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from app.crm_sync.types import CRMAccountRow, CRMContactRow
from app.db import get_connection


def clean(value: str | None) -> str | None:
    if value is None:
        return None
    parsed = value.strip()
    return parsed or None


def load_accounts_csv(path: Path) -> list[CRMAccountRow]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows: list[CRMAccountRow] = []
        for row in reader:
            crm_account_id = clean(row.get("crm_account_id"))
            company_name = clean(row.get("company_name"))
            if not crm_account_id or not company_name:
                continue
            rows.append(
                CRMAccountRow(
                    crm_account_id=crm_account_id,
                    company_name=company_name,
                    domain=clean(row.get("domain")),
                    industry=clean(row.get("industry")),
                    employee_band=clean(row.get("employee_band")),
                    owner_name=clean(row.get("owner_name")),
                    lifecycle_stage=clean(row.get("lifecycle_stage")),
                    target_tier=clean(row.get("target_tier")),
                    status=clean(row.get("status")),
                    last_activity_date=clean(row.get("last_activity_date")),
                )
            )
    return rows


def load_contacts_csv(path: Path) -> list[CRMContactRow]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows: list[CRMContactRow] = []
        for row in reader:
            crm_contact_id = clean(row.get("crm_contact_id"))
            crm_account_id = clean(row.get("crm_account_id"))
            full_name = clean(row.get("full_name"))
            if not crm_contact_id or not crm_account_id or not full_name:
                continue
            rows.append(
                CRMContactRow(
                    crm_contact_id=crm_contact_id,
                    crm_account_id=crm_account_id,
                    full_name=full_name,
                    title=clean(row.get("title")),
                    email=clean(row.get("email")),
                    linkedin_url=clean(row.get("linkedin_url")),
                    phone=clean(row.get("phone")),
                    status=clean(row.get("status")),
                )
            )
    return rows


class CRMSyncService:
    def run(self, accounts_file: str, contacts_file: str) -> dict[str, Any]:
        account_rows = load_accounts_csv(Path(accounts_file))
        contact_rows = load_contacts_csv(Path(contacts_file))

        summary = {
            "accounts_rows_read": len(account_rows),
            "contacts_rows_read": len(contact_rows),
            "accounts_created": 0,
            "accounts_updated": 0,
            "contacts_created": 0,
            "contacts_updated": 0,
            "contacts_skipped_missing_account": 0,
        }

        with get_connection() as conn:
            with conn.cursor() as cur:
                for row in account_rows:
                    cur.execute(
                        """
                        SELECT id FROM accounts WHERE crm_account_id = %s
                        """,
                        (row.crm_account_id,),
                    )
                    existing = cur.fetchone()

                    target_tier = row.target_tier or "Tier 3"
                    if existing:
                        cur.execute(
                            """
                            UPDATE accounts
                            SET
                                company_name = %s,
                                domain = COALESCE(%s, domain),
                                target_tier = COALESCE(%s, target_tier)
                            WHERE id = %s
                            """,
                            (row.company_name, row.domain, target_tier, existing[0]),
                        )
                        summary["accounts_updated"] += 1
                        continue

                    cur.execute(
                        """
                        SELECT id FROM accounts WHERE LOWER(company_name) = LOWER(%s) OR (domain IS NOT NULL AND LOWER(domain) = LOWER(%s))
                        LIMIT 1
                        """,
                        (row.company_name, row.domain or ""),
                    )
                    fallback = cur.fetchone()
                    if fallback:
                        cur.execute(
                            """
                            UPDATE accounts
                            SET
                                crm_account_id = %s,
                                company_name = %s,
                                domain = COALESCE(%s, domain),
                                target_tier = COALESCE(%s, target_tier)
                            WHERE id = %s
                            """,
                            (row.crm_account_id, row.company_name, row.domain, target_tier, fallback[0]),
                        )
                        summary["accounts_updated"] += 1
                    else:
                        cur.execute(
                            """
                            INSERT INTO accounts (crm_account_id, company_name, domain, target_tier)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (row.crm_account_id, row.company_name, row.domain, target_tier),
                        )
                        summary["accounts_created"] += 1

                account_map: dict[str, int] = {}
                cur.execute("SELECT id, crm_account_id FROM accounts WHERE crm_account_id IS NOT NULL;")
                for account_id, crm_account_id in cur.fetchall():
                    account_map[str(crm_account_id)] = int(account_id)

                for row in contact_rows:
                    account_id = account_map.get(row.crm_account_id)
                    if account_id is None:
                        summary["contacts_skipped_missing_account"] += 1
                        continue

                    cur.execute(
                        """
                        SELECT id FROM contacts WHERE crm_contact_id = %s
                        """,
                        (row.crm_contact_id,),
                    )
                    existing = cur.fetchone()
                    if existing:
                        cur.execute(
                            """
                            UPDATE contacts
                            SET
                                account_id = %s,
                                full_name = %s,
                                email = COALESCE(%s, email),
                                linkedin_url = COALESCE(%s, linkedin_url),
                                title = COALESCE(%s, title)
                            WHERE id = %s
                            """,
                            (account_id, row.full_name, row.email, row.linkedin_url, row.title, existing[0]),
                        )
                        summary["contacts_updated"] += 1
                        continue

                    cur.execute(
                        """
                        SELECT id
                        FROM contacts
                        WHERE account_id = %s
                          AND (
                              (email IS NOT NULL AND LOWER(email) = LOWER(%s))
                              OR (linkedin_url IS NOT NULL AND LOWER(linkedin_url) = LOWER(%s))
                              OR LOWER(full_name) = LOWER(%s)
                          )
                        LIMIT 1
                        """,
                        (account_id, row.email or "", row.linkedin_url or "", row.full_name),
                    )
                    fallback = cur.fetchone()
                    if fallback:
                        cur.execute(
                            """
                            UPDATE contacts
                            SET
                                crm_contact_id = %s,
                                full_name = %s,
                                email = COALESCE(%s, email),
                                linkedin_url = COALESCE(%s, linkedin_url),
                                title = COALESCE(%s, title)
                            WHERE id = %s
                            """,
                            (row.crm_contact_id, row.full_name, row.email, row.linkedin_url, row.title, fallback[0]),
                        )
                        summary["contacts_updated"] += 1
                    else:
                        cur.execute(
                            """
                            INSERT INTO contacts (account_id, crm_contact_id, full_name, email, linkedin_url, title)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (account_id, row.crm_contact_id, row.full_name, row.email, row.linkedin_url, row.title),
                        )
                        summary["contacts_created"] += 1

            conn.commit()

        return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Load simulated CRM CSV accounts/contacts into local Postgres.")
    parser.add_argument("--accounts-file", required=True)
    parser.add_argument("--contacts-file", required=True)
    args = parser.parse_args()

    service = CRMSyncService()
    summary = service.run(accounts_file=args.accounts_file, contacts_file=args.contacts_file)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
