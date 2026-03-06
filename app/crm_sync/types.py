from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class CRMAccountRow:
    crm_account_id: str
    company_name: str
    domain: str | None
    industry: str | None
    employee_band: str | None
    owner_name: str | None
    lifecycle_stage: str | None
    target_tier: str | None
    status: str | None
    last_activity_date: str | None


@dataclass(slots=True)
class CRMContactRow:
    crm_contact_id: str
    crm_account_id: str
    full_name: str
    title: str | None
    email: str | None
    linkedin_url: str | None
    phone: str | None
    status: str | None

