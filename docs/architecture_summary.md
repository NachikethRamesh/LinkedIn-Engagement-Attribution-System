# Architecture Summary

## Pipeline Stages

1. **LinkedIn Ingestion**
- Inputs: CSV presets, generic CSV, mock batches, API scaffold
- Outputs: `posts`, `social_events`, `imports_log`

2. **Identity Resolution**
- Deterministic matching from social events to contacts/accounts
- Outputs: `social_event_matches`, `v_social_event_match_status`

3. **Intent Scoring**
- Account-level rolling scores from matched social + website signals
- Outputs: `account_intent_scores`, `v_latest_account_intent_status`

4. **Opportunity Attribution**
- Pre-opportunity influence classification and scoring
- Outputs: `opportunity_influence`, `v_opportunity_influence_status`

5. **Orchestration Layer**
- Triggers jobs and tracks operational status
- Outputs: `pipeline_runs`, API run endpoints

6. **Writeback + Enrichment**
- Selects actionable entities, sends outbound payloads, ingests normalized enrichment
- Outputs: `writeback_runs`, `writeback_records`, `enrichment_results`
  - Includes unresolved identity candidate research path (`unresolved_account_candidates` -> Exa)

7. **Simulation Layer (Demo Realism)**
- CRM CSV sync (`app/crm_sync`) updates local `accounts`/`contacts` using deterministic upserts.
- Clay simulated-local adapter writes outbound/inbound files for realistic handoff demos.

## System Boundaries

- **Source of truth**: local Postgres + deterministic Python services.
- **External systems (CRM/Clay/Exa)**: activation/enrichment sinks/sources, not scoring/attribution owners.
- **Exa unresolved-candidate research**: assistive research for manual review/CRM enrichment, not direct identity mutation.
- **Simulation mode**: local CSV/files mimic CRM + Clay interactions for deterministic demos.
- **n8n**: orchestration client (trigger, poll, branch, notify), not business-logic engine.

## Data Model Role Map

- Core activity: `posts`, `social_events`, `website_events`
- Commercial context: `accounts`, `contacts`, `opportunities`
- Derived analytics: `social_event_matches`, `account_intent_scores`, `opportunity_influence`
- Operations: `pipeline_runs`, `imports_log`, `writeback_runs`, `writeback_records`, `enrichment_results`

## Why This Is a Business Tool

- Turns noisy social activity into account/opportunity-level operating signals.
- Adds explainability at every layer (match reasons, score reasons, attribution notes).
- Provides action paths (writeback payloads) instead of only reporting.
