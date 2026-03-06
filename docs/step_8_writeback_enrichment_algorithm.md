# Step 8 Algorithm: Writeback + Enrichment

## Goal
Operationalize outputs by sending selected entities outward and ingesting normalized enrichment inward.

## Outbound Selection Modes
- High-intent accounts
- Socially influenced opportunities
- Low-confidence promising accounts
- Unresolved account candidates (Exa assistive research)

## Process
1. Select deterministic candidates.
2. Build target-specific payloads (`crm`, `clay`, `exa`, `webhook_generic`).
3. Execute adapter in live or simulated mode.
4. Record run + per-record statuses (`writeback_runs`, `writeback_records`).
5. Ingest normalized enrichment results (`enrichment_results`).

## Exa Assistive Rule
- Exa can enrich unresolved candidates.
- Exa does not directly mutate deterministic match decisions.
- Any downstream rematch is explicit and rerun via identity job.

## Outputs
- Audit-ready writeback logs.
- Enrichment data available for CRM enrichment and follow-up workflows.

