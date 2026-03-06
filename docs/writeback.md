# Writeback And Enrichment Layer

## Purpose

Step 8 adds a deterministic outbound handoff + inbound enrichment writeback layer while keeping local Postgres as the source of truth.

This layer:
- selects eligible entities from local scoring/attribution outputs
- builds target-specific payloads
- sends (or dry-runs) outbound records
- tracks run-level and record-level statuses
- ingests normalized enrichment results back into local tables

This layer does **not**:
- move attribution logic to external systems
- treat CRM/Clay/Exa as source-of-truth for intent/influence decisions

## Target Types

Supported `target_type`:
- `crm`
- `clay`
- `exa`
- `webhook_generic`

Simulation support:
- `simulate_local` is disabled for `clay` and `exa`.
- Configure real endpoint URLs and credentials for delivery.

## Selection Modes

Supported `selection_mode`:
- `high_intent_accounts`
- `socially_influenced_opportunities`
- `low_confidence_promising_accounts`
- `unresolved_account_candidates`

Defaults by target:
- `crm` -> `high_intent_accounts`
- `clay` -> `low_confidence_promising_accounts`
- `exa` -> `unresolved_account_candidates`
- `webhook_generic` -> `socially_influenced_opportunities`

Core configurable filters:
- `limit`
- `min_intent_score`
- `min_intent_confidence`
- `max_intent_confidence`
- `score_window`
- `min_influence_band`
- `min_influence_score`

Default selector guardrails:
- `high_intent_accounts`:
  - `min_intent_score=55`
  - `min_intent_confidence=0.60`
  - `min_contributing_events=3`
  - `min_unique_stakeholders=1`
- `socially_influenced_opportunities`:
  - `min_influence_band=medium`
  - `min_influence_score=40`
  - `min_influence_confidence=0.45`
- `low_confidence_promising_accounts`:
  - `min_intent_score=50`
  - `max_intent_confidence=0.65`
  - must also have either `contributing_event_count>=2` or `latest_influence_score>=30`
- `unresolved_account_candidates`:
  - sourced from `social_event_matches` unresolved rows and weak account-only matches below threshold
  - requires `actor_company_raw` and non-trivial signal evidence
  - defaults:
    - `weak_match_confidence_threshold=0.70`
    - `min_contributing_events=3`
    - `min_strong_signals=1`
    - `min_recent_signals=1` within `recent_days=30`
    - generic placeholders excluded by default (`include_generic_candidates=false`)

## Operational Tables

### `writeback_runs`

Tracks each writeback execution:
- `writeback_run_id`
- `target_type`
- `status`
- timing fields
- `trigger_source`
- `selection_params_json`
- `result_metrics_json`
- `error_message`

### `writeback_records`

Tracks each outbound entity payload inside a run:
- run/entity identity
- deterministic payload snapshot
- per-record status (`pending|sent|success|failed|skipped`)
- response/error metadata

### `enrichment_results`

Stores normalized enrichment responses:
- `target_type`
- `entity_type`
- `entity_id`
- `enrichment_type`
- `normalized_data_json`
- `source_run_id`
- `notes`
- `dedupe_key` (unique replay safety)

## Payload Contracts

### CRM account activation payload

Includes:
- `account_id`
- `company_name`
- `domain`
- `latest_intent_score`
- `latest_intent_confidence`
- `score_window`
- `score_reason`
- `latest_influence_band`
- `latest_influence_score`
- `recommended_action`
- `source_system`

### Clay enrichment payload

Includes:
- `account_id`
- `company_name`
- `domain`
- `enrichment_context`
- `weak_match_reasons`
- `latest_intent_score`
- `latest_influence_score`
- `enrichment_goal`

### Exa research payload

Includes:
- `account_id`
- `company_name`
- `domain`
- `research_goal`
- `research_context`

For unresolved candidate research:
- `entity_type=unresolved_account_candidate`
- `candidate_id` (stable deterministic key)
- `candidate_company_name_raw`
- `candidate_company_name_normalized`
- `supporting_signal_summary`
- `strongest_signal_type`
- `recent_signal_count`
- `weak_match_reason`
- `source_social_event_ids`

## Delivery Behavior

- `dry_run=true` writes record rows with `status=skipped` and payload preview, no external POST.
- dry-run is evaluated before replay-skip, so payload previews remain visible even for previously successful entities.
- For `crm`, `clay`, `exa`: if endpoint is not configured, adapter returns explicit deterministic stub success.
- For `webhook_generic`: endpoint is required; missing/failed endpoint records explicit failure.

Endpoint resolution order:
1. `endpoint_url` request parameter
2. env var by target:
   - `WRITEBACK_CRM_URL`
   - `WRITEBACK_CLAY_URL`
   - `WRITEBACK_EXA_URL`
   - `WRITEBACK_WEBHOOK_GENERIC_URL`

Optional auth header env vars (used when endpoint requires auth):
- `CRM_API_KEY` (or `WRITEBACK_CRM_API_KEY`)
- `CLAY_API_KEY` (or `WRITEBACK_CLAY_API_KEY`)
- `EXA_API_KEY` (or `WRITEBACK_EXA_API_KEY`)
- `WEBHOOK_GENERIC_SECRET` + optional `WEBHOOK_GENERIC_SECRET_HEADER` (default `X-Webhook-Secret`)

Local simulation artifact generation is disabled.

## Unresolved Candidate Research Role

- Exa is used as a research assist for unresolved identity candidates.
- Exa does not directly mutate `social_event_matches` or deterministic matching outputs.
- Enrichment is ingested into `enrichment_results` with `entity_type=unresolved_account_candidate` for review/CRM enrichment workflows.

Stable candidate identity:
- `candidate_id` is deterministic from normalized company candidate text.
- reused across writeback and enrichment ingestion for replay-safe dedupe behavior.

## Idempotency And Replay Safety

- `writeback_records` unique per run/entity/target.
- optional replay guard (`skip_if_previously_successful=true` default):
  - if entity already has prior `success` for same target, new record is created as `skipped`.
- replay outcomes are explicit in record `status` plus run metrics (`replay_skipped_count`).
- `enrichment_results` uses deterministic `dedupe_key` to avoid duplicate inserts.

## CLI

Run writeback:

```powershell
python -m app.writeback.run_writeback --target-type crm --selection-mode high_intent_accounts --limit 25 --dry-run
python -m app.writeback.run_writeback --target-type clay --selection-mode low_confidence_promising_accounts --limit 20
python -m app.writeback.run_writeback --target-type clay --selection-mode low_confidence_promising_accounts --endpoint-url https://your-endpoint.example --limit 20
python -m app.writeback.run_writeback --target-type exa --selection-mode unresolved_account_candidates --endpoint-url https://your-endpoint.example --limit 10
python -m app.writeback.run_writeback --target-type webhook_generic --selection-mode socially_influenced_opportunities --endpoint-url http://localhost:9001/webhook
```

Ingest normalized enrichment results:

```powershell
python -m app.writeback.ingest_enrichment --file data/sample_clay_enrichment_result.json
python -m app.writeback.ingest_enrichment --file data/sample_exa_enrichment_result.json
python -m app.writeback.ingest_enrichment --file data/sample_exa_unresolved_candidate_result.json
python -m app.writeback.ingest_enrichment --file data/sample_crm_response.json
```

Inspect and verify:

```powershell
python scripts/inspect_writeback_runs.py
python scripts/inspect_writeback_summary.py
python scripts/inspect_unresolved_candidates.py
python scripts/verify_writeback.py --simulate-replay --print-snapshot
python scripts/verify_unresolved_account_research_flow.py
python scripts/verify_writeback.py --dry-run-only
python scripts/verify_simulated_clay_flow.py
```

## API Endpoints (n8n-friendly)

- `POST /writeback/run`
- `GET /writeback/runs`
- `GET /writeback/runs/{writeback_run_id}`
- `POST /writeback/enrichment-results`

Example `POST /writeback/run` body:

```json
{
  "target_type": "crm",
  "selection_mode": "high_intent_accounts",
  "limit": 25,
  "min_intent_score": 60,
  "min_intent_confidence": 0.65,
  "score_window": "rolling_30d",
  "dry_run": false,
  "skip_if_previously_successful": true,
  "trigger_source": "api"
}
```

Simulated Clay API example:

```json
{
  "target_type": "clay",
  "selection_mode": "low_confidence_promising_accounts",
  "limit": 10,
  "simulate_local": false,
  "skip_if_previously_successful": false,
  "trigger_source": "api"
}
```

Example `POST /writeback/enrichment-results` body:

```json
{
  "trigger_source": "api",
  "results": [
    {
      "target_type": "clay",
      "entity_type": "account",
      "entity_id": 1,
      "enrichment_type": "firmographic_refresh",
      "normalized_data_json": {
        "industry": "B2B SaaS",
        "employee_band": "201-500"
      },
      "source_run_id": "writeback-run-123"
    }
  ]
}
```
