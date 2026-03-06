# Step 6 Algorithm: Orchestration Layer

## Goal
Provide one deterministic trigger/status interface for CLI/API/n8n.

## Inputs
- Job request (`job_name`, params, trigger source)

## Process
1. Create run record (`pipeline_runs`) with `running`.
2. Execute selected stage(s) using existing business modules.
3. Capture output metrics and timings.
4. Mark run `success` or `failed` with error details.

## Jobs
- `linkedin_ingestion_csv`
- `linkedin_ingestion_mock`
- `linkedin_ingestion_org_url`
- `identity_resolution`
- `intent_scoring`
- `opportunity_attribution`
- `full_pipeline`

## Outputs
- API endpoints for trigger/poll.
- Structured metrics in `output_metrics_json`.
- Audit trail in `pipeline_runs`.

