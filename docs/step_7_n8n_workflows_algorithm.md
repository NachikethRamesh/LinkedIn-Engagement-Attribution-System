# Step 7 Algorithm: n8n Workflow Artifacts

## Goal
Use n8n for control-plane orchestration only (no business logic migration).

## Workflow Pattern
1. Trigger (manual/schedule).
2. Health check (`GET /health`).
3. Start job (`POST /jobs/...`).
4. Poll run (`GET /jobs/{run_id}`).
5. Branch on `success`/`failed`.
6. Optional metric sanity checks.
7. Notify/handoff.

## Artifact Set
- `full_refresh_pipeline.json`
- `incremental_pipeline.json`
- `stage_run_or_retry.json`
- `failure_notification.json`

## Determinism Rules
- n8n does not compute matching/scoring/attribution.
- It only triggers and observes backend jobs.

