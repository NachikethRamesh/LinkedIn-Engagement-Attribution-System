# n8n Workflow Artifacts

## Purpose

These workflows orchestrate the existing API layer only. They do not contain ingestion, matching, scoring, or attribution business logic.

Folder:
- `n8n/`

Files:
- `n8n/full_refresh_pipeline.json`
- `n8n/incremental_pipeline.json`
- `n8n/stage_run_or_retry.json`
- `n8n/failure_notification.json`
- `n8n/sample_requests.md`

## Import and Configure

1. Open n8n and import each JSON file.
2. In each workflow, edit the first `Set` node values:
- `api_base_url`
- `notification_webhook_url`
- `poll_interval_seconds`
- `max_poll_attempts`
- default payload values (`source`, `rebuild`, `posts`, `events`, `window_days`)
3. Activate the workflows you want to run.
4. For `failure_notification`, set `notification_webhook_url` in request payload or keep the placeholder fallback and replace it in the workflow.

## Workflow Behavior

### `full_refresh_pipeline`

Use for rebuild-oriented runs (`rebuild=true`).

Flow:
- Manual Trigger
- `GET /health` preflight
- `POST /jobs/full-pipeline`
- Poll `GET /jobs/{run_id}` until `success|failed`
- On success: metric sanity checks
- Route to success, warning, or failure notification

### `incremental_pipeline`

Use for recurring incremental runs (`rebuild=false`).

Flow is the same as full refresh with different defaults.

### `stage_run_or_retry`

Use for manual remediation/debug of a single stage.

Supported `stage_name` values:
- `linkedin_ingestion_csv`
- `linkedin_ingestion_mock`
- `identity_resolution`
- `intent_scoring`
- `opportunity_attribution`

Flow:
- Manual Trigger
- Build stage endpoint/payload
- Start stage run
- Poll run status
- Optional retry via `max_run_retries`
- Success/failure notification

### `failure_notification`

Standalone notification sink.

Flow:
- Webhook Trigger (`POST /webhook/pipeline-failure`)
- Normalize inbound payload
- Send notification only when status is `failed` or `warning`

## API Mapping

- `GET /health`
- `POST /jobs/full-pipeline`
- `POST /jobs/linkedin-ingestion/csv`
- `POST /jobs/linkedin-ingestion/mock`
- `POST /jobs/identity-resolution`
- `POST /jobs/intent-scoring`
- `POST /jobs/opportunity-attribution`
- `GET /jobs/{run_id}`

## Polling and Branching

Polling is implemented in a minimal Code node:
- fetch run status
- sleep for `poll_interval_seconds`
- stop on terminal status
- fail when poll attempts exceed `max_poll_attempts`
- terminal success condition: `GET /jobs/{run_id}` returns `status == "success"`
- terminal failure condition: `GET /jobs/{run_id}` returns `status == "failed"` or poll timeout is reached

Branches:
- success
- warning (success with suspicious metrics)
- failed

## Metric Sanity Checks (full/incremental)

Checks inspect `output_metrics_json.stages` and flag warning when:
- ingestion stage metrics are missing
- `identity_resolution.events_processed <= 0`
- `intent_scoring.rows_written <= 0`
- `opportunity_attribution.opportunities_processed <= 0`

Metric checks read from:
- `run.output_metrics_json`
- `run.output_metrics_json.stages`

## Placeholder Integrations

Notification nodes use an HTTP webhook placeholder:
- `https://example.com/replace-with-slack-or-email-webhook`

Replace with:
- Slack Incoming Webhook
- internal webhook relay
- email gateway endpoint

Failure payload normalization includes:
- `run_id`
- `job_name`
- `status`
- `error_message`
- `metric_summary` (compact stage summary when available)

## Diagram

`Trigger -> Health Check -> Start Job -> Poll Status -> Success/Warning/Failure -> Notify`

## Scheduling Note

Exports use Manual Trigger for deterministic imports. For scheduled runs, add an n8n `Schedule Trigger` and connect it to the same `Set Config` node.

## Local Validation

Outside n8n, run:

```powershell
python scripts/verify_n8n_artifacts.py
```

This validates:
- workflow JSON parseability
- required nodes/keys/connections presence
- expected endpoint and polling strings
- docs files presence

## Failure Notification Test

After importing `failure_notification.json`, test webhook path with:

```powershell
curl -X POST http://localhost:5678/webhook/pipeline-failure ^
  -H "Content-Type: application/json" ^
  -d "{\"status\":\"failed\",\"run_id\":\"demo-run-1\",\"job_name\":\"full_pipeline\",\"error_message\":\"Simulated failure\",\"output_metrics_json\":{\"stages\":{\"identity_resolution\":{\"events_processed\":0}}},\"notification_webhook_url\":\"https://example.com/replace-with-slack-or-email-webhook\"}"
```

Assumptions:
- n8n instance supports Code node JavaScript runtime with `fetch`.
- placeholder webhook URLs are replaced before production/demo notification testing.
