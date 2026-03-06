# Orchestration Layer (n8n-facing)

## Purpose

This layer wraps existing stage CLIs/services into a single deterministic orchestration interface.

It is designed for tools like n8n to:
- trigger jobs
- receive a `run_id`
- poll run status
- read output metrics

Core business logic remains in stage modules (ingestion, matching, scoring, attribution).

## Architecture

Location: `app/orchestration/`
- `job_runner.py`: executes stage/full jobs and records run state
- `status_store.py`: DB read/write for `pipeline_runs`
- `pipeline.py`: CLI wrapper using the same job runner logic
- `api.py`: FastAPI endpoints for orchestration triggers and status polling
- `types.py`: run/job data types

## Jobs supported

- `linkedin_ingestion_csv`
- `linkedin_ingestion_mock`
- `linkedin_ingestion_org_url`
- `identity_resolution`
- `intent_scoring`
- `opportunity_attribution`
- `full_pipeline`

All jobs are synchronous for MVP and still persisted in `pipeline_runs`.

## Run tracking table

`pipeline_runs` fields:
- `run_id`
- `job_name`
- `stage_name`
- `status` (`queued|running|success|failed`)
- `started_at`
- `completed_at`
- `duration_ms`
- `trigger_source` (`manual|api|n8n|scheduled`)
- `input_params_json`
- `output_metrics_json`
- `error_message`

## CLI usage

Run full pipeline (mock):

```powershell
python -m app.orchestration.pipeline run-full --source mock --posts 20 --events 250 --rebuild
```

Run full pipeline (CSV):

```powershell
python -m app.orchestration.pipeline run-full --source shield_csv --file data/shield_sample.csv --rebuild
```

Run single stage:

```powershell
python -m app.orchestration.pipeline run-stage intent_scoring --rebuild
python -m app.orchestration.pipeline run-stage opportunity_attribution --rebuild --window-days 30
python -m app.orchestration.pipeline run-stage linkedin_ingestion_org_url --post-url "https://www.linkedin.com/posts/<REDACTED_POST>"  --run-pipeline
```

Inspect runs:

```powershell
python -m app.orchestration.pipeline list-runs --limit 20
python -m app.orchestration.pipeline list-runs --limit 20 --json
python -m app.orchestration.pipeline get-run --run-id <RUN_ID>
```

`list-runs` default output is intentionally compact for terminal debugging and includes:
- `run_id`
- `job_name`
- `status`
- `trigger_source`
- `started_at`
- `duration_ms`
- compact metrics summary (or short error summary)

## API endpoints

Start API:

```powershell
uvicorn app.orchestration.api:app --host 127.0.0.1 --port 8000
```

Health:
- `GET /health`

Run jobs:
- `POST /jobs/linkedin-ingestion/csv`
- `POST /jobs/linkedin-ingestion/mock`
- `POST /jobs/linkedin-ingestion/org-url`
- `POST /jobs/identity-resolution`
- `POST /jobs/intent-scoring`
- `POST /jobs/opportunity-attribution`
- `POST /jobs/full-pipeline`

Status:
- `GET /jobs/{run_id}`
- `GET /jobs?limit=100`

UI summaries (read-only helper endpoints):
- `GET /ui/ingestion-latest`
- `GET /ui/identity-summary`
- `GET /ui/intent-summary?window=rolling_30d`
- `GET /ui/opportunity-summary`

Writeback:
- `POST /writeback/run`
- `GET /writeback/runs`
- `GET /writeback/runs/{writeback_run_id}`
- `POST /writeback/enrichment-results`

### Example request: full pipeline (mock)

```http
POST /jobs/full-pipeline
Content-Type: application/json

{
  "source": "mock",
  "posts": 20,
  "events": 250,
  "rebuild": true,
  "window_days": 30
}
```

### Example request: org post URL ingestion

```http
POST /jobs/linkedin-ingestion/org-url
Content-Type: application/json

{
  "post_url": "https://www.linkedin.com/posts/<REDACTED_POST>?trk=public_post",
  "simulation_mode": false,
  "run_pipeline": true,
  "rebuild_downstream": false,
  "window_days": 30
}
```

### Example response

```json
{
  "run_id": "7cb6f8f4-5f76-4f3f-9fb0-1f80a8c34a5a",
  "job_name": "full_pipeline",
  "stage_name": null,
  "status": "success",
  "started_at": "2026-03-02T18:10:00.125000+00:00",
  "completed_at": "2026-03-02T18:10:03.541000+00:00",
  "duration_ms": 3416,
  "trigger_source": "api",
  "input_params_json": {
    "source": "mock",
    "posts": 20,
    "events": 250,
    "rebuild": true,
    "window_days": 30
  },
  "output_metrics_json": {
    "source": "mock",
    "rebuild": true,
    "window_days": 30,
    "stages": {
      "linkedin_ingestion_mock": {"rows_read": 250, "events_inserted": 250},
      "identity_resolution": {"events_processed": 590},
      "intent_scoring": {"rows_written": 100},
      "opportunity_attribution": {"opportunities_processed": 20}
    }
  },
  "error_message": null
}
```

The full-pipeline response includes deterministic stage ordering in:
- `output_metrics_json.stage_order`

### Example failure response (stable shape)

```json
{
  "run_id": "7f11fdd2-5082-4272-aeb6-2b7f4db66b89",
  "job_name": "linkedin_ingestion_csv",
  "stage_name": "linkedin_ingestion_csv",
  "status": "failed",
  "started_at": "2026-03-02T18:15:00.010000+00:00",
  "completed_at": "2026-03-02T18:15:00.019000+00:00",
  "duration_ms": 9,
  "trigger_source": "api",
  "input_params_json": {
    "source": "shield"
  },
  "output_metrics_json": {
    "partial_metrics": {},
    "error_type": "ValueError",
    "error_detail": "'file' is required for linkedin_ingestion_csv",
    "traceback": "Traceback (most recent call last): ..."
  },
  "error_message": "ValueError: 'file' is required for linkedin_ingestion_csv"
}
```

## n8n integration pattern (next step)

n8n can:
1. preflight with `GET /health`
2. `POST` a job endpoint
2. read `run_id` from response
3. poll `GET /jobs/{run_id}`
4. branch on `status`
5. read `output_metrics_json` for guardrails

## Verification

```powershell
python scripts/verify_orchestration.py
python scripts/verify_orchestration.py --simulate-failure --print-snapshot
python scripts/verify_orchestration.py --api-base-url http://127.0.0.1:8000
```

`verify_orchestration.py` checks:
- successful full-pipeline tracking in `pipeline_runs`
- status/timestamp/duration completeness
- stage-level metrics and `stage_order`
- failure-path logging (`failed`, `error_message`, timing)
- stable payload shape for polling APIs
- CLI `list-runs` visibility

Additional inspection helper:

```powershell
python scripts/inspect_pipeline_runs.py
```
