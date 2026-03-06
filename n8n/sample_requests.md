# n8n Sample Requests And Response Shapes

All examples assume orchestration API base URL:
- `http://127.0.0.1:8000`

## Full refresh request

```json
{
  "source": "mock",
  "posts": 20,
  "events": 250,
  "rebuild": true,
  "window_days": 30
}
```

CSV variant:

```json
{
  "source": "shield_csv",
  "file": "data/shield_sample.csv",
  "rebuild": true,
  "window_days": 30
}
```

## Incremental request

```json
{
  "source": "mock",
  "posts": 10,
  "events": 80,
  "rebuild": false,
  "window_days": 30
}
```

## Stage-run requests

Identity resolution:

```json
{
  "rebuild": false
}
```

Intent scoring:

```json
{
  "rebuild": false
}
```

Opportunity attribution:

```json
{
  "rebuild": false,
  "window_days": 30
}
```

LinkedIn ingestion CSV:

```json
{
  "source": "shield",
  "file": "data/shield_sample.csv",
  "delimiter": ","
}
```

LinkedIn ingestion mock:

```json
{
  "posts": 20,
  "events": 250
}
```

## Success payload shape (`GET /jobs/{run_id}`)

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
    "stage_order": [
      "linkedin_ingestion_mock",
      "identity_resolution",
      "intent_scoring",
      "opportunity_attribution"
    ],
    "stages": {
      "linkedin_ingestion_mock": {
        "events_inserted": 250
      },
      "identity_resolution": {
        "events_processed": 590
      },
      "intent_scoring": {
        "rows_written": 100
      },
      "opportunity_attribution": {
        "opportunities_processed": 20
      }
    }
  },
  "error_message": null
}
```

## Failure payload shape (`GET /jobs/{run_id}`)

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

## Suggested sanity checks

For successful full pipeline runs:
- `output_metrics_json.stages` contains expected stage keys
- ingestion stage has events inserted when applicable
- `identity_resolution.events_processed > 0`
- `intent_scoring.rows_written > 0`
- `opportunity_attribution.opportunities_processed > 0`

Route to warning branch if any check fails.

## Failure notification webhook test payload

Use with `failure_notification` workflow webhook endpoint:

```json
{
  "status": "failed",
  "run_id": "demo-run-1",
  "job_name": "full_pipeline",
  "trigger_source": "n8n",
  "duration_ms": 2100,
  "error_message": "Simulated pipeline failure for notification test",
  "output_metrics_json": {
    "stages": {
      "identity_resolution": {
        "events_processed": 0
      }
    }
  },
  "notification_webhook_url": "https://example.com/replace-with-slack-or-email-webhook"
}
```

Replace `notification_webhook_url` with Slack/email/internal webhook target before live use.
