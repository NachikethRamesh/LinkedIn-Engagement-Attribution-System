# LinkedIn Organization URL Ingestion

## Purpose

This feature ingests a LinkedIn organization post from a pasted URL through the official organization/community API integration seam.

It is built as an adapter-based ingestion path, not a scraper.

## Flow

1. Validate and normalize the LinkedIn post URL.
2. Resolve the org-post identifier required by the org/community API adapter.
3. Fetch post metadata + social metadata (+ comments/reactions when available).
4. Normalize into canonical `posts` + `social_events` rows.
4b. Also write normalized actor/object rows into:
   - `social_posts`
   - `social_comments`
   - `social_engagement_actors`
   - `social_engagement_events`
   - metrics snapshot tables
5. Write import audit record into `imports_log`.
6. Optionally trigger downstream:
   - identity resolution
   - intent scoring
   - opportunity attribution

## Modules

- `app/linkedin_ingestion/org_post_resolver.py`
- `app/linkedin_ingestion/org_api_adapter.py`
- `app/linkedin_ingestion/url_ingestion.py`
- `app/linkedin_ingestion/run_url_ingestion.py`

## URL normalization and resolution

Normalization rules:
- enforce `https://www.linkedin.com/...`
- remove query parameters and fragments
- canonicalize `m.linkedin.com` and `linkedin.com` to `www.linkedin.com`
- remove trailing slash

Resolution modes:
- `real`: identifier extracted from supported URL path patterns (`/posts/...`, `urn:li:activity` path form)
- `override`: caller explicitly passed `resolved_id_override`
- `mock`: deterministic fallback identifier when identifier cannot be extracted

## Adapter modes

- `simulation_mode=true`: disabled and rejected by backend guardrails
- `simulation_mode=false`: real API mode interface (OAuth/network calls intentionally deferred)

Optional env vars for future real mode:
- `LINKEDIN_ORGANIZATION_ID`
- `LINKEDIN_CLIENT_ID`
- `LINKEDIN_CLIENT_SECRET`
- `LINKEDIN_ACCESS_TOKEN`

For local/production usage, keep `simulation_mode=false` and provide LinkedIn credentials.

## Data mapping

### `posts`
- `post_url`: normalized URL
- `author_name`: organization/author from API payload
- `topic`: topic/text fallback
- `cta_url`
- `created_at`

### `social_events`

Event source: `source_name=linkedin_org_api`, `import_mode=url_ingestion`.

- Aggregate metrics become aggregate-derived events when detailed rows are unavailable:
  - `post_impression`, `post_repost`, `website_click` from counts
  - `post_like` only if no reaction rows are provided
  - `post_comment` only if no comment rows are provided
- Person-level rows:
  - comments -> `post_comment`
  - reactions -> `post_like`
- Provenance keys in `metadata_json`:
  - `original_url`
  - `normalized_url`
  - `resolved_org_post_identifier`
  - `resolution_mode`
  - `adapter_mode`
  - `raw_row_id`
  - `aggregated_import`
  - `source_metric_count`
  - `actor_origin`

Actor safety:
- aggregate-derived events do not invent actor identity fields.
- actor identity only appears when source data provides it.

Actor-resolution metadata:
- `actor_resolution_status`: `resolved | unresolved | aggregate_only`
- `availability_status`: `actor_resolved | aggregate_only | not_exposed`
- object tracking fields:
  - `platform_object_type`
  - `platform_object_id`
  - `parent_platform_object_id`

## CLI usage

```powershell
python -m app.linkedin_ingestion.run_url_ingestion --post-url "https://www.linkedin.com/posts/<REDACTED_POST>" 
python -m app.linkedin_ingestion.run_url_ingestion --post-url "https://www.linkedin.com/posts/<REDACTED_POST>"  --run-pipeline
```

## Orchestration API + CLI

API endpoint:
- `POST /jobs/linkedin-ingestion/org-url`

Example request:

```json
{
  "post_url": "https://www.linkedin.com/posts/<REDACTED_POST>?trk=public_post",
  "simulation_mode": false,
  "run_pipeline": true,
  "rebuild_downstream": false,
  "window_days": 30
}
```

CLI stage run:

```powershell
python -m app.orchestration.pipeline run-stage linkedin_ingestion_org_url --post-url "https://www.linkedin.com/posts/<REDACTED_POST>"  --run-pipeline
```

## Verification

```powershell
python scripts/verify_org_url_ingestion.py
python scripts/verify_org_url_ingestion.py --run-db
python scripts/verify_org_url_ingestion.py --run-db --run-pipeline
```

These checks cover:
- URL canonicalization
- invalid URL rejection
- aggregate actor safety
- rerun dedupe behavior
- imports log presence
- orchestration path + optional downstream sequence

Additional actor-level verification:

```powershell
python scripts/verify_linkedin_actor_level_tracking.py
```
