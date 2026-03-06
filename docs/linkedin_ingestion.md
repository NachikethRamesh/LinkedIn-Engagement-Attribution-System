# LinkedIn Ingestion Layer

## Purpose

This module is a modular ingestion layer for LinkedIn signal ingestion. It is intentionally not a scraper and does not automate browser login.

Current supported ingestion paths:
- CSV imports from third-party analytics exports (`shield`, `sprout`, `generic`)
- Mock/demo generator for local development
- Official API adapter scaffold for future implementation
- Organization post URL ingestion path (`linkedin_org_api`) with simulation fixtures

## Architecture

Code lives in `app/linkedin_ingestion/`:
- `base.py`: adapter interface + ingestion service that writes to DB
- `csv_adapter.py`: CSV extraction and normalization execution
- `mock_adapter.py`: synthetic post/event generation
- `official_api_adapter.py`: API integration scaffold (not implemented)
- `org_post_resolver.py`: validates/normalizes org post URL and resolves org-post identifier
- `org_api_adapter.py`: org/community API adapter interface with simulation fixtures
- `url_ingestion.py`: URL ingestion service that normalizes API payloads into canonical rows
- `run_url_ingestion.py`: URL ingestion CLI entry point
- `normalizer.py`: source-specific and generic field mapping logic
- `validator.py`: value cleaning and datetime/event validation helpers
- `types.py`: canonical dataclasses and import stats
- `import_csv.py`: CSV CLI entry point
- `generate_mock.py`: mock CLI entry point

## Canonical event model

Canonical event types:
- `post_impression`
- `post_like`
- `post_comment`
- `post_repost`
- `profile_view`
- `company_page_view`
- `website_click`

Normalized social event shape (internal):
- `post_url`
- `actor_name` (nullable)
- `actor_linkedin_url` (nullable)
- `actor_company_raw` (nullable)
- `event_type`
- `event_timestamp`
- `metadata_json`
- `source_name`
- `import_mode`
- `aggregated_import`

Normalized actor-level storage tables (new, additive):
- `social_posts`
- `social_comments`
- `social_engagement_actors`
- `social_engagement_events`
- `social_post_metrics_snapshots`
- `social_comment_metrics_snapshots`

Normalized post shape (internal):
- `post_url`
- `author_name`
- `topic`
- `cta_url`
- `created_at`
- `source_name`
- `raw_payload_json`

## CSV normalization behavior

- Accepts varying header names and normalizes common aliases.
- Supports presets:
  - `shield`: aggregate metrics-oriented export shape
  - `sprout`: event-row oriented export shape
  - `generic`: broad alias matching for arbitrary CSVs
- Optional mapping overrides via `--mapping-file` for `generic` mode.

### Aggregated imports

If aggregate counts are present (for example `impressions`, `likes`, `comments`), the importer synthesizes canonical events and marks provenance fields in `metadata_json`:
- `aggregated_import = true`
- `source_metric_count`
- `original_columns`
- `actor_origin = "aggregate_unknown"`
- `source_name`
- `import_mode`
- `raw_row_id`
- `import_timestamp`

Aggregate safety semantics:
- Aggregate-derived events always set `actor_name = null`.
- Aggregate-derived events always set `actor_linkedin_url = null`.
- This prevents aggregate metrics from being misread as person-level identity signals.

## Provenance and idempotency

- `posts` use normalized LinkedIn `post_url` upsert semantics.
- URL normalization removes query parameters/fragments, removes trailing slash, and canonicalizes mobile/domain variants (`m.linkedin.com` -> `www.linkedin.com`).
- `social_events` use a best-effort deterministic dedupe key hashed from stable normalized fields and provenance.
- Dedupe key includes source-aware fields (`source_name`, `import_mode`) and excludes volatile fields (for example import timestamps).
- Deduplication is enforced by a unique partial index on `metadata_json->>'dedupe_key'`.
- Every import writes an `imports_log` entry with row/success/skip/warning counts.

`imports_log` fields captured per run:
- `source_name`
- `filename`
- `import_mode`
- `imported_at`
- `row_count`
- `success_count`
- `skip_count`
- `warning_count`
- `notes` (human-readable summary with key counters and warning preview)

Known limitation:
- Dedupe is best-effort and row-shape dependent. If source exports materially change row identity fields, duplicates may still be inserted.

Actor-level limitations by engagement type (official API dependent):
- `post_comment`: actor-level stored when API returns actor metadata.
- `post_like` / reactions: actor-level stored when API returns reaction actors.
- `post_repost` / shares: currently count-only in org URL simulation fixtures (no actor list exposed).
- `profile_view` / `company_page_view`: currently count-only unless actor-level API scope/endpoint provides identities.
- `comment replies`: stored as `platform_object_type=reply` when parent comment id is available.
- `comment reactions`: stored as `platform_object_type=comment` + `engagement_type=post_like` when target comment id is available.

Actor identity semantics for downstream matching:
- `actor_origin = "known"`: source provided actor name and/or actor profile URL.
- `actor_origin = "aggregate_unknown"`: row came from aggregate-only import or unresolved actor identity.
- `actor_origin = "mock_generated"`: event came from mock generator.

## CLI commands

CSV import:

```powershell
python -m app.linkedin_ingestion.import_csv --source shield --file data/shield_sample.csv
python -m app.linkedin_ingestion.import_csv --source sprout --file data/sprout_sample.csv
python -m app.linkedin_ingestion.import_csv --source generic --file data/generic_sample.csv
```

Mock generation/import:

```powershell
python -m app.linkedin_ingestion.generate_mock --posts 20 --events 250
```

Organization post URL ingestion:

```powershell
python -m app.linkedin_ingestion.run_url_ingestion --post-url "https://www.linkedin.com/posts/<REDACTED_POST>" 
python -m app.linkedin_ingestion.run_url_ingestion --post-url "https://www.linkedin.com/posts/<REDACTED_POST>"  --run-pipeline
```

URL-ingestion verification:

```powershell
python scripts/verify_org_url_ingestion.py
python scripts/verify_org_url_ingestion.py --run-db
python scripts/verify_org_url_ingestion.py --run-db --run-pipeline
python scripts/verify_linkedin_actor_level_tracking.py
python scripts/backfill_social_actor_model.py
```

Verification script:

```powershell
python scripts/verify_linkedin_ingestion.py
python scripts/verify_linkedin_ingestion.py --run-db --shield-file data/shield_sample.csv
```

## Official API path (future)

`official_api_adapter.py` defines the integration seam for future production work.

Deferred implementation points:
- OAuth and token lifecycle
- LinkedIn permissions/scopes selection
- endpoint configuration/versioning
- rate limit handling and retries
- pagination
- endpoint response-to-canonical normalization

Planned credential env vars for real API mode:
- `LINKEDIN_ORGANIZATION_ID`
- `LINKEDIN_CLIENT_ID`
- `LINKEDIN_CLIENT_SECRET`
- `LINKEDIN_ACCESS_TOKEN`

Local simulation/CSV/mock ingestion paths do not require these.

This allows implementation of official API ingestion without refactoring CSV/mock ingestion paths.

## Org URL ingestion behavior

- Source name: `linkedin_org_api`
- Import mode: `url_ingestion`
- Canonical anchor: normalized `post_url` in `posts`
- Provenance stored in `metadata_json`:
  - `original_url`
  - `normalized_url`
  - `resolved_org_post_identifier`
  - `resolution_mode` (`real|override|mock`)
  - `adapter_mode` (`real|simulation`)

Aggregate/actor safety in URL-ingestion mode:
- Counts-only signals are stored as aggregate-derived events (`aggregated_import=true`, `actor_origin=aggregate_unknown`).
- Aggregate-derived URL-ingestion events never set `actor_name` or `actor_linkedin_url`.
- Person-level comments/reactions are stored only when actor data is actually present in source payloads.
