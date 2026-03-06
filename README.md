# LinkedIn-attribution-engine (Prototype)

Lightweight local backend foundation for attributing organic LinkedIn engagement to account-level pipeline influence.

Current project includes:
- Project structure
- PostgreSQL schema
- Realistic seed data
- Local scripts for init/seed/reset/inspect
- LinkedIn ingestion subsystem (CSV + mock + org URL ingestion + API scaffold)
- Identity resolution and deterministic account/contact matching
- Account intent scoring
- Opportunity influence attribution
- Orchestration API + n8n artifacts
- Writeback and enrichment integration layer
- Local dark-themed frontend operator dashboard

## Project structure

- `app/` Python app utilities (config + DB connection)
- `db/` SQL schema
- `scripts/` local setup and data scripts
- `data/` reserved for future local artifacts
- `docs/` design notes and assumptions
- `docker-compose.yml` local Postgres service
- `requirements.txt` Python dependencies
- `.env.example` environment template
- `frontend/` local React dashboard UI

## Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- Python 3.10+

## Local setup

1. Create env file

```powershell
Copy-Item .env.example .env
```

2. Create Python virtual env and install deps

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3. Start Postgres

```powershell
docker compose up -d
```

Or use the helper script:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_db.ps1
```

4. Initialize schema

```powershell
python scripts/init_db.py
```

5. Seed demo data

```powershell
python scripts/seed_data.py
```

6. Inspect row counts

```powershell
python scripts/inspect_tables.py
```

## Integration credentials (.env)

All secrets/tokens must live in local `.env` and must not be committed.

Required today for local demo:
- none (core pipeline can run with mock ingestion and local DB)

Optional for real endpoint delivery:
- `WRITEBACK_CRM_URL`, `CRM_API_KEY`
- `WRITEBACK_CLAY_URL`, `CLAY_API_KEY`
- `WRITEBACK_EXA_URL`, `EXA_API_KEY`
- `WRITEBACK_WEBHOOK_GENERIC_URL`, `WEBHOOK_GENERIC_SECRET` (optional but recommended)

Optional for future real LinkedIn org API mode:
- `LINKEDIN_ORGANIZATION_ID`
- `LINKEDIN_CLIENT_ID`
- `LINKEDIN_CLIENT_SECRET`
- `LINKEDIN_ACCESS_TOKEN`

Frontend local config:
- `frontend/.env` -> `VITE_API_BASE_URL` (non-secret)

n8n workflow notification target (optional, managed in n8n/config):
- `N8N_NOTIFICATION_WEBHOOK_URL` (documented in `.env.example` as a local reference variable)

Validate integration config presence (without printing secret values):

```powershell
python scripts/validate_integration_config.py
python scripts/validate_integration_config.py --check-writeback-target exa
python scripts/validate_integration_config.py --check-linkedin-real-mode
```

## Sanitization

This workspace has been sanitized for sharing. Sensitive details were replaced with placeholders:

- Personal identifiers: `<REDACTED_USER>`, `<REDACTED_PERSON>`, `<REDACTED_PROFILE>`
- Company identifiers: `<REDACTED_COMPANY>`, `placeholder-company.example`
- Credential values: `<LINKEDIN_CLIENT_ID>`, `<LINKEDIN_CLIENT_SECRET>`, `<LINKEDIN_ACCESS_TOKEN>`, `<GEMINI_API_KEY>`, `<EXA_API_KEY>`, `<CRM_API_KEY>`, `<CLAY_API_KEY>`, `<WEBHOOK_GENERIC_SECRET>`
- Contact details: `redacted<number>@example.com`, `+1-000-000-0000`
- Local machine paths: `C:\Users\<REDACTED_USER>\<REDACTED_PATH>\...`

If you run this project locally, replace placeholder values in `.env` with your own credentials and endpoints.

`inspect_tables.py` validates that source tables are populated and derived tables remain empty, and exits with a non-zero code on failure.

Optional one-shot bootstrap after DB is up:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap_local.ps1
```

## Local DB auth reset (Docker authoritative)

Canonical local DB env values:
- `POSTGRES_HOST=localhost`
- `POSTGRES_PORT=5432`
- `POSTGRES_DB=social_attribution_engine`
- `POSTGRES_USER=postgres`
- `POSTGRES_PASSWORD=postgres`

Safe reset (keeps data volume):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\reset_local_db_env.ps1
```

Destructive reset (wipes Docker Postgres volume/data):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\reset_local_db_env.ps1 -WipeData
```

Manual equivalents:

```powershell
docker compose down
docker compose up -d
```

Destructive manual equivalent:

```powershell
docker compose down -v
docker compose up -d
```

Check DB connectivity before running pipeline scripts:

```powershell
python scripts/check_db_connection.py
```

If `.env` is missing, copy from `.env.example`:

```powershell
Copy-Item .env.example .env
```

## Reset database

Drops and recreates the `public` schema, then reapplies tables.

```powershell
python scripts/reset_db.py
python scripts/seed_data.py
```

## End-to-end DB-backed verification order

Run this sequence after reset:

```powershell
python scripts/check_db_connection.py
python scripts/init_db.py
python scripts/reset_db.py
python scripts/seed_data.py
python -m app.orchestration.pipeline run-full --source mock --posts 20 --events 250 --rebuild
python scripts/verify_orchestration.py --simulate-failure --print-snapshot
```

## Quick table inspection with psql

```powershell
docker exec -it social-attribution-postgres psql -U postgres -d social_attribution_engine
```

Inside psql:

```sql
\dt
SELECT COUNT(*) FROM social_events;
SELECT event_type, COUNT(*) FROM social_events GROUP BY event_type;
SELECT stage, COUNT(*) FROM opportunities GROUP BY stage;
```

## Seed profile

The seed script inserts realistic demo patterns:
- 25 posts
- 340 social events
- 50 accounts
- 75 contacts
- 150 website events
- 20 opportunities
- 0 account_intent_scores (derived)
- 0 opportunity_influence (derived)

Data includes:
- Accounts that engage repeatedly and later open opportunities
- Accounts that engage but never convert
- Uneven post engagement concentration (some posts drive more activity)
- Chronological consistency across social events, website activity, and opportunity creation

Seed behavior:
- Source/input tables are seeded: `posts`, `social_events`, `accounts`, `contacts`, `website_events`, `opportunities`.
- Derived/output tables are intentionally not seeded: `account_intent_scores`, `opportunity_influence`.
- This avoids circular demo logic and keeps attribution/scoring outputs owned by downstream jobs.

## Documentation

See [`docs/foundation.md`](docs/foundation.md) for schema rationale, assumptions, and deferred scope.
See [`docs/linkedin_ingestion.md`](docs/linkedin_ingestion.md) for ingestion architecture and usage.
See [`docs/org_url_ingestion.md`](docs/org_url_ingestion.md) for direct org-post URL ingestion (official adapter seam).
See [`docs/demo_csv_simulation_flow.md`](docs/demo_csv_simulation_flow.md) for deprecated CSV simulation notes.
See [`docs/identity_resolution.md`](docs/identity_resolution.md) for matching logic and confidence model.
See [`docs/intent_scoring.md`](docs/intent_scoring.md) for deterministic intent scoring model and tuning points.
See [`docs/opportunity_attribution.md`](docs/opportunity_attribution.md) for deterministic opportunity influence attribution logic.
See [`docs/orchestration.md`](docs/orchestration.md) for orchestration APIs/CLI and n8n-friendly run tracking.
See [`docs/n8n_workflows.md`](docs/n8n_workflows.md) for Step 7 n8n workflow artifacts and import/config instructions.
See [`docs/writeback.md`](docs/writeback.md) for Step 8 outbound writeback and inbound enrichment handling.
See [`docs/unresolved_account_research_flow.md`](docs/unresolved_account_research_flow.md) for unresolved-candidate -> Exa assistive research flow.
See [`docs/demo_runbook.md`](docs/demo_runbook.md) for the end-to-end final walkthrough.
See [`docs/architecture_summary.md`](docs/architecture_summary.md) for interview-style architecture overview.
See [`docs/business_tool_framing.md`](docs/business_tool_framing.md) for GTM/revops business framing.
See [`docs/demo_queries.md`](docs/demo_queries.md) and [`scripts/demo_queries.sql`](scripts/demo_queries.sql) for SQL demo query pack.
See [`docs/simulated_clay_crm_flow.md`](docs/simulated_clay_crm_flow.md) for legacy simulated Clay/CRM notes.
See [`docs/windows_db_reset_runbook.md`](docs/windows_db_reset_runbook.md) for PowerShell-first DB auth recovery and verification.
Use the **"Single PowerShell Recovery Sequence"** section in that runbook for the fastest Windows destructive reset + verification path.
See [`docs/tool_algorithm_sim_and_live.md`](docs/tool_algorithm_sim_and_live.md) for end-to-end system behavior notes.
Step algorithm quick references:
- [`docs/step_1_foundation_algorithm.md`](docs/step_1_foundation_algorithm.md)
- [`docs/step_2_linkedin_ingestion_algorithm.md`](docs/step_2_linkedin_ingestion_algorithm.md)
- [`docs/step_3_identity_resolution_algorithm.md`](docs/step_3_identity_resolution_algorithm.md)
- [`docs/step_4_intent_scoring_algorithm.md`](docs/step_4_intent_scoring_algorithm.md)
- [`docs/step_5_opportunity_attribution_algorithm.md`](docs/step_5_opportunity_attribution_algorithm.md)
- [`docs/step_6_orchestration_algorithm.md`](docs/step_6_orchestration_algorithm.md)
- [`docs/step_7_n8n_workflows_algorithm.md`](docs/step_7_n8n_workflows_algorithm.md)
- [`docs/step_8_writeback_enrichment_algorithm.md`](docs/step_8_writeback_enrichment_algorithm.md)
See [`docs/frontend_dashboard.md`](docs/frontend_dashboard.md) for local dashboard setup and usage.

## LinkedIn ingestion commands

Import CSV presets:

```powershell
python -m app.linkedin_ingestion.import_csv --source shield --file data/shield_sample.csv
python -m app.linkedin_ingestion.import_csv --source sprout --file data/sprout_sample.csv
python -m app.linkedin_ingestion.import_csv --source generic --file data/generic_sample.csv
```

Generate and ingest mock data:

```powershell
python -m app.linkedin_ingestion.generate_mock --posts 20 --events 250
```

Ingest organization post by URL (real mode):

```powershell
python -m app.linkedin_ingestion.run_url_ingestion --post-url "https://www.linkedin.com/posts/<REDACTED_POST>"
python -m app.linkedin_ingestion.run_url_ingestion --post-url "https://www.linkedin.com/posts/<REDACTED_POST>" --run-pipeline
```

Simulation-mode CSV ingestion is disabled.

Optional generic override mapping:

```powershell
python -m app.linkedin_ingestion.import_csv --source generic --file data/custom.csv --mapping-file data/generic_mapping.json
```

CSV import summary output includes:
- rows successful
- posts created
- posts updated
- events inserted
- rows skipped
- warnings count

Ingestion metadata guarantees:
- aggregate-derived events are tagged with `metadata_json.aggregated_import = true`
- aggregate-derived events carry `source_metric_count` and `original_columns`
- aggregate-derived events keep `actor_name` null and use `metadata_json.actor_origin = "aggregate_unknown"`
- mock-generated events use `metadata_json.actor_origin = "mock_generated"`
- known source actor rows use `metadata_json.actor_origin = "known"` when actor identity fields are present

Verification helpers:

```powershell
python scripts/verify_linkedin_ingestion.py
python scripts/verify_linkedin_ingestion.py --run-db --shield-file data/shield_sample.csv
python scripts/verify_org_url_ingestion.py
python scripts/verify_org_url_ingestion.py --run-db --run-pipeline
python scripts/verify_linkedin_actor_level_tracking.py
python scripts/backfill_social_actor_model.py
```

Each ingestion run is audit-logged in `imports_log` with source/file/mode/timestamps, row counters, warning counters, and a summary note.

`account_intent_scores` and `opportunity_influence` remain derived tables and are not populated by ingestion.

## Identity resolution commands

Run incremental matching (only unmatched social events):

```powershell
python -m app.identity_resolution.run_matching
```

Rebuild matches from scratch:

```powershell
python -m app.identity_resolution.run_matching --rebuild
```

Inspect match output:

```powershell
python scripts/inspect_matches.py
```

Verify identity resolution guarantees:

```powershell
python scripts/verify_identity_resolution.py
python scripts/verify_identity_resolution.py --include-ambiguity-fixture
```

Concise match summary:

```powershell
python scripts/inspect_match_summary.py
```

Identity-resolution guarantees:
- exactly one final match row per social event (`social_event_matches.social_event_id` unique)
- incremental runs are idempotent for already matched events
- repeated rebuild runs are snapshot-stable (same totals and match-type distributions)
- aggregate-import events are never contact-matched
- ambiguous contact/account candidates are left unresolved

## Intent scoring commands

Run scoring:

```powershell
python -m app.intent_scoring.run_scoring
python -m app.intent_scoring.run_scoring --rebuild
```

Inspect scoring output:

```powershell
python scripts/inspect_intent_scores.py
python scripts/inspect_intent_summary.py
```

Verify scoring determinism and integrity:

```powershell
python scripts/verify_intent_scoring.py
python scripts/verify_intent_scoring.py --print-snapshot
```

## Opportunity attribution commands

Run attribution:

```powershell
python -m app.opportunity_attribution.run_attribution
python -m app.opportunity_attribution.run_attribution --rebuild
python -m app.opportunity_attribution.run_attribution --window-days 60 --rebuild
```

Inspect attribution output:

```powershell
python scripts/inspect_opportunity_influence.py
```

Verify attribution determinism and safety:

```powershell
python scripts/verify_opportunity_attribution.py
python scripts/verify_opportunity_attribution.py --print-snapshot
python scripts/verify_opportunity_attribution.py --include-fixture
```

## Orchestration commands

Run full pipeline through orchestration CLI:

```powershell
python -m app.orchestration.pipeline run-full --source mock --posts 20 --events 250 --rebuild
python -m app.orchestration.pipeline run-full --source shield_csv --file data/shield_sample.csv --rebuild
```

Run a single stage through orchestration CLI:

```powershell
python -m app.orchestration.pipeline run-stage identity_resolution --rebuild
python -m app.orchestration.pipeline run-stage intent_scoring --rebuild
python -m app.orchestration.pipeline run-stage opportunity_attribution --rebuild --window-days 30
python -m app.orchestration.pipeline run-stage linkedin_ingestion_org_url --post-url "https://www.linkedin.com/posts/<REDACTED_POST>" --run-pipeline
```

Inspect orchestration run history:

```powershell
python -m app.orchestration.pipeline list-runs --limit 20
python -m app.orchestration.pipeline list-runs --limit 20 --json
python -m app.orchestration.pipeline get-run --run-id <RUN_ID>
```

Run orchestration API:

```powershell
uvicorn app.orchestration.api:app --host 127.0.0.1 --port 8000
```

Verify orchestration:

```powershell
python scripts/verify_orchestration.py
python scripts/verify_orchestration.py --simulate-failure --print-snapshot
python scripts/verify_orchestration.py --api-base-url http://127.0.0.1:8000
```

## Frontend dashboard (local)

Start backend API:

```powershell
uvicorn app.orchestration.api:app --host 127.0.0.1 --port 8000
```

Start frontend:

```powershell
cd frontend
Copy-Item .env.example .env -ErrorAction SilentlyContinue
npm install --include=dev
npm run dev
```

Open:
- `http://127.0.0.1:5173`

Dashboard includes unresolved-candidate research surfaces:
- inspect top unresolved account candidates
- run Exa research for unresolved candidates
- inspect recent unresolved-candidate Exa enrichment results

## n8n artifacts

Step 7 n8n workflow exports are in `n8n/`:

- `n8n/full_refresh_pipeline.json`
- `n8n/incremental_pipeline.json`
- `n8n/stage_run_or_retry.json`
- `n8n/failure_notification.json`
- `n8n/sample_requests.md`

Import these files in n8n, update config values (`api_base_url`, notification webhook, poll settings), then run manually or attach schedule triggers.

## Writeback and enrichment commands

Load CRM CSV data (optional):

```powershell
python -m app.crm_sync.load_crm_csv --accounts-file <path-to-accounts.csv> --contacts-file <path-to-contacts.csv>
```

Run outbound writeback:

```powershell
python -m app.writeback.run_writeback --target-type crm --selection-mode high_intent_accounts --limit 25 --dry-run
python -m app.writeback.run_writeback --target-type clay --selection-mode low_confidence_promising_accounts --limit 20
python -m app.writeback.run_writeback --target-type clay --selection-mode low_confidence_promising_accounts --endpoint-url https://your-clay-endpoint.example --limit 20
python -m app.writeback.run_writeback --target-type exa --selection-mode unresolved_account_candidates --endpoint-url https://your-exa-endpoint.example --limit 20
python -m app.writeback.run_writeback --target-type webhook_generic --selection-mode socially_influenced_opportunities --endpoint-url http://localhost:9001/webhook
```

Ingest normalized enrichment results:

```powershell
python -m app.writeback.ingest_enrichment --file data/sample_clay_enrichment_result.json
python -m app.writeback.ingest_enrichment --file data/sample_exa_enrichment_result.json
python -m app.writeback.ingest_enrichment --file data/sample_exa_unresolved_candidate_result.json
python -m app.writeback.ingest_enrichment --file data/sample_crm_response.json
```

Inspect/verify writeback:

```powershell
python scripts/inspect_writeback_runs.py
python scripts/inspect_writeback_summary.py
python scripts/inspect_unresolved_candidates.py
python scripts/verify_writeback.py --simulate-replay --print-snapshot
python scripts/verify_unresolved_account_research_flow.py
python scripts/verify_writeback.py --dry-run-only
```

Writeback API endpoints (served by orchestration API):
- `POST /writeback/run`
- `GET /writeback/runs`
- `GET /writeback/runs/{writeback_run_id}`
- `POST /writeback/enrichment-results`

## Final demo quickstart

```powershell
python -m app.crm_sync.load_crm_csv --accounts-file <path-to-accounts.csv> --contacts-file <path-to-contacts.csv>
python -m app.orchestration.pipeline run-full --source mock --posts 20 --events 250 --rebuild
python scripts/inspect_demo_surfaces.py
python scripts/generate_demo_report.py
```

Key final artifacts:
- `docs/demo_runbook.md`
- `docs/architecture_summary.md`
- `docs/business_tool_framing.md`
- `docs/demo_report_latest.md` (generated)

Inspect pipeline run summaries:

```powershell
python scripts/inspect_pipeline_runs.py
```

## Workspace housekeeping

Generated runtime artifacts can be removed safely:
- Python caches: `__pycache__/`, `*.pyc`
- Runtime logs: `*.log`

Example cleanup commands (PowerShell):

```powershell
Get-ChildItem -Recurse -Directory -Filter __pycache__ | Remove-Item -Recurse -Force
Get-ChildItem -Recurse -File -Include *.pyc,*.log | Remove-Item -Force
```
