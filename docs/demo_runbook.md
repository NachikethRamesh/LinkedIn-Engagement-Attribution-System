# Demo Runbook

## Objective

Walk the reviewer through the full system from ingestion to writeback/enrichment in one deterministic flow.

## Prerequisites

```powershell
docker compose up -d
python scripts/init_db.py
python scripts/reset_db.py
python scripts/seed_data.py
```

## Step 1: Run Full Pipeline

Optional realism pre-step (CRM CSV sync):

```powershell
python -m app.crm_sync.load_crm_csv --accounts-file <path-to-crm-accounts.csv> --contacts-file <path-to-crm-contacts.csv>
```

Then run pipeline:

```powershell
python -m app.orchestration.pipeline run-full --source mock --posts 20 --events 250 --rebuild
```

Point out:
- one command orchestrates ingestion -> identity -> intent -> attribution
- run metadata is captured in `pipeline_runs`

## Step 2: Show Match Quality

```powershell
python scripts/inspect_match_summary.py
```

Point out:
- aggregate imports are handled safely
- unresolved events are explicit, not forced

## Step 3: Show Top Intent Accounts

```powershell
python scripts/inspect_intent_summary.py
```

Point out:
- deterministic score windows and reasons
- confidence attached to each account score

## Step 4: Show Influenced Opportunities

```powershell
python scripts/inspect_opportunity_influence.py
```

Point out:
- influence bands (`none|weak|medium|strong`)
- explainable notes and score components
- no direct causality claims

## Step 5: Trigger Writeback (Dry-Run First)

```powershell
python -m app.writeback.run_writeback --target-type crm --selection-mode high_intent_accounts --limit 10 --dry-run
python scripts/inspect_writeback_summary.py
```

Point out:
- dry-run creates auditable records with payload previews
- no external side effects in dry-run

## Step 6: Trigger Real/Stub Writeback

```powershell
python -m app.writeback.run_writeback --target-type clay --selection-mode low_confidence_promising_accounts --limit 10
python -m app.writeback.run_writeback --target-type exa --selection-mode low_confidence_promising_accounts --limit 10
```

Point out:
- records show status, responses, and replay-safe behavior
- adapters are thin and explicit

## Step 6b: Simulated Clay Round-Trip (Recommended Demo)

```powershell
python scripts/run_simulated_clay_roundtrip.py --limit 10
```

Point out:
- outbound payload files appear in `data/outbound/clay_requests/`
- generated normalized inbound files appear in `data/inbound/clay_results/`
- results are ingested back into `enrichment_results`

## Step 7: Ingest Enrichment Results

```powershell
python -m app.writeback.ingest_enrichment --file data/sample_clay_enrichment_result.json
python -m app.writeback.ingest_enrichment --file data/sample_exa_enrichment_result.json
python scripts/inspect_demo_surfaces.py
```

Point out:
- enrichment results are normalized and deduped
- local DB remains source of truth

## Step 7b: Unresolved Candidate -> Exa Research Assist

```powershell
python scripts/inspect_unresolved_candidates.py --limit 5
python -m app.writeback.run_writeback --target-type exa --selection-mode unresolved_account_candidates --endpoint-url https://your-endpoint.example --limit 5
python scripts/inspect_writeback_summary.py
```

Point out:
- unresolved company candidates are selected deterministically from identity outputs
- outbound Exa request artifacts are visible in `data/outbound/exa_requests/`
- simulated normalized inbound files are visible in `data/inbound/exa_results/`
- Exa remains assistive research only and does not mutate identity matching directly

Optional verification:

```powershell
python scripts/verify_unresolved_account_research_flow.py
```

## Step 8: Generate Final Demo Snapshot

```powershell
python scripts/generate_demo_report.py
```

Output:
- `docs/demo_report_latest.md`

Use this file as a leave-behind summary.

## Optional SQL Deep-Dive

```powershell
docker exec -it social-attribution-postgres psql -U postgres -d social_attribution_engine
```

Then run sections from:
- `scripts/demo_queries.sql`

Or paste the queries from that file directly into the `psql` session.
