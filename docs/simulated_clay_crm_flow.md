# Simulated Clay + CRM CSV Flow

## Why This Exists

For demo/interview realism, this project simulates:
- CRM source data via CSV files
- Clay enrichment handoff via local file adapters

The local app + Postgres remain the source of truth.  
No core attribution logic is moved to external systems.

## Storyline

1. Load CRM-like accounts/contacts CSV into local `accounts` + `contacts`.
2. Run the existing social attribution pipeline.
3. Select low-confidence but promising accounts for enrichment.
4. Send to Clay adapter in simulated-local mode.
5. Write outbound requests to `data/outbound/clay_requests/`.
6. Generate normalized simulated enrichment results in `data/inbound/clay_results/`.
7. Ingest normalized results back into `enrichment_results`.

## Sample Files

- `<path-to-crm-accounts.csv>`
- `<path-to-crm-contacts.csv>`

## Fast Demo Commands

```powershell
python -m app.crm_sync.load_crm_csv --accounts-file <path-to-crm-accounts.csv> --contacts-file <path-to-crm-contacts.csv>
python scripts/run_simulated_clay_roundtrip.py --limit 10
```

By default the helper disables replay-skip so each run produces visible artifacts for demos.
If needed, enable replay protection:

```powershell
python scripts/run_simulated_clay_roundtrip.py --limit 10 --enable-replay-skip
```

That produces:
- outbound payload artifacts: `data/outbound/clay_requests/*.json`
- generated inbound normalized results: `data/inbound/clay_results/*.json`
- ingested rows in `enrichment_results`

## Manual Commands (Equivalent)

```powershell
python -m app.writeback.run_writeback --target-type clay --selection-mode low_confidence_promising_accounts --endpoint-url https://your-endpoint.example --limit 10
python -m app.writeback.ingest_enrichment --file data/inbound/clay_results/<generated_file>.json
```

## Verification

```powershell
python scripts/verify_simulated_clay_flow.py
```

## Production Mapping Later

- CRM CSV loader -> real CRM incremental sync API/webhook.
- Simulated Clay file adapter -> authenticated Clay API adapter.
- Inbound local JSON ingestion -> normalized webhook/API ingestion service.
