# Unresolved Account Candidate -> Exa Research Flow

## Why this exists

Some social events contain promising company hints (`actor_company_raw`) but remain unresolved or weakly matched after deterministic identity resolution.

Instead of forcing low-confidence matches, the system:
- keeps core match outputs deterministic and conservative
- routes unresolved company candidates to Exa as assistive research
- ingests normalized research back for review/enrichment workflows

## Deterministic candidate extraction

Selection mode: `unresolved_account_candidates`

Candidate sources:
- `social_event_matches.match_type = unresolved`
- weak account-only rows below threshold (default `match_confidence < 0.70`)

Guardrails:
- `actor_company_raw` must be present
- minimum signal evidence thresholds:
  - `min_contributing_events`
  - `min_strong_signals`
  - `min_recent_signals` in `recent_days` window
- generic placeholders are excluded by default (`include_generic_candidates=false`)

## Stable candidate identity

- `candidate_id` is a deterministic bigint derived from normalized company candidate text.
- this ID is used as `entity_id` for:
  - `writeback_records` (`entity_type=unresolved_account_candidate`)
  - `enrichment_results` (`entity_type=unresolved_account_candidate`)

This gives replay-safe tracking without requiring an `accounts.id` that does not yet exist.

## Exa payload role

Outbound payload includes:
- raw/normalized company candidate
- supporting signal summary
- strongest signal type
- weak match reason
- source social event references
- research goal

Exa is assistive:
- payload is for external company research context
- Exa does **not** mutate `social_event_matches`

## Inbound normalized result shape

Normalized enrichment should include fields like:
- `likely_company_name`
- `likely_domain`
- `industry`
- `company_description`
- `recent_initiatives`
- `hiring_or_growth_signals`
- `confidence_notes`
- `possible_match_hints`

Stored in `enrichment_results` with:
- `target_type=exa`
- `entity_type=unresolved_account_candidate`
- `entity_id=candidate_id`
- `enrichment_type=account_resolution_research` (or `company_research`)

## Commands

Inspect candidates:

```powershell
python scripts/inspect_unresolved_candidates.py --limit 10
```

Run Exa writeback (simulated):

```powershell
python -m app.writeback.run_writeback --target-type exa --selection-mode unresolved_account_candidates --endpoint-url https://your-endpoint.example --limit 10
```

Simulation artifacts:
- outbound requests: `data/outbound/exa_requests/`
- generated normalized inbound results: `data/inbound/exa_results/`

Ingest sample normalized result:

```powershell
python -m app.writeback.ingest_enrichment --file data/sample_exa_unresolved_candidate_result.json
```

Frontend demo surfaces:
- unresolved candidates: `GET /ui/unresolved-candidates`
- recent unresolved-candidate Exa results: `GET /ui/exa-unresolved-results`
