# End-to-End Tool Algorithm (Simulation + Non-Simulation)

## System Role
Local Postgres-backed GTM attribution tool that turns LinkedIn engagement into:
- deterministic identity matches,
- account intent scores,
- opportunity influence,
- operational handoff payloads.

## Mode A: Simulation
Used for demos/local testing when external access is unavailable.

### Ingestion source
- CSV/fixture-backed org-post simulation.
- Can include actor-level likes/comments/reposts and aggregate metrics.

### Flow
1. Ingest post URL (simulation data mapped to normalized events).
2. Identity resolution against CRM tables (`accounts`, `contacts`).
3. Exa simulated unresolved-candidate research (optional).
4. Intent scoring (Gemini comment analysis + deterministic scoring).
5. Opportunity attribution.
6. Optional writeback/enrichment loops.

## Mode B: Non-Simulation (Live integrations)
Used when official integrations/keys/scopes are available.

### Ingestion source
- Official LinkedIn org/community adapter path.
- No browser scraping in core flow.

### Flow
Same downstream stages as simulation:
1. Ingest (official adapter payload normalization)
2. Identity resolution
3. Exa research (live endpoint optional)
4. Intent scoring
5. Opportunity attribution
6. Writeback/enrichment

## Shared Guarantees (Both Modes)
- Deterministic matching/scoring/attribution logic.
- Idempotent reruns and rebuild modes.
- Provenance in metadata and run logs.
- Local app + Postgres remain source of truth.
- External systems are assistive sinks/sources, not core-decision owners.

## Core Tables by Layer
- Ingestion: `posts`, `social_events`, `imports_log`
- Matching: `social_event_matches`
- Scoring: `account_intent_scores`
- Attribution: `opportunity_influence`
- Orchestration: `pipeline_runs`
- Writeback: `writeback_runs`, `writeback_records`, `enrichment_results`

## Frontend Operator Sequence
1. Ingest Post URL
2. Run Identity Resolution
3. Run Exa Research for Unresolved Candidates
4. Run Intent Scoring
5. Run Opportunity Attribution

Each summary card maps directly to a stage and run history provides operational traceability.

