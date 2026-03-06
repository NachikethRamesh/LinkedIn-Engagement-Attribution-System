# Step 1 Algorithm: Foundation (Schema + Seed)

## Goal
Create a deterministic local baseline so all later pipeline jobs run on the same source-of-truth schema.

## Inputs
- Local `.env` DB settings
- `db/schema.sql`
- `scripts/seed_data.py`

## Process
1. Start Postgres (Docker Compose).
2. Apply schema from `db/schema.sql`.
3. Seed only source/input tables:
   - `posts`, `social_events`, `accounts`, `contacts`, `website_events`, `opportunities`
4. Keep derived tables empty:
   - `account_intent_scores`
   - `opportunity_influence`

## Outputs
- Deterministic local dataset for ingestion, matching, scoring, attribution.

## Determinism Rules
- No external API calls.
- Repeatable seed generation patterns.
- Derived tables are not treated as source data.

