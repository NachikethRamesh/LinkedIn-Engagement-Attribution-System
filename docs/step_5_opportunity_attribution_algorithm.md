# Step 5 Algorithm: Opportunity Influence Attribution

## Goal
Classify opportunity influence from pre-opportunity social/intent signals (not strict causality).

## Inputs
- `opportunities`
- Resolved social signals (`social_event_matches` + `social_events`)
- `account_intent_scores`
- Account-linked `website_events`

## Window Logic
- Primary lookback before `opportunities.created_at` (e.g., 30-day).
- Post-opportunity signals are excluded from sourcing influence.

## Scoring Components
1. Pre-opp social signal strength (weighted + recency).
2. Stakeholder breadth and strong-signal counts.
3. Website reinforcement.
4. Intent snapshot contribution.
5. Proximity bonus.
6. Confidence adjustments (aggregate-heavy/weak-match dampening).

## Outputs
- `opportunity_influence` (one final row per opportunity), including:
  - `influence_score`
  - `influence_band`
  - `matched_event_count`, `matched_post_count`
  - `confidence`
  - `notes` / breakdown

## Determinism Rules
- Explainable influence model.
- Idempotent rebuild/incremental runs.

