# Step 4 Algorithm: Account Intent Scoring

## Goal
Compute deterministic account-level intent scores from resolved engagement + supporting signals.

## Inputs
- Resolved events from `social_event_matches` + `social_events`
- `website_events` (account-linked only)
- Exa enrichment context from `enrichment_results` (assistive bonus only)

## Important Current Behavior
- Scores are computed for **resolved accounts only**.
- At scoring start, Gemini comment analysis is executed for resolved comment events with comment text.

## Scoring Components
1. Base weighted events (e.g., likes/comments/reposts/impressions/link clicks).
2. Recency multipliers (0-7, 8-14, 15-30, >30 days).
3. Stakeholder breadth bonus.
4. High-signal sequence bonus.
5. Optional website reinforcement bonus.
6. Target-tier adjustment.
7. Exa research bonus (assistive context).
8. Comment AI bonus (Gemini sentiment + intent + confidence).
9. Aggregate dampening for aggregate-derived signals.

## Outputs
- `account_intent_scores` rows by `(account_id, score_date, score_window)` with:
  - `score`
  - `score_reason`
  - `score_breakdown_json`
  - `contributing_event_count`
  - `confidence`

## Determinism Rules
- No opaque ML scoring.
- Same input state -> same score output.
- Upsert-based idempotency.

