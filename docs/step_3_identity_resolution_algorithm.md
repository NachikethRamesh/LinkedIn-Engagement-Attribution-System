# Step 3 Algorithm: Identity Resolution

## Goal
Deterministically map social events to CRM entities (contacts/accounts) with explainable confidence.

## Inputs
- `social_events`
- `accounts`, `contacts` (CRM source-of-truth in this project)

## Match Order (strict precedence)
1. Aggregate-safe handling (skip/limited account inference).
2. Exact contact LinkedIn URL match.
3. Contact name + company/account match (unambiguous only).
4. Account-only company match.
5. Unresolved.

## Ambiguity Rule
- If multiple plausible matches exist, keep unresolved.
- Never force a risky match.

## Outputs
- `social_event_matches` (one final match row per social event)
- `v_social_event_match_status` for debugging/inspection.

## Determinism Rules
- No AI/embedding matching.
- Rule-based normalization only (name/company/url).
- Idempotent reruns and deterministic rebuild.

