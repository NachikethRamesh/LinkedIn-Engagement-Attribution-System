# Intent Scoring Layer

## Purpose

Intent scoring converts matched, account-resolved activity into deterministic account-level intent scores.

This layer is:
- deterministic
- explainable
- tunable via explicit weights and bonuses
- auditable via score breakdown JSON

This layer is not attribution and does not write `opportunity_influence`.

## Inputs and exclusions

Inputs:
- `social_event_matches` joined to `social_events`
- account-linked `website_events` (optional signal boost)

Inclusion rules:
- include only events with `matched_account_id` and `match_type NOT IN ('unresolved', 'skipped_aggregate_import')`
- unresolved events do not contribute
- aggregate imports may contribute only when account-matched
- contact-level and account-level matches both roll up to account scoring
- website events contribute only when `website_events.account_id` is present

## Scoring windows

Computed windows:
- `rolling_7d`
- `rolling_30d`

Rows are written to `account_intent_scores` keyed by:
- `account_id`
- `score_date`
- `score_window`

Idempotency:
- DB uniqueness is enforced on (`account_id`, `score_date`, `score_window`)
- reruns upsert the same logical score row rather than duplicating rows

## Event weights

Base social event weights:
- `post_impression`: `0.25`
- `post_like`: `1`
- `post_comment`: `5`
- `post_repost`: `7`
- `profile_view`: `3`
- `company_page_view`: `3`
- `website_click`: `8`

## Recency weighting

Per-event multiplier by age:
- `0-7 days`: `1.0x`
- `8-14 days`: `0.75x`
- `15-30 days`: `0.5x`
- `>30 days`: `0.0x` (excluded)

## Bonuses

1. Stakeholder breadth bonus:
- `2` unique stakeholders: `+5`
- `3+` unique stakeholders: `+10`

2. High-signal bonus:
- `+5` when `3+` strong social events (`post_comment`, `post_repost`, `website_click`)
- `+4` when social engagement is followed by website signal in window

3. Website bonus:
- account-linked `website_events` contribute `2 * recency_multiplier` each
- capped at `+8`

4. Account tier bonus:
- `Tier 1`: `+4`
- `Tier 2`: `+2`
- `Tier 3`: `+0`

## Aggregate import handling

Aggregate events are dampened to avoid inflating intent:
- apply metric intensity: `min(log2(source_metric_count + 1), 5)`
- apply aggregate dampening factor: `0.35`

This keeps aggregate account-level signals useful but conservative.

## Final score

For each account/window:

`score = clamp_0_100(base_event_points + stakeholder_bonus + high_signal_bonus + website_bonus + tier_bonus)`

Stored as numeric (`account_intent_scores.score`).

## Confidence semantics

`confidence` remains numeric `[0, 1]` and is deterministic:
- anchored to average identity match confidence from contributing matched social events
- adjusted by signal volume and aggregate-signal ratio
- no contributing signals => low default confidence

Interpretation guideline:
- `>=0.80` high
- `0.60 - 0.79` medium
- `<0.60` low

## Explainability fields

Each row stores:
- `score_reason` (human-readable summary)
- `score_breakdown_json` with components:
  - `base_event_points`
  - `stakeholder_bonus`
  - `high_signal_bonus`
  - `website_bonus`
  - `tier_bonus`
  - `aggregate_signal_count`
  - `included_event_counts_by_type`
  - `recency_buckets`

Additional columns:
- `unique_stakeholder_count`
- `strong_signal_count`
- `website_signal_count`
- `contributing_event_count`
- `score_window`

## Commands

Run scoring:

```powershell
python -m app.intent_scoring.run_scoring
python -m app.intent_scoring.run_scoring --rebuild
```

Inspect scores:

```powershell
python scripts/inspect_intent_scores.py
```

Verify scoring behavior:

```powershell
python scripts/verify_intent_scoring.py
python scripts/verify_intent_scoring.py --print-snapshot
```

Latest score view:
- `v_latest_account_intent_status`

Concise summary helper:

```powershell
python scripts/inspect_intent_summary.py
```

Verification coverage includes:
- idempotency (incremental rerun snapshot unchanged)
- rebuild stability (rebuild snapshot unchanged across runs)
- duplicate-row absence
- unresolved exclusion (including fixture-based unresolved-leak check)
- aggregate safety
- explainability completeness
- ranking sanity (top vs low-signal comparison)

## Limitations

- deterministic rule engine only; no ML intent model yet
- does not score opportunity influence (that is Step 5)
- website signal usage is account-level only (no advanced journey stitching yet)
