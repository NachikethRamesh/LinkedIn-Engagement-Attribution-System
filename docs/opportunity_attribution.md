# Opportunity Influence Attribution

## Philosophy

This layer provides deterministic influence attribution, not causal proof.

It answers:
- whether an opportunity/account appears socially influenced
- whether the account is already in the funnel (Path A) or not yet engaged (Path B)
- what action should be taken next and why

It does not claim guaranteed direct causation.

## Inputs

Per opportunity/account, within lookback window before `opportunities.created_at`:
- matched social events (`social_event_matches` + `social_events`)
- account-linked website events (`website_events.account_id`)
- latest intent score snapshot in window (`account_intent_scores`)

Excluded:
- unresolved match rows
- skipped aggregate imports
- social activity after opportunity creation

Aggregate imports:
- allowed only when account-matched upstream
- dampened in attribution math via aggregate intensity and dampening factor

## Lookback windows

Supported:
- `30d` (default)
- `60d` (optional CLI switch)

## Path split logic

For each resolved account/opportunity:

- **Path A — already engaged in funnel** if farthest deterministic progression is found:
  - `Purchased`
  - `In Sales Process`
  - `Replied to Outbound`
  - `Visited Website`
- **Path B — not yet engaged** when none of the above exists.

Funnel-state source of truth:
- `accounts.website_visited`, `accounts.website_last_visited_at`
- `accounts.outbound_replied`, `accounts.outbound_replied_at`
- `accounts.sales_process_started`, `accounts.sales_process_stage`, `accounts.sales_process_started_at`
- `accounts.purchased_or_closed_won`, `accounts.purchased_at`

For local demo reliability, Step 4 auto-populates deterministic funnel-state examples for resolved accounts that have no state yet.

## Influence formula (0-100)

Components:
- `social_event_points`: weighted social events with recency multipliers
- `website_points`: account website reinforcement
- `stakeholder_bonus`: breadth of unique stakeholders
- `strong_signal_bonus`: comments/reposts/website_click density
- `sequence_bonus`: social engagement followed by website activity
- `intent_score_component`: recent account intent score contribution
- `proximity_bonus`: closeness of last social touch to opportunity creation
- `aggregate_dampening_adjustment`: penalty when aggregate ratio is high

Final score:
- `influence_score = clamp(0..100, sum(components))`

Path behavior:
- **Path A**: score is progression-weighted (commercial progression emphasis).
- **Path B**: score is deterministic opportunity score from intent + engagement mix + recency + stakeholder breadth + strong signals + aggregate dampening.

Influence bands:
- `none` (<20)
- `weak` (20-44.99)
- `medium` (45-69.99)
- `strong` (>=70)

## Confidence

`confidence` is numeric `[0,1]` and deterministic.

Driven by:
- average identity-match confidence of contributing social events
- signal richness (count + stakeholder breadth)
- intent snapshot availability
- aggregate-heavy activity dampening

Interpretation guideline:
- `>=0.80` high reliability
- `0.60-0.79` medium
- `<0.60` low

## Output table

`opportunity_influence` stores one final row per opportunity (`opportunity_id` unique).

Key fields:
- `influence_score`
- `influence_band`
- `influenced`
- `influence_window_days`
- `matched_event_count`
- `matched_post_count`
- `unique_stakeholder_count`
- `website_signal_count`
- `intent_score_snapshot`
- `strongest_signal_type`
- `last_social_touch_at`
- `days_from_last_social_touch_to_opp`
- `confidence`
- `funnel_path`
- `commercial_progression_flag`
- `opportunity_score` (Path B)
- `action_priority`
- `recommended_next_action`
- `gemini_summary` (optional lightweight summary)
- `notes`
- `score_breakdown_json`

## CLI

Run attribution:

```powershell
python -m app.opportunity_attribution.run_attribution
python -m app.opportunity_attribution.run_attribution --rebuild
python -m app.opportunity_attribution.run_attribution --window-days 60 --rebuild
```

Inspect output:

```powershell
python scripts/inspect_opportunity_influence.py
```

Verify determinism/safety:

```powershell
python scripts/verify_opportunity_attribution.py
python scripts/verify_opportunity_attribution.py --print-snapshot
python scripts/verify_opportunity_attribution.py --include-fixture
```

## Debug view

`v_opportunity_influence_status` provides a joined inspection surface for opportunities, accounts, and influence outputs.

## Limitations

- influence attribution only; no causal claim
- no stage-movement attribution decomposition yet
- no orchestration scheduling yet (single-job CLI execution)
- Gemini is assistive summarization only; deterministic scoring/path split remains source of truth
