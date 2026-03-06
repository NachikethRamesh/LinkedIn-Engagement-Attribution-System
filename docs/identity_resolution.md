# Identity Resolution Layer

## Purpose

Identity resolution maps raw `social_events` to CRM entities so later phases can compute intent and influence without guessing identities at scoring time.

This step is deterministic and auditable:
- no AI/LLM matching
- no embeddings
- no hidden heuristics
- every match stores a reason and matched fields

## Output table

`social_event_matches` is a derived table keyed by `social_event_id`.

Columns:
- `social_event_id`
- `matched_contact_id` (nullable)
- `matched_account_id` (nullable)
- `match_type`
- `match_confidence`
- `match_reason`
- `matched_on_fields_json`
- `created_at`

## Matching order

For each unmatched social event:

1. Aggregate safety handling
- If `metadata_json.aggregated_import = true`:
  - never create person-level matches
  - allow account-only match when deterministic
  - else mark `skipped_aggregate_import`

2. Contact match by LinkedIn URL
- `actor_linkedin_url` -> exact normalized match to `contacts.linkedin_url`
- Match type: `exact_contact_linkedin_url`

3. Contact match by actor name + account
- Normalize `actor_name` and `actor_company_raw`
- First derive deterministic account match
- Then match `actor_name` to `contacts.full_name` inside that account
- Only accepted when unambiguous
- Match type: `exact_contact_name_and_account`

4. Account-only match by company/domain
- Exact company name: `exact_account_name`
- Normalized company name: `normalized_account_name`
- Domain-like company field to account domain: `inferred_from_website_domain`
- Token-overlap fallback (single account only): `inferred_from_actor_company`

5. No match
- Match type: `unresolved`

## Confidence model

Deterministic confidence assignments:
- `exact_contact_linkedin_url`: `0.95`
- `exact_contact_name_and_account`: `0.85`
- `exact_account_name`: `0.80`
- `normalized_account_name`: `0.75`
- `inferred_from_website_domain`: `0.60`
- `inferred_from_actor_company`: `0.60`
- `skipped_aggregate_import`: `0.00`
- `unresolved`: `0.00`

## Normalization rules

Implemented in `app/identity_resolution/normalization.py`:
- Person names: lowercase, punctuation stripped, whitespace normalized
- Company names: lowercase, punctuation stripped, suffix cleanup (`inc`, `llc`, `ltd`, `corp`, etc.)
- LinkedIn URLs: canonical host, remove query/fragment, remove trailing slash
- Domains: remove protocol/path and normalize `www.`

## Idempotency and run modes

CLI:

```powershell
python -m app.identity_resolution.run_matching
python -m app.identity_resolution.run_matching --rebuild
```

Behavior:
- default mode processes only events not already in `social_event_matches`
- `--rebuild` truncates and recomputes all matches
- upsert on `social_event_id` keeps reruns safe and deterministic
- one social event maps to at most one final match row (`social_event_id` is unique)
- rerunning incremental mode is idempotent (already matched events are not reprocessed)
- uniqueness is DB-enforced (`UNIQUE` on `social_event_id` plus unique index)

## Inspecting results

```powershell
python scripts/inspect_matches.py
```

Also available:
- SQL view `v_social_event_match_status` (includes `source_name`, `aggregated_import`, `actor_origin`, and `has_match_row`)
- unmatched events appear with `has_match_row = false`

Concise summary helper:

```powershell
python scripts/inspect_match_summary.py
```

Verification script:

```powershell
python scripts/verify_identity_resolution.py
python scripts/verify_identity_resolution.py --include-ambiguity-fixture
```

Checks performed:
- idempotency + rebuild stability
- rebuild stability compares full snapshots across two rebuild runs:
  total match rows, counts by `match_type`, matched contact/account counts,
  `unresolved` count, and `skipped_aggregate_import` count
- aggregate-import safety (no contact matches for aggregate rows)
- confidence/field completeness + referential integrity
- optional ambiguity fixture that must remain unresolved

## Ambiguity policy

- Ambiguous contact candidates => `unresolved`
- Ambiguous account candidates => `unresolved`
- Matching intentionally prefers unresolved over risky over-matching

## Aggregate-import policy

- Aggregate imports never produce contact-level matches
- They may produce account-only matches when deterministic
- Otherwise they are labeled `skipped_aggregate_import`
- `match_reason` and `matched_on_fields_json` explicitly record aggregate-safe handling

## Limitations (intentional for this phase)

- No probabilistic/fuzzy AI matching beyond deterministic normalization + token overlap fallback
- Ambiguous contact candidates are not force-matched
- No cross-channel identity graph yet (email, cookies, CRM activity stitching deferred)
- No intent scoring or opportunity influence logic in this phase
