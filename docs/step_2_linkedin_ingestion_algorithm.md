# Step 2 Algorithm: LinkedIn Ingestion

## Goal
Normalize LinkedIn post/activity data into `posts` and `social_events` with provenance and dedupe.

## Modes
- **Simulation mode**: CSV/fixtures.
- **Non-real mode**: official org-post adapter path (no scraping), depending on API availability/scopes.

## Inputs
- Post URL (org URL ingestion path) and/or CSV imports
- Adapter payloads (official/mock)

## Process
1. Validate + normalize post URL (canonical anchor).
2. Resolve org-post identifier (real or simulated resolver mode).
3. Fetch payload via adapter.
4. Normalize into canonical events (`post_like`, `post_comment`, `post_repost`, `post_impression`, `post_link_click`, etc.).
5. Preserve provenance in `metadata_json`.
6. Dedupe with deterministic key.
7. Upsert/insert:
   - `posts`
   - `social_events`
   - `imports_log`

## Actor Safety
- If actor-level identity is not available, row is marked aggregate (`aggregated_import=true`).
- No fake actor identity is invented for aggregate rows.

## Outputs
- Clean, queryable, provenance-rich social activity rows for identity resolution.

