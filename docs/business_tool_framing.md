# Business Tool Framing

## Problem Statement

GTM teams see organic LinkedIn engagement but struggle to connect it to:
- account prioritization
- opportunity progression
- enrichment and activation workflows

Most teams either ignore social signals or overclaim causality with weak attribution.

## Why Organic Social Attribution Is Hard

- Engagement identity is often partial or ambiguous.
- Some inputs are aggregate (not person-level).
- Opportunity creation can lag social activity by days or weeks.
- Multiple channels influence the same account simultaneously.

## Design Choice: Influence, Not Fake Causality

This system intentionally models **influence attribution**:
- deterministic lookback windows
- explainable scoring components
- confidence tied to match quality and signal strength

It does not claim that one social event directly caused pipeline movement.

## How Signals Become Actions

1. Normalize social activity and preserve provenance.
2. Resolve identities conservatively (prefer unresolved over risky matches).
3. Score account intent with transparent rules.
4. Attribute opportunity influence with clear evidence.
5. Select high-value entities for outbound activation/enrichment writeback.
6. Ingest normalized enrichment results back into the local source of truth.
7. Route unresolved account candidates to Exa research for assistive company-resolution context.

## GTM/RevOps Outcomes

- Prioritized account queues based on recent social intent.
- Opportunity context for reps and managers (why influenced, confidence, evidence).
- Structured handoff to CRM/Clay/Exa with deterministic payloads.
- Deterministic unresolved-candidate research queue for account-review workflows without weakening core matching rigor.
- Auditability for operations and leadership reviews.

## What Makes This Interview-Credible

- Deterministic and auditable over black-box heuristics.
- Separation of concerns (logic in app layer, orchestration in n8n/API).
- Explicit handling of edge cases (aggregate imports, ambiguity, replay safety).
- Practical local-first MVP that can evolve into production patterns.
