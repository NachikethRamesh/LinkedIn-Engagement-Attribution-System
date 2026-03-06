# Frontend Dashboard (Local Operator Console)

## Purpose

A lightweight dark-themed React dashboard for local demo/operator use.

It allows:
1. paste LinkedIn organization post URL
2. trigger org URL ingestion
3. inspect ingestion output
4. run downstream stages from buttons
5. inspect stage summaries + run history
6. inspect unresolved account candidates and run Exa research
7. inspect returned unresolved-candidate Exa enrichment results

## Location

- `frontend/` (React + Vite + TypeScript)

## Local run

Start backend API first:

```powershell
uvicorn app.orchestration.api:app --host 127.0.0.1 --port 8000
```

Start frontend:

```powershell
cd frontend
Copy-Item .env.example .env -ErrorAction SilentlyContinue
npm install --include=dev
npm run dev
```

Open:
- `http://127.0.0.1:5173`

## Backend endpoints used

Write/trigger:
- `POST /jobs/linkedin-ingestion/org-url`
- `POST /jobs/identity-resolution`
- `POST /jobs/intent-scoring`
- `POST /jobs/opportunity-attribution`

Run status:
- `GET /jobs/{run_id}`
- `GET /jobs`
- `GET /health`

UI summary endpoints:
- `GET /ui/ingestion-latest`
- `GET /ui/identity-summary`
- `GET /ui/intent-summary`
- `GET /ui/opportunity-summary`
- `GET /ui/unresolved-candidates`
- `GET /ui/exa-unresolved-results`

Writeback trigger endpoint:
- `POST /writeback/run` (used for unresolved-candidate -> Exa research action)

## UX flow

1. Paste org-post URL in URL Input card.
2. Use real mode with configured credentials/endpoints.
3. Click `Ingest Post URL`.
4. Watch banner + latest run status while polling completes.
5. Review Ingestion Results card (normalized URL, post metadata, counts).
6. Click next-stage buttons or `Run Full Downstream Pipeline`.
7. Review Identity/Intent/Attribution summary cards + run history.
8. Open `Unresolved Account Candidates`, inspect top candidates + weak-match reasons.
9. Click `Run Exa Research for Unresolved Candidates` (real endpoint mode).
10. Review `XR Research Summary` for likely company/domain + confidence notes.

### Simulation Status

Simulation mode is disabled in the UI and backend. The dashboard sends real-mode ingestion and writeback requests only.

## Notes

- Frontend is orchestration-only: business logic remains in backend jobs.
- Exa research does not mutate `social_event_matches`; results are stored in `enrichment_results` for manual review and downstream operational workflows.
- CORS is enabled in backend API for local frontend dev origins:
  - `http://127.0.0.1:5173`
  - `http://localhost:5173`
