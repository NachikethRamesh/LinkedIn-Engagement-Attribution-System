from __future__ import annotations

import csv
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.db import get_connection
from app.identity_resolution.matcher import IdentityResolutionService
from app.orchestration.job_runner import JobRunner, run_record_to_dict
from app.writeback.exa_crm_enrichment import ExaCRMEnrichmentService
from app.writeback.ingest_enrichment import EnrichmentIngestionService
from app.writeback.run_writeback import WritebackService, parse_target_type
from app.writeback.selector import WritebackSelector

app = FastAPI(title="social-attribution-engine orchestration API", version="0.1.0")
runner = JobRunner()
writeback_service = WritebackService()
enrichment_service = EnrichmentIngestionService()
writeback_selector = WritebackSelector()
exa_crm_service = ExaCRMEnrichmentService()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class IngestionCSVRequest(BaseModel):
    source: str = Field(pattern="^(shield|sprout|generic)$")
    file: str
    delimiter: str = ","


class IngestionMockRequest(BaseModel):
    posts: int = 20
    events: int = 250


class IngestionOrgURLRequest(BaseModel):
    post_url: str
    simulation_mode: bool = False
    resolved_id_override: str | None = None
    run_pipeline: bool = False
    rebuild_downstream: bool = False
    window_days: int = 30


class StageRequest(BaseModel):
    rebuild: bool = False
    crm_sync_enabled: bool = False
    crm_accounts_file: str | None = None
    crm_contacts_file: str | None = None


class AttributionRequest(BaseModel):
    rebuild: bool = False
    window_days: int = 30


class FullPipelineRequest(BaseModel):
    source: str = Field(pattern="^(mock|shield_csv|sprout_csv|generic_csv)$")
    file: str | None = None
    posts: int = 20
    events: int = 250
    rebuild: bool = False
    window_days: int = 30


class WritebackRunRequest(BaseModel):
    target_type: str = Field(pattern="^(crm|clay|exa|webhook_generic)$")
    selection_mode: str | None = Field(
        default=None,
        pattern="^(high_intent_accounts|socially_influenced_opportunities|low_confidence_promising_accounts|unresolved_account_candidates)$",
    )
    limit: int = Field(default=100, ge=1, le=5000)
    min_intent_score: float | None = None
    min_intent_confidence: float | None = None
    max_intent_confidence: float | None = None
    min_contributing_events: int | None = Field(default=None, ge=0)
    min_unique_stakeholders: int | None = Field(default=None, ge=0)
    score_window: str | None = Field(default=None, pattern="^(rolling_7d|rolling_14d|rolling_30d)$")
    min_influence_band: str | None = Field(default=None, pattern="^(none|weak|medium|strong)$")
    min_influence_score: float | None = None
    min_influence_confidence: float | None = None
    min_strong_signals: int | None = Field(default=None, ge=0)
    min_recent_signals: int | None = Field(default=None, ge=0)
    recent_days: int | None = Field(default=None, ge=1)
    weak_match_confidence_threshold: float | None = None
    include_generic_candidates: bool = False
    endpoint_url: str | None = None
    timeout_seconds: int = Field(default=15, ge=1, le=120)
    dry_run: bool = False
    simulate_local: bool = False
    skip_if_previously_successful: bool = True
    trigger_source: str = "api"


class EnrichmentResultsRequest(BaseModel):
    results: list[dict[str, Any]]
    trigger_source: str = "api"


def _execute(job_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    record = runner.run_job(job_name=job_name, params=payload, trigger_source="api")
    return run_record_to_dict(record)


def _to_iso(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _get_ui_data_counts(cur) -> dict[str, int]:
    counts: dict[str, int] = {}
    table_names = [
        "accounts",
        "contacts",
        "posts",
        "social_events",
        "social_event_matches",
        "account_intent_scores",
        "opportunity_influence",
        "imports_log",
        "pipeline_runs",
        "writeback_runs",
        "writeback_records",
        "enrichment_results",
        "social_engagement_events",
        "social_comments",
        "social_posts",
        "social_engagement_actors",
        "social_post_metrics_snapshots",
        "social_comment_metrics_snapshots",
        "website_events",
        "opportunities",
    ]
    for table in table_names:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        counts[table] = int(cur.fetchone()[0])
    return counts


@app.get("/health")
def health() -> dict[str, Any]:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                _ = cur.fetchone()
                cur.execute("SELECT NOW();")
                db_now = cur.fetchone()[0]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"db_unhealthy: {exc}")

    return {
        "status": "ok",
        "service": "orchestration-api",
        "db": "ok",
        "time_utc": datetime.now(UTC).isoformat(),
        "db_time": db_now.isoformat() if hasattr(db_now, "isoformat") else str(db_now),
    }


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "social-attribution-engine orchestration API",
        "status_endpoint": "/health",
        "jobs_endpoint": "/jobs",
    }


@app.post("/jobs/linkedin-ingestion/csv")
def run_linkedin_csv(payload: IngestionCSVRequest) -> dict[str, Any]:
    return _execute("linkedin_ingestion_csv", payload.model_dump())


@app.post("/jobs/linkedin-ingestion/mock")
def run_linkedin_mock(payload: IngestionMockRequest) -> dict[str, Any]:
    return _execute("linkedin_ingestion_mock", payload.model_dump())


@app.post("/jobs/linkedin-ingestion/org-url")
def run_linkedin_org_url(payload: IngestionOrgURLRequest) -> dict[str, Any]:
    if payload.simulation_mode:
        raise HTTPException(status_code=400, detail="simulation_mode is disabled for org URL ingestion.")
    return _execute("linkedin_ingestion_org_url", payload.model_dump())


@app.post("/jobs/identity-resolution")
def run_identity_resolution(payload: StageRequest) -> dict[str, Any]:
    return _execute("identity_resolution", payload.model_dump())


@app.post("/jobs/intent-scoring")
def run_intent_scoring(payload: StageRequest) -> dict[str, Any]:
    return _execute("intent_scoring", payload.model_dump())


@app.post("/jobs/opportunity-attribution")
def run_opportunity_attribution(payload: AttributionRequest) -> dict[str, Any]:
    return _execute("opportunity_attribution", payload.model_dump())


@app.post("/jobs/full-pipeline")
def run_full_pipeline(payload: FullPipelineRequest) -> dict[str, Any]:
    if payload.source != "mock" and not payload.file:
        raise HTTPException(status_code=400, detail="'file' is required for *_csv full pipeline sources")
    return _execute("full_pipeline", payload.model_dump())


@app.get("/jobs/{run_id}")
def get_job(run_id: str) -> dict[str, Any]:
    record = runner.get_run(run_id)
    if record is None:
        raise HTTPException(status_code=404, detail="run_id not found")
    return run_record_to_dict(record)


@app.get("/jobs")
def list_jobs(limit: int = Query(default=100, ge=1, le=500)) -> list[dict[str, Any]]:
    return [run_record_to_dict(r) for r in runner.list_runs(limit=limit)]


@app.post("/ui/reset-data")
def ui_reset_data() -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]
    demo_account_ids, demo_account_names = _load_demo_account_markers(root)
    demo_contact_ids, demo_contact_emails, demo_contact_linkedin = _load_demo_contact_markers(root)

    with get_connection() as conn:
        with conn.cursor() as cur:
            before = _get_ui_data_counts(cur)
            cur.execute(
                """
                TRUNCATE TABLE
                    social_event_matches,
                    account_intent_scores,
                    opportunity_influence,
                    writeback_records,
                    writeback_runs,
                    enrichment_results,
                    pipeline_runs,
                    imports_log,
                    social_events,
                    posts,
                    website_events,
                    opportunities,
                    social_engagement_events,
                    social_comment_metrics_snapshots,
                    social_post_metrics_snapshots,
                    social_comments,
                    social_posts,
                    social_engagement_actors
                RESTART IDENTITY CASCADE;
                """
            )

            # Selective CRM cleanup: remove frontend-added + demo/dummy rows, preserve external CRM rows.
            # 1) Exa/frontend-generated rows (tagged as exa_sim:* or lacking CRM ids)
            cur.execute(
                """
                DELETE FROM contacts
                WHERE crm_contact_id LIKE 'exa_sim:%'
                   OR (crm_contact_id IS NULL AND account_id IN (
                        SELECT id FROM accounts WHERE crm_account_id IS NULL OR crm_account_id LIKE 'exa_sim:%'
                   ))
                """
            )
            cur.execute(
                """
                DELETE FROM accounts
                WHERE crm_account_id LIKE 'exa_sim:%'
                   OR crm_account_id IS NULL
                """
            )

            # 2) Dummy/demo rows from bundled sample CSV markers
            if demo_contact_ids:
                cur.execute("DELETE FROM contacts WHERE crm_contact_id = ANY(%s)", (demo_contact_ids,))
            if demo_contact_emails:
                cur.execute("DELETE FROM contacts WHERE lower(email) = ANY(%s)", (demo_contact_emails,))
            if demo_contact_linkedin:
                cur.execute("DELETE FROM contacts WHERE lower(linkedin_url) = ANY(%s)", (demo_contact_linkedin,))

            if demo_account_ids:
                cur.execute("DELETE FROM accounts WHERE crm_account_id = ANY(%s)", (demo_account_ids,))
            if demo_account_names:
                cur.execute("DELETE FROM accounts WHERE lower(company_name) = ANY(%s)", (demo_account_names,))

            # 3) Cleanup orphaned accounts after contact deletes.
            cur.execute(
                """
                DELETE FROM accounts a
                WHERE NOT EXISTS (SELECT 1 FROM contacts c WHERE c.account_id = a.id)
                  AND (a.crm_account_id IS NULL OR a.crm_account_id LIKE 'exa_sim:%')
                """
            )

            after = _get_ui_data_counts(cur)
        conn.commit()

    return {
        "status": "success",
        "message": "Frontend pipeline data cleared; frontend-added/demo CRM rows removed.",
        "before_counts": before,
        "after_counts": after,
        "preserved_reference_tables": ["accounts", "contacts (non-demo/non-frontend)"],
        "reset_at_utc": datetime.now(UTC).isoformat(),
    }


def _load_demo_account_markers(root: Path) -> tuple[list[str], list[str]]:
    configured_path = os.getenv("DEMO_CRM_ACCOUNTS_CSV", "").strip()
    if not configured_path:
        return [], []
    csv_path = Path(configured_path)
    if not csv_path.is_absolute():
        csv_path = root / csv_path
    if not csv_path.exists():
        return [], []
    crm_ids: list[str] = []
    names: list[str] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            crm_id = str(row.get("crm_account_id") or "").strip()
            name = str(row.get("company_name") or "").strip().lower()
            if crm_id:
                crm_ids.append(crm_id)
            if name:
                names.append(name)
    return crm_ids, names


def _load_demo_contact_markers(root: Path) -> tuple[list[str], list[str], list[str]]:
    configured_path = os.getenv("DEMO_CRM_CONTACTS_CSV", "").strip()
    if not configured_path:
        return [], [], []
    csv_path = Path(configured_path)
    if not csv_path.is_absolute():
        csv_path = root / csv_path
    if not csv_path.exists():
        return [], [], []
    crm_ids: list[str] = []
    emails: list[str] = []
    linkedin_urls: list[str] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            crm_id = str(row.get("crm_contact_id") or "").strip()
            email = str(row.get("email") or "").strip().lower()
            linkedin = str(row.get("linkedin_url") or "").strip().lower()
            if crm_id:
                crm_ids.append(crm_id)
            if email:
                emails.append(email)
            if linkedin:
                linkedin_urls.append(linkedin)
    return crm_ids, emails, linkedin_urls


def _load_baseline_account_names(root: Path) -> list[str]:
    csv_path = root / "data" / "accounts_current.csv"
    if not csv_path.exists():
        return []
    names: list[str] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            name = str(row.get("company_name") or "").strip().lower()
            if name:
                names.append(name)
    return names


@app.get("/ui/ingestion-latest")
def ui_ingestion_latest() -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.id,
                    p.post_url,
                    p.author_name,
                    p.topic,
                    p.cta_url,
                    p.created_at,
                    MAX(se.id) AS latest_event_id
                FROM posts p
                JOIN social_events se ON se.post_id = p.id
                WHERE se.metadata_json->>'source_name' = 'linkedin_org_api'
                  AND se.metadata_json->>'import_mode' = 'url_ingestion'
                GROUP BY p.id, p.post_url, p.author_name, p.topic, p.cta_url, p.created_at
                ORDER BY latest_event_id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row is None:
                return {
                    "source_name": "linkedin_org_api",
                    "import_mode": "url_ingestion",
                    "post": None,
                    "event_counts": {},
                    "db_counts": {"posts": 0, "social_events": 0},
                }

            post_id = int(row[0])
            post = {
                "post_id": post_id,
                "post_url": row[1],
                "author_name": row[2],
                "topic": row[3],
                "cta_url": row[4],
                "created_at": _to_iso(row[5]),
                "imported_at": None,
                "import_notes": None,
            }

            cur.execute(
                """
                SELECT imported_at, notes
                FROM imports_log
                WHERE source_name = 'linkedin_org_api'
                  AND import_mode = 'url_ingestion'
                ORDER BY imported_at DESC
                LIMIT 1
                """
            )
            import_row = cur.fetchone()
            if import_row is not None:
                post["imported_at"] = _to_iso(import_row[0])
                post["import_notes"] = import_row[1]

            cur.execute(
                """
                SELECT
                    event_type,
                    SUM(
                        CASE
                            WHEN COALESCE((metadata_json->>'aggregated_import')::boolean, false)
                                THEN COALESCE(NULLIF(metadata_json->>'source_metric_count', '')::int, 1)
                            ELSE 1
                        END
                    ) AS effective_count
                FROM social_events
                WHERE post_id = %s
                  AND metadata_json->>'source_name' = 'linkedin_org_api'
                  AND metadata_json->>'import_mode' = 'url_ingestion'
                GROUP BY event_type
                """,
                (post_id,),
            )
            event_counts = {event_type: int(count) for event_type, count in cur.fetchall()}

            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT post_id),
                    COUNT(*)
                FROM social_events
                WHERE metadata_json->>'source_name' = 'linkedin_org_api'
                  AND metadata_json->>'import_mode' = 'url_ingestion'
                """
            )
            db_counts_row = cur.fetchone()
            db_counts = {
                "posts": int(db_counts_row[0]),
                "social_events": int(db_counts_row[1]),
            }

            return {
                "source_name": "linkedin_org_api",
                "import_mode": "url_ingestion",
                "post": post,
                "event_counts": event_counts,
                "db_counts": db_counts,
            }


@app.get("/ui/identity-summary")
def ui_identity_summary(
    limit: int = Query(default=10, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]
    baseline_account_names = _load_baseline_account_names(root)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE matched_contact_id IS NOT NULL) AS contact_matches,
                    COUNT(*) FILTER (WHERE matched_contact_id IS NULL AND matched_account_id IS NOT NULL) AS account_only_matches,
                    COUNT(*) FILTER (WHERE match_type = 'unresolved') AS unresolved,
                    COUNT(*) FILTER (WHERE match_type = 'skipped_aggregate_import') AS skipped_aggregate
                FROM social_event_matches
                """
            )
            counts = cur.fetchone()
            # CRM total for demo should be baseline CRM accounts + Exa-added CRM accounts.
            # This prevents unrelated legacy/non-CRM rows from inflating the front-end number.
            baseline_count = 0
            if baseline_account_names:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM accounts
                    WHERE lower(company_name) = ANY(%s)
                    """,
                    (baseline_account_names,),
                )
                baseline_count = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM accounts WHERE crm_account_id LIKE 'exa_sim:%'")
            total_new_accounts_added_to_crm = int(cur.fetchone()[0])
            total_accounts_in_crm = baseline_count + total_new_accounts_added_to_crm

            cur.execute(
                """
                SELECT social_event_id, matched_contact_id, matched_account_id, match_type, match_confidence, match_reason, created_at
                FROM social_event_matches
                ORDER BY created_at DESC
                LIMIT 5
                """
            )
            sample_rows = [
                {
                    "social_event_id": int(r[0]),
                    "matched_contact_id": int(r[1]) if r[1] is not None else None,
                    "matched_account_id": int(r[2]) if r[2] is not None else None,
                    "match_type": r[3],
                    "match_confidence": float(r[4]),
                    "match_reason": r[5],
                    "created_at": _to_iso(r[6]),
                }
                for r in cur.fetchall()
            ]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM social_event_matches sem
                WHERE sem.match_type <> 'skipped_aggregate_import'
                  AND sem.matched_account_id IS NOT NULL
                """
            )
            total_matched_rows = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT
                    sem.social_event_id,
                    a.company_name AS account_name,
                    c.full_name AS contact_name,
                    se.event_type,
                    sem.match_type,
                    sem.match_confidence,
                    se.event_timestamp
                FROM social_event_matches sem
                JOIN social_events se ON se.id = sem.social_event_id
                LEFT JOIN accounts a ON a.id = sem.matched_account_id
                LEFT JOIN contacts c ON c.id = sem.matched_contact_id
                WHERE sem.match_type <> 'skipped_aggregate_import'
                  AND sem.matched_account_id IS NOT NULL
                ORDER BY se.event_timestamp DESC, sem.social_event_id DESC
                OFFSET %s
                LIMIT %s
                """
                ,
                (offset, limit),
            )
            matched_rows = [
                {
                    "social_event_id": int(r[0]),
                    "account_name": r[1],
                    "contact_name": r[2],
                    "engagement_type": r[3],
                    "match_type": r[4],
                    "match_confidence": float(r[5]),
                    "event_timestamp": _to_iso(r[6]),
                }
                for r in cur.fetchall()
            ]

            return {
                "counts": {
                    "resolved_total": int(counts[0]) + int(counts[1]),
                    "contact_matches": int(counts[0]),
                    "account_only_matches": int(counts[1]),
                    "unresolved": int(counts[2]),
                    "skipped_aggregate": int(counts[3]),
                    "total_accounts_in_crm": total_accounts_in_crm,
                    "total_new_accounts_added_to_crm": total_new_accounts_added_to_crm,
                },
                "samples": sample_rows,
                "matched_rows_total_count": total_matched_rows,
                "matched_rows_limit": limit,
                "matched_rows_offset": offset,
                "matched_rows": matched_rows,
            }


@app.get("/ui/intent-summary")
def ui_intent_summary(
    window: str = Query(default="rolling_30d"),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH resolved_accounts AS (
                    SELECT DISTINCT matched_account_id AS account_id
                    FROM social_event_matches
                    WHERE matched_account_id IS NOT NULL
                      AND match_type NOT IN ('unresolved', 'skipped_aggregate_import')
                )
                SELECT
                    vis.account_id,
                    vis.company_name,
                    vis.score_window,
                    vis.score,
                    vis.confidence,
                    vis.score_reason,
                    vis.unique_stakeholder_count,
                    vis.strong_signal_count,
                    vis.website_signal_count,
                    vis.contributing_event_count,
                    vis.score_date
                FROM v_latest_account_intent_status vis
                JOIN resolved_accounts ra ON ra.account_id = vis.account_id
                WHERE vis.score_window = %s
                ORDER BY vis.score DESC, vis.account_id ASC
                LIMIT %s
                """,
                (window, limit),
            )
            rows = [
                {
                    "account_id": int(r[0]),
                    "company_name": r[1],
                    "score_window": r[2],
                    "score": float(r[3]),
                    "confidence": float(r[4]),
                    "score_reason": r[5],
                    "unique_stakeholder_count": int(r[6]),
                    "strong_signal_count": int(r[7]),
                    "website_signal_count": int(r[8]),
                    "contributing_event_count": int(r[9]),
                    "score_date": _to_iso(r[10]),
                    "comment_analysis_count": 0,
                    "comment_analyses": [],
                }
                for r in cur.fetchall()
            ]
            if rows:
                account_ids = [row["account_id"] for row in rows]
                cur.execute(
                    """
                    SELECT
                        sem.matched_account_id,
                        se.id AS social_event_id,
                        se.event_timestamp,
                        COALESCE(se.metadata_json->>'comment_text', '') AS comment_text,
                        COALESCE(se.metadata_json->'comment_analysis'->>'sentiment', 'unknown') AS sentiment,
                        COALESCE(se.metadata_json->'comment_analysis'->>'intent', 'unknown') AS intent,
                        COALESCE(NULLIF(se.metadata_json->'comment_analysis'->>'confidence', '')::float, 0.0) AS confidence,
                        COALESCE(se.metadata_json->'comment_analysis'->>'summary', '') AS summary,
                        COALESCE(se.metadata_json->'comment_analysis'->>'source', '') AS source
                    FROM social_event_matches sem
                    JOIN social_events se ON se.id = sem.social_event_id
                    WHERE sem.matched_account_id = ANY(%s)
                      AND sem.match_type NOT IN ('unresolved', 'skipped_aggregate_import')
                      AND se.event_type = 'post_comment'
                      AND COALESCE((se.metadata_json->>'aggregated_import')::boolean, false) = false
                    ORDER BY se.event_timestamp DESC, se.id DESC
                    """,
                    (account_ids,),
                )
                by_account: dict[int, list[dict[str, Any]]] = {}
                for r in cur.fetchall():
                    account_id = int(r[0])
                    by_account.setdefault(account_id, []).append(
                        {
                            "social_event_id": int(r[1]),
                            "event_timestamp": _to_iso(r[2]),
                            "comment_text": r[3],
                            "sentiment": r[4],
                            "intent": r[5],
                            "confidence": float(r[6]),
                            "summary": r[7],
                            "source": r[8],
                        }
                    )
                for row in rows:
                    comment_rows = by_account.get(int(row["account_id"]), [])
                    row["comment_analysis_count"] = len(comment_rows)
                    row["comment_analyses"] = comment_rows
            return {"window": window, "top_accounts": rows}


@app.get("/ui/opportunity-summary")
def ui_opportunity_summary() -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Backward/forward-safe schema guards for Step 4 revamp.
            cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS funnel_path TEXT NOT NULL DEFAULT 'not_yet_engaged';")
            cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS commercial_progression_flag TEXT;")
            cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS opportunity_score NUMERIC(5,2);")
            cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS action_priority TEXT;")
            cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS recommended_next_action TEXT;")
            cur.execute("ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS gemini_summary TEXT;")

            cur.execute(
                """
                SELECT
                    oi.opportunity_id,
                    o.opportunity_name,
                    a.company_name,
                    oi.influence_band,
                    oi.influence_score,
                    oi.confidence,
                    oi.funnel_path,
                    oi.commercial_progression_flag,
                    oi.opportunity_score,
                    oi.action_priority,
                    oi.recommended_next_action,
                    oi.gemini_summary,
                    oi.notes
                FROM opportunity_influence oi
                JOIN opportunities o ON o.id = oi.opportunity_id
                JOIN accounts a ON a.id = oi.account_id
                ORDER BY oi.influence_score DESC, oi.opportunity_id ASC
                """
            )
            rows = [
                {
                    "opportunity_id": int(r[0]),
                    "opportunity_name": r[1],
                    "company_name": r[2],
                    "influence_band": r[3],
                    "influence_score": float(r[4]),
                    "confidence": float(r[5]),
                    "funnel_path": r[6],
                    "commercial_progression_flag": r[7],
                    "opportunity_score": float(r[8]) if r[8] is not None else None,
                    "action_priority": r[9],
                    "recommended_next_action": r[10],
                    "gemini_summary": r[11],
                    "notes": r[12],
                }
                for r in cur.fetchall()
            ]
            all_path_a = [r for r in rows if r["funnel_path"] == "already_engaged"]
            all_path_b = [r for r in rows if r["funnel_path"] == "not_yet_engaged"]
            path_a = all_path_a[:25]
            path_b = all_path_b[:25]

            cur.execute(
                """
                SELECT influence_band, COUNT(*)
                FROM opportunity_influence
                GROUP BY influence_band
                """
            )
            by_band = {band: int(count) for band, count in cur.fetchall()}

            return {
                "by_band": by_band,
                "top_opportunities": rows[:5],
                "path_a_already_engaged": path_a,
                "path_b_not_yet_engaged": path_b,
                "counts": {
                    "path_a": len(all_path_a),
                    "path_b": len(all_path_b),
                    "total": len(rows),
                },
            }


@app.get("/ui/unresolved-candidates")
def ui_unresolved_candidates(
    limit: int = Query(default=5, ge=1, le=100),
    min_contributing_events: int = Query(default=3, ge=0),
    min_strong_signals: int = Query(default=1, ge=0),
    min_recent_signals: int = Query(default=1, ge=0),
    recent_days: int = Query(default=30, ge=1, le=3650),
    weak_match_confidence_threshold: float = Query(default=0.7, ge=0, le=1),
    include_generic_candidates: bool = False,
) -> dict[str, Any]:
    selected = writeback_selector.select(
        target_type="exa",
        params={
            "selection_mode": "unresolved_account_candidates",
            "limit": limit,
            "min_contributing_events": min_contributing_events,
            "min_strong_signals": min_strong_signals,
            "min_recent_signals": min_recent_signals,
            "recent_days": recent_days,
            "weak_match_confidence_threshold": weak_match_confidence_threshold,
            "include_generic_candidates": include_generic_candidates,
        },
    )
    return {
        "count": len(selected),
        "candidates": [
            {
                "candidate_id": int(s.entity_id),
                "candidate_company_name_raw": s.data.get("candidate_company_name_raw"),
                "candidate_company_name_normalized": s.data.get("candidate_company_name_normalized"),
                "supporting_signal_summary": s.data.get("supporting_signal_summary", {}),
                "strongest_signal_type": s.data.get("strongest_signal_type"),
                "recent_signal_count": s.data.get("recent_signal_count", 0),
                "contributing_event_count": s.data.get("contributing_event_count", 0),
                "weak_match_reason": s.data.get("weak_match_reason"),
                "selection_reason": s.selection_reason,
                "source_social_event_ids": s.data.get("source_social_event_ids", []),
            }
            for s in selected
        ],
    }


@app.get("/ui/exa-unresolved-results")
def ui_exa_unresolved_results(
    limit: int = Query(default=5, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    source_run_id: str | None = Query(default=None),
) -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            effective_source_run_id = source_run_id
            if source_run_id:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM enrichment_results
                    WHERE target_type = 'exa'
                      AND entity_type = 'unresolved_account_candidate'
                      AND source_run_id = %s
                    """,
                    (source_run_id,),
                )
                total_count = int(cur.fetchone()[0])
                # If the latest writeback run was replay-skipped (no new enrichment rows),
                # fall back to the most recent unresolved-candidate Exa result set so the
                # dashboard remains informative.
                if total_count == 0:
                    cur.execute(
                        """
                        SELECT source_run_id
                        FROM enrichment_results
                        WHERE target_type = 'exa'
                          AND entity_type = 'unresolved_account_candidate'
                          AND source_run_id IS NOT NULL
                        ORDER BY received_at DESC, id DESC
                        LIMIT 1
                        """
                    )
                    fallback = cur.fetchone()
                    if fallback and fallback[0]:
                        effective_source_run_id = str(fallback[0])
                        cur.execute(
                            """
                            SELECT COUNT(*)
                            FROM enrichment_results
                            WHERE target_type = 'exa'
                              AND entity_type = 'unresolved_account_candidate'
                              AND source_run_id = %s
                            """,
                            (effective_source_run_id,),
                        )
                        total_count = int(cur.fetchone()[0])
                cur.execute(
                    """
                    SELECT
                        entity_id,
                        enrichment_type,
                        normalized_data_json,
                        source_run_id,
                        notes,
                        received_at
                    FROM enrichment_results
                    WHERE target_type = 'exa'
                      AND entity_type = 'unresolved_account_candidate'
                      AND source_run_id = %s
                    ORDER BY received_at DESC, id DESC
                    OFFSET %s
                    LIMIT %s
                    """,
                    (effective_source_run_id, offset, limit),
                )
            else:
                total_count = 0
                cur.execute(
                    """
                    SELECT
                        entity_id,
                        enrichment_type,
                        normalized_data_json,
                        source_run_id,
                        notes,
                        received_at
                    FROM enrichment_results
                    WHERE 1=0
                    OFFSET %s
                    LIMIT %s
                    """,
                    (offset, limit),
                )
            rows = cur.fetchall()
    return {
        "count": len(rows),
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "effective_source_run_id": effective_source_run_id if source_run_id else None,
        "results": [
            {
                "candidate_id": int(row[0]),
                "enrichment_type": row[1],
                "likely_company_name": (row[2] or {}).get("likely_company_name"),
                "likely_domain": (row[2] or {}).get("likely_domain"),
                "industry": (row[2] or {}).get("industry"),
                "confidence_notes": (row[2] or {}).get("confidence_notes"),
                "possible_match_hints": (row[2] or {}).get("possible_match_hints"),
                "normalized_data_json": row[2] or {},
                "source_run_id": row[3],
                "notes": row[4],
                "received_at": _to_iso(row[5]),
            }
            for row in rows
        ],
    }


@app.post("/writeback/run")
def run_writeback(payload: WritebackRunRequest) -> dict[str, Any]:
    body = payload.model_dump()
    if bool(body.get("simulate_local", False)):
        raise HTTPException(status_code=400, detail="simulate_local is disabled. Configure a real endpoint_url.")
    target_type = parse_target_type(body.pop("target_type"))
    trigger_source = str(body.pop("trigger_source", "api"))
    result = writeback_service.run(target_type=target_type, params=body, trigger_source=trigger_source)

    # NOTE: simulate_local is currently blocked above; this branch remains for future
    # restoration behind explicit product approval.
    if (
        target_type == "exa"
        and bool(body.get("simulate_local", False))
        and str(body.get("selection_mode", "")) == "unresolved_account_candidates"
    ):
        generated_files: list[str] = []
        records = result.get("records", []) if isinstance(result, dict) else []
        for record in records:
            response_json = record.get("response_json") if isinstance(record, dict) else None
            if isinstance(response_json, dict):
                file_path = response_json.get("generated_inbound_result_file")
                if isinstance(file_path, str) and file_path.strip():
                    generated_files.append(file_path.strip())

        parsed_results: list[dict[str, Any]] = []
        for file_path in generated_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    payload_json = json.load(f)
                parsed = enrichment_service.parse_payload(payload_json)
                parsed_results.extend(
                    [
                        {
                            "target_type": item.target_type,
                            "entity_type": item.entity_type,
                            "entity_id": item.entity_id,
                            "enrichment_type": item.enrichment_type,
                            "normalized_data_json": item.normalized_data_json,
                            "source_run_id": item.source_run_id,
                            "notes": item.notes,
                        }
                        for item in parsed
                    ]
                )
            except Exception:
                continue

        ingest_summary: dict[str, Any] = {"results_received": 0, "inserted": 0, "skipped_duplicates": 0}
        crm_apply_summary: dict[str, int] = {
            "accounts_created": 0,
            "accounts_updated": 0,
            "contacts_created": 0,
            "contacts_updated": 0,
        }
        identity_summary: dict[str, Any] = {}

        if parsed_results:
            ingest_inputs = enrichment_service.parse_payload({"results": parsed_results})
            ingest_summary = enrichment_service.ingest(ingest_inputs, trigger_source=f"{trigger_source}:exa_simulated")
            with get_connection() as conn:
                with conn.cursor() as cur:
                    crm_apply_summary = exa_crm_service.apply(cur=cur, results=parsed_results)
                conn.commit()
            identity_summary = IdentityResolutionService().run(rebuild=True)

        result["post_actions"] = {
            "exa_inbound_files_detected": len(generated_files),
            "enrichment_ingest_summary": ingest_summary,
            "crm_enrichment_apply_summary": crm_apply_summary,
            "identity_resolution_rerun_summary": identity_summary,
        }

    return result


@app.get("/writeback/runs")
def list_writeback_runs(limit: int = Query(default=100, ge=1, le=500)) -> list[dict[str, Any]]:
    return writeback_service.list_runs(limit=limit)


@app.get("/writeback/runs/{writeback_run_id}")
def get_writeback_run(writeback_run_id: str) -> dict[str, Any]:
    record = writeback_service.get_run(writeback_run_id)
    if record is None:
        raise HTTPException(status_code=404, detail="writeback_run_id not found")
    return record


@app.post("/writeback/enrichment-results")
def ingest_enrichment_results(payload: EnrichmentResultsRequest) -> dict[str, Any]:
    parsed = enrichment_service.parse_payload(payload.model_dump())
    return enrichment_service.ingest(parsed, trigger_source=payload.trigger_source)
