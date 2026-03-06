CREATE TABLE IF NOT EXISTS posts (
    id BIGSERIAL PRIMARY KEY,
    author_name TEXT NOT NULL,
    post_url TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    cta_url TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    id BIGSERIAL PRIMARY KEY,
    company_name TEXT NOT NULL UNIQUE,
    crm_account_id TEXT,
    domain TEXT,
    target_tier TEXT NOT NULL,
    website_visited BOOLEAN NOT NULL DEFAULT FALSE,
    website_last_visited_at TIMESTAMPTZ,
    outbound_replied BOOLEAN NOT NULL DEFAULT FALSE,
    outbound_replied_at TIMESTAMPTZ,
    sales_process_started BOOLEAN NOT NULL DEFAULT FALSE,
    sales_process_stage TEXT,
    sales_process_started_at TIMESTAMPTZ,
    purchased_or_closed_won BOOLEAN NOT NULL DEFAULT FALSE,
    purchased_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS social_events (
    id BIGSERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    actor_name TEXT,
    actor_linkedin_url TEXT,
    actor_company_raw TEXT,
    event_type TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS social_engagement_actors (
    id BIGSERIAL PRIMARY KEY,
    platform TEXT NOT NULL DEFAULT 'linkedin',
    external_actor_id TEXT,
    actor_urn TEXT,
    display_name TEXT,
    profile_url TEXT,
    headline TEXT,
    title TEXT,
    company_name TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    dedupe_key TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS social_posts (
    id BIGSERIAL PRIMARY KEY,
    platform TEXT NOT NULL DEFAULT 'linkedin',
    workspace_id TEXT,
    tenant_id TEXT,
    sync_source TEXT NOT NULL,
    sync_job_id TEXT,
    platform_post_id TEXT,
    platform_post_urn TEXT,
    post_url TEXT NOT NULL,
    author_name TEXT,
    organization_name TEXT,
    text_content TEXT,
    post_created_at TIMESTAMPTZ,
    raw_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    dedupe_key TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS social_comments (
    id BIGSERIAL PRIMARY KEY,
    platform TEXT NOT NULL DEFAULT 'linkedin',
    social_post_id BIGINT NOT NULL REFERENCES social_posts(id) ON DELETE CASCADE,
    platform_comment_id TEXT NOT NULL,
    parent_platform_comment_id TEXT,
    parent_comment_id BIGINT REFERENCES social_comments(id) ON DELETE SET NULL,
    depth INTEGER NOT NULL DEFAULT 0,
    actor_id BIGINT REFERENCES social_engagement_actors(id) ON DELETE SET NULL,
    comment_text TEXT,
    comment_created_at TIMESTAMPTZ,
    raw_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    dedupe_key TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS social_engagement_events (
    id BIGSERIAL PRIMARY KEY,
    platform TEXT NOT NULL DEFAULT 'linkedin',
    workspace_id TEXT,
    tenant_id TEXT,
    sync_source TEXT NOT NULL,
    sync_job_id TEXT,
    platform_object_type TEXT NOT NULL,
    platform_object_id TEXT NOT NULL,
    parent_platform_object_id TEXT,
    social_post_id BIGINT REFERENCES social_posts(id) ON DELETE SET NULL,
    social_comment_id BIGINT REFERENCES social_comments(id) ON DELETE SET NULL,
    actor_id BIGINT REFERENCES social_engagement_actors(id) ON DELETE SET NULL,
    actor_resolution_status TEXT NOT NULL DEFAULT 'unresolved',
    engagement_type TEXT NOT NULL,
    engagement_timestamp TIMESTAMPTZ NOT NULL,
    raw_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    availability_status TEXT NOT NULL DEFAULT 'not_exposed',
    dedupe_key TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_social_engagement_object_type CHECK (
        platform_object_type IN ('post', 'comment', 'reply', 'profile_interaction', 'company_page_interaction')
    ),
    CONSTRAINT chk_social_engagement_actor_resolution CHECK (
        actor_resolution_status IN ('resolved', 'unresolved', 'aggregate_only')
    ),
    CONSTRAINT chk_social_engagement_availability CHECK (
        availability_status IN ('actor_resolved', 'aggregate_only', 'not_exposed')
    )
);

CREATE TABLE IF NOT EXISTS social_post_metrics_snapshots (
    id BIGSERIAL PRIMARY KEY,
    platform TEXT NOT NULL DEFAULT 'linkedin',
    social_post_id BIGINT NOT NULL REFERENCES social_posts(id) ON DELETE CASCADE,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    reaction_count INTEGER,
    comment_count INTEGER,
    repost_count INTEGER,
    impression_count INTEGER,
    reach_count INTEGER,
    click_count INTEGER,
    raw_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    dedupe_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS social_comment_metrics_snapshots (
    id BIGSERIAL PRIMARY KEY,
    platform TEXT NOT NULL DEFAULT 'linkedin',
    social_comment_id BIGINT NOT NULL REFERENCES social_comments(id) ON DELETE CASCADE,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    reaction_count INTEGER,
    reply_count INTEGER,
    raw_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    dedupe_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS contacts (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    crm_contact_id TEXT,
    full_name TEXT NOT NULL,
    email TEXT,
    linkedin_url TEXT,
    title TEXT
);

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS crm_account_id TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS crm_contact_id TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS website_visited BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS website_last_visited_at TIMESTAMPTZ;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS outbound_replied BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS outbound_replied_at TIMESTAMPTZ;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS sales_process_started BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS sales_process_stage TEXT;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS sales_process_started_at TIMESTAMPTZ;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS purchased_or_closed_won BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS purchased_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS website_events (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
    anonymous_visitor_id TEXT,
    page_url TEXT NOT NULL,
    utm_source TEXT,
    utm_campaign TEXT,
    event_timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS opportunities (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    opportunity_name TEXT NOT NULL,
    stage TEXT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    closed_won_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS account_intent_scores (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    score_date DATE NOT NULL,
    score NUMERIC(5,2) NOT NULL,
    score_window TEXT NOT NULL DEFAULT 'rolling_30d',
    score_reason TEXT,
    confidence NUMERIC(4,2) NOT NULL,
    unique_stakeholder_count INTEGER NOT NULL DEFAULT 0,
    strong_signal_count INTEGER NOT NULL DEFAULT 0,
    website_signal_count INTEGER NOT NULL DEFAULT 0,
    contributing_event_count INTEGER NOT NULL DEFAULT 0,
    score_breakdown_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT chk_account_intent_window CHECK (score_window IN ('rolling_7d', 'rolling_30d', 'rolling_14d'))
);

ALTER TABLE account_intent_scores ADD COLUMN IF NOT EXISTS score_window TEXT NOT NULL DEFAULT 'rolling_30d';
ALTER TABLE account_intent_scores ADD COLUMN IF NOT EXISTS unique_stakeholder_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_intent_scores ADD COLUMN IF NOT EXISTS strong_signal_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_intent_scores ADD COLUMN IF NOT EXISTS website_signal_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_intent_scores ADD COLUMN IF NOT EXISTS contributing_event_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_intent_scores ADD COLUMN IF NOT EXISTS score_breakdown_json JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS opportunity_influence (
    id BIGSERIAL PRIMARY KEY,
    opportunity_id BIGINT NOT NULL REFERENCES opportunities(id) ON DELETE CASCADE,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    influence_score NUMERIC(5,2) NOT NULL,
    influence_band TEXT NOT NULL DEFAULT 'none',
    influenced BOOLEAN NOT NULL DEFAULT FALSE,
    influence_window_days INTEGER NOT NULL,
    matched_event_count INTEGER NOT NULL,
    matched_post_count INTEGER NOT NULL,
    unique_stakeholder_count INTEGER NOT NULL DEFAULT 0,
    website_signal_count INTEGER NOT NULL DEFAULT 0,
    intent_score_snapshot NUMERIC(5,2),
    strongest_signal_type TEXT,
    last_social_touch_at TIMESTAMPTZ,
    days_from_last_social_touch_to_opp INTEGER,
    confidence NUMERIC(4,2) NOT NULL,
    funnel_path TEXT NOT NULL DEFAULT 'not_yet_engaged',
    commercial_progression_flag TEXT,
    opportunity_score NUMERIC(5,2),
    action_priority TEXT,
    recommended_next_action TEXT,
    gemini_summary TEXT,
    notes TEXT,
    score_breakdown_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT chk_opportunity_influence_band CHECK (
        influence_band IN ('none', 'weak', 'medium', 'strong')
    ),
    CONSTRAINT chk_opportunity_influence_path CHECK (
        funnel_path IN ('already_engaged', 'not_yet_engaged')
    ),
    CONSTRAINT chk_opportunity_influence_confidence CHECK (confidence >= 0 AND confidence <= 1),
    CONSTRAINT chk_opportunity_influence_score CHECK (influence_score >= 0 AND influence_score <= 100)
);

ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS influence_band TEXT NOT NULL DEFAULT 'none';
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS influenced BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS unique_stakeholder_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS website_signal_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS intent_score_snapshot NUMERIC(5,2);
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS strongest_signal_type TEXT;
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS last_social_touch_at TIMESTAMPTZ;
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS days_from_last_social_touch_to_opp INTEGER;
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS score_breakdown_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS funnel_path TEXT NOT NULL DEFAULT 'not_yet_engaged';
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS commercial_progression_flag TEXT;
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS opportunity_score NUMERIC(5,2);
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS action_priority TEXT;
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS recommended_next_action TEXT;
ALTER TABLE opportunity_influence ADD COLUMN IF NOT EXISTS gemini_summary TEXT;

CREATE TABLE IF NOT EXISTS imports_log (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    filename TEXT,
    import_mode TEXT NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    row_count INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    skip_count INTEGER NOT NULL DEFAULT 0,
    warning_count INTEGER NOT NULL DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS social_event_matches (
    id BIGSERIAL PRIMARY KEY,
    social_event_id BIGINT NOT NULL UNIQUE REFERENCES social_events(id) ON DELETE CASCADE,
    matched_contact_id BIGINT REFERENCES contacts(id) ON DELETE SET NULL,
    matched_account_id BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
    match_type TEXT NOT NULL,
    match_confidence NUMERIC(4,2) NOT NULL,
    match_reason TEXT NOT NULL,
    matched_on_fields_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_social_event_matches_confidence_range CHECK (match_confidence >= 0 AND match_confidence <= 1),
    CONSTRAINT chk_social_event_matches_contact_requires_account CHECK (
        matched_contact_id IS NULL OR matched_account_id IS NOT NULL
    ),
    CONSTRAINT chk_social_event_matches_type CHECK (
        match_type IN (
            'exact_contact_linkedin_url',
            'exact_contact_name_and_account',
            'exact_account_name',
            'normalized_account_name',
            'inferred_from_actor_company',
            'inferred_from_website_domain',
            'unresolved',
            'skipped_aggregate_import'
        )
    )
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    job_name TEXT NOT NULL,
    stage_name TEXT,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms BIGINT,
    trigger_source TEXT NOT NULL DEFAULT 'manual',
    input_params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS writeback_runs (
    id BIGSERIAL PRIMARY KEY,
    writeback_run_id TEXT NOT NULL UNIQUE,
    target_type TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms BIGINT,
    trigger_source TEXT NOT NULL DEFAULT 'manual',
    selection_params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    CONSTRAINT chk_writeback_runs_target_type CHECK (
        target_type IN ('crm', 'clay', 'exa', 'webhook_generic')
    ),
    CONSTRAINT chk_writeback_runs_status CHECK (
        status IN ('queued', 'running', 'success', 'partial_success', 'failed')
    )
);

CREATE TABLE IF NOT EXISTS writeback_records (
    id BIGSERIAL PRIMARY KEY,
    writeback_run_id TEXT NOT NULL REFERENCES writeback_runs(writeback_run_id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    target_type TEXT NOT NULL,
    external_key TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    response_json JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_writeback_records_entity_type CHECK (
        entity_type IN ('account', 'opportunity', 'contact', 'unresolved_account_candidate')
    ),
    CONSTRAINT chk_writeback_records_target_type CHECK (
        target_type IN ('crm', 'clay', 'exa', 'webhook_generic')
    ),
    CONSTRAINT chk_writeback_records_status CHECK (
        status IN ('pending', 'sent', 'success', 'failed', 'skipped')
    )
);

CREATE TABLE IF NOT EXISTS enrichment_results (
    id BIGSERIAL PRIMARY KEY,
    target_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    enrichment_type TEXT NOT NULL,
    normalized_data_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_run_id TEXT,
    notes TEXT,
    dedupe_key TEXT NOT NULL,
    CONSTRAINT chk_enrichment_results_target_type CHECK (
        target_type IN ('crm', 'clay', 'exa', 'webhook_generic')
    ),
    CONSTRAINT chk_enrichment_results_entity_type CHECK (
        entity_type IN ('account', 'opportunity', 'contact', 'unresolved_account_candidate')
    )
);

ALTER TABLE writeback_records DROP CONSTRAINT IF EXISTS chk_writeback_records_entity_type;
ALTER TABLE writeback_records
    ADD CONSTRAINT chk_writeback_records_entity_type CHECK (
        entity_type IN ('account', 'opportunity', 'contact', 'unresolved_account_candidate')
    );

ALTER TABLE enrichment_results DROP CONSTRAINT IF EXISTS chk_enrichment_results_entity_type;
ALTER TABLE enrichment_results
    ADD CONSTRAINT chk_enrichment_results_entity_type CHECK (
        entity_type IN ('account', 'opportunity', 'contact', 'unresolved_account_candidate')
    );

CREATE INDEX IF NOT EXISTS idx_social_events_post_id ON social_events(post_id);
CREATE INDEX IF NOT EXISTS idx_social_events_event_timestamp ON social_events(event_timestamp);
CREATE UNIQUE INDEX IF NOT EXISTS uq_social_events_dedupe_key
    ON social_events ((metadata_json->>'dedupe_key'))
    WHERE metadata_json ? 'dedupe_key';
CREATE INDEX IF NOT EXISTS idx_contacts_account_id ON contacts(account_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_accounts_crm_account_id
    ON accounts(crm_account_id)
    WHERE crm_account_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_contacts_crm_contact_id
    ON contacts(crm_contact_id)
    WHERE crm_contact_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_website_events_account_id ON website_events(account_id);
CREATE INDEX IF NOT EXISTS idx_website_events_event_timestamp ON website_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_opportunities_account_id ON opportunities(account_id);
CREATE INDEX IF NOT EXISTS idx_account_intent_scores_account_id ON account_intent_scores(account_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_account_intent_scores_account_date_window
    ON account_intent_scores(account_id, score_date, score_window);
CREATE INDEX IF NOT EXISTS idx_opportunity_influence_opportunity_id ON opportunity_influence(opportunity_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_opportunity_influence_opportunity_id ON opportunity_influence(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_imports_log_imported_at ON imports_log(imported_at);
CREATE INDEX IF NOT EXISTS idx_social_event_matches_match_type ON social_event_matches(match_type);
CREATE INDEX IF NOT EXISTS idx_social_event_matches_account_id ON social_event_matches(matched_account_id);
CREATE INDEX IF NOT EXISTS idx_social_event_matches_contact_id ON social_event_matches(matched_contact_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_social_event_matches_social_event_id ON social_event_matches(social_event_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON pipeline_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_job_name ON pipeline_runs(job_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_writeback_runs_started_at ON writeback_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_writeback_runs_target_type ON writeback_runs(target_type);
CREATE INDEX IF NOT EXISTS idx_writeback_runs_status ON writeback_runs(status);
CREATE INDEX IF NOT EXISTS idx_writeback_records_run_id ON writeback_records(writeback_run_id);
CREATE INDEX IF NOT EXISTS idx_writeback_records_target_status ON writeback_records(target_type, status);
CREATE INDEX IF NOT EXISTS idx_writeback_records_entity ON writeback_records(entity_type, entity_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_writeback_records_run_entity_target
    ON writeback_records(writeback_run_id, entity_type, entity_id, target_type);
CREATE UNIQUE INDEX IF NOT EXISTS uq_enrichment_results_dedupe_key ON enrichment_results(dedupe_key);
CREATE INDEX IF NOT EXISTS idx_enrichment_results_entity ON enrichment_results(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_results_target_type ON enrichment_results(target_type);
CREATE UNIQUE INDEX IF NOT EXISTS uq_social_engagement_actors_dedupe_key ON social_engagement_actors(dedupe_key);
CREATE INDEX IF NOT EXISTS idx_social_engagement_actors_external_actor_id ON social_engagement_actors(external_actor_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_social_posts_dedupe_key ON social_posts(dedupe_key);
CREATE INDEX IF NOT EXISTS idx_social_posts_post_url ON social_posts(post_url);
CREATE INDEX IF NOT EXISTS idx_social_posts_platform_post_id ON social_posts(platform_post_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_social_comments_dedupe_key ON social_comments(dedupe_key);
CREATE INDEX IF NOT EXISTS idx_social_comments_post_id ON social_comments(social_post_id);
CREATE INDEX IF NOT EXISTS idx_social_comments_parent_comment_id ON social_comments(parent_comment_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_social_engagement_events_dedupe_key ON social_engagement_events(dedupe_key);
CREATE INDEX IF NOT EXISTS idx_social_engagement_events_post_id ON social_engagement_events(social_post_id);
CREATE INDEX IF NOT EXISTS idx_social_engagement_events_comment_id ON social_engagement_events(social_comment_id);
CREATE INDEX IF NOT EXISTS idx_social_engagement_events_actor_id ON social_engagement_events(actor_id);
CREATE INDEX IF NOT EXISTS idx_social_engagement_events_type_ts ON social_engagement_events(engagement_type, engagement_timestamp);
CREATE UNIQUE INDEX IF NOT EXISTS uq_social_post_metrics_snapshots_dedupe_key ON social_post_metrics_snapshots(dedupe_key);
CREATE UNIQUE INDEX IF NOT EXISTS uq_social_comment_metrics_snapshots_dedupe_key ON social_comment_metrics_snapshots(dedupe_key);

CREATE OR REPLACE VIEW crm_entities AS
SELECT
    c.id AS contact_id,
    a.id AS account_id,
    a.crm_account_id,
    c.crm_contact_id,
    a.company_name,
    a.domain,
    a.target_tier,
    c.full_name,
    c.email,
    c.linkedin_url,
    c.title,
    a.created_at AS account_created_at
FROM contacts c
JOIN accounts a ON a.id = c.account_id;

CREATE OR REPLACE VIEW v_social_event_match_status AS
SELECT
    se.id AS social_event_id,
    se.event_type,
    se.event_timestamp,
    se.actor_name,
    se.actor_linkedin_url,
    se.actor_company_raw,
    se.metadata_json->>'source_name' AS source_name,
    se.metadata_json->>'import_mode' AS import_mode,
    COALESCE((se.metadata_json->>'aggregated_import')::boolean, false) AS aggregated_import,
    se.metadata_json->>'actor_origin' AS actor_origin,
    CASE WHEN sem.social_event_id IS NOT NULL THEN true ELSE false END AS has_match_row,
    sem.matched_contact_id,
    sem.matched_account_id,
    sem.match_type,
    sem.match_confidence,
    sem.match_reason,
    sem.created_at AS matched_at
FROM social_events se
LEFT JOIN social_event_matches sem ON sem.social_event_id = se.id;

CREATE OR REPLACE VIEW v_latest_account_intent_status AS
SELECT
    ais.account_id,
    a.company_name,
    ais.score_date,
    ais.score_window,
    ais.score,
    ais.confidence,
    ais.score_reason,
    ais.unique_stakeholder_count,
    ais.strong_signal_count,
    ais.website_signal_count,
    ais.contributing_event_count
FROM account_intent_scores ais
JOIN accounts a ON a.id = ais.account_id
WHERE ais.score_date = (
    SELECT MAX(inner_ais.score_date)
    FROM account_intent_scores inner_ais
    WHERE inner_ais.account_id = ais.account_id
      AND inner_ais.score_window = ais.score_window
);

CREATE OR REPLACE VIEW v_opportunity_influence_status AS
SELECT
    oi.opportunity_id,
    o.opportunity_name,
    o.stage,
    o.created_at AS opportunity_created_at,
    oi.account_id,
    a.company_name,
    oi.influence_score,
    oi.influence_band,
    oi.influenced,
    oi.influence_window_days,
    oi.matched_event_count,
    oi.matched_post_count,
    oi.unique_stakeholder_count,
    oi.website_signal_count,
    oi.intent_score_snapshot,
    oi.strongest_signal_type,
    oi.last_social_touch_at,
    oi.days_from_last_social_touch_to_opp,
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
JOIN accounts a ON a.id = oi.account_id;

CREATE OR REPLACE VIEW v_social_engagement_event_rollups AS
SELECT
    sp.id AS social_post_id,
    sp.post_url,
    see.engagement_type,
    COUNT(*) AS engagement_event_count,
    COUNT(*) FILTER (WHERE see.actor_resolution_status = 'resolved') AS actor_resolved_count,
    COUNT(*) FILTER (WHERE see.actor_resolution_status = 'aggregate_only') AS aggregate_only_count,
    MIN(see.engagement_timestamp) AS first_engagement_at,
    MAX(see.engagement_timestamp) AS last_engagement_at
FROM social_engagement_events see
JOIN social_posts sp ON sp.id = see.social_post_id
GROUP BY sp.id, sp.post_url, see.engagement_type;

CREATE OR REPLACE VIEW account_enrichment_summary AS
SELECT
    er.entity_id AS account_id,
    a.company_name,
    COUNT(*) AS enrichment_result_count,
    MAX(er.received_at) AS last_enriched_at,
    ARRAY_AGG(DISTINCT er.target_type) AS targets_seen,
    ARRAY_AGG(DISTINCT er.enrichment_type) AS enrichment_types_seen
FROM enrichment_results er
JOIN accounts a ON a.id = er.entity_id
WHERE er.entity_type = 'account'
GROUP BY er.entity_id, a.company_name;

CREATE OR REPLACE VIEW v_demo_pipeline_summary AS
SELECT
    pr.run_id,
    pr.job_name,
    pr.status,
    pr.started_at,
    pr.completed_at,
    pr.duration_ms,
    pr.trigger_source,
    pr.output_metrics_json,
    pr.error_message
FROM pipeline_runs pr
ORDER BY pr.started_at DESC;

CREATE OR REPLACE VIEW v_demo_account_summary AS
SELECT
    vis.account_id,
    vis.company_name,
    vis.score_date,
    vis.score_window,
    vis.score,
    vis.confidence,
    vis.score_reason,
    vis.unique_stakeholder_count,
    vis.strong_signal_count,
    vis.website_signal_count,
    vis.contributing_event_count,
    aes.enrichment_result_count,
    aes.last_enriched_at,
    aes.targets_seen,
    aes.enrichment_types_seen
FROM v_latest_account_intent_status vis
LEFT JOIN account_enrichment_summary aes ON aes.account_id = vis.account_id;

CREATE OR REPLACE VIEW v_demo_opportunity_summary AS
SELECT
    o.id AS opportunity_id,
    o.opportunity_name,
    o.stage,
    o.amount,
    o.created_at AS opportunity_created_at,
    a.id AS account_id,
    a.company_name,
    oi.influence_score,
    oi.influence_band,
    oi.confidence,
    oi.influenced,
    oi.matched_event_count,
    oi.unique_stakeholder_count,
    oi.website_signal_count,
    oi.notes
FROM opportunities o
JOIN accounts a ON a.id = o.account_id
LEFT JOIN opportunity_influence oi ON oi.opportunity_id = o.id;

CREATE OR REPLACE VIEW v_demo_writeback_summary AS
SELECT
    wr.writeback_run_id,
    wr.target_type,
    wr.status,
    wr.started_at,
    wr.completed_at,
    wr.duration_ms,
    wr.trigger_source,
    wr.result_metrics_json,
    wr.error_message,
    (
        SELECT COUNT(*)
        FROM writeback_records r
        WHERE r.writeback_run_id = wr.writeback_run_id
    ) AS record_count
FROM writeback_runs wr
ORDER BY wr.started_at DESC;
