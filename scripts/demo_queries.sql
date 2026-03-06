-- Demo query pack for final walkthrough

-- 1) Latest pipeline runs
SELECT run_id, job_name, status, started_at, duration_ms, error_message
FROM v_demo_pipeline_summary
LIMIT 15;

-- 2) Pipeline status breakdown
SELECT status, COUNT(*) AS run_count
FROM pipeline_runs
GROUP BY status
ORDER BY run_count DESC, status;

-- 3) Top accounts by intent (rolling 30d)
SELECT
    company_name,
    score,
    confidence,
    score_reason,
    enrichment_result_count,
    last_enriched_at
FROM v_demo_account_summary
WHERE score_window = 'rolling_30d'
ORDER BY score DESC, confidence DESC
LIMIT 20;

-- 4) Influenced opportunities by band
SELECT
    influence_band,
    COUNT(*) AS opportunity_count,
    ROUND(AVG(influence_score)::numeric, 2) AS avg_influence_score
FROM v_demo_opportunity_summary
WHERE influence_score IS NOT NULL
GROUP BY influence_band
ORDER BY avg_influence_score DESC NULLS LAST, influence_band;

-- 5) Top influenced opportunities
SELECT
    opportunity_name,
    company_name,
    influence_band,
    influence_score,
    confidence,
    notes
FROM v_demo_opportunity_summary
WHERE influence_score IS NOT NULL
ORDER BY influence_score DESC, confidence DESC
LIMIT 20;

-- 6) Writeback runs by target and status
SELECT
    target_type,
    status,
    COUNT(*) AS run_count
FROM writeback_runs
GROUP BY target_type, status
ORDER BY target_type, status;

-- 7) Recent writeback failures
SELECT
    writeback_run_id,
    target_type,
    status,
    started_at,
    error_message
FROM v_demo_writeback_summary
WHERE status IN ('failed', 'partial_success')
ORDER BY started_at DESC
LIMIT 15;

-- 8) Sample successful/skipped payloads
SELECT
    wr.target_type,
    wr.entity_type,
    wr.entity_id,
    wr.status,
    wr.payload_json
FROM writeback_records wr
WHERE wr.status IN ('success', 'skipped')
ORDER BY wr.created_at DESC
LIMIT 10;

-- 9) Enrichment results by target
SELECT
    target_type,
    COUNT(*) AS enrichment_count
FROM enrichment_results
GROUP BY target_type
ORDER BY enrichment_count DESC, target_type;

-- 10) Recent enrichment samples
SELECT
    target_type,
    entity_type,
    entity_id,
    enrichment_type,
    received_at,
    normalized_data_json
FROM enrichment_results
ORDER BY received_at DESC
LIMIT 10;
