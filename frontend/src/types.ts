export type RunStatus = "queued" | "running" | "success" | "failed";

export interface RunRecord {
  run_id: string;
  job_name: string;
  stage_name: string | null;
  status: RunStatus;
  started_at: string;
  completed_at: string | null;
  duration_ms: number | null;
  trigger_source: string;
  input_params_json: Record<string, unknown>;
  output_metrics_json: Record<string, unknown>;
  error_message: string | null;
}

export interface IngestionLatestResponse {
  source_name: string;
  import_mode: string;
  post: {
    post_id: number;
    post_url: string;
    author_name: string;
    topic: string;
    cta_url: string | null;
    created_at: string;
    imported_at: string | null;
    import_notes: string | null;
  } | null;
  event_counts: Record<string, number>;
  db_counts: {
    posts: number;
    social_events: number;
  };
}

export interface IdentitySummaryResponse {
  counts: {
    resolved_total?: number;
    contact_matches: number;
    account_only_matches: number;
    unresolved: number;
    skipped_aggregate: number;
    total_accounts_in_crm?: number;
    total_new_accounts_added_to_crm?: number;
  };
  samples: Array<{
    social_event_id: number;
    matched_contact_id: number | null;
    matched_account_id: number | null;
    match_type: string;
    match_confidence: number;
    match_reason: string;
    created_at: string;
  }>;
  matched_rows_total_count?: number;
  matched_rows_limit?: number;
  matched_rows_offset?: number;
  matched_rows?: Array<{
    social_event_id: number;
    account_name: string | null;
    contact_name: string | null;
    engagement_type: string;
    match_type: string;
    match_confidence: number;
    event_timestamp: string;
  }>;
}

export interface IntentSummaryResponse {
  window: string;
  top_accounts: Array<{
    account_id: number;
    company_name: string;
    score_window: string;
    score: number;
    confidence: number;
    score_reason: string;
    unique_stakeholder_count: number;
    strong_signal_count: number;
    website_signal_count: number;
    contributing_event_count: number;
    score_date: string;
    comment_analysis_count?: number;
    comment_analyses?: Array<{
      social_event_id: number;
      event_timestamp: string;
      comment_text: string;
      sentiment: string;
      intent: string;
      confidence: number;
      summary: string;
      source: string;
    }>;
  }>;
}

export interface OpportunitySummaryResponse {
  by_band: Record<string, number>;
  counts?: {
    path_a: number;
    path_b: number;
    total: number;
  };
  top_opportunities: Array<{
    opportunity_id: number;
    opportunity_name: string;
    company_name: string;
    influence_band: string;
    influence_score: number;
    confidence: number;
    funnel_path?: string;
    commercial_progression_flag?: string | null;
    opportunity_score?: number | null;
    action_priority?: string | null;
    recommended_next_action?: string | null;
    gemini_summary?: string | null;
    notes: string | null;
  }>;
  path_a_already_engaged?: Array<{
    opportunity_id: number;
    opportunity_name: string;
    company_name: string;
    influence_band: string;
    influence_score: number;
    confidence: number;
    funnel_path: string;
    commercial_progression_flag: string | null;
    opportunity_score: number | null;
    action_priority: string | null;
    recommended_next_action: string | null;
    gemini_summary: string | null;
    notes: string | null;
  }>;
  path_b_not_yet_engaged?: Array<{
    opportunity_id: number;
    opportunity_name: string;
    company_name: string;
    influence_band: string;
    influence_score: number;
    confidence: number;
    funnel_path: string;
    commercial_progression_flag: string | null;
    opportunity_score: number | null;
    action_priority: string | null;
    recommended_next_action: string | null;
    gemini_summary: string | null;
    notes: string | null;
  }>;
}

export interface UnresolvedCandidatesResponse {
  count: number;
  candidates: Array<{
    candidate_id: number;
    candidate_company_name_raw: string;
    candidate_company_name_normalized: string;
    supporting_signal_summary: Record<string, number>;
    strongest_signal_type: string | null;
    recent_signal_count: number;
    contributing_event_count: number;
    weak_match_reason: string;
    selection_reason: string;
    source_social_event_ids: number[];
  }>;
}

export interface ExaUnresolvedResultsResponse {
  count: number;
  total_count?: number;
  limit?: number;
  offset?: number;
  results: Array<{
    candidate_id: number;
    enrichment_type: string;
    likely_company_name: string | null;
    likely_domain: string | null;
    industry: string | null;
    confidence_notes: string | null;
    possible_match_hints: string[] | string | null;
    normalized_data_json: Record<string, unknown>;
    source_run_id: string | null;
    notes: string | null;
    received_at: string;
  }>;
}

export interface WritebackRunRecord {
  writeback_run_id: string;
  target_type: string;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  duration_ms: number | null;
  trigger_source: string;
  selection_params_json: Record<string, unknown>;
  result_metrics_json: Record<string, unknown>;
  error_message: string | null;
}

export interface ResetDataResponse {
  status: string;
  message: string;
  before_counts: Record<string, number>;
  after_counts: Record<string, number>;
  preserved_reference_tables: string[];
  reset_at_utc: string;
}
