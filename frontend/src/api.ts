import type {
  ExaUnresolvedResultsResponse,
  IdentitySummaryResponse,
  IngestionLatestResponse,
  IntentSummaryResponse,
  OpportunitySummaryResponse,
  RunRecord,
  UnresolvedCandidatesResponse,
  ResetDataResponse,
  WritebackRunRecord
} from "./types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const method = init?.method ?? "GET";
  const headers: HeadersInit = {};
  if (method !== "GET" && method !== "HEAD") {
    headers["Content-Type"] = "application/json";
  }

  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers,
    ...init
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${method} ${path} -> ${response.status} ${response.statusText}: ${text}`);
  }
  return (await response.json()) as T;
}

function isNotFoundError(error: unknown): boolean {
  return error instanceof Error && error.message.includes(" 404 ");
}

export async function getHealth(): Promise<{ status: string; db: string; time_utc: string }> {
  return request("/health");
}

export async function triggerOrgUrlIngestion(payload: {
  post_url: string;
  simulation_mode: boolean;
  run_pipeline: boolean;
  rebuild_downstream: boolean;
  window_days: number;
}): Promise<RunRecord> {
  return request("/jobs/linkedin-ingestion/org-url", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export async function triggerIdentityResolution(rebuild = false): Promise<RunRecord> {
  return request("/jobs/identity-resolution", {
    method: "POST",
    body: JSON.stringify({ rebuild })
  });
}

export async function triggerIntentScoring(rebuild = false): Promise<RunRecord> {
  return request("/jobs/intent-scoring", {
    method: "POST",
    body: JSON.stringify({ rebuild })
  });
}

export async function triggerOpportunityAttribution(rebuild = false, window_days = 30): Promise<RunRecord> {
  return request("/jobs/opportunity-attribution", {
    method: "POST",
    body: JSON.stringify({ rebuild, window_days })
  });
}

export async function getJob(runId: string): Promise<RunRecord> {
  return request(`/jobs/${runId}`);
}

export async function getJobs(limit = 5): Promise<RunRecord[]> {
  return request(`/jobs?limit=${limit}`);
}

export async function getIngestionLatest(): Promise<IngestionLatestResponse> {
  return request("/ui/ingestion-latest");
}

export async function getIdentitySummary(limit = 10, offset = 0): Promise<IdentitySummaryResponse> {
  return request(`/ui/identity-summary?limit=${limit}&offset=${offset}`);
}

export async function getIntentSummary(window = "rolling_30d", limit = 100): Promise<IntentSummaryResponse> {
  return request(`/ui/intent-summary?window=${window}&limit=${limit}`);
}

export async function getOpportunitySummary(): Promise<OpportunitySummaryResponse> {
  return request("/ui/opportunity-summary");
}

export async function getUnresolvedCandidates(limit = 5): Promise<UnresolvedCandidatesResponse> {
  try {
    return await request(`/ui/unresolved-candidates?limit=${limit}`);
  } catch (error) {
    if (isNotFoundError(error)) {
      return { count: 0, candidates: [] };
    }
    throw error;
  }
}

export async function getExaUnresolvedResults(
  limit = 5,
  sourceRunId?: string,
  offset = 0
): Promise<ExaUnresolvedResultsResponse> {
  if (!sourceRunId) {
    return { count: 0, total_count: 0, limit, offset, results: [] };
  }
  try {
    return await request(
      `/ui/exa-unresolved-results?limit=${limit}&offset=${offset}&source_run_id=${encodeURIComponent(sourceRunId)}`
    );
  } catch (error) {
    if (isNotFoundError(error)) {
      return { count: 0, total_count: 0, limit, offset, results: [] };
    }
    throw error;
  }
}

export async function triggerExaUnresolvedResearch(payload?: {
  limit?: number;
  dry_run?: boolean;
  simulate_local?: boolean;
}): Promise<WritebackRunRecord> {
  return request("/writeback/run", {
    method: "POST",
    body: JSON.stringify({
      target_type: "exa",
      selection_mode: "unresolved_account_candidates",
      limit: payload?.limit ?? 10,
      dry_run: payload?.dry_run ?? false,
      simulate_local: payload?.simulate_local ?? false,
      skip_if_previously_successful: false
    })
  });
}

export async function resetUiData(): Promise<ResetDataResponse> {
  return request("/ui/reset-data", {
    method: "POST",
    body: JSON.stringify({})
  });
}

export async function pollRunUntilTerminal(
  runId: string,
  onUpdate: (record: RunRecord) => void,
  options?: { intervalMs?: number; maxAttempts?: number }
): Promise<RunRecord> {
  const intervalMs = options?.intervalMs ?? 1500;
  const maxAttempts = options?.maxAttempts ?? 120;

  let attempts = 0;
  while (attempts < maxAttempts) {
    const record = await getJob(runId);
    onUpdate(record);
    if (record.status === "success" || record.status === "failed") {
      return record;
    }
    attempts += 1;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(`Polling timed out for run_id=${runId}`);
}
