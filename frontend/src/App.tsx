import { useEffect, useMemo, useState, type ReactNode } from "react";
import {
  getExaUnresolvedResults,
  getHealth,
  getIdentitySummary,
  getIngestionLatest,
  getIntentSummary,
  getJobs,
  getOpportunitySummary,
  getUnresolvedCandidates,
  pollRunUntilTerminal,
  resetUiData,
  triggerExaUnresolvedResearch,
  triggerIdentityResolution,
  triggerIntentScoring,
  triggerOpportunityAttribution,
  triggerOrgUrlIngestion
} from "./api";
import type {
  ExaUnresolvedResultsResponse,
  IdentitySummaryResponse,
  IngestionLatestResponse,
  IntentSummaryResponse,
  OpportunitySummaryResponse,
  RunRecord,
  UnresolvedCandidatesResponse,
  WritebackRunRecord
} from "./types";

const DEFAULT_POST_URL =
  "https://www.linkedin.com/posts/<REDACTED_POST>";

type Banner = { tone: "neutral" | "success" | "error"; text: string };

function formatDuration(duration: number | null): string {
  if (duration === null) return "-";
  return `${(duration / 1000).toFixed(2)}s`;
}

function firstHint(value: string[] | string | null): string {
  if (!value) return "-";
  if (Array.isArray(value)) return value.length > 0 ? value[0] : "-";
  return value;
}

function downloadIdentityResolutionCsv(identity: IdentitySummaryResponse | null) {
  const rows = identity?.matched_rows ?? [];
  const headers = [
    "social_event_id",
    "account_name",
    "contact_name",
    "engagement_type",
    "match_type",
    "match_confidence",
    "event_timestamp"
  ];
  const escapeCsv = (value: unknown) => {
    const text = value === null || value === undefined ? "" : String(value);
    const escaped = text.replace(/"/g, "\"\"");
    return `"${escaped}"`;
  };
  const content = [headers.join(","), ...rows.map((row) => headers.map((h) => escapeCsv((row as Record<string, unknown>)[h])).join(","))].join(
    "\n"
  );
  const blob = new Blob([content], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "identity resolution results.csv";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function App() {
  const EXA_RESULTS_LIMIT = 100;
  const IDENTITY_ROWS_LIMIT = 100;
  const [postUrl, setPostUrl] = useState(DEFAULT_POST_URL);
  const [windowDays] = useState(30);

  const [health, setHealth] = useState<{ status: string; db: string; time_utc: string } | null>(null);
  const [jobs, setJobs] = useState<RunRecord[]>([]);
  const [latestRun, setLatestRun] = useState<RunRecord | null>(null);
  const [runningJob, setRunningJob] = useState<string | null>(null);
  const [banner, setBanner] = useState<Banner>({ tone: "neutral", text: "Ready for ingestion." });

  const [ingestion, setIngestion] = useState<IngestionLatestResponse | null>(null);
  const [identity, setIdentity] = useState<IdentitySummaryResponse | null>(null);
  const [intent, setIntent] = useState<IntentSummaryResponse | null>(null);
  const [opportunity, setOpportunity] = useState<OpportunitySummaryResponse | null>(null);
  const [unresolvedCandidates, setUnresolvedCandidates] = useState<UnresolvedCandidatesResponse | null>(null);
  const [exaResults, setExaResults] = useState<ExaUnresolvedResultsResponse | null>(null);
  const [latestWritebackRun, setLatestWritebackRun] = useState<WritebackRunRecord | null>(null);
  const [intentModalAccount, setIntentModalAccount] = useState<IntentSummaryResponse["top_accounts"][number] | null>(null);

  const isBusy = runningJob !== null;
  const unresolvedCandidateCount = unresolvedCandidates?.count ?? 0;
  const canRunExaStep = unresolvedCandidateCount > 0;

  const pipelineState = useMemo(() => {
    const successByJob = new Set(jobs.filter((j) => j.status === "success").map((j) => j.job_name));
    return {
      ingested: Boolean(ingestion?.post),
      matched: successByJob.has("identity_resolution"),
      scored: successByJob.has("intent_scoring"),
      attributed: successByJob.has("opportunity_attribution")
    };
  }, [jobs, ingestion?.post]);

  async function refreshDashboardData(exaRunId?: string) {
    const effectiveExaRunId = exaRunId ?? latestWritebackRun?.writeback_run_id;
    const results = await Promise.allSettled([
      getHealth(),
      getJobs(5),
      getIngestionLatest(),
      getIdentitySummary(IDENTITY_ROWS_LIMIT, 0),
      getIntentSummary("rolling_30d", 100),
      getOpportunitySummary(),
      getUnresolvedCandidates(5),
      getExaUnresolvedResults(EXA_RESULTS_LIMIT, effectiveExaRunId, 0)
    ]);

    const errors: string[] = [];
    const [healthResp, jobsResp, ingestionResp, identityResp, intentResp, oppResp, unresolvedResp, exaResp] = results;

    if (healthResp.status === "fulfilled") setHealth(healthResp.value);
    else errors.push(`health: ${String(healthResp.reason)}`);

    if (jobsResp.status === "fulfilled") setJobs(jobsResp.value);
    else errors.push(`jobs: ${String(jobsResp.reason)}`);

    if (ingestionResp.status === "fulfilled") setIngestion(ingestionResp.value);
    else errors.push(`ingestion: ${String(ingestionResp.reason)}`);

    if (identityResp.status === "fulfilled") setIdentity(identityResp.value);
    else errors.push(`identity: ${String(identityResp.reason)}`);

    if (intentResp.status === "fulfilled") setIntent(intentResp.value);
    else errors.push(`intent: ${String(intentResp.reason)}`);

    if (oppResp.status === "fulfilled") setOpportunity(oppResp.value);
    else errors.push(`opportunity: ${String(oppResp.reason)}`);

    if (unresolvedResp.status === "fulfilled") setUnresolvedCandidates(unresolvedResp.value);
    else errors.push(`unresolved: ${String(unresolvedResp.reason)}`);

    if (exaResp.status === "fulfilled") setExaResults(exaResp.value);
    else errors.push(`exa: ${String(exaResp.reason)}`);

    if (errors.length > 0) {
      setBanner({
        tone: "error",
        text: `Partial dashboard refresh: ${errors[0]}`
      });
    }
  }

  useEffect(() => {
    refreshDashboardData().catch((error) => {
      setBanner({ tone: "error", text: `Failed to load dashboard data: ${String(error)}` });
    });
  }, []);

  useEffect(() => {
    refreshDashboardData().catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [latestWritebackRun?.writeback_run_id]);

  async function executeAndTrack(jobLabel: string, trigger: () => Promise<RunRecord>) {
    setRunningJob(jobLabel);
    setBanner({ tone: "neutral", text: `Running ${jobLabel}...` });
    try {
      const initial = await trigger();
      setLatestRun(initial);
      const final = await pollRunUntilTerminal(initial.run_id, (run) => setLatestRun(run));
      await refreshDashboardData();
      if (final.status === "success") {
        setBanner({ tone: "success", text: `${jobLabel} succeeded (run_id: ${final.run_id}).` });
      } else {
        setBanner({
          tone: "error",
          text: `${jobLabel} failed (run_id: ${final.run_id}): ${final.error_message ?? "unknown error"}`
        });
      }
      return final;
    } catch (error) {
      setBanner({ tone: "error", text: `${jobLabel} failed: ${String(error)}` });
      throw error;
    } finally {
      setRunningJob(null);
    }
  }

  async function runExaResearchForUnresolvedCandidates() {
    setRunningJob("Exa Unresolved Research");
    setBanner({ tone: "neutral", text: "Running Exa research for unresolved account candidates..." });
    try {
      const run = await triggerExaUnresolvedResearch({
        limit: 10,
        dry_run: false,
        simulate_local: false
      });
      setLatestWritebackRun(run);
      await refreshDashboardData(run.writeback_run_id);
      if (run.status === "success" || run.status === "partial_success") {
        setBanner({ tone: "success", text: `Exa research completed (writeback_run_id: ${run.writeback_run_id}).` });
      } else {
        setBanner({
          tone: "error",
          text: `Exa research failed (writeback_run_id: ${run.writeback_run_id}): ${run.error_message ?? "unknown error"}`
        });
      }
      return run;
    } catch (error) {
      setBanner({ tone: "error", text: `Exa unresolved research failed: ${String(error)}` });
      throw error;
    } finally {
      setRunningJob(null);
    }
  }

  async function runFullDownstream() {
    const identity = await executeAndTrack("Identity Resolution", () => triggerIdentityResolution(false));
    if (identity.status === "failed") return;

    if (canRunExaStep) {
      const exaRun = await runExaResearchForUnresolvedCandidates();
      if (!(exaRun.status === "success" || exaRun.status === "partial_success")) return;
    } else {
      setBanner({
        tone: "neutral",
        text: "Step 2 skipped: no unresolved candidates currently available for Exa research."
      });
    }

    const scoring = await executeAndTrack("Intent Scoring", () => triggerIntentScoring(false));
    if (scoring.status === "failed") return;

    await executeAndTrack("Opportunity Attribution", () => triggerOpportunityAttribution(false, windowDays));
  }

  async function handleReset() {
    setRunningJob("Reset Data");
    setBanner({ tone: "neutral", text: "Resetting dashboard data..." });
    try {
      await resetUiData();
      setLatestRun(null);
      setLatestWritebackRun(null);
      await refreshDashboardData();
      setBanner({ tone: "success", text: "Reset complete. Dashboard data cleared." });
    } catch (error) {
      setBanner({ tone: "error", text: `Reset failed: ${String(error)}` });
    } finally {
      setRunningJob(null);
    }
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">LOCAL OPERATOR CONSOLE</p>
          <h1>LinkedIn Attribution Engine</h1>
          <p className="subtle">Paste org-post URL, ingest, then run the downstream pipeline in sequence.</p>
        </div>
        <div className="health-chip">
          <span>{health?.status === "ok" ? "API+DB Healthy" : "Health Unknown"}</span>
          <small>{health?.time_utc ? new Date(health.time_utc).toLocaleString() : "No heartbeat yet"}</small>
        </div>
      </header>

      <div className={`banner banner-${banner.tone}`}>{banner.text}</div>

      <section className="pipeline-state">
        <div className="pipeline-state-left">
          <StatePill label="Ingested" active={pipelineState.ingested} />
          <StatePill label="Matched" active={pipelineState.matched} />
          <StatePill label="Scored" active={pipelineState.scored} />
          <StatePill label="Attributed" active={pipelineState.attributed} />
        </div>
        <button className="stage-btn reset-top-btn" disabled={isBusy} onClick={handleReset}>
          Reset
        </button>
      </section>

      <main className="grid">
        <Card title="URL Input" accent="cyan" span="span-5">
          <label className="field-label">LinkedIn organization post URL</label>
          <input
            className="text-input"
            value={postUrl}
            onChange={(e) => setPostUrl(e.target.value)}
            placeholder="https://www.linkedin.com/posts/..."
          />

          <p className="subtle inline">Simulation mode is disabled. Org URL ingestion runs in real mode only.</p>

          <button
            className="primary-btn"
            disabled={isBusy || !postUrl.trim()}
            onClick={() =>
              executeAndTrack("Org URL Ingestion", () =>
                triggerOrgUrlIngestion({
                  post_url: postUrl.trim(),
                  simulation_mode: false,
                  run_pipeline: false,
                  rebuild_downstream: false,
                  window_days: windowDays
                })
              )
            }
          >
            {isBusy && runningJob === "Org URL Ingestion" ? "Ingesting..." : "Ingest Post URL"}
          </button>
        </Card>

        <Card title="Ingestion Results" accent="violet" span="span-7">
          {!ingestion?.post ? (
            <p className="empty">No org URL ingestion data yet.</p>
          ) : (
            <>
              <p className="meta-line">
                <strong>Normalized URL:</strong> {ingestion.post.post_url}
              </p>
              <p className="meta-line">
                <strong>Source:</strong> {ingestion.source_name} | <strong>Mode:</strong> {ingestion.import_mode}
              </p>
              <p className="meta-line">
                <strong>Author:</strong> {ingestion.post.author_name} | <strong>Created:</strong>{" "}
                {new Date(ingestion.post.created_at).toLocaleString()}
              </p>
              <p className="meta-line">
                <strong>Topic:</strong> {ingestion.post.topic}
              </p>
              <div className="stats-grid">
                <Stat label="Post Impressions" value={ingestion.event_counts.post_impression ?? 0} />
                <Stat label="Likes/Reactions" value={ingestion.event_counts.post_like ?? 0} />
                <Stat label="Comments" value={ingestion.event_counts.post_comment ?? 0} />
                <Stat label="Reposts" value={ingestion.event_counts.post_repost ?? 0} />
                <Stat label="Post Link Clicks" value={ingestion.event_counts.post_link_click ?? 0} />
              </div>
            </>
          )}
        </Card>

        <Card title="Next Stages" accent="orange" span="span-12">
          <p className="subtle inline">Run these steps in order after post ingestion.</p>
          <div className="pipeline-steps">
            <div className="pipeline-step">
              <span className="step-index">1</span>
              <button
                className="stage-btn step-btn"
                disabled={isBusy}
                onClick={() => executeAndTrack("Identity Resolution", () => triggerIdentityResolution(false))}
              >
                Run Identity Resolution
              </button>
            </div>
            <span className="step-connector" aria-hidden>
              →
            </span>
            <div className="pipeline-step">
              <span className="step-index">2</span>
              <button className="stage-btn step-btn" disabled={isBusy || !canRunExaStep} onClick={() => runExaResearchForUnresolvedCandidates()}>
                Run Exa Research for Unresolved Candidates
              </button>
            </div>
            <span className="step-connector" aria-hidden>
              →
            </span>
            <div className="pipeline-step">
              <span className="step-index">3</span>
              <button
                className="stage-btn step-btn"
                disabled={isBusy}
                onClick={() => executeAndTrack("Intent Scoring", () => triggerIntentScoring(false))}
              >
                Run Intent Scoring
              </button>
            </div>
            <span className="step-connector" aria-hidden>
              →
            </span>
            <div className="pipeline-step">
              <span className="step-index">4</span>
              <button
                className="stage-btn step-btn"
                disabled={isBusy}
                onClick={() =>
                  executeAndTrack("Opportunity Attribution", () => triggerOpportunityAttribution(false, windowDays))
                }
              >
                Run Opportunity Attribution
              </button>
            </div>
          </div>
          <p className="subtle inline">
            Step 2 (Exa) is {canRunExaStep ? "available" : "skippable"} ({unresolvedCandidateCount} unresolved candidate
            {unresolvedCandidateCount === 1 ? "" : "s"}).
          </p>
          <button className="stage-btn highlight full-width" disabled={isBusy} onClick={() => runFullDownstream()}>
            Run Full Downstream Pipeline
          </button>
        </Card>

        <Card title="1. Identity Resolution Summary" accent="teal" span="span-6">
          <p className="subtle inline">Step 1 output: deterministic contact/account matching results.</p>
          <div className="stats-grid compact">
            <Stat
              label="Matched Accounts"
              value={(identity?.counts.contact_matches ?? 0) + (identity?.counts.account_only_matches ?? 0)}
            />
            <Stat label="Unresolved" value={identity?.counts.unresolved ?? 0} />
            <Stat label="Total Accounts in CRM" value={identity?.counts.total_accounts_in_crm ?? 0} />
            <Stat
              label="Total New Accounts Added to CRM"
              value={identity?.counts.total_new_accounts_added_to_crm ?? 0}
            />
          </div>
          <div className="identity-table-toolbar">
            <p className="subtle inline">Matched accounts + contacts + engagement type.</p>
            <button
              className="stage-btn"
              disabled={!identity?.matched_rows || identity.matched_rows.length === 0}
              onClick={() => downloadIdentityResolutionCsv(identity)}
            >
              Export CSV
            </button>
          </div>
          <div className="table-scroll-wrap table-scroll-5rows">
            <table className="mini-table">
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Contact</th>
                  <th>Engagement</th>
                  <th>Event ID</th>
                </tr>
              </thead>
              <tbody>
                {identity?.matched_rows?.map((row) => (
                  <tr key={row.social_event_id}>
                    <td>{row.account_name ?? "-"}</td>
                    <td>{row.contact_name ?? "-"}</td>
                    <td>{row.engagement_type}</td>
                    <td>#{row.social_event_id}</td>
                  </tr>
                )) ?? null}
              </tbody>
            </table>
          </div>
        </Card>

        <Card title="2. Exa Research Summary" accent="indigo" span="span-6">
          <p className="subtle inline">Step 2 output: unresolved engaged contact research using Exa.</p>
          {latestWritebackRun ? (
            <p className="meta-line">
              <strong>Latest writeback:</strong> {latestWritebackRun.writeback_run_id} ({latestWritebackRun.status})
            </p>
          ) : null}
          <div className="table-scroll-wrap table-scroll-5rows">
            <table className="mini-table">
              <thead>
                <tr>
                  <th>Candidate</th>
                  <th>Likely Domain</th>
                  <th>Industry</th>
                  <th>Notes</th>
                </tr>
              </thead>
              <tbody>
                {(exaResults?.results ?? []).map((row) => (
                  <tr key={`${row.candidate_id}-${row.received_at}`}>
                    <td>{row.likely_company_name ?? `Candidate ${row.candidate_id}`}</td>
                    <td>{row.likely_domain ?? "-"}</td>
                    <td>{row.industry ?? "-"}</td>
                    <td>{row.confidence_notes ?? firstHint(row.possible_match_hints)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {(exaResults?.count ?? 0) === 0 ? <p className="empty">No Exa research results yet. Run Step 2 to populate this section.</p> : null}
        </Card>

        <Card title="3. Intent Scoring Summary" accent="green" span="span-6">
          <p className="subtle inline">Step 3 output: resolved account engagement intent scores.</p>
          <div className="table-scroll-wrap table-scroll-5rows">
            <table className="mini-table">
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Engagement</th>
                  <th>Score</th>
                  <th>AI Sentiment Analysis</th>
                </tr>
              </thead>
              <tbody>
                {intent?.top_accounts.map((row) => (
                  <tr key={row.account_id}>
                    <td>{row.company_name}</td>
                    <td>{`${row.strong_signal_count} strong / ${row.contributing_event_count} total`}</td>
                    <td>{row.score.toFixed(2)}</td>
                    <td>
                      {(row.comment_analysis_count ?? 0) > 0 ? (
                        <button className="stage-btn compact-btn" onClick={() => setIntentModalAccount(row)}>
                          View ({row.comment_analysis_count})
                        </button>
                      ) : (
                        <span className="subtle">-</span>
                      )}
                    </td>
                  </tr>
                )) ?? null}
              </tbody>
            </table>
          </div>
        </Card>

        <Card title="4. Opportunity Attribution Summary" accent="pink" span="span-6">
          <p className="subtle inline">Step 4 output: actionable split by funnel status.</p>
          <div className="stats-grid compact">
            <Stat label="Path A (Engaged)" value={opportunity?.counts?.path_a ?? 0} />
            <Stat label="Path B (Not Yet Engaged)" value={opportunity?.counts?.path_b ?? 0} />
            <Stat label="Total Resolved Accounts" value={opportunity?.counts?.total ?? 0} />
          </div>
          <p className="subtle inline">
            <strong>Path A — Already Engaged in Funnel</strong>
          </p>
          <div className="table-scroll-wrap table-scroll-5rows">
            <table className="mini-table">
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Progression</th>
                  <th>Next Action</th>
                </tr>
              </thead>
              <tbody>
                {(opportunity?.path_a_already_engaged ?? []).map((row) => (
                  <tr key={`a-${row.opportunity_id}`}>
                    <td>{row.company_name}</td>
                    <td>{row.commercial_progression_flag ?? "-"}</td>
                    <td>{row.recommended_next_action ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="subtle inline">
            <strong>Path B — Not Yet Engaged in Funnel</strong>
          </p>
          <div className="table-scroll-wrap table-scroll-5rows">
            <table className="mini-table">
              <thead>
                <tr>
                  <th>Account</th>
                  <th>Priority</th>
                  <th>Next Action</th>
                </tr>
              </thead>
              <tbody>
                {(opportunity?.path_b_not_yet_engaged ?? []).map((row) => (
                  <tr key={`b-${row.opportunity_id}`}>
                    <td>{row.company_name}</td>
                    <td>{row.action_priority ?? "-"}</td>
                    <td>{row.recommended_next_action ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>

        <Card title="Job Status / Run History" accent="slate" span="span-12">
          <p className="subtle inline">Execution/operational layer for ingestion and downstream pipeline runs.</p>
          {latestRun ? (
            <div className="latest-run">
              <p>
                <strong>Latest:</strong> {latestRun.job_name} ({latestRun.status})
              </p>
              <p>
                <strong>run_id:</strong> {latestRun.run_id}
              </p>
              <p>
                <strong>duration:</strong> {formatDuration(latestRun.duration_ms)}
              </p>
              {latestRun.error_message ? <p className="error-text">{latestRun.error_message}</p> : null}
            </div>
          ) : null}
          <table className="mini-table">
            <thead>
              <tr>
                <th>Run ID</th>
                <th>Job</th>
                <th>Status</th>
                <th>Duration</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((run) => (
                <tr key={run.run_id}>
                  <td className="mono">{run.run_id.slice(0, 8)}</td>
                  <td>{run.job_name}</td>
                  <td>{run.status}</td>
                  <td>{formatDuration(run.duration_ms)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      </main>

      {intentModalAccount ? (
        <div className="modal-backdrop" onClick={() => setIntentModalAccount(null)}>
          <div className="modal-panel" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>
                Gemini Comment Analysis: {intentModalAccount.company_name} ({intentModalAccount.comment_analysis_count ?? 0})
              </h3>
              <button className="stage-btn compact-btn" onClick={() => setIntentModalAccount(null)}>
                Close
              </button>
            </div>
            <div className="modal-body">
              {(intentModalAccount.comment_analyses ?? []).map((comment) => (
                <details key={comment.social_event_id} className="comment-item">
                  <summary>
                    #{comment.social_event_id} | {new Date(comment.event_timestamp).toLocaleString()} | Sentiment: {comment.sentiment} | Intent:{" "}
                    {comment.intent} ({comment.confidence.toFixed(2)})
                  </summary>
                  <div className="comment-content">
                    <p>
                      <strong>Original comment:</strong> {comment.comment_text || "-"}
                    </p>
                    <p>
                      <strong>Gemini summary:</strong> {comment.summary || "-"}
                    </p>
                  </div>
                </details>
              ))}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function Card(props: { title: string; accent: string; children: ReactNode; span?: string }) {
  return (
    <section className={`card card-${props.accent} ${props.span ?? "span-12"}`}>
      <h2>{props.title}</h2>
      {props.children}
    </section>
  );
}

function StatePill({ label, active }: { label: string; active: boolean }) {
  return <span className={`state-pill ${active ? "active" : ""}`}>{label}</span>;
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div className="stat">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

export default App;
