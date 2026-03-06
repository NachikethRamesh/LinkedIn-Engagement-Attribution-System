from __future__ import annotations

import argparse
import json

from app.orchestration.job_runner import JobRunner, run_record_to_dict


def main() -> None:
    parser = argparse.ArgumentParser(description="Orchestration CLI for stage and full-pipeline jobs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_full = subparsers.add_parser("run-full", help="Run full pipeline in one invocation.")
    run_full.add_argument("--source", required=True, choices=["mock", "shield_csv", "sprout_csv", "generic_csv"])
    run_full.add_argument("--file", help="CSV file path for *_csv sources")
    run_full.add_argument("--posts", type=int, default=20)
    run_full.add_argument("--events", type=int, default=250)
    run_full.add_argument("--rebuild", action="store_true")
    run_full.add_argument("--window-days", type=int, default=30)

    run_stage = subparsers.add_parser("run-stage", help="Run a single stage job.")
    run_stage.add_argument(
        "stage",
        choices=[
            "linkedin_ingestion_csv",
            "linkedin_ingestion_mock",
            "linkedin_ingestion_org_url",
            "identity_resolution",
            "intent_scoring",
            "opportunity_attribution",
        ],
    )
    run_stage.add_argument("--source", choices=["shield", "sprout", "generic"])
    run_stage.add_argument("--file")
    run_stage.add_argument("--posts", type=int, default=20)
    run_stage.add_argument("--events", type=int, default=250)
    run_stage.add_argument("--rebuild", action="store_true")
    run_stage.add_argument("--window-days", type=int, default=30)
    run_stage.add_argument("--post-url")
    run_stage.add_argument("--simulation-mode", action="store_true")
    run_stage.add_argument("--resolved-id-override")
    run_stage.add_argument("--run-pipeline", action="store_true")
    run_stage.add_argument("--rebuild-downstream", action="store_true")

    list_runs = subparsers.add_parser("list-runs", help="List recent pipeline runs")
    list_runs.add_argument("--limit", type=int, default=20)
    list_runs.add_argument("--json", action="store_true", help="Emit full JSON records instead of concise lines")

    show_run = subparsers.add_parser("get-run", help="Fetch single run by run_id")
    show_run.add_argument("--run-id", required=True)

    args = parser.parse_args()
    runner = JobRunner()

    if args.command == "run-full":
        params = {
            "source": args.source,
            "file": args.file,
            "posts": args.posts,
            "events": args.events,
            "rebuild": args.rebuild,
            "window_days": args.window_days,
        }
        record = runner.run_job("full_pipeline", params=params, trigger_source="manual")
        print(json.dumps(run_record_to_dict(record), indent=2))
        return

    if args.command == "run-stage":
        if args.simulation_mode:
            raise SystemExit("--simulation-mode is disabled.")
        params = {
            "source": args.source,
            "file": args.file,
            "posts": args.posts,
            "events": args.events,
            "rebuild": args.rebuild,
            "window_days": args.window_days,
            "post_url": args.post_url,
            "simulation_mode": args.simulation_mode,
            "resolved_id_override": args.resolved_id_override,
            "run_pipeline": args.run_pipeline,
            "rebuild_downstream": args.rebuild_downstream,
        }
        if args.stage == "linkedin_ingestion_csv" and not args.file:
            raise SystemExit("--file is required for linkedin_ingestion_csv")
        if args.stage == "linkedin_ingestion_csv" and not args.source:
            raise SystemExit("--source is required for linkedin_ingestion_csv (shield|sprout|generic)")
        if args.stage == "linkedin_ingestion_org_url" and not args.post_url:
            raise SystemExit("--post-url is required for linkedin_ingestion_org_url")

        record = runner.run_job(args.stage, params=params, trigger_source="manual")
        print(json.dumps(run_record_to_dict(record), indent=2))
        return

    if args.command == "list-runs":
        runs = [run_record_to_dict(r) for r in runner.list_runs(limit=args.limit)]
        if args.json:
            print(json.dumps(runs, indent=2))
            return

        for run in runs:
            metrics = run.get("output_metrics_json") or {}
            if run["job_name"] == "full_pipeline":
                stage_keys = list((metrics.get("stages") or {}).keys())
                metric_summary = f"stages={stage_keys}"
            else:
                metric_summary = f"metrics_keys={list(metrics.keys())[:4]}"

            if run.get("error_message"):
                error_summary = run["error_message"].splitlines()[0]
            else:
                error_summary = ""

            print(
                f"{run['started_at']} | run_id={run['run_id']} | job={run['job_name']} | "
                f"status={run['status']} | trigger={run['trigger_source']} | duration_ms={run['duration_ms']} | "
                f"{metric_summary}" + (f" | error={error_summary}" if error_summary else "")
            )
        return

    if args.command == "get-run":
        run = runner.get_run(args.run_id)
        if run is None:
            raise SystemExit(f"No run found for run_id={args.run_id}")
        print(json.dumps(run_record_to_dict(run), indent=2))


if __name__ == "__main__":
    main()
