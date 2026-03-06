from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
N8N_DIR = ROOT / "n8n"
DOCS_DIR = ROOT / "docs"

WORKFLOWS = {
    "full_refresh_pipeline.json": {
        "required_node_names": [
            "Manual Trigger",
            "Set Config",
            "GET /health",
            "POST /jobs/full-pipeline",
            "Poll Until Terminal",
            "Run Success?",
            "Metric Sanity Check",
            "Notify Success",
            "Notify Warning",
            "Notify Failure",
        ],
        "required_strings": [
            "/jobs/full-pipeline",
            "/jobs/",
            "max_poll_attempts",
            "final_status",
            "output_metrics_json",
            "stages",
        ],
    },
    "incremental_pipeline.json": {
        "required_node_names": [
            "Manual Trigger",
            "Set Config",
            "GET /health",
            "POST /jobs/full-pipeline",
            "Poll Until Terminal",
            "Run Success?",
            "Metric Sanity Check",
            "Notify Success",
            "Notify Warning",
            "Notify Failure",
        ],
        "required_strings": [
            "/jobs/full-pipeline",
            "/jobs/",
            "max_poll_attempts",
            "final_status",
            "output_metrics_json",
            "stages",
        ],
    },
    "stage_run_or_retry.json": {
        "required_node_names": [
            "Manual Trigger",
            "Set Stage Inputs",
            "Build Stage Request",
            "Run Stage + Poll + Retry",
            "Stage Success?",
            "Notify Stage Success",
            "Notify Stage Failure",
        ],
        "required_strings": [
            "/jobs/linkedin-ingestion/csv",
            "/jobs/linkedin-ingestion/mock",
            "/jobs/identity-resolution",
            "/jobs/intent-scoring",
            "/jobs/opportunity-attribution",
            "/jobs/",
            "max_run_retries",
        ],
    },
    "failure_notification.json": {
        "required_node_names": [
            "Webhook Trigger",
            "Normalize Input",
            "Is Failure Or Warning?",
            "Send Notification",
        ],
        "required_strings": [
            "pipeline-failure",
            "status",
            "run_id",
            "error_message",
            "metric_summary",
        ],
    },
}


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def check_workflow_structure(filename: str, data: dict) -> None:
    assert_true(isinstance(data, dict), f"{filename}: top-level JSON must be object")
    for key in ("name", "nodes", "connections"):
        assert_true(key in data, f"{filename}: missing top-level key '{key}'")
    assert_true(isinstance(data["nodes"], list) and data["nodes"], f"{filename}: nodes must be non-empty list")
    assert_true(isinstance(data["connections"], dict), f"{filename}: connections must be object")

    seen_names = set()
    for idx, node in enumerate(data["nodes"], start=1):
        assert_true(isinstance(node, dict), f"{filename}: node #{idx} must be object")
        for field in ("id", "name", "type", "typeVersion", "position", "parameters"):
            assert_true(field in node, f"{filename}: node #{idx} missing '{field}'")
        assert_true(isinstance(node["position"], list) and len(node["position"]) == 2, f"{filename}: node #{idx} invalid position")
        assert_true(isinstance(node["name"], str) and node["name"], f"{filename}: node #{idx} invalid name")
        seen_names.add(node["name"])

    expected = WORKFLOWS[filename]["required_node_names"]
    for name in expected:
        assert_true(name in seen_names, f"{filename}: missing expected node '{name}'")

    serialized = json.dumps(data)
    for snippet in WORKFLOWS[filename]["required_strings"]:
        assert_true(snippet in serialized, f"{filename}: missing expected string '{snippet}'")


def main() -> None:
    assert_true(N8N_DIR.exists(), "n8n directory missing")

    for filename in WORKFLOWS:
        path = N8N_DIR / filename
        assert_true(path.exists(), f"missing workflow file: {path}")
        data = load_json(path)
        check_workflow_structure(filename, data)
        print(f"[PASS] {filename}")

    n8n_docs = DOCS_DIR / "n8n_workflows.md"
    sample_docs = N8N_DIR / "sample_requests.md"
    assert_true(n8n_docs.exists(), f"missing docs file: {n8n_docs}")
    assert_true(sample_docs.exists(), f"missing docs file: {sample_docs}")
    print("[PASS] docs_present")

    n8n_doc_text = n8n_docs.read_text(encoding="utf-8")
    assert_true("poll_interval_seconds" in n8n_doc_text, "docs/n8n_workflows.md missing poll config guidance")
    assert_true("max_poll_attempts" in n8n_doc_text, "docs/n8n_workflows.md missing max poll guidance")
    assert_true("/jobs/{run_id}" in n8n_doc_text, "docs/n8n_workflows.md missing polling endpoint guidance")
    assert_true("warning" in n8n_doc_text.lower(), "docs/n8n_workflows.md missing warning-path guidance")
    print("[PASS] docs_content")


if __name__ == "__main__":
    main()
