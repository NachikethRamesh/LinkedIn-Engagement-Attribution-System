from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.writeback.run_writeback import WritebackService, parse_target_type
from app.writeback.selector import WritebackSelector


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _check_docs() -> None:
    required_docs = [
        ROOT / "docs" / "writeback.md",
        ROOT / "docs" / "business_tool_framing.md",
        ROOT / "docs" / "architecture_summary.md",
        ROOT / "docs" / "unresolved_account_research_flow.md",
        ROOT / "README.md",
    ]
    for path in required_docs:
        _assert(path.exists(), f"missing required doc: {path}")
        text = path.read_text(encoding="utf-8").lower()
        _assert("unresolved" in text, f"doc missing unresolved mention: {path}")
        _assert("exa" in text, f"doc missing exa mention: {path}")


def _check_artifact_folders() -> tuple[Path, Path]:
    outbound = ROOT / "data" / "outbound" / "exa_requests"
    inbound = ROOT / "data" / "inbound" / "exa_results"
    _assert(outbound.exists(), f"missing outbound artifact folder: {outbound}")
    _assert(inbound.exists(), f"missing inbound artifact folder: {inbound}")
    return outbound, inbound


def _check_selector() -> None:
    selector = WritebackSelector()
    selected = selector.select(
        target_type="exa",
        params={
            "selection_mode": "unresolved_account_candidates",
            "limit": 10,
            "min_contributing_events": 2,
            "min_strong_signals": 0,
            "min_recent_signals": 0,
            "recent_days": 60,
            "include_generic_candidates": True,
        },
    )
    print(f"selector returned {len(selected)} unresolved candidate(s)")


def _run_simulated_exa(limit: int) -> dict:
    service = WritebackService()
    return service.run(
        target_type=parse_target_type("exa"),
        params={
            "selection_mode": "unresolved_account_candidates",
            "limit": limit,
            "simulate_local": True,
            "dry_run": False,
            "skip_if_previously_successful": False,
            "min_contributing_events": 2,
            "min_strong_signals": 0,
            "min_recent_signals": 0,
            "recent_days": 60,
            "include_generic_candidates": True,
        },
        trigger_source="verify_unresolved_flow",
    )


def _check_summary_script() -> None:
    output = subprocess.check_output(
        [sys.executable, str(ROOT / "scripts" / "inspect_writeback_summary.py")],
        cwd=str(ROOT),
        text=True,
    )
    _assert(
        "unresolved-candidate exa runs" in output.lower(),
        "inspect_writeback_summary missing unresolved-candidate section",
    )


def _check_api_endpoints(api_base_url: str) -> None:
    for path in ("/ui/unresolved-candidates?limit=3", "/ui/exa-unresolved-results?limit=3"):
        url = f"{api_base_url.rstrip('/')}{path}"
        try:
            with urlopen(url, timeout=10) as resp:  # nosec B310 (local verification utility)
                body = resp.read().decode("utf-8")
                parsed = json.loads(body)
                _assert(isinstance(parsed, dict), f"non-json dict response from {url}")
        except (HTTPError, URLError) as exc:
            raise RuntimeError(f"failed endpoint {url}: {exc}") from exc


def _check_internal_ui_endpoints() -> None:
    from fastapi.testclient import TestClient

    from app.orchestration.api import app

    client = TestClient(app)
    for path in ("/ui/unresolved-candidates?limit=3", "/ui/exa-unresolved-results?limit=3"):
        response = client.get(path)
        _assert(response.status_code == 200, f"internal endpoint failed: {path} status={response.status_code}")
        payload = response.json()
        _assert(isinstance(payload, dict), f"internal endpoint returned non-dict json: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify unresolved-account-candidate -> Exa research flow surfaces.")
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--api-base-url", default=None, help="Optional, verifies /ui unresolved endpoints when provided.")
    args = parser.parse_args()

    _check_docs()
    outbound, inbound = _check_artifact_folders()
    _check_selector()

    before_outbound = {p.name for p in outbound.glob("*.json")}
    before_inbound = {p.name for p in inbound.glob("*.json")}
    run_result = _run_simulated_exa(limit=args.limit)
    print(
        f"simulated exa run: id={run_result.get('writeback_run_id')} status={run_result.get('status')} "
        f"selected={run_result.get('result_metrics_json', {}).get('selected_count')}"
    )

    after_outbound = {p.name for p in outbound.glob("*.json")}
    after_inbound = {p.name for p in inbound.glob("*.json")}
    _assert(len(after_outbound) >= len(before_outbound), "outbound artifact count regressed")
    _assert(len(after_inbound) >= len(before_inbound), "inbound artifact count regressed")
    _assert(len(after_outbound - before_outbound) > 0, "no new outbound exa request artifact written")
    _assert(len(after_inbound - before_inbound) > 0, "no new inbound exa result artifact written")

    _check_summary_script()
    _check_internal_ui_endpoints()

    if args.api_base_url:
        _check_api_endpoints(args.api_base_url)

    print("verify_unresolved_account_research_flow: PASS")


if __name__ == "__main__":
    main()
