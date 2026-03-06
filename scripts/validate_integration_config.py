from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.integrations_config import (  # noqa: E402
    collect_env_presence,
    get_linkedin_credentials,
    get_writeback_endpoint,
    summarize_integration_requirements,
)


def _print_status(label: str, value: bool) -> None:
    print(f"- {label}: {'set' if value else 'missing'}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate integration credential/config presence for local .env (non-secret output)."
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    parser.add_argument(
        "--check-writeback-target",
        choices=["crm", "clay", "exa", "webhook_generic"],
        help="Optional target-specific warning check.",
    )
    parser.add_argument("--simulate-local", action="store_true", help="Assume simulated local mode for target checks.")
    parser.add_argument(
        "--check-linkedin-real-mode",
        action="store_true",
        help="Warn when LinkedIn org-url ingestion would run with simulation_mode=false and missing credentials.",
    )
    args = parser.parse_args()

    presence = collect_env_presence()
    warnings: list[str] = []

    if args.check_writeback_target:
        endpoint = get_writeback_endpoint(args.check_writeback_target, explicit_endpoint=None)
        warnings.extend(
            summarize_integration_requirements(
                target_type=args.check_writeback_target,
                endpoint_url=endpoint,
                simulate_local=args.simulate_local,
            )
        )

    if args.check_linkedin_real_mode:
        creds = get_linkedin_credentials()
        missing = []
        if not creds.organization_id:
            missing.append("LINKEDIN_ORGANIZATION_ID")
        if not creds.access_token:
            missing.append("LINKEDIN_ACCESS_TOKEN")
        if missing:
            warnings.append(
                "LinkedIn real org API mode may fail without: " + ", ".join(missing) + ". "
                "Simulation mode does not require these values."
            )

    payload = {"presence": presence, "warnings": warnings}

    if args.json:
        print(json.dumps(payload, indent=2))
        return

    print("integration env presence:")
    for section, values in presence.items():
        print(f"\n[{section}]")
        if isinstance(values, dict):
            for key, value in values.items():
                _print_status(key, bool(value))

    print("\nwarnings:")
    if not warnings:
        print("- none")
    else:
        for warning in warnings:
            print(f"- {warning}")


if __name__ == "__main__":
    main()
