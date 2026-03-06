from __future__ import annotations

import argparse

from app.identity_resolution.matcher import IdentityResolutionService


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve social_events to contacts/accounts.")
    parser.add_argument("--rebuild", action="store_true", help="Clear and rebuild all social event matches.")
    args = parser.parse_args()

    service = IdentityResolutionService()
    summary = service.run(rebuild=args.rebuild)

    print(f"events processed: {summary['events_processed']}")
    print(f"contact matches: {summary['contact_matches']}")
    print(f"account-only matches: {summary['account_only_matches']}")
    print(f"unresolved: {summary['unresolved']}")
    print(f"skipped aggregate imports: {summary['skipped_aggregate_imports']}")


if __name__ == "__main__":
    main()