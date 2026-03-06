from __future__ import annotations

import argparse

from app.intent_scoring.scorer import IntentScoringService


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute account intent scores from matched signals.")
    parser.add_argument("--rebuild", action="store_true", help="Clear and recompute all account intent scores.")
    args = parser.parse_args()

    service = IntentScoringService()
    summary = service.run(rebuild=args.rebuild)

    print(f"rows computed: {summary['rows_computed']}")
    print(f"rows written: {summary['rows_written']}")
    print(f"windows: {summary['windows']}")
    print(f"accounts: {summary['accounts']}")


if __name__ == "__main__":
    main()