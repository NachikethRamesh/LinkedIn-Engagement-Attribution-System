from __future__ import annotations

import argparse

from app.opportunity_attribution.attributor import OpportunityAttributionService
from app.opportunity_attribution.config import DEFAULT_WINDOW_DAYS


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic opportunity influence attribution.")
    parser.add_argument("--rebuild", action="store_true", help="Clear and recompute all opportunity influence rows.")
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS, help="Lookback window in days (30 or 60).")
    args = parser.parse_args()

    service = OpportunityAttributionService()
    summary = service.run(rebuild=args.rebuild, window_days=args.window_days)

    print(f"opportunities processed: {summary['opportunities_processed']}")
    print(f"rows written: {summary['rows_written']}")
    print(f"window days: {summary['window_days']}")


if __name__ == "__main__":
    main()