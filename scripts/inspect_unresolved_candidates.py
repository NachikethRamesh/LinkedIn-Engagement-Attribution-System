from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.writeback.selector import WritebackSelector


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect unresolved account candidates for Exa research selection.")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--min-contributing-events", type=int, default=3)
    parser.add_argument("--min-strong-signals", type=int, default=1)
    parser.add_argument("--min-recent-signals", type=int, default=1)
    parser.add_argument("--recent-days", type=int, default=30)
    parser.add_argument("--weak-match-confidence-threshold", type=float, default=0.7)
    parser.add_argument("--include-generic-candidates", action="store_true")
    args = parser.parse_args()

    selector = WritebackSelector()
    selected = selector.select(
        target_type="exa",
        params={
            "selection_mode": "unresolved_account_candidates",
            "limit": args.limit,
            "min_contributing_events": args.min_contributing_events,
            "min_strong_signals": args.min_strong_signals,
            "min_recent_signals": args.min_recent_signals,
            "recent_days": args.recent_days,
            "weak_match_confidence_threshold": args.weak_match_confidence_threshold,
            "include_generic_candidates": args.include_generic_candidates,
        },
    )

    print(f"unresolved candidates selected: {len(selected)}")
    for candidate in selected:
        data = candidate.data
        print(
            f"- candidate_id={candidate.entity_id} raw='{data.get('candidate_company_name_raw')}' "
            f"normalized='{data.get('candidate_company_name_normalized')}' "
            f"events={data.get('contributing_event_count')} recent={data.get('recent_signal_count')} "
            f"strongest={data.get('strongest_signal_type')}"
        )
        print(f"  reason={candidate.selection_reason}")
        print(f"  signals={json.dumps(data.get('supporting_signal_summary', {}), sort_keys=True)}")


if __name__ == "__main__":
    main()
