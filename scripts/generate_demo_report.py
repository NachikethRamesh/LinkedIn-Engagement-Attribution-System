from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a compact markdown demo report from current database state.")
    parser.add_argument("--output", default="docs/demo_report_latest.md", help="Markdown output file path")
    args = parser.parse_args()

    output_path = (ROOT / args.output).resolve() if not Path(args.output).is_absolute() else Path(args.output)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM social_events;")
            total_social_events = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM social_event_matches WHERE matched_account_id IS NOT NULL;")
            matched_events = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM accounts;")
            account_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM opportunities;")
            opportunity_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM enrichment_results;")
            enrichment_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT company_name, score, confidence, score_reason
                FROM v_demo_account_summary
                WHERE score_window = 'rolling_30d'
                ORDER BY score DESC, confidence DESC
                LIMIT 5
                """
            )
            top_accounts = cur.fetchall()

            cur.execute(
                """
                SELECT opportunity_name, company_name, influence_band, influence_score, notes
                FROM v_demo_opportunity_summary
                WHERE influence_score IS NOT NULL
                ORDER BY influence_score DESC
                LIMIT 5
                """
            )
            top_opportunities = cur.fetchall()

            cur.execute(
                """
                SELECT writeback_run_id, target_type, status, started_at, duration_ms
                FROM v_demo_writeback_summary
                LIMIT 5
                """
            )
            recent_writeback = cur.fetchall()

            cur.execute(
                """
                SELECT target_type, COUNT(*)
                FROM enrichment_results
                GROUP BY target_type
                ORDER BY COUNT(*) DESC, target_type
                """
            )
            enrichment_by_target = cur.fetchall()

    lines: list[str] = []
    lines.append("# Social Attribution Engine Demo Report")
    lines.append("")
    lines.append(f"Generated at: {datetime.now(UTC).isoformat()}")
    lines.append("")
    lines.append("## Snapshot")
    lines.append(f"- Social events: {total_social_events}")
    lines.append(f"- Matched social events: {matched_events}")
    lines.append(f"- Accounts: {account_count}")
    lines.append(f"- Opportunities: {opportunity_count}")
    lines.append(f"- Enrichment results: {enrichment_count}")
    lines.append("")
    lines.append("## Top Intent Accounts (rolling_30d)")
    for company_name, score, confidence, reason in top_accounts:
        lines.append(f"- {company_name}: score={score}, confidence={confidence}, reason={reason}")
    if not top_accounts:
        lines.append("- none")
    lines.append("")
    lines.append("## Top Influenced Opportunities")
    for opp_name, company_name, band, score, notes in top_opportunities:
        lines.append(f"- {opp_name} ({company_name}): band={band}, score={score}, notes={notes}")
    if not top_opportunities:
        lines.append("- none")
    lines.append("")
    lines.append("## Recent Writeback Runs")
    for run_id, target, status, started_at, duration_ms in recent_writeback:
        lines.append(f"- {run_id}: target={target}, status={status}, started={started_at.isoformat()}, duration_ms={duration_ms}")
    if not recent_writeback:
        lines.append("- none")
    lines.append("")
    lines.append("## Enrichment Results By Target")
    for target, count in enrichment_by_target:
        lines.append(f"- {target}: {count}")
    if not enrichment_by_target:
        lines.append("- none")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Demo report written: {output_path}")


if __name__ == "__main__":
    main()
