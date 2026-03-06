import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM opportunity_influence;")
            total = cur.fetchone()[0]
            print(f"opportunity_influence total rows: {total}")

            cur.execute(
                """
                SELECT influence_band, COUNT(*)
                FROM opportunity_influence
                GROUP BY influence_band
                ORDER BY COUNT(*) DESC, influence_band
                """
            )
            print("\ncounts by influence band:")
            for band, count in cur.fetchall():
                print(f"- {band}: {count}")

            cur.execute(
                """
                SELECT
                    o.opportunity_name,
                    a.company_name,
                    oi.influence_score,
                    oi.influence_band,
                    oi.confidence,
                    oi.matched_event_count,
                    oi.unique_stakeholder_count,
                    oi.website_signal_count,
                    oi.notes
                FROM opportunity_influence oi
                JOIN opportunities o ON o.id = oi.opportunity_id
                JOIN accounts a ON a.id = oi.account_id
                ORDER BY oi.influence_score DESC, oi.confidence DESC
                LIMIT 20
                """
            )
            print("\ntop influenced opportunities:")
            for row in cur.fetchall():
                print(
                    f"- {row[0]} ({row[1]}): score={row[2]} band={row[3]} conf={row[4]} "
                    f"events={row[5]} stakeholders={row[6]} website={row[7]} notes={row[8]}"
                )


if __name__ == "__main__":
    main()