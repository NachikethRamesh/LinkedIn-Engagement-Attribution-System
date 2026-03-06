import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM account_intent_scores;")
            total = cur.fetchone()[0]
            print(f"account_intent_scores total rows: {total}")

            cur.execute(
                """
                SELECT score_window, COUNT(*)
                FROM account_intent_scores
                GROUP BY score_window
                ORDER BY score_window
                """
            )
            print("\nrows by window:")
            for window, count in cur.fetchall():
                print(f"- {window}: {count}")

            cur.execute(
                """
                SELECT a.company_name, ais.score_window, ais.score, ais.confidence, ais.score_reason,
                       ais.unique_stakeholder_count, ais.strong_signal_count, ais.website_signal_count
                FROM account_intent_scores ais
                JOIN accounts a ON a.id = ais.account_id
                WHERE ais.score_date = (SELECT MAX(score_date) FROM account_intent_scores)
                ORDER BY ais.score DESC, a.company_name
                LIMIT 20
                """
            )
            print("\ntop scored accounts:")
            for row in cur.fetchall():
                print(
                    f"- {row[0]} [{row[1]}] score={row[2]} confidence={row[3]} "
                    f"stakeholders={row[5]} strong={row[6]} website={row[7]} reason={row[4]}"
                )


if __name__ == "__main__":
    main()