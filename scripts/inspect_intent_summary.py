import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT account_id || ':' || score_window) FROM account_intent_scores;")
            scored_account_windows = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM account_intent_scores;")
            total_rows = cur.fetchone()[0]

            cur.execute(
                """
                SELECT score_window, COUNT(*)
                FROM account_intent_scores
                GROUP BY score_window
                ORDER BY score_window
                """
            )
            by_window = cur.fetchall()

            cur.execute(
                """
                SELECT a.company_name, ais.score_window, ais.score, ais.confidence
                FROM account_intent_scores ais
                JOIN accounts a ON a.id = ais.account_id
                WHERE ais.score_date = (SELECT MAX(score_date) FROM account_intent_scores)
                ORDER BY ais.score DESC, a.company_name
                LIMIT 10
                """
            )
            top10 = cur.fetchall()

            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN score < 10 THEN 1 ELSE 0 END) AS lt10,
                    SUM(CASE WHEN score >= 10 AND score < 25 THEN 1 ELSE 0 END) AS b10_25,
                    SUM(CASE WHEN score >= 25 AND score < 50 THEN 1 ELSE 0 END) AS b25_50,
                    SUM(CASE WHEN score >= 50 THEN 1 ELSE 0 END) AS gte50
                FROM account_intent_scores
                """
            )
            buckets = cur.fetchone()

            cur.execute(
                """
                SELECT score_window, ROUND(AVG(confidence)::numeric, 3)
                FROM account_intent_scores
                GROUP BY score_window
                ORDER BY score_window
                """
            )
            avg_confidence = cur.fetchall()

            cur.execute(
                """
                SELECT COUNT(*)
                FROM account_intent_scores
                WHERE score_reason IS NULL
                   OR score_breakdown_json IS NULL
                   OR score_window IS NULL
                   OR confidence IS NULL
                """
            )
            missing_explainability = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT account_id, score_date, score_window, COUNT(*)
                    FROM account_intent_scores
                    GROUP BY account_id, score_date, score_window
                    HAVING COUNT(*) > 1
                ) dup
                """
            )
            duplicates = cur.fetchone()[0]

    print(f"scored account-window pairs: {scored_account_windows}")
    print(f"total score rows: {total_rows}")
    print("rows by window:")
    for window, count in by_window:
        print(f"- {window}: {count}")

    print("top 10 accounts by score:")
    for company_name, window, score, confidence in top10:
        print(f"- {company_name} [{window}] score={score} confidence={confidence}")

    print("score distribution buckets:")
    print(f"- <10: {buckets[0]}")
    print(f"- 10-24.99: {buckets[1]}")
    print(f"- 25-49.99: {buckets[2]}")
    print(f"- >=50: {buckets[3]}")

    print("average confidence by window:")
    for window, avg in avg_confidence:
        print(f"- {window}: {avg}")

    print(f"rows missing explainability fields: {missing_explainability}")
    print(f"duplicate account/date/window rows: {duplicates}")


if __name__ == "__main__":
    main()