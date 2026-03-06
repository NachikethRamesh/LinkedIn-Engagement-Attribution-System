from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection
from app.intent_scoring.scorer import IntentScoringService


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def fetchone_int(cur, query: str, params=None) -> int:
    cur.execute(query, params or ())
    return int(cur.fetchone()[0])


def snapshot() -> dict[str, object]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM account_intent_scores;")
            total_rows = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT score_window, COUNT(*), ROUND(AVG(score)::numeric, 2)
                FROM account_intent_scores
                GROUP BY score_window
                ORDER BY score_window
                """
            )
            window_counts = tuple(cur.fetchall())

            cur.execute(
                """
                SELECT COUNT(*)
                FROM account_intent_scores ais
                JOIN accounts a ON a.id = ais.account_id
                WHERE ais.score_date = (SELECT MAX(score_date) FROM account_intent_scores)
                  AND ais.score_window = 'rolling_30d'
                  AND ais.score > 0
                """
            )
            positive_accounts = int(cur.fetchone()[0])

            missing_explainability = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM account_intent_scores
                WHERE score_reason IS NULL
                   OR score_window IS NULL
                   OR confidence IS NULL
                   OR contributing_event_count IS NULL
                   OR score_breakdown_json IS NULL
                """,
            )

            score_without_signals = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM account_intent_scores ais
                WHERE ais.contributing_event_count = 0
                  AND ais.score > 0
                """,
            )

            duplicates = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM (
                    SELECT account_id, score_date, score_window, COUNT(*)
                    FROM account_intent_scores
                    GROUP BY account_id, score_date, score_window
                    HAVING COUNT(*) > 1
                ) dup
                """,
            )

            unresolved_with_account = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM social_event_matches sem
                JOIN social_events se ON se.id = sem.social_event_id
                WHERE sem.match_type = 'unresolved'
                  AND sem.matched_account_id IS NOT NULL
                """,
            )

            aggregate_account_matched = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM social_event_matches sem
                JOIN social_events se ON se.id = sem.social_event_id
                WHERE COALESCE((se.metadata_json->>'aggregated_import')::boolean, false) = true
                  AND sem.match_type NOT IN ('unresolved', 'skipped_aggregate_import')
                  AND sem.matched_account_id IS NOT NULL
                """,
            )

            aggregate_contrib_without_account_match = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM account_intent_scores ais
                WHERE COALESCE((ais.score_breakdown_json->>'aggregate_signal_count')::int, 0) > 0
                  AND ais.account_id NOT IN (
                      SELECT DISTINCT sem.matched_account_id
                      FROM social_event_matches sem
                      JOIN social_events se ON se.id = sem.social_event_id
                      WHERE sem.matched_account_id IS NOT NULL
                        AND COALESCE((se.metadata_json->>'aggregated_import')::boolean, false) = true
                  )
                """,
            )

            website_bonus_without_website_events = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM account_intent_scores ais
                WHERE COALESCE((ais.score_breakdown_json->>'website_bonus')::numeric, 0) > 0
                  AND ais.account_id NOT IN (
                      SELECT DISTINCT we.account_id
                      FROM website_events we
                      WHERE we.account_id IS NOT NULL
                  )
                """,
            )

            min_confidence = fetchone_int(
                cur,
                "SELECT COALESCE(MIN((confidence * 100)::int), 0) FROM account_intent_scores;",
            )
            max_confidence = fetchone_int(
                cur,
                "SELECT COALESCE(MAX((confidence * 100)::int), 0) FROM account_intent_scores;",
            )

            cur.execute(
                """
                WITH ranked AS (
                    SELECT account_id, score, contributing_event_count,
                           ROW_NUMBER() OVER (ORDER BY score DESC) AS high_rank,
                           ROW_NUMBER() OVER (ORDER BY score ASC) AS low_rank
                    FROM account_intent_scores
                    WHERE score_window = 'rolling_30d'
                )
                SELECT
                    (SELECT AVG(score) FROM ranked WHERE high_rank <= 5) AS avg_top5,
                    (SELECT AVG(score) FROM ranked WHERE low_rank <= 5) AS avg_bottom5,
                    (SELECT AVG(contributing_event_count) FROM ranked WHERE high_rank <= 5) AS avg_top5_events,
                    (SELECT AVG(contributing_event_count) FROM ranked WHERE low_rank <= 5) AS avg_bottom5_events
                """
            )
            comparative = cur.fetchone()

    return {
        "total_rows": total_rows,
        "window_counts": window_counts,
        "positive_accounts": positive_accounts,
        "missing_explainability": missing_explainability,
        "score_without_signals": score_without_signals,
        "duplicates": duplicates,
        "unresolved_with_account": unresolved_with_account,
        "aggregate_account_matched": aggregate_account_matched,
        "aggregate_contrib_without_account_match": aggregate_contrib_without_account_match,
        "website_bonus_without_website_events": website_bonus_without_website_events,
        "min_confidence_pct": min_confidence,
        "max_confidence_pct": max_confidence,
        "comparative": comparative,
    }


def unresolved_fixture_check(service: IntentScoringService) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM accounts ORDER BY id LIMIT 1;")
            row = cur.fetchone()
            assert_true(row is not None, "Expected at least one account for unresolved fixture check")
            account_id = int(row[0])

            cur.execute(
                """
                SELECT COALESCE(MAX(score), 0)
                FROM account_intent_scores
                WHERE account_id = %s
                  AND score_window = 'rolling_30d'
                  AND score_date = (SELECT MAX(score_date) FROM account_intent_scores)
                """,
                (account_id,),
            )
            baseline_score = float(cur.fetchone()[0] or 0.0)

            cur.execute("SELECT id FROM posts ORDER BY id LIMIT 1;")
            post_id = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO social_events (
                    post_id,
                    actor_name,
                    actor_linkedin_url,
                    actor_company_raw,
                    event_type,
                    event_timestamp,
                    metadata_json
                ) VALUES (%s, 'Fixture User', NULL, NULL, 'post_repost', NOW(), '{"source_name":"fixture","aggregated_import":false}')
                RETURNING id
                """,
                (post_id,),
            )
            social_event_id = int(cur.fetchone()[0])

            cur.execute(
                """
                INSERT INTO social_event_matches (
                    social_event_id,
                    matched_contact_id,
                    matched_account_id,
                    match_type,
                    match_confidence,
                    match_reason,
                    matched_on_fields_json,
                    created_at
                ) VALUES (%s, NULL, %s, 'unresolved', 0.00, 'Fixture unresolved test', '{}'::jsonb, NOW())
                """,
                (social_event_id, account_id),
            )
        conn.commit()

    try:
        service.run(rebuild=True)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(MAX(score), 0)
                    FROM account_intent_scores
                    WHERE account_id = %s
                      AND score_window = 'rolling_30d'
                      AND score_date = (SELECT MAX(score_date) FROM account_intent_scores)
                    """,
                    (account_id,),
                )
                after_score = float(cur.fetchone()[0] or 0.0)
        assert_true(
            round(after_score, 2) == round(baseline_score, 2),
            "Injected unresolved fixture event changed account score; unresolved events should be excluded",
        )
    finally:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM social_event_matches WHERE social_event_id = %s;", (social_event_id,))
                cur.execute("DELETE FROM social_events WHERE id = %s;", (social_event_id,))
            conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify deterministic intent scoring behavior.")
    parser.add_argument("--print-snapshot", action="store_true", help="Print scoring snapshot details.")
    args = parser.parse_args()

    service = IntentScoringService()

    service.run(rebuild=True)
    first = snapshot()

    service.run(rebuild=False)
    second = snapshot()

    service.run(rebuild=True)
    third = snapshot()

    assert_true(second["duplicates"] == 0, "Incremental run created duplicate score rows")
    assert_true(first == second, "Incremental rerun changed score snapshot unexpectedly")
    assert_true(first == third, "Rebuild snapshot drift detected between rebuild runs")
    assert_true(second["missing_explainability"] == 0, "All score rows must include reason and breakdown")
    assert_true(second["score_without_signals"] == 0, "Scores with no contributing events should not be >0")
    assert_true(second["unresolved_with_account"] == 0, "Unresolved matches should not carry matched_account_id")
    assert_true(
        second["aggregate_contrib_without_account_match"] == 0,
        "Aggregate signals contributed to accounts without account-matched aggregate events",
    )
    assert_true(
        second["website_bonus_without_website_events"] == 0,
        "Website bonus assigned to accounts with no account-linked website events",
    )
    assert_true(
        second["max_confidence_pct"] - second["min_confidence_pct"] >= 10,
        "Confidence values show insufficient spread; expected meaningful variance",
    )

    avg_top5, avg_bottom5, avg_top5_events, avg_bottom5_events = second["comparative"]
    if avg_top5 is not None and avg_bottom5 is not None:
        assert_true(float(avg_top5) >= float(avg_bottom5), "Top-scored accounts should outscore low-signal accounts")
    if avg_top5_events is not None and avg_bottom5_events is not None:
        assert_true(
            float(avg_top5_events) >= float(avg_bottom5_events),
            "Top-scored accounts should generally have >= contributing events than low-signal accounts",
        )

    unresolved_fixture_check(service)

    print("[PASS] scoring_runs")
    print("[PASS] rebuild_stability")
    print("[PASS] incremental_idempotency")
    print("[PASS] explainability")
    print("[PASS] unresolved_exclusion")
    print("[PASS] aggregate_handling")

    if args.print_snapshot:
        print("snapshot:")
        for key, value in second.items():
            print(f"- {key}: {value}")


if __name__ == "__main__":
    main()
