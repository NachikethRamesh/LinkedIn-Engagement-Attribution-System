from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection
from app.opportunity_attribution.attributor import OpportunityAttributionService


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def snapshot() -> dict[str, object]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM opportunities;")
            total_opps = int(cur.fetchone()[0])

            cur.execute("SELECT COUNT(*) FROM opportunity_influence;")
            total_rows = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT influence_band, COUNT(*)
                FROM opportunity_influence
                GROUP BY influence_band
                ORDER BY influence_band
                """
            )
            bands = tuple(cur.fetchall())

            cur.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT opportunity_id, COUNT(*)
                    FROM opportunity_influence
                    GROUP BY opportunity_id
                    HAVING COUNT(*) > 1
                ) dup
                """
            )
            duplicates = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(*)
                FROM opportunity_influence
                WHERE score_breakdown_json IS NULL
                   OR notes IS NULL
                   OR influence_band IS NULL
                """
            )
            missing_explainability = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(*)
                FROM opportunity_influence oi
                JOIN opportunities o ON o.id = oi.opportunity_id
                WHERE oi.last_social_touch_at IS NOT NULL
                  AND oi.last_social_touch_at > o.created_at
                """
            )
            post_opp_social_used = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(*)
                FROM opportunity_influence oi
                WHERE oi.confidence < 0 OR oi.confidence > 1
                """
            )
            bad_confidence = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT COUNT(*)
                FROM opportunity_influence oi
                WHERE oi.matched_event_count > 0
                  AND oi.opportunity_id NOT IN (
                      SELECT DISTINCT o.id
                      FROM opportunities o
                      JOIN social_event_matches sem ON sem.matched_account_id = o.account_id
                      JOIN social_events se ON se.id = sem.social_event_id
                      WHERE sem.match_type NOT IN ('unresolved', 'skipped_aggregate_import')
                        AND se.event_timestamp <= o.created_at
                        AND se.event_timestamp >= (o.created_at - (oi.influence_window_days || ' days')::interval)
                  )
                """
            )
            unresolved_leak = int(cur.fetchone()[0])

            cur.execute(
                """
                WITH scored AS (
                    SELECT influence_score, matched_event_count, website_signal_count
                    FROM opportunity_influence
                )
                SELECT
                    (SELECT AVG(influence_score) FROM scored WHERE matched_event_count >= 3 OR website_signal_count >= 2) AS high_signal_avg,
                    (SELECT AVG(influence_score) FROM scored WHERE matched_event_count <= 1 AND website_signal_count = 0) AS low_signal_avg
                """
            )
            high_vs_low = cur.fetchone()

    return {
        "total_opps": total_opps,
        "total_rows": total_rows,
        "bands": bands,
        "duplicates": duplicates,
        "missing_explainability": missing_explainability,
        "post_opp_social_used": post_opp_social_used,
        "bad_confidence": bad_confidence,
        "unresolved_leak": unresolved_leak,
        "high_vs_low": high_vs_low,
    }


def fixture_ranking_check(service: OpportunityAttributionService) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO accounts (company_name, domain, target_tier, created_at)
                VALUES ('Fixture Influenced Co', 'fixture-influenced.test', 'Tier 1', NOW()),
                       ('Fixture Quiet Co', 'fixture-quiet.test', 'Tier 3', NOW())
                RETURNING id
                """
            )
            account_ids = [int(r[0]) for r in cur.fetchall()]

            cur.execute(
                """
                INSERT INTO opportunities (account_id, opportunity_name, stage, amount, created_at, closed_won_at)
                VALUES
                    (%s, 'Fixture Influenced Opp', 'Discovery', 50000, NOW(), NULL),
                    (%s, 'Fixture Quiet Opp', 'Discovery', 50000, NOW(), NULL)
                RETURNING id, account_id
                """,
                (account_ids[0], account_ids[1]),
            )
            opp_rows = cur.fetchall()
            opp_influenced_id = int(opp_rows[0][0])
            opp_quiet_id = int(opp_rows[1][0])

            cur.execute(
                """
                INSERT INTO posts (author_name, post_url, topic, cta_url, created_at)
                VALUES ('Fixture Author', 'https://www.linkedin.com/posts/<REDACTED_POST>', 'Fixture', NULL, NOW() - INTERVAL '12 days')
                RETURNING id
                """
            )
            post_id = int(cur.fetchone()[0])

            cur.execute(
                """
                INSERT INTO social_events (post_id, actor_name, actor_linkedin_url, actor_company_raw, event_type, event_timestamp, metadata_json)
                VALUES
                    (%s, 'Fixture One', 'https://www.linkedin.com/in/<REDACTED_PROFILE>', 'Fixture Influenced Co', 'post_comment', NOW() - INTERVAL '6 days', '{"aggregated_import":false,"source_name":"fixture"}'),
                    (%s, 'Fixture Two', 'https://www.linkedin.com/in/<REDACTED_PROFILE>', 'Fixture Influenced Co', 'post_repost', NOW() - INTERVAL '5 days', '{"aggregated_import":false,"source_name":"fixture"}'),
                    (%s, 'Fixture Three', 'https://www.linkedin.com/in/<REDACTED_PROFILE>', 'Fixture Influenced Co', 'post_link_click', NOW() - INTERVAL '4 days', '{"aggregated_import":false,"source_name":"fixture"}')
                RETURNING id
                """,
                (post_id, post_id, post_id),
            )
            event_ids = [int(r[0]) for r in cur.fetchall()]

            cur.execute(
                """
                INSERT INTO social_event_matches (
                    social_event_id, matched_contact_id, matched_account_id,
                    match_type, match_confidence, match_reason, matched_on_fields_json, created_at
                ) VALUES
                    (%s, NULL, %s, 'exact_account_name', 0.80, 'fixture', '{}'::jsonb, NOW()),
                    (%s, NULL, %s, 'normalized_account_name', 0.75, 'fixture', '{}'::jsonb, NOW()),
                    (%s, NULL, %s, 'exact_account_name', 0.80, 'fixture', '{}'::jsonb, NOW())
                """,
                (event_ids[0], account_ids[0], event_ids[1], account_ids[0], event_ids[2], account_ids[0]),
            )

            cur.execute(
                """
                INSERT INTO website_events (account_id, anonymous_visitor_id, page_url, utm_source, utm_campaign, event_timestamp)
                VALUES (%s, NULL, 'https://catalystlabs.io/demo', 'linkedin', 'fixture', NOW() - INTERVAL '3 days')
                """,
                (account_ids[0],),
            )

            cur.execute(
                """
                INSERT INTO account_intent_scores (
                    account_id, score_date, score, score_window, score_reason, confidence,
                    unique_stakeholder_count, strong_signal_count, website_signal_count,
                    contributing_event_count, score_breakdown_json
                ) VALUES
                    (%s, CURRENT_DATE - 2, 78.0, 'rolling_30d', 'fixture', 0.85, 3, 3, 1, 4, '{}'::jsonb),
                    (%s, CURRENT_DATE - 2, 12.0, 'rolling_30d', 'fixture', 0.55, 0, 0, 0, 1, '{}'::jsonb)
                ON CONFLICT (account_id, score_date, score_window) DO UPDATE
                SET score = EXCLUDED.score, confidence = EXCLUDED.confidence
                """,
                (account_ids[0], account_ids[1]),
            )

        conn.commit()

    try:
        service.run(rebuild=True, window_days=30)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT influence_score FROM opportunity_influence WHERE opportunity_id = %s", (opp_influenced_id,))
                influenced_score = float(cur.fetchone()[0])
                cur.execute("SELECT influence_score FROM opportunity_influence WHERE opportunity_id = %s", (opp_quiet_id,))
                quiet_score = float(cur.fetchone()[0])

        assert_true(
            influenced_score > quiet_score,
            f"Fixture ranking sanity failed: influenced_score={influenced_score}, quiet_score={quiet_score}",
        )
    finally:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM opportunity_influence WHERE opportunity_id IN (%s, %s)", (opp_influenced_id, opp_quiet_id))
                cur.execute("DELETE FROM social_event_matches WHERE social_event_id = ANY(%s)", (event_ids,))
                cur.execute("DELETE FROM social_events WHERE id = ANY(%s)", (event_ids,))
                cur.execute("DELETE FROM website_events WHERE account_id IN (%s, %s)", (account_ids[0], account_ids[1]))
                cur.execute("DELETE FROM opportunities WHERE id IN (%s, %s)", (opp_influenced_id, opp_quiet_id))
                cur.execute("DELETE FROM account_intent_scores WHERE account_id IN (%s, %s)", (account_ids[0], account_ids[1]))
                cur.execute("DELETE FROM posts WHERE post_url = 'https://www.linkedin.com/posts/<REDACTED_POST>'")
                cur.execute("DELETE FROM accounts WHERE id IN (%s, %s)", (account_ids[0], account_ids[1]))
            conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify deterministic opportunity attribution behavior.")
    parser.add_argument("--window-days", type=int, default=30, help="Attribution lookback window in days (30 or 60).")
    parser.add_argument("--include-fixture", action="store_true", help="Run synthetic influenced vs quiet fixture check.")
    parser.add_argument("--print-snapshot", action="store_true", help="Print snapshot output.")
    args = parser.parse_args()

    service = OpportunityAttributionService()

    service.run(rebuild=True, window_days=args.window_days)
    first = snapshot()

    service.run(rebuild=False, window_days=args.window_days)
    second = snapshot()

    service.run(rebuild=True, window_days=args.window_days)
    third = snapshot()

    assert_true(first == second, "Incremental rerun changed attribution snapshot unexpectedly")
    assert_true(first == third, "Rebuild snapshot drift detected")
    assert_true(second["total_rows"] == second["total_opps"], "Expected one attribution row per opportunity")
    assert_true(second["duplicates"] == 0, "Duplicate opportunity attribution rows found")
    assert_true(second["missing_explainability"] == 0, "Missing notes or score breakdown in attribution rows")
    assert_true(second["post_opp_social_used"] == 0, "Post-opportunity social events leaked into attribution")
    assert_true(second["bad_confidence"] == 0, "Confidence out of range [0,1]")
    assert_true(second["unresolved_leak"] == 0, "Unresolved/skipped events leaked into matched_event_count")

    avg_high, avg_low = second["high_vs_low"]
    if avg_high is not None and avg_low is not None:
        assert_true(float(avg_high) >= float(avg_low), "High-signal opportunities should score >= low-signal opportunities")

    if args.include_fixture:
        fixture_ranking_check(service)

    print("[PASS] rebuild_stability")
    print("[PASS] incremental_idempotency")
    print("[PASS] one_row_per_opportunity")
    print("[PASS] explainability_fields")
    print("[PASS] no_post_opp_leakage")
    print("[PASS] confidence_bounds")
    print("[PASS] ranking_sanity")

    if args.include_fixture:
        print("[PASS] fixture_ranking_sanity")

    if args.print_snapshot:
        print("snapshot:")
        for key, value in second.items():
            print(f"- {key}: {value}")


if __name__ == "__main__":
    main()
