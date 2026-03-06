from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection
from app.identity_resolution.matcher import IdentityResolutionService


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def fetchone_int(cur, query: str, params=None) -> int:
    cur.execute(query, params or ())
    return int(cur.fetchone()[0])


def snapshot_match_metrics() -> dict[str, object]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            total_match_rows = fetchone_int(cur, "SELECT COUNT(*) FROM social_event_matches;")
            total_social_events = fetchone_int(cur, "SELECT COUNT(*) FROM social_events;")
            matched_contact_count = fetchone_int(
                cur, "SELECT COUNT(*) FROM social_event_matches WHERE matched_contact_id IS NOT NULL;"
            )
            matched_account_count = fetchone_int(
                cur, "SELECT COUNT(*) FROM social_event_matches WHERE matched_account_id IS NOT NULL;"
            )
            unresolved_count = fetchone_int(
                cur, "SELECT COUNT(*) FROM social_event_matches WHERE match_type = 'unresolved';"
            )
            skipped_aggregate_count = fetchone_int(
                cur, "SELECT COUNT(*) FROM social_event_matches WHERE match_type = 'skipped_aggregate_import';"
            )
            cur.execute(
                """
                SELECT match_type, COUNT(*)
                FROM social_event_matches
                GROUP BY match_type
                ORDER BY match_type
                """
            )
            counts_by_match_type = tuple(cur.fetchall())

    return {
        "total_social_events": total_social_events,
        "total_match_rows": total_match_rows,
        "matched_contact_count": matched_contact_count,
        "matched_account_count": matched_account_count,
        "unresolved_count": unresolved_count,
        "skipped_aggregate_import_count": skipped_aggregate_count,
        "counts_by_match_type": counts_by_match_type,
    }


def check_idempotency_and_rebuild() -> None:
    service = IdentityResolutionService()

    service.run(rebuild=True)
    snapshot_after_first_rebuild = snapshot_match_metrics()
    summary_incremental = service.run(rebuild=False)
    service.run(rebuild=True)
    snapshot_after_second_rebuild = snapshot_match_metrics()

    assert_true(summary_incremental["events_processed"] == 0, "Incremental rerun should process zero already-matched events")
    assert_true(
        snapshot_after_second_rebuild["total_match_rows"] == snapshot_after_second_rebuild["total_social_events"],
        "After rebuild, every social_event should have exactly one match row",
    )
    assert_true(
        snapshot_after_first_rebuild == snapshot_after_second_rebuild,
        (
            "Rebuild runs drifted. "
            f"first={snapshot_after_first_rebuild} second={snapshot_after_second_rebuild}"
        ),
    )


def check_aggregate_safety() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            count_bad_contact_matches = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM social_event_matches sem
                JOIN social_events se ON se.id = sem.social_event_id
                WHERE COALESCE((se.metadata_json->>'aggregated_import')::boolean, false) = true
                  AND sem.matched_contact_id IS NOT NULL
                """,
            )
            assert_true(
                count_bad_contact_matches == 0,
                "Aggregate-import events must never produce contact matches",
            )

            count_aggregate_without_safe_type = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM social_event_matches sem
                JOIN social_events se ON se.id = sem.social_event_id
                WHERE COALESCE((se.metadata_json->>'aggregated_import')::boolean, false) = true
                  AND sem.match_type NOT IN (
                      'skipped_aggregate_import',
                      'exact_account_name',
                      'normalized_account_name',
                      'inferred_from_actor_company',
                      'inferred_from_website_domain'
                  )
                """,
            )
            assert_true(
                count_aggregate_without_safe_type == 0,
                "Aggregate-import events must resolve to safe aggregate-oriented match types",
            )


def insert_ambiguity_fixture() -> tuple[int, int, list[int], int, int]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO accounts (company_name, domain, target_tier, created_at)
                VALUES
                    ('ACME LLC', 'acme.test', 'Tier 2', NOW()),
                    ('ACME Inc.', 'acme-inc.test', 'Tier 2', NOW())
                RETURNING id
                """
            )
            account_ids = [row[0] for row in cur.fetchall()]

            cur.execute(
                """
                INSERT INTO contacts (account_id, full_name, email, linkedin_url, title)
                VALUES
                    (%s, 'Alex Doe', 'redacted123@example.com', 'https://www.linkedin.com/in/<REDACTED_PROFILE>', 'Director'),
                    (%s, 'Alex Doe', 'redacted124@example.com', 'https://www.linkedin.com/in/<REDACTED_PROFILE>', 'Director')
                RETURNING id
                """,
                (account_ids[0], account_ids[0]),
            )
            contact_ids = [row[0] for row in cur.fetchall()]

            cur.execute(
                """
                INSERT INTO posts (author_name, post_url, topic, cta_url, created_at)
                VALUES ('Fixture Author', 'https://www.linkedin.com/posts/<REDACTED_POST>', 'Fixture', NULL, NOW())
                RETURNING id
                """
            )
            post_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO social_events (post_id, actor_name, actor_linkedin_url, actor_company_raw, event_type, event_timestamp, metadata_json)
                VALUES
                    (%s, 'Alex Doe', NULL, 'ACME LLC', 'post_comment', NOW(), '{"source_name":"fixture","import_mode":"test","aggregated_import":false,"actor_origin":"known"}'),
                    (%s, NULL, NULL, 'Acme', 'post_like', NOW(), '{"source_name":"fixture","import_mode":"test","aggregated_import":false,"actor_origin":"aggregate_unknown"}')
                RETURNING id
                """,
                (post_id, post_id),
            )
            event_ids = [row[0] for row in cur.fetchall()]

        conn.commit()

    return account_ids[0], account_ids[1], contact_ids, event_ids[0], event_ids[1]


def cleanup_ambiguity_fixture(account_a: int, account_b: int, contact_ids: list[int], event_a: int, event_b: int) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM social_event_matches WHERE social_event_id IN (%s, %s);", (event_a, event_b))
            cur.execute("DELETE FROM social_events WHERE id IN (%s, %s);", (event_a, event_b))
            cur.execute("DELETE FROM contacts WHERE id = ANY(%s);", (contact_ids,))
            cur.execute("DELETE FROM posts WHERE post_url = 'https://www.linkedin.com/posts/<REDACTED_POST>';")
            cur.execute("DELETE FROM accounts WHERE id IN (%s, %s);", (account_a, account_b))
        conn.commit()


def check_ambiguity_safety() -> None:
    account_a, account_b, contact_ids, event_contact_ambig, event_account_ambig = insert_ambiguity_fixture()
    try:
        service = IdentityResolutionService()
        service.run(rebuild=False)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT match_type, matched_contact_id, matched_account_id, match_reason FROM social_event_matches WHERE social_event_id = %s;",
                    (event_contact_ambig,),
                )
                contact_case = cur.fetchone()

                cur.execute(
                    "SELECT match_type, matched_contact_id, matched_account_id, match_reason FROM social_event_matches WHERE social_event_id = %s;",
                    (event_account_ambig,),
                )
                account_case = cur.fetchone()

        assert_true(contact_case is not None, "Ambiguous contact fixture must produce a match row")
        assert_true(account_case is not None, "Ambiguous account fixture must produce a match row")

        assert_true(contact_case[0] == "unresolved", "Ambiguous contact candidates must resolve to unresolved")
        assert_true(contact_case[1] is None, "Ambiguous contact candidates must not set matched_contact_id")
        assert_true("Ambiguous" in contact_case[3], "Ambiguous contact reason should be explicit")

        assert_true(account_case[0] == "unresolved", "Ambiguous account candidates must resolve to unresolved")
        assert_true(account_case[2] is None, "Ambiguous account candidates must not set matched_account_id")
        assert_true("Ambiguous" in account_case[3], "Ambiguous account reason should be explicit")
    finally:
        cleanup_ambiguity_fixture(account_a, account_b, contact_ids, event_contact_ambig, event_account_ambig)


def check_confidence_and_integrity() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            missing_required = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM social_event_matches
                WHERE match_type IS NULL
                   OR match_reason IS NULL
                   OR matched_on_fields_json IS NULL
                """,
            )
            assert_true(missing_required == 0, "Every match row must have match_type, match_reason, and matched_on_fields_json")

            out_of_range = fetchone_int(
                cur,
                "SELECT COUNT(*) FROM social_event_matches WHERE match_confidence < 0 OR match_confidence > 1;",
            )
            assert_true(out_of_range == 0, "Match confidence values must be within [0,1]")

            bad_contact_account_link = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM social_event_matches sem
                JOIN contacts c ON c.id = sem.matched_contact_id
                WHERE sem.matched_account_id IS DISTINCT FROM c.account_id
                """,
            )
            assert_true(
                bad_contact_account_link == 0,
                "When both matched_contact_id and matched_account_id exist, they must refer to the same account",
            )

            duplicates = fetchone_int(
                cur,
                """
                SELECT COUNT(*)
                FROM (
                    SELECT social_event_id
                    FROM social_event_matches
                    GROUP BY social_event_id
                    HAVING COUNT(*) > 1
                ) dup
                """,
            )
            assert_true(duplicates == 0, "There must be at most one final match row per social_event_id")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify identity resolution idempotency and safety guarantees.")
    parser.add_argument("--include-ambiguity-fixture", action="store_true", help="Insert temporary ambiguity fixtures and verify unresolved behavior.")
    args = parser.parse_args()

    checks = [
        ("idempotency_and_rebuild", check_idempotency_and_rebuild),
        ("aggregate_safety", check_aggregate_safety),
        ("confidence_and_integrity", check_confidence_and_integrity),
    ]

    if args.include_ambiguity_fixture:
        checks.append(("ambiguity_safety", check_ambiguity_safety))

    for name, check in checks:
        check()
        print(f"[PASS] {name}")

    stability = snapshot_match_metrics()
    print("[INFO] rebuild_stability_snapshot")
    print(f"- total_social_events: {stability['total_social_events']}")
    print(f"- total_match_rows: {stability['total_match_rows']}")
    print(f"- matched_contact_count: {stability['matched_contact_count']}")
    print(f"- matched_account_count: {stability['matched_account_count']}")
    print(f"- unresolved_count: {stability['unresolved_count']}")
    print(f"- skipped_aggregate_import_count: {stability['skipped_aggregate_import_count']}")
    print(f"- counts_by_match_type: {stability['counts_by_match_type']}")


if __name__ == "__main__":
    main()
