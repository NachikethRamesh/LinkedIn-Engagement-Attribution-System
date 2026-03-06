from __future__ import annotations

import argparse

from app.linkedin_ingestion.base import LinkedInIngestionService
from app.linkedin_ingestion.mock_adapter import MockLinkedInAdapter


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate and ingest mock LinkedIn events.")
    parser.add_argument("--posts", type=int, default=20, help="Number of mock posts to generate.")
    parser.add_argument("--events", type=int, default=250, help="Number of mock events to generate.")
    args = parser.parse_args()

    adapter = MockLinkedInAdapter(posts=args.posts, events=args.events)
    batch = adapter.collect()

    service = LinkedInIngestionService()
    stats = service.ingest_batch(
        batch=batch,
        source_name="mock",
        filename=f"generated_posts_{args.posts}_events_{args.events}",
        import_mode="mock",
    )

    print("source: mock")
    print(f"rows read: {stats.row_count}")
    print(f"rows successful: {stats.success_count}")
    print(f"posts created: {stats.posts_created}")
    print(f"posts updated: {stats.posts_updated}")
    print(f"events inserted: {stats.events_inserted}")
    print(f"rows skipped: {stats.skip_count}")
    print(f"warnings: {stats.warning_count}")


if __name__ == "__main__":
    main()
