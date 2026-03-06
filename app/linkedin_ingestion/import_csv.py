from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.linkedin_ingestion.base import LinkedInIngestionService
from app.linkedin_ingestion.csv_adapter import CSVLinkedInAdapter


def load_mapping_override(mapping_file: str | None) -> dict[str, list[str]] | None:
    if mapping_file is None:
        return None

    path = Path(mapping_file)
    raw = json.loads(path.read_text(encoding="utf-8"))
    mapping: dict[str, list[str]] = {}
    for key, value in raw.items():
        if isinstance(value, list):
            mapping[key] = [str(item) for item in value]
        else:
            mapping[key] = [str(value)]
    return mapping


def main() -> None:
    parser = argparse.ArgumentParser(description="Import LinkedIn CSV data into posts and social_events.")
    parser.add_argument("--source", required=True, choices=["shield", "sprout", "generic"])
    parser.add_argument("--file", required=True, help="Path to CSV file.")
    parser.add_argument("--mapping-file", help="Optional JSON mapping file for generic mode.")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter (default: ',').")
    args = parser.parse_args()

    mapping_override = load_mapping_override(args.mapping_file)
    adapter = CSVLinkedInAdapter(
        file_path=args.file,
        source_name=args.source,
        mapping_override=mapping_override,
        delimiter=args.delimiter,
    )
    batch = adapter.collect()

    service = LinkedInIngestionService()
    stats = service.ingest_batch(
        batch=batch,
        source_name=args.source,
        filename=args.file,
        import_mode="csv",
    )

    print(f"source: {stats.source_name}")
    print(f"file: {stats.filename}")
    print(f"rows read: {stats.row_count}")
    print(f"rows successful: {stats.success_count}")
    print(f"posts created: {stats.posts_created}")
    print(f"posts updated: {stats.posts_updated}")
    print(f"events inserted: {stats.events_inserted}")
    print(f"rows skipped: {stats.skip_count}")
    print(f"warnings: {stats.warning_count}")

    if stats.warnings:
        print("sample warnings:")
        for warning in stats.warnings[:10]:
            print(f"- row {warning.row_id}: {warning.message}")


if __name__ == "__main__":
    main()
