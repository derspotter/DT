#!/usr/bin/env python3
import argparse
from dl_lit.db_manager import DatabaseManager


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill citation_edges from with_metadata relationships.")
    parser.add_argument("--db-path", required=True, help="Path to literature.db")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of rows to process")
    parser.add_argument("--dry-run", action="store_true", help="Estimate without inserting edges")
    args = parser.parse_args()

    db = DatabaseManager(db_path=args.db_path)
    summary = db.backfill_citation_edges(limit=args.limit, dry_run=args.dry_run)
    print(
        f"Backfill complete. rows={summary['rows_seen']} "
        f"inserted={summary['edges_inserted']} skipped={summary['edges_skipped']}"
    )
    db.close_connection()


if __name__ == "__main__":
    main()
