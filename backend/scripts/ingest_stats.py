import argparse
import json
import sqlite3


def fetch_count(cur, table, corpus_id=None):
    if corpus_id is None:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM corpus_items WHERE corpus_id = ? AND table_name = ?",
        (corpus_id, table),
    )
    return cur.fetchone()[0]


def main():
    parser = argparse.ArgumentParser(description="Fetch ingest stats from the database.")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to filter stats.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    cur = conn.cursor()

    stats = {
        "no_metadata": fetch_count(cur, "no_metadata", args.corpus_id),
        "with_metadata": fetch_count(cur, "with_metadata", args.corpus_id),
        "to_download_references": fetch_count(cur, "to_download_references", args.corpus_id),
        "downloaded_references": fetch_count(cur, "downloaded_references", args.corpus_id),
    }
    conn.close()

    print(json.dumps({"stats": stats, "source": "db"}))


if __name__ == "__main__":
    main()
