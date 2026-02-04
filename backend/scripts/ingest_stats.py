import argparse
import json
import sqlite3


def fetch_count(cur, table):
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def main():
    parser = argparse.ArgumentParser(description="Fetch ingest stats from the database.")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    cur = conn.cursor()

    stats = {
        "no_metadata": fetch_count(cur, "no_metadata"),
        "with_metadata": fetch_count(cur, "with_metadata"),
        "to_download_references": fetch_count(cur, "to_download_references"),
        "downloaded_references": fetch_count(cur, "downloaded_references"),
    }
    conn.close()

    print(json.dumps({"stats": stats, "source": "db"}))


if __name__ == "__main__":
    main()
