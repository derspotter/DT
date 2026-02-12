import argparse
import json
import sqlite3


def _has_column(cursor, table, column):
    rows = cursor.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in rows)


def fetch_count(cur, table, corpus_id=None):
    if corpus_id is None:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM corpus_items WHERE corpus_id = ? AND table_name = ?",
        (corpus_id, table),
    )
    return cur.fetchone()[0]


def fetch_queue_count(cur, corpus_id=None):
    queue_table = "to_download_references"
    has_state = _has_column(cur, queue_table, "download_state")
    state_filter = " wm.download_state IN ('queued', 'in_progress')" if has_state else " 1 = 1"

    if corpus_id is None:
        query = f"SELECT COUNT(*) FROM {queue_table}"
        if has_state:
            query += " WHERE download_state IN ('queued', 'in_progress')"
        cur.execute(query)
        return cur.fetchone()[0]
    cur.execute(
        f"""
        SELECT COUNT(*)
           FROM {queue_table} wm
           JOIN corpus_items ci ON ci.table_name = '{queue_table}' AND ci.row_id = wm.id
          WHERE ci.corpus_id = ?
            AND ({state_filter})
        """,
        (corpus_id,),
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
        "to_download_references": fetch_queue_count(cur, args.corpus_id),
        "downloaded_references": fetch_count(cur, "downloaded_references", args.corpus_id),
    }
    conn.close()

    print(json.dumps({"stats": stats, "source": "db"}))


if __name__ == "__main__":
    main()
