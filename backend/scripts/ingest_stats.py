import argparse
import json
import sqlite3


def _has_column(cursor, table, column):
    rows = cursor.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in rows)


def _table_exists(cursor, table):
    row = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return bool(row)


def fetch_count(cur, table, corpus_id=None):
    if not _table_exists(cur, table):
        return 0
    if corpus_id is None:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
    cur.execute(
        f"""
        SELECT COUNT(*)
          FROM {table} t
          JOIN corpus_items ci
            ON ci.table_name = ? AND ci.row_id = t.id
         WHERE ci.corpus_id = ?
        """,
        (table, corpus_id),
    )
    return cur.fetchone()[0]


def fetch_queue_count(cur, corpus_id=None):
    # Canonical queue now lives in with_metadata.download_state.
    queue_table = "with_metadata"
    if not _table_exists(cur, queue_table):
        return 0
    has_state = _has_column(cur, queue_table, "download_state")
    state_filter = "wm.download_state IN ('queued', 'in_progress')" if has_state else "1 = 0"

    if corpus_id is None:
        cur.execute(f"SELECT COUNT(*) FROM {queue_table} wm WHERE {state_filter}")
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
