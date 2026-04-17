import argparse
import json
import sqlite3


def fetch_count(cur, where_sql="", params=()):
    query = "SELECT COUNT(*) FROM works"
    if where_sql:
        query += f" WHERE {where_sql}"
    cur.execute(query, params)
    return int(cur.fetchone()[0] or 0)


def fetch_corpus_count(cur, corpus_id, where_sql="", params=()):
    query = """
        SELECT COUNT(*)
        FROM works w
        JOIN corpus_works cw ON cw.work_id = w.id
        WHERE cw.corpus_id = ?
    """
    final_params = [corpus_id]
    if where_sql:
        query += f" AND ({where_sql})"
        final_params.extend(params)
    cur.execute(query, final_params)
    return int(cur.fetchone()[0] or 0)


def main():
    parser = argparse.ArgumentParser(description="Fetch ingest stats from the database.")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to filter stats.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    cur = conn.cursor()

    if args.corpus_id is None:
        count_fn = lambda where_sql="", params=(): fetch_count(cur, where_sql, params)
    else:
        count_fn = lambda where_sql="", params=(): fetch_corpus_count(cur, args.corpus_id, where_sql, params)

    stats = {
        "raw_pending": count_fn("metadata_status IN ('pending', 'in_progress')"),
        "matched": count_fn("metadata_status = 'matched' AND COALESCE(download_status, 'not_requested') = 'not_requested'"),
        "queued_download": count_fn("download_status IN ('queued', 'in_progress')"),
        "downloaded": count_fn("download_status = 'downloaded'"),
    }
    conn.close()

    print(json.dumps({"stats": stats, "source": "db"}))


if __name__ == "__main__":
    main()
