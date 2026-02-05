import argparse
import json
import sqlite3


def main():
    parser = argparse.ArgumentParser(description="List recent ingest runs from the database.")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--limit", type=int, default=20, help="Max number of runs to return.")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to filter runs.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ingest_entries'")
    has_ingest_entries = cur.fetchone() is not None

    runs = []
    if has_ingest_entries:
        conditions = []
        params = []
        if args.corpus_id is not None:
            conditions.append("corpus_id = ?")
            params.append(args.corpus_id)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT ingest_source,
                   COUNT(*) AS entry_count,
                   MAX(created_at) AS last_created_at,
                   MAX(source_pdf) AS source_pdf
            FROM ingest_entries
            {where_clause}
            GROUP BY ingest_source
            ORDER BY last_created_at DESC
            LIMIT ?
        """
        params.append(args.limit)
        cur.execute(query, params)
        runs = [dict(row) for row in cur.fetchall()]
    else:
        # Fallback for older DBs: infer runs from no_metadata rows.
        joins = ""
        conditions = []
        params = []
        if args.corpus_id is not None:
            joins = "JOIN corpus_items ci ON ci.table_name = 'no_metadata' AND ci.row_id = no_metadata.id"
            conditions.append("ci.corpus_id = ?")
            params.append(args.corpus_id)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT COALESCE(no_metadata.source_pdf, 'unknown') AS ingest_source,
                   COUNT(*) AS entry_count,
                   MAX(no_metadata.date_added) AS last_created_at,
                   MAX(no_metadata.source_pdf) AS source_pdf
            FROM no_metadata
            {joins}
            {where_clause}
            GROUP BY COALESCE(no_metadata.source_pdf, 'unknown')
            ORDER BY last_created_at DESC
            LIMIT ?
        """
        params.append(args.limit)
        cur.execute(query, params)
        runs = [dict(row) for row in cur.fetchall()]

    conn.close()
    print(json.dumps({"runs": runs, "total": len(runs), "source": "db"}))


if __name__ == "__main__":
    main()

