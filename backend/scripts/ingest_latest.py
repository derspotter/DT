import argparse
import json
import sqlite3


def parse_maybe_json_list(value):
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return value
    return value


def main():
    parser = argparse.ArgumentParser(description="Fetch latest ingested bibliography entries.")
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--base-name", default="", help="Base filename (without .pdf) to filter source_pdf.")
    parser.add_argument("--limit", type=int, default=200, help="Max number of entries to return.")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to filter entries.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ingest_entries'")
    has_ingest_entries = cur.fetchone() is not None

    if has_ingest_entries:
        conditions = []
        params = []
        if args.base_name:
            conditions.append("ingest_source = ?")
            params.append(args.base_name)
        if args.corpus_id is not None:
            conditions.append("corpus_id = ?")
            params.append(args.corpus_id)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT id, title, authors, year, doi, source, publisher, url, source_pdf, created_at AS date_added
            FROM ingest_entries
            {where_clause}
            ORDER BY created_at DESC LIMIT ?
        """
        params.append(args.limit)
    else:
        joins = ""
        conditions = []
        params = []
        if args.corpus_id is not None:
            joins = "JOIN corpus_items ci ON ci.table_name = 'no_metadata' AND ci.row_id = no_metadata.id"
            conditions.append("ci.corpus_id = ?")
            params.append(args.corpus_id)
        if args.base_name:
            conditions.append("no_metadata.source_pdf LIKE ?")
            params.append(f"%{args.base_name}%")
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT no_metadata.id, no_metadata.title, no_metadata.authors, no_metadata.year, no_metadata.doi,
                   no_metadata.source, no_metadata.publisher, no_metadata.url, no_metadata.source_pdf,
                   no_metadata.date_added
            FROM no_metadata
            {joins}
            {where_clause}
            ORDER BY no_metadata.date_added DESC LIMIT ?
        """
        params.append(args.limit)

    cur.execute(query, params)
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["authors"] = parse_maybe_json_list(row.get("authors"))
    conn.close()

    print(json.dumps({"items": rows, "total": len(rows), "source": "db"}))


if __name__ == "__main__":
    main()
