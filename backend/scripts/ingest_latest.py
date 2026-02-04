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
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ingest_entries'")
    has_ingest_entries = cur.fetchone() is not None

    if has_ingest_entries:
        if args.base_name:
            query = """
                SELECT id, title, authors, year, doi, source, publisher, url, source_pdf, created_at AS date_added
                FROM ingest_entries
                WHERE ingest_source = ?
                ORDER BY created_at DESC LIMIT ?
            """
            params = (args.base_name, args.limit)
        else:
            query = """
                SELECT id, title, authors, year, doi, source, publisher, url, source_pdf, created_at AS date_added
                FROM ingest_entries
                ORDER BY created_at DESC LIMIT ?
            """
            params = (args.limit,)
    else:
        if args.base_name:
            query = """
                SELECT id, title, authors, year, doi, source, publisher, url, source_pdf, date_added
                FROM no_metadata
                WHERE source_pdf LIKE ?
                ORDER BY date_added DESC LIMIT ?
            """
            params = (f"%{args.base_name}%", args.limit)
        else:
            query = """
                SELECT id, title, authors, year, doi, source, publisher, url, source_pdf, date_added
                FROM no_metadata
                ORDER BY date_added DESC LIMIT ?
            """
            params = (args.limit,)

    cur.execute(query, params)
    rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["authors"] = parse_maybe_json_list(row.get("authors"))
    conn.close()

    print(json.dumps({"items": rows, "total": len(rows), "source": "db"}))


if __name__ == "__main__":
    main()
