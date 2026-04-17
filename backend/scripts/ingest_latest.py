import argparse
import json
import re
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


def normalize_text(text):
    if not text:
        return None
    return re.sub(r'[^a-z0-9\s]', '', str(text).lower()).strip() or None


def normalize_doi(doi):
    if not doi:
        return None
    match = re.search(r'(10\.\d{4,9}/[-._;()/:A-Z0-9]+)', str(doi), re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return str(doi).upper()


def parse_name_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            except Exception:
                pass
        return [raw]
    val = str(value).strip()
    return [val] if val else []


def normalize_contributors(authors):
    names = parse_name_list(authors)
    if not names:
        return None
    joined = " ".join(names)
    return normalize_text(joined)


def normalize_year(value):
    if value is None:
        return None
    raw = str(value).strip()
    return raw if raw else None


def is_row_downloaded(cur, row, has_downloaded_references, has_work_aliases):
    if not has_downloaded_references:
        return False

    doi = normalize_doi(row.get("doi"))
    if doi:
        cur.execute("SELECT 1 FROM downloaded_references WHERE doi = ? LIMIT 1", (doi,))
        if cur.fetchone():
            return True

    normalized_title = normalize_text(row.get("title"))
    normalized_authors = normalize_contributors(row.get("authors"))
    year_str = normalize_year(row.get("year"))

    if normalized_title and normalized_authors and year_str:
        cur.execute(
            """
            SELECT 1
            FROM downloaded_references
            WHERE normalized_title = ?
              AND normalized_authors = ?
              AND CAST(year AS TEXT) = ?
            LIMIT 1
            """,
            (normalized_title, normalized_authors, year_str),
        )
        if cur.fetchone():
            return True

    if normalized_title and normalized_authors and not year_str:
        cur.execute(
            """
            SELECT 1
            FROM downloaded_references
            WHERE normalized_title = ?
              AND normalized_authors = ?
            LIMIT 1
            """,
            (normalized_title, normalized_authors),
        )
        if cur.fetchone():
            return True

    # Alias fallback: catches translated/alternate titles linked to downloaded rows.
    if has_work_aliases and normalized_title:
        if year_str:
            try:
                year_int = int(year_str)
            except Exception:
                year_int = None
            if year_int is not None:
                cur.execute(
                    """
                    SELECT 1
                    FROM work_aliases
                    WHERE work_table = 'downloaded_references'
                      AND normalized_alias_title = ?
                      AND (alias_year IS NULL OR alias_year BETWEEN ? AND ?)
                    LIMIT 1
                    """,
                    (normalized_title, year_int - 1, year_int + 1),
                )
                if cur.fetchone():
                    return True
        else:
            cur.execute(
                """
                SELECT 1
                FROM work_aliases
                WHERE work_table = 'downloaded_references'
                  AND normalized_alias_title = ?
                LIMIT 1
                """,
                (normalized_title,),
            )
            if cur.fetchone():
                return True

    return False


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
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ingest_source_metadata'")
    has_source_metadata = cur.fetchone() is not None
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='downloaded_references'")
    has_downloaded_references = cur.fetchone() is not None
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='work_aliases'")
    has_work_aliases = cur.fetchone() is not None
    ingest_entries_columns = set()
    if has_ingest_entries:
        cur.execute("PRAGMA table_info(ingest_entries)")
        ingest_entries_columns = {str(row["name"]) for row in cur.fetchall()}

    if has_ingest_entries:
        has_processed_column = "processed" in ingest_entries_columns
        conditions = []
        params = []
        if args.base_name:
            conditions.append("ingest_source = ?")
            params.append(args.base_name)
        if args.corpus_id is not None:
            conditions.append("corpus_id = ?")
            params.append(args.corpus_id)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        processed_select = "processed" if has_processed_column else "0"
        query = f"""
            SELECT id, title, authors, year, doi, source, publisher, url, source_pdf,
                   created_at AS date_added, {processed_select} AS processed
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
                   no_metadata.date_added, 0 AS processed
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
        downloaded = is_row_downloaded(cur, row, has_downloaded_references, has_work_aliases)
        processed = int(row.get("processed") or 0) > 0
        if downloaded:
            row["ingest_state"] = "downloaded"
        elif processed:
            row["ingest_state"] = "enriched"
        else:
            row["ingest_state"] = "pending"

    seed_document = None
    if args.base_name and has_source_metadata:
        metadata_params = [args.base_name]
        corpus_filter = ""
        if args.corpus_id is not None:
            corpus_filter = "AND corpus_id = ?"
            metadata_params.append(args.corpus_id)
        else:
            corpus_filter = "AND corpus_id = 0"

        cur.execute(
            f"""
            SELECT title, authors, year, doi, source, publisher, source_pdf, ingest_source, updated_at, created_at
            FROM ingest_source_metadata
            WHERE ingest_source = ?
            {corpus_filter}
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            metadata_params,
        )
        md_row = cur.fetchone()
        if md_row:
            seed_document = dict(md_row)
            seed_document["authors"] = parse_maybe_json_list(seed_document.get("authors"))
    conn.close()

    print(json.dumps({"items": rows, "total": len(rows), "source": "db", "seed_document": seed_document}))


if __name__ == "__main__":
    main()
