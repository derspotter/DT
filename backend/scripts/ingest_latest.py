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


def normalize_text(text):
    if not text:
        return None
    return re.sub(r"[^a-z0-9\s]", "", str(text).lower()).strip() or None


def normalize_doi(doi):
    if not doi:
        return None
    match = re.search(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", str(doi), re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return str(doi).upper()


def normalize_contributors(authors):
    names = parse_name_list(authors)
    if not names:
        return None
    return normalize_text(" ".join(names))


def normalize_year(value):
    if value is None:
        return None
    raw = str(value).strip()
    return raw if raw else None


def _table_exists(cursor, table):
    row = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return bool(row)


def _has_column(cursor, table, column):
    rows = cursor.execute(f"PRAGMA table_info({table})").fetchall()
    return any(str(row["name"]) == column for row in rows)


def _load_match_rows(cur, table, *, corpus_id=None, require_selected=False, only_queued=False):
    if not _table_exists(cur, table):
        return []

    join = ""
    conditions = []
    params = []
    if corpus_id is not None and table in {"no_metadata", "with_metadata", "downloaded_references", "to_download_references"}:
        join = f"JOIN corpus_items ci ON ci.table_name = '{table}' AND ci.row_id = t.id"
        conditions.append("ci.corpus_id = ?")
        params.append(corpus_id)

    if table == "no_metadata" and require_selected and _has_column(cur, table, "selected_for_enrichment"):
        conditions.append("COALESCE(t.selected_for_enrichment, 0) = 1")

    if table == "with_metadata" and only_queued and _has_column(cur, table, "download_state"):
        conditions.append("COALESCE(t.download_state, 'with_metadata') IN ('queued', 'in_progress')")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
        SELECT t.id, t.title, t.authors, t.year, t.doi
        FROM {table} t
        {join}
        {where}
    """
    cur.execute(query, params)
    return [dict(row) for row in cur.fetchall()]


def _build_match_index(rows):
    index = {
        "doi": set(),
        "title_authors_year": set(),
        "title_authors": set(),
        "title_year": set(),
    }
    for row in rows:
        doi = normalize_doi(row.get("doi"))
        if doi:
            index["doi"].add(doi)
        title = normalize_text(row.get("title"))
        authors = normalize_contributors(row.get("authors"))
        year = normalize_year(row.get("year"))
        if title and authors and year:
            index["title_authors_year"].add((title, authors, year))
        if title and authors and not year:
            index["title_authors"].add((title, authors))
        if title and year and not authors:
            index["title_year"].add((title, year))
    return index


def _matches_index(row, index):
    doi = normalize_doi(row.get("doi"))
    if doi and doi in index["doi"]:
        return True

    title = normalize_text(row.get("title"))
    authors = normalize_contributors(row.get("authors"))
    year = normalize_year(row.get("year"))
    if title and authors and year and (title, authors, year) in index["title_authors_year"]:
        return True
    if title and authors and not year and (title, authors) in index["title_authors"]:
        return True
    if title and year and not authors and (title, year) in index["title_year"]:
        return True
    return False


def _is_row_downloaded(cur, row, downloaded_index, has_work_aliases):
    if _matches_index(row, downloaded_index):
        return True

    if not has_work_aliases:
        return False

    normalized_title = normalize_text(row.get("title"))
    if not normalized_title:
        return False

    year_str = normalize_year(row.get("year"))
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


def _derive_ingest_state(cur, row, indexes, has_work_aliases):
    if _is_row_downloaded(cur, row, indexes["downloaded"], has_work_aliases):
        return "downloaded"
    if _matches_index(row, indexes["failed_download"]):
        return "failed_download"
    if _matches_index(row, indexes["queued_download"]):
        return "queued_download"
    if _matches_index(row, indexes["with_metadata"]):
        return "enriched"
    if _matches_index(row, indexes["failed_enrichment"]):
        return "failed_enrichment"
    if _matches_index(row, indexes["queued_enrichment"]):
        return "queued_enrichment"
    if _matches_index(row, indexes["raw"]):
        return "staged_raw"
    return "pending"


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

    has_ingest_entries = _table_exists(cur, "ingest_entries")
    has_source_metadata = _table_exists(cur, "ingest_source_metadata")
    has_work_aliases = _table_exists(cur, "work_aliases")

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
            SELECT id, title, authors, year, doi, source, publisher, url, source_pdf,
                   created_at AS date_added
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

    indexes = {
        "raw": _build_match_index(_load_match_rows(cur, "no_metadata", corpus_id=args.corpus_id)),
        "queued_enrichment": _build_match_index(
            _load_match_rows(cur, "no_metadata", corpus_id=args.corpus_id, require_selected=True)
        ),
        "with_metadata": _build_match_index(_load_match_rows(cur, "with_metadata", corpus_id=args.corpus_id)),
        "queued_download": _build_match_index(
            _load_match_rows(cur, "with_metadata", corpus_id=args.corpus_id, only_queued=True)
        ),
        "downloaded": _build_match_index(_load_match_rows(cur, "downloaded_references", corpus_id=args.corpus_id)),
        "failed_enrichment": _build_match_index(_load_match_rows(cur, "failed_enrichments")),
        "failed_download": _build_match_index(_load_match_rows(cur, "failed_downloads")),
    }

    for row in rows:
        row["authors"] = parse_maybe_json_list(row.get("authors"))
        row["ingest_state"] = _derive_ingest_state(cur, row, indexes, has_work_aliases)

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
