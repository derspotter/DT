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
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                pass
        return [raw]
    return [str(value)]


def parse_name_list(value):
    parsed = parse_maybe_json_list(value)
    if parsed is None:
        return []
    if isinstance(parsed, list):
        return [str(item).strip() for item in parsed if str(item).strip()]
    raw = str(parsed).strip()
    return [raw] if raw else []


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
    return raw or None


def build_work_indexes(conn, corpus_id=None):
    if corpus_id is not None:
        rows = conn.execute(
            """
            SELECT w.id, w.title, w.authors, w.year, w.doi, w.metadata_status, w.download_status
            FROM works w
            JOIN corpus_works cw ON cw.work_id = w.id
            WHERE cw.corpus_id = ?
            """,
            (corpus_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, title, authors, year, doi, metadata_status, download_status FROM works"
        ).fetchall()

    indexes = {
        "downloaded": {"doi": set(), "tay": set(), "ta": set()},
        "failed_download": {"doi": set(), "tay": set(), "ta": set()},
        "queued_download": {"doi": set(), "tay": set(), "ta": set()},
        "matched": {"doi": set(), "tay": set(), "ta": set()},
        "failed_enrichment": {"doi": set(), "tay": set(), "ta": set()},
        "raw": {"doi": set(), "tay": set(), "ta": set()},
    }

    def add(index_name, row):
        doi = normalize_doi(row["doi"])
        title = normalize_text(row["title"])
        authors = normalize_contributors(row["authors"])
        year = normalize_year(row["year"])
        if doi:
            indexes[index_name]["doi"].add(doi)
        if title and authors and year:
            indexes[index_name]["tay"].add((title, authors, year))
        if title and authors:
            indexes[index_name]["ta"].add((title, authors))

    for row in rows:
        metadata = str(row["metadata_status"] or "pending").strip().lower()
        download = str(row["download_status"] or "not_requested").strip().lower()
        if download == "downloaded":
            add("downloaded", row)
        elif download == "failed":
            add("failed_download", row)
        elif download in {"queued", "in_progress"}:
            add("queued_download", row)
        elif metadata == "matched":
            add("matched", row)
        elif metadata == "failed":
            add("failed_enrichment", row)
        else:
            add("raw", row)

    return indexes


def matches_index(row, index):
    doi = normalize_doi(row.get("doi"))
    if doi and doi in index["doi"]:
        return True
    title = normalize_text(row.get("title"))
    authors = normalize_contributors(row.get("authors"))
    year = normalize_year(row.get("year"))
    if title and authors and year and (title, authors, year) in index["tay"]:
        return True
    if title and authors and (title, authors) in index["ta"]:
        return True
    return False


def is_downloaded_by_alias(conn, row, corpus_id=None):
    normalized_title = normalize_text(row.get("title"))
    if not normalized_title:
        return False
    year = normalize_year(row.get("year"))
    params = [normalized_title]
    sql = """
        SELECT 1
        FROM work_aliases wa
        JOIN works w ON w.id = wa.work_id
    """
    if corpus_id is not None:
        sql += " JOIN corpus_works cw ON cw.work_id = w.id "
    sql += """
        WHERE w.download_status = 'downloaded'
          AND wa.normalized_alias_title = ?
    """
    if corpus_id is not None:
        sql += " AND cw.corpus_id = ?"
        params.append(corpus_id)
    if year:
        try:
            year_int = int(year)
        except Exception:
            year_int = None
        if year_int is not None:
            sql += " AND (wa.alias_year IS NULL OR wa.alias_year BETWEEN ? AND ?)"
            params.extend([year_int - 1, year_int + 1])
    sql += " LIMIT 1"
    return conn.execute(sql, params).fetchone() is not None


def derive_ingest_state(conn, row, indexes, corpus_id=None):
    if matches_index(row, indexes["downloaded"]) or is_downloaded_by_alias(conn, row, corpus_id=corpus_id):
        return "downloaded"
    if matches_index(row, indexes["failed_download"]):
        return "failed_download"
    if matches_index(row, indexes["queued_download"]):
        return "queued_download"
    if matches_index(row, indexes["matched"]):
        return "enriched"
    if matches_index(row, indexes["failed_enrichment"]):
        return "failed_enrichment"
    if matches_index(row, indexes["raw"]):
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
    rows = [dict(row) for row in conn.execute(query, params).fetchall()]

    indexes = build_work_indexes(conn, corpus_id=args.corpus_id)
    for row in rows:
        row["authors"] = parse_maybe_json_list(row.get("authors"))
        row["ingest_state"] = derive_ingest_state(conn, row, indexes, corpus_id=args.corpus_id)

    seed_document = None
    metadata_params = []
    metadata_conditions = []
    if args.base_name:
        metadata_conditions.append("ingest_source = ?")
        metadata_params.append(args.base_name)
    if args.corpus_id is not None:
        metadata_conditions.append("corpus_id = ?")
        metadata_params.append(args.corpus_id)
    elif args.base_name:
        metadata_conditions.append("corpus_id = 0")

    if metadata_conditions:
        md_where = f"WHERE {' AND '.join(metadata_conditions)}"
        md_row = conn.execute(
            f"""
            SELECT title, authors, year, doi, source, publisher, source_pdf, ingest_source, updated_at, created_at
            FROM ingest_source_metadata
            {md_where}
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            metadata_params,
        ).fetchone()
        if md_row:
            seed_document = dict(md_row)
            seed_document["authors"] = parse_maybe_json_list(seed_document.get("authors"))

    conn.close()
    print(json.dumps({"items": rows, "total": len(rows), "source": "db", "seed_document": seed_document}))


if __name__ == "__main__":
    main()
