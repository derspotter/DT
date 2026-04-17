import argparse
import json
import re
import sqlite3
from pathlib import Path


GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"


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
    return str(doi).strip().upper() or None


def normalize_openalex_id(value):
    if not value:
        return None
    match = re.search(r"(W\d+)", str(value), re.IGNORECASE)
    return match.group(1).upper() if match else None


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
            except json.JSONDecodeError:
                pass
        return [raw]
    return [str(value).strip()] if str(value).strip() else []


def normalize_contributors(authors=None, editors=None):
    author_list = parse_name_list(authors)
    if author_list:
        return normalize_text(" ".join(author_list))
    editor_list = parse_name_list(editors)
    if editor_list:
        return normalize_text(" ".join(editor_list))
    return None


def clean_json_text(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS app_meta (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS works (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          title TEXT NOT NULL,
          authors TEXT,
          editors TEXT,
          year INTEGER,
          doi TEXT,
          normalized_doi TEXT,
          openalex_id TEXT,
          semantic_scholar_id TEXT,
          source TEXT,
          publisher TEXT,
          volume TEXT,
          issue TEXT,
          pages TEXT,
          type TEXT,
          url TEXT,
          isbn TEXT,
          issn TEXT,
          abstract TEXT,
          keywords TEXT,
          normalized_title TEXT,
          normalized_authors TEXT,
          metadata_status TEXT NOT NULL DEFAULT 'pending',
          metadata_source TEXT,
          metadata_error TEXT,
          metadata_updated_at TIMESTAMP,
          download_status TEXT NOT NULL DEFAULT 'not_requested',
          download_source TEXT,
          download_error TEXT,
          downloaded_at TIMESTAMP,
          metadata_claimed_by TEXT,
          metadata_claimed_at INTEGER,
          metadata_lease_expires_at INTEGER,
          metadata_attempt_count INTEGER NOT NULL DEFAULT 0,
          metadata_last_attempt_at INTEGER,
          download_claimed_by TEXT,
          download_claimed_at INTEGER,
          download_lease_expires_at INTEGER,
          download_attempt_count INTEGER NOT NULL DEFAULT 0,
          download_last_attempt_at INTEGER,
          file_path TEXT,
          file_checksum TEXT,
          origin_type TEXT,
          origin_key TEXT,
          source_pdf TEXT,
          run_id INTEGER,
          source_work_id INTEGER,
          relationship_type TEXT,
          crossref_json TEXT,
          openalex_json TEXT,
          bibtex_entry_json TEXT,
          status_notes TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_works (
          corpus_id INTEGER NOT NULL,
          work_id INTEGER NOT NULL,
          added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (corpus_id, work_id)
        )
        """
    )
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_works_openalex_id ON works(openalex_id) WHERE openalex_id IS NOT NULL AND openalex_id != ''")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_works_normalized_doi ON works(normalized_doi) WHERE normalized_doi IS NOT NULL AND normalized_doi != ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_works_title_year ON works(normalized_title, year)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_works_authors ON works(normalized_authors)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_works_metadata_status ON works(metadata_status, metadata_lease_expires_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_works_download_status ON works(download_status, download_lease_expires_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_corpus_works_corpus ON corpus_works(corpus_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_corpus_works_work ON corpus_works(work_id)")
    if table_exists(conn, "search_results"):
        cols = {row[1] for row in cur.execute("PRAGMA table_info(search_results)").fetchall()}
        if "work_id" not in cols:
            cur.execute("ALTER TABLE search_results ADD COLUMN work_id INTEGER")
    conn.commit()


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def get_meta(conn, key):
    row = conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def set_meta(conn, key, value):
    conn.execute(
        """
        INSERT INTO app_meta(key, value, updated_at)
        VALUES(?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
        """,
        (key, value),
    )


def determine_legacy_status(table_name, row):
    metadata_status = "pending"
    download_status = "not_requested"
    metadata_error = None
    download_error = None
    if table_name == "downloaded_references":
        metadata_status = "matched"
        download_status = "downloaded"
    elif table_name == "with_metadata":
        metadata_status = "matched"
        state = str(row.get("download_state") or "with_metadata").strip().lower()
        if state in ("queued", "in_progress"):
            download_status = state
        elif state == "failed":
            download_status = "failed"
            download_error = row.get("download_last_error")
        else:
            download_status = "not_requested"
    elif table_name == "failed_enrichments":
        metadata_status = "failed"
        metadata_error = row.get("reason")
    elif table_name == "failed_downloads":
        metadata_status = "matched"
        download_status = "failed"
        download_error = row.get("status_notes") or row.get("reason")
    elif table_name == "to_download_references":
        metadata_status = "matched"
        download_status = "queued"
    elif table_name == "no_metadata":
        metadata_status = "pending"
    return metadata_status, download_status, metadata_error, download_error


def legacy_row_to_work_data(table_name, row):
    metadata_status, download_status, metadata_error, download_error = determine_legacy_status(table_name, row)
    title = row.get("title") or "[Untitled]"
    authors = row.get("authors")
    editors = row.get("editors")
    doi = row.get("doi")
    return {
        "title": title,
        "authors": authors,
        "editors": editors,
        "year": row.get("year"),
        "doi": doi,
        "normalized_doi": row.get("normalized_doi") or normalize_doi(doi),
        "openalex_id": normalize_openalex_id(row.get("openalex_id")),
        "semantic_scholar_id": row.get("semantic_scholar_id"),
        "source": row.get("source") or row.get("journal_conference"),
        "publisher": row.get("publisher"),
        "volume": row.get("volume"),
        "issue": row.get("issue"),
        "pages": row.get("pages"),
        "type": row.get("type") or row.get("entry_type"),
        "url": row.get("url") or row.get("url_source"),
        "isbn": row.get("isbn"),
        "issn": row.get("issn"),
        "abstract": row.get("abstract") or row.get("raw_text_search"),
        "keywords": row.get("keywords"),
        "normalized_title": row.get("normalized_title") or normalize_text(title),
        "normalized_authors": row.get("normalized_authors") or normalize_contributors(authors, editors),
        "metadata_status": metadata_status,
        "metadata_source": row.get("metadata_source_type"),
        "metadata_error": metadata_error,
        "metadata_updated_at": row.get("date_processed") or row.get("timestamp"),
        "download_status": download_status,
        "download_source": row.get("source_of_download"),
        "download_error": download_error,
        "downloaded_at": row.get("date_processed") if download_status == "downloaded" else None,
        "metadata_claimed_by": row.get("enrich_claimed_by"),
        "metadata_claimed_at": row.get("enrich_claimed_at"),
        "metadata_lease_expires_at": row.get("enrich_lease_expires_at"),
        "metadata_attempt_count": row.get("enrich_attempt_count") or 0,
        "metadata_last_attempt_at": row.get("enrich_last_attempt_at"),
        "download_claimed_by": row.get("download_claimed_by"),
        "download_claimed_at": row.get("download_claimed_at"),
        "download_lease_expires_at": row.get("download_lease_expires_at"),
        "download_attempt_count": row.get("download_attempt_count") or 0,
        "download_last_attempt_at": row.get("download_last_attempt_at"),
        "file_path": row.get("file_path"),
        "file_checksum": row.get("checksum_pdf"),
        "origin_type": "migration",
        "origin_key": f"{table_name}:{row.get('id')}",
        "source_pdf": row.get("source_pdf"),
        "run_id": row.get("run_id"),
        "source_work_id": row.get("source_work_id"),
        "relationship_type": row.get("relationship_type"),
        "crossref_json": clean_json_text(row.get("crossref_json")),
        "openalex_json": clean_json_text(row.get("openalex_json")),
        "bibtex_entry_json": clean_json_text(row.get("bibtex_entry_json")),
        "status_notes": row.get("status_notes") or row.get("reason"),
        "created_at": row.get("date_added") or row.get("created_at") or row.get("timestamp"),
        "updated_at": row.get("date_processed") or row.get("timestamp") or row.get("date_added"),
    }


def apply_merge_priority(existing, incoming):
    merged = dict(existing)
    preferred_download_rank = {"downloaded": 3, "in_progress": 2, "queued": 1, "not_requested": 0, "failed": 0}
    if preferred_download_rank.get(str(incoming.get("download_status") or ""), -1) > preferred_download_rank.get(str(existing.get("download_status") or ""), -1):
        for key in ("download_status", "download_source", "download_error", "downloaded_at", "file_path", "file_checksum"):
            merged[key] = incoming.get(key) or merged.get(key)
    metadata_rank = {"matched": 3, "in_progress": 2, "pending": 1, "failed": 0}
    if metadata_rank.get(str(incoming.get("metadata_status") or ""), -1) > metadata_rank.get(str(existing.get("metadata_status") or ""), -1):
        for key in ("metadata_status", "metadata_source", "metadata_error", "metadata_updated_at"):
            merged[key] = incoming.get(key) or merged.get(key)
    for key, value in incoming.items():
        if key in {"download_status", "download_source", "download_error", "downloaded_at", "file_path", "file_checksum", "metadata_status", "metadata_source", "metadata_error", "metadata_updated_at"}:
            continue
        if merged.get(key) in (None, "", "[]", "{}") and value not in (None, "", "[]", "{}"):
            merged[key] = value
    return merged


def insert_work(conn, data):
    payload = {k: v for k, v in data.items() if v is not None}
    cols = ", ".join(payload.keys())
    placeholders = ", ".join(["?"] * len(payload))
    cur = conn.cursor()
    cur.execute(f"INSERT INTO works ({cols}) VALUES ({placeholders})", tuple(payload.values()))
    return int(cur.lastrowid)


def update_work(conn, work_id, data):
    payload = {k: v for k, v in data.items() if k != "id"}
    assignments = ", ".join([f"{k} = ?" for k in payload.keys()]) + ", updated_at = CURRENT_TIMESTAMP"
    conn.execute(f"UPDATE works SET {assignments} WHERE id = ?", tuple(payload.values()) + (int(work_id),))


def migrate(db_path):
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    ensure_schema(conn)
    if get_meta(conn, "works_schema_version") == "1":
        print(json.dumps({"status": "already_migrated"}))
        return

    legacy_tables = [
        "downloaded_references",
        "with_metadata",
        "failed_downloads",
        "failed_enrichments",
        "no_metadata",
        "to_download_references",
    ]

    corpus_memberships = {}
    if table_exists(conn, "corpus_items"):
        for row in conn.execute("SELECT corpus_id, table_name, row_id FROM corpus_items").fetchall():
            corpus_memberships.setdefault((str(row["table_name"]), int(row["row_id"])), []).append(int(row["corpus_id"]))

    legacy_aliases = {}
    if table_exists(conn, "work_aliases"):
        for row in conn.execute(
            "SELECT work_table, work_id, alias_title, normalized_alias_title, alias_language, alias_year, relationship_type FROM work_aliases"
        ).fetchall():
            legacy_aliases.setdefault((str(row["work_table"]), int(row["work_id"])), []).append(
                (
                    row["alias_title"],
                    row["normalized_alias_title"],
                    row["alias_language"],
                    row["alias_year"],
                    row["relationship_type"],
                )
            )

    work_cache = {}
    doi_index = {}
    openalex_index = {}
    tay_index = {}
    ta_index = {}
    ty_index = {}

    def add_indices(work_id, data):
        normalized_doi = normalize_doi(data.get("doi") or data.get("normalized_doi"))
        if normalized_doi:
            doi_index[normalized_doi] = int(work_id)
        normalized_openalex = normalize_openalex_id(data.get("openalex_id"))
        if normalized_openalex:
            openalex_index[normalized_openalex] = int(work_id)
        normalized_title = data.get("normalized_title") or normalize_text(data.get("title"))
        normalized_authors = data.get("normalized_authors") or normalize_contributors(data.get("authors"), data.get("editors"))
        year = data.get("year")
        year_str = str(year).strip() if year not in (None, "") else None
        if normalized_title and normalized_authors and year_str:
            tay_index[(normalized_title, normalized_authors, year_str)] = int(work_id)
        if normalized_title and normalized_authors:
            ta_index[(normalized_title, normalized_authors)] = int(work_id)
        if normalized_title and year_str:
            ty_index[(normalized_title, year_str)] = int(work_id)

    def find_work_id(data):
        normalized_doi = normalize_doi(data.get("doi") or data.get("normalized_doi"))
        if normalized_doi and normalized_doi in doi_index:
            return int(doi_index[normalized_doi]), "normalized_doi"
        normalized_openalex = normalize_openalex_id(data.get("openalex_id"))
        if normalized_openalex and normalized_openalex in openalex_index:
            return int(openalex_index[normalized_openalex]), "openalex_id"
        normalized_title = data.get("normalized_title") or normalize_text(data.get("title"))
        normalized_authors = data.get("normalized_authors") or normalize_contributors(data.get("authors"), data.get("editors"))
        year = data.get("year")
        year_str = str(year).strip() if year not in (None, "") else None
        if normalized_title and normalized_authors and year_str:
            work_id = tay_index.get((normalized_title, normalized_authors, year_str))
            if work_id:
                return int(work_id), "title_authors_year"
        if normalized_title and normalized_authors:
            work_id = ta_index.get((normalized_title, normalized_authors))
            if work_id:
                return int(work_id), "title_authors"
        if normalized_title and year_str:
            work_id = ty_index.get((normalized_title, year_str))
            if work_id:
                return int(work_id), "title_year"
        return None, None

    existing_rows = conn.execute("SELECT * FROM works").fetchall()
    for row in existing_rows:
        data = dict(row)
        work_id = int(data["id"])
        work_cache[work_id] = data
        add_indices(work_id, data)

    conn.execute("BEGIN IMMEDIATE")
    migrated_rows = 0
    for table_name in legacy_tables:
        if not table_exists(conn, table_name):
            continue
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        if rows:
            print(f"{GREEN}[migrate_to_works] {table_name}: {len(rows)} row(s){RESET}", flush=True)
        for index, row in enumerate(rows, start=1):
            raw = dict(row)
            legacy_id = int(raw["id"])
            data = legacy_row_to_work_data(table_name, raw)
            work_id, _ = find_work_id(data)
            if work_id:
                merged = apply_merge_priority(work_cache[work_id], data)
                if merged != work_cache[work_id]:
                    update_work(conn, work_id, merged)
                    work_cache[work_id] = merged
                    add_indices(work_id, merged)
            else:
                work_id = insert_work(conn, data)
                work_cache[work_id] = dict(data, id=work_id)
                add_indices(work_id, data)
            for corpus_id in corpus_memberships.get((table_name, legacy_id), []):
                conn.execute(
                    "INSERT OR IGNORE INTO corpus_works(corpus_id, work_id, added_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    (int(corpus_id), int(work_id)),
                )
            for alias in legacy_aliases.get((table_name, legacy_id), []):
                conn.execute(
                    """INSERT OR IGNORE INTO work_aliases
                       (work_table, work_id, alias_title, normalized_alias_title, alias_language, alias_year, relationship_type)
                       VALUES ('works', ?, ?, ?, ?, ?, ?)""",
                    (int(work_id), alias[0], alias[1], alias[2], alias[3], alias[4]),
                )
            migrated_rows += 1
            if index % 5000 == 0:
                print(f"{GREEN}[migrate_to_works]   ... {table_name}: {index}/{len(rows)}{RESET}", flush=True)

    if table_exists(conn, "search_results"):
        rows = conn.execute("SELECT id, doi, openalex_id, title, year FROM search_results WHERE work_id IS NULL").fetchall()
        for row in rows:
            work_id, _ = find_work_id(
                {
                    "doi": row["doi"],
                    "openalex_id": row["openalex_id"],
                    "title": row["title"],
                    "year": row["year"],
                    "normalized_title": normalize_text(row["title"]),
                }
            )
            if work_id:
                conn.execute("UPDATE search_results SET work_id = ? WHERE id = ?", (int(work_id), int(row["id"])))

    set_meta(conn, "works_schema_version", "1")
    conn.commit()
    print(json.dumps({"status": "ok", "migrated_rows": migrated_rows, "works": len(work_cache)}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    args = parser.parse_args()
    migrate(Path(args.db_path))


if __name__ == "__main__":
    main()
