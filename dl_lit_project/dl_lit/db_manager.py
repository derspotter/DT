import sqlite3
import json
from pathlib import Path
import re
from datetime import datetime
import time
from dl_lit.utils import parse_bibtex_file_field

# ANSI escape codes for colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
CYAN = "\033[96m"
RESET = "\033[0m"

class DatabaseManager:
    """Manages all SQLite database interactions for the literature management tool."""

    def __init__(self, db_path: str | Path = "data/literature.db"):
        """Initializes the DatabaseManager, connects to the SQLite database,
        and ensures the necessary table schema is created.

        Args:
            db_path: Path to the SQLite database file. 
                     Defaults to 'data/literature.db' relative to project root.
        """
        self.is_in_memory = (str(db_path).lower() == ":memory:")

        if self.is_in_memory:
            self.db_path = ":memory:"
            print(f"{GREEN}[DB Manager] Initializing in-memory database.{RESET}")
        else:
            self.db_path = Path(db_path)
            # Ensure the parent directory for the database exists
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            print(f"{GREEN}[DB Manager] Connecting to database file: {self.db_path.resolve()}{RESET}")
        
        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,  # Shared by design in some threaded call paths.
            timeout=30.0,
        )
        self.conn.execute("PRAGMA foreign_keys = ON")
        # Improve concurrent-read/write behavior and reduce transient lock failures.
        try:
            self.conn.execute("PRAGMA journal_mode = WAL")
            self.conn.execute("PRAGMA synchronous = NORMAL")
            self.conn.execute("PRAGMA busy_timeout = 5000")
        except sqlite3.Error:
            # Best effort only (e.g., in-memory DB can reject WAL mode).
            pass
        if self.is_in_memory:
            print(f"{GREEN}[DB Manager] In-memory database connected.{RESET}")
        else:
            print(f"{GREEN}[DB Manager] Connected to database: {self.db_path.resolve()}{RESET}")
        self._create_schema()

    def close_connection(self):
        """Closes the database connection."""
        if hasattr(self, 'conn') and self.conn:
            print(f"{GREEN}[DB Manager] Closing connection to {self.db_path}{RESET}")
            self.conn.close()
            self.conn = None # Indicate that the connection is closed
        else:
            print(f"{YELLOW}[DB Manager] Connection already closed or not established for {self.db_path}.{RESET}")

    def _create_schema(self):
        """Creates the database schema, including all necessary tables if they don't exist."""
        cursor = self.conn.cursor()

        # Canonical schema bootstrap always runs first. Once works_schema_version=1 is present
        # we do not need to touch or backfill legacy stage tables on every startup.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
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
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS corpus_works (
                corpus_id INTEGER NOT NULL,
                work_id INTEGER NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (corpus_id, work_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS work_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_table TEXT NOT NULL,
                work_id INTEGER NOT NULL,
                alias_title TEXT NOT NULL,
                normalized_alias_title TEXT NOT NULL,
                alias_language TEXT,
                alias_year INTEGER,
                relationship_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_references (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bibtex_key TEXT,
                entry_type TEXT,
                title TEXT NOT NULL,
                authors TEXT,
                editors TEXT,
                year INTEGER,
                doi TEXT,
                openalex_id TEXT,
                metadata_source_type TEXT,
                original_bibtex_entry_json TEXT,
                source_bibtex_file TEXT,
                existing_entry_id INTEGER NOT NULL,
                existing_entry_table TEXT NOT NULL,
                matched_on_field TEXT NOT NULL,
                date_detected TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS merge_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_table TEXT NOT NULL,
                canonical_id INTEGER NOT NULL,
                duplicate_table TEXT NOT NULL,
                duplicate_id INTEGER,
                match_field TEXT,
                action TEXT NOT NULL,
                notes TEXT,
                updates_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS citation_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                relationship_type TEXT NOT NULL,
                source_table TEXT,
                target_table TEXT,
                source_row_id INTEGER,
                target_row_id INTEGER,
                run_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_id, target_id, relationship_type)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ingest_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ingest_source TEXT,
                source_pdf TEXT,
                title TEXT,
                authors TEXT,
                year INTEGER,
                doi TEXT,
                source TEXT,
                publisher TEXT,
                url TEXT,
                entry_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                corpus_id INTEGER,
                processed INTEGER DEFAULT 0
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ingest_source_metadata (
                corpus_id INTEGER NOT NULL DEFAULT 0,
                ingest_source TEXT NOT NULL,
                source_pdf TEXT,
                title TEXT,
                authors TEXT,
                year INTEGER,
                doi TEXT,
                source TEXT,
                publisher TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (corpus_id, ingest_source)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT NOT NULL,
                filters_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                search_run_id INTEGER NOT NULL,
                openalex_id TEXT,
                doi TEXT,
                title TEXT,
                year INTEGER,
                raw_json TEXT,
                work_id INTEGER,
                FOREIGN KEY(search_run_id) REFERENCES search_runs(id) ON DELETE CASCADE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_run_corpora (
                search_run_id INTEGER PRIMARY KEY,
                corpus_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS seed_sources_hidden (
                corpus_id INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                source_key TEXT NOT NULL,
                hidden_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (corpus_id, source_type, source_key)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS seed_candidates_dismissed (
                corpus_id INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                source_key TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                dismissed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (corpus_id, source_type, source_key, candidate_key)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daemon_heartbeat (
                daemon_name TEXT PRIMARY KEY,
                worker_id TEXT,
                status TEXT,
                details_json TEXT,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                corpus_id INTEGER,
                job_type TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                parameters_json TEXT,
                result_json TEXT,
                worker_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                finished_at TIMESTAMP
            )
        """)
        cursor.execute("PRAGMA table_info(pipeline_jobs)")
        pipeline_columns = {row[1] for row in cursor.fetchall()}
        if "worker_id" not in pipeline_columns:
            cursor.execute("ALTER TABLE pipeline_jobs ADD COLUMN worker_id TEXT")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP,
                input_path TEXT,
                parameters_json TEXT,
                status TEXT,
                notes TEXT
            )
        """)
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_works_openalex_id ON works(openalex_id) WHERE openalex_id IS NOT NULL AND openalex_id != ''")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_works_normalized_doi ON works(normalized_doi) WHERE normalized_doi IS NOT NULL AND normalized_doi != ''")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_works_title_year ON works(normalized_title, year)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_works_authors ON works(normalized_authors)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_works_metadata_status ON works(metadata_status, metadata_lease_expires_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_works_download_status ON works(download_status, download_lease_expires_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_works_origin ON works(origin_type, origin_key)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_corpus_works_corpus ON corpus_works(corpus_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_corpus_works_work ON corpus_works(work_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_work_aliases_title ON work_aliases(normalized_alias_title)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_work_aliases_work ON work_aliases(work_table, work_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_duplicate_references_existing ON duplicate_references(existing_entry_table, existing_entry_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_citation_edges_source ON citation_edges(source_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_citation_edges_target ON citation_edges(target_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ingest_entries_source ON ingest_entries(ingest_source)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_search_results_run ON search_results(search_run_id)")

        try:
            has_search_results = cursor.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'search_results' LIMIT 1"
            ).fetchone()
            if has_search_results and "work_id" not in [row[1] for row in cursor.execute("PRAGMA table_info(search_results)").fetchall()]:
                cursor.execute("ALTER TABLE search_results ADD COLUMN work_id INTEGER")
        except sqlite3.Error:
            pass

        meta_row = cursor.execute("SELECT value FROM app_meta WHERE key = 'works_schema_version'").fetchone()
        if not meta_row:
            self._set_meta("works_schema_version", "1")
            meta_row = ("1",)
        if str(meta_row[0]) == "1":
            self.conn.commit()
            print(f"{GREEN}[DB Manager] Canonical works schema verified successfully.{RESET}")
            return

    def _get_meta(self, key: str) -> str | None:
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM app_meta WHERE key = ?", (str(key),))
        row = cur.fetchone()
        return row[0] if row else None

    def _set_meta(self, key: str, value: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO app_meta(key, value, updated_at)
               VALUES(?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP""",
            (str(key), str(value)),
        )
        self.conn.commit()

    def _clean_json_text(self, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value)
        except Exception:
            return str(value)

    def _apply_work_merge_priority(self, existing: dict, incoming: dict) -> dict:
        merged = dict(existing)
        preferred_download_rank = {
            "downloaded": 3,
            "in_progress": 2,
            "queued": 1,
            "not_requested": 0,
            "failed": 0,
        }
        if preferred_download_rank.get(str(incoming.get("download_status") or ""), -1) > preferred_download_rank.get(str(existing.get("download_status") or ""), -1):
            merged["download_status"] = incoming.get("download_status")
            merged["download_source"] = incoming.get("download_source")
            merged["download_error"] = incoming.get("download_error")
            merged["downloaded_at"] = incoming.get("downloaded_at")
            merged["file_path"] = incoming.get("file_path") or existing.get("file_path")
            merged["file_checksum"] = incoming.get("file_checksum") or existing.get("file_checksum")
        metadata_rank = {"matched": 3, "in_progress": 2, "pending": 1, "failed": 0}
        if metadata_rank.get(str(incoming.get("metadata_status") or ""), -1) > metadata_rank.get(str(existing.get("metadata_status") or ""), -1):
            merged["metadata_status"] = incoming.get("metadata_status")
            merged["metadata_source"] = incoming.get("metadata_source") or existing.get("metadata_source")
            merged["metadata_error"] = incoming.get("metadata_error")
            merged["metadata_updated_at"] = incoming.get("metadata_updated_at") or existing.get("metadata_updated_at")
        for key, value in incoming.items():
            if key in {"download_status", "download_source", "download_error", "downloaded_at", "file_path", "file_checksum", "metadata_status", "metadata_source", "metadata_error", "metadata_updated_at"}:
                continue
            if merged.get(key) in (None, "", "[]", "{}") and value not in (None, "", "[]", "{}"):
                merged[key] = value
        return merged

    def _find_existing_work(self, *, doi=None, openalex_id=None, title=None, authors=None, editors=None, year=None, exclude_id: int | None = None) -> tuple[int | None, str | None]:
        cur = self.conn.cursor()
        normalized_contributors = self._normalize_contributor_fields(authors, editors)
        def _one(query, params, field):
            cur.execute(query, params)
            row = cur.fetchone()
            return (int(row[0]), field) if row else (None, None)
        normalized_doi = self._normalize_doi(doi)
        if normalized_doi:
            params = [normalized_doi]
            query = "SELECT id FROM works WHERE normalized_doi = ?"
            if exclude_id:
                query += " AND id != ?"
                params.append(int(exclude_id))
            found_id, field = _one(query, params, "normalized_doi")
            if found_id:
                return found_id, field
        normalized_openalex = self._normalize_openalex_id(openalex_id)
        if normalized_openalex:
            params = [normalized_openalex]
            query = "SELECT id FROM works WHERE openalex_id = ?"
            if exclude_id:
                query += " AND id != ?"
                params.append(int(exclude_id))
            found_id, field = _one(query, params, "openalex_id")
            if found_id:
                return found_id, field
        normalized_title = self._normalize_text(title)
        year_str = str(year).strip() if year is not None and str(year).strip() else None
        if normalized_title:
            if year_str and normalized_contributors:
                params = [normalized_title, normalized_contributors, year_str]
                query = "SELECT id FROM works WHERE normalized_title = ? AND normalized_authors = ? AND CAST(year AS TEXT) = ?"
                if exclude_id:
                    query += " AND id != ?"
                    params.append(int(exclude_id))
                found_id, field = _one(query, params, "title_authors_year")
                if found_id:
                    return found_id, field
            if normalized_contributors:
                params = [normalized_title, normalized_contributors]
                query = "SELECT id FROM works WHERE normalized_title = ? AND normalized_authors = ?"
                if exclude_id:
                    query += " AND id != ?"
                    params.append(int(exclude_id))
                found_id, field = _one(query, params, "title_authors")
                if found_id:
                    return found_id, field
            if year_str:
                params = [normalized_title, year_str]
                query = "SELECT id FROM works WHERE normalized_title = ? AND CAST(year AS TEXT) = ?"
                if exclude_id:
                    query += " AND id != ?"
                    params.append(int(exclude_id))
                found_id, field = _one(query, params, "title_year")
                if found_id:
                    return found_id, field
            # alias fallback
            params = [normalized_title]
            query = "SELECT work_id FROM work_aliases WHERE work_table = 'works' AND normalized_alias_title = ?"
            if year_str and year_str.isdigit():
                query += " AND (alias_year IS NULL OR alias_year BETWEEN ? AND ?)"
                params.extend([int(year_str) - 1, int(year_str) + 1])
            cur.execute(query, params)
            row = cur.fetchone()
            if row and (exclude_id is None or int(row[0]) != int(exclude_id)):
                return int(row[0]), "alias_title"
        return None, None

    def _insert_work_record(self, data: dict) -> int:
        payload = {k: v for k, v in data.items() if v is not None}
        cols = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        cur = self.conn.cursor()
        cur.execute(f"INSERT INTO works ({cols}) VALUES ({placeholders})", tuple(payload.values()))
        return int(cur.lastrowid)

    def _update_work_record(self, work_id: int, data: dict) -> None:
        payload = {k: v for k, v in data.items() if k != "id"}
        if not payload:
            return
        assignments = ", ".join([f"{k} = ?" for k in payload.keys()]) + ", updated_at = CURRENT_TIMESTAMP"
        self.conn.cursor().execute(f"UPDATE works SET {assignments} WHERE id = ?", tuple(payload.values()) + (int(work_id),))

    def _resolve_or_create_work(self, data: dict, *, allow_merge: bool = True, commit: bool = True) -> tuple[int, str | None]:
        existing_id, matched_field = self._find_existing_work(
            doi=data.get("doi"),
            openalex_id=data.get("openalex_id"),
            title=data.get("title"),
            authors=data.get("authors"),
            editors=data.get("editors"),
            year=data.get("year"),
        )
        if existing_id:
            existing = self.get_entry_by_id("works", existing_id) or {}
            merged = self._apply_work_merge_priority(existing, data) if allow_merge else existing
            self._update_work_record(existing_id, merged)
            if commit:
                self.conn.commit()
            return existing_id, matched_field
        new_id = self._insert_work_record(data)
        if commit:
            self.conn.commit()
        return new_id, None

    def _resolve_work_id_from_legacy_pointer(self, table_name: str, row_id: int | None) -> int | None:
        if row_id is None:
            return None
        try:
            row_id = int(row_id)
        except (TypeError, ValueError):
            return None
        if table_name == "works":
            return row_id
        origin_key = f"{table_name}:{row_id}"
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM works WHERE origin_key = ? LIMIT 1", (origin_key,))
        row = cur.fetchone()
        if row:
            return int(row[0])
        return None

    def _serialize_name_list(self, value):
        names = self._parse_name_list(value)
        return json.dumps(names) if names else None

    def _build_raw_work_payload(self, ref: dict) -> dict:
        authors = self._parse_name_list(ref.get("authors"))
        editors = self._parse_name_list(ref.get("editors"))
        doi = ref.get("doi")
        title = ref.get("title") or "[Untitled]"
        openalex_id = self._normalize_openalex_id(ref.get("openalex_id"))
        return {
            "title": title,
            "authors": json.dumps(authors) if authors else None,
            "editors": json.dumps(editors) if editors else None,
            "year": ref.get("year"),
            "doi": doi,
            "normalized_doi": self._normalize_doi(doi),
            "openalex_id": openalex_id,
            "semantic_scholar_id": ref.get("semantic_scholar_id"),
            "source": ref.get("source"),
            "publisher": ref.get("publisher"),
            "volume": ref.get("volume"),
            "issue": ref.get("issue"),
            "pages": ref.get("pages"),
            "type": ref.get("type") or ref.get("entry_type"),
            "url": ref.get("url") or ref.get("url_source"),
            "isbn": ref.get("isbn"),
            "issn": ref.get("issn"),
            "abstract": ref.get("abstract"),
            "keywords": json.dumps(ref.get("keywords")) if ref.get("keywords") is not None and not isinstance(ref.get("keywords"), str) else ref.get("keywords"),
            "normalized_title": self._normalize_text(title),
            "normalized_authors": self._normalize_contributor_fields(authors, editors),
            "metadata_status": "pending",
            "download_status": "not_requested",
            "origin_type": ref.get("origin_type") or "promotion",
            "origin_key": ref.get("origin_key") or ref.get("candidate_key") or ref.get("ingest_source") or (f"search:{ref.get('run_id')}" if ref.get("run_id") else None),
            "source_pdf": ref.get("source_pdf"),
            "run_id": ref.get("run_id"),
            "source_work_id": ref.get("source_work_id"),
            "relationship_type": ref.get("relationship_type"),
            "crossref_json": self._clean_json_text(ref.get("crossref_json")),
            "openalex_json": self._clean_json_text(ref.get("openalex_json")),
            "bibtex_entry_json": self._clean_json_text(ref.get("bibtex_entry_json")),
            "status_notes": ref.get("status_notes"),
        }

    def mark_metadata_failed(self, work_id: int, reason: str) -> tuple[int | None, str | None]:
        """Mark a canonical work as failed during metadata enrichment."""
        cur = self.conn.cursor()
        try:
            cur.execute(
                """UPDATE works
                   SET metadata_status = 'failed',
                       metadata_error = ?,
                       metadata_claimed_by = NULL,
                       metadata_claimed_at = NULL,
                       metadata_lease_expires_at = NULL
                 WHERE id = ?""",
                (reason, int(work_id)),
            )
            self.conn.commit()
            return int(work_id), None
        except sqlite3.Error as e:
            self.conn.rollback()
            return None, str(e)

    def _clean_string_value(self, value: str | None) -> str | None:
        """Strips leading/trailing curly braces from a string value."""
        if isinstance(value, str):
            return value.strip('{}')
        return value

    def _normalize_doi(self, doi: str | None) -> str | None:
        """Normalizes a DOI by extracting the core identifier from a URL if present."""
        if not doi:
            return None
        # This regex is designed to find a DOI pattern, which typically starts with '10.'.
        # It handles cases with a 'https://doi.org/' prefix or other URL variations.
        match = re.search(r'(10\.\d{4,9}/[-._;()/:A-Z0-9]+)', doi, re.IGNORECASE)
        if match:
            # Return the matched DOI, standardized to uppercase for consistency.
            normalized = match.group(1).upper()
            return normalized
        # If no URL-like structure is found, assume it might be a bare DOI already.
        # We still standardize it to uppercase.
        if doi: # Ensure doi is not None before calling upper()
            normalized = doi.upper() # Ensure even bare DOIs are uppercased for consistency
            return normalized
        return None

    def _normalize_openalex_id(self, openalex_id: str | None) -> str | None:
        """Normalizes an OpenAlex ID by extracting the core identifier (e.g., W123456789)."""
        if not openalex_id:
            return None
        
        # Regex to find 'W' followed by digits, possibly at the end of a URL
        # Example: https://openalex.org/W123456789 -> W123456789
        # Example: W123456789 -> W123456789
        match = re.search(r'(W\d+)', openalex_id, re.IGNORECASE)
        if match:
            normalized = match.group(1).upper() # Ensure 'W' is uppercase
            return normalized
        
        return None

    def _make_node_id(self, openalex_id: str | None, doi: str | None, table: str | None, row_id: int | None) -> str | None:
        normalized_openalex = self._normalize_openalex_id(openalex_id)
        if normalized_openalex:
            return normalized_openalex
        normalized_doi = self._normalize_doi(doi)
        if normalized_doi:
            return normalized_doi
        if table and row_id:
            return f"{table}:{row_id}"
        return None

    def _insert_citation_edge(
        self,
        source_id: str | None,
        target_id: str | None,
        relationship_type: str,
        *,
        source_table: str | None = None,
        target_table: str | None = None,
        source_row_id: int | None = None,
        target_row_id: int | None = None,
        run_id: int | None = None,
    ) -> bool:
        if not source_id or not target_id or source_id == target_id:
            return False
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO citation_edges
                (source_id, target_id, relationship_type, source_table, target_table, source_row_id, target_row_id, run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (source_id, target_id, relationship_type, source_table, target_table, source_row_id, target_row_id, run_id),
            )
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            print(f"{YELLOW}[DB Manager] Warning: Failed to insert citation edge: {e}{RESET}")
            return False

    def _normalize_text(self, text: str | None) -> str | None:
        """Basic normalization for text fields like title and authors."""
        if not text:
            return None
        # Lowercase, remove non-alphanumeric characters (except spaces)
        return re.sub(r'[^a-z0-9\s]', '', text.lower()).strip()

    def _normalize_authors_value(self, authors: list | str | None) -> str | None:
        """Normalize authors input (list or string) into a comparable text token."""
        if not authors:
            return None
        if isinstance(authors, list):
            joined = " ".join([str(a) for a in authors if a])
        else:
            joined = str(authors)
        return self._normalize_text(joined)

    def _parse_name_list(self, value: list | str | None) -> list[str]:
        """Parse a list-like field (authors/editors) into a clean list of names."""
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

    def _normalize_contributor_fields(
        self,
        authors: list | str | None = None,
        editors: list | str | None = None,
    ) -> str | None:
        """Normalize contributors for duplicate matching; fall back to editors when authors are empty."""
        author_list = self._parse_name_list(authors)
        if author_list:
            return self._normalize_authors_value(author_list)
        editor_list = self._parse_name_list(editors)
        if editor_list:
            return self._normalize_authors_value(editor_list)
        return None

    def add_pending_works_from_json(
        self,
        json_file_path: str | Path,
        source_pdf: str | None = None,
        *,
        searcher=None,
        rate_limiter=None,
        resolve_potential_duplicates: bool = False
    ) -> tuple[int, int, list]:
        """
        Adds entries from a JSON file (from API extraction) as pending works.

        Args:
            json_file_path: Path to the JSON file containing a list of reference dicts.
            source_pdf: The original PDF file this JSON was extracted from.

        Returns:
            A tuple (added_count, skipped_count, errors_list).
        """
        json_path = Path(json_file_path)
        if not json_path.exists():
            return 0, 0, [f"JSON file not found: {json_path}"]

        with open(json_path, 'r', encoding='utf-8') as f:
            try:
                entries = json.load(f)
            except json.JSONDecodeError as e:
                return 0, 0, [f"Failed to parse JSON file: {e}"]

        if not isinstance(entries, list):
            return 0, 0, ["JSON content is not a list of entries."]

        added_count = 0
        skipped_count = 0
        errors = []
        for entry in entries:
            entry = dict(entry)
            if source_pdf:
                entry["source_pdf"] = source_pdf

            row_id, err = self.create_pending_work(
                entry,
                searcher=searcher,
                rate_limiter=rate_limiter,
                resolve_potential_duplicates=resolve_potential_duplicates,
            )
            if row_id:
                added_count += 1
            else:
                skipped_count += 1
                if err and "Duplicate already exists" not in err and "Title is required" not in err:
                    errors.append(err)

        return added_count, skipped_count, errors

    def load_from_disk(self, disk_db_path: Path) -> bool:
        """Loads data from an on-disk SQLite database into the current in-memory database.

        This is intended to be used when self.conn is an in-memory database.
        It effectively clones the content of the disk_db_path into memory.

        Args:
            disk_db_path: Path to the on-disk SQLite database file to load from.

        Returns:
            True if loading was successful or if there was nothing to load (e.g., disk_db_path doesn't exist),
            False if an error occurred during loading.
        """
        if not self.is_in_memory:
            print(f"{RED}[DB Manager] Error: load_from_disk() called on a non-in-memory database instance. Aborting load.{RESET}")
            return False

        if not disk_db_path.exists() or disk_db_path.stat().st_size == 0:
            print(f"{YELLOW}[DB Manager] Disk database at {disk_db_path} does not exist or is empty. Nothing to load.{RESET}")
            return True # Not an error, just nothing to load

        source_disk_conn = None
        try:
            print(f"{GREEN}[DB Manager] Attempting to load data from disk database: {disk_db_path} into in-memory DB: {self.db_path}{RESET}")
            source_disk_conn = sqlite3.connect(disk_db_path)
            
            # Backup from the source_disk_conn (on-disk) to self.conn (in-memory)
            # The backup API will overwrite the destination (self.conn)
            source_disk_conn.backup(self.conn)
            
            print(f"{GREEN}[DB Manager] Successfully loaded data from {disk_db_path} into in-memory database.{RESET}")
            # After loading, ensure the schema is what we expect (e.g., if disk DB was old/different schema)
            # This also re-applies PRAGMA foreign_keys = ON if the backup didn't preserve it for the connection.
            self.conn.execute("PRAGMA foreign_keys = ON")
            self._create_schema() # This ensures tables exist with IF NOT EXISTS, and indices are correct.
            return True
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] SQLite error during load_from_disk (backup method): {e}{RESET}")
            return False
        finally:
            if source_disk_conn:
                source_disk_conn.close()

    def check_if_exists(
        self,
        doi: str | None,
        openalex_id: str | None,
        title: str | None = None,
        authors: list | str | None = None,
        editors: list | str | None = None,
        year: int | str | None = None,
        exclude_id: int | None = None,
        exclude_table: str | None = None
    ) -> tuple[str | None, int | None, str | None]:
        """Checks if an entry already exists in the canonical works table."""
        effective_exclude_id = exclude_id if exclude_table == "works" else None
        work_id, field = self._find_existing_work(
            doi=doi,
            openalex_id=openalex_id,
            title=title,
            authors=authors,
            editors=editors,
            year=year,
            exclude_id=effective_exclude_id,
        )
        if work_id:
            return "works", int(work_id), field
        return None, None, None

    def add_work_alias(
        self,
        work_table: str,
        work_id: int,
        alias_title: str | None,
        alias_year: int | str | None = None,
        alias_language: str | None = None,
        relationship_type: str | None = None,
    ) -> tuple[int | None, str | None]:
        """Add an alternate title/translation for a work."""
        if not alias_title:
            return None, "Alias title is required"

        normalized_alias = self._normalize_text(alias_title)
        cur = self.conn.cursor()
        try:
            cur.execute(
                """INSERT OR IGNORE INTO work_aliases
                   (work_table, work_id, alias_title, normalized_alias_title, alias_language, alias_year, relationship_type)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    work_table,
                    work_id,
                    alias_title,
                    normalized_alias,
                    alias_language,
                    int(alias_year) if isinstance(alias_year, str) and alias_year.isdigit() else alias_year,
                    relationship_type,
                ),
            )
            self.conn.commit()
            return cur.lastrowid, None
        except sqlite3.Error as e:
            self.conn.rollback()
            return None, str(e)

    def update_work_alias_owner(self, old_table: str, old_id: int, new_table: str, new_id: int) -> None:
        """Move aliases from one work record to another."""
        cur = self.conn.cursor()
        try:
            cur.execute(
                "UPDATE work_aliases SET work_table = ?, work_id = ? WHERE work_table = ? AND work_id = ?",
                (new_table, new_id, old_table, old_id),
            )
            self.conn.commit()
        except sqlite3.Error:
            self.conn.rollback()

    def delete_work_aliases(self, work_table: str, work_id: int) -> None:
        """Remove aliases for a work record."""
        cur = self.conn.cursor()
        try:
            cur.execute(
                "DELETE FROM work_aliases WHERE work_table = ? AND work_id = ?",
                (work_table, work_id),
            )
            self.conn.commit()
        except sqlite3.Error:
            self.conn.rollback()

    def _log_merge(
        self,
        canonical_table: str,
        canonical_id: int,
        duplicate_table: str,
        duplicate_id: int | None,
        match_field: str | None,
        action: str,
        notes: str | None = None,
        updates: dict | None = None,
    ) -> None:
        """Record a dedupe/merge decision."""
        cur = self.conn.cursor()
        try:
            cur.execute(
                """INSERT INTO merge_log
                   (canonical_table, canonical_id, duplicate_table, duplicate_id, match_field, action, notes, updates_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    canonical_table,
                    canonical_id,
                    duplicate_table,
                    duplicate_id,
                    match_field,
                    action,
                    notes,
                    json.dumps(updates) if updates else None,
                ),
            )
            self.conn.commit()
        except sqlite3.Error:
            self.conn.rollback()

    def _fetch_record_dict(self, table: str, record_id: int) -> dict | None:
        """Fetch a record by table + id as a dict."""
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {table} WHERE id = ?", (record_id,))
        row = cur.fetchone()
        if not row:
            return None
        columns = [d[0] for d in cur.description]
        return dict(zip(columns, row))

    def add_corpus_item(self, corpus_id: int, table_name: str, row_id: int) -> None:
        """Associate a work with a corpus using the canonical join table."""
        if not corpus_id or not row_id:
            return
        work_id = None
        if table_name == "works":
            work_id = int(row_id)
        else:
            try:
                work_id = self._resolve_work_id_from_legacy_pointer(table_name, int(row_id))
            except Exception:
                work_id = None
        if not work_id:
            return
        cur = self.conn.cursor()
        try:
            cur.execute(
                """INSERT OR IGNORE INTO corpus_works (corpus_id, work_id, added_at)
                   VALUES (?, ?, CURRENT_TIMESTAMP)""",
                (int(corpus_id), int(work_id)),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"{YELLOW}[DB Manager] Warning: failed to add corpus work: {e}{RESET}")
            self.conn.rollback()

    def _get_corpus_ids_for_item(self, table_name: str, row_id: int) -> list[int]:
        """Fetch all corpus IDs that include the given work."""
        work_id = None
        if table_name == "works":
            work_id = int(row_id)
        else:
            work_id = self._resolve_work_id_from_legacy_pointer(table_name, int(row_id))
        if not work_id:
            return []
        cur = self.conn.cursor()
        cur.execute("SELECT corpus_id FROM corpus_works WHERE work_id = ?", (int(work_id),))
        return [int(row[0]) for row in cur.fetchall()]

    def reassign_corpus_membership(
        self,
        old_table: str,
        old_row_id: int,
        new_table: str,
        new_row_id: int,
    ) -> None:
        """Compatibility no-op in the unified work model."""
        return None

    def _prepare_resolution_ref(self, ref: dict) -> dict:
        """Build a minimal ref dict for OpenAlex/Crossref resolution."""
        authors_raw = ref.get("authors") or []
        if isinstance(authors_raw, str):
            authors_str = authors_raw.strip()
            if authors_str.startswith("[") and authors_str.endswith("]"):
                try:
                    parsed = json.loads(authors_str)
                    if isinstance(parsed, list):
                        authors_raw = parsed
                except json.JSONDecodeError:
                    authors_raw = [authors_raw]
            else:
                authors_raw = [authors_raw]

        return {
            "title": ref.get("title"),
            "authors": authors_raw,
            "year": ref.get("year"),
            "doi": ref.get("doi"),
            "container-title": ref.get("source") or ref.get("container-title") or ref.get("journal_conference"),
            "source": ref.get("source") or ref.get("journal_conference"),
            "volume": ref.get("volume"),
            "issue": ref.get("issue"),
            "pages": ref.get("pages"),
            "abstract": ref.get("abstract"),
            "keywords": ref.get("keywords"),
        }

    def _resolve_identifiers_for_ref(self, ref: dict, searcher, rate_limiter) -> dict:
        """Resolve DOI/OpenAlex ID for a ref using OpenAlex/Crossref."""
        if not searcher:
            return {}
        try:
            from .OpenAlexScraper import process_single_reference
        except Exception:
            return {}

        ref_for_scraper = self._prepare_resolution_ref(ref)
        resolved = process_single_reference(
            ref_for_scraper,
            searcher,
            rate_limiter,
            fetch_references=False,
            fetch_citations=False,
            max_citations=0,
        )
        if not resolved:
            return {}

        return {
            "doi": self._normalize_doi(resolved.get("doi")),
            "openalex_id": self._normalize_openalex_id(resolved.get("openalex_id")),
        }

    def _merge_records(
        self,
        canonical_table: str,
        canonical_id: int,
        duplicate_table: str,
        duplicate_id: int,
        match_field: str | None,
        notes: str | None = None,
    ) -> tuple[bool, str | None]:
        """Merge duplicate into canonical record and log the action."""
        cur = self.conn.cursor()
        canonical = self._fetch_record_dict(canonical_table, canonical_id)
        duplicate = self._fetch_record_dict(duplicate_table, duplicate_id)
        if not canonical or not duplicate:
            return False, "Record not found for merge"

        try:
            cur.execute("BEGIN")

            # Determine safe columns to backfill.
            cur.execute(f"PRAGMA table_info({canonical_table})")
            canonical_cols = {row[1] for row in cur.fetchall()}
            cur.execute(f"PRAGMA table_info({duplicate_table})")
            duplicate_cols = {row[1] for row in cur.fetchall()}

            merge_fields = [
                "doi",
                "normalized_doi",
                "openalex_id",
                "year",
                "source",
                "journal_conference",
                "volume",
                "issue",
                "pages",
                "publisher",
                "type",
                "url",
                "isbn",
                "issn",
                "abstract",
                "keywords",
                "authors",
                "editors",
                "normalized_title",
                "normalized_authors",
                "source_pdf",
                "source_work_id",
                "relationship_type",
                "ingest_source",
                "run_id",
            ]

            updates: dict[str, object] = {}
            for field in merge_fields:
                if field not in canonical_cols or field not in duplicate_cols:
                    continue
                canon_val = canonical.get(field)
                dup_val = duplicate.get(field)
                if (canon_val is None or canon_val == "") and dup_val not in (None, ""):
                    updates[field] = dup_val

            # Maintain normalized fields when updating DOI/title/authors.
            if "doi" in updates and "normalized_doi" in canonical_cols and not updates.get("normalized_doi"):
                updates["normalized_doi"] = self._normalize_doi(updates.get("doi"))
            if "title" in updates and "normalized_title" in canonical_cols:
                updates["normalized_title"] = self._normalize_text(updates.get("title"))
            if "normalized_authors" in canonical_cols and ("authors" in updates or "editors" in updates):
                updates["normalized_authors"] = self._normalize_contributor_fields(
                    updates.get("authors", canonical.get("authors")),
                    updates.get("editors", canonical.get("editors")),
                )

            if updates:
                set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
                cur.execute(
                    f"UPDATE {canonical_table} SET {set_clause} WHERE id = ?",
                    list(updates.values()) + [canonical_id],
                )

            # Move aliases to canonical record
            cur.execute(
                "UPDATE work_aliases SET work_table = ?, work_id = ? WHERE work_table = ? AND work_id = ?",
                (canonical_table, canonical_id, duplicate_table, duplicate_id),
            )

            # Add duplicate title as alias if different
            duplicate_title = duplicate.get("title")
            if duplicate_title:
                canonical_title = canonical.get("title")
                if self._normalize_text(duplicate_title) != self._normalize_text(canonical_title):
                    cur.execute(
                        """INSERT OR IGNORE INTO work_aliases
                           (work_table, work_id, alias_title, normalized_alias_title, alias_year, relationship_type)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            canonical_table,
                            canonical_id,
                            duplicate_title,
                            self._normalize_text(duplicate_title),
                            duplicate.get("year"),
                            "alias",
                        ),
                    )

            if canonical_table == "works" and duplicate_table == "works":
                cur.execute(
                    """INSERT OR IGNORE INTO corpus_works (corpus_id, work_id, added_at)
                       SELECT corpus_id, ?, CURRENT_TIMESTAMP
                         FROM corpus_works
                        WHERE work_id = ?""",
                    (canonical_id, duplicate_id),
                )
                cur.execute("DELETE FROM corpus_works WHERE work_id = ?", (duplicate_id,))

            # Delete duplicate row
            cur.execute(f"DELETE FROM {duplicate_table} WHERE id = ?", (duplicate_id,))

            cur.execute("COMMIT")
            self._log_merge(
                canonical_table=canonical_table,
                canonical_id=canonical_id,
                duplicate_table=duplicate_table,
                duplicate_id=duplicate_id,
                match_field=match_field,
                action="merged",
                notes=notes,
                updates=updates,
            )
            return True, None
        except sqlite3.Error as e:
            cur.execute("ROLLBACK")
            self._log_merge(
                canonical_table=canonical_table,
                canonical_id=canonical_id,
                duplicate_table=duplicate_table,
                duplicate_id=duplicate_id,
                match_field=match_field,
                action="merge_failed",
                notes=str(e),
                updates=None,
            )
            return False, str(e)

    def _merge_incoming_into_existing(
        self,
        existing_table: str,
        existing_id: int,
        incoming_ref: dict,
        match_field: str | None,
        notes: str | None = None,
        action: str = "skipped_duplicate",
    ) -> tuple[bool, str | None]:
        """Merge incoming (not yet inserted) data into an existing record."""
        cur = self.conn.cursor()
        existing = self._fetch_record_dict(existing_table, existing_id)
        if not existing:
            return False, "Existing record not found"

        try:
            cur.execute("BEGIN")

            cur.execute(f"PRAGMA table_info({existing_table})")
            existing_cols = {row[1] for row in cur.fetchall()}

            merge_fields = [
                "doi",
                "normalized_doi",
                "openalex_id",
                "year",
                "source",
                "journal_conference",
                "volume",
                "issue",
                "pages",
                "publisher",
                "type",
                "url",
                "isbn",
                "issn",
                "abstract",
                "keywords",
                "authors",
                "editors",
                "normalized_title",
                "normalized_authors",
                "source_pdf",
                "source_work_id",
                "relationship_type",
                "ingest_source",
                "run_id",
            ]

            updates: dict[str, object] = {}
            for field in merge_fields:
                if field not in existing_cols:
                    continue
                canon_val = existing.get(field)
                incoming_val = incoming_ref.get(field)
                if (canon_val is None or canon_val == "") and incoming_val not in (None, ""):
                    updates[field] = incoming_val

            if "doi" in updates and "normalized_doi" in existing_cols and not updates.get("normalized_doi"):
                updates["normalized_doi"] = self._normalize_doi(updates.get("doi"))
            if "title" in updates and "normalized_title" in existing_cols:
                updates["normalized_title"] = self._normalize_text(updates.get("title"))
            if "normalized_authors" in existing_cols and ("authors" in updates or "editors" in updates):
                updates["normalized_authors"] = self._normalize_contributor_fields(
                    updates.get("authors", existing.get("authors")),
                    updates.get("editors", existing.get("editors")),
                )

            if updates:
                set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
                cur.execute(
                    f"UPDATE {existing_table} SET {set_clause} WHERE id = ?",
                    list(updates.values()) + [existing_id],
                )

            # Add incoming title as alias if it differs
            incoming_title = incoming_ref.get("title")
            if incoming_title and self._normalize_text(incoming_title) != self._normalize_text(existing.get("title")):
                cur.execute(
                    """INSERT OR IGNORE INTO work_aliases
                       (work_table, work_id, alias_title, normalized_alias_title, alias_year, relationship_type)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        existing_table,
                        existing_id,
                        incoming_title,
                        self._normalize_text(incoming_title),
                        incoming_ref.get("year"),
                        "alias",
                    ),
                )

            cur.execute("COMMIT")
            self._log_merge(
                canonical_table=existing_table,
                canonical_id=existing_id,
                duplicate_table="incoming",
                duplicate_id=None,
                match_field=match_field,
                action=action,
                notes=notes,
                updates=updates,
            )
            return True, None
        except sqlite3.Error as e:
            cur.execute("ROLLBACK")
            self._log_merge(
                canonical_table=existing_table,
                canonical_id=existing_id,
                duplicate_table="incoming",
                duplicate_id=None,
                match_field=match_field,
                action="merge_failed",
                notes=str(e),
                updates=None,
            )
            return False, str(e)

    # ------------------------------------------------------------------
    # Canonical works workflow helpers
    # ------------------------------------------------------------------

    def create_pending_work(
        self,
        ref: dict,
        *,
        searcher=None,
        rate_limiter=None,
        resolve_potential_duplicates: bool = False
    ) -> tuple[int | None, str | None]:
        """Insert or merge a raw reference into the canonical works table."""
        title = ref.get("title")
        if not title:
            return None, "Title is required"

        corpus_id = ref.get("corpus_id")
        doi_raw = ref.get("doi")
        norm_doi = self._normalize_doi(doi_raw)
        norm_title = self._normalize_text(title)
        authors_raw = self._parse_name_list(ref.get("authors"))
        editors_raw = self._parse_name_list(ref.get("editors"))
        norm_authors = self._normalize_contributor_fields(authors_raw, editors_raw)

        tbl, eid, field = self.check_if_exists(
            doi=norm_doi, 
            openalex_id=ref.get("openalex_id"),
            title=title,
            authors=authors_raw,
            editors=editors_raw,
            year=ref.get("year")
        )
        if tbl:
            high_confidence = field in {"normalized_doi", "openalex_id", "title_authors_year", "alias_title_year"}
            incoming_ref_for_merge = {
                **ref,
                "authors": json.dumps(authors_raw) if authors_raw else None,
                "editors": json.dumps(editors_raw) if editors_raw else None,
                "keywords": json.dumps(ref.get("keywords")) if ref.get("keywords") else None,
                "normalized_doi": norm_doi,
                "normalized_title": norm_title,
                "normalized_authors": norm_authors,
            }
            resolved_conflict = False
            # If potential duplicate and both are missing identifiers, resolve both.
            if resolve_potential_duplicates and searcher and field not in {"doi", "openalex_id"}:
                existing = self._fetch_record_dict(tbl, eid)
                incoming_missing_id = not norm_doi and not self._normalize_openalex_id(ref.get("openalex_id"))
                existing_missing_id = existing and not self._normalize_doi(existing.get("doi")) and not self._normalize_openalex_id(existing.get("openalex_id"))

                if existing and incoming_missing_id and existing_missing_id:
                    incoming_resolved = self._resolve_identifiers_for_ref(ref, searcher, rate_limiter)
                    existing_resolved = self._resolve_identifiers_for_ref(existing, searcher, rate_limiter)

                    if incoming_resolved and existing_resolved:
                        if (
                            incoming_resolved.get("doi")
                            and incoming_resolved.get("doi") == existing_resolved.get("doi")
                        ) or (
                            incoming_resolved.get("openalex_id")
                            and incoming_resolved.get("openalex_id") == existing_resolved.get("openalex_id")
                        ):
                            # Confirmed duplicate via resolved IDs
                            if incoming_resolved.get("doi"):
                                incoming_ref_for_merge["doi"] = incoming_resolved.get("doi")
                                incoming_ref_for_merge["normalized_doi"] = self._normalize_doi(incoming_resolved.get("doi"))
                            if incoming_resolved.get("openalex_id"):
                                incoming_ref_for_merge["openalex_id"] = incoming_resolved.get("openalex_id")
                            if corpus_id:
                                self.add_corpus_item(corpus_id, tbl, eid)
                            work_row = self.get_entry_by_id("works", int(eid)) or {}
                            merged = self._apply_work_merge_priority(work_row, self._build_raw_work_payload(ref))
                            self._update_work_record(int(eid), merged)
                            self.conn.commit()
                            return None, f"Duplicate already exists in {tbl} (ID {eid}) on {field}"
                        else:
                            # Resolved IDs disagree; keep both and log.
                            self._log_merge(
                                canonical_table=tbl,
                                canonical_id=eid,
                                duplicate_table="incoming",
                                duplicate_id=None,
                                match_field=field,
                                action="conflict",
                                notes="resolved_ids_conflict",
                                updates=None,
                            )
                            resolved_conflict = True
                    else:
                        # Resolution failed; fall through to high-confidence check.
                        pass

            if high_confidence and not resolved_conflict:
                if corpus_id:
                    self.add_corpus_item(corpus_id, tbl, eid)
                work_row = self.get_entry_by_id("works", int(eid)) or {}
                merged = self._apply_work_merge_priority(work_row, self._build_raw_work_payload(ref))
                self._update_work_record(int(eid), merged)
                self.conn.commit()
                return None, f"Duplicate already exists in {tbl} (ID {eid}) on {field}"

            # Low-confidence duplicates are logged but allowed to insert.
            self._log_merge(
                canonical_table=tbl,
                canonical_id=eid,
                duplicate_table="incoming",
                duplicate_id=None,
                match_field=field,
                action="possible_duplicate",
                notes="low_confidence_match_inserted",
                updates=None,
            )

        try:
            payload = self._build_raw_work_payload(ref)
            new_id, _ = self._resolve_or_create_work(payload, allow_merge=True)
            if corpus_id:
                self.add_corpus_item(corpus_id, "works", int(new_id))

            translated_title = ref.get("translated_title")
            if translated_title:
                self.add_work_alias(
                    work_table="works",
                    work_id=int(new_id),
                    alias_title=translated_title,
                    alias_year=ref.get("translated_year"),
                    alias_language=ref.get("translated_language"),
                    relationship_type=ref.get("translation_relationship") or "translation",
                )

            return new_id, None
            return int(new_id), None
        except sqlite3.IntegrityError as e:
            return None, f"IntegrityError inserting into works: {e}"

    def insert_ingest_entry(self, ref: dict, ingest_source: str | None = None) -> None:
        """Insert raw extracted entry for UI display, without dedupe enforcement."""
        cursor = self.conn.cursor()
        authors_raw = ref.get("authors") or ref.get("author") or []
        if isinstance(authors_raw, str):
            authors_raw = [authors_raw]
        try:
            cursor.execute(
                """INSERT INTO ingest_entries (
                    corpus_id, ingest_source, source_pdf, title, authors, year, doi, source,
                    publisher, url, entry_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ref.get("corpus_id"),
                    ingest_source,
                    ref.get("source_pdf"),
                    ref.get("title"),
                    json.dumps(authors_raw) if authors_raw else None,
                    ref.get("year"),
                    ref.get("doi"),
                    ref.get("source"),
                    ref.get("publisher"),
                    ref.get("url"),
                    json.dumps(ref, ensure_ascii=False),
                ),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"{YELLOW}[DB Manager] Warning: failed to insert ingest entry: {e}{RESET}")

    # ------------------------------------------------------------------
    # Convenience method: fetch a batch of pending works for enrichment
    # ------------------------------------------------------------------

    def fetch_matched_work_batch(self, limit: int = 50) -> list[int]:
        """Return IDs of matched works that are not yet queued for download."""
        cur = self.conn.cursor()
        cur.execute(
            """SELECT id
               FROM works
               WHERE metadata_status = 'matched'
                 AND COALESCE(download_status, 'not_requested') = 'not_requested'
               ORDER BY id
               LIMIT ?""",
            (limit,),
        )
        return [row[0] for row in cur.fetchall()]

    def fetch_pending_metadata_batch(self, limit: int = 50) -> list[dict]:
        """Return up to ``limit`` pending works for enrichment as a list of dicts."""
        cursor = self.conn.cursor()
        cursor.execute(
            """SELECT id, title, authors, editors, year, doi, source, volume, issue, pages,
                      publisher, type, url, isbn, issn, abstract, keywords, source_pdf,
                      origin_key, run_id
               FROM works
               WHERE metadata_status IN ('pending', 'in_progress')
               ORDER BY id
               LIMIT ?""",
            (limit,),
        )
        rows = cursor.fetchall()

        result: list[dict] = []
        for row in rows:
            (row_id, title, authors_json, editors_json, year, doi, source, volume, issue, pages,
             publisher, entry_type, url, isbn, issn, abstract, keywords_json, source_pdf,
             origin_key, run_id) = row
            authors: list[str] | None = None
            if authors_json:
                try:
                    authors = json.loads(authors_json)
                except json.JSONDecodeError:
                    authors = [authors_json]  # fall back to raw string
            
            editors: list[str] | None = None
            if editors_json:
                try:
                    editors = json.loads(editors_json)
                except json.JSONDecodeError:
                    editors = [editors_json]  # fall back to raw string

            keywords: list[str] | None = None
            if keywords_json:
                try:
                    keywords = json.loads(keywords_json)
                except json.JSONDecodeError:
                    keywords = [keywords_json]
            
            result.append(
                {
                    "id": row_id,
                    "title": title,
                    "authors": authors,
                    "editors": editors,
                    "year": year,
                    "doi": doi,
                    "source": source,
                    "volume": volume,
                    "issue": issue,
                    "pages": pages,
                    "publisher": publisher,
                    "type": entry_type,
                    "url": url,
                    "isbn": isbn,
                    "issn": issn,
                    "abstract": abstract,
                    "keywords": keywords,
                    "source_pdf": source_pdf,
                    "ingest_source": origin_key,
                    "run_id": run_id,
                }
            )
        return result

    def apply_enriched_metadata(
        self,
        work_id: int,
        enrichment: dict,
        *,
        expand_related: bool = False,
        max_related_per_source: int = 40,
    ) -> tuple[int | None, str | None]:
        """Update a pending work with enriched metadata."""
        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            base = self.get_entry_by_id("works", work_id)
            if not base:
                cur.execute("ROLLBACK")
                return None, "Row not found in works"

            authors = enrichment.get("authors")
            if authors is not None and not isinstance(authors, str):
                authors = json.dumps(authors)
            editors = enrichment.get("editors")
            if editors is not None and not isinstance(editors, str):
                editors = json.dumps(editors)
            keywords = enrichment.get("keywords")
            if keywords is not None and not isinstance(keywords, str):
                keywords = json.dumps(keywords)
            merged = {
                **base,
                **{k: v for k, v in enrichment.items() if v is not None},
                "authors": authors if authors is not None else base.get("authors"),
                "editors": editors if editors is not None else base.get("editors"),
                "keywords": keywords if keywords is not None else base.get("keywords"),
                "doi": enrichment.get("doi") or base.get("doi"),
                "normalized_doi": self._normalize_doi(enrichment.get("doi") or base.get("doi")),
                "openalex_id": self._normalize_openalex_id(enrichment.get("openalex_id") or enrichment.get("id") or base.get("openalex_id")),
                "normalized_title": self._normalize_text(enrichment.get("title") or base.get("title")),
                "normalized_authors": self._normalize_contributor_fields(authors if authors is not None else base.get("authors"), editors if editors is not None else base.get("editors")),
                "metadata_status": "matched",
                "metadata_source": enrichment.get("metadata_source_type") or base.get("metadata_source") or "enrichment",
                "metadata_error": None,
                "metadata_updated_at": datetime.now().isoformat(),
                "download_status": "not_requested" if str(base.get("download_status") or "not_requested") == "not_requested" else base.get("download_status"),
                "crossref_json": self._clean_json_text(enrichment.get("crossref_json")) or base.get("crossref_json"),
                "openalex_json": self._clean_json_text(enrichment.get("openalex_json") or enrichment),
                "bibtex_entry_json": self._clean_json_text(enrichment.get("bibtex_entry_json")) or base.get("bibtex_entry_json"),
            }

            existing_id, field = self._find_existing_work(
                doi=merged.get("doi"),
                openalex_id=merged.get("openalex_id"),
                title=merged.get("title"),
                authors=merged.get("authors"),
                editors=merged.get("editors"),
                year=merged.get("year"),
                exclude_id=int(work_id),
            )
            if existing_id:
                existing = self.get_entry_by_id("works", int(existing_id)) or {}
                self._update_work_record(int(existing_id), self._apply_work_merge_priority(existing, merged))
                self.add_work_alias("works", int(existing_id), base.get("title"), alias_year=base.get("year"))
                self.conn.execute("DELETE FROM works WHERE id = ?", (int(work_id),))
                cur.execute("COMMIT")
                marker = f"DUPLICATE_MERGED|table=works|id={existing_id}|field={field}|queued=0|queue_id=|queue_error="
                return None, f"Entry already exists in works (ID {existing_id}) on {field} [{marker}]"

            self._update_work_record(int(work_id), merged)
            cur.execute("COMMIT")
            return int(work_id), None
        except sqlite3.Error as e:
            cur.execute("ROLLBACK")
            return None, f"A database error occurred during promotion: {e}"

    def enqueue_for_download(self, with_meta_id: int) -> tuple[int | None, str | None]:
        """Mark a canonical work row as queued for download."""
        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute("SELECT id FROM works WHERE id = ?", (with_meta_id,))
            if not cur.fetchone():
                cur.execute("ROLLBACK")
                return None, "Row not found in works"

            cur.execute(
                """UPDATE works
                   SET download_status = 'queued',
                       download_claimed_by = NULL,
                       download_claimed_at = NULL,
                       download_lease_expires_at = NULL,
                       download_error = NULL
                   WHERE id = ?""",
                (with_meta_id,),
            )
            cur.execute("COMMIT")
            return with_meta_id, None
        except sqlite3.Error as e:
            cur.execute("ROLLBACK")
            return None, f"Transaction failed: {e}"

    def queue_for_enrichment(self, no_meta_id: int) -> tuple[int | None, str | None]:
        """Mark a work row as pending enrichment and clear any stale claim."""
        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute("SELECT id FROM works WHERE id = ?", (no_meta_id,))
            if not cur.fetchone():
                cur.execute("ROLLBACK")
                return None, "Row not found in works"

            cur.execute(
                """UPDATE works
                      SET metadata_status = 'pending',
                          metadata_claimed_by = NULL,
                          metadata_claimed_at = NULL,
                          metadata_lease_expires_at = NULL,
                          metadata_error = NULL
                    WHERE id = ?""",
                (no_meta_id,),
            )
            cur.execute("COMMIT")
            return int(no_meta_id), None
        except sqlite3.Error as e:
            cur.execute("ROLLBACK")
            return None, f"Transaction failed: {e}"

    def claim_enrich_batch(
        self,
        *,
        limit: int,
        corpus_id: int | None,
        claimed_by: str,
        lease_seconds: int = 15 * 60,
        max_attempts: int | None = None,
        target_ids: list[int] | None = None,
    ) -> list[dict]:
        """Atomically claim pending work rows for enrichment processing."""
        if limit <= 0:
            return []

        target_ids = [int(value) for value in (target_ids or []) if int(value) > 0]
        now = int(time.time())
        lease_expires_at = now + int(max(1, lease_seconds))
        eligible_state_sql = """
            (
              COALESCE(metadata_status, 'pending') = 'pending'
              OR (
                   metadata_status = 'in_progress'
                   AND metadata_lease_expires_at IS NOT NULL
                   AND metadata_lease_expires_at <= ?
                 )
            )
        """
        params: list = [now]
        attempts_sql = ""
        if max_attempts is not None:
            attempts_sql = " AND COALESCE(metadata_attempt_count, 0) < ?"
            params.append(int(max_attempts))

        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            where_clauses = [eligible_state_sql]
            query_params: list = list(params)

            if corpus_id is not None:
                where_clauses.append(
                    "EXISTS (SELECT 1 FROM corpus_works cw WHERE cw.work_id = nm.id AND cw.corpus_id = ?)"
                )
                query_params.append(int(corpus_id))

            if target_ids:
                placeholders = ",".join("?" for _ in target_ids)
                where_clauses.append(f"nm.id IN ({placeholders})")
                query_params.extend(target_ids)

            where_sql = " AND ".join(where_clauses)
            cur.execute(
                f"""
                SELECT nm.id
                  FROM works nm
                 WHERE {where_sql}
                   {attempts_sql}
                 ORDER BY nm.id ASC
                 LIMIT ?
                """,
                query_params + [int(limit)],
            )
            ids = [int(r[0]) for r in cur.fetchall()]
            if not ids:
                self.conn.commit()
                return []

            placeholders = ",".join(["?"] * len(ids))
            claim_sql = f"""
                UPDATE works
                   SET metadata_status = 'in_progress',
                       metadata_claimed_by = ?,
                       metadata_claimed_at = ?,
                       metadata_lease_expires_at = ?,
                       metadata_attempt_count = COALESCE(metadata_attempt_count, 0) + 1,
                       metadata_last_attempt_at = ?,
                       metadata_error = NULL
                 WHERE id IN ({placeholders})
                   AND {eligible_state_sql}
                   {attempts_sql}
            """
            cur.execute(
                claim_sql,
                [claimed_by, now, lease_expires_at, now] + ids + params,
            )
            cur.execute(
                f"""
                SELECT id, title, authors, year, doi, source, volume, issue, pages,
                       publisher, type, url, isbn, issn, abstract, keywords
                  FROM works
                 WHERE id IN ({placeholders})
                   AND metadata_claimed_by = ?
                   AND metadata_claimed_at = ?
                 ORDER BY id ASC
                """,
                ids + [claimed_by, now],
            )
            rows = cur.fetchall()
            self.conn.commit()

            out: list[dict] = []
            for r in rows:
                out.append(
                    {
                        "id": r[0],
                        "title": r[1],
                        "authors": r[2],
                        "year": r[3],
                        "doi": r[4],
                        "source": r[5],
                        "volume": r[6],
                        "issue": r[7],
                        "pages": r[8],
                        "publisher": r[9],
                        "type": r[10],
                        "url": r[11],
                        "isbn": r[12],
                        "issn": r[13],
                        "abstract": r[14],
                        "keywords": r[15],
                    }
                )
            return out
        except sqlite3.Error:
            try:
                self.conn.rollback()
            except Exception:
                pass
            return []

    def reset_enrich_claim(self, no_meta_id: int, *, state: str = "pending", error: str | None = None, selected: int = 0) -> tuple[bool, str | None]:
        cur = self.conn.cursor()
        try:
            cur.execute(
                """UPDATE works
                      SET metadata_status = ?,
                          metadata_claimed_by = NULL,
                          metadata_claimed_at = NULL,
                          metadata_lease_expires_at = NULL,
                          metadata_error = ?
                    WHERE id = ?""",
                (state, error, no_meta_id),
            )
            self.conn.commit()
            return True, None
        except sqlite3.Error as e:
            self.conn.rollback()
            return False, str(e)

    def delete_work(self, work_id: int) -> tuple[bool, str | None]:
        cur = self.conn.cursor()
        try:
            cur.execute("DELETE FROM works WHERE id = ?", (work_id,))
            self.delete_work_aliases("works", work_id)
            self.conn.commit()
            return True, None
        except sqlite3.Error as e:
            self.conn.rollback()
            return False, str(e)

    def fetch_download_queue(self, limit: int = None) -> list[dict]:
        """Fetch queued works."""
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row
        query = """
            SELECT id, bibtex_key, entry_type, title, authors, year, doi, pmid,
                   arxiv_id, openalex_id, semantic_scholar_id, mag_id, url_source,
                   file_path, abstract, keywords, journal_conference, volume, issue,
                   pages, publisher, metadata_source_type, bibtex_entry_json,
                   crossref_json, openalex_json, status_notes, date_added,
                   date_processed, checksum_pdf
            FROM (
                SELECT id, NULL AS bibtex_key, type AS entry_type, title, authors, year, doi, NULL AS pmid,
                       NULL AS arxiv_id, openalex_id, semantic_scholar_id, NULL AS mag_id, url AS url_source,
                       file_path, abstract, keywords, source AS journal_conference, volume, issue,
                       pages, publisher, metadata_source AS metadata_source_type, bibtex_entry_json,
                       crossref_json, openalex_json, status_notes, created_at AS date_added,
                       downloaded_at AS date_processed, file_checksum AS checksum_pdf
                FROM works
                WHERE download_status = 'queued'
            )
            ORDER BY date_added ASC
        """

        if limit is not None:
            query += " LIMIT ?"
            cursor.execute(query, (limit,))
        else:
            cursor.execute(query)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def _convert_inverted_index_to_text(self, inverted_index: dict | None) -> str | None:
        """Convert OpenAlex abstract_inverted_index dict to continuous text, or return None if input is None."""
        if not inverted_index:
            return None
        
        max_index = -1
        for indices in inverted_index.values():
            if indices:
                max_index = max(max_index, *indices)
        
        if max_index == -1:
            return ""

        word_list = [''] * (max_index + 1)
        
        for word, indices in inverted_index.items():
            for index in indices:
                word_list[index] = word
                
        return ' '.join(word_list)

    def move_entry_to_downloaded(self, source_id: int, enriched_metadata: dict, file_path: str, checksum: str, source_of_download: str):
        """Mark a matched work as downloaded."""
        cursor = self.conn.cursor()
        try:
            original_entry = self.get_entry_by_id('works', source_id)
            if not original_entry:
                raise sqlite3.Error(f"No entry found in works with ID {source_id}")

            authorships = enriched_metadata.get('authorships', [])
            authors = json.dumps([author['author']['display_name'] for author in authorships]) if authorships else original_entry.get('authors')

            keywords_list = enriched_metadata.get('keywords', [])
            keywords = json.dumps([kw.get('display_name') for kw in keywords_list if kw.get('display_name')]) if keywords_list else original_entry.get('keywords')

            abstract = self._convert_inverted_index_to_text(enriched_metadata.get('abstract_inverted_index'))
            biblio = enriched_metadata.get('biblio', {})
            pages = f"{biblio.get('first_page')}--{biblio.get('last_page')}" if biblio.get('first_page') and biblio.get('last_page') else original_entry.get('pages')
            primary_location = enriched_metadata.get('primary_location', {})
            source_info = primary_location.get('source', {}) if primary_location and primary_location.get('source') else {}

            merged = {
                **original_entry,
                "title": enriched_metadata.get('title', original_entry.get('title')),
                "authors": authors,
                "year": enriched_metadata.get('publication_year', original_entry.get('year')),
                "doi": enriched_metadata.get('doi', original_entry.get('doi')),
                "normalized_doi": self._normalize_doi(enriched_metadata.get('doi', original_entry.get('doi'))),
                "openalex_id": self._normalize_openalex_id(enriched_metadata.get('id', original_entry.get('openalex_id'))),
                "url": enriched_metadata.get('url_source', original_entry.get('url')),
                "file_path": file_path,
                "abstract": abstract,
                "keywords": keywords,
                "source": source_info.get('display_name', original_entry.get('source')),
                "volume": biblio.get('volume', original_entry.get('volume')),
                "issue": biblio.get('issue', original_entry.get('issue')),
                "pages": pages,
                "publisher": source_info.get('publisher', original_entry.get('publisher')),
                "metadata_source": original_entry.get('metadata_source') or 'openalex_api',
                "bibtex_entry_json": json.dumps(enriched_metadata),
                "status_notes": 'Successfully downloaded and processed',
                "file_checksum": checksum,
                "download_status": "downloaded",
                "download_source": source_of_download,
                "download_error": None,
                "downloaded_at": datetime.now().isoformat(),
                "download_claimed_by": None,
                "download_claimed_at": None,
                "download_lease_expires_at": None,
                "normalized_title": self._normalize_text(enriched_metadata.get('title', original_entry.get('title'))),
                "normalized_authors": self._normalize_contributor_fields(authors, original_entry.get('editors')),
            }
            self._update_work_record(int(source_id), merged)
            self.conn.commit()
            return int(source_id), None
        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"{RED}[DB Manager] Error moving entry {source_id} to downloaded: {e}{RESET}")
            return None, str(e)

    def mark_raw_work_downloaded(
        self,
        source_id: int,
        download_result: dict,
        file_path: str,
        checksum: str,
        source_of_download: str,
    ) -> tuple[int | None, str | None]:
        """Mark a raw work row as downloaded after raw-download fallback."""
        cursor = self.conn.cursor()
        try:
            original_entry = self.get_entry_by_id('works', source_id)
            if not original_entry:
                raise sqlite3.Error(f"No entry found in works with ID {source_id}")

            title = download_result.get('title') or original_entry.get('title')
            authors = download_result.get('authors')
            if authors is None:
                authors = original_entry.get('authors')
            elif isinstance(authors, list):
                authors = json.dumps(authors)
            elif not isinstance(authors, str):
                authors = json.dumps([str(authors)])

            doi = download_result.get('doi') or original_entry.get('doi')
            openalex_id = download_result.get('openalex_id') or original_entry.get('openalex_id')
            url_source = download_result.get('url') or download_result.get('open_access_url') or original_entry.get('url')
            metadata_blob = json.dumps(download_result)
            normalized_title = self._normalize_text(title)
            normalized_authors = self._normalize_contributor_fields(authors, original_entry.get('editors'))

            duplicate_table, duplicate_id, duplicate_field = self.check_if_exists(
                doi=doi,
                openalex_id=openalex_id,
                title=title,
                authors=authors,
                editors=original_entry.get('editors'),
                year=download_result.get('year') or original_entry.get('year'),
                exclude_id=source_id,
                exclude_table="works",
            )
            if duplicate_table == "works" and duplicate_id is not None:
                existing = self.get_entry_by_id("works", int(duplicate_id)) or {}
                payload = {
                    **original_entry,
                    "title": title,
                    "authors": authors,
                    "year": download_result.get('year') or original_entry.get('year'),
                    "doi": doi,
                    "normalized_doi": self._normalize_doi(doi),
                    "openalex_id": self._normalize_openalex_id(openalex_id),
                    "url": url_source,
                    "file_path": file_path,
                    "abstract": download_result.get('abstract') or original_entry.get('abstract'),
                    "keywords": json.dumps(download_result.get('keywords')) if isinstance(download_result.get('keywords'), list) else (download_result.get('keywords') or original_entry.get('keywords')),
                    "source": download_result.get('source') or original_entry.get('source'),
                    "volume": download_result.get('volume') or original_entry.get('volume'),
                    "issue": download_result.get('issue') or original_entry.get('issue'),
                    "pages": download_result.get('pages') or original_entry.get('pages'),
                    "publisher": download_result.get('publisher') or original_entry.get('publisher'),
                    "metadata_source": 'raw_download_fallback',
                    "bibtex_entry_json": metadata_blob,
                    "status_notes": 'Downloaded after metadata enrichment failed',
                    "file_checksum": checksum,
                    "download_status": 'downloaded',
                    "download_source": source_of_download,
                    "download_error": None,
                    "downloaded_at": datetime.now().isoformat(),
                    "normalized_title": normalized_title,
                    "normalized_authors": normalized_authors,
                }
                self._update_work_record(int(duplicate_id), self._apply_work_merge_priority(existing, payload))
                cursor.execute("DELETE FROM works WHERE id = ?", (int(source_id),))
                self.conn.commit()
                return int(duplicate_id), None

            payload = {
                **original_entry,
                'type': download_result.get('type') or original_entry.get('type'),
                'title': title,
                'authors': authors,
                'year': download_result.get('year') or original_entry.get('year'),
                'doi': doi,
                'normalized_doi': self._normalize_doi(doi),
                'openalex_id': self._normalize_openalex_id(openalex_id),
                'url': url_source,
                'file_path': file_path,
                'abstract': download_result.get('abstract') or original_entry.get('abstract'),
                'keywords': json.dumps(download_result.get('keywords')) if isinstance(download_result.get('keywords'), list) else (download_result.get('keywords') or original_entry.get('keywords')),
                'source': download_result.get('source') or original_entry.get('source'),
                'volume': download_result.get('volume') or original_entry.get('volume'),
                'issue': download_result.get('issue') or original_entry.get('issue'),
                'pages': download_result.get('pages') or original_entry.get('pages'),
                'publisher': download_result.get('publisher') or original_entry.get('publisher'),
                'metadata_source': 'raw_download_fallback',
                'bibtex_entry_json': metadata_blob,
                'status_notes': 'Downloaded after metadata enrichment failed',
                'file_checksum': checksum,
                'download_status': 'downloaded',
                'download_source': source_of_download,
                'download_error': None,
                'downloaded_at': datetime.now().isoformat(),
                'normalized_title': normalized_title,
                'normalized_authors': normalized_authors,
            }
            self._update_work_record(int(source_id), payload)
            self.conn.commit()
            return int(source_id), None
        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"{RED}[DB Manager] Error moving raw entry {source_id} to downloaded: {e}{RESET}")
            return None, str(e)

    def move_entry_to_failed(self, source_id: int, reason: str) -> tuple[int | None, str | None]:
        """Mark a canonical work as failed during download."""
        cursor = self.conn.cursor()
        try:
            original_entry = self.get_entry_by_id('works', source_id)
            if not original_entry:
                return None, f"No entry found in works with ID {source_id}"
            original_entry['download_status'] = 'failed'
            original_entry['download_error'] = reason
            original_entry['download_claimed_by'] = None
            original_entry['download_claimed_at'] = None
            original_entry['download_lease_expires_at'] = None
            original_entry['status_notes'] = reason
            self._update_work_record(int(source_id), original_entry)
            self.conn.commit()
            print(f"{GREEN}[DB Manager] Marked work ID {source_id} as failed download.{RESET}")
            return int(source_id), None
        except sqlite3.Error as e:
            self.conn.rollback()
            print(f"{RED}[DB Manager] Error moving entry {source_id} to failed: {e}{RESET}")
            return None, str(e)

    def get_entry_by_id(self, table_name: str, entry_id: int) -> dict | None:
        """Fetches a single entry from a table by its primary key ID."""
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(f"SELECT * FROM {table_name} WHERE id = ?", (entry_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def add_bibtex_entry_to_downloaded(
        self,
        entry: object,
        pdf_base_dir: Path | str | None = None,
        input_doi: str | None = None,
        input_openalex_id: str | None = None,
        original_bibtex_entry_json: str | None = None,
        source_bibtex_file: str | None = None,
    ) -> tuple[int | None, str | None]:
        """Add a BibTeX entry as a downloaded canonical work."""
        def get_primitive_val(data_object, default=None):
            if data_object is None:
                return default
            if hasattr(data_object, 'value'):
                return str(data_object.value)
            return str(data_object)

        authors_list = []
        raw_author_data = entry.get('author')
        if raw_author_data:
            if isinstance(raw_author_data, list):
                authors_list = [get_primitive_val(author_item) for author_item in raw_author_data]
            else:
                author_string = get_primitive_val(raw_author_data)
                if author_string:
                    authors_list = [name.strip() for name in author_string.split(' and ')]
        authors_json = json.dumps([self._clean_string_value(str(author)) for author in authors_list if author]) if authors_list else None

        editors_list = []
        raw_editor_data = entry.get('editor')
        if raw_editor_data:
            if isinstance(raw_editor_data, list):
                editors_list = [get_primitive_val(editor_item) for editor_item in raw_editor_data]
            else:
                editor_string = get_primitive_val(raw_editor_data)
                if editor_string:
                    editors_list = [name.strip() for name in editor_string.split(' and ')]
        editors_json = json.dumps([self._clean_string_value(str(editor)) for editor in editors_list if editor]) if editors_list else None

        keywords_list = []
        raw_keyword_data = entry.get('keywords')
        if raw_keyword_data:
            if isinstance(raw_keyword_data, list):
                keywords_list = [get_primitive_val(kw_item) for kw_item in raw_keyword_data]
            else:
                keyword_string = get_primitive_val(raw_keyword_data)
                if keyword_string:
                    keywords_list = [kw.strip() for kw in keyword_string.split(',')]
        keywords_json = json.dumps([self._clean_string_value(str(kw)) for kw in keywords_list if kw]) if keywords_list else None

        serializable_entry_dict = {k: get_primitive_val(v) for k, v in entry.items()}
        bibtex_json = original_bibtex_entry_json or json.dumps(serializable_entry_dict)

        bibtex_key_val = self._clean_string_value(entry.key if hasattr(entry, 'key') else None)
        entry_type_val = self._clean_string_value(entry.entry_type if hasattr(entry, 'entry_type') else None)
        title_val = self._clean_string_value(get_primitive_val(entry.get('title')))
        if not title_val:
            title_val = f"[Title from Key: {bibtex_key_val}]" if bibtex_key_val else "[No Title or Key Available]"

        year_str_raw = get_primitive_val(entry.get('year'))
        year_int = None
        if year_str_raw:
            year_str_cleaned = year_str_raw.strip('{}')
            if year_str_cleaned:
                try:
                    year_int = int(year_str_cleaned)
                except ValueError:
                    year_int = None

        doi_val = self._normalize_doi(self._clean_string_value(input_doi))
        openalexid_val = self._normalize_openalex_id(self._clean_string_value(input_openalex_id))
        url_source_val = self._clean_string_value(get_primitive_val(entry.get('url')) or get_primitive_val(entry.get('link')))
        raw_file_field = get_primitive_val(entry.get('file'))
        parsed_filename = parse_bibtex_file_field(raw_file_field)
        file_path_val = None
        if parsed_filename:
            if pdf_base_dir:
                full_path = Path(pdf_base_dir) / parsed_filename
                file_path_val = str(full_path.resolve())
            else:
                file_path_val = parsed_filename

        data = {
            'title': title_val,
            'authors': authors_json,
            'editors': editors_json,
            'year': year_int,
            'doi': doi_val,
            'normalized_doi': doi_val,
            'openalex_id': openalexid_val,
            'semantic_scholar_id': self._clean_string_value(get_primitive_val(entry.get('semanticscholarid'))),
            'source': self._clean_string_value(get_primitive_val(entry.get('journal')) or get_primitive_val(entry.get('booktitle'))),
            'publisher': self._clean_string_value(get_primitive_val(entry.get('publisher'))),
            'volume': self._clean_string_value(get_primitive_val(entry.get('volume'))),
            'issue': self._clean_string_value(get_primitive_val(entry.get('number')) or get_primitive_val(entry.get('issue'))),
            'pages': self._clean_string_value(get_primitive_val(entry.get('pages'))),
            'type': entry_type_val,
            'url': url_source_val,
            'abstract': self._clean_string_value(get_primitive_val(entry.get('abstract'))),
            'keywords': keywords_json,
            'normalized_title': self._normalize_text(title_val),
            'normalized_authors': self._normalize_contributor_fields(authors_json, editors_json),
            'metadata_status': 'matched',
            'metadata_source': 'bibtex_import',
            'download_status': 'downloaded',
            'download_source': 'bibtex_import',
            'downloaded_at': datetime.now().isoformat(),
            'file_path': file_path_val,
            'origin_type': 'bibtex_import',
            'origin_key': source_bibtex_file or bibtex_key_val,
            'bibtex_entry_json': bibtex_json,
            'status_notes': 'Imported from BibTeX file',
        }
        try:
            work_id, _ = self._resolve_or_create_work(data, allow_merge=True, commit=True)
            return work_id, None
        except sqlite3.IntegrityError as e:
            return None, str(e.args[0] if e.args else str(e))
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] Error adding BibTeX entry to works: {e} (Entry details: Key='{bibtex_key_val}', Title='{title_val}'){RESET}")
            return None, str(e)
        except Exception as e:
            print(f"{RED}[DB Manager] General error while adding BibTeX entry {bibtex_key_val if bibtex_key_val else 'N/A'}: {e}{RESET}")
            return None, str(e)

    def add_entry_to_download_queue(
        self,
        parsed_data: dict,
        exclude_id: int | None = None,
        exclude_table: str | None = None,
        corpus_id: int | None = None,
    ) -> tuple[str, int | None, str, str | None]:
        """Queue an entry for download in the canonical works table."""
        if corpus_id is None:
            corpus_id = parsed_data.get("corpus_id")

        input_doi = parsed_data.get('doi')
        input_openalex_id = parsed_data.get('openalex_id')
        title = parsed_data.get('title')

        if not title:
            return "error", None, "Entry is missing a title.", None

        table_name, existing_id, matched_field = self.check_if_exists(
            doi=input_doi,
            openalex_id=input_openalex_id,
            title=title,
            authors=parsed_data.get('authors'),
            editors=parsed_data.get('editors'),
            year=parsed_data.get('year'),
            exclude_id=exclude_id,
            exclude_table=exclude_table,
        )

        if table_name and existing_id is not None:
            if table_name == 'works':
                existing_id = int(existing_id)
                existing = self.get_entry_by_id("works", existing_id) or {}
                if corpus_id:
                    self.add_corpus_item(corpus_id, 'works', existing_id)

                download_status = str(existing.get("download_status") or "not_requested").strip().lower()
                if download_status == "downloaded":
                    dup_log_id, dup_log_err = self.add_entry_to_duplicates(
                        entry=parsed_data.get('original_json_object', parsed_data),
                        input_doi=input_doi,
                        input_openalex_id=input_openalex_id,
                        source_bibtex_file=None,
                        existing_entry_id=existing_id,
                        existing_entry_table="works",
                        matched_on_field=matched_field,
                    )
                    if dup_log_id:
                        msg = f"Duplicate of work ID {existing_id}. Logged as duplicate ID {dup_log_id}."
                        return "duplicate", existing_id, msg, "works"
                    err_msg = f"Duplicate of work ID {existing_id} but failed to log: {dup_log_err}"
                    return "error", None, err_msg, None

                incoming = self._build_raw_work_payload(parsed_data)
                incoming.update(
                    {
                        "metadata_status": "matched",
                        "metadata_source": parsed_data.get("metadata_source_type") or existing.get("metadata_source") or "queue_request",
                        "metadata_error": None,
                        "metadata_updated_at": datetime.now().isoformat(),
                        "download_status": "queued" if download_status not in {"queued", "in_progress"} else download_status,
                        "download_claimed_by": None,
                        "download_claimed_at": None,
                        "download_lease_expires_at": None,
                        "download_error": None,
                        "bibtex_entry_json": self._clean_json_text(parsed_data.get('bibtex_entry_json')) or existing.get("bibtex_entry_json"),
                        "crossref_json": self._clean_json_text(parsed_data.get('crossref_json')) or existing.get("crossref_json"),
                        "openalex_json": self._clean_json_text(parsed_data.get('openalex_json')) or existing.get("openalex_json"),
                    }
                )
                try:
                    self._update_work_record(existing_id, self._apply_work_merge_priority(existing, incoming))
                    self.conn.commit()
                    return "added_to_queue", existing_id, "Existing work queued.", 'works'
                except sqlite3.Error as e:
                    self.conn.rollback()
                    return "error", None, f"Failed to queue existing work: {e}", None

            dup_log_id, dup_log_err = self.add_entry_to_duplicates(
                entry=parsed_data.get('original_json_object', parsed_data),
                input_doi=input_doi,
                input_openalex_id=input_openalex_id,
                source_bibtex_file=None,
                existing_entry_id=existing_id,
                existing_entry_table=table_name,
                matched_on_field=matched_field,
            )
            if dup_log_id:
                msg = f"Duplicate of entry ID {existing_id} in '{table_name}'. Logged as duplicate ID {dup_log_id}."
                if corpus_id:
                    self.add_corpus_item(corpus_id, table_name, existing_id)
                return "duplicate", existing_id, msg, table_name
            err_msg = f"Duplicate of entry ID {existing_id} in '{table_name}' but failed to log: {dup_log_err}"
            return "error", None, err_msg, None

        try:
            payload = self._build_raw_work_payload(parsed_data)
            payload.update(
                {
                    "metadata_status": "matched",
                    "metadata_source": parsed_data.get("metadata_source_type") or "queue_request",
                    "metadata_error": None,
                    "metadata_updated_at": datetime.now().isoformat(),
                    "download_status": "queued",
                    "download_claimed_by": None,
                    "download_claimed_at": None,
                    "download_lease_expires_at": None,
                    "download_error": None,
                    "bibtex_entry_json": self._clean_json_text(parsed_data.get('bibtex_entry_json')),
                    "crossref_json": self._clean_json_text(parsed_data.get('crossref_json')),
                    "openalex_json": self._clean_json_text(parsed_data.get('openalex_json')),
                }
            )
            last_id, _ = self._resolve_or_create_work(payload, allow_merge=True)
            if corpus_id:
                self.add_corpus_item(corpus_id, "works", int(last_id))
            return "added_to_queue", int(last_id), "Entry added to download queue.", None
        except sqlite3.Error as e:
            err_msg = f"SQLite error adding entry '{title}' to queue: {e}"
            return "error", None, err_msg, None
        except Exception as e:
            err_msg = f"Unexpected error adding entry '{title}' to queue: {e}"
            print(f"{RED}[DB Manager] {err_msg}{RESET}")
            return "error", None, err_msg, None

    def add_entry_to_duplicates(
        self,
        entry: object, # bibtexparser.model.Entry
        input_doi: str | None,
        input_openalex_id: str | None,
        source_bibtex_file: str | None,
        existing_entry_id: int,
        existing_entry_table: str,
        matched_on_field: str
    ) -> tuple[int | None, str | None]:
        """Adds a detected duplicate entry to the 'duplicate_references' table.

        Args:
            entry: The bibtexparser.model.Entry object that is a duplicate.
            input_doi: The DOI extracted from the duplicate entry.
            input_openalex_id: The OpenAlex ID extracted from the duplicate entry.
            source_bibtex_file: The path to the BibTeX file from which this duplicate was read.
            existing_entry_id: The ID of the entry it duplicates in the database.
            existing_entry_table: The table where the existing entry is stored (normally 'works').
            matched_on_field: The field that caused the duplicate detection (e.g., 'doi', 'openalex_id').

        Returns:
            A tuple: (row_id, None) on successful insertion,
                     (None, error_message_string) if insertion failed.
        """
        cursor = self.conn.cursor()

        def get_primitive_val(data_object, default=None):
            if data_object is None: return default
            if hasattr(data_object, 'value'): return str(data_object.value)
            return str(data_object)

        bibtex_key_val = self._clean_string_value(entry.key if hasattr(entry, 'key') else None)
        entry_type_val = self._clean_string_value(entry.entry_type if hasattr(entry, 'entry_type') else None)
        title_val = self._clean_string_value(get_primitive_val(entry.get('title')))
        
        authors_list = []
        raw_author_data = entry.get('author')
        if raw_author_data:
            if isinstance(raw_author_data, list):
                authors_list = [get_primitive_val(author_item) for author_item in raw_author_data]
            else:
                author_string = get_primitive_val(raw_author_data)
                if author_string: authors_list = [name.strip() for name in author_string.split(' and ')]
        authors_json = json.dumps([self._clean_string_value(str(author)) for author in authors_list if author]) if authors_list else None

        year_str_raw = get_primitive_val(entry.get('year'))
        year_int = None
        if year_str_raw:
            year_str_cleaned = year_str_raw.strip('{}')
            if year_str_cleaned:
                try: year_int = int(year_str_cleaned)
                except ValueError: print(f"{RED}[DB Manager] Warning: Could not parse year '{year_str_raw}' for duplicate entry {bibtex_key_val}{RESET}")
        
        normalized_doi = self._normalize_doi(self._clean_string_value(input_doi))
        normalized_openalex_id = self._normalize_openalex_id(self._clean_string_value(input_openalex_id))

        serializable_entry_dict = {k: get_primitive_val(v) for k, v in entry.items()}
        original_bibtex_json = json.dumps(serializable_entry_dict)

        # Ensure title is not None for the database constraint
        if not title_val:
            title_val = f"[Duplicate Entry: {bibtex_key_val or 'No Key'}]"
            print(f"{YELLOW}[DB Manager] Info: Missing title for duplicate entry. Using placeholder: '{title_val}'{RESET}")
            
        sql = """
            INSERT INTO duplicate_references (
                bibtex_key, entry_type, title, authors, year, 
                doi, openalex_id, 
                metadata_source_type, original_bibtex_entry_json, source_bibtex_file, 
                existing_entry_id, existing_entry_table, matched_on_field, 
                date_detected
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        
        try:
            cursor.execute(sql, (
                bibtex_key_val, entry_type_val, title_val, authors_json, year_int,
                normalized_doi, normalized_openalex_id,
                'bibtex_import_duplicate', original_bibtex_json, source_bibtex_file,
                existing_entry_id, existing_entry_table, matched_on_field
            ))
            self.conn.commit()
            # print(f"{GREEN}[DB Manager] Successfully added duplicate entry '{bibtex_key_val}' to duplicate_references. Matched on '{matched_on_field}' with ID {existing_entry_id} in '{existing_entry_table}'.{RESET}") # Keep for potential future, non-debug use
            return cursor.lastrowid, None
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] Error adding entry to duplicate_references: {e} (Entry: {bibtex_key_val}){RESET}")
            return None, str(e)

    def create_search_run(self, query: str, filters: dict | None = None) -> int:
        """Create a new search run record and return its ID."""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO search_runs (query, filters_json) VALUES (?, ?)",
            (query, json.dumps(filters) if filters else None),
        )
        self.conn.commit()
        return cursor.lastrowid

    def add_search_results(self, search_run_id: int, results: list[dict]) -> int:
        """Insert search results for a run. Returns number of inserted rows."""
        if not results:
            return 0
        cursor = self.conn.cursor()
        rows = []
        for result in results:
            rows.append(
                (
                    search_run_id,
                    result.get('openalex_id'),
                    result.get('doi'),
                    result.get('title'),
                    result.get('year'),
                    json.dumps(result.get('raw_json')) if result.get('raw_json') is not None else None,
                )
            )
        cursor.executemany(
            """INSERT INTO search_results (search_run_id, openalex_id, doi, title, year, raw_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.conn.commit()
        return len(rows)

    def get_all_downloaded_works_as_dicts(self) -> list[dict]:
        """Fetch downloaded works as dictionaries using the canonical works table."""
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    id,
                    id AS work_id,
                    NULL AS bibtex_key,
                    type AS entry_type,
                    source_pdf,
                    title,
                    authors,
                    editors,
                    year,
                    doi,
                    NULL AS pmid,
                    NULL AS arxiv_id,
                    openalex_id,
                    semantic_scholar_id,
                    NULL AS mag_id,
                    url AS url_source,
                    file_path,
                    abstract,
                    keywords,
                    source AS journal_conference,
                    volume,
                    issue,
                    pages,
                    publisher,
                    metadata_source AS metadata_source_type,
                    bibtex_entry_json,
                    status_notes,
                    downloaded_at AS date_processed,
                    file_checksum AS checksum_pdf,
                    download_source AS source_of_download,
                    origin_key AS ingest_source,
                    run_id,
                    normalized_title,
                    normalized_authors,
                    normalized_doi
                FROM works
                WHERE download_status = 'downloaded'
                ORDER BY id
                """
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] Error fetching downloaded works: {e}{RESET}")
            return []
        finally:
            self.conn.row_factory = None

    def get_sample_downloaded_works(self, limit: int = 5) -> list[tuple]:
        """Fetch a sample of downloaded works from the canonical works table."""
        if not self.conn:
            print(f"{RED}[DB Manager] Database connection is not open.{RESET}")
            return []
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT id, title, authors, year, doi, openalex_id, file_path FROM works WHERE download_status = 'downloaded' ORDER BY id LIMIT ?",
                (limit,),
            )
            return cursor.fetchall()
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] SQLite error while fetching sample downloaded works: {e}{RESET}")
            return []

    def save_to_disk(self, target_disk_path: Path | str) -> bool:
        """Saves the current database (which might be in-memory) to a specified disk file.

        Args:
            target_disk_path: The path to the file where the database should be saved.

        Returns:
            True if successful, False otherwise.
        """
        target_path = Path(target_disk_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.conn:
            print(f"{RED}[DB Manager] No active database connection to save.{RESET}")
            return False

        print(f"{YELLOW}[DB Manager] Attempting to save current database to: {target_path.resolve()}{RESET}")
        
        try:
            # Connect to the target disk database (this will create it if it doesn't exist)
            disk_db_conn = sqlite3.connect(target_path)
            
            # Use the backup API
            with disk_db_conn:
                self.conn.backup(disk_db_conn)
            
            disk_db_conn.close()
            print(f"{GREEN}[DB Manager] Database successfully saved to: {target_path.resolve()}{RESET}")
            return True
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] SQLite error during save_to_disk: {e}{RESET}")
            return False
        except Exception as e:
            print(f"{RED}[DB Manager] General error during save_to_disk: {e}{RESET}")
            return False

    def get_all_from_queue(self) -> list[dict]:
        """Fetch all currently queued or in-progress works."""
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT * FROM works WHERE download_status IN ('queued', 'in_progress')")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] SQLite error while fetching queue: {e}{RESET}")
            return []
        finally:
            self.conn.row_factory = None

    def claim_download_batch(
        self,
        *,
        limit: int,
        corpus_id: int | None,
        claimed_by: str,
        lease_seconds: int = 15 * 60,
        max_attempts: int | None = None,
    ) -> list[dict]:
        """Atomically claim queued works for download processing."""
        if limit <= 0:
            return []

        now = int(time.time())
        lease_expires_at = now + int(max(1, lease_seconds))

        eligible_state_sql = """
            (
              download_status = 'queued'
              OR (
                   download_status = 'in_progress'
                   AND download_lease_expires_at IS NOT NULL
                   AND download_lease_expires_at <= ?
                 )
            )
        """
        attempts_sql = ""
        params: list = [now]
        if max_attempts is not None:
            attempts_sql = " AND COALESCE(download_attempt_count, 0) < ?"
            params.append(int(max_attempts))

        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")

            if corpus_id is not None:
                cur.execute(
                    f"""
                    SELECT t.id
                    FROM works t
                    JOIN corpus_works cw ON cw.work_id = t.id
                    WHERE cw.corpus_id = ?
                      AND {eligible_state_sql}
                      {attempts_sql}
                    ORDER BY t.created_at ASC
                    LIMIT ?
                    """,
                    [int(corpus_id)] + params + [int(limit)],
                )
            else:
                cur.execute(
                    f"""
                    SELECT t.id
                    FROM works t
                    WHERE {eligible_state_sql}
                      {attempts_sql}
                    ORDER BY t.created_at ASC
                    LIMIT ?
                    """,
                    params + [int(limit)],
                )

            ids = [int(r[0]) for r in cur.fetchall()]
            if not ids:
                self.conn.commit()
                return []

            placeholders = ",".join(["?"] * len(ids))
            claim_sql = f"""
                UPDATE works
                   SET download_status = 'in_progress',
                       download_claimed_by = ?,
                       download_claimed_at = ?,
                       download_lease_expires_at = ?,
                       download_attempt_count = COALESCE(download_attempt_count, 0) + 1,
                       download_last_attempt_at = ?,
                       download_error = NULL
                 WHERE id IN ({placeholders})
                   AND {eligible_state_sql}
                   {attempts_sql}
            """
            cur.execute(
                claim_sql,
                [claimed_by, now, lease_expires_at, now] + ids + params,
            )

            fetch_sql = f"""
                SELECT id, title, authors, year, doi, openalex_id, NULL AS entry_type, type, url, url, openalex_json
                  FROM works
                 WHERE id IN ({placeholders})
                   AND download_claimed_by = ?
                   AND download_claimed_at = ?
                 ORDER BY created_at ASC
            """
            cur.execute(fetch_sql, ids + [claimed_by, now])
            rows = cur.fetchall()
            self.conn.commit()

            out: list[dict] = []
            for r in rows:
                out.append(
                    {
                        "id": r[0],
                        "title": r[1],
                        "authors": r[2],
                        "year": r[3],
                        "doi": r[4],
                        "openalex_id": r[5],
                        "entry_type": r[6],
                        "type": r[7],
                        "url": r[8],
                        "url_source": r[9],
                        "openalex_json": r[10],
                    }
                )
            return out
        except sqlite3.Error:
            try:
                self.conn.rollback()
            except Exception:
                pass
            return []

    def _convert_inverted_index_to_text(self, inverted_index: dict | None) -> str | None:
        """Converts an OpenAlex abstract_inverted_index to readable text."""
        if not inverted_index:
            return None
        word_positions = []
        for word, positions in inverted_index.items():
            for position in positions:
                word_positions.append((word, position))
        word_positions.sort(key=lambda x: x[1])
        return " ".join(word for word, _ in word_positions)

    def mark_downloaded_work(self, queue_id: int, enriched_data: dict, file_path: str, checksum: str) -> tuple[int | None, str | None]:
        """Mark a queued work as downloaded."""
        return self.move_entry_to_downloaded(queue_id, enriched_data, file_path, checksum, enriched_data.get('download_source', 'unknown'))

    def mark_download_failed(self, queue_id: int, reason: str) -> tuple[int | None, str | None]:
        """Mark a queued work as failed."""
        return self.move_entry_to_failed(queue_id, reason)

    def merge_duplicate_queued_work(
        self,
        queue_id: int,
        existing_table: str,
        existing_id: int,
        matched_field: str | None = None
    ) -> tuple[bool, str | None]:
        """Merge a queued work row into an existing canonical work."""
        if not self._fetch_record_dict("works", queue_id):
            return False, f"Queue entry {queue_id} not found."
        if existing_table != "works":
            existing_work_id = self._resolve_work_id_from_legacy_pointer(existing_table, existing_id)
        else:
            existing_work_id = int(existing_id)
        if not existing_work_id:
            return False, f"Canonical work not found for duplicate queue entry {queue_id}."
        duplicate = self.get_entry_by_id("works", int(queue_id)) or {}
        canonical = self.get_entry_by_id("works", int(existing_work_id)) or {}
        try:
            self._update_work_record(int(existing_work_id), self._apply_work_merge_priority(canonical, duplicate))
            self.conn.execute("DELETE FROM works WHERE id = ?", (int(queue_id),))
            self.conn.commit()
        except sqlite3.Error as err:
            self.conn.rollback()
            return False, f"SQLite error dropping duplicate queue entry {queue_id}: {err}"

        msg = f"Queue entry {queue_id} is duplicate of {existing_table} ID {existing_id}"
        if matched_field:
            msg += f" (matched on {matched_field})"
        return True, msg

if __name__ == '__main__':
    # Example usage: Initialize and create DB if run directly
    # This will create the db in dl_lit_project/data/literature.db
    # assuming you run this script from dl_lit_project/dl_lit/
    # or more robustly, ensure the path is relative to the script's location or a defined project root.
    
    # Determine project root assuming this script is in dl_lit_project/dl_lit/
    project_root = Path(__file__).resolve().parent.parent 
    db_file_path = project_root / "data" / "literature.db"
    
    print(f"Attempting to initialize DB at: {db_file_path}")
    db_manager = DatabaseManager(db_path=db_file_path)
    # Perform some simple operation to test, e.g., count tables (optional)
    cursor = db_manager.conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"{GREEN}[DB Manager Test] Tables found: {tables}{RESET}")
    db_manager.close_connection()
    print("DB Manager test finished.")
