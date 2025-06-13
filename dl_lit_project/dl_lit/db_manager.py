import sqlite3
import json
from pathlib import Path
import re
from .utils import parse_bibtex_file_field

# ANSI escape codes for colors
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

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
        
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False) # check_same_thread=False for broader usability if needed
        self.conn.execute("PRAGMA foreign_keys = ON")
        if self.is_in_memory:
            print(f"{GREEN}[DB Manager] In-memory database connected.{RESET}")
        else:
            print(f"{GREEN}[DB Manager] Connected to database: {self.db_path.resolve()}{RESET}")
        self._create_schema()

    def _create_schema(self):
        """Creates the database schema, including all necessary tables if they don't exist."""
        cursor = self.conn.cursor()

        # 1. Downloaded References (Master Table)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS downloaded_references (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bibtex_key TEXT,
                entry_type TEXT,
                title TEXT NOT NULL,
                authors TEXT, -- JSON list of strings
                year INTEGER,
                doi TEXT,
                pmid TEXT,
                arxiv_id TEXT,
                openalex_id TEXT, -- Stores 'W123456789'
                semantic_scholar_id TEXT,
                mag_id TEXT,
                url_source TEXT,
                file_path TEXT,
                abstract TEXT,
                keywords TEXT, -- JSON list of strings
                journal_conference TEXT,
                volume TEXT,
                issue TEXT,
                pages TEXT,
                publisher TEXT,
                metadata_source_type TEXT, -- e.g., 'bibtex_import', 'openalex_api', 'grobid'
                bibtex_entry_json TEXT, -- Full bib entry as JSON from bibtexparser
                status_notes TEXT, -- e.g., 'Successfully downloaded and processed'
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                date_processed TIMESTAMP,
                checksum_pdf TEXT,
                UNIQUE(doi),
                UNIQUE(openalex_id)
            )
        """)

        # 2. To-Download References
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS to_download_references (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bibtex_key TEXT,
                entry_type TEXT,
                title TEXT NOT NULL,
                authors TEXT,
                year INTEGER,
                doi TEXT,
                pmid TEXT,
                arxiv_id TEXT,
                openalex_id TEXT,
                semantic_scholar_id TEXT,
                mag_id TEXT,
                url_source TEXT, -- URL to attempt download from, or API query
                file_path TEXT, -- Would be NULL initially
                abstract TEXT,
                keywords TEXT,
                journal_conference TEXT,
                volume TEXT,
                issue TEXT,
                pages TEXT,
                publisher TEXT,
                metadata_source_type TEXT, -- e.g., 'input_json_list', 'user_added_doi'
                bibtex_entry_json TEXT, -- Minimal, or to be populated
                status_notes TEXT, -- e.g., 'Pending download', 'Attempt 1 failed - network error'
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                date_processed TIMESTAMP,
                checksum_pdf TEXT -- NULL initially
            )
        """)

        # 3. Duplicate References
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_references (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bibtex_key TEXT,
                entry_type TEXT,
                title TEXT NOT NULL,
                authors TEXT,
                year INTEGER,
                doi TEXT,
                pmid TEXT,
                arxiv_id TEXT,
                openalex_id TEXT,
                semantic_scholar_id TEXT,
                mag_id TEXT,
                url_source TEXT, -- Source of this duplicate entry
                file_path TEXT, -- Path of the file if it was a duplicate file
                abstract TEXT,
                keywords TEXT,
                journal_conference TEXT,
                volume TEXT,
                issue TEXT,
                pages TEXT,
                publisher TEXT,
                metadata_source_type TEXT, -- e.g., 'input_bib_parsed'
                bibtex_entry_json TEXT, -- Bib entry of the duplicate
                status_notes TEXT, -- e.g., 'Duplicate of downloaded_references.id=X (DOI match)'
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                date_processed TIMESTAMP,
                checksum_pdf TEXT
            )
        """)

        # 4. Failed Downloads
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS failed_downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bibtex_key TEXT,
                entry_type TEXT,
                title TEXT NOT NULL,
                authors TEXT,
                year INTEGER,
                doi TEXT,
                pmid TEXT,
                arxiv_id TEXT,
                openalex_id TEXT,
                semantic_scholar_id TEXT,
                mag_id TEXT,
                url_source TEXT, -- URL that failed
                file_path TEXT, -- NULL
                abstract TEXT,
                keywords TEXT,
                journal_conference TEXT,
                volume TEXT,
                issue TEXT,
                pages TEXT,
                publisher TEXT,
                metadata_source_type TEXT,
                bibtex_entry_json TEXT, -- Data available before failure
                status_notes TEXT NOT NULL, -- Reason for failure
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                date_processed TIMESTAMP,
                checksum_pdf TEXT -- NULL
            )
        """)
        self.conn.commit()
        print(f"{GREEN}[DB Manager] All tables ensured in the database.{RESET}")

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
            return match.group(1).upper()
        # If no URL-like structure is found, assume it might be a bare DOI already.
        # We still standardize it to uppercase.
        return doi.upper()

    def check_if_exists(self, doi: str | None, openalex_id: str | None) -> tuple[str | None, int | None, str | None]:
        """Checks if an entry with the given DOI or OpenAlex ID exists in relevant tables.

        Args:
            doi: The DOI to check.
            openalex_id: The OpenAlex ID to check (e.g., 'W12345').

        Returns:
            A tuple (table_name, entry_id, matched_field_name) if found, else (None, None, None).
            'table_name' can be 'downloaded_references' or 'to_download_references'.
            'matched_field_name' can be 'doi' or 'openalex_id'.
        """
        cursor = self.conn.cursor()

        normalized_doi = self._normalize_doi(doi)

        # Prioritize DOI if available
        if normalized_doi:
            # Check in downloaded_references
            cursor.execute("SELECT id FROM downloaded_references WHERE doi = ?", (normalized_doi,))
            row = cursor.fetchone()
            if row:
                return "downloaded_references", row[0], "doi"
            
            # Check in to_download_references
            cursor.execute("SELECT id FROM to_download_references WHERE doi = ?", (normalized_doi,))
            row = cursor.fetchone()
            if row:
                return "to_download_references", row[0], "doi"

        # Then check OpenAlex ID if available
        if openalex_id:
            # Check in downloaded_references
            cursor.execute("SELECT id FROM downloaded_references WHERE openalex_id = ?", (openalex_id,))
            row = cursor.fetchone()
            if row:
                return "downloaded_references", row[0], "openalex_id"

            # Check in to_download_references
            cursor.execute("SELECT id FROM to_download_references WHERE openalex_id = ?", (openalex_id,))
            row = cursor.fetchone()
            if row:
                return "to_download_references", row[0], "openalex_id"
                
        return None, None, None

    def add_entry_to_duplicates(
        self,
        entry_data: dict,
        duplicate_of_table: str,
        duplicate_of_id: int,
        matched_on_field: str
    ) -> tuple[int | None, str | None]:
        """Adds an entry to the 'duplicate_references' table."""
        sql = """
            INSERT INTO duplicate_references (
                bibtex_key, entry_type, title, authors, year, doi, openalex_id,
                metadata_source_type, bibtex_entry_json, status_notes, url_source
            ) VALUES (
                :bibtex_key, :entry_type, :title, :authors, :year, :doi, :openalex_id,
                :metadata_source_type, :bibtex_entry_json, :status_notes, :url_source
            )
        """
        url_source_val = entry_data.get('input_id_field')
        status_note = f"Duplicate of {duplicate_of_table}.id={duplicate_of_id} (matched on {matched_on_field})"
        
        params = {
            "bibtex_key": entry_data.get('bibtex_key'),
            "entry_type": entry_data.get('entry_type'),
            "title": entry_data.get('title', url_source_val or "[Title N/A]"),
            "authors": json.dumps(entry_data.get('authors_list', [])),
            "year": entry_data.get('year'),
            "doi": self._normalize_doi(entry_data.get('doi')),
            "openalex_id": entry_data.get('openalex_id'),
            "metadata_source_type": 'json_import_duplicate',
            "bibtex_entry_json": json.dumps(entry_data.get('original_json_object', {})),
            "status_notes": status_note,
            "url_source": url_source_val
        }

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            self.conn.commit()
            inserted_id = cursor.lastrowid
            print(f"{YELLOW}[DB Manager] Logged duplicate: ID {inserted_id} for input '{url_source_val}'. Reason: {status_note}{RESET}")
            return inserted_id, None
        except sqlite3.Error as e:
            error_msg = f"SQLite error adding to duplicates: {e}"
            print(f"{RED}[DB Manager] Error: {error_msg} for input '{url_source_val}'{RESET}")
            return None, error_msg

    def add_entry_to_download_queue(self, entry_data: dict) -> tuple[str, int | None, str | None]:
        """Adds an entry to the 'to_download_references' table after checking for duplicates."""
        doi = self._normalize_doi(entry_data.get('doi'))
        openalex_id = entry_data.get('openalex_id')
        input_id = entry_data.get('input_id_field', 'N/A')

        # Check for duplicates
        table_name, existing_id, matched_field = self.check_if_exists(doi, openalex_id)
        if table_name and existing_id is not None:
            dup_id, dup_err = self.add_entry_to_duplicates(
                entry_data, table_name, existing_id, matched_field
            )
            if dup_err:
                return "error", None, f"Failed to log duplicate for '{input_id}': {dup_err}"
            return "duplicate", dup_id, f"Entry '{input_id}' is a duplicate of {table_name}.id={existing_id} on {matched_field}"

        # Not a duplicate, proceed to add to to_download_references
        sql = """
            INSERT INTO to_download_references (
                bibtex_key, entry_type, title, authors, year, doi, openalex_id,
                metadata_source_type, bibtex_entry_json, status_notes, url_source
            ) VALUES (
                :bibtex_key, :entry_type, :title, :authors, :year, :doi, :openalex_id,
                :metadata_source_type, :bibtex_entry_json, :status_notes, :url_source
            )
        """
        
        title = entry_data.get('title')
        if not title:
            title = entry_data.get('input_id_field') or "[Title N/A from JSON Import]"
            print(f"{YELLOW}[DB Manager] Info: Missing title for entry from JSON '{input_id}'. Using placeholder: '{title}'{RESET}")

        params = {
            "bibtex_key": entry_data.get('bibtex_key'),
            "entry_type": entry_data.get('entry_type'),
            "title": title,
            "authors": json.dumps(entry_data.get('authors_list', [])),
            "year": entry_data.get('year'),
            "doi": doi,
            "openalex_id": openalex_id,
            "metadata_source_type": 'json_import',
            "bibtex_entry_json": json.dumps(entry_data.get('original_json_object', {})),
            "status_notes": 'pending_enrichment',
            "url_source": entry_data.get('input_id_field')
        }

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            self.conn.commit()
            inserted_id = cursor.lastrowid
            print(f"{GREEN}[DB Manager] Added to download queue: ID {inserted_id} for input '{input_id}'{RESET}")
            return "added_to_queue", inserted_id, None
        except sqlite3.IntegrityError as e:
            error_msg = f"SQLite integrity error adding to queue for '{input_id}': {e}"
            print(f"{RED}[DB Manager] Error: {error_msg}{RESET}")
            return "error", None, error_msg
        except sqlite3.Error as e:
            error_msg = f"SQLite error adding to queue for '{input_id}': {e}"
            print(f"{RED}[DB Manager] Error: {error_msg}{RESET}")
            return "error", None, error_msg

    def get_all_downloaded_references_as_dicts(self) -> list[dict]:
        """
        Fetches all entries from the 'downloaded_references' table
        and returns them as a list of dictionaries.
        """
        try:
            # Ensure row_factory is set to sqlite3.Row for this operation
            original_row_factory = self.conn.row_factory
            self.conn.row_factory = sqlite3.Row
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM downloaded_references")
            rows = cursor.fetchall()
            self.conn.row_factory = original_row_factory # Reset row factory
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            print(f"Error fetching all downloaded references: {e}")
            # Ensure row factory is reset in case of error too
            if hasattr(self, 'conn') and self.conn:
                 self.conn.row_factory = original_row_factory if 'original_row_factory' in locals() else None
            return []

    def save_to_disk(self, target_disk_db_path: Path | str) -> bool:
        """
        Saves the current in-memory database to a specified disk file.
        This is typically used if the DatabaseManager was initialized with ':memory:'.
        It uses SQLite's backup functionality.

        Args:
            target_disk_db_path: The path to the disk file where the database should be saved.

        Returns:
            True if successful, False otherwise.
        """
        if not self.is_in_memory:
            print(f"{YELLOW}[DB Manager] Warning: 'save_to_disk' called on a disk-based database. No action taken.{RESET}")
            # Consider if this should be True or False. If it's not an error, True is fine.
            return True 

        target_path = Path(target_disk_db_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"{GREEN}[DB Manager] Attempting to save in-memory database to disk: {target_path.resolve()}{RESET}")
        
        disk_conn = None
        try:
            disk_conn = sqlite3.connect(str(target_path)) # Ensure path is string for connect
            self.conn.backup(disk_conn) # self.conn is the in-memory connection
            # backup implicitly handles transactions, commit on disk_conn might not be strictly needed
            # but can be good for ensuring data is flushed.
            disk_conn.commit() 
            print(f"{GREEN}[DB Manager] Successfully saved database to {target_path.resolve()}{RESET}")
            return True
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] SQLite error during save_to_disk: {e}{RESET}")
            return False
        except Exception as e:
            print(f"{RED}[DB Manager] UNEXPECTED error during save_to_disk: {type(e).__name__} - {e}{RESET}")
            return False
        finally:
            if disk_conn:
                disk_conn.close()

    def close_connection(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print(f"{GREEN}[DB Manager] Database connection closed.{RESET}")

    def add_bibtex_entry_to_downloaded(self, entry: object, pdf_base_dir: Path | str | None = None) -> tuple[int | None, str | None]:
        """Adds a parsed BibTeX entry (bibtexparser.model.Entry object)
        to the 'downloaded_references' table.

        Args:
            entry: A bibtexparser.model.Entry object.
            pdf_base_dir: Optional base directory for resolving relative PDF paths.

        Returns:
            A tuple: (row_id, None) on successful insertion,
                     (None, error_message_string) if insertion failed.
        """
        cursor = self.conn.cursor()

        def get_primitive_val(data_object, default=None):
            """Safely extracts the primitive string value from a bibtexparser Field object or returns the value if already primitive."""
            if data_object is None:
                return default
            if hasattr(data_object, 'value'): # It's a Field object
                return str(data_object.value) # Get the actual value and ensure it's a string
            return str(data_object) # Already a primitive type (e.g. string from ENTRYTYPE) or needs string conversion

        # Process authors
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

        # Process keywords
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
        original_bibtex_json = json.dumps(serializable_entry_dict)

        bibtex_key_val = self._clean_string_value(entry.key if hasattr(entry, 'key') else None)
        entry_type_val = self._clean_string_value(entry.entry_type if hasattr(entry, 'entry_type') else None)

        year_str_raw = get_primitive_val(entry.get('year'))
        year_int = None
        if year_str_raw:
            year_str_cleaned = year_str_raw.strip('{}')
            if year_str_cleaned:
                try:
                    year_int = int(year_str_cleaned)
                except ValueError:
                    print(f"{RED}[DB Manager] Warning: Could not parse year '{year_str_raw}' (cleaned to: '{year_str_cleaned}') as int for entry {bibtex_key_val if bibtex_key_val else 'N/A'}{RESET}")
        
        title_val = self._clean_string_value(get_primitive_val(entry.get('title')))
        doi_val = self._normalize_doi(self._clean_string_value(get_primitive_val(entry.get('doi'))))
        pmid_val = self._clean_string_value(get_primitive_val(entry.get('pmid')))
        arxiv_val = self._clean_string_value(get_primitive_val(entry.get('arxiv')))
        openalexid_val = self._clean_string_value(get_primitive_val(entry.get('openalexid')))
        semanticscholarid_val = self._clean_string_value(get_primitive_val(entry.get('semanticscholarid')))
        magid_val = self._clean_string_value(get_primitive_val(entry.get('magid')))
        url_source_val = self._clean_string_value(get_primitive_val(entry.get('url')) or get_primitive_val(entry.get('link')))
        # Process file path
        raw_file_field = get_primitive_val(entry.get('file'))
        parsed_filename = parse_bibtex_file_field(raw_file_field)
        file_path_val = None
        if parsed_filename:
            if pdf_base_dir:
                # Construct the full path and resolve it to be absolute
                full_path = Path(pdf_base_dir) / parsed_filename
                # Check if the file actually exists before storing the path
                if full_path.is_file():
                    file_path_val = str(full_path.resolve())
                else:
                    # Log a warning if the resolved file path doesn't exist
                    print(f"{YELLOW}[DB Manager] Warning: Resolved PDF path does not exist: {full_path.resolve()} for entry {bibtex_key_val if bibtex_key_val else 'N/A'}{RESET}")
                    # Store the intended path anyway, as it might be resolved later or on a different system
                    file_path_val = str(full_path.resolve())
            else:
                # If no base directory is given, store just the parsed filename
                file_path_val = parsed_filename
        abstract_val = self._clean_string_value(get_primitive_val(entry.get('abstract')))
        journal_conference_val = self._clean_string_value(get_primitive_val(entry.get('journal')) or get_primitive_val(entry.get('booktitle')))
        volume_val = self._clean_string_value(get_primitive_val(entry.get('volume')))
        issue_val = self._clean_string_value(get_primitive_val(entry.get('number')) or get_primitive_val(entry.get('issue')))
        pages_val = self._clean_string_value(get_primitive_val(entry.get('pages')))
        publisher_val = self._clean_string_value(get_primitive_val(entry.get('publisher')))

        sql = """
            INSERT INTO downloaded_references (
                bibtex_key, entry_type, title, authors, year, doi, pmid, arxiv_id, 
                openalex_id, semantic_scholar_id, mag_id, url_source, file_path, 
                abstract, keywords, journal_conference, volume, issue, pages, publisher, 
                metadata_source_type, bibtex_entry_json, status_notes, date_processed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        try:
            # If title is missing, use bibtex_key as a placeholder, or a generic string if key is also missing.
            if not title_val:
                if bibtex_key_val:
                    title_val = f"[Title from Key: {bibtex_key_val}]"
                    print(f"{YELLOW}[DB Manager] Info: Missing title for entry. Using bibtex_key as placeholder: '{title_val}' for key '{bibtex_key_val}'{RESET}")
                else:
                    title_val = "[No Title or Key Available]"
                    print(f"{YELLOW}[DB Manager] Info: Missing title and bibtex_key for entry. Using generic placeholder: '{title_val}'{RESET}")
            
            cursor.execute(sql, (
                bibtex_key_val, entry_type_val, title_val, authors_json, 
                year_int, doi_val, pmid_val, arxiv_val,
                openalexid_val, semanticscholarid_val, magid_val,
                url_source_val, file_path_val, abstract_val, keywords_json, 
                journal_conference_val, volume_val, issue_val, pages_val, publisher_val,
                'bibtex_import', original_bibtex_json, 'Imported from BibTeX file'
            ))
            self.conn.commit()
            return cursor.lastrowid, None
        except sqlite3.IntegrityError as e:
            error_msg = str(e.args[0] if e.args else str(e))
            print(f"{YELLOW}[DB Manager] Integrity error for entry {bibtex_key_val if bibtex_key_val else 'N/A'}: {error_msg}{RESET}")
            return None, error_msg
        except sqlite3.Error as e:
            error_msg = f"SQLite error: {e}"
            print(f"{RED}[DB Manager] {error_msg} while adding BibTeX entry {bibtex_key_val if bibtex_key_val else 'N/A'}{RESET}")
            return None, error_msg
        except Exception as e:
            error_msg = f"General error: {e}"
            print(f"{RED}[DB Manager] {error_msg} while adding BibTeX entry {bibtex_key_val if bibtex_key_val else 'N/A'}{RESET}")
            return None, error_msg

    def get_sample_downloaded_references(self, limit: int = 5) -> list[tuple]:
        """Fetches a sample of entries from the downloaded_references table.

        Args:
            limit: The maximum number of entries to fetch.

        Returns:
            A list of tuples, where each tuple represents a row from the table.
        """
        if not self.conn:
            print(f"{RED}[DB Manager] Database connection is not open.{RESET}")
            return []
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM downloaded_references LIMIT ?", (limit,))
            rows = cursor.fetchall()
            return rows
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] SQLite error while fetching sample entries: {e}{RESET}")
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
        """Fetches all entries from the 'to_download_references' table.

        Returns:
            A list of dictionaries, where each dictionary represents a row.
        """
        self.conn.row_factory = sqlite3.Row # Fetch results as dict-like rows
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT * FROM to_download_references")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] SQLite error while fetching queue: {e}{RESET}")
            return []
        finally:
            self.conn.row_factory = None # Reset row factory to default

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

    def move_queue_entry_to_downloaded(self, queue_id: int, enriched_data: dict, file_path: str, checksum: str) -> tuple[int | None, str | None]:
        """Adds a successfully processed entry to downloaded_references and removes it from the queue."""
        
        # 1. Prepare data for insertion from the enriched OpenAlex data
        title = enriched_data.get('display_name')
        authors_list = [authorship['author'].get('display_name') for authorship in enriched_data.get('authorships', []) if authorship.get('author')]
        authors_json = json.dumps(authors_list) if authors_list else None
        
        keywords_list = [kw.get('display_name') for kw in enriched_data.get('keywords', [])]
        keywords_json = json.dumps(keywords_list) if keywords_list else None

        abstract = self._convert_inverted_index_to_text(enriched_data.get('abstract_inverted_index'))
        
        # Extract OpenAlex ID from the ID URL
        openalex_id_url = enriched_data.get('id')
        openalex_id = openalex_id_url.split('/')[-1] if openalex_id_url else None

        # Journal/source info
        primary_location = enriched_data.get('primary_location', {}) or {}
        source = primary_location.get('source', {}) or {}
        journal_conference = source.get('display_name')
        
        biblio = enriched_data.get('biblio', {})
        volume = biblio.get('volume')
        issue = biblio.get('issue')
        pages = f"{biblio.get('first_page')}--{biblio.get('last_page')}" if biblio.get('first_page') and biblio.get('last_page') else None

        sql_insert = """
            INSERT INTO downloaded_references (
                entry_type, title, authors, year, doi, openalex_id, url_source, file_path,
                abstract, keywords, journal_conference, volume, issue, pages,
                metadata_source_type, bibtex_entry_json, status_notes, date_processed, checksum_pdf
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        """
        
        sql_delete = "DELETE FROM to_download_references WHERE id = ?"

        try:
            cursor = self.conn.cursor()
            # Use a transaction
            cursor.execute("BEGIN")
            
            cursor.execute(sql_insert, (
                enriched_data.get('type'), title, authors_json, enriched_data.get('publication_year'),
                enriched_data.get('doi'), openalex_id, enriched_data.get('id'), file_path,
                abstract, keywords_json, journal_conference, volume, issue, pages,
                'openalex_api', json.dumps(enriched_data), 'Successfully processed from queue', checksum
            ))
            inserted_id = cursor.lastrowid
            
            cursor.execute(sql_delete, (queue_id,))
            
            self.conn.commit()
            print(f"{GREEN}[DB Manager] Moved queue item {queue_id} to downloaded_references (ID: {inserted_id}){RESET}")
            return inserted_id, None
        except sqlite3.Error as e:
            self.conn.rollback()
            error_msg = f"Transaction failed: {e}"
            print(f"{RED}[DB Manager] {error_msg}{RESET}")
            return None, error_msg

    def move_queue_entry_to_failed(self, queue_id: int, reason: str) -> tuple[int | None, str | None]:
        """Moves a failed entry from the queue to failed_downloads, preserving its data."""
        
        # 1. Get the original data from the queue
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM to_download_references WHERE id = ?", (queue_id,))
        original_entry = cursor.fetchone()
        self.conn.row_factory = None # Reset

        if not original_entry:
            return None, f"No entry found in queue with ID {queue_id}"

        # 2. Prepare for insertion into failed_downloads
        # The columns are mostly the same. We just need to add the failure reason.
        original_data = dict(original_entry)
        del original_data['id'] # Remove the old primary key
        original_data['status_notes'] = reason # Add the failure reason
        
        columns = ', '.join(original_data.keys())
        placeholders = ', '.join(['?'] * len(original_data))
        sql_insert = f"INSERT INTO failed_downloads ({columns}) VALUES ({placeholders})"
        
        sql_delete = "DELETE FROM to_download_references WHERE id = ?"

        try:
            cursor = self.conn.cursor()
            cursor.execute("BEGIN")
            
            cursor.execute(sql_insert, list(original_data.values()))
            inserted_id = cursor.lastrowid
            
            cursor.execute(sql_delete, (queue_id,))
            
            self.conn.commit()
            print(f"{YELLOW}[DB Manager] Moved queue item {queue_id} to failed_downloads (ID: {inserted_id}). Reason: {reason}{RESET}")
            return inserted_id, None
        except sqlite3.Error as e:
            self.conn.rollback()
            error_msg = f"Transaction failed while moving to failed: {e}"
            print(f"{RED}[DB Manager] {error_msg}{RESET}")
            return None, error_msg

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
