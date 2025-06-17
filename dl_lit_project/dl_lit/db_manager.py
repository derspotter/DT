import sqlite3
import json
from pathlib import Path
import re
from .utils import parse_bibtex_file_field

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
        
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False) # check_same_thread=False for broader usability if needed
        self.conn.execute("PRAGMA foreign_keys = ON")
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
                authors TEXT, -- JSON list of strings
                year INTEGER,
                doi TEXT, -- Normalized DOI of the duplicate entry
                openalex_id TEXT, -- Normalized OpenAlex ID of the duplicate entry
                metadata_source_type TEXT, -- e.g., 'bibtex_import_duplicate'
                original_bibtex_entry_json TEXT, -- Full original BibTeX entry of the duplicate as JSON
                source_bibtex_file TEXT, -- Path to the BibTeX file this duplicate came from
                existing_entry_id INTEGER NOT NULL, -- ID of the entry it duplicates
                existing_entry_table TEXT NOT NULL, -- Table of the existing entry (e.g., 'downloaded_references')
                matched_on_field TEXT NOT NULL, -- Field that matched (e.g., 'doi', 'openalex_id')
                date_detected TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- When the duplicate was detected
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
        
        # Prioritize DOI if available
        if doi:
            normalized_doi_to_check = self._normalize_doi(doi)
            if normalized_doi_to_check: # Ensure DOI is not None after normalization
                # Check in downloaded_references
                cursor.execute("SELECT id FROM downloaded_references WHERE doi = ?", (normalized_doi_to_check,))
                row = cursor.fetchone()
                if row:
                    return "downloaded_references", row[0], "doi"
                
                # Check in to_download_references
                cursor.execute("SELECT id FROM to_download_references WHERE doi = ?", (normalized_doi_to_check,))
                row = cursor.fetchone()
                if row:
                    return "to_download_references", row[0], "doi"

        # Then check OpenAlex ID if available
        if openalex_id:
            normalized_openalex_id_to_check = self._normalize_openalex_id(openalex_id)
            if normalized_openalex_id_to_check: # Ensure OpenAlex ID is not None after normalization
                # Check in downloaded_references
                cursor.execute("SELECT id FROM downloaded_references WHERE openalex_id = ?", (normalized_openalex_id_to_check,))
                row = cursor.fetchone()
                if row:
                    return "downloaded_references", row[0], "openalex_id"

                # Check in to_download_references
                cursor.execute("SELECT id FROM to_download_references WHERE openalex_id = ?", (normalized_openalex_id_to_check,))
                row = cursor.fetchone()
                if row:
                    return "to_download_references", row[0], "openalex_id"
        
        # If no matches were found after checking DOI and OpenAlex ID in both tables
        return None, None, None

    def add_bibtex_entry_to_downloaded(
        self,
        entry: object, # This is bibtexparser.model.Entry
        pdf_base_dir: Path | str | None = None,
        input_doi: str | None = None, # New parameter
        input_openalex_id: str | None = None, # New parameter
        original_bibtex_entry_json: str | None = None, # Added based on CLI usage
        source_bibtex_file: str | None = None # Added based on CLI usage
    ) -> tuple[int | None, str | None]:
        """Adds a parsed BibTeX entry (bibtexparser.model.Entry object)
        to the 'downloaded_references' table.

        Args:
            entry: A bibtexparser.model.Entry object.
            pdf_base_dir: Optional base directory for resolving relative PDF paths.
            input_doi: Optional DOI to use instead of extracting from the entry.
            input_openalex_id: Optional OpenAlex ID to use instead of extracting from the entry.

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
        
        # Use passed-in DOI and OpenAlex ID
        cleaned_doi_val = self._clean_string_value(input_doi)
        doi_val = self._normalize_doi(cleaned_doi_val)

        cleaned_openalex_id_val = self._clean_string_value(input_openalex_id)
        openalexid_val = self._normalize_openalex_id(cleaned_openalex_id_val)

        pmid_val = self._clean_string_value(get_primitive_val(entry.get('pmid')))
        arxiv_val = self._clean_string_value(get_primitive_val(entry.get('arxiv')))
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
            print(f"{RED}[DB Manager] Error adding BibTeX entry to downloaded_references: {e} (Entry details: Key='{bibtex_key_val}', Title='{title_val}'){RESET}")
            return None, str(e)
        except Exception as e:
            error_msg = f"General error: {e}"
            print(f"{RED}[DB Manager] {error_msg} while adding BibTeX entry {bibtex_key_val if bibtex_key_val else 'N/A'}{RESET}")
            return None, error_msg

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
            existing_entry_table: The table where the existing entry is stored (e.g., 'downloaded_references').
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

    def get_all_downloaded_references_as_dicts(self) -> list[dict]:
        """Fetches all entries from the downloaded_references table as a list of dictionaries."""
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT * FROM downloaded_references ORDER BY id")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            print(f"{RED}[DB Manager] Error fetching all downloaded references: {e}{RESET}")
            return []
        finally:
            self.conn.row_factory = None # Reset row_factory

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
