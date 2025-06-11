import sqlite3
import json
from pathlib import Path
from dl_lit.utils import parse_bibtex_file_field

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

        # ... (schema creation code is identical and omitted for brevity)

        self.conn.commit()
        print(f"{GREEN}[DB Manager] All tables ensured in the database.{RESET}")

    def _clean_string_value(self, value: str | None) -> str | None:
        """Strips leading/trailing curly braces from a string value."""
        if isinstance(value, str):
            return value.strip('{}')
        return value

    def check_if_exists(self, doi: str | None, openalex_id: str | None) -> tuple[str | None, int | None, str | None]:
        cursor = self.conn.cursor()
        if doi:
            cursor.execute("SELECT id FROM downloaded_references WHERE doi = ?", (doi,))
            row = cursor.fetchone()
            if row:
                return "downloaded_references", row[0], "doi"
            cursor.execute("SELECT id FROM to_download_references WHERE doi = ?", (doi,))
            row = cursor.fetchone()
            if row:
                return "to_download_references", row[0], "doi"
        if openalex_id:
            cursor.execute("SELECT id FROM downloaded_references WHERE openalex_id = ?", (openalex_id,))
            row = cursor.fetchone()
            if row:
                return "downloaded_references", row[0], "openalex_id"
            cursor.execute("SELECT id FROM to_download_references WHERE openalex_id = ?", (openalex_id,))
            row = cursor.fetchone()
            if row:
                return "to_download_references", row[0], "openalex_id"
        return None, None, None

    def add_entry_to_duplicates(self, entry_data: dict, duplicate_of_table: str, duplicate_of_id: int, matched_on_field: str) -> tuple[int | None, str | None]:
        # ... (implementation is identical)
        return None, None # Simplified for brevity

    def add_entry_to_download_queue(self, entry_data: dict) -> tuple[str, int | None, str | None]:
        doi = entry_data.get('doi')
        openalex_id = entry_data.get('openalex_id')
        input_id = entry_data.get('input_id_field', 'N/A')

        table_name, existing_id, matched_field = self.check_if_exists(doi, openalex_id)
        if table_name and existing_id is not None:
            # ... (duplicate handling)
            return "duplicate", None, f"Entry '{input_id}' is a duplicate of {table_name}.id={existing_id} on {matched_field}"

        # ... (insert into queue logic)
        return "added_to_queue", 1, None # Simplified

    def add_bibtex_entry_to_downloaded(self, entry: object, pdf_base_dir: Path | str | None = None) -> tuple[int | None, str | None]:
        # ... (implementation uses get_primitive_val)
        doi_val = self._clean_string_value(get_primitive_val(entry.get('doi')))
        # ... (rest of the insertion logic)
        return 1, None # Simplified

    # ... (other methods are identical)
