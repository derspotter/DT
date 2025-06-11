import click
from pathlib import Path
from .db_manager import DatabaseManager # Relative import
import bibtexparser # For parsing BibTeX files
from .metadata_fetcher import MetadataFetcher
from .pdf_downloader import PDFDownloader
import traceback

# ANSI escape codes for colors
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

# Define project root for default DB path calculation if needed elsewhere
# Or rely on db_manager's default path logic
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "literature.db"

@click.group()
def cli():
    """A command-line tool for managing your literature library."""
    pass

@cli.command("clear-downloaded")
@click.option('--db-path', default=str(DEFAULT_DB_PATH), help='Path to the SQLite database file.')
@click.confirmation_option(prompt='Are you sure you want to clear all entries from the downloaded_references table?')
def clear_downloaded_command(db_path):
    """Clears all entries from the downloaded_references table."""
    db_manager = DatabaseManager(db_path)
    try:
        cursor = db_manager.conn.cursor()
        cursor.execute("DELETE FROM downloaded_references")
        db_manager.conn.commit()
        click.echo(f"Successfully cleared all entries from 'downloaded_references' in {db_path}.")
    except Exception as e:
        click.echo(f"Error clearing table: {e}", err=True)
    finally:
        db_manager.close_connection()


@cli.command("init-db")
@click.option('--db-path', 
              default=str(DEFAULT_DB_PATH), 
              help='Path to the SQLite database file.', 
              type=click.Path(dir_okay=False, writable=True))
def init_db_command(db_path):
    """Initializes or re-initializes the literature database and schema."""
    try:
        db_manager = DatabaseManager(db_path=Path(db_path))
        click.echo(f"Database initialized successfully at {Path(db_path).resolve()}")
        # The schema creation is handled by DatabaseManager's __init__
        db_manager.close_connection()
    except Exception as e:
        click.echo(f"Error initializing database: {e}", err=True)

@cli.command("show-sample")
@click.option('--db-path', default=str(DEFAULT_DB_PATH), help='Path to the SQLite database file.')
@click.option('--limit', default=5, type=int, help='Number of entries to display.')
def show_sample_command(db_path, limit):
    """Shows a sample of entries from the downloaded_references table."""
    db_manager = DatabaseManager(db_path) # Connection and table creation are handled in __init__

    click.echo(f"Fetching sample of {limit} entries from {db_path}...")
    entries = db_manager.get_sample_downloaded_references(limit=limit)

    if not entries:
        click.echo("No entries found or an error occurred.")
        db_manager.close_connection()
        return

    # Get column names for header
    cursor = db_manager.conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(downloaded_references)")
        columns = [col[1] for col in cursor.fetchall()]
    except Exception as e:
        click.echo(f"Error fetching column names: {e}", err=True)
        columns = ["ID", "BibTeX Key", "Type", "Title", "Authors", "Year", "DOI", "...(other fields)"] # Fallback

    click.echo("\n--- Sample Entries ---")
    for i, entry_tuple in enumerate(entries):
        click.echo(f"\nEntry {i+1}:")
        # Truncate long fields for display
        for col_name, value in zip(columns, entry_tuple):
            display_value = str(value)
            if len(display_value) > 70 and col_name not in ['authors', 'bibtex_entry_json']: # Don't truncate authors or full json for now
                display_value = display_value[:67] + "..."
            click.echo(f"  {col_name}: {display_value}")
    
    click.echo("\n--- End of Sample ---")

    db_manager.close_connection()


@cli.command("import-bib")
@click.argument('bibtex_file', type=click.Path(exists=True, dir_okay=False, readable=True))
@click.option('--pdf-base-dir', 
              type=click.Path(exists=True, file_okay=False, readable=True),
              help='Optional base directory for resolving relative PDF paths in the BibTeX file.')
@click.option('--db-path', 
              default=str(DEFAULT_DB_PATH), 
              help='Path to the SQLite database file.', 
              type=click.Path(dir_okay=False, writable=True))
def import_bib_command(bibtex_file, pdf_base_dir, db_path):
    """Imports references from a BibTeX file into the 'downloaded_references' table."""
    click.echo(f"Attempting to import from BibTeX file: {bibtex_file}")
    if pdf_base_dir:
        click.echo(f"Using PDF base directory (feature not fully implemented yet): {pdf_base_dir}")
    
    db_manager = None # Initialize to ensure it's defined for finally block
    actual_disk_db_path = Path(db_path) # The final destination for the data
    try:
        # Initialize with an in-memory database for fast processing
        db_manager = DatabaseManager(db_path=":memory:") 
        # The db_path variable from click options now refers to the *target* disk file path
        click.echo(f"Processing import into in-memory database. Target disk DB: {actual_disk_db_path.resolve()}")

        try:
            with open(bibtex_file, 'r', encoding='utf-8') as bibfile:
                bib_content = bibfile.read()
            bib_database = bibtexparser.parse_string(bib_content)
            # For v2, common customizations are often passed to parse_string or set globally
            # e.g., bibtexparser.parse_string(bib_content, common_strings=True)
            # However, the default behavior of parse_string in v2 is generally quite robust.
        except Exception as e_parse: # Temporarily catch generic exception for diagnostics
            click.echo(f"Diagnostics: Encountered an error during BibTeX parsing (parse_string call).", err=True)
            click.echo(f"Diagnostics: Exception type: {type(e_parse)}", err=True)
            click.echo(f"Diagnostics: Exception message: {str(e_parse)}", err=True)
            # Depending on the error, decide if to return or re-raise
            # For now, let's return to stop the import on any parsing related error.
            return

        if not bib_database.entries:
            click.echo("No entries found in the BibTeX file.")
            return

        click.echo(f"Found {len(bib_database.entries)} entries in {bibtex_file}.")
        
        successful_imports = 0
        failed_imports = []
        duplicate_entries = [] # Placeholder for future duplicate handling

        for entry_obj in bib_database.entries: # entry_obj is a bibtexparser.model.Entry
            entry_id, error_msg = db_manager.add_bibtex_entry_to_downloaded(entry_obj, pdf_base_dir)
            
            if entry_id:
                successful_imports += 1
            else:
                # Try to get an identifier for the failed entry
                identifier = entry_obj.key if hasattr(entry_obj, 'key') and entry_obj.key else entry_obj.get('title', 'Unknown Title')
                # Clean the identifier just in case it has braces, for display purposes
                if isinstance(identifier, str):
                    identifier = identifier.strip('{}')
                
                reason = error_msg if error_msg else "Unknown reason (entry ignored or error not captured)"
                failed_imports.append((str(identifier)[:100], reason)) # Store a tuple of (identifier, reason)
        
        click.echo("\n--- Import Summary ---")
        click.echo(f"{GREEN}Successfully imported: {successful_imports} entries{RESET}")
        
        if failed_imports:
            click.echo(f"{RED}Failed to import/ignored: {len(failed_imports)} entries{RESET}")
            for i, (item_id, reason) in enumerate(failed_imports):
                click.echo(f"  {RED}Failed/Ignored {i+1}: {item_id} - Reason: {reason}{RESET}")
        else:
            click.echo(f"{GREEN}No import failures.{RESET}")

        if duplicate_entries: # This will be empty for now
            click.echo(f"{YELLOW}Duplicates found (and skipped): {len(duplicate_entries)} entries{RESET}")
            for i, item in enumerate(duplicate_entries):
                click.echo(f"  {YELLOW}Duplicate {i+1}: {item}{RESET}")
        else:
            click.echo(f"{GREEN}No duplicates detected (duplicate check not yet fully implemented).{RESET}")

        click.echo("\n--- Saving to Disk ---")
        if db_manager.save_to_disk(actual_disk_db_path):
            click.echo(f"{GREEN}Successfully saved database to: {actual_disk_db_path.resolve()}{RESET}")
        else:
            click.echo(f"{RED}Failed to save database to disk: {actual_disk_db_path.resolve()}{RESET}")

        click.echo("\nBibTeX import process completed.")

    except FileNotFoundError:
        click.echo(f"Error: BibTeX file not found at {bibtex_file}", err=True)
    # The specific BibtexParserError catch is removed. 
    # Parsing errors are now caught by the generic Exception in the inner try-except block around parse_string,
    # or by the general Exception catch below if they occur outside that specific parsing step.
    except Exception as e: # General catch-all for other unexpected errors
        click.echo(f"An unexpected error occurred during import: {e}", err=True)
        click.echo(f"Type of error: {type(e)}", err=True)
    finally:
        if db_manager:
            db_manager.close_connection()

import json # For parsing JSON files
import re # For OpenAlex ID extraction

def _parse_json_entry(json_obj: dict) -> dict:
    """Parses a single JSON object from the input file into a standardized dict.
    
    Args:
        json_obj: A dictionary representing a single bibliographic entry from JSON.

    Returns:
        A dictionary formatted for use with DatabaseManager methods.
    """
    entry_data = {}
    entry_data['original_json_object'] = json_obj # Store the whole thing

    entry_data['input_id_field'] = json_obj.get('id')
    entry_data['doi'] = json_obj.get('doi')
    entry_data['title'] = json_obj.get('display_name')
    entry_data['year'] = json_obj.get('publication_year')
    entry_data['entry_type'] = json_obj.get('type')

    authors_list = []
    authorships = json_obj.get('authorships', [])
    if isinstance(authorships, list):
        for authorship in authorships:
            if isinstance(authorship, dict) and 'author' in authorship and isinstance(authorship['author'], dict):
                author_name = authorship['author'].get('display_name')
                if author_name:
                    authors_list.append(author_name)
    entry_data['authors_list'] = authors_list

    # Attempt to extract OpenAlex ID from the 'id' field
    openalex_id = None
    input_id_str = entry_data['input_id_field']
    if input_id_str and isinstance(input_id_str, str):
        # Regex to find 'W' followed by 8-11 digits (common OpenAlex ID pattern)
        match = re.search(r'(W[0-9]{8,11})', input_id_str, re.IGNORECASE)
        if match:
            openalex_id = match.group(1).upper()
    entry_data['openalex_id'] = openalex_id
    
    # Add bibtex_key if present in JSON, though unlikely for this format
    entry_data['bibtex_key'] = json_obj.get('bibtex_key') 

    return entry_data

def process_json_directory(dbm: DatabaseManager, json_dir_path: Path):
    """Processes all JSON files in a given directory.

    Args:
        dbm: An instance of DatabaseManager.
        json_dir_path: Path to the directory containing JSON files.
    """
    total_files_processed = 0
    total_entries_considered = 0
    total_added_to_queue = 0
    total_duplicates_logged = 0
    total_errors = 0

    click.echo(f"Scanning directory: {json_dir_path}")
    json_files = list(json_dir_path.glob('*.json')) # Non-recursive for now

    if not json_files:
        click.echo(f"{YELLOW}No .json files found in {json_dir_path}{RESET}")
        return

    click.echo(f"Found {len(json_files)} JSON files to process.")

    for json_file_path in json_files:
        total_files_processed += 1
        click.echo(f"\nProcessing file: {json_file_path.name}...")
        file_added = 0
        file_duplicates = 0
        file_errors = 0
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                content = json.load(f)
            
            entries_to_process = []
            if isinstance(content, list):
                entries_to_process = content
            elif isinstance(content, dict):
                entries_to_process = [content] # Handle single JSON object in a file
            else:
                click.echo(f"{RED}  Error: File {json_file_path.name} does not contain a valid JSON list or object.{RESET}")
                total_errors +=1 # Count this as a file-level error for now
                continue

            if not entries_to_process:
                click.echo(f"{YELLOW}  No entries found in {json_file_path.name}.{RESET}")
                continue
            
            click.echo(f"  Found {len(entries_to_process)} potential entries in {json_file_path.name}.")

            for i, entry_obj in enumerate(entries_to_process):
                total_entries_considered +=1
                if not isinstance(entry_obj, dict):
                    click.echo(f"{RED}    Skipping item {i+1} in {json_file_path.name}: not a valid JSON object.{RESET}")
                    file_errors += 1
                    continue
                
                parsed_data = _parse_json_entry(entry_obj)
                status_code, item_id, message = dbm.add_entry_to_download_queue(parsed_data)

                if status_code == "added_to_queue":
                    file_added += 1
                elif status_code == "duplicate":
                    file_duplicates += 1
                    # Message already printed by db_manager for duplicates
                elif status_code == "error":
                    file_errors += 1
                    click.echo(f"{RED}    Error processing entry '{parsed_data.get('input_id_field', 'N/A')}' from {json_file_path.name}: {message}{RESET}")
            
            click.echo(f"  File {json_file_path.name} summary: Added: {file_added}, Duplicates: {file_duplicates}, Errors: {file_errors}")
            total_added_to_queue += file_added
            total_duplicates_logged += file_duplicates
            total_errors += file_errors

        except json.JSONDecodeError as e_json:
            click.echo(f"{RED}  Error decoding JSON from file {json_file_path.name}: {e_json}{RESET}")
            total_errors +=1 # Count as one error for the file
        except Exception as e_file:
            click.echo(f"{RED}  An unexpected error occurred processing file {json_file_path.name}: {e_file}{RESET}")
            total_errors +=1

    click.echo("\n--- JSON Import Summary ---")
    click.echo(f"Total files processed: {total_files_processed}")
    click.echo(f"Total entries considered: {total_entries_considered}")
    click.echo(f"{GREEN}Total entries added to download queue: {total_added_to_queue}{RESET}")
    click.echo(f"{YELLOW}Total duplicates logged: {total_duplicates_logged}{RESET}")
    if total_errors > 0:
        click.echo(f"{RED}Total errors encountered: {total_errors}{RESET}")
    else:
        click.echo(f"{GREEN}No errors encountered.{RESET}")

@cli.command("add-json")
@click.argument('directory_path', type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True))
@click.option('--db-path', 
              default=str(DEFAULT_DB_PATH), 
              help='Path to the SQLite database file.', 
              type=click.Path(dir_okay=False, writable=True))
def add_json_command(directory_path, db_path):
    """Processes JSON files from a directory and adds new entries to the download queue."""
    click.echo(f"Starting JSON import from directory: {directory_path}")
    db_manager = None
    try:
        db_manager = DatabaseManager(db_path=Path(db_path))
        process_json_directory(db_manager, Path(directory_path))
    except Exception as e:
        click.echo(f"{RED}An unexpected error occurred during JSON import setup: {e}{RESET}", err=True)
    finally:
        if db_manager:
            db_manager.close_connection()

@cli.command("process-queue")
@click.option('--db-path', default=str(DEFAULT_DB_PATH), help='Path to the SQLite database file.')
@click.option('--pdf-dir', required=True, type=click.Path(file_okay=False, dir_okay=True, writable=True, resolve_path=True), help='Directory to save downloaded PDFs.')
@click.option('--mailto', default='your.email@example.com', help='Email for API politeness headers.')
def process_queue_command(db_path, pdf_dir, mailto):
    """Processes the 'to_download_references' queue: fetches metadata and downloads PDFs."""
    db_manager = None
    actual_disk_db_path = Path(db_path).resolve()
    
    if not actual_disk_db_path.exists():
        click.echo(f"{RED}Database file not found at {actual_disk_db_path}. Please run init-db or import-bib first.{RESET}")
        return

    try:
        db_manager = DatabaseManager(db_path=actual_disk_db_path)
        
        fetcher = MetadataFetcher(mailto=mailto)
        downloader = PDFDownloader(download_dir=pdf_dir, mailto=mailto)

        queue_entries = db_manager.get_all_from_queue()
        if not queue_entries:
            click.echo(f"{GREEN}[CLI] Download queue is empty. Nothing to process.{RESET}")
            return

        click.echo(f"{YELLOW}[CLI] Found {len(queue_entries)} entries to process in the queue.{RESET}")

        for entry in queue_entries:
            queue_id = entry['id']
            click.echo(f"\n--- Processing Queue Entry ID: {queue_id}, Title: {entry.get('title', 'N/A')} ---")

            # 1. Fetch metadata
            enriched_data = fetcher.fetch_metadata(entry)
            if not enriched_data:
                reason = "Failed to fetch metadata from any source."
                db_manager.move_queue_entry_to_failed(queue_id, reason)
                continue

            # 2. Find PDF URL from metadata
            pdf_url = None
            primary_location = enriched_data.get('primary_location')
            if primary_location and isinstance(primary_location, dict):
                pdf_url = primary_location.get('pdf_url')
            
            if not pdf_url:
                reason = "Metadata found, but no PDF URL was available."
                db_manager.move_queue_entry_to_failed(queue_id, reason)
                continue

            # 3. Download PDF
            year = enriched_data.get('publication_year', 'UnknownYear')
            
            first_author_lastname = 'UnknownAuthor'
            authorships = enriched_data.get('authorships')
            if authorships and isinstance(authorships, list) and len(authorships) > 0:
                author_info = authorships[0].get('author')
                if author_info and isinstance(author_info, dict):
                    display_name = author_info.get('display_name')
                    if display_name and isinstance(display_name, str):
                        # Take last word of name as lastname
                        first_author_lastname = display_name.split()[-1]

            title_short = enriched_data.get('display_name', 'Untitled')[:40]
            # Sanitize for filesystem
            safe_title = re.sub(r'[\\/*?:"<>|]',"", title_short)
            filename = f"{first_author_lastname}{year}_{safe_title}.pdf"

            file_path, checksum, error_msg = downloader.download_pdf(pdf_url, filename)
            if error_msg:
                reason = f"PDF download failed: {error_msg}"
                db_manager.move_queue_entry_to_failed(queue_id, reason)
                continue

            # 4. Move to downloaded_references
            db_manager.move_queue_entry_to_downloaded(queue_id, enriched_data, str(file_path), checksum)

        click.echo(f"\n{GREEN}[CLI] Queue processing complete.{RESET}")

    except Exception as e:
        click.echo(f"{RED}An unexpected error occurred during queue processing: {e}{RESET}", err=True)
        traceback.print_exc()
    finally:
        if db_manager:
            db_manager.close_connection()

if __name__ == '__main__':
    cli()
