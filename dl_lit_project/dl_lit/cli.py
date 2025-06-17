import click
from pathlib import Path
from .db_manager import DatabaseManager # Relative import
import bibtexparser # For parsing BibTeX files
from .metadata_fetcher import MetadataFetcher

# ANSI escape codes for colors
RESET = "\033[0m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
WHITE = "\033[97m"
from .pdf_downloader import PDFDownloader
from .bibtex_formatter import BibTeXFormatter # Added for export
from .get_bib_pages import extract_reference_sections # Added for bibliography extraction
from .APIscraper_v2 import configure_api_client, process_directory_v2, process_single_pdf # Added for API scraping
from .OpenAlexScraper import OpenAlexCrossrefSearcher, process_single_file, process_bibliography_files # Added for OpenAlex scraping
import os # Added for API scraping and OpenAlexScraper path checks
from pathlib import Path # Already imported, but ensure it's available for OpenAlexScraper logic if needed directly in CLI
from threading import Lock # Added for API scraping
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
@click.option('--limit', default=10, help='Number of entries to show. Shows all if 0.')
@click.option('--show-all', is_flag=True, help='Show all entries, overrides --limit.')
def show_sample_command(db_path, limit, show_all):
    """Shows entries from the downloaded_references table."""
    db_manager = DatabaseManager(db_path=db_path)
    try:
        click.echo(f"{CYAN}Fetching entries from downloaded_references...{RESET}")
        all_entries_dicts = db_manager.get_all_downloaded_references_as_dicts()
        
        if not all_entries_dicts:
            click.echo(f"{YELLOW}No entries found in downloaded_references.{RESET}")
            return

        display_limit = len(all_entries_dicts) if show_all else limit
        if display_limit == 0 and not show_all: # if limit is 0 and show_all is not set, show all
            display_limit = len(all_entries_dicts)

        click.echo(f"{GREEN}--- Downloaded References (showing up to {display_limit if display_limit < len(all_entries_dicts) else 'all'}) ---{RESET}")
        
        for i, entry_dict in enumerate(all_entries_dicts):
            if i >= display_limit and not show_all and limit != 0:
                break
            click.echo(f"{BLUE}--- Entry {i+1} ---{RESET}")
            click.echo(f"  {WHITE}ID:{RESET} {entry_dict.get('id')}")
            click.echo(f"  {WHITE}BibTeX Key:{RESET} {entry_dict.get('bibtex_key')}")
            click.echo(f"  {WHITE}Title:{RESET} {entry_dict.get('title')}")
            click.echo(f"  {WHITE}DOI:{RESET} {entry_dict.get('doi')}")
            click.echo(f"  {WHITE}OpenAlex ID:{RESET} {entry_dict.get('openalex_id')}")
            click.echo(f"  {WHITE}PDF Path:{RESET} {entry_dict.get('file_path')}")
            click.echo(f"  {WHITE}Year:{RESET} {entry_dict.get('year')}")
            authors_json = entry_dict.get('authors')
            authors_list = []
            if authors_json:
                try:
                    authors_list = json.loads(authors_json)
                except json.JSONDecodeError:
                    authors_list = [authors_json] # Treat as single author if not JSON
            click.echo(f"  {WHITE}Authors:{RESET} {', '.join(authors_list) if authors_list else 'N/A'}")
            click.echo(f"  {WHITE}Journal/Conf:{RESET} {entry_dict.get('journal_conference')}")
            click.echo(f"  {WHITE}Date Processed:{RESET} {entry_dict.get('date_processed')}")
        if display_limit < len(all_entries_dicts) and (limit != 0 or not show_all) :
            click.echo(f"{YELLOW}... and {len(all_entries_dicts) - display_limit} more entries not shown. Use --show-all or increase --limit.{RESET}")

    except Exception as e:
        click.echo(f"{RED}Error showing entries: {e}{RESET}")
    finally:
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
        click.echo(f"Using PDF base directory: {pdf_base_dir}")
    
    db_manager = None # Initialize to ensure it's defined for finally block
    actual_disk_db_path = Path(db_path) # The final destination for the data
    try:
        # Initialize with an in-memory database for fast processing
        db_manager = DatabaseManager(db_path=":memory:") 
        click.echo(f"Processing import into in-memory database. Target disk DB: {actual_disk_db_path.resolve()}")

        # Load existing data from disk into the in-memory DB if the disk DB exists
        if actual_disk_db_path.exists() and actual_disk_db_path.stat().st_size > 0:
            click.echo(f"Loading existing data from {actual_disk_db_path.resolve()} into memory...")
            load_success = db_manager.load_from_disk(actual_disk_db_path)
            if load_success:
                click.echo("Successfully loaded existing data into memory.")
            else:
                click.echo(f"{RED}Failed to load existing data from {actual_disk_db_path.resolve()} into memory. Proceeding with empty in-memory DB.{RESET}")
        else:
            click.echo(f"No existing database found at {actual_disk_db_path.resolve()} or it is empty. Starting fresh in memory.")

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
        duplicates_logged = 0 # For tracking logged duplicates

        for entry_obj in bib_database.entries: # entry_obj is a bibtexparser.model.Entry
            # Determine a display identifier for the entry for logging purposes
            entry_display_identifier = "Unknown Entry"
            if entry_obj.key: # Use .key for the BibTeX entry ID
                entry_display_identifier = entry_obj.key
            elif hasattr(entry_obj, 'fields_dict'):
                title_field = entry_obj.fields_dict.get('title')
                if title_field and hasattr(title_field, 'value'):
                    entry_display_identifier = title_field.value
                elif title_field: # if title_field is a simple string (less common with bibtexparser v2)
                    entry_display_identifier = str(title_field)
            
            if isinstance(entry_display_identifier, str):
                 entry_display_identifier = entry_display_identifier.strip('{}')

            
            # Extract DOI and OpenAlex ID
            doi_to_check = None
            openalex_id_to_check = None

            if hasattr(entry_obj, 'fields_dict'):
                # Check for DOI (common casings: 'doi', 'DOI')
                doi_field_obj = entry_obj.fields_dict.get('doi') or entry_obj.fields_dict.get('DOI')
                if doi_field_obj and hasattr(doi_field_obj, 'value'):
                    doi_to_check = doi_field_obj.value
                elif doi_field_obj: # Should be a Field object, but value might be missing or it's just a string
                    doi_to_check = str(doi_field_obj) 

                # Check for OpenAlex ID (common casings: 'openalex', 'OPENALEX', 'openalexid')
                openalex_field_obj = entry_obj.fields_dict.get('openalex') or \
                                     entry_obj.fields_dict.get('OPENALEX') or \
                                     entry_obj.fields_dict.get('openalexid')
                if openalex_field_obj and hasattr(openalex_field_obj, 'value'):
                    openalex_id_to_check = openalex_field_obj.value
                elif openalex_field_obj:
                    openalex_id_to_check = str(openalex_field_obj)


            # 2. Check for duplicates
            table_name, existing_id, matched_field = db_manager.check_if_exists(
                doi=doi_to_check, 
                openalex_id=openalex_id_to_check
            )

            if table_name and existing_id is not None:
                # 3. It's a duplicate, log it
                dup_log_id, dup_log_err = db_manager.add_entry_to_duplicates(
                    entry=entry_obj,  # Corrected: entry_data_obj -> entry
                    input_doi=doi_to_check, # Added
                    input_openalex_id=openalex_id_to_check, # Added
                    source_bibtex_file=str(Path(bibtex_file).resolve()),
                    existing_entry_id=existing_id, # Corrected: duplicate_of_id -> existing_entry_id
                    existing_entry_table=table_name, # Corrected: duplicate_of_table -> existing_entry_table
                    matched_on_field=matched_field
                    # Removed entry_source_type as it's handled in db_manager
                )
                if dup_log_id:
                    duplicates_logged += 1
                else:
                    click.echo(f"{RED}  Error logging duplicate for '{entry_display_identifier}': {dup_log_err}{RESET}")
                    failed_imports.append((str(entry_display_identifier)[:100], f"Duplicate logging error: {dup_log_err}"))
                continue 

            # 4. Not a duplicate, try to add to downloaded_references
            original_entry_json = json.dumps({
                'ID': entry_obj.key, 
                'ENTRYTYPE': entry_obj.entry_type, 
                **{k: v.value if hasattr(v, 'value') else str(v) for k, v in entry_obj.fields_dict.items()}
            })
            pdf_dir_path = Path(pdf_base_dir) if pdf_base_dir else None # pdf_base_dir is the function param from import_bib_command

            entry_id, error_msg = db_manager.add_bibtex_entry_to_downloaded(
                entry=entry_obj,
                input_doi=doi_to_check,
                input_openalex_id=openalex_id_to_check,
                pdf_base_dir=pdf_dir_path, # Use the Path object or None
                original_bibtex_entry_json=original_entry_json,
                source_bibtex_file=str(Path(bibtex_file).resolve()) # Use the correct parameter bibtex_file
            )
            
            if entry_id:
                successful_imports += 1
            else:
                reason = error_msg if error_msg else "Unknown import error"
                failed_imports.append((str(entry_display_identifier)[:100], reason))
        
        click.echo("\n--- Import Summary ---")
        click.echo(f"{GREEN}Successfully imported: {successful_imports} new entries{RESET}")
        
        if duplicates_logged > 0:
            click.echo(f"{YELLOW}Duplicates found and logged: {duplicates_logged} entries{RESET}")

        if failed_imports:
            click.echo(f"{RED}Failed to import (or log as duplicate): {len(failed_imports)} entries{RESET}")
            for i, (item_id, reason) in enumerate(failed_imports):
                click.echo(f"  {RED}Failure {i+1}: {item_id} - Reason: {reason}{RESET}")
        elif successful_imports > 0 and duplicates_logged == 0:
             click.echo(f"{GREEN}No import failures or duplicates encountered among processed entries.{RESET}")
        elif successful_imports == 0 and duplicates_logged == 0 and not failed_imports and len(bib_database.entries) > 0 :
             click.echo(f"{YELLOW}No new entries were imported, and no duplicates found among the {len(bib_database.entries)} processed entries.{RESET}")
        elif len(bib_database.entries) == 0:
            pass # Already handled by "No entries found in the BibTeX file."
        elif successful_imports == 0 and duplicates_logged > 0 and not failed_imports:
             click.echo(f"{GREEN}All processed entries were identified and logged as duplicates. No new entries imported.{RESET}")

        click.echo("\n>>> CLI: Attempting to call db_manager.save_to_disk() now. <<<")
        save_successful = db_manager.save_to_disk(actual_disk_db_path)
        click.echo(f">>> CLI: db_manager.save_to_disk() returned: {save_successful} <<<")

        if save_successful:
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

@cli.command("export-bibtex")
@click.argument('output_bib_file', type=click.Path(dir_okay=False, writable=True, resolve_path=True))
@click.option('--db-path', 
              default=str(DEFAULT_DB_PATH), 
              help='Path to the SQLite database file.', 
              type=click.Path(exists=True, dir_okay=False, readable=True, resolve_path=True))
@click.option('--skip-pdf-check', is_flag=True, default=False, help="Skip checking if the PDF file exists on disk.")
def export_bibtex_command(output_bib_file, db_path, skip_pdf_check):
    """Exports the 'downloaded_references' table to a BibTeX file."""
    click.echo(f"Exporting downloaded references from '{Path(db_path).resolve()}' to '{Path(output_bib_file).resolve()}'...")
    
    db_manager = None
    output_file = Path(output_bib_file)
    output_file.parent.mkdir(parents=True, exist_ok=True) # Ensure output directory exists

    try:
        db_manager = DatabaseManager(db_path=Path(db_path))
        formatter = BibTeXFormatter()

        all_entries_dicts = db_manager.get_all_downloaded_references_as_dicts()
        
        # DEBUG PRINT START
        # click.echo(f"[CLI DEBUG] Retrieved {len(all_entries_dicts)} entries from DB for export.", err=True)
        # if all_entries_dicts and len(all_entries_dicts) > 0 and isinstance(all_entries_dicts[0], dict):
        #     click.echo(f"[CLI DEBUG] First entry ID: {all_entries_dicts[0].get('id')}, (first 50 chars of bibtex_entry_json): {str(all_entries_dicts[0].get('bibtex_entry_json', ''))[:50]}...", err=True)
        # elif all_entries_dicts: # If it's not empty but not a list of dicts or first item isn't a dict
        #     click.echo(f"[CLI DEBUG] all_entries_dicts is not empty but first item is not a dict or has no 'id'/'bibtex_entry_json'. Type: {type(all_entries_dicts[0])}", err=True)
        # DEBUG PRINT END
            
        if not all_entries_dicts:
            click.echo(f"{YELLOW}No entries found in 'downloaded_references' table to export.{RESET}")
            if db_manager:
                db_manager.close_connection()
            return

        exported_count = 0 # Renamed from 'count' for clarity
        skipped_count = 0  # Renamed from 'failed_count' for clarity
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for entry_dict in all_entries_dicts: # CORRECTED: Use all_entries_dicts
                # The BibTeXFormatter.format_entry will handle missing/invalid file_paths
                # by omitting the 'file' field. No need for pre-filtering here.
                bibtex_string = formatter.format_entry(entry_dict)
                if bibtex_string:
                    f.write(bibtex_string + "\n\n")
                    exported_count += 1
                else:
                    skipped_count += 1
                    # Debug messages from formatter.format_entry should indicate why it failed.
        
        click.echo(f"\n--- Export Summary ---")
        if exported_count > 0:
            click.echo(f"{GREEN}Successfully exported: {exported_count} entries to {output_file}{RESET}")
        else:
            if skipped_count > 0 and len(all_entries_dicts) > 0:
                 click.echo(f"{RED}Exported 0 entries. All {skipped_count} processed entries failed to format.{RESET}")
            elif not all_entries_dicts: # Should be caught by the earlier check
                 click.echo(f"{YELLOW}No entries were found in the database to export.{RESET}")
            else: 
                 click.echo(f"{YELLOW}Exported 0 entries. No entries were successfully formatted or no entries to process.{RESET}")
        
        if skipped_count > 0:
            click.echo(f"{YELLOW}Skipped or failed to format: {skipped_count} entries{RESET}")
        elif exported_count > 0 and skipped_count == 0 and len(all_entries_dicts) > 0:
            click.echo(f"{GREEN}All {exported_count} entries formatted and exported successfully.{RESET}")
        
    except Exception as e:
        click.echo(f"{RED}An unexpected error occurred during BibTeX export: {e}{RESET}", err=True)
        # import traceback
        # traceback.print_exc()
    finally:
        if db_manager:
            db_manager.close_connection()


@cli.command("extract-bib-pages")
@click.argument('pdf_file_path', type=click.Path(exists=True, dir_okay=False, readable=True, resolve_path=True))
@click.option('--output-dir', 
              type=click.Path(file_okay=False, writable=True, resolve_path=True),
              default=str(Path.home() / "Nextcloud/DT/papers_extracted_references"), 
              help='Directory to save extracted reference PDFs. Defaults to ~/Nextcloud/DT/papers_extracted_references.')
def extract_bib_pages_command(pdf_file_path, output_dir):
    """Extracts bibliography/reference section pages from a PDF file."""
    click.echo(f"Attempting to extract bibliography pages from: {pdf_file_path}")
    click.echo(f"Output will be saved to: {output_dir}")
    try:
        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        extract_reference_sections(str(pdf_file_path), str(output_dir))
        click.echo(f"{GREEN}Bibliography page extraction process completed.{RESET}")
    except Exception as e:
        click.echo(f"{RED}An error occurred during bibliography page extraction: {e}{RESET}", err=True)
        # For more detailed debugging, you might want to print the traceback
        # import traceback
        # traceback.print_exc()

@cli.command("extract-bib-api")
@click.option('--input-path', 
              type=click.Path(exists=True, readable=True),
              required=True,
              help='Path to the input PDF file or directory containing PDF files.')
@click.option('--output-dir', 
              type=click.Path(file_okay=False, writable=True),
              required=True,
              help='Directory to save output JSON bibliography files.')
@click.option('--workers', 
              type=int, 
              default=5, 
              show_default=True,
              help='Maximum number of concurrent worker threads.')
def extract_bib_api_command(input_path, output_dir, workers):
    """Extracts bibliographies from PDF(s) using an API and saves them as JSON files."""
    click.echo(f"{CYAN}--- Starting API Bibliography Extraction ---{RESET}")
    click.echo(f"Input path: {input_path}")
    click.echo(f"Output directory: {output_dir}")
    click.echo(f"Max workers: {workers}")

    # Ensure output directory exists
    try:
        os.makedirs(output_dir, exist_ok=True)
        click.echo(f"Ensured output directory exists: {Path(output_dir).resolve()}")
    except Exception as e:
        click.echo(f"{RED}Error creating output directory {output_dir}: {e}{RESET}", err=True)
        return

    click.echo("Configuring API client...")
    if not configure_api_client():
        click.echo(f"{RED}Failed to configure API client. Please check your GOOGLE_API_KEY and .env file.{RESET}", err=True)
        click.echo(f"{RED}Exiting due to API configuration failure.{RESET}", err=True)
        return
    click.echo(f"{GREEN}API client configured successfully.{RESET}")

    input_p = Path(input_path)
    output_d = Path(output_dir)

    try:
        if input_p.is_dir():
            click.echo(f"Processing directory: {input_p.resolve()}")
            process_directory_v2(str(input_p.resolve()), str(output_d.resolve()), workers)
        elif input_p.is_file() and input_p.name.lower().endswith('.pdf'):
            click.echo(f"Processing single PDF file: {input_p.resolve()}")
            # For single_pdf, process_single_pdf expects a summary_dict and a lock
            summary = {'processed_files': 0, 'successful_files': 0, 'failed_files': 0, 'failures': []}
            summary_lock = Lock()
            process_single_pdf(str(input_p.resolve()), str(output_d.resolve()), summary, summary_lock)
            # Print summary for single file processing
            click.echo(f"\n--- Single File Processing Summary ---")
            click.echo(f"File processed: {summary['processed_files']}")
            click.echo(f"Successful extraction: {summary['successful_files']}")
            click.echo(f"Failed extraction: {summary['failed_files']}")
            if summary['failures']:
                click.echo(f"{RED}Failure: {summary['failures'][0]['reason']}{RESET}")
            click.echo("-------------------------")
        else:
            click.echo(f"{RED}Error: Input path {input_path} is neither a valid directory nor a PDF file.{RESET}", err=True)
            return
        
        click.echo(f"\n{GREEN}--- API Bibliography Extraction Completed ---{RESET}")
        click.echo(f"Output JSON files should be in: {output_d.resolve()}")

    except Exception as e:
        click.echo(f"{RED}An unexpected error occurred during API bibliography extraction: {e}{RESET}", err=True)
        click.echo(traceback.format_exc(), err=True)

@cli.command("enrich-openalex")
@click.option('--input-path', 
              type=click.Path(exists=True, readable=True, resolve_path=True),
              required=True, 
              help='Path to the input JSON file or directory containing JSON files to enrich.')
@click.option('--output-dir', 
              type=click.Path(file_okay=False, writable=True, resolve_path=True),
              required=True, 
              help='Directory to save the enriched JSON output files.')
@click.option('--mailto', 
              type=str, 
              required=True, 
              help='Your email address for OpenAlex API politeness (User-Agent).')
@click.option('--fetch-citations/--no-fetch-citations', 
              default=True, 
              show_default=True, 
              help='Fetch citing work IDs from OpenAlex for matched entries.')
def enrich_openalex_command(input_path, output_dir, mailto, fetch_citations):
    """Enriches bibliography JSON files with data from OpenAlex and Crossref."""
    click.echo(f"{CYAN}Starting OpenAlex enrichment process...{RESET}")
    click.echo(f"Input path: {input_path}")
    click.echo(f"Output directory: {output_dir}")
    click.echo(f"Mailto (for API): {mailto}")
    click.echo(f"Fetch citations: {'Enabled' if fetch_citations else 'Disabled'}")

    input_p = Path(input_path)
    output_p = Path(output_dir)

    try:
        output_p.mkdir(parents=True, exist_ok=True)
        click.echo(f"Ensured output directory exists: {output_p.resolve()}")

        searcher = OpenAlexCrossrefSearcher(mailto=mailto)
        click.echo("OpenAlexCrossrefSearcher initialized.")

        if input_p.is_file():
            if input_p.suffix.lower() != '.json':
                click.echo(f"{RED}Error: Input file must be a .json file.{RESET}")
                return
            click.echo(f"Processing single file: {input_p.name}")
            process_single_file(str(input_p), str(output_p), searcher, fetch_citations)
        elif input_p.is_dir():
            click.echo(f"Processing directory: {input_p.resolve()}")
            process_bibliography_files(str(input_p), str(output_p), searcher, fetch_citations)
        else:
            click.echo(f"{RED}Error: Input path is neither a file nor a directory.{RESET}")
            return
        
        click.echo(f"{GREEN}OpenAlex enrichment process completed.{RESET}")
        click.echo(f"Output saved to: {output_p.resolve()}")

    except Exception as e:
        click.echo(f"{RED}An error occurred during OpenAlex enrichment: {e}{RESET}")
        click.echo(f"{YELLOW}Traceback:\n{traceback.format_exc()}{RESET}")

if __name__ == '__main__':
    cli()
