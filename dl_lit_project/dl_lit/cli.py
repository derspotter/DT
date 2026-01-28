import click
import json
import os
import logging
import traceback
import sqlite3
from pathlib import Path
from datetime import datetime
import pprint
import copy
from tqdm import tqdm
from threading import Lock
import hashlib

# Local application imports
from .db_manager import DatabaseManager
# For tests: patched in test_get_bib_pages. Lazy import in handler for runtime.
extract_reference_sections = None
from .OpenAlexScraper import OpenAlexCrossrefSearcher, process_single_reference
from .new_dl import BibliographyEnhancer
from .utils import get_global_rate_limiter
from .pdf_downloader import PDFDownloader
from .bibtex_formatter import BibTeXFormatter
from .APIscraper_v2 import process_directory_v2, process_single_pdf

# ANSI escape codes for colors
RESET = "\033[0m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
WHITE = "\033[97m"

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
@click.argument('input_path', type=click.Path(exists=True, file_okay=True, dir_okay=True, readable=True, resolve_path=True))
@click.option('--output-dir', 
              type=click.Path(file_okay=False, writable=True, resolve_path=True),
              default=str(Path.home() / "Nextcloud/DT/papers_extracted_references"), 
              help='Directory to save extracted reference PDFs. Defaults to ~/Nextcloud/DT/papers_extracted_references.')
def extract_bib_pages_command(input_path, output_dir):
    """Extracts bibliography/reference section pages from a PDF file or all PDFs in a directory."""
    # Lazy import to avoid debug prints at module level
    global extract_reference_sections
    if extract_reference_sections is None:
        from .get_bib_pages import extract_reference_sections as _extract_reference_sections
        extract_reference_sections = _extract_reference_sections
    
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    if input_path.is_dir():
        pdf_files = list(input_path.glob('**/*.pdf'))
        click.echo(f"Found {len(pdf_files)} PDF(s) in directory: {input_path}")
        if not pdf_files:
            click.echo("No PDF files to process.")
            return
    else:
        pdf_files = [input_path]

    for pdf_file in pdf_files:
        click.echo(f"\n--- Processing: {pdf_file.name} ---")
        try:
            extract_reference_sections(str(pdf_file), str(output_dir))
            click.echo(f"{GREEN}Successfully processed {pdf_file.name}.{RESET}")
        except Exception as e:
            click.echo(f"{RED}An error occurred while processing {pdf_file.name}: {e}{RESET}", err=True)

    click.echo(f"\n{GREEN}Batch bibliography page extraction process completed.{RESET}")

@cli.command("add-to-no-metadata")
@click.option('--json-file', 
              type=click.Path(exists=True, dir_okay=False, readable=True), 
              required=True,
              help='Path to the JSON file with extracted references.')
@click.option('--source-pdf', 
              type=click.Path(exists=True, dir_okay=False, readable=True), 
              required=False, 
              help='Path to the original source PDF file.')
@click.option('--db-path', 
              default=str(DEFAULT_DB_PATH), 
              help='Path to the SQLite database file.')
def add_to_no_metadata_command(json_file, source_pdf, db_path):
    """Loads extracted bibliography entries from a JSON file into the no_metadata table."""
    click.echo(f"Loading entries from {json_file} into the database...")
    db_manager = DatabaseManager(db_path)
    try:
        added, skipped, errors = db_manager.add_entries_to_no_metadata_from_json(json_file, source_pdf)
        click.echo(f"{GREEN}Successfully added {added} new entries.{RESET}")
        if skipped > 0:
            click.echo(f"{YELLOW}Skipped {skipped} entries (duplicates or missing title).{RESET}")
        if errors:
            click.echo(f"{RED}Encountered {len(errors)} errors:{RESET}")
            for error in errors:
                click.echo(f"  - {error}")
    except Exception as e:
        click.echo(f"{RED}An unexpected error occurred: {e}{RESET}")
    finally:
        db_manager.close_connection()
        # traceback.print_exc()

@cli.command("extract-bib-api")
@click.argument('input_path', type=click.Path(exists=True, resolve_path=True))
@click.option('--db-path',
              type=click.Path(dir_okay=False, writable=True, resolve_path=True),
              default=str(DEFAULT_DB_PATH),
              help='Path to the SQLite database file to save results.')
@click.option('--workers', type=int, default=10, help='Number of concurrent workers.')
def extract_bib_api_command(input_path, db_path, workers):
    """Extracts bibliographies from PDF(s) using an API and saves them directly to the database."""
    input_path = Path(input_path)
    db_manager = None

    try:
        # Import and configure API client for APIscraper_v2
        from .APIscraper_v2 import configure_api_client
        if not configure_api_client():
            click.echo(f"{RED}API client configuration failed. Please check your GOOGLE_API_KEY.{RESET}", err=True)
            return
        click.echo("Google API client configured successfully.")

        # Initialize DatabaseManager
        db_manager = DatabaseManager(db_path=db_path)
        click.echo(f"Database connection established at: {db_path}")

        # Rate limiting and summary logic is now handled inside the scraper functions.

        if input_path.is_dir():
            click.echo(f"Processing all PDFs in directory: {input_path}")
            process_directory_v2(
                input_dir=input_path,
                db_manager=db_manager,
                max_workers=workers
            )
        elif input_path.is_file() and input_path.suffix.lower() == '.pdf':
            click.echo(f"Processing single PDF file: {input_path}")
            # For a single file, we create a summary dict and lock for the function call.
            summary = {'processed_files': 0, 'successful_files': 0, 'failed_files': 0, 'failures': []}
            lock = Lock()
            process_single_pdf(
                pdf_path=input_path,
                db_manager=db_manager,
                summary_dict=summary,
                summary_lock=lock
            )
            # Optionally, print the summary for the single file for better user feedback
            click.echo("\n--- Single File Processing Summary ---")
            success_count = summary.get('successful_files', 0)
            failure_count = summary.get('failed_files', 0)
            click.echo(f"Status: {'Success' if success_count > 0 else 'Failed'}")
            if failure_count > 0 and summary.get('failures'):
                click.echo(f"Reason: {summary['failures'][0].get('reason', 'Unknown')}")
            click.echo("------------------------------------")
        else:
            click.echo(f"{RED}Error: Input path must be a valid directory or a .pdf file.{RESET}", err=True)

        click.echo(f"{GREEN}API-based bibliography extraction process completed.{RESET}")

    except Exception as e:
        click.echo(f"{RED}An error occurred during the process: {e}{RESET}", err=True)
        traceback.print_exc()
    finally:
        if db_manager:
            db_manager.close_connection()
            click.echo("Database connection closed.")


@cli.command("run-pipeline")
@click.argument('input_path', type=click.Path(exists=True, resolve_path=True))
@click.option('--db-path',
              type=click.Path(dir_okay=False, writable=True, resolve_path=True),
              default=str(DEFAULT_DB_PATH),
              help='Path to the SQLite database file.')
@click.option('--output-dir',
              type=click.Path(file_okay=False, writable=True, resolve_path=True),
              default=str(Path.cwd() / "extracted_refs"),
              help='Directory to store extracted reference pages.')
@click.option('--json-dir',
              type=click.Path(file_okay=False, writable=True, resolve_path=True),
              default=None,
              help='Directory to store extracted bibliography JSON files.')
@click.option('--batch-size', default=50, show_default=True, help='Batch size for enrichment.')
@click.option('--queue-batch', default=50, show_default=True, help='Batch size for enqueueing downloads.')
@click.option('--mailto', default='spott@wzb.eu', show_default=True, help='Email for API politeness.')
@click.option('--fetch-references/--no-fetch-references', default=True, show_default=True, help='Fetch referenced works during enrichment.')
@click.option('--fetch-citations/--no-fetch-citations', default=False, show_default=True, help='Fetch citing works during enrichment.')
@click.option('--max-citations', default=100, show_default=True, help='Maximum citing works to fetch per paper.')
@click.option('--max-ref-pages', type=int, default=None, help='Limit the number of extracted reference pages per PDF.')
@click.option('--max-entries', type=int, default=None, help='Limit the number of extracted entries processed per run.')
@click.option('--enrich/--no-enrich', default=True, show_default=True, help='Run OpenAlex/Crossref enrichment after insertion.')
def run_pipeline_command(input_path, db_path, output_dir, json_dir, batch_size, queue_batch, mailto, fetch_references, fetch_citations, max_citations, max_ref_pages, max_entries, enrich):
    """Run extract → enrich → queue pipeline for PDFs (no download step)."""
    try:
        from .pipeline import run_pipeline

        summary = run_pipeline(
            input_path=input_path,
            db_path=db_path,
            output_dir=output_dir,
            json_dir=json_dir,
            batch_size=batch_size,
            queue_batch=queue_batch,
            mailto=mailto,
            fetch_references=fetch_references,
            fetch_citations=fetch_citations,
            max_citations=max_citations,
            max_ref_pages=max_ref_pages,
            max_entries=max_entries,
            run_enrichment=enrich,
        )

        click.echo("\n--- Pipeline Summary ---")
        for key in [
            "processed_files",
            "extracted_pages",
            "extracted_entries",
            "inserted_entries",
            "skipped_entries",
            "enriched_promoted",
            "enriched_failed",
            "queued",
            "skipped_queue",
        ]:
            click.echo(f"{key.replace('_', ' ').title()}: {summary.get(key, 0)}")
        if summary.get("errors"):
            click.echo(f"{RED}Errors:{RESET}")
            for err in summary["errors"]:
                click.echo(f"  - {err}")
        click.echo(f"{GREEN}Pipeline complete.{RESET}")
    finally:
        pass


@cli.command("keyword-search")
@click.option('--query', required=True, help='Boolean keyword query (AND/OR/NOT).')
@click.option('--max-results', default=200, show_default=True, help='Maximum number of OpenAlex results to fetch.')
@click.option('--year-from', type=int, default=None, help='Filter: publication year >= year-from.')
@click.option('--year-to', type=int, default=None, help='Filter: publication year <= year-to.')
@click.option('--mailto', default='spott@wzb.eu', show_default=True, help='Email for API politeness.')
@click.option('--db-path',
              type=click.Path(dir_okay=False, writable=True, resolve_path=True),
              default=str(DEFAULT_DB_PATH),
              help='Path to the SQLite database file.')
@click.option('--enqueue/--no-enqueue', default=False, show_default=True, help='Enqueue results for download.')
def keyword_search_command(query, max_results, year_from, year_to, mailto, db_path, enqueue):
    """Run a keyword search against OpenAlex and persist results."""
    from .keyword_search import search_openalex, openalex_result_to_record, dedupe_results

    db = DatabaseManager(db_path=db_path)
    try:
        filters = {
            "year_from": year_from,
            "year_to": year_to,
            "max_results": max_results,
            "mailto": mailto,
        }
        run_id = db.create_search_run(query=query, filters=filters)
        click.echo(f"Search run created (id={run_id}). Fetching results...")

        raw_results = search_openalex(
            query=query,
            max_results=max_results,
            year_from=year_from,
            year_to=year_to,
            mailto=mailto,
        )

        records = [openalex_result_to_record(item, run_id=run_id) for item in raw_results]
        records = dedupe_results(records)

        stored = db.add_search_results(run_id, [
            {
                "openalex_id": r.get("openalex_id"),
                "doi": r.get("doi"),
                "title": r.get("title"),
                "year": r.get("year"),
                "raw_json": r.get("openalex_json"),
            }
            for r in records
        ])
        click.echo(f"Stored {stored} search results.")

        if enqueue:
            added = 0
            skipped = 0
            for record in records:
                status, _, _ = db.add_entry_to_download_queue(record)
                if status == "added_to_queue":
                    added += 1
                else:
                    skipped += 1
            click.echo(f"Enqueue complete: {added} added, {skipped} skipped.")
        click.echo(f"{GREEN}Keyword search complete.{RESET}")
    finally:
        db.close_connection()
@cli.command("enrich-openalex-db")
@click.option('--db-path', default=str(DEFAULT_DB_PATH), help='Path to the SQLite database file.')
@click.option('--batch-size', default=50, help='Number of entries to process in one batch.')
@click.option('--mailto', default='spott@wzb.eu', help='Email for API politeness.')
@click.option('--fetch-references/--no-fetch-references', default=True, show_default=True, help='Fetch referenced works for enriched papers.')
@click.option('--fetch-citations/--no-fetch-citations', default=False, show_default=True, help='Fetch citing works for enriched papers (API intensive).')
@click.option('--max-citations', default=100, show_default=True, help='Maximum number of citing works to fetch per paper.')
def enrich_openalex_db_command(db_path, batch_size, mailto, fetch_references, fetch_citations, max_citations):
    """Fetches metadata from OpenAlex/Crossref for entries in the no_metadata table."""
    click.echo(f"Starting DB enrichment – DB: {db_path} | Batch: {batch_size}")
    db_manager = DatabaseManager(db_path)
    
    # Use the more advanced searcher from OpenAlexScraper
    searcher = OpenAlexCrossrefSearcher(mailto=mailto)
    # Use the global rate limiter shared across all components
    rate_limiter = get_global_rate_limiter() 

    promoted_count = 0
    failed_count = 0
    
    try:
        entries_to_process = db_manager.fetch_no_metadata_batch(batch_size)
        if not entries_to_process:
            click.echo("No entries found in 'no_metadata' table to process.")
            return

        for entry in entries_to_process:
            click.echo(f"\n[ENRICH] Processing ID {entry['id']} – {entry['title'][:50]}")
            
            # Adapt the DB entry to the format expected by process_single_reference
            ref_for_scraper = {
                'title': entry.get('title'),
                'authors': entry.get('authors') if entry.get('authors') else [],
                'doi': entry.get('doi'),
                'year': entry.get('year'),  # Use extracted year
                'container-title': entry.get('source'),  # Use extracted source/journal
                'volume': entry.get('volume'),
                'issue': entry.get('issue'),
                'pages': entry.get('pages'),
                'abstract': entry.get('abstract'),
                'keywords': entry.get('keywords')
            }

            # Use the powerful multi-step search process with related works options
            enriched_data = process_single_reference(
                ref_for_scraper, searcher, rate_limiter, 
                fetch_references=fetch_references,
                fetch_citations=fetch_citations,
                max_citations=max_citations
            )
            
            if enriched_data:
                db_manager.promote_to_with_metadata(entry['id'], enriched_data)
                promoted_count += 1
                click.echo(f"  -> metadata found, promoted to with_metadata.")
                # Pretty print the enriched data for immediate review
                click.echo(f"{BLUE}--- Enriched Data ---{RESET}")
                click.echo(pprint.pformat(enriched_data))
                click.echo(f"{BLUE}-----------------------{RESET}")
            else:
                reason = "Metadata fetch failed (no match found in OpenAlex/Crossref)"
                failed_id, error_msg = db_manager.move_no_meta_entry_to_failed(entry['id'], reason)
                if failed_id:
                    click.echo(f"  ! metadata fetch failed, moved to failed_enrichments.")
                else:
                    click.echo(f"  ! FAILED to move entry {entry['id']} to failed_enrichments: {error_msg}")
                failed_count += 1

    except Exception as e:
        click.echo(f"{RED}An unexpected error occurred during enrichment: {e}{RESET}")
        import traceback
        traceback.print_exc()
    finally:
        click.echo(f"\nEnrichment finished: {promoted_count} promoted, {failed_count} failed.")
        db_manager.close_connection()


@cli.command("retry-failed-enrichments")
@click.option('--db-path', default=str(DEFAULT_DB_PATH), help='Path to the SQLite database file.')
def retry_failed_enrichments_command(db_path):
    """Moves all entries from failed_enrichments back to no_metadata."""
    click.echo("Retrying failed enrichments...")
    db_manager = DatabaseManager(db_path)
    try:
        moved_count, error = db_manager.retry_failed_enrichments()
        if error:
            click.echo(f"{RED}An error occurred: {error}{RESET}")
        # The success message is already printed by the db_manager method
    finally:
        db_manager.close_connection()


@cli.command("process-downloads")
@click.option('--db-path', default=str(DEFAULT_DB_PATH), type=click.Path(dir_okay=False, writable=True, resolve_path=True), help='Path to SQLite database file.')
@click.option('--batch-size', default=50, show_default=True, help='Max number of with_metadata rows to enqueue.')
def process_downloads_command(db_path, batch_size):
    click.echo("!!!!!!!!!! DEBUG: ATTEMPTING TO RUN PROCESS-DOWNLOADS !!!!!!!!!!")
    """Move enriched references into the download queue (`to_download_references`)."""
    click.echo(f"Queueing up to {batch_size} items from with_metadata…")

    db = DatabaseManager(db_path)
    ids = db.fetch_with_metadata_batch(limit=batch_size)
    if not ids:
        click.echo("No items in with_metadata.")
        db.close_connection()
        return

    added = 0
    skipped = 0
    for wid in ids:
        qid, err = db.enqueue_for_download(wid)
        if qid:
            added += 1
            click.echo(f"  → queued (qid {qid}) from wid {wid}")
        else:
            skipped += 1
            click.echo(f"  ! skipped wid {wid}: {err}")

    click.echo(f"Finished: {added} queued, {skipped} skipped.")
    db.close_connection()


# ----------------------------------------------------------------------
# Download PDFs Worker
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------

@cli.command("retry-failed-downloads")
@click.option('--db-path', default=str(DEFAULT_DB_PATH), type=click.Path(), help='SQLite database path.')
def retry_failed_downloads_command(db_path):
    """Move all entries from failed_downloads back to no_metadata for reprocessing."""
    click.echo("Retrying failed downloads...")
    db_manager = DatabaseManager(db_path=db_path)
    try:
        moved_count = db_manager.retry_failed_downloads()
        click.echo(f"Successfully moved {moved_count} entries from 'failed_downloads' to 'no_metadata'.")
    except Exception as e:
        click.echo(f"An error occurred: {e}", err=True)
    finally:
        db_manager.close_connection()

@cli.command("download-pdfs")
@click.option('--db-path', default=str(DEFAULT_DB_PATH), type=click.Path(dir_okay=False, resolve_path=True), help='SQLite database path.')
@click.option('--limit', default=10, show_default=True, help='Number of queue entries to process per run.')
@click.option('--download-dir', default=str(Path.cwd() / 'pdf_library'), type=click.Path(file_okay=False, resolve_path=True), help='Directory to store downloaded PDFs.')
def download_pdfs_command(db_path, limit, download_dir):
    """Process download queue and retrieve PDFs."""
    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    click.echo(f"Processing up to {limit} queued items…")

    rl = get_global_rate_limiter()

    db = DatabaseManager(db_path)
    enhancer = BibliographyEnhancer(db_manager=db, rate_limiter=rl, email='spott@wzb.eu', output_folder=download_dir)

    queue = db.get_entries_to_download(limit=limit)
    if not queue:
        click.echo("Queue is empty.")
        db.close_connection()
        return

    ok = 0
    failed = 0
    for row in queue:
        # --- DEBUG: Print the raw row from the database ---
        click.echo("\n--- DEBUG: Raw row from DB ---")
        click.echo(dict(row))
        click.echo("--- END DEBUG ---\n")

        # Correctly parse author data from the database row, prioritizing rich JSON
        author_structs = []
        author_names_for_display = []

        # 1. Try to get authors from the full OpenAlex JSON first
        openalex_json_str = row.get('openalex_json')
        if openalex_json_str:
            try:
                openalex_data = json.loads(openalex_json_str)
                if openalex_data and 'authorships' in openalex_data:
                    for authorship in openalex_data['authorships']:
                        author_name = authorship.get('author', {}).get('display_name')
                        if author_name:
                            author_names_for_display.append(author_name)
                            # Also populate author_structs for the downloader, which needs 'family' for LibGen
                            name_parts = author_name.split()
                            author_structs.append({
                                'raw': author_name,
                                'family': name_parts[-1] if name_parts else ''
                            })
            except (json.JSONDecodeError, TypeError):
                pass # Fallback to simple authors field if JSON is malformed

        # 2. Fallback to the basic 'authors' column if OpenAlex data was not available or failed
        if not author_structs:
            try:
                authors_json = row.get('authors')
                if authors_json:
                    loaded_json = json.loads(authors_json)
                    if isinstance(loaded_json, list):
                        author_structs = loaded_json  # Assume it's a list of dicts with 'family', 'given'
                        for author in author_structs:
                            if isinstance(author, dict):
                                given = author.get('given', '')
                                family = author.get('family', '')
                                if family:
                                    author_names_for_display.append(f"{given} {family}".strip())
            except (json.JSONDecodeError, TypeError):
                pass # Keep lists empty if data is malformed

        # Drop queue entry if it is already in downloaded_references
        dup_table, dup_id, dup_field = db.check_if_exists(
            doi=row.get('doi'),
            openalex_id=row.get('openalex_id'),
            title=row.get('title'),
            authors=author_structs or row.get('authors'),
            year=row.get('year'),
            exclude_id=row['id'],
            exclude_table='to_download_references'
        )
        if dup_table == 'downloaded_references' and dup_id is not None:
            ok, msg = db.drop_queue_entry_as_duplicate(row['id'], dup_table, dup_id, dup_field)
            click.echo(f"  ↺ duplicate detected: {msg}")
            continue

        # Update the user on progress
        click.echo(f"\n[QID {row['id']}] {row['title'][:80]}")
        click.echo(f"Authors: {', '.join(author_names_for_display) if author_names_for_display else 'N/A'}")
        click.echo(f"Year: {row.get('year', 'N/A')}")

        ref = {
            'title': row.get('title'),
            'authors': author_structs,  # Pass the structured data for searching
            'doi': row.get('doi'),
            'type': row.get('entry_type') or '',
            'year': row.get('year')
        }

        result = enhancer.enhance_bibliography(copy.deepcopy(ref), download_dir)
        if result and result.get('downloaded_file'):
            fp = result['downloaded_file']
            checksum = ''
            try:
                with open(fp, 'rb') as fh:
                    checksum = hashlib.sha256(fh.read()).hexdigest()
            except Exception:
                pass
            db.move_entry_to_downloaded(row['id'], result, fp, checksum, result.get('download_source', 'unknown'))
            click.echo(f"  ✓ downloaded via {result.get('download_source', '?')}")
            ok += 1
        else:
            db.move_queue_entry_to_failed(row['id'], 'download_failed')
            click.echo("  ✗ download failed")
            failed += 1

    click.echo(f"\nWorker finished: {ok} downloaded, {failed} failed.")
    db.close_connection()



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
    """Enriches bibliography JSON files with metadata from OpenAlex and Crossref."""
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

@cli.command("inspect-tables")
@click.option('--db-path', default=str(DEFAULT_DB_PATH), help='Path to the SQLite database file.')
@click.option('--table', type=click.Choice(['all', 'no_metadata', 'with_metadata', 'to_download_references', 'downloaded_references', 'failed_enrichments', 'failed_downloads', 'duplicate_references', 'merge_log']), default='all', help='Specific table to inspect.')
@click.option('--limit', default=5, help='Number of entries to show per table.')
def inspect_tables_command(db_path, table, limit):
    """Inspects database tables to debug data flow issues."""
    try:
        db_manager = DatabaseManager(db_path=db_path)
        cursor = db_manager.conn.cursor()
        cursor.row_factory = sqlite3.Row
        
        # Define workflow tables
        workflow_tables = [
            ("no_metadata", "Raw extracted references (before enrichment)"),
            ("with_metadata", "Enriched references (after OpenAlex/Crossref lookup)"),
            ("to_download_references", "Queue for PDF downloads"),
            ("downloaded_references", "Successfully downloaded papers"),
            ("failed_enrichments", "References that failed metadata enrichment"),
            ("failed_downloads", "References that failed PDF download"),
            ("duplicate_references", "Detected duplicates"),
            ("merge_log", "Deduplication merge decisions")
        ]
        
        tables_to_check = workflow_tables if table == 'all' else [(table, "Selected table")]
        
        click.echo(f"{CYAN}Database Inspection: {Path(db_path).resolve()}{RESET}")
        click.echo("=" * 80)
        
        total_items = 0
        
        for table_name, description in tables_to_check:
            # Skip if specific table requested but doesn't match
            if table != 'all' and table_name != table:
                continue
                
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            total_items += count
            
            status_icon = "✅" if count > 0 else "📭"
            click.echo(f"\n{status_icon} {BLUE}{table_name.upper()}{RESET} ({count} rows)")
            click.echo(f"    {description}")
            
            if count > 0:
                # Get column names
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                col_names = [col[1] for col in columns]
                
                # Show sample rows
                display_limit = min(limit, count)
                cursor.execute(f"SELECT * FROM {table_name} LIMIT {display_limit}")
                rows = cursor.fetchall()
                
                click.echo(f"    {WHITE}Columns:{RESET} {', '.join(col_names)}")
                click.echo(f"    {WHITE}Sample entries (showing {display_limit} of {count}):{RESET}")
                
                for i, row in enumerate(rows, 1):
                    click.echo(f"\n    {GREEN}Entry {i}:{RESET}")
                    for col_name in col_names:
                        value = row[col_name]
                        # Handle special formatting for different column types
                        if value is None:
                            display_value = f"{YELLOW}None{RESET}"
                        elif isinstance(value, str):
                            if col_name in ['authors', 'keywords', 'crossref_json', 'openalex_json', 'bibtex_entry_json']:
                                # JSON fields - show truncated
                                if len(value) > 100:
                                    display_value = f"{value[:97]}..."
                                else:
                                    display_value = value
                            elif col_name in ['title', 'abstract']:
                                # Text fields - show truncated
                                if len(value) > 80:
                                    display_value = f"{value[:77]}..."
                                else:
                                    display_value = value
                            else:
                                display_value = value
                        else:
                            display_value = str(value)
                        
                        click.echo(f"      {CYAN}{col_name}:{RESET} {display_value}")
            else:
                click.echo(f"    {YELLOW}Empty table{RESET}")
        
        if table == 'all':
            click.echo(f"\n{CYAN}Total items across workflow: {total_items}{RESET}")
            
            # Check for potential workflow issues
            cursor.execute("SELECT COUNT(*) FROM no_metadata")
            no_meta_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM with_metadata")
            with_meta_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM to_download_references")
            queue_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM downloaded_references")
            downloaded_count = cursor.fetchone()[0]
            
            click.echo(f"\n{YELLOW}Workflow Analysis:{RESET}")
            if no_meta_count > 0 and with_meta_count == 0:
                click.echo(f"  ⚠️  {no_meta_count} items stuck in no_metadata (enrichment may have failed)")
            elif with_meta_count > 0 and queue_count == 0:
                click.echo(f"  ⚠️  {with_meta_count} items stuck in with_metadata (run process-downloads)")
            elif queue_count > 0:
                click.echo(f"  📥 {queue_count} items ready for download (run download-pdfs)")
            elif downloaded_count > 0:
                click.echo(f"  ✅ {downloaded_count} items successfully processed")
            else:
                click.echo(f"  📭 No items in the workflow pipeline")
        
    except Exception as e:
        click.echo(f"{RED}Error inspecting tables: {e}{RESET}")


@cli.command("merge-log")
@click.option('--db-path', default=str(DEFAULT_DB_PATH), help='Path to the SQLite database file.')
@click.option('--limit', default=20, show_default=True, help='Number of merge_log rows to show.')
@click.option('--action', default=None, help='Filter by action (e.g., merged, conflict, possible_duplicate).')
@click.option('--table', 'table_filter', default=None, help='Filter by canonical_table or duplicate_table.')
def merge_log_command(db_path, limit, action, table_filter):
    """Show recent merge_log entries for dedupe auditing."""
    try:
        db_manager = DatabaseManager(db_path=db_path)
        cursor = db_manager.conn.cursor()
        cursor.row_factory = sqlite3.Row

        query = "SELECT * FROM merge_log"
        params = []
        clauses = []
        if action:
            clauses.append("action = ?")
            params.append(action)
        if table_filter:
            clauses.append("(canonical_table = ? OR duplicate_table = ?)")
            params.extend([table_filter, table_filter])
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            click.echo(f"{YELLOW}No merge_log entries found.{RESET}")
            return

        click.echo(f"{CYAN}merge_log (showing {len(rows)} rows){RESET}")
        for row in rows:
            click.echo(
                f"[{row['id']}] {row['action']} | "
                f"{row['canonical_table']}:{row['canonical_id']} "
                f"<- {row['duplicate_table']}:{row['duplicate_id']} "
                f"| match={row['match_field']} | {row['created_at']}"
            )
            if row["notes"]:
                click.echo(f"  notes: {row['notes']}")
    except Exception as e:
        click.echo(f"{RED}Error reading merge_log: {e}{RESET}")
        import traceback
        traceback.print_exc()
    finally:
        if 'db_manager' in locals():
            db_manager.close_connection()

@cli.command("process-pdf")
@click.argument('pdf_path', type=click.Path(exists=True))
@click.option('--fetch-references/--no-fetch-references', default=True, 
              help='Fetch referenced works from enriched papers.')
@click.option('--fetch-citations/--no-fetch-citations', default=False,
              help='Fetch citing works for enriched papers.')
@click.option('--max-citations', default=100, type=int,
              help='Maximum number of citing works to fetch per paper.')
@click.option('--move-completed/--no-move-completed', default=False,
              help='Move PDF to completed/failed folder after processing.')
@click.option('--db-path', default=str(DEFAULT_DB_PATH), 
              help='Path to the SQLite database file.')
def process_pdf_command(pdf_path, fetch_references, fetch_citations, max_citations, 
                       move_completed, db_path):
    """Process a single PDF through the entire pipeline."""
    from .pipeline import PipelineOrchestrator
    
    click.echo(f"\n{CYAN}Processing PDF: {pdf_path}{RESET}")
    
    # Set up options
    options = {
        'fetch_references': fetch_references,
        'fetch_citations': fetch_citations,
        'max_citations': max_citations,
        'move_on_complete': move_completed,
        'completed_folder': 'completed',
        'failed_folder': 'failed'
    }
    
    try:
        # Create orchestrator and process PDF
        orchestrator = PipelineOrchestrator(db_path=db_path)
        result = orchestrator.process_pdf_complete(pdf_path, options)
        
        # Display summary
        click.echo(f"\n{'='*60}")
        click.echo(f"{CYAN}Processing Complete{RESET}")
        click.echo(f"{'='*60}")
        
        if result['success']:
            click.echo(f"{GREEN}✓ Success{RESET}")
        else:
            click.echo(f"{RED}✗ Failed{RESET}")
            
        click.echo(f"\nStatistics:")
        click.echo(f"  Extracted references: {result['extracted_refs']}")
        click.echo(f"  Parsed references: {result['parsed_refs']}")
        click.echo(f"  Enriched references: {result['enriched_refs']}")
        click.echo(f"  Queued for download: {result['queued_downloads']}")
        click.echo(f"  Successfully downloaded: {result['successful_downloads']}")
        click.echo(f"  Failed downloads: {result['failed_downloads']}")
        
        if result['errors']:
            click.echo(f"\n{RED}Errors:{RESET}")
            for error in result['errors']:
                click.echo(f"  • {error}")
                
        # Processing time
        summary = result.get('processing_summary', {})
        if summary:
            click.echo(f"\nTotal processing time: {summary['total_time']:.1f} seconds")
            
    except Exception as e:
        click.echo(f"{RED}Error: {e}{RESET}")
        import traceback
        traceback.print_exc()

@cli.command("process-folder")
@click.argument('folder_path', type=click.Path(exists=True))
@click.option('--watch/--no-watch', default=False,
              help='Continuously watch folder for new PDFs.')
@click.option('--interval', default=5, type=int,
              help='Watch interval in seconds (only with --watch).')
@click.option('--fetch-references/--no-fetch-references', default=True,
              help='Fetch referenced works from enriched papers.')
@click.option('--fetch-citations/--no-fetch-citations', default=False,
              help='Fetch citing works for enriched papers.')
@click.option('--max-citations', default=100, type=int,
              help='Maximum number of citing works to fetch per paper.')
@click.option('--db-path', default=str(DEFAULT_DB_PATH),
              help='Path to the SQLite database file.')
def process_folder_command(folder_path, watch, interval, fetch_references, 
                          fetch_citations, max_citations, db_path):
    """Process all PDFs in a folder (optionally with continuous watching)."""
    from .pipeline import PipelineOrchestrator
    
    # Set up options
    options = {
        'fetch_references': fetch_references,
        'fetch_citations': fetch_citations,
        'max_citations': max_citations,
        'move_on_complete': watch,  # Only move files when watching
        'completed_folder': 'completed',
        'failed_folder': 'failed'
    }
    
    try:
        orchestrator = PipelineOrchestrator(db_path=db_path)
        
        if watch:
            # Continuous watching mode
            click.echo(f"\n{CYAN}Starting folder watch: {folder_path}{RESET}")
            click.echo(f"Check interval: {interval} seconds")
            click.echo(f"Press Ctrl+C to stop...\n")
            
            orchestrator.watch_folder(folder_path, options, interval)
        else:
            # One-time processing
            click.echo(f"\n{CYAN}Processing all PDFs in: {folder_path}{RESET}")
            
            results = orchestrator.process_folder(folder_path, options)
            
            # Display summary
            if results:
                successful = sum(1 for r in results if r['success'])
                failed = len(results) - successful
                
                click.echo(f"\n{'='*60}")
                click.echo(f"{CYAN}Batch Processing Complete{RESET}")
                click.echo(f"{'='*60}")
                click.echo(f"Total PDFs processed: {len(results)}")
                click.echo(f"  {GREEN}✓ Successful: {successful}{RESET}")
                click.echo(f"  {RED}✗ Failed: {failed}{RESET}")
                
                # Aggregate statistics
                total_refs = sum(r['parsed_refs'] for r in results)
                total_enriched = sum(r['enriched_refs'] for r in results)
                total_downloaded = sum(r['successful_downloads'] for r in results)
                
                click.echo(f"\nAggregate Statistics:")
                click.echo(f"  Total references parsed: {total_refs}")
                click.echo(f"  Total references enriched: {total_enriched}")
                click.echo(f"  Total papers downloaded: {total_downloaded}")
                
    except KeyboardInterrupt:
        click.echo(f"\n{YELLOW}Processing interrupted by user.{RESET}")
    except Exception as e:
        click.echo(f"{RED}Error: {e}{RESET}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    cli()
