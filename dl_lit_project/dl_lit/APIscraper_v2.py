import os
import json
import concurrent.futures
import time
import glob
import argparse
from typing import Optional, List
from pathlib import Path

import sys
# Add project root to path to allow absolute imports from `dl_lit`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import DatabaseManager for staged DB insertion
from dl_lit.db_manager import DatabaseManager
from dl_lit.utils import get_global_rate_limiter
from pdfminer.high_level import extract_text
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False
try:
    from PyPDF2 import PdfReader
    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False
from dotenv import load_dotenv
from google import genai
from google.genai import types
import concurrent.futures
from threading import Lock
try:
    from pydantic import BaseModel, ValidationError
    try:
        from pydantic import ConfigDict
    except Exception:
        ConfigDict = None
except ImportError:
    BaseModel = None
    ValidationError = Exception
    ConfigDict = None

# --- Configuration ---
# Use Gemini 3 Flash Preview model
DEFAULT_MODEL_NAME = 'gemini-3-flash-preview'

# Global client
api_client = None

# --- Rate Limiter --- (Shared global instance)
rate_limiter = get_global_rate_limiter()

# --- Bibliography Prompt ---
BIBLIOGRAPHY_PROMPT = """You are an expert research assistant specializing in academic literature. Your task is to extract bibliography or reference list entries from the provided text, which is a page from a PDF document containing academic references. Return the bibliography entries as a JSON array of objects. Each object should represent a single bibliography entry and include the following fields where available, regardless of the citation style (APA, MLA, Chicago, etc.) or language:

- authors: array of author names (strings, split multiple authors into separate entries in the array)
- editors: array of editor names if present (strings, for books/edited volumes/conference proceedings)
- year: publication year (integer or string)
- title: title of the work (string)
- source: journal name, book title, conference name, etc. (string)
- volume: journal volume (integer or string)
- issue: journal issue (integer or string)
- pages: page range (string, e.g., "123-145")
- publisher: publisher name if present (string)
- location: place of publication if present (string)
- type: type of document (e.g., 'journal article', 'book', 'book chapter', 'conference paper', etc.) (string)
- doi: DOI if present (string)
- url: URL if present (string)
- isbn: ISBN if present for books (string)
- issn: ISSN if present for journals (string)
- abstract: abstract if present (string)
- keywords: array of keywords if present (strings)
- language: language of publication if not English (string)
- translated_title: translated or alternate-language title if the citation explicitly mentions a translation (string)
- translated_year: year associated with the translated title if mentioned (integer or string)
- translated_language: language code for the translated title if known (string, e.g., "de" for German)
- translation_relationship: relationship label for the translated title (string, e.g., "translation")

INSTRUCTIONS:
1. Recognize bibliography entries even if they are in different formats or languages. Look for patterns like author names followed by years, titles in quotes or italics, and publication details.
2. Extract as many fields as possible, even partial entries. Make educated guesses for the 'type' field based on context.
3. If a citation explicitly mentions a translated or alternate-language edition, keep the primary (original) work as the main entry and populate the translated_* fields rather than replacing the main title. Use translation_relationship="translation".
4. For the 'source' field, extract ONLY the actual title of the journal, book, or conference. DO NOT include:
   - Series editors' names (e.g., "Smith/Jones" in "Smith/Jones Economics Handbook")
   - Original editors' names that precede the title (e.g., "Obst/Hintner" in "Obst/Hintner Geld-, Bank- und Börsenwesen")
   - Edition information (e.g., "40th edn.")
   - Publisher information
   Examples: 
   - From "in J. Doe (ed.), Smith/Jones Financial Systems, 3rd edn." → source: "Financial Systems"
   - From "Obst/Hintner Geld-, Bank- und Börsenwesen" → source: "Geld-, Bank- und Börsenwesen"
5. If no clear bibliography entries are found, return an empty JSON array [].
6. Return ONLY the JSON array. Do not include introductory text, explanations, apologies, or markdown formatting like ```json.
"""

if BaseModel:
    if ConfigDict:
        class BibliographyEntry(BaseModel):
            model_config = ConfigDict(extra="ignore")
            authors: Optional[List[str]] = None
            editors: Optional[List[str]] = None
            year: Optional[int | str] = None
            title: Optional[str] = None
            source: Optional[str] = None
            volume: Optional[int | str] = None
            issue: Optional[int | str] = None
            pages: Optional[str] = None
            publisher: Optional[str] = None
            location: Optional[str] = None
            type: Optional[str] = None
            doi: Optional[str] = None
            url: Optional[str] = None
            isbn: Optional[str] = None
            issn: Optional[str] = None
            abstract: Optional[str] = None
            keywords: Optional[List[str]] = None
            language: Optional[str] = None
            translated_title: Optional[str] = None
            translated_year: Optional[int | str] = None
            translated_language: Optional[str] = None
            translation_relationship: Optional[str] = None
    else:
        class BibliographyEntry(BaseModel):
            class Config:
                extra = "ignore"
            authors: Optional[List[str]] = None
            editors: Optional[List[str]] = None
            year: Optional[int | str] = None
            title: Optional[str] = None
            source: Optional[str] = None
            volume: Optional[int | str] = None
            issue: Optional[int | str] = None
            pages: Optional[str] = None
            publisher: Optional[str] = None
            location: Optional[str] = None
            type: Optional[str] = None
            doi: Optional[str] = None
            url: Optional[str] = None
            isbn: Optional[str] = None
            issn: Optional[str] = None
            abstract: Optional[str] = None
            keywords: Optional[List[str]] = None
            language: Optional[str] = None
            translated_title: Optional[str] = None
            translated_year: Optional[int | str] = None
            translated_language: Optional[str] = None
            translation_relationship: Optional[str] = None

# --- Helper Functions ---

def load_api_key():
    """Loads the appropriate API key based on the chosen model."""
    load_dotenv() # Load .env for local execution
    key = os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_API_KEY')
    if not key:
        print("[ERROR] GEMINI_API_KEY (or GOOGLE_API_KEY) not found.", flush=True)
    return key

def configure_api_client():
    """Configures the API client/model."""
    global api_client
    api_key = load_api_key()
    if not api_key:
        return False

    try:
        api_client = genai.Client(api_key=api_key)
        print(f"[INFO] Successfully configured Google client ({DEFAULT_MODEL_NAME}).", flush=True)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to configure API client: {e}", flush=True)
        api_client = None
        return False

def upload_pdf_and_extract_bibliography(pdf_path, timeout_seconds: int = 120):
    """Uploads a PDF file directly to the API and attempts to extract bibliography entries."""
    print(f"[INFO] Uploading PDF directly for bibliography extraction: {pdf_path}", flush=True)
    start_time = time.time()
    bibliography_data = []
    uploaded_file = None

    if api_client is None:
        if not configure_api_client():
            print("[ERROR] API client not configured; aborting extraction.", flush=True)
            return []
    
    try:
        # Upload the PDF file
        with open(pdf_path, "rb") as file_handle:
            uploaded_file = api_client.files.upload(
                file=file_handle,
                config={
                    "mime_type": "application/pdf",
                    "display_name": Path(pdf_path).name,
                },
            )
        print(f"[DEBUG] Uploaded PDF as name: {uploaded_file.name}", flush=True)
        
        # Prepare the prompt for bibliography extraction
        prompt = BIBLIOGRAPHY_PROMPT
        rate_limiter.wait_if_needed('gemini')
        rate_limiter.wait_if_needed('gemini_daily')
        
        def _call_model():
            return api_client.models.generate_content(
                model=DEFAULT_MODEL_NAME,
                contents=[prompt, uploaded_file],
                config=types.GenerateContentConfig(
                    temperature=0.0,
                    response_mime_type="application/json"
                )
            )

        # Send request to API with uploaded file (timeout guard)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_call_model)
            try:
                response = future.result(timeout=timeout_seconds)
            except concurrent.futures.TimeoutError as e:
                future.cancel()
                raise TimeoutError(f"Gemini request timed out after {timeout_seconds}s") from e

        response_text = response.text
        print(f"[DEBUG] Raw API response content: {response_text[:200]}...", flush=True)
        
        try:
            bibliography_data = json.loads(response_text)
            if isinstance(bibliography_data, list):
                print(f"[DEBUG] Parsed JSON successfully as list.", flush=True)
            else:
                print(f"[WARNING] API response is not a list, attempting extraction.", flush=True)
                extracted_data = extract_json_from_text(response_text)
                if extracted_data is not None:
                    bibliography_data = extracted_data
                else:
                    print(f"[WARNING] Extraction failed.", flush=True)
                    bibliography_data = []
        except json.JSONDecodeError as jde:
            print(f"[ERROR] JSON decode error: {jde}", flush=True)
            print(f"[ERROR] Response content: {response_text[:500]}...", flush=True)
            extracted_data = extract_json_from_text(response_text)
            if extracted_data is not None:
                bibliography_data = extracted_data
            else:
                bibliography_data = []

        if isinstance(bibliography_data, list):
            bibliography_data = validate_bibliography_entries(bibliography_data)
    except Exception as e:
        print(f"[ERROR] API error while processing {pdf_path}: {e}", flush=True)
        bibliography_data = []
    finally:
        # Attempt to delete the uploaded file
        if uploaded_file:
            try:
                api_client.files.delete(name=uploaded_file.name)
                print(f"[DEBUG] Deleted uploaded file: {uploaded_file.name}", flush=True)
            except Exception as delete_error:
                print(f"[WARNING] Failed to delete uploaded file {uploaded_file.name if uploaded_file else 'unknown'}: {delete_error}", flush=True)
                print(f"[INFO] Continuing with processing despite deletion failure.", flush=True)
        end_time = time.time()
        print(f"[TIMER] Bibliography extraction for {pdf_path} took {end_time - start_time:.2f} seconds.", flush=True)
        return bibliography_data

def extract_json_from_text(text):
    """Attempts to extract valid JSON array from text, even if surrounded by non-JSON content."""
    text = text.strip()
    start_idx = text.find('[')
    end_idx = text.rfind(']')
    if start_idx >= 0 and end_idx > start_idx:
        potential_json = text[start_idx:end_idx+1]
        try:
            data = json.loads(potential_json)
            if isinstance(data, list):
                print(f"[DEBUG] Extracted valid JSON array from response.", flush=True)
                return data
        except json.JSONDecodeError:
            pass
    return None

def validate_bibliography_entries(entries):
    """Validate/normalize model output against the bibliography schema."""
    if os.getenv("DISABLE_BIBLIO_PYDANTIC", "").lower() in {"1", "true", "yes"}:
        print("[INFO] Pydantic validation disabled via DISABLE_BIBLIO_PYDANTIC.", flush=True)
        return entries
    if not BaseModel:
        print("[WARNING] pydantic not available; skipping bibliography validation.", flush=True)
        return entries

    validated = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            if hasattr(BibliographyEntry, "model_validate"):
                model = BibliographyEntry.model_validate(entry)
                data = model.model_dump(exclude_none=True)
            else:
                model = BibliographyEntry.parse_obj(entry)
                data = model.dict(exclude_none=True)

            # Normalize numeric fields to strings for storage consistency
            for key in ("volume", "issue", "pages"):
                if key in data and data[key] is not None and not isinstance(data[key], str):
                    data[key] = str(data[key])
            if "year" in data and isinstance(data["year"], int):
                data["year"] = data["year"]

            validated.append(data)
        except ValidationError as e:
            print(f"[WARNING] Skipping invalid entry: {e}", flush=True)
            validated.append(entry)
    return validated

# --- Main Processing Logic ---

def process_single_pdf(pdf_path, db_manager: DatabaseManager, summary_dict, summary_lock):
    """Processes a single PDF file: upload directly, call API, save result."""
    filename = os.path.basename(pdf_path)
    print(f"\n[INFO] Processing {filename}...", flush=True)
    start_process_time = time.time()
    file_success = False
    failure_reason = "Unknown"

    try:
        # Get Bibliography via API by uploading PDF directly
        stage_start_time = time.time()
        bibliography_data = upload_pdf_and_extract_bibliography(pdf_path)
        api_duration = time.time() - stage_start_time
        print(f"[TIMER] Bibliography extraction for {filename} took {api_duration:.2f} seconds.", flush=True)

        if bibliography_data is None:
            failure_reason = "API call failed or returned None"
            raise ValueError(failure_reason)
        elif not bibliography_data:
            print(f"[WARNING] No bibliography found by API for {filename}", flush=True)
            # Consider this a success in terms of processing, but no data found
            failure_reason = "No bibliography content found by API" # Not strictly a failure, but no output
            file_success = True # Treat as success because process completed
            # Don't save an empty file, just report no data found
        else:

            # Insert each entry into no_metadata stage
            successes = 0
            for entry in bibliography_data:
                minimal_ref = {
                    "title": entry.get("title"),
                    "authors": entry.get("authors"),
                    "editors": entry.get("editors"),
                    "year": entry.get("year"),
                    "doi": entry.get("doi"),
                    "source": entry.get("source"),  # journal/conference/book title
                    "volume": entry.get("volume"),
                    "issue": entry.get("issue"),
                    "pages": entry.get("pages"),
                    "publisher": entry.get("publisher"),
                    "type": entry.get("type"),
                    "url": entry.get("url"),
                    "isbn": entry.get("isbn"),
                    "issn": entry.get("issn"),
                    "abstract": entry.get("abstract"),
                    "keywords": entry.get("keywords"),
                    "source_pdf": str(pdf_path),
                    "translated_title": entry.get("translated_title"),
                    "translated_year": entry.get("translated_year"),
                    "translated_language": entry.get("translated_language"),
                    "translation_relationship": entry.get("translation_relationship"),
                }
                row_id, err = db_manager.insert_no_metadata(minimal_ref)
                if err:
                    print(f"[WARNING] Skipped entry due to: {err}", flush=True)
                else:
                    successes += 1
            print(f"[SUCCESS] Inserted {successes}/{len(bibliography_data)} entries from {filename} into database.", flush=True)
            file_success = True if successes else False
            if not file_success:
                failure_reason = "All inserts skipped (duplicates or errors)"


    except Exception as e:
        # Catch errors during API call steps
        if failure_reason == "Unknown": # If not already set
            failure_reason = f"Processing error: {e}"
        print(f"[ERROR] Failed processing {filename}: {failure_reason}", flush=True)
        file_success = False

    # Update shared summary dictionary safely
    with summary_lock:
        summary_dict["processed_files"] += 1
        if file_success:
            summary_dict["successful_files"] += 1
        else:
            summary_dict["failed_files"] += 1
            summary_dict["failures"].append({"file": filename, "reason": failure_reason})

def process_directory_v2(input_dir, db_manager: DatabaseManager, max_workers=5):
    """Processes all PDFs in the input directory concurrently, saving to the specified output dir."""
    print(f"[INFO] Processing directory: {input_dir}", flush=True)


    # Find all PDFs recursively using glob
    pdf_files = glob.glob(os.path.join(input_dir, "**/*.pdf"), recursive=True)
    print(f"[INFO] Found {len(pdf_files)} PDF files in {input_dir} and its subdirectories.", flush=True)
    if not pdf_files:
        print("[WARNING] No PDF files found to process. Exiting.", flush=True)
        return

    print(f"[INFO] Starting concurrent processing with up to {max_workers} workers and 15 RPM limit.", flush=True)

    # Initialize summary for tracking results
    results_summary = {
        'processed_files': 0,
        'successful_files': 0,
        'failed_files': 0,
        'failures': []
    }
    summary_lock = Lock()

    try:
        # Use ThreadPoolExecutor for I/O-bound tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for each PDF
            futures = [executor.submit(process_single_pdf, pdf_path, db_manager, results_summary, summary_lock)
                       for pdf_path in pdf_files]

            # Wait for all tasks to complete and handle potential exceptions
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # Retrieve result or re-raise exception from the thread
                except Exception as exc:
                    print(f'[ERROR] An error occurred in a worker thread: {exc}', flush=True)
                    # Error is already logged within process_single_pdf and summary updated
    except KeyboardInterrupt:
        print("[INFO] Received interrupt (Ctrl+C). Shutting down gracefully...", flush=True)
        executor.shutdown(wait=False, cancel_futures=True)  # Properly shutdown the executor and cancel pending tasks
        print("[INFO] All tasks stopped. Exiting.", flush=True)
        # Print partial summary before exiting
        print("\n--- Partial Processing Summary ---")
        print(f"Total files found: {len(pdf_files)}")
        print(f"Files processed: {results_summary['processed_files']}")
        print(f"Successful extractions: {results_summary['successful_files']}")
        print(f"Failed extractions: {results_summary['failed_files']}")
        if results_summary['failures']:
            print("Failures:")
            for failure in results_summary['failures']:
                print(f"  - {failure['file']}: {failure['reason']}")
        print("-------------------------")
        return

    # Print summary
    print("\n--- Processing Summary ---")
    print(f"Total files found: {len(pdf_files)}")
    print(f"Files processed: {results_summary['processed_files']}")
    print(f"Successful extractions: {results_summary['successful_files']}")
    print(f"Failed extractions: {results_summary['failed_files']}")
    if results_summary['failures']:
        print("Failures:")
        for failure in results_summary['failures']:
            print(f"  - {failure['file']}: {failure['reason']}")
    print("-------------------------")

# --- Entry Point ---

if __name__ == "__main__":
    print("--- APIscraper_v2.py MAIN START ---", flush=True)

    # Configure API client at the start
    if not configure_api_client():
        print("[FATAL] Exiting due to API configuration failure.", flush=True)
        exit(1) # Or handle more gracefully depending on needs

    parser = argparse.ArgumentParser(description='V2: Extract bibliographies from PDFs using an API.')
    parser.add_argument('--input-dir', required=True, help='Directory containing input PDF files.')
    parser.add_argument('--workers', type=int, default=5, help='Maximum number of concurrent worker threads.')
    parser.add_argument('--db-path', required=True, help='Path to the SQLite database file to insert extracted references into.')
    args = parser.parse_args()

    input_path = args.input_dir
    max_workers = args.workers
    db_path_arg = args.db_path

    db_manager = None
    if not db_path_arg:
        print("[FATAL] --db-path is a required argument. Exiting.", flush=True)
        exit(1)
    else:
        db_path_resolved = Path(db_path_arg).expanduser().resolve()
        print(f"[INFO] Using database at: {db_path_resolved}", flush=True)
        db_manager = DatabaseManager(db_path=db_path_resolved)


    # Check if input is a directory or single file
    if os.path.isdir(input_path):
        print(f"[INFO] Processing directory: {input_path}", flush=True)
        process_directory_v2(input_path, db_manager, max_workers)
    elif os.path.isfile(input_path) and input_path.lower().endswith('.pdf'):
        print(f"[INFO] Processing single PDF file: {input_path}", flush=True)
        summary = {'processed_files': 0, 'successful_files': 0, 'failed_files': 0, 'failures': []}
        process_single_pdf(input_path, db_manager, summary, Lock())
        print(f"[INFO] Single file processing complete. Summary: {summary}", flush=True)
    else:
        print(f"[ERROR] Input {input_path} is neither a directory nor a PDF file.", flush=True)

    start_total_time = time.time()
    print(f"\n[INFO] Total processing time: {time.time() - start_total_time:.2f} seconds.", flush=True)
