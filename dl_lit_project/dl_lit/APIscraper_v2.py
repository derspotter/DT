"""Gemini-based bibliography extraction from page-level PDFs into database staging.

Accepted inputs:
- a directory of PDFs via `--input-dir`
- a single PDF via `--input-pdf`
- a positional path (file or directory) for backward compatibility.
"""

import argparse
import concurrent.futures
import glob
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from threading import Lock
from typing import List, Optional

from dotenv import load_dotenv
from google import genai
from google.genai import types

try:
    import pikepdf
except Exception:
    pikepdf = None

# Make local package imports work when this module is executed directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dl_lit.db_manager import DatabaseManager
from dl_lit.utils import get_global_rate_limiter
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
# Keep default model stable, but allow override in runtime environments.
DEFAULT_MODEL_NAME = os.getenv("RAG_FEEDER_GEMINI_MODEL", "gemini-2.5-flash")

# Global client
api_client = None

# --- Rate Limiter --- (Shared global instance)
rate_limiter = get_global_rate_limiter()

# --- Bibliography Prompt ---
BIBLIOGRAPHY_PROMPT = (
    "Extract bibliography entries from the attached PDF page. "
    "Return JSON that matches the provided schema. "
    "If no entries are found, return an empty list."
)

INLINE_CITATION_PROMPT = (
    "Extract all cited works from this PDF chunk, including inline citations, footnotes, endnotes, "
    "and per-page references. Return JSON that matches the provided schema. "
    "Include each cited work once if identifiable. If no citations are found, return an empty list."
)

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

        class BibliographyEntryList(BaseModel):
            model_config = ConfigDict(extra="ignore")
            items: List[BibliographyEntry] = []

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

        class BibliographyEntryList(BaseModel):
            class Config:
                extra = "ignore"
            items: List[BibliographyEntry] = []


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

def _wait_rate_limit(service_name: str) -> None:
    """Best-effort rate limiting without turning throttling issues into hard failures."""
    try:
        if rate_limiter:
            rate_limiter.wait_if_needed(service_name)
    except Exception:
        return

def _resolve_prompt(extract_mode: str) -> str:
    return INLINE_CITATION_PROMPT if extract_mode == "inline" else BIBLIOGRAPHY_PROMPT


def _split_pdf_into_chunks(pdf_path: str, chunk_pages: int) -> tuple[str | None, list[str]]:
    """Create temporary chunk PDFs and return (temp_dir, chunk_paths)."""
    if chunk_pages <= 0:
        return None, [pdf_path]
    if pikepdf is None:
        raise RuntimeError("pikepdf is required for chunked extraction fallback")

    with pikepdf.Pdf.open(pdf_path) as src_pdf:
        total_pages = len(src_pdf.pages)
        if total_pages <= chunk_pages:
            return None, [pdf_path]

    temp_dir = tempfile.mkdtemp(prefix="apiscraper_chunks_")
    chunk_paths: list[str] = []

    with pikepdf.Pdf.open(pdf_path) as src_pdf:
        for start in range(0, len(src_pdf.pages), chunk_pages):
            end = min(start + chunk_pages, len(src_pdf.pages))
            chunk_pdf = pikepdf.Pdf.new()
            for idx in range(start, end):
                chunk_pdf.pages.append(src_pdf.pages[idx])
            chunk_path = os.path.join(temp_dir, f"{Path(pdf_path).stem}_chunk_{start+1:05d}_{end:05d}.pdf")
            chunk_pdf.save(chunk_path)
            chunk_paths.append(chunk_path)

    return temp_dir, chunk_paths


def upload_pdf_and_extract_bibliography(
    pdf_path,
    timeout_seconds: int = 120,
    extract_mode: str = "page",
    uploaded_file_name: str | None = None,
):
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
        if uploaded_file_name:
            uploaded_file = api_client.files.get(name=uploaded_file_name)
            print(f"[DEBUG] Reusing uploaded PDF as name: {uploaded_file.name}", flush=True)
        else:
            # Upload the PDF file via Files API.
            uploaded_file = api_client.files.upload(
                file=str(pdf_path),
                config={
                    "mime_type": "application/pdf",
                    "display_name": Path(pdf_path).name,
                },
            )
            print(f"[DEBUG] Uploaded PDF as name: {uploaded_file.name}", flush=True)
        
        # Prepare the prompt for bibliography extraction
        prompt = _resolve_prompt(extract_mode)
        _wait_rate_limit("gemini")
        _wait_rate_limit("gemini_daily")
        
        def _call_model():
            config = types.GenerateContentConfig(
                temperature=1.0,
                response_mime_type="application/json",
            )
            if BaseModel:
                config.response_schema = BibliographyEntryList
            return api_client.models.generate_content(
                model=DEFAULT_MODEL_NAME,
                contents=[prompt, uploaded_file],
                config=config,
            )

        # Send request to API with uploaded file (timeout guard)
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(_call_model)
        try:
            response = future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError as e:
            future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            raise TimeoutError(f"Gemini request timed out after {timeout_seconds}s") from e
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        response_text = response.text
        print(f"[DEBUG] Raw API response content: {response_text[:200]}...", flush=True)
        
        try:
            bibliography_data = json.loads(response_text)
            if isinstance(bibliography_data, dict) and "items" in bibliography_data:
                bibliography_data = bibliography_data.get("items") or []
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
        if uploaded_file and not uploaded_file_name:
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

def process_single_pdf(
    pdf_path,
    db_manager: DatabaseManager,
    summary_dict,
    summary_lock,
    ingest_source: str | None = None,
    corpus_id: int | None = None,
    extract_mode: str = "page",
    source_pdf_override: str | None = None,
    uploaded_file_name: str | None = None,
):
    """Processes a single PDF file: upload directly, call API, save result."""
    filename = os.path.basename(pdf_path)
    print(f"\n[INFO] Processing {filename}...", flush=True)
    start_process_time = time.time()
    file_success = False
    failure_reason = "Unknown"

    try:
        # Get Bibliography via API by uploading PDF directly
        stage_start_time = time.time()
        bibliography_data = upload_pdf_and_extract_bibliography(
            pdf_path,
            extract_mode=extract_mode,
            uploaded_file_name=uploaded_file_name,
        )
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

            # Record entries for UI display and insert into no_metadata stage
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
                    "source_pdf": str(source_pdf_override or pdf_path),
                    "translated_title": entry.get("translated_title"),
                    "translated_year": entry.get("translated_year"),
                    "translated_language": entry.get("translated_language"),
                    "translation_relationship": entry.get("translation_relationship"),
                    "ingest_source": ingest_source,
                    "corpus_id": corpus_id,
                }
                db_manager.insert_ingest_entry(minimal_ref, ingest_source=ingest_source)
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


def process_single_pdf_chunked(
    pdf_path,
    db_manager: DatabaseManager,
    summary_dict,
    summary_lock,
    ingest_source: str | None = None,
    corpus_id: int | None = None,
    extract_mode: str = "inline",
    chunk_pages: int = 50,
    uploaded_file_name: str | None = None,
):
    """Fallback extraction for inline/per-page citations by chunking larger PDFs."""
    temp_chunk_dir = None
    try:
        temp_chunk_dir, chunk_paths = _split_pdf_into_chunks(str(pdf_path), int(chunk_pages))
    except Exception as exc:
        print(f"[ERROR] Failed to create PDF chunks for {pdf_path}: {exc}", flush=True)
        with summary_lock:
            summary_dict["processed_files"] += 1
            summary_dict["failed_files"] += 1
            summary_dict["failures"].append({"file": os.path.basename(str(pdf_path)), "reason": str(exc)})
        return

    try:
        print(
            f"[INFO] Chunked fallback extraction for {pdf_path}: {len(chunk_paths)} chunk(s), mode={extract_mode}, chunk_pages={chunk_pages}",
            flush=True,
        )
        for chunk_path in chunk_paths:
            process_single_pdf(
                chunk_path,
                db_manager,
                summary_dict,
                summary_lock,
                ingest_source=ingest_source,
                corpus_id=corpus_id,
                extract_mode=extract_mode,
                source_pdf_override=str(pdf_path),
                uploaded_file_name=None if chunk_path != str(pdf_path) else uploaded_file_name,
            )
    finally:
        if temp_chunk_dir and os.path.exists(temp_chunk_dir):
            shutil.rmtree(temp_chunk_dir, ignore_errors=True)

def process_directory_v2(
    input_dir,
    db_manager: DatabaseManager,
    max_workers=5,
    ingest_source: str | None = None,
    corpus_id: int | None = None,
    extract_mode: str = "page",
    chunk_pages: int = 0,
):
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

    if chunk_pages and chunk_pages > 0:
        print(f"[INFO] Chunked mode enabled for directory processing (chunk_pages={chunk_pages}).", flush=True)
        for pdf_path in pdf_files:
            process_single_pdf_chunked(
                pdf_path,
                db_manager,
                results_summary,
                summary_lock,
                ingest_source=ingest_source,
                corpus_id=corpus_id,
                extract_mode=extract_mode,
                chunk_pages=chunk_pages,
            )
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
        return

    try:
        # Use ThreadPoolExecutor for I/O-bound tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks for each PDF
            futures = [
                executor.submit(
                    process_single_pdf,
                    pdf_path,
                    db_manager,
                    results_summary,
                    summary_lock,
                    ingest_source,
                    corpus_id,
                    extract_mode,
                )
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

def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="V2: Extract bibliographies from PDFs using Gemini.")
    parser.add_argument("input", nargs="?", help="Path to a PDF file or a directory containing PDFs.")
    parser.add_argument("--input-pdf", dest="input_pdf", help="Path to a single input PDF file.")
    parser.add_argument("--input-dir", dest="input_dir", help="Path to a directory containing input PDF files.")
    parser.add_argument("--workers", type=int, default=5, help="Maximum number of concurrent worker threads.")
    parser.add_argument("--db-path", required=True, help="Path to the SQLite database file.")
    parser.add_argument("--ingest-source", default=None, help="Identifier for this ingest run (e.g., base filename).")
    parser.add_argument("--corpus-id", type=int, default=None, help="Corpus ID to associate with inserted entries.")
    parser.add_argument(
        "--extract-mode",
        choices=("page", "inline"),
        default="page",
        help="Extraction strategy: page=reference page PDFs, inline=all citations in full/chunked PDF.",
    )
    parser.add_argument(
        "--chunk-pages",
        type=int,
        default=0,
        help="If > 0, split each PDF into chunks with this many pages before extraction.",
    )
    parser.add_argument(
        "--uploaded-file-name",
        default=None,
        help="Reuse an already uploaded Gemini file (files/<id>) for single-PDF extraction.",
    )
    return parser


def _resolve_input_path(args: argparse.Namespace) -> str | None:
    return args.input_pdf or args.input_dir or args.input


def main(argv: list[str] | None = None) -> int:
    start_total_time = time.time()

    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if not configure_api_client():
        print("[FATAL] Exiting due to API configuration failure.", flush=True)
        return 1

    input_path = _resolve_input_path(args)
    if not input_path:
        print("[FATAL] No input provided. Use --input-dir, --input-pdf, or positional input.", flush=True)
        return 2

    db_path_resolved = Path(args.db_path).expanduser().resolve()
    print(f"[INFO] Using database at: {db_path_resolved}", flush=True)

    db_manager = None
    try:
        db_manager = DatabaseManager(db_path=db_path_resolved)
        if os.path.isdir(input_path):
            print(f"[INFO] Processing directory: {input_path}", flush=True)
            process_directory_v2(
                input_path,
                db_manager,
                args.workers,
                args.ingest_source,
                args.corpus_id,
                args.extract_mode,
                args.chunk_pages,
            )
        elif os.path.isfile(input_path) and str(input_path).lower().endswith(".pdf"):
            print(f"[INFO] Processing single PDF file: {input_path}", flush=True)
            summary = {"processed_files": 0, "successful_files": 0, "failed_files": 0, "failures": []}
            if args.chunk_pages and args.chunk_pages > 0:
                process_single_pdf_chunked(
                    input_path,
                    db_manager,
                    summary,
                    Lock(),
                    args.ingest_source,
                    args.corpus_id,
                    args.extract_mode,
                    args.chunk_pages,
                    args.uploaded_file_name,
                )
            else:
                process_single_pdf(
                    input_path,
                    db_manager,
                    summary,
                    Lock(),
                    args.ingest_source,
                    args.corpus_id,
                    args.extract_mode,
                    None,
                    args.uploaded_file_name,
                )
            print(f"[INFO] Single file processing complete. Summary: {summary}", flush=True)
        else:
            print(f"[ERROR] Input {input_path} is neither a directory nor a PDF file.", flush=True)
            return 2
    finally:
        if db_manager:
            db_manager.close_connection()

    print(f"\n[INFO] Total processing time: {time.time() - start_total_time:.2f} seconds.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
