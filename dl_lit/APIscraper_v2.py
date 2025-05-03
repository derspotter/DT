import os
import json
import time
import glob
import argparse
from pdfminer.high_level import extract_text
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
    print("[INFO] PyMuPDF (fitz) is available for text extraction.", flush=True)
except ImportError:
    PYMUPDF_AVAILABLE = False
    print("[WARNING] PyMuPDF (fitz) not installed. Falling back to pdfminer for text extraction.", flush=True)
try:
    from PyPDF2 import PdfReader
    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False
    print("[WARNING] PyPDF2 not installed. Secondary fallback text extraction will not be available.", flush=True)
from dotenv import load_dotenv
import google.generativeai as genai
from google.generativeai.types import GenerationConfig
import concurrent.futures
from collections import deque
from threading import Lock
from datetime import datetime, timedelta

# --- Configuration ---
# Use Gemini standard Flash model for speed
DEFAULT_MODEL_NAME = 'gemini-2.0-flash'

# Global client/model variable
api_model = None

# --- Rate Limiter --- (Respecting 15 RPM = 0.25 RPS)
MAX_REQUESTS_PER_MINUTE = 15
MIN_INTERVAL_SECONDS = 60.0 / MAX_REQUESTS_PER_MINUTE
request_timestamps = deque()
rate_limiter_lock = Lock()

def wait_for_rate_limit():
    """Blocks until it's safe to make another API request based on 15 RPM."""
    with rate_limiter_lock:
        now = datetime.now()
        # Remove timestamps older than 1 minute
        while request_timestamps and (now - request_timestamps[0]) > timedelta(minutes=1):
            request_timestamps.popleft()

        # If we've made max requests in the last minute, wait
        if len(request_timestamps) >= MAX_REQUESTS_PER_MINUTE:
            time_since_oldest = (now - request_timestamps[0]).total_seconds()
            wait_time = 60.0 - time_since_oldest
            if wait_time > 0:
                print(f"[RATE LIMIT] Reached {MAX_REQUESTS_PER_MINUTE} RPM. Waiting {wait_time:.2f} seconds...", flush=True)
                time.sleep(wait_time)

        # Check minimum interval between requests
        if request_timestamps:
             time_since_last = (now - request_timestamps[-1]).total_seconds()
             if time_since_last < MIN_INTERVAL_SECONDS:
                 interval_wait = MIN_INTERVAL_SECONDS - time_since_last
                 # print(f"[RATE LIMIT] Minimum interval not met. Waiting {interval_wait:.2f} seconds...", flush=True)
                 time.sleep(interval_wait)

        # Record this request's timestamp
        request_timestamps.append(datetime.now())

# --- Bibliography Prompt ---
BIBLIOGRAPHY_PROMPT = """You are an expert research assistant specializing in academic literature. Your task is to extract bibliography or reference list entries from the provided text, which is a page from a PDF document containing academic references. Return the bibliography entries as a JSON array of objects. Each object should represent a single bibliography entry and include the following fields where available, regardless of the citation style (APA, MLA, Chicago, etc.) or language:

- authors: array of author names (strings, split multiple authors into separate entries in the array)
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

INSTRUCTIONS:
1. Recognize bibliography entries even if they are in different formats or languages. Look for patterns like author names followed by years, titles in quotes or italics, and publication details.
2. Extract as many fields as possible, even partial entries. Make educated guesses for the 'type' field based on context.
3. If no clear bibliography entries are found, return an empty JSON array [].
4. Return ONLY the JSON array. Do not include introductory text, explanations, apologies, or markdown formatting like ```json.
"""

# --- Helper Functions ---

def load_api_key():
    """Loads the appropriate API key based on the chosen model."""
    load_dotenv() # Load .env for local execution
    if 'google' in DEFAULT_MODEL_NAME or 'gemini' in DEFAULT_MODEL_NAME:
        key = os.getenv('GOOGLE_API_KEY')
        if not key:
            print("[ERROR] GOOGLE_API_KEY not found.", flush=True)
        return key
    else:
        print(f"[ERROR] Unknown model type in DEFAULT_MODEL_NAME: {DEFAULT_MODEL_NAME}", flush=True)
        return None

def configure_api_client():
    """Configures the API client/model."""
    global api_model
    api_key = load_api_key()
    if not api_key:
        return False

    try:
        if 'google' in DEFAULT_MODEL_NAME or 'gemini' in DEFAULT_MODEL_NAME:
            genai.configure(api_key=api_key)
            api_model = genai.GenerativeModel(DEFAULT_MODEL_NAME)
            print(f"[INFO] Successfully configured Google client ({DEFAULT_MODEL_NAME}).", flush=True)
            return True
        else:
            return False
    except Exception as e:
        print(f"[ERROR] Failed to configure API client: {e}", flush=True)
        api_model = None
        return False

def upload_pdf_and_extract_bibliography(pdf_path):
    """Uploads a PDF file directly to the API and attempts to extract bibliography entries."""
    print(f"[INFO] Uploading PDF directly for bibliography extraction: {pdf_path}", flush=True)
    start_time = time.time()
    bibliography_data = []
    uploaded_file = None
    
    try:
        # Upload the PDF file
        uploaded_file = genai.upload_file(pdf_path)
        uploaded_file_uri = uploaded_file.uri
        print(f"[DEBUG] Uploaded PDF as URI: {uploaded_file_uri}", flush=True)
        
        # Prepare the prompt for bibliography extraction
        prompt = BIBLIOGRAPHY_PROMPT<
        wait_for_rate_limit()
        
        # Send request to API with uploaded file
        response = api_model.generate_content(
            contents=[{"role": "user", "parts": [{"text": prompt}, {"file_data": {"file_uri": uploaded_file_uri, "mime_type": "application/pdf"}}]}],
            generation_config=GenerationConfig(
                temperature=0.0,
                response_mime_type="application/json"
            )
        )
        
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
    except Exception as e:
        print(f"[ERROR] API error while processing {pdf_path}: {e}", flush=True)
        bibliography_data = []
    finally:
        # Attempt to delete the uploaded file
        if uploaded_file:
            try:
                genai.delete_file(uploaded_file.name)
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

# --- Main Processing Logic ---

def process_single_pdf(pdf_path, output_dir, summary_dict, summary_lock):
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
            # Save Output
            output_filename = f"{os.path.splitext(filename)[0]}_bibliography.json"
            output_path = os.path.join(output_dir, output_filename)
            try:
                write_start = time.time()
                with open(output_path, 'w', encoding='utf-8') as f_out:
                    json.dump(bibliography_data, f_out, indent=2, ensure_ascii=False)
                write_duration = time.time() - write_start
                print(f"[SUCCESS] Saved bibliography for {filename} to {output_path} (write took {write_duration:.2f} seconds).", flush=True)
                file_success = True
            except Exception as e:
                failure_reason = f"Failed to write JSON output: {e}"
                print(f"[ERROR] {failure_reason} for {filename}", flush=True)
                file_success = False

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

def process_directory_v2(input_dir, output_dir, max_workers=5):
    """Processes all PDFs in the input directory concurrently, saving to the specified output dir."""
    print(f"[INFO] Processing directory: {input_dir}", flush=True)
    print(f"[INFO] Using output directory: {output_dir}", flush=True)

    # Find all PDFs recursively using glob
    pdf_files = glob.glob(os.path.join(input_dir, "**/*.pdf"), recursive=True)
    print(f"[INFO] Found {len(pdf_files)} PDF files in {input_dir} and its subdirectories.", flush=True)
    if not pdf_files:
        print("[WARNING] No PDF files found to process. Exiting.", flush=True)
        return

    print(f"[INFO] Starting concurrent processing with up to {max_workers} workers and 15 RPM limit.", flush=True)
    print(f"[INFO] Results will be saved in: {output_dir}", flush=True)

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
            # Submit tasks for each PDF, passing the actual_output_dir
            futures = [executor.submit(process_single_pdf, pdf_path, output_dir, results_summary, summary_lock)
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
    parser.add_argument('--output-dir', required=True, help='Directory to save output JSON files.')
    parser.add_argument('--workers', type=int, default=5, help='Maximum number of concurrent worker threads.')
    args = parser.parse_args()

    input_path = args.input_dir
    output_dir = args.output_dir
    max_workers = args.workers

    # Check if input is a directory or single file
    if os.path.isdir(input_path):
        print(f"[INFO] Processing directory: {input_path}", flush=True)
        process_directory_v2(input_path, output_dir, max_workers)
    elif os.path.isfile(input_path) and input_path.lower().endswith('.pdf'):
        print(f"[INFO] Processing single PDF file: {input_path}", flush=True)
        summary = {'processed_files': 0, 'successful_files': 0, 'failed_files': 0, 'failures': []}
        process_single_pdf(input_path, output_dir, summary, Lock())
        print(f"[INFO] Single file processing complete. Summary: {summary}", flush=True)
    else:
        print(f"[ERROR] Input {input_path} is neither a directory nor a PDF file.", flush=True)

    start_total_time = time.time()
    print(f"\n[INFO] Total processing time: {time.time() - start_total_time:.2f} seconds.", flush=True)
