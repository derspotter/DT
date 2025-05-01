import os
import json
import time
import glob
import argparse
from pdfminer.high_level import extract_text
from dotenv import load_dotenv
import google.generativeai as genai
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

def extract_text_from_pdf_v2(pdf_path):
    """Extracts text content from a PDF file."""
    try:
        print(f"[DEBUG] Extracting text from PDF: {pdf_path}", flush=True)
        text = extract_text(pdf_path)
        print(f"[DEBUG] Text successfully extracted.", flush=True)
        return text
    except Exception as e:
        print(f"[ERROR] Failed to extract text from {pdf_path}: {e}", flush=True)
        return None

def get_bibliography_via_api(text_content):
    """Sends text to the configured API and attempts to extract bibliography."""
    if not api_model:
        print("[ERROR] API client not configured. Cannot extract bibliography.", flush=True)
        return None

    # Basic prompt - can be refined
    system_prompt = """You are an expert research assistant. Your task is to extract bibliography or reference list entries from the provided text, which is a page from a PDF document. Return the bibliography entries as a JSON array of objects. Each object should represent a single bibliography entry and have the following fields where available:

- authors: array of author names (strings)
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

Return ONLY the JSON array. Do not include any introductory text, explanations, apologies, or markdown formatting like ```json. If no bibliography entries are found in the text, return an empty JSON array []."""
    # Fix: Added closing quote to the f-string
    user_prompt = f"Extract bibliography entries from the following text:\n\n---\n{text_content}\n---\n\nOutput only the JSON array:"

    start_time = time.time()
    print(f"[DEBUG] Sending request to {DEFAULT_MODEL_NAME} API...", flush=True)
    try:
        # --- Wait for Rate Limit before API Call ---
        wait_for_rate_limit()

        # --- Gemini API Call ---
        if 'google' in DEFAULT_MODEL_NAME or 'gemini' in DEFAULT_MODEL_NAME:
             # Configure for JSON output with Gemini
            generation_config = genai.types.GenerationConfig(
                response_mime_type="application/json" 
            )
            response = api_model.generate_content(
                [system_prompt, user_prompt], # Using prompts similar to chat
                generation_config=generation_config 
            )
            raw_response_content = response.text # Gemini specific
        else:
            print("[ERROR] API call logic not implemented for this model.", flush=True)
            return None

        api_call_duration = time.time() - start_time
        print(f"[TIMER] API call took {api_call_duration:.2f} seconds.", flush=True)
        print(f"[DEBUG] Raw API response content: {raw_response_content[:500]}...", flush=True)

        # --- Robust JSON Parsing ---
        try:
            parsed_json = json.loads(raw_response_content)
            if isinstance(parsed_json, dict) and len(parsed_json) == 1:
                # If it's a dict with one key, assume the value is the list we want
                potential_list = next(iter(parsed_json.values()))
                if isinstance(potential_list, list):
                    print("[DEBUG] Parsed JSON was dict, extracted list from its value.", flush=True)
                    return potential_list 
            # If it's already a list, great!
            if isinstance(parsed_json, list):
                print("[DEBUG] Parsed JSON successfully as list.", flush=True)
                return parsed_json
            else:
                # It's valid JSON, but not a list or expected dict format
                print(f"[ERROR] Parsed JSON is not a list or expected dict format. Type: {type(parsed_json)}", flush=True)
                return None
        except json.JSONDecodeError as json_e:
            print(f"[ERROR] Failed to parse API response as JSON: {json_e}", flush=True)
            print(f"[DEBUG] Failing content: {raw_response_content}", flush=True)
            return None

    except Exception as api_e:
        api_call_duration = time.time() - start_time
        print(f"[ERROR] API call failed after {api_call_duration:.2f} seconds: {api_e}", flush=True)
        return None


# --- Main Processing Logic ---

def process_single_pdf(pdf_path, output_dir, summary_dict, summary_lock):
    """Processes a single PDF file: extract text, call API, save result."""
    filename = os.path.basename(pdf_path)
    print(f"\n[INFO] Processing {filename}...", flush=True)
    start_process_time = time.time()
    file_success = False
    failure_reason = "Unknown"

    try:
        # 1. Extract Text
        text = extract_text_from_pdf_v2(pdf_path)
        if text is None:
            failure_reason = "Text extraction failed"
            raise ValueError(failure_reason)
        extract_duration = time.time() - start_process_time
        print(f"[TIMER] Text extraction for {filename} took {extract_duration:.2f} seconds.", flush=True)

        # 2. Get Bibliography via API
        stage_start_time = time.time()
        bibliography_data = get_bibliography_via_api(text)
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
            # 3. Save Output
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
        # Catch errors during text extraction or API call steps
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

def process_directory_v2(input_dir, base_output_dir, max_workers=5):
    """Processes all PDFs in the input directory concurrently, saving to a temporary subdir."""
    # Create a timestamped temporary directory within the base output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_output_dir_name = f"temp_scrape_{timestamp}"
    actual_output_dir = os.path.join(base_output_dir, temp_output_dir_name)
    
    try:
        os.makedirs(actual_output_dir, exist_ok=True)
        print(f"[INFO] Created temporary output directory: {actual_output_dir}", flush=True)
    except OSError as e:
        print(f"[ERROR] Could not create temporary output directory {actual_output_dir}: {e}", flush=True)
        return # Cannot proceed without output directory

    pdf_files = glob.glob(os.path.join(input_dir, '*.pdf'))

    if not pdf_files:
        print(f"[WARNING] No PDF files found in {input_dir}", flush=True)
        return

    print(f"[INFO] Found {len(pdf_files)} PDF files in {input_dir}.")
    print(f"[INFO] Starting concurrent processing with up to {max_workers} workers and 15 RPM limit.", flush=True)
    print(f"[INFO] Results will be saved in: {actual_output_dir}", flush=True)
    results_summary = {
        "processed_files": 0,
        "successful_files": 0,
        "failed_files": 0,
        "failures": []
    }
    summary_lock = Lock()

    # Use ThreadPoolExecutor for I/O-bound tasks
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks for each PDF, passing the actual_output_dir
        futures = [executor.submit(process_single_pdf, pdf_path, actual_output_dir, results_summary, summary_lock)
                   for pdf_path in pdf_files]

        # Wait for all tasks to complete and handle potential exceptions
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Retrieve result or re-raise exception from the thread
            except Exception as exc:
                print(f'[ERROR] An error occurred in a worker thread: {exc}', flush=True)
                # Error is already logged within process_single_pdf and summary updated

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
    # Changed argument name
    parser.add_argument('--base-output-dir', required=True, help='Base directory to create a temporary subdir for output JSON files.')
    parser.add_argument('--workers', type=int, default=5, help='Maximum number of concurrent worker threads.')
    args = parser.parse_args()

    print(f"Input PDF directory: {args.input_dir}")
    print(f"Base Output directory: {args.base_output_dir}")

    # Process the directory
    start_total_time = time.time()
    # Pass workers as a keyword argument
    process_directory_v2(args.input_dir, args.base_output_dir, max_workers=args.workers)
    end_total_time = time.time()
    print(f"\n[INFO] Total processing time: {end_total_time - start_total_time:.2f} seconds.", flush=True)
