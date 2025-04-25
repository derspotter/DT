import os
import json
import time
import glob
import argparse
from pdfminer.high_level import extract_text
from dotenv import load_dotenv
import google.generativeai as genai

# --- Configuration ---
# Use Gemini Flash by default for speed
DEFAULT_MODEL_NAME = 'gemini-2.0-flash-lite' # Updated model name
# DEFAULT_MODEL_NAME = 'gemini-1.5-flash-latest' 
# DEFAULT_MODEL_NAME = 'deepseek-chat' # Can switch later if needed

# Global client/model variable
api_model = None

# --- Helper Functions ---

def load_api_key():
    """Loads the appropriate API key based on the chosen model."""
    load_dotenv() # Load .env for local execution
    if 'google' in DEFAULT_MODEL_NAME or 'gemini' in DEFAULT_MODEL_NAME:
        key = os.getenv('GOOGLE_API_KEY')
        if not key:
            print("[ERROR] GOOGLE_API_KEY not found.", flush=True)
        return key
    elif 'deepseek' in DEFAULT_MODEL_NAME:
        # Placeholder for DeepSeek key if needed later
        # key = os.getenv('DEEPSEEK_API_KEY')
        # if not key:
        #     print("[ERROR] DEEPSEEK_API_KEY not found.", flush=True)
        # return key
        print("[ERROR] DeepSeek configuration not yet implemented in V2.", flush=True)
        return None
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
        # Add elif for deepseek if needed later
        # elif 'deepseek' in DEFAULT_MODEL_NAME:
            # from openai import OpenAI
            # api_model = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
            # print("[INFO] Successfully configured DeepSeek client.", flush=True)
            # return True
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
    system_prompt = "You are an expert academic assistant. Extract all bibliography reference entries from the provided text. Return the result ONLY as a JSON array of objects. Each object should represent one reference and include fields like 'authors' (as a list of strings), 'year' (integer), 'title', 'source', 'volume', 'pages', etc., where available. If no references are found, return an empty JSON array []."
    # Fix: Added closing quote to the f-string
    user_prompt = f"Extract bibliography entries from the following text:\n\n---\n{text_content}\n---\n\nOutput only the JSON array:"

    start_time = time.time()
    print(f"[DEBUG] Sending request to {DEFAULT_MODEL_NAME} API...", flush=True)
    try:
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
        # --- Add DeepSeek logic here if needed later ---
        # elif 'deepseek' in DEFAULT_MODEL_NAME:
            # response = api_model.chat.completions.create(...) # DeepSeek specific call
            # raw_response_content = response.choices[0].message.content
        else:
            print("[ERROR] API call logic not implemented for this model.", flush=True)
            return None

        api_call_duration = time.time() - start_time
        print(f"[TIMER] API call took {api_call_duration:.2f} seconds.", flush=True)
        print(f"[DEBUG] Raw API response content: {raw_response_content[:500]}...", flush=True)

        # --- Robust JSON Parsing ---
        try:
            parsed_json = json.loads(raw_response_content)
            # Handle cases where the API might wrap the list in a dict (like observed with DeepSeek)
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

def process_directory_v2(input_dir, output_dir):
    """Processes all PDFs in the input directory."""
    os.makedirs(output_dir, exist_ok=True)
    pdf_files = glob.glob(os.path.join(input_dir, '*.pdf'))
    
    if not pdf_files:
        print(f"[WARNING] No PDF files found in {input_dir}", flush=True)
        return

    print(f"[DEBUG] Starting processing of directory: {input_dir}", flush=True)
    results_summary = {
        "processed_files": 0,
        "successful_files": 0,
        "failed_files": 0,
        "failures": []
    }

    for pdf_path in pdf_files:
        start_process_time = time.time()
        filename = os.path.basename(pdf_path)
        print(f"\n[INFO] Processing {filename}...", flush=True)
        results_summary["processed_files"] += 1

        # 1. Extract Text
        text = extract_text_from_pdf_v2(pdf_path)
        if text is None:
            results_summary["failed_files"] += 1
            results_summary["failures"].append({"file": filename, "reason": "Text extraction failed"})
            continue
        extract_duration = time.time() - start_process_time
        print(f"[TIMER] Text extraction took {extract_duration:.2f} seconds.", flush=True)

        # 2. Get Bibliography via API
        bibliography_data = get_bibliography_via_api(text)
        api_duration = time.time() - start_process_time - extract_duration
        print(f"[TIMER] Bibliography extraction function call took {api_duration:.2f} seconds.", flush=True)

        if bibliography_data is None or not bibliography_data: # Handles None and empty list
             print(f"[WARNING] No bibliography found or extracted for {filename}", flush=True)
             results_summary["failed_files"] += 1
             results_summary["failures"].append({"file": filename, "reason": "No bibliography returned by API or processing failed"})
             # Optionally save an empty file? For now, we just record failure.
        else:
            # 3. Save Output
            output_filename = f"{os.path.splitext(filename)[0]}_bibliography.json"
            output_path = os.path.join(output_dir, output_filename)
            try:
                write_start = time.time()
                with open(output_path, 'w', encoding='utf-8') as f_out:
                    json.dump(bibliography_data, f_out, indent=2, ensure_ascii=False)
                write_duration = time.time() - write_start
                print(f"[SUCCESS] Saved bibliography to {output_path} (write took {write_duration:.2f} seconds).", flush=True)
                results_summary["successful_files"] += 1
            except Exception as e:
                print(f"[ERROR] Failed to write JSON output for {filename}: {e}", flush=True)
                results_summary["failed_files"] += 1
                results_summary["failures"].append({"file": filename, "reason": f"Failed to write JSON: {e}"}) 

        total_duration = time.time() - start_process_time
        print(f"[TIMER] Total processing for {filename} took {total_duration:.2f} seconds.", flush=True)

    # --- Save Summary ---
    summary_path = os.path.join(output_dir, 'processing_summary_v2.json')
    try:
        with open(summary_path, 'w', encoding='utf-8') as f_summary:
            json.dump(results_summary, f_summary, indent=2, ensure_ascii=False)
        print(f"\nProcessing Summary saved to {summary_path}", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to write summary file: {e}", flush=True)

    print("\n--- APIscraper_v2.py MAIN END ---", flush=True)


# --- Entry Point ---

if __name__ == "__main__":
    print("--- APIscraper_v2.py MAIN START ---", flush=True)

    # Configure API client at the start
    if not configure_api_client():
        print("[FATAL] Exiting due to API client configuration failure.", flush=True)
        exit(1) # Or handle more gracefully depending on needs

    parser = argparse.ArgumentParser(description='V2: Extract bibliographies from PDFs using an API.')
    parser.add_argument('--input-dir', required=True, help='Directory containing input PDF files.')
    parser.add_argument('--output-dir', required=True, help='Directory to save output JSON files.')
    args = parser.parse_args()

    print(f"Input PDF directory: {args.input_dir}")
    print(f"Output JSON directory: {args.output_dir}")

    process_directory_v2(args.input_dir, args.output_dir)
