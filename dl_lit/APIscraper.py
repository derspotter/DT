import os
import json
import time
import glob
import argparse
from pdfminer.high_level import extract_text
from dotenv import load_dotenv
from openai import OpenAI

def extract_text_from_pdf(pdf_path):
    try:
        print(f"[DEBUG] Extracting text from PDF: {pdf_path}", flush=True)
        text = extract_text(pdf_path)
        if text:
            print(f"[DEBUG] Text successfully extracted from {pdf_path}", flush=True)
        else:
            print(f"[DEBUG] No text found in {pdf_path}", flush=True)
        return text
    except Exception as e:
        print(f"[ERROR] Error extracting text from {pdf_path}: {e}", flush=True)
        return None

def extract_bibliography(text):
    """Extracts bibliography entries from text using DeepSeek AI."""
    if not deepseek_client:
        print("[ERROR] DeepSeek client not configured. Cannot extract bibliography.", flush=True)
        return []

    # Define the system prompt for DeepSeek
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

    user_prompt = f"Extract bibliography entries from the following text:\n\n---\n{text}\n---"

    try:
        start_time = time.time()
        try:
            print(f"[DEBUG] Sending request to DeepSeek API...", flush=True)
            response = deepseek_client.chat.completions.create(
                model="deepseek-chat", # Or use "deepseek-coder" if more appropriate
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                stream=False,
                response_format={"type": "json_object"} # Request JSON output directly if supported
            )
        finally:
            api_call_duration = time.time() - start_time
            print(f"[TIMER] DeepSeek API call took {api_call_duration:.2f} seconds.", flush=True)

        print(f"[DEBUG] Received response from DeepSeek API", flush=True)

        response_content = response.choices[0].message.content
        print(f"[DEBUG] Raw API response content: {response_content[:500]}...", flush=True)

        try:
            # The response_content should ideally be the JSON string directly
            # thanks to response_format={'type': 'json_object'}
            bibliography_list = json.loads(response_content)
            if isinstance(bibliography_list, dict) and 'bibliography' in bibliography_list and isinstance(bibliography_list['bibliography'], list):
                 # Handle case where model wraps the list in a key like {"bibliography": [...]}
                 print("[DEBUG] Extracted list from 'bibliography' key in response.", flush=True)
                 bibliography_list = bibliography_list['bibliography']
            elif not isinstance(bibliography_list, list):
                print(f"[ERROR] Expected JSON array, got {type(bibliography_list)}. Content: {response_content[:200]}", flush=True)
                return [] # Or try to salvage if possible
            return bibliography_list

        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to decode JSON from DeepSeek response: {e}", flush=True)
            print(f"[ERROR] Response content was: {response_content[:500]}", flush=True)
            # Optional: Add more sophisticated salvage logic here if needed
            return []
        except Exception as e:
            print(f"[ERROR] Unexpected error processing DeepSeek response: {e}", flush=True)
            print(f"[ERROR] Raw response content: {response_content[:500]}", flush=True)
            return []

    except Exception as e:
        print(f"[ERROR] Failed to call DeepSeek API: {e}", flush=True)
        # Log the exception details for debugging
        import traceback
        traceback.print_exc()
        return []

def process_pdf_directory(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    results = {
        'success': [],
        'failure': []
    }
    print(f"[DEBUG] Starting processing of directory: {input_dir}", flush=True)
    
    for pdf_path in glob.glob(os.path.join(input_dir, "*.pdf")):
        print(f"\n[INFO] Processing {os.path.basename(pdf_path)}...", flush=True)
        start_time_pdf = time.time()
        try:
            start_time_extract = time.time()
            text = extract_text_from_pdf(pdf_path)
            end_time_extract = time.time()
            print(f"[TIMER] Text extraction for {os.path.basename(pdf_path)} took {end_time_extract - start_time_extract:.2f} seconds.", flush=True)

            if not text or not text.strip():
                print(f"[WARNING] No text extracted from {os.path.basename(pdf_path)}. Skipping...", flush=True)
                results['failure'].append((os.path.basename(pdf_path), "No text extracted"))
                continue

            overall_bib_start_time = time.time()
            bibliography_data = extract_bibliography(text)
            overall_bib_end_time = time.time()
            print(f'[TIMER] Bibliography extraction function call for {os.path.basename(pdf_path)} took {overall_bib_end_time - overall_bib_start_time:.2f} seconds.', flush=True)

            if bibliography_data:
                output_path = os.path.join(output_dir, f"{os.path.splitext(os.path.basename(pdf_path))[0]}_bibliography.json")
                start_time_write = time.time()
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(bibliography_data, f, indent=2, ensure_ascii=False)
                end_time_write = time.time()
                print(f"[SUCCESS] Saved bibliography to {output_path} (write took {end_time_write - start_time_write:.2f} seconds).", flush=True)
                results['success'].append(os.path.basename(pdf_path))
            else:
                print(f"[WARNING] No bibliography found for {os.path.basename(pdf_path)}", flush=True)
                results['failure'].append((os.path.basename(pdf_path), "No bibliography found"))
                
        except Exception as e:
            print(f"[ERROR] Error processing {os.path.basename(pdf_path)}: {str(e)}", flush=True)
            results['failure'].append((os.path.basename(pdf_path), str(e)))
        finally:
            end_time_pdf = time.time()
            print(f"[TIMER] Total processing for {os.path.basename(pdf_path)} took {end_time_pdf - start_time_pdf:.2f} seconds.", flush=True)
    
    print("\nProcessing Summary:", flush=True)
    print(f"Successfully processed: {len(results['success'])} files", flush=True)
    print(f"Failed to process: {len(results['failure'])} files", flush=True)
    
    if results['failure']:
        print("\nFailed files and reasons:", flush=True)
        for file, reason in results['failure']:
            print(f"- {file}: {reason}", flush=True)
    
    summary_path = os.path.join(output_dir, "processing_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nSummary saved to {summary_path}", flush=True)

def configure_deepseek():
    global deepseek_client
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        print("[ERROR] DEEPSEEK_API_KEY environment variable not set.", flush=True)
        return False
    try:
        deepseek_client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
        print("[INFO] Successfully configured DeepSeek client.", flush=True)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to configure DeepSeek client: {e}", flush=True)
        return False

if __name__ == "__main__":
    print("--- APIscraper.py MAIN START ---", flush=True)
    # Attempt to load .env file for local execution. Docker env vars take precedence if set.
    load_dotenv() 
    
    deepseek_client = None
    DEEPSEEK_CONFIGURED = configure_deepseek()

    # --- Argument Parsing Setup ---
    parser = argparse.ArgumentParser(description='Extract bibliographies from PDF files in a directory.')
    parser.add_argument('--input-dir', required=True, help='Directory containing PDF files to process.')
    parser.add_argument('--output-dir', required=True, help='Directory to save extracted bibliography JSON files.')
    args = parser.parse_args()
    # --- End Argument Parsing Setup ---

    # Use arguments directly
    INPUT_DIR = args.input_dir
    OUTPUT_DIR = args.output_dir

    # Validate input directory
    if not os.path.isdir(INPUT_DIR):
        print(f"Error: Input directory not found at {INPUT_DIR}", flush=True)
        sys.exit(1)

    print(f"Input PDF directory: {INPUT_DIR}", flush=True)
    print(f"Output JSON directory: {OUTPUT_DIR}", flush=True)

    # Pass arguments to the processing function
    process_pdf_directory(INPUT_DIR, OUTPUT_DIR)
    print("--- APIscraper.py MAIN END ---", flush=True)