# filepath: /home/jay/DT/dl_lit/main_json.py
import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
import json
import os
import sys
import argparse
import time
from dotenv import load_dotenv

# --- Configuration ---
# Use a model suitable for large document analysis and JSON output
MODEL_NAME = 'gemini-2.5-flash-preview-04-17' # Or 'gemini-1.5-pro' if higher quality needed

# --- Helper Functions ---

def load_and_configure_api():
    """Loads Google API key and configures the genai client."""
    print("--- Configuring GenAI --- ", flush=True)
    load_dotenv()
    api_key = os.getenv('GOOGLE_API_KEY')
    if not api_key:
        print("Error: GOOGLE_API_KEY environment variable not set.", flush=True)
        return None
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(MODEL_NAME)
        print(f"Successfully configured Google Generative AI with API key for model {MODEL_NAME}.")
        return model
    except Exception as e:
        print(f"Error configuring Google Generative AI: {e}")
        return None

def clean_json_response(text):
    """Clean JSON response from markdown formatting."""
    text = text.strip()
    if text.startswith('```json'):
        text = text[7:]
    elif text.startswith('```'):
        text = text[3:]
    if text.endswith('```'):
        text = text[:-3]
    return text.strip()

def upload_pdf_to_gemini(pdf_path: str) -> genai.types.File | None:
    """Uploads a PDF file to Google Generative AI and returns the File object."""
    print(f"--- Uploading PDF: {pdf_path} ---", flush=True)
    try:
        # Upload the file and specify the MIME type explicitly
        uploaded_file = genai.upload_file(path=pdf_path, mime_type='application/pdf')
        # Wait briefly for the file processing to complete (optional but can help)
        time.sleep(5)
        print(f"Successfully uploaded PDF. File Name: {uploaded_file.name}", flush=True)
        return uploaded_file
    except Exception as e:
        print(f"Error uploading PDF {pdf_path}: {e}", flush=True)
        return None

def extract_bibliography_from_pdf(model: genai.GenerativeModel, uploaded_pdf: genai.types.File):
    """Prompts the model to extract the bibliography entry for the uploaded PDF itself."""
    print(f"--- Extracting self-bibliography entry from {uploaded_pdf.name} ---", flush=True)
    prompt = """
You are an expert research assistant. Your task is to identify and extract the bibliographic information for the provided PDF document itself. Analyze the document (e.g., title page, headers, footers, first few pages, metadata if accessible) to determine its own citation details. Return this information as a single JSON object. The object should have the following fields where available:

- authors: array of author names (strings)
- year: publication year (integer or string)
- title: title of the work (string)
- source: journal name, book title, conference name, etc. (string)
- volume: journal volume (integer or string)
- issue: journal issue (integer or string)
- pages: page range (string, e.g., "1-15" or total pages if applicable)
- publisher: publisher name if present (string)
- location: place of publication if present (string)
- type: type of document (e.g., 'journal article', 'book', 'conference paper', 'report', etc.) (string)
- doi: DOI if present (string)
- url: URL if present (string)
- isbn: ISBN if present for books (string)
- issn: ISSN if present for journals (string)
- abstract: abstract if present (string)
- keywords: array of keywords if present (strings)
- language: language of publication if not English (string)

INSTRUCTIONS:
1. Analyze the provided document to find its own bibliographic details.
2. Format the output STRICTLY as a single JSON object.
3. Return ONLY the JSON object. Do not include any introductory text, explanations, apologies, or markdown formatting like ```json.
4. If you cannot confidently determine the bibliographic information for the document itself, return an empty JSON object {}.
"""
    try:
        print("Sending request to Gemini model...", flush=True)
        # Configure for JSON output
        generation_config = genai.types.GenerationConfig(
            response_mime_type="application/json"
        )
        # Make the API call
        response = model.generate_content(
            [prompt, uploaded_pdf],
            generation_config=generation_config,
            request_options={'timeout': 600} # Timeout can likely be shorter now
        )
        print("Received response from Gemini model.", flush=True)
        # print(f"Raw response: {response.text[:500]}...", flush=True) # Debugging

        if response.text:
            try:
                # Expecting a single JSON object now
                bibliography_entry = json.loads(response.text)
                if isinstance(bibliography_entry, dict):
                    # Check if it's not empty (unless empty is the intended 'not found' signal)
                    if bibliography_entry:
                        print("Successfully parsed self-bibliography JSON object.", flush=True)
                    else:
                         print("Parsed an empty JSON object {} - likely means bibliography info not found.", flush=True)
                    return bibliography_entry # Return the dict
                else:
                    print(f"Error: Parsed JSON is not a dictionary. Type: {type(bibliography_entry)}", flush=True)
                    print(f"Raw response: {response.text}", flush=True) # Log the problematic response
                    # Attempt cleaning as a fallback
                    cleaned_text = clean_json_response(response.text)
                    try:
                        bibliography_entry = json.loads(cleaned_text)
                        if isinstance(bibliography_entry, dict):
                             print("Successfully parsed self-bibliography JSON object after cleaning.", flush=True)
                             return bibliography_entry
                        else:
                            print(f"Error: Cleaned JSON is still not a dictionary. Type: {type(bibliography_entry)}", flush=True)
                            return None
                    except json.JSONDecodeError as clean_e:
                        print(f"Error parsing cleaned JSON response: {clean_e}", flush=True)
                        print(f"Cleaned text was: {cleaned_text}", flush=True)
                        return None

            except json.JSONDecodeError as e:
                print(f"Error parsing JSON response: {e}", flush=True)
                print(f"Raw response: {response.text}", flush=True)
                # Try cleaning even if mime type was set
                cleaned_text = clean_json_response(response.text)
                try:
                    bibliography_entry = json.loads(cleaned_text)
                    if isinstance(bibliography_entry, dict):
                         print("Successfully parsed self-bibliography JSON object after cleaning.", flush=True)
                         return bibliography_entry
                    else:
                        print(f"Error: Cleaned JSON is still not a dictionary. Type: {type(bibliography_entry)}", flush=True)
                        return None
                except json.JSONDecodeError as clean_e:
                    print(f"Error parsing cleaned JSON response: {clean_e}", flush=True)
                    print(f"Cleaned text was: {cleaned_text}", flush=True)
                    return None
        else:
            print("Empty response from model.", flush=True)
            return None

    except google_exceptions.GoogleAPIError as e:
        print(f"Error during Gemini API call: {e}", file=sys.stderr, flush=True)
        return None
    except Exception as e:
        print(f"Unexpected error during bibliography extraction: {e}", file=sys.stderr, flush=True)
        return None

def delete_uploaded_file(uploaded_file: genai.types.File):
    """Deletes the file from Google Generative AI storage."""
    if not uploaded_file:
        return
    print(f"--- Deleting uploaded file: {uploaded_file.name} ---", flush=True)
    try:
        genai.delete_file(uploaded_file.name)
        print("Successfully deleted uploaded file.", flush=True)
    except Exception as e:
        print(f"Warning: Failed to delete uploaded file {uploaded_file.name}: {e}", flush=True)

# --- Main Execution ---

if __name__ == "__main__":
    print("--- main_json.py script started ---", flush=True)

    # --- Argument Parsing Setup ---
    parser = argparse.ArgumentParser(description='Extract bibliography from a PDF using Gemini and save as JSON.')
    parser.add_argument('--input-pdf', required=True, help='Path to the input PDF file.')
    parser.add_argument('--output-dir', required=True, help='Directory to save the output JSON file.')
    args = parser.parse_args()

    # Validate input PDF path
    if not os.path.isfile(args.input_pdf):
        print(f"Error: Input PDF not found at {args.input_pdf}", flush=True)
        sys.exit(1)

    # Expand user path for output directory
    output_dir_expanded = os.path.expanduser(args.output_dir)

    # Create output directory if it doesn't exist
    os.makedirs(output_dir_expanded, exist_ok=True)

    print(f"Input PDF: {args.input_pdf}", flush=True)
    print(f"Output directory: {output_dir_expanded}", flush=True)

    # --- Workflow ---
    model = load_and_configure_api()
    uploaded_file = None # Initialize to ensure finally block works

    if model:
        try:
            # 1. Upload PDF
            uploaded_file = upload_pdf_to_gemini(args.input_pdf)

            if uploaded_file:
                # 2. Extract Bibliography (now expects a dict)
                bibliography_result = extract_bibliography_from_pdf(model, uploaded_file)

                # Check if a non-None dictionary was returned
                if bibliography_result is not None and isinstance(bibliography_result, dict):
                    # 3. Save Output (saving a dict is fine)
                    base_name = os.path.splitext(os.path.basename(args.input_pdf))[0]
                    # Keep the filename convention, or change if desired e.g., _metadata.json
                    output_filename = f"{base_name}_bibliography.json"
                    output_path = os.path.join(output_dir_expanded, output_filename)

                    try:
                        with open(output_path, 'w', encoding='utf-8') as f_out:
                            # Dump the single dictionary
                            json.dump(bibliography_result, f_out, indent=2, ensure_ascii=False)
                        print(f"--- Successfully saved bibliography entry to: {output_path} ---", flush=True)
                    except Exception as e:
                        print(f"Error saving JSON output to {output_path}: {e}", flush=True)
                # Handle case where API returned None or not a dict
                elif bibliography_result is None:
                     print("--- Failed to extract bibliography entry (API error or parsing issue). No JSON file saved. ---", flush=True)
                else: # It wasn't None, but wasn't a dict (shouldn't happen with current logic, but good practice)
                     print(f"--- Received unexpected result type ({type(bibliography_result)}), expected dict. No JSON file saved. ---", flush=True)

            else:
                print("--- PDF upload failed. Cannot proceed with extraction. ---", flush=True)

        except Exception as e:
            print(f"An unexpected error occurred during processing: {e}", flush=True)
        finally:
            # 4. Cleanup uploaded file
            delete_uploaded_file(uploaded_file)
    else:
        print("--- API configuration failed. Exiting. ---", flush=True)
        sys.exit(1)

    print("--- main_json.py script finished ---", flush=True)

