# filepath: /home/jay/DT/dl_lit/main_json.py
import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
import json
import os
import sys
import argparse
import time
from dotenv import load_dotenv
import re  # Added import
import uuid  # Added import
import hashlib  # Added import
import requests  # Added import
from urllib.parse import quote_plus  # Added import
from collections import deque  # Added import

# --- Configuration ---
# Use a model suitable for large document analysis and JSON output
MODEL_NAME = 'gemini-2.5-flash-preview-04-17'  # Or 'gemini-1.5-pro' if higher quality needed

# --- Helper Functions ---

# Added BibTeX type mapping
BIBTEX_TYPE_MAP = {
    'journal article': 'article',
    'article': 'article',
    'book': 'book',
    'conference paper': 'inproceedings',
    'inproceedings': 'inproceedings',
    'report': 'techreport',
    'technical report': 'techreport',
    'thesis': 'phdthesis',  # General assumption
    'mastersthesis': 'mastersthesis',
    'phdthesis': 'phdthesis',
    'preprint': 'article',  # Often treated like articles
    'unpublished': 'unpublished',
    'manual': 'manual',
    'misc': 'misc',
    'miscellaneous': 'misc',
    'webpage': 'misc',  # BibTeX doesn't have a standard webpage type, often handled by misc + howpublished/url
    'software': 'misc',
    'dataset': 'misc',
}

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
            request_options={'timeout': 600}  # Timeout can likely be shorter now
        )
        print("Received response from Gemini model.", flush=True)

        if response.text:
            try:
                bibliography_entry = json.loads(response.text)
                if isinstance(bibliography_entry, dict):
                    if bibliography_entry:
                        print("Successfully parsed self-bibliography JSON object.", flush=True)
                    else:
                        print("Parsed an empty JSON object {} - likely means bibliography info not found.", flush=True)
                    return bibliography_entry
                else:
                    print(f"Error: Parsed JSON is not a dictionary. Type: {type(bibliography_entry)}", flush=True)
                    print(f"Raw response: {response.text}", flush=True)
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

# --- DOI Search Helper Functions and Rate Limiter ---

class SimpleRateLimiter:
    """A simple rate limiter to manage API request frequency."""
    def __init__(self, max_per_second: float):
        self.max_per_second = max_per_second
        self.request_times = deque()

    def wait_if_needed(self):
        """Pauses execution if the request rate exceeds the defined limit."""
        now = time.time()
        # Remove timestamps older than 1 second
        while self.request_times and (now - self.request_times[0]) > 1.0:
            self.request_times.popleft()
        
        if len(self.request_times) >= self.max_per_second:
            wait_duration = (self.request_times[0] + 1.0) - now
            if wait_duration > 0:
                print(f"Rate limiter: waiting for {wait_duration:.2f}s")
                time.sleep(wait_duration)
        self.request_times.append(time.time())

# Instantiate limiters for external DOI search APIs
OPENALEX_MAILTO = "spott@wzb.eu"  # As used in other scripts by user
crossref_limiter = SimpleRateLimiter(max_per_second=10)  # Crossref is generally more permissive

def search_crossref_for_doi(title: str, authors: list | None, year: str | int | None, mailto: str = OPENALEX_MAILTO) -> str | None:
    """Queries Crossref to find a DOI for a publication."""
    if not title:
        return None
        
    crossref_limiter.wait_if_needed()
    
    query_parts = []
    # Use quote_plus for title and author names
    query_parts.append(f'query.bibliographic={quote_plus(title)}')
    
    if authors and isinstance(authors, list) and authors:
        query_parts.append(f'query.author={quote_plus(str(authors[0]))}')  # Use first author

    # Construct URL
    url = f"https://api.crossref.org/works?{'&'.join(query_parts)}&rows=5&mailto={mailto}"
    if year:  # Add year filter if available
        url += f"&filter=from-pub-date:{str(year)}-01-01,until-pub-date:{str(year)}-12-31"

    headers = {'User-Agent': f'DOI_Fetcher/0.1 (mailto:{mailto})'}
    
    try:
        print(f"Querying Crossref with URL: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get("message", {}).get("items"):
            for item in data["message"]["items"]:
                if item.get("DOI"):
                    # Crossref DOIs are usually bare
                    doi_val = item["DOI"]
                    print(f"Crossref found DOI: {doi_val} for title '{item.get('title', ['N/A'])[0]}'")
                    # Additional check for year if provided
                    if year:
                        item_year_parts = item.get("published-print", {}).get("date-parts", [[]])[0] or \
                                          item.get("published-online", {}).get("date-parts", [[]])[0] or \
                                          item.get("published", {}).get("date-parts", [[]])[0]
                        if item_year_parts and str(item_year_parts[0]) == str(year):
                            return doi_val.strip()
                        # If year doesn't match, continue to check other results
                    else:  # No year to check against
                        return doi_val.strip()
    except requests.RequestException as e:
        print(f"Crossref request failed: {e}")
    except json.JSONDecodeError as e:
        print(f"Failed to parse Crossref JSON response: {e}")
    return None

def find_doi_online(title: str | None, authors: list | None, year: str | int | None) -> str | None:
    """
    Attempts to find a DOI online using Crossref.
    Args:
        title: The title of the publication.
        authors: A list of author names (strings).
        year: The publication year.
    Returns:
        A bare DOI string if found, otherwise None.
    """
    if not title:
        print("Cannot search for DOI without a title.")
        return None

    print(f"Attempting to find DOI online for title: {title[:60]}...")

    # Try Crossref
    doi = search_crossref_for_doi(title, authors, year)
    if doi:
        print(f"Found DOI via Crossref: {doi}")
        return doi  # Already confirmed bare

    print(f"Could not find DOI for '{title[:60]}...' via Crossref.")
    return None

# --- New Helper Functions for BibTeX ---
def generate_bibtex_key(entry_data: dict, pdf_filename: str) -> str:
    """Generates a BibTeX key for an entry.
    1. Uses DOI if available.
    2. Falls back to a hash of title, first author, and year.
    3. Falls back to a random ID if other fields are missing.
    """
    doi = entry_data.get('doi')
    if doi:
        # Remove common DOI prefixes and sanitize
        doi_cleaned = re.sub(r'^(https?://)?(dx\.)?doi\.org/', '', doi, flags=re.IGNORECASE)
        # Replace characters not suitable for BibTeX keys, but keep it readable
        bib_key = re.sub(r'[^a-zA-Z0-9_:\-\.]', '_', doi_cleaned)
        # Ensure it's not empty after cleaning
        if bib_key:
            return bib_key

    # Fallback to hash-based key
    title_str = entry_data.get('title', '')
    authors_list = entry_data.get('authors', [])
    first_author_name = ''

    if authors_list and isinstance(authors_list, list) and authors_list[0]:
        # Take the first author's name directly
        first_author_name = str(authors_list[0])
        # Attempt to get last name, simple split for now
        try:
            first_author_name = first_author_name.split()[-1]
        except IndexError:
            pass  # Use full name if split fails

    year_str = str(entry_data.get('year', '')).strip()

    if title_str or first_author_name or year_str:
        # Sanitize parts for key generation to avoid overly complex keys
        # and make them somewhat more readable if possible
        key_title = re.sub(r'\W+', '', title_str.lower())[:20]  # First 20 alphanumeric chars of title
        key_author = re.sub(r'\W+', '', first_author_name.lower())[:15]  # First 15 alphanumeric chars of author
        
        hash_input_str = f"{key_title}|{key_author}|{year_str}"
        
        # If the combined string is too short, use a more robust hash
        if len(hash_input_str) < 10 and (title_str or first_author_name or year_str):  # Check original fields
            # Use a more detailed hash if the simplified one is too short but fields exist
            hash_input_str = f"{title_str}|{first_author_name}|{year_str}"

        hash_object = hashlib.sha1(hash_input_str.encode('utf-8'))
        bib_key = f"ref_{hash_object.hexdigest()[:10]}"
        return bib_key
    else:
        # Absolute fallback: use a portion of a UUID
        return f"ref_{uuid.uuid4().hex[:12]}"

def format_as_bibtex(entry_data: dict, pdf_filename: str, bib_key: str) -> str:
    """Formats the extracted data as a BibTeX entry string."""
    if not entry_data or not isinstance(entry_data, dict):
        return ""

    entry_type_gemini = entry_data.get('type', 'misc').lower().strip()
    bib_type = BIBTEX_TYPE_MAP.get(entry_type_gemini, 'misc')

    bib_items = [f"@{bib_type}{{{bib_key},"]

    if 'title' in entry_data and entry_data['title']:
        bib_items.append(f"  title = {{{{{entry_data['title']}}}}},")

    authors = entry_data.get('authors')
    if authors and isinstance(authors, list) and all(isinstance(a, str) for a in authors):
        bib_items.append(f"  author = {{{{{' and '.join(authors)}}}}},")
    elif isinstance(authors, str) and authors:
        bib_items.append(f"  author = {{{{{authors}}}}},")

    if 'year' in entry_data and entry_data['year']:
        bib_items.append(f"  year = {{{{{str(entry_data['year'])}}}}},")

    if 'doi' in entry_data and entry_data['doi']:
        doi_value = entry_data['doi']
        if not doi_value.startswith('http') and not doi_value.startswith('doi:'):
            doi_value = f"https://doi.org/{doi_value}"
        bib_items.append(f"  doi = {{{{{doi_value}}}}},")

    if 'abstract' in entry_data and entry_data['abstract']:
        abstract_text = str(entry_data['abstract']).replace('\\n', ' ').replace('\n', ' ').strip()
        abstract_text = re.sub(r'\s+', ' ', abstract_text)
        bib_items.append(f"  abstract = {{{{{abstract_text}}}}},")

    original_pdf_basename = os.path.basename(pdf_filename)
    bib_items.append(f"  file = {{{{{original_pdf_basename}:PDF}}}}")

    if bib_type == 'article':
        if 'source' in entry_data and entry_data['source']:
            bib_items.append(f"  journal = {{{{{entry_data['source']}}}}},")
        if 'volume' in entry_data and entry_data['volume']:
            bib_items.append(f"  volume = {{{{{str(entry_data['volume'])}}}}},")
        if 'issue' in entry_data and entry_data['issue']:
            bib_items.append(f"  number = {{{{{str(entry_data['issue'])}}}}},")
        if 'pages' in entry_data and entry_data['pages']:
            bib_items.append(f"  pages = {{{{{entry_data['pages']}}}}},")

    elif bib_type == 'book':
        if 'publisher' in entry_data and entry_data['publisher']:
            bib_items.append(f"  publisher = {{{{{entry_data['publisher']}}}}},")
        if 'isbn' in entry_data and entry_data['isbn']:
            bib_items.append(f"  isbn = {{{{{entry_data['isbn']}}}}},")

    elif bib_type == 'inproceedings':
        if 'source' in entry_data and entry_data['source']:
            bib_items.append(f"  booktitle = {{{{{entry_data['source']}}}}},")
        if 'publisher' in entry_data and entry_data['publisher']:
            bib_items.append(f"  publisher = {{{{{entry_data['publisher']}}}}},")
        if 'pages' in entry_data and entry_data['pages']:
            bib_items.append(f"  pages = {{{{{entry_data['pages']}}}}},")

    elif bib_type == 'techreport':
        if 'publisher' in entry_data and entry_data['publisher']:
            bib_items.append(f"  institution = {{{{{entry_data['publisher']}}}}},")
        elif 'source' in entry_data and entry_data['source']:
            bib_items.append(f"  institution = {{{{{entry_data['source']}}}}},")
        if 'number' in entry_data and entry_data['number']:
            bib_items.append(f"  number = {{{{{str(entry_data['number'])}}}}},")

    if 'url' in entry_data and entry_data['url'] and not ('doi' in entry_data and entry_data['doi']):
        bib_items.append(f"  url = {{{{{entry_data['url']}}}}},")

    if bib_items[-1].endswith(','):
        bib_items[-1] = bib_items[-1][:-1]

    bib_items.append("}")
    return '\n'.join(bib_items)

# --- Main Execution ---

if __name__ == "__main__":
    print("--- main_json.py script started (BibTeX generation mode) ---", flush=True)

    # --- Argument Parsing Setup ---
    parser = argparse.ArgumentParser(description='Extract bibliography from PDFs in a directory using Gemini and save as a single BibTeX file.')
    parser.add_argument('--input-dir', required=True, help='Path to the input directory containing PDF files.')
    parser.add_argument('--output-dir', required=True, help='Directory to save the output BibTeX file.')
    args = parser.parse_args()

    # Validate input directory path
    if not os.path.isdir(args.input_dir):
        print(f"Error: Input directory not found at {args.input_dir}", flush=True)
        sys.exit(1)

    # Expand user path for output directory
    output_dir_expanded = os.path.expanduser(args.output_dir)
    os.makedirs(output_dir_expanded, exist_ok=True)
    output_bib_file_path = os.path.join(output_dir_expanded, "generated_bibliography.bib")

    print(f"Input PDF directory: {args.input_dir}", flush=True)
    print(f"Output BibTeX file: {output_bib_file_path}", flush=True)

    # --- Workflow ---
    model = load_and_configure_api()
    all_bibtex_entries = []

    if model:
        pdf_files_processed = 0
        for filename in os.listdir(args.input_dir):
            if filename.lower().endswith(".pdf"):
                pdf_path = os.path.join(args.input_dir, filename)
                print(f"\n--- Processing PDF: {pdf_path} ---", flush=True)
                uploaded_file = None  # Initialize for each file

                try:
                    # 1. Upload PDF
                    uploaded_file = upload_pdf_to_gemini(pdf_path)

                    if uploaded_file:
                        # 2. Extract Bibliography metadata
                        bibliography_data = extract_bibliography_from_pdf(model, uploaded_file)

                        if bibliography_data and isinstance(bibliography_data, dict) and bibliography_data:
                            # Attempt to find DOI online if Gemini didn't provide one or if it's empty
                            current_doi = bibliography_data.get('doi')
                            if not current_doi or str(current_doi).strip() == "":
                                print(f"DOI missing or empty from Gemini output for {filename}. Attempting online search.")
                                search_title = bibliography_data.get('title')
                                search_authors = bibliography_data.get('authors')  # Expected list of strings
                                search_year = bibliography_data.get('year')
                                
                                online_doi = find_doi_online(search_title, search_authors, search_year)
                                if online_doi:
                                    bibliography_data['doi'] = online_doi  # Update the dict
                                    print(f"Updated DOI for {filename} from online search: {online_doi}")
                                else:
                                    print(f"Online DOI search failed for {filename}.")
                            
                            # 3. Generate BibTeX key
                            bib_key = generate_bibtex_key(bibliography_data, filename)
                            # 4. Format as BibTeX entry
                            bibtex_entry_str = format_as_bibtex(bibliography_data, filename, bib_key)

                            if bibtex_entry_str:
                                all_bibtex_entries.append(bibtex_entry_str)
                                print(f"Successfully generated BibTeX entry for {filename} with key {bib_key}", flush=True)
                                pdf_files_processed += 1
                            else:
                                print(f"Warning: Could not format BibTeX for {filename}. Data: {bibliography_data}", flush=True)
                        elif bibliography_data == {}:
                            print(f"--- No bibliographic information found by model for {filename}. Skipping. ---", flush=True)
                        else:
                            print(f"--- Failed to extract bibliography for {filename} (API error or parsing issue). Skipping. ---", flush=True)
                    else:
                        print(f"--- PDF upload failed for {filename}. Cannot proceed with extraction. ---", flush=True)

                except Exception as e:
                    print(f"An unexpected error occurred during processing of {filename}: {e}", flush=True)
                finally:
                    # 5. Cleanup uploaded file from Gemini
                    if uploaded_file:
                        delete_uploaded_file(uploaded_file)

        # 6. Save all collected BibTeX entries to the .bib file
        if all_bibtex_entries:
            try:
                with open(output_bib_file_path, 'w', encoding='utf-8') as f_out:
                    f_out.write("\n\n".join(all_bibtex_entries))
                    f_out.write("\n")
                print(f"\n--- Successfully processed {pdf_files_processed} PDF(s) and saved all BibTeX entries to: {output_bib_file_path} ---", flush=True)
            except Exception as e:
                print(f"Error writing BibTeX output to {output_bib_file_path}: {e}", flush=True)
        else:
            print("\n--- No BibTeX entries were generated. Output file not created. ---", flush=True)

    else:
        print("--- API configuration failed. Exiting. ---", flush=True)
        sys.exit(1)

    print("--- main_json.py script finished ---", flush=True)

