from google import genai as google_genai
from google.genai import types
from google.api_core import exceptions as google_exceptions
import pikepdf
import json
import os
from collections import defaultdict
import sys
import argparse
import tempfile
import shutil
import time
import concurrent.futures
from dotenv import load_dotenv
import glob

print("--- Script Top --- ", flush=True)

print("--- Imports Done --- ", flush=True)

# --- Added: Configure Google API Key ---
print("--- Configuring GenAI --- ", flush=True)
load_dotenv()
api_key = os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_API_KEY')
api_client = None
if not api_key:
    print("Error: GEMINI_API_KEY (or GOOGLE_API_KEY) environment variable not set.", flush=True)
    # Consider exiting or raising an error if the key is essential
    # sys.exit(1) # Uncomment to exit if key is missing
else:
    try:
        api_client = google_genai.Client(api_key=api_key)
        print("Successfully configured Google GenAI client with API key.")
    except Exception as e:
        print(f"Error configuring Google GenAI client: {e}")
        # Consider exiting or raising an error
        # sys.exit(1)
print("--- GenAI Configured (or skipped) --- ", flush=True)
# --- End Added Section ---


class GenAIModel:
    def __init__(self, model_name: str, temperature: float = 0.0):
        self.model_name = model_name
        self.temperature = temperature

    def generate_content(self, contents, request_options=None):
        if api_client is None:
            raise RuntimeError("GenAI client not configured.")
        config = types.GenerateContentConfig(temperature=self.temperature)

        def _call():
            return api_client.models.generate_content(
                model=self.model_name,
                contents=contents,
                config=config,
            )

        timeout = None
        if request_options and isinstance(request_options, dict):
            timeout = request_options.get("timeout")
        if timeout:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_call)
                try:
                    return future.result(timeout=timeout)
                except concurrent.futures.TimeoutError as exc:
                    future.cancel()
                    raise TimeoutError(f"Gemini request timed out after {timeout}s") from exc
        return _call()


def upload_pdf(path: str, display_name: str | None = None):
    if api_client is None:
        raise RuntimeError("GenAI client not configured.")
    # Follow current Gemini Files API docs: pass the path or file-like object.
    # Display names are optional; omit for simplicity to match the reference example.
    return api_client.files.upload(file=path)


def delete_uploaded(name: str):
    if api_client is None:
        return
    api_client.files.delete(name)

def clean_json_response(text):
    """Clean JSON response from markdown formatting."""
    # Remove markdown code block markers if present
    text = text.strip()
    if text.startswith('```json'):
        text = text[7:]  # Remove ```json prefix
    elif text.startswith('```'):
        text = text[3:]  # Remove ``` prefix
    if text.endswith('```'):
        text = text[:-3]  # Remove ``` suffix
    return text.strip()

def find_reference_section_pages(pdf_path: str, model, uploaded_pdf):
    """Use GenAI to find reference section pages in the PDF."""
    print(f"Attempting to find reference sections in PDF: {pdf_path}", flush=True)
    print("Using pre-uploaded PDF for section finding.", flush=True)
    
    # Comprehensive prompt for finding reference sections
    prompt = """
Your task is to identify ALL bibliography or reference sections in this PDF document.

INSTRUCTIONS:
1. Find sections with references, citations, or bibliographic entries, titled 'References', 'Bibliography', 'Works Cited', or similar. Look for patterns like lists of authors, years, titles, even if untitled.
2. Use printed page numbers visible on the pages. If none, use sequential order as in the PDF.
3. **IMPORTANT: 'start_page' is where the section begins (title or first entry), even if it starts mid-page or at the end after other content. Include this page at all costs. 'end_page' is where the last entry ends, excluding empty or unrelated pages after it.**
4. **IMPORTANT: Find ALL bibliography sections, especially after chapters. Do not skip any reference lists, even short or unusual ones.**
5. Return ONLY a JSON array as shown below, with no extra text or markdown.

FORMAT:
[
    {
        "start_page": 16,  // Printed page number (or sequential if none) where section starts (even mid-page or end)
        "end_page": 19     // Printed page number where last entry ends (exclude empty/unrelated pages)
    }
]

RULES:
- Always include 'start_page' and 'end_page' for each section.
- Use INTEGER numbers only.
- Ensure 'start_page' includes the first entry or title, even mid-page. 'end_page' must be the last entry's page, not beyond.
- Include EVERY bibliography section, checking chapter ends.
- Return ONLY valid JSON, no explanations.
"""

    try:
        # Generate content with timeout
        response = model.generate_content([prompt, uploaded_pdf], request_options={'timeout': 600})
        print("Raw find_reference_section_pages response: ```json")
        print(response.text)
        print("```", flush=True)
        
        # Extract JSON from response
        import re
        json_match = re.search(r'```json\s*([\s\S]*?)\s*```', response.text)
        if json_match:
            json_text = json_match.group(1)
        else:
            json_text = response.text

        sections = json.loads(json_text)
        print(f"Found reference sections: {json.dumps(sections, indent=2)}")
        print("")
        return sections
    except Exception as e:
        print(f"Error in find_reference_section_pages: {e}", flush=True)
        return []

def detect_page_number_offset(pdf_path: str, model: GenAIModel, total_pages: int):
    """
    Sample pages at 20/40/60/80%, extract them, upload as a batch, 
    and use a single prompt to detect the offset.
    Args:
        pdf_path: Path to the PDF file.
        model: The GenerativeModel instance.
        total_pages: Total number of physical pages in the PDF.
    Returns:
        A dictionary containing the detected offset and the samples used, or None if detection fails.
    """
    print("\n--- Detecting Page Number Offset ---", flush=True)
    # Use total_pages passed from the caller
    # with pikepdf.Pdf.open(pdf_path) as doc:
    #     total_pages = len(doc.pages)

    if total_pages <= 0:
        print("Error: Cannot detect offset for PDF with 0 pages.")
        return
        
    offsets = []
    samples = []
    
    # Calculate sample indices based on percentages
    percentages = [0.2, 0.4, 0.6, 0.8]
    # Calculate 0-based index from 1-based percentage calculation
    sample_indices = [max(0, int(p * total_pages) - 1) for p in percentages]

    # Ensure indices are within bounds and unique
    sample_indices = sorted(list(set(idx for idx in sample_indices if 0 <= idx < total_pages)))

    # Handle very short PDFs where percentages might yield no valid indices
    if not sample_indices and total_pages > 0:
        sample_indices = [0] # Sample the first page
    elif not sample_indices and total_pages == 0:
        print("Cannot sample pages, PDF has 0 pages.", flush=True)
        return None

    # Ensure indices are within bounds and sort
    valid_indices = sample_indices # Use the calculated percentage-based indices
    
    print(f"Detecting offset using physical page indices: {valid_indices}", flush=True)
    
    # --- Batch Extraction and Upload --- 
    uploaded_files_info = {} # Store { 'upload_name': {'physical_index': index, 'file_object': file}} 
    temp_file_paths = []
    upload_parts = []

    try:
        with pikepdf.open(pdf_path) as pdf_doc:
            for i, page_index in enumerate(valid_indices):
                # Create a temporary PDF with only the target page
                single_page_pdf = pikepdf.Pdf.new()
                single_page_pdf.pages.append(pdf_doc.pages[page_index])

                # Save to a temporary file
                temp_dir = tempfile.mkdtemp()
                # Use a more descriptive name including the original index
                temp_pdf_path = os.path.join(temp_dir, f"offset_sample_{chr(ord('A') + i)}_{int(time.time())}" + ".pdf")
                single_page_pdf.save(temp_pdf_path)
                single_page_pdf.close()
                print(f"      - Saved temporary single-page PDF: {temp_pdf_path}", flush=True)
                temp_file_paths.append(temp_pdf_path)
                # Prepare for potential future batch upload if API supports it directly
                # For now, we upload sequentially but reference in one prompt
                print(f"    - Uploading temp page for index {page_index}...", flush=True)
                # Use a display name that's simple for the model
                display_name = f"page{chr(ord('A') + i)}.pdf" 
                uploaded_file = upload_pdf(temp_pdf_path, display_name=display_name)
                print(f"      - Uploaded as URI: {uploaded_file.name}", flush=True)
                uploaded_files_info[uploaded_file.name] = {
                    'physical_index': page_index,
                    'file_object': uploaded_file
                }
                upload_parts.append(uploaded_file) # Add file object for the prompt
            if len(upload_parts) < 2: # Need at least 2 samples for reliable offset
                print("    - Insufficient pages successfully uploaded for offset detection.", flush=True)
                return None

            # --- Single Prompt Construction (strict JSON schema) --- 
            uris = list(uploaded_files_info.keys())
            attached_list = "\n".join(f"- {u}" for u in uris)
            uris_nulls = ','.join(f'    "{u}": null' for u in uris)
            full_prompt = f"""
Objective:
  Determine the printed page numbers visible on each attached PDF page.

Attachments:
{attached_list}

Output Format:
  Return ONLY a valid JSON object mapping each URI to its printed page number or null, for example:
{{
{uris_nulls}
}}
No markdown or extra text.
"""

            print("\n--- Sending Batch Offset Detection Prompt ---")
            # print(full_prompt) # Uncomment for debugging prompt

            # --- Send Prompt with All Uploaded Files --- 
            # The API expects a list where the first item is the prompt string
            # and subsequent items are the File objects.
            request_content = [full_prompt] + upload_parts 
            # Make the API call
            try:
                response = model.generate_content(request_content, request_options={'timeout': 600})
                print("--- Received Batch Offset Detection Response ---")
                print("--- Raw Offset Detection Response ---", flush=True)
                print(response.text, flush=True)
                # --- Parse JSON Response for offset detection ---
                try:
                    offset_json = json.loads(clean_json_response(response.text))
                except Exception as e:
                    print(f"    - Error parsing offset JSON: {e}", flush=True)
                    return None
                # Map URIs to raw values
                parsed_numbers = {}
                for uri, value in offset_json.items():
                    if uri not in uploaded_files_info:
                        print(f"    - Warning: Unknown URI '{uri}' in JSON offset response", flush=True)
                        continue
                    parsed_numbers[uploaded_files_info[uri]['physical_index']] = value
                print(f"Parsed raw numbers: {parsed_numbers}", flush=True)

                # --- Calculate Offset ---
                offsets = []
                samples = []
                for phys_idx, raw in parsed_numbers.items():
                    # raw may be int or str; convert here
                    if raw is None:
                        samples.append({"physical_index": phys_idx, "printed_number": None, "calculated_offset": None})
                        continue
                    try:
                        printed_num = int(raw)
                    except (TypeError, ValueError):
                        print(f"    - Warning: Non-integer printed value '{raw}' for index {phys_idx}", flush=True)
                        samples.append({"physical_index": phys_idx, "printed_number": raw, "calculated_offset": None})
                        continue
                    offset = printed_num - phys_idx
                    offsets.append(offset)
                    samples.append({"physical_index": phys_idx, "printed_number": printed_num, "calculated_offset": offset})
            except google_exceptions.GoogleAPIError as e:
                print(f"Error during batch offset detection API call: {e}", file=sys.stderr)
                return None # Indicate error
            except Exception as e:
                print(f"Unexpected error during batch offset detection processing: {e}", file=sys.stderr)
                return None # Indicate error
                
    except Exception as e:
        print(f"Error during offset detection setup or file handling: {e}", file=sys.stderr)
    finally:
        # --- Cleanup --- 
        # Delete uploaded files from GenAI
        print("--- Cleaning up uploaded offset sample files ---")
        for upload_name, info in uploaded_files_info.items():
            try:
                print(f"    - Deleting {info['file_object'].name}...", flush=True)
                delete_uploaded(info['file_object'].name)
            except Exception as del_err:
                print(f"    - Warning: Failed to delete {info['file_object'].name}: {del_err}", flush=True)
        # Delete local temporary files/dirs
        print("--- Cleaning up local offset sample files ---")
        for temp_path in temp_file_paths:
            try:
                temp_dir = os.path.dirname(temp_path)
                print(f"    - Deleting {temp_dir}...", flush=True)
                shutil.rmtree(temp_dir)
            except Exception as clean_err:
                print(f"    - Warning: Failed to delete {temp_dir}: {clean_err}", flush=True)

    if not offsets:
        print("Could not determine printed page numbers for any sampled pages.", flush=True)
        return None

    # Find the most common offset
    most_common_offset = max(set(offsets), key=offsets.count)
    if offsets.count(most_common_offset) >= 2:  # At least 2 pages agree on offset
        return {"offset": most_common_offset, "samples": samples, "frequencies": {offset: offsets.count(offset) for offset in set(offsets)}}
    else:
        print("Insufficient agreement on page number offset.", flush=True)
        return None

def get_printed_page_number(pdf_doc: pikepdf.Pdf, page_index: int, model: GenAIModel, uploaded_full_pdf) -> int | None:
    """Gets the printed page number from a specific physical page index within an already uploaded PDF.
    
    NOTE: This function is no longer used by the primary validation flow as of the last refactor.
          It's kept for potential future use or alternative strategies.

    Args:
        pdf_doc: The opened pikepdf document (used for context/fallback if needed, but not for primary extraction).
        page_index: The 0-based physical index of the page to check.
        model: The configured GenerativeModel instance.
        uploaded_full_pdf: The File object returned by upload_pdf() for the complete PDF.

    Returns:
        The detected printed page number as an integer, or None if not found or error.
    """
    print(f"    - Validating physical page index: {page_index} using uploaded PDF {uploaded_full_pdf.name}", flush=True)
    try:
        # Prompt asking for the page number from the specific physical page index of the uploaded PDF
        # Use 1-based indexing for the prompt as it's more natural for page numbers
        physical_page_num_for_prompt = page_index + 1
        prompt = f"""
        Examine the uploaded PDF document ({uploaded_full_pdf.name}).
        What is the printed page number shown on physical page {physical_page_num_for_prompt} (using 1-based indexing for physical pages)?
        If no clear printed page number is visible on that specific page, respond with 'None'.
        Otherwise, respond with only the integer number.
        """
        
        # Send the prompt and the uploaded single-page PDF
        # Make the API call
        try:
            response = model.generate_content([prompt, uploaded_full_pdf], request_options={'timeout': 600})
            printed_num_str = response.text.strip()
            
            if printed_num_str.lower() == 'none':
                print(f"      - Validation model reported no clear page number.", flush=True)
                return None
                
            printed_num = int(printed_num_str)
            print(f"      - Validation model detected printed number: {printed_num}", flush=True)
            return printed_num

        except google_exceptions.GoogleAPIError as e:
            print(f"Error during get_printed_page_number API call: {e}", file=sys.stderr)
            return None # Indicate error
        except Exception as e:
            print(f"Unexpected error during get_printed_page_number: {e}", file=sys.stderr)
            return None # Indicate error
            
    except ValueError:
        print(f"      - Validation model response '{printed_num_str}' is not a valid integer or 'None'.", flush=True)
        return None
    except Exception as e:
        print(f"      - Error during page validation for index {page_index}: {e}", file=sys.stderr)
        return None
    # No finally block needed as we are not creating/deleting temporary files here

def _extract_and_save_single_page(pdf_doc: pikepdf.Pdf, page_index: int, base_filename: str) -> str | None:
    """Extracts a single page to a temporary file.
    Returns the path to the temporary file or None on error.
    """
    temp_pdf_path = None
    try:
        # Create a temporary PDF with only the target page
        single_page_pdf = pikepdf.Pdf.new()
        single_page_pdf.pages.append(pdf_doc.pages[page_index])

        # Save to a temporary file
        temp_dir = tempfile.mkdtemp()
        # Use a more descriptive name including the original index
        temp_pdf_path = os.path.join(temp_dir, f"{base_filename}_physical_page_{page_index}.pdf")
        single_page_pdf.save(temp_pdf_path)
        single_page_pdf.close()
        print(f"      - Saved temporary single-page PDF: {temp_pdf_path}", flush=True)
        return temp_pdf_path
    except Exception as e:
        print(f"      - Error saving temporary page {page_index}: {e}", flush=True)
        # Cleanup if path was created but save failed?
        if temp_pdf_path and os.path.exists(os.path.dirname(temp_pdf_path)):
             try: shutil.rmtree(os.path.dirname(temp_pdf_path)) 
             except: pass # Ignore cleanup errors
        return None

def extract_reference_sections(pdf_path: str, output_dir: str = "~/Nextcloud/DT/papers"):
    """Main function to find, adjust, validate, and extract reference sections."""
    print("--- Entered extract_reference_sections --- ", flush=True)
    # Ensure genai is configured before calling functions that use it
    if not api_key:
        print("Skipping extraction: GOOGLE_API_KEY not configured.", flush=True)
        return
        
    try:
        # --- Centralized Initialization --- 
        print("Initializing Generative Models and uploading PDF...")
        model = GenAIModel("gemini-3-flash-preview", temperature=0.0)
        uploaded_pdf = upload_pdf(pdf_path)
        print("Initialization complete.")
        # --- End Initialization --- 

        # 1 & 2. Find sections and detect offset concurrently
        with pikepdf.Pdf.open(pdf_path) as temp_doc:
            page_count_for_offset = len(temp_doc.pages)
            if page_count_for_offset == 0:
                print("Error: PDF has 0 pages.", flush=True)
                return

        print("Starting sequential tasks for section finding and offset detection...", flush=True)
        sections = find_reference_section_pages(pdf_path, model, uploaded_pdf)
        offset_details = detect_page_number_offset(pdf_path, model, page_count_for_offset)

        if not sections:
            print("No reference sections found.", flush=True)
            return
        if not offset_details or 'offset' not in offset_details:
            print("Could not determine page number offset. Cannot proceed with extraction.", flush=True)
            return
        
        offset = offset_details['offset']
        print(f"Detected page number offset: {offset}", flush=True)

        # --- Step 3: Validate and Adjust Sections (Includes Model Call 3) ---
        print("\n--- Validating Sections ---", flush=True)
        with pikepdf.open(pdf_path) as doc: # Open PDF locally for potential page extractions
            total_pages = len(doc.pages)
            print(f"Total physical pages in PDF: {total_pages}", flush=True)
            adjusted_sections = [] # Initialize here
            
            # Determine which sections to validate: first, two middle, and last if more than 4 sections
            if len(sections) <= 4:
                sections_to_validate = list(range(len(sections)))
            else:
                mid_point = len(sections) // 2
                sections_to_validate = [0, mid_point-1, mid_point, len(sections)-1]
                # Ensure no duplicates in case mid_point calculations overlap
                sections_to_validate = sorted(list(set(sections_to_validate)))
            
            print(f"Validating {len(sections_to_validate)} out of {len(sections)} sections: indices {sections_to_validate}", flush=True)
            
            validated_count = 0
            for index in sections_to_validate:
                i = index
                section = sections[index]
                section_validated = False
                temp_start_file_path = None
                temp_end_file_path = None
                uploaded_validation_files = {} # Store { 'type': {'file_object': obj, 'temp_path': path}}

                try:
                    reported_start = section.get('start_page')
                    reported_end = section.get('end_page')
                    
                    if reported_start is None or reported_end is None:
                        print(f"  - Section {i+1}: Skipping due to missing start/end page in initial report.", flush=True)
                        continue
                        
                    print(f"\n  - Section {i+1}: Processing reported printed pages: Start={reported_start}, End={reported_end}", flush=True)

                    # Calculate Potential Physical Indices using offset
                    physical_start_offset = reported_start - offset
                    physical_end_offset = reported_end - offset
                    print(f"    - Calculated potential physical indices using offset {offset}: Start={physical_start_offset}, End={physical_end_offset}", flush=True)

                    # Determine which physical indices to use for validation
                    physical_start_to_validate = -1
                    physical_end_to_validate = -1
                    validation_method = None # 'offset' or 'direct'

                    # Check Offset Index Validity (Local)
                    if (0 <= physical_start_offset < total_pages and 
                        0 <= physical_end_offset < total_pages and 
                        physical_start_offset <= physical_end_offset):
                        print(f"    - Offset-calculated physical indices are initially VALID. Will attempt validation using these.", flush=True)
                        physical_start_to_validate = physical_start_offset
                        physical_end_to_validate = physical_end_offset
                        validation_method = 'offset'
                    else:
                        print(f"    - Offset-calculated physical indices are INVALID. Attempting fallback using direct mapping.", flush=True)
                        # --- Fallback Logic --- 
                        # Calculate direct physical indices (reported_page - 1)
                        physical_start_direct = reported_start - 1
                        physical_end_direct = reported_end - 1
                        print(f"    - Calculated direct-mapped physical indices: Start={physical_start_direct}, End={physical_end_direct}", flush=True)
                        
                        # Check Direct Index Validity (Local)
                        if (0 <= physical_start_direct < total_pages and 
                            0 <= physical_end_direct < total_pages and 
                            physical_start_direct <= physical_end_direct):
                            print(f"    - Direct-mapped physical indices are initially VALID. Will attempt validation using these.", flush=True)
                            physical_start_to_validate = physical_start_direct
                            physical_end_to_validate = physical_end_direct
                            validation_method = 'direct'
                        else:
                             print(f"    - Direct-mapped physical indices are also INVALID. Skipping section {i+1}.", flush=True)
                             continue # Skip to the next section if both methods fail

                    # --- Model Validation (Model Call 3) --- 
                    # Uses physical_start_to_validate and physical_end_to_validate
                    
                    # Extract start and end pages locally using determined valid indices
                    base_filename_start = f"validate_start_{i+1}_{int(time.time())}"
                    base_filename_end = f"validate_end_{i+1}_{int(time.time())}"
                    temp_start_file_path = _extract_and_save_single_page(doc, physical_start_to_validate, base_filename_start)
                    temp_end_file_path = _extract_and_save_single_page(doc, physical_end_to_validate, base_filename_end)

                    if not temp_start_file_path or not temp_end_file_path:
                        print(f"    - Failed to extract one or both pages (physical {physical_start_to_validate}, {physical_end_to_validate}) for validation. Skipping section.", flush=True)
                        continue

                    # Upload the temporary pages
                    validation_upload_parts = []
                    try:
                        print(f"    - Uploading validation page: Start (physical {physical_start_to_validate})...", flush=True)
                        uploaded_start = upload_pdf(temp_start_file_path, display_name=f"start_page_{i+1}.pdf")
                        uploaded_validation_files['start'] = {'file_object': uploaded_start, 'temp_path': temp_start_file_path}
                        validation_upload_parts.append(uploaded_start)
                        print(f"      - Uploaded as URI: {uploaded_start.name}", flush=True)

                        print(f"    - Uploading validation page: End (physical {physical_end_to_validate})...", flush=True)
                        uploaded_end = upload_pdf(temp_end_file_path, display_name=f"end_page_{i+1}.pdf")
                        uploaded_validation_files['end'] = {'file_object': uploaded_end, 'temp_path': temp_end_file_path}
                        validation_upload_parts.append(uploaded_end)
                        print(f"      - Uploaded as URI: {uploaded_end.name}", flush=True)

                    except Exception as upload_err:
                        print(f"    - Error uploading validation pages: {upload_err}. Skipping section.", flush=True)
                        continue # Cannot proceed without uploads

                    # Construct Validation Prompt (Always use REPORTED numbers for validation check)
                    validation_prompt = f"""
Objective:
  Validate that two attached PDF pages correspond to the START and END of a bibliography/reference section, and verify their printed page numbers.

Attachments:
  - START page URI: {uploaded_start.name}
  - END page URI:   {uploaded_end.name}

Context:
  - Reported START printed number: {reported_start}
  - Reported END printed number:   {reported_end}

Tasks:
  1. For URI '{uploaded_start.name}':
     a. Extract the printed page number on the page. Provide the integer or 'None'.
     b. Confirm if this page marks the START of a bibliography section. Respond 'Yes' or 'No'.
  2. For URI '{uploaded_end.name}':
     a. Extract the printed page number on the page. Provide the integer or 'None'.
     b. Confirm if this page marks the END of a bibliography section. Respond 'Yes' or 'No'.

Output Format:
  Return a JSON object with two keys (the URIs), for example:
  {{
    "{uploaded_start.name}": {{"printed_number": 53, "is_start": true}},
    "{uploaded_end.name}":   {{"printed_number": 54, "is_end":   true}}
  }}
  ONLY valid JSON. No markdown or extra text.
"""

                    print(f"\n    --- Sending Validation Prompt for Section {i+1} (Checking reported pages {reported_start}-{reported_end}) ---")
                    # print(validation_prompt) # Debugging

                    # Send Validation Request
                    request_content = [validation_prompt] + validation_upload_parts
                    # Make the API call
                    try:
                        response = model.generate_content(request_content, request_options={'timeout': 600})
                        print(f"    --- Received Validation Response for Section {i+1} ---")
                        # print(f"Raw validation response: {response.text}") # DEBUG

                        # Parse JSON Validation Response
                        try:
                            validation_json = json.loads(clean_json_response(response.text))
                        except Exception as e:
                            print(f"      - Error parsing validation JSON: {e}", flush=True)
                            return None
                        # Map JSON to internal validation dict
                        start_info = validation_json.get(uploaded_start.name, {})
                        end_info   = validation_json.get(uploaded_end.name, {})
                        parsed_validation = {
                            'start': {
                                'number': start_info.get('printed_number'),
                                'is_marker': bool(start_info.get('is_start'))
                            },
                            'end': {
                                'number': end_info.get('printed_number'),
                                'is_marker': bool(end_info.get('is_end'))
                            }
                        }
                        print(f"    - Parsed Validation (JSON): {parsed_validation}", flush=True)

                        # Final Check (Local) - Compare parsed numbers against originally REPORTED numbers
                        tolerance = 2  # Allow small discrepancy in page numbers to handle off-by-one errors
                        start_num_match = (parsed_validation['start']['number'] is None or 
                                         abs(parsed_validation['start']['number'] - reported_start) <= tolerance)
                        end_num_match = (parsed_validation['end']['number'] is None or 
                                       abs(parsed_validation['end']['number'] - reported_end) <= tolerance)
                        
                        # Marker checks are lenient: if number is None, marker can be False; also, one of start or end can have False marker
                        start_marker_match = (parsed_validation['start']['number'] is not None or parsed_validation['start']['is_marker'])
                        end_marker_match = (parsed_validation['end']['number'] is not None or parsed_validation['end']['is_marker'])
                        marker_condition = (start_marker_match or end_marker_match)  # At least one marker should be True if numbers are present

                        print(f"    - Validation Checks: StartNumMatch={start_num_match} (Target: {reported_start}), StartMarkerOK={start_marker_match}, EndNumMatch={end_num_match} (Target: {reported_end}), EndMarkerOK={end_marker_match}", flush=True)

                        # Pass validation if number matches (or is None) and marker condition is satisfied
                        if (start_num_match and end_num_match and marker_condition):
                            print(f"    - Section {i+1} PASSED ALL VALIDATIONS using physical pages {physical_start_to_validate}-{physical_end_to_validate} (Method: {validation_method}).", flush=True)
                            adjusted_sections.append({
                                "start_page": physical_start_to_validate, 
                                "end_page": physical_end_to_validate
                            })
                            section_validated = True
                            validated_count += 1
                        else:
                            print(f"    - Section {i+1} FAILED model validation checks using physical pages {physical_start_to_validate}-{physical_end_to_validate} (Method: {validation_method}).", flush=True)
                            section_validated = False
                        # --- End Model Validation ---

                    except google_exceptions.GoogleAPIError as e:
                        print(f"Error during validation API call for Section {i+1}: {e}", file=sys.stderr)
                        return None # Indicate validation failure due to API error
                    except Exception as e:
                        print(f"Unexpected error during validation processing for Section {i+1}: {e}", file=sys.stderr)
                        return None # Indicate validation failure due to other error

                except (ValueError, TypeError) as e:
                    print(f"  - Section {i+1}: Error processing section {section}: {e}", flush=True)
                    section_validated = False
                except Exception as gen_err:
                     print(f"  - Section {i+1}: Unexpected error during processing/validation: {gen_err}", flush=True)
                     section_validated = False # Ensure it's false on error
                finally:
                    # Cleanup validation files for this section
                    print(f"    - Cleaning up validation files for Section {i+1}...", flush=True)
                    for key, info in uploaded_validation_files.items():
                        # Delete uploaded file
                        try:
                            print(f"      - Deleting uploaded file: {info['file_object'].name}...", flush=True)
                            delete_uploaded(info['file_object'].name)
                        except Exception as del_err:
                            print(f"      - Warning: Failed to delete uploaded {key} file {info['file_object'].name}: {del_err}", flush=True)
                        # Delete local temp file
                        if info['temp_path'] and os.path.exists(info['temp_path']):
                            try:
                                temp_dir = os.path.dirname(info['temp_path'])
                                print(f"      - Deleting local temp dir: {temp_dir}...", flush=True)
                                shutil.rmtree(temp_dir)
                            except Exception as clean_err:
                                print(f"      - Warning: Failed to delete local temp dir {temp_dir}: {clean_err}", flush=True)
                    # Clear for next iteration
                    uploaded_validation_files = {}
                    temp_start_file_path = None
                    temp_end_file_path = None
            
            # Check if at least 3 out of 4 validated sections passed (or all if fewer than 4)
            validation_threshold = min(3, len(sections_to_validate)) if len(sections_to_validate) == 4 else len(sections_to_validate)
            if validated_count >= validation_threshold:
                print(f"Validation successful: {validated_count}/{len(sections_to_validate)} sections validated. Extracting all sections.", flush=True)
                adjusted_sections = sections
            else:
                print(f"Validation failed: Only {validated_count}/{len(sections_to_validate)} sections validated. Skipping extraction.", flush=True)
                return

        # --- Step 4: Extract Fully Validated Sections ---
        if not adjusted_sections:
            print("\n--- No sections passed all validation steps. Nothing to extract. ---", flush=True)
            # No return here, allow finally block to run for main upload cleanup
        else:
            print(f"\n--- Extracting {len(adjusted_sections)} Validated Section(s) ---", flush=True)
            print("Validated sections (physical 0-based indices):", json.dumps(adjusted_sections, indent=2), flush=True)
            
            output_dir_abs = os.path.expanduser(output_dir) # Ensure path is expanded
            base_name = os.path.splitext(os.path.basename(pdf_path))[0]
            # --- ADDED: Create specific subdirectory for this PDF --- 
            specific_output_dir = os.path.join(output_dir_abs, base_name)
            os.makedirs(specific_output_dir, exist_ok=True) # Ensure specific output dir exists
            print(f"Output directory for intermediate pages: {specific_output_dir}", flush=True)
            # --- END ADDED ---

            # --- ADDED: List to store names of generated page PDFs --- 
            generated_page_pdf_filenames = []
            # --- END ADDED ---

            # Re-open the PDF for the final extraction pass
            with pikepdf.open(pdf_path) as final_doc:
                for i, section in enumerate(adjusted_sections, 1):
                    start_page_idx = section["start_page"]  # Already 0-based physical index
                    end_page_idx = section["end_page"]      # Already 0-based physical index
                    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
                    
                    print(f"  - Extracting Section {i} (Physical Pages {start_page_idx} to {end_page_idx})...", flush=True)
                    
                    # Extracting each page individually as per previous logic
                    for page_num_idx in range(start_page_idx, end_page_idx + 1): # +1 for inclusive range
                        new_pdf_page = pikepdf.Pdf.new()
                        try:
                             # Check if page index is still valid (belt-and-suspenders)
                            if 0 <= page_num_idx < len(final_doc.pages):
                                new_pdf_page.pages.append(final_doc.pages[page_num_idx])
                                
                                # Use physical index + 1 for human-readable filename numbering
                                output_filename = f"{base_name}_section{i}_page{page_num_idx + 1}.pdf"
                                # --- MODIFIED: Save inside the specific subdirectory ---
                                output_path = os.path.join(specific_output_dir, output_filename)
                                # --- END MODIFIED ---
                                
                                new_pdf_page.save(output_path)
                                print(f"    - Saved: {output_path}", flush=True)
                                # --- ADDED: Record the filename --- 
                                generated_page_pdf_filenames.append(output_filename)
                                # --- END ADDED ---
                            else:
                                 print(f"    - Warning: Physical page index {page_num_idx} out of bounds during final extraction for section {i}. Skipping page.", flush=True)

                        except Exception as extract_err:
                            print(f"    - Error extracting physical page {page_num_idx} of section {i}: {extract_err}", flush=True)
                        finally:
                            new_pdf_page.close() # Close the single-page PDF object

            # --- ADDED: Create the manifest JSON file if pages were generated --- 
            if generated_page_pdf_filenames:
                manifest_json_path = os.path.join(specific_output_dir, f"{base_name}_bibliography_pages.json")
                manifest_data = {"pages": generated_page_pdf_filenames}
                try:
                    with open(manifest_json_path, 'w') as f_json:
                        json.dump(manifest_data, f_json, indent=2)
                    print(f"--- Created manifest JSON: {manifest_json_path} ---", flush=True)
                except Exception as json_err:
                    print(f"--- Error creating manifest JSON {manifest_json_path}: {json_err} ---", flush=True)
            else:
                print("--- No page PDFs were generated, skipping manifest JSON creation. ---", flush=True)
            # --- END ADDED ---
        
        print("--- Extraction Complete (if any sections were valid) ---", flush=True)
        # --- End Step 4 ---

    except Exception as e:
        print(f"An unexpected error occurred in the main processing block: {e}", flush=True)
        # Consider adding more specific error handling or logging stack trace
    finally:
        # --- Cleanup Main Uploaded File --- 
        # This should always run, regardless of success/failure earlier
        if 'uploaded_pdf' in locals() and uploaded_pdf is not None:
            try:
                print(f"\n--- Final Cleanup: Deleting main uploaded file '{uploaded_pdf.name}' ---", flush=True)
                delete_uploaded(uploaded_pdf.name)
                print("Main uploaded file deleted successfully.", flush=True)
            except Exception as del_err:
                print(f"Warning: Failed to delete main uploaded file '{uploaded_pdf.name}': {del_err}", flush=True)
        else:
             print("\n--- Final Cleanup: No main uploaded file object found to delete. ---", flush=True)
        print("--- extract_reference_sections function finished ---", flush=True)
        return generated_page_pdf_filenames if 'generated_page_pdf_filenames' in locals() else []

def process_pdf(pdf_path, output_dir):
    return extract_reference_sections(pdf_path, output_dir)


if __name__ == "__main__":
    print("--- get_bib_pages.py script started ---", flush=True)
    # --- Argument Parsing Setup ---
    print("--- Setting up ArgParser --- ", flush=True)
    parser = argparse.ArgumentParser(description='Extract bibliography pages from PDFs.')
    parser.add_argument('input', help='Path to a PDF file or directory containing PDFs')
    parser.add_argument('--output-dir', default='bib_output', help='Output directory for extracted pages and JSON files')
    args = parser.parse_args()
    print("--- Parsing Arguments --- ", flush=True)
    print("--- Arguments Parsed --- ", flush=True)

    # --- Setup Output Directory ---
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory set to: {output_dir}", flush=True)

    # --- Determine Input Type (File or Directory) ---
    input_path = os.path.abspath(args.input)
    if os.path.isdir(input_path):
        pdf_files = glob.glob(os.path.join(input_path, '*.pdf'))
        print(f"Found {len(pdf_files)} PDF files in directory: {input_path}", flush=True)
    else:
        pdf_files = [input_path] if input_path.endswith('.pdf') else []
        if not pdf_files:
            print(f"Error: {input_path} is not a PDF file.", flush=True)
            sys.exit(1)
        print(f"Processing single PDF file: {input_path}", flush=True)

    # --- Process PDFs Sequentially ---
    print(f"Starting sequential processing of {len(pdf_files)} PDF(s)...", flush=True)
    start_time = time.time()

    total_bib_pages = 0
    processed_count = 0

    done_bibs_dir = os.path.join(output_dir, 'done_bibs')
    os.makedirs(done_bibs_dir, exist_ok=True)

    for pdf_path in pdf_files:
        bib_pages = process_pdf(pdf_path, output_dir)
        total_bib_pages += len(bib_pages)
        processed_count += 1
        print(f"Completed {processed_count}/{len(pdf_files)} PDFs: {os.path.basename(pdf_path)} - {len(bib_pages)} bibliography pages found.", flush=True)

        # Move PDF to done_bibs if bibliography pages were extracted
        if bib_pages:
            dest_path = os.path.join(done_bibs_dir, os.path.basename(pdf_path))
            try:
                shutil.move(pdf_path, dest_path)
                print(f"Moved {os.path.basename(pdf_path)} to {done_bibs_dir}", flush=True)
            except Exception as e:
                print(f"Error moving {os.path.basename(pdf_path)} to {done_bibs_dir}: {e}", flush=True)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Processing complete. Total execution time: {execution_time:.2f} seconds", flush=True)
    print(f"Processed {processed_count} PDFs, found {total_bib_pages} bibliography pages.", flush=True)
