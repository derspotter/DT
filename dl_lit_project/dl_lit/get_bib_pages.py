from google import genai
from google.genai import types
import pikepdf
import json
import os
import sys
import argparse
import tempfile
import shutil
import concurrent.futures
from dotenv import load_dotenv
import glob
from .utils import get_global_rate_limiter, ServiceRateLimiter


print("--- Script Top --- ", flush=True)

print("--- Imports Done --- ", flush=True)

# --- Added: Configure Google API Key ---
print("--- Configuring GenAI --- ", flush=True)
load_dotenv()
api_key = os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_API_KEY')
if not api_key:
    print("Error: GEMINI_API_KEY (or GOOGLE_API_KEY) environment variable not set.", flush=True)
    api_client = None
else:
    try:
        api_client = genai.Client(api_key=api_key)
        print("Successfully configured Google GenAI client with API key.")
    except Exception as e:
        api_client = None
        print(f"Error configuring Google GenAI client: {e}")
print("--- GenAI Configured (or skipped) --- ", flush=True)
# --- End Added Section ---

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

def find_reference_section_pages(
    pdf_path: str,
    client,
    uploaded_pdf,
    total_physical_pages: int,
    rate_limiter: ServiceRateLimiter,
    timeout_seconds: int = 180,
):
    """Use GenAI to find reference section pages in the PDF."""
    print(f"Attempting to find reference sections in PDF: {pdf_path}", flush=True)
    print("Using pre-uploaded PDF for section finding.", flush=True)
    
    prompt = f"""
Identify ALL bibliography/reference sections in this PDF.

Use 1-based PHYSICAL page indices (first PDF page = 1) and ignore any printed page numbers.
Return only a JSON array of objects with start_page and end_page.

Rules:
- start_page = page where the references section begins (title or first entry). Be inclusive.
- end_page = page where the last reference entry ends (do not include unrelated pages after).
- Include every distinct bibliography/reference section, especially after chapters.

Document has {total_physical_pages} physical pages.

Format:
[
  {{"start_page": 12, "end_page": 15}}
]
"""

    try:
        rate_limiter.wait_if_needed('gemini')
        rate_limiter.wait_if_needed('gemini_daily')
        print("Sending request to GenAI...", flush=True)
        def _call_model():
            return client.models.generate_content(
                model="gemini-3-flash-preview",
                contents=[prompt, uploaded_pdf],
                config=types.GenerateContentConfig(
                    temperature=1.0,
                    response_mime_type="application/json",
                ),
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_call_model)
            try:
                response = future.result(timeout=timeout_seconds)
            except concurrent.futures.TimeoutError as e:
                future.cancel()
                raise TimeoutError(f"Gemini request timed out after {timeout_seconds}s") from e
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

def extract_reference_sections(pdf_path: str, output_dir: str = "~/Nextcloud/DT/papers"):
    """Main function to find, adjust, validate, and extract reference sections, with chunking for large PDFs."""
    print("--- Entered extract_reference_sections --- ", flush=True)
    if not api_client:
        print("Skipping extraction: GEMINI_API_KEY (or GOOGLE_API_KEY) not configured.", flush=True)
        return
    password_error = getattr(pikepdf, "PasswordError", Exception)
    if not isinstance(password_error, type) or not issubclass(password_error, BaseException):
        password_error = Exception
    rate_limiter = get_global_rate_limiter()
    files_to_clean_up = []
    temp_chunk_dir = None
    all_sections = []

    MAX_PAGES_FOR_API = 50
    CHUNK_SIZE = 50

    try:
        with pikepdf.Pdf.open(pdf_path) as pdf_doc:
            total_physical_pages = len(pdf_doc.pages)
            if total_physical_pages == 0:
                print("Error: PDF has 0 pages.", flush=True)
                return
            print(f"PDF has {total_physical_pages} pages.")

            if total_physical_pages > MAX_PAGES_FOR_API:
                # --- Chunking Logic for Large PDFs ---
                print(f"PDF exceeds {MAX_PAGES_FOR_API} pages. Splitting into chunks of {CHUNK_SIZE} pages.")
                temp_chunk_dir = tempfile.mkdtemp()

                for i in range(0, total_physical_pages, CHUNK_SIZE):
                    start_page = i
                    end_page = min(i + CHUNK_SIZE, total_physical_pages)
                    print(f"\nProcessing chunk: pages {start_page + 1}-{end_page}")

                    # Create a new PDF for the chunk
                    chunk_pdf = pikepdf.Pdf.new()
                    for page_num in range(start_page, end_page):
                        chunk_pdf.pages.append(pdf_doc.pages[page_num])
                    
                    base_filename = os.path.splitext(os.path.basename(pdf_path))[0]
                    chunk_display_name = f"{base_filename}_chunk_pages_{start_page+1}-{end_page}.pdf"
                    chunk_path = os.path.join(temp_chunk_dir, chunk_display_name)
                    chunk_pdf.save(chunk_path)

                    # Upload and process the chunk
                    rate_limiter.wait_if_needed('gemini')
                    rate_limiter.wait_if_needed('gemini_daily')
                    print(f"Uploading chunk: {chunk_display_name}")
                    with open(chunk_path, "rb") as file_handle:
                        uploaded_chunk = api_client.files.upload(
                            file=file_handle,
                            config={
                                "mime_type": "application/pdf",
                                "display_name": chunk_display_name,
                            },
                        )
                    files_to_clean_up.append(uploaded_chunk)
                    print(f"Uploaded as {uploaded_chunk.name}. Finding reference sections in chunk...")

                    chunk_page_count = end_page - start_page
                    chunk_sections = find_reference_section_pages(
                        chunk_path,
                        api_client,
                        uploaded_chunk,
                        total_physical_pages=chunk_page_count,
                        rate_limiter=rate_limiter,
                    )
                    if chunk_sections:
                        # Adjust page numbers to be absolute to the original PDF
                        for sec in chunk_sections:
                            sec['start_page'] += start_page
                            sec['end_page'] += start_page
                        all_sections.extend(chunk_sections)
                        print(f"Found {len(chunk_sections)} section(s) in chunk. Current total: {len(all_sections)}.")
                    else:
                        print("No reference sections found in this chunk.")

            else:
                # --- Standard Logic for Smaller PDFs ---
                print("PDF is within size limits, processing as a single file.")
                rate_limiter.wait_if_needed('gemini')
                rate_limiter.wait_if_needed('gemini_daily')
                print("Uploading full PDF...")
                with open(pdf_path, "rb") as file_handle:
                    uploaded_pdf = api_client.files.upload(
                        file=file_handle,
                        config={
                            "mime_type": "application/pdf",
                            "display_name": os.path.basename(pdf_path),
                        },
                    )
                files_to_clean_up.append(uploaded_pdf)
                print(f"Uploaded as {uploaded_pdf.name}. Finding reference sections...")
                all_sections = find_reference_section_pages(
                    pdf_path,
                    api_client,
                    uploaded_pdf,
                    total_physical_pages=total_physical_pages,
                    rate_limiter=rate_limiter,
                )

        # --- Aggregated Section Processing ---
        if not all_sections:
            print("No reference sections found by the AI in the entire document.", flush=True)
            return

        print(f"\n--- AI Identified Sections (1-based physical indices) ({len(all_sections)}) ---")
        for i, sec in enumerate(all_sections):
            print(f"  Section {i+1}: 1-based Physical Pages {sec['start_page']} - {sec['end_page']}")

        # Convert AI's 1-based physical indices to 0-based for pikepdf
        adjusted_sections = []
        for sec in all_sections:
            try:
                phys_start_1based = int(sec['start_page'])
                phys_end_1based = int(sec['end_page'])
                if phys_start_1based <= 0 or phys_end_1based <= 0 or phys_start_1based > phys_end_1based:
                    print(f"Warning: Invalid page range from AI: {sec}. Skipping.")
                    continue
                adjusted_sections.append({
                    'physical_start_index': phys_start_1based - 1,
                    'physical_end_index': phys_end_1based - 1
                })
            except (ValueError, TypeError) as e:
                print(f"Warning: Invalid page number format in section {sec} (Error: {e}). Skipping.")

        # --- Extraction Logic (Modified to always output individual pages) ---
        output_base_filename = os.path.splitext(os.path.basename(pdf_path))[0]
        final_extracted_files = []
        print(f"\n--- Validating and Extracting Pages from original PDF ---")
        with pikepdf.Pdf.open(pdf_path) as pdf_doc:
            for i, section_to_extract in enumerate(adjusted_sections):
                start_idx = section_to_extract['physical_start_index']
                end_idx = section_to_extract['physical_end_index']
                phys_start_1_based_for_naming = start_idx + 1
                phys_end_1_based_for_naming = end_idx + 1

                print(f"\n  Processing Section {i+1} (Physical 1-based: {phys_start_1_based_for_naming}-{phys_end_1_based_for_naming}, Target 0-based: {start_idx}-{end_idx}):")
                if not (0 <= start_idx < total_physical_pages and 0 <= end_idx < total_physical_pages):
                    print(f"    - Error: Invalid page range. Skipping section.")
                    continue
                
                # Extract each page individually instead of as a continuous section
                total_pages_in_section = end_idx - start_idx + 1
                for page_offset in range(total_pages_in_section):
                    current_page_idx = start_idx + page_offset
                    current_page_1_based = current_page_idx + 1
                    
                    try:
                        new_pdf = pikepdf.Pdf.new()
                        new_pdf.pages.append(pdf_doc.pages[current_page_idx])
                        expanded_output_dir = os.path.expanduser(output_dir)
                        os.makedirs(expanded_output_dir, exist_ok=True)
                        output_filename = f"{output_base_filename}_refs_physical_p{current_page_1_based}.pdf"
                        output_path = os.path.join(expanded_output_dir, output_filename)
                        new_pdf.save(output_path)
                        final_extracted_files.append(output_path)
                        print(f"    - Successfully extracted page {current_page_1_based} to {output_path}")
                    except Exception as e:
                        print(f"    - Error extracting page {current_page_1_based}: {e}. Skipping page.")

        print("\n--- Final Extracted Files ---")
        if final_extracted_files:
            for f_path in final_extracted_files:
                print(f"  - {f_path}")
        else:
            print("  No files were extracted.")

    except (FileNotFoundError, password_error, Exception) as e:
        print(f"An unexpected error occurred in extract_reference_sections: {e}", flush=True)
        import traceback
        traceback.print_exc()
    finally:
        # --- Centralized Cleanup --- 
        if files_to_clean_up:
            print(f"\nCleaning up {len(files_to_clean_up)} uploaded file(s)...")
            for f in files_to_clean_up:
                try:
                    api_client.files.delete(name=f.name)
                    print(f"  - Successfully deleted {f.name}.")
                except Exception as e:
                    print(f"  - Failed to delete {f.name}: {e}")
        if temp_chunk_dir and os.path.exists(temp_chunk_dir):
            print(f"Cleaning up temporary chunk directory: {temp_chunk_dir}")
            shutil.rmtree(temp_chunk_dir)
        print("--- Exiting extract_reference_sections --- ", flush=True)

def process_pdf(pdf_path, output_dir):
    extract_reference_sections(pdf_path, output_dir)

def main(input_path, output_dir):
    print(f"Input path: {input_path}", flush=True)
    print(f"Output directory: {output_dir}", flush=True)
    args = parser.parse_args()

    print(f"Input path: {args.input_path}", flush=True)
    print(f"Output directory: {args.output_dir}", flush=True)

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    print(f"Ensured output directory exists: {args.output_dir}", flush=True)

    # --- Main Processing Logic ---
    if os.path.isfile(args.input_path) and args.input_path.lower().endswith('.pdf'):
        print(f"Processing single PDF file: {args.input_path}", flush=True)
        process_pdf(args.input_path, args.output_dir)
    elif os.path.isdir(args.input_path):
        print(f"Processing directory: {args.input_path}", flush=True)
        pdf_files = glob.glob(os.path.join(args.input_path, '*.pdf')) + \
                    glob.glob(os.path.join(args.input_path, '**/*.pdf'), recursive=True) # Add recursive search
        pdf_files = sorted(list(set(pdf_files))) # Remove duplicates and sort
        
        if not pdf_files:
            print(f"No PDF files found in directory: {args.input_path}", flush=True)
        else:
            print(f"Found {len(pdf_files)} PDF files to process:", flush=True)
            for i, pdf_file in enumerate(pdf_files):
                print(f"  {i+1}/{len(pdf_files)}: {pdf_file}", flush=True)
            
            # --- Concurrent Processing --- # 
            # Set max_workers to a reasonable number, e.g., number of CPU cores or a fixed number like 4-8
            # Be mindful of API rate limits if processing many files very quickly.
            # For now, sequential processing to avoid overwhelming the API or local resources during development.
            # max_workers = 1 # os.cpu_count() or 4 
            # print(f"Using up to {max_workers} workers for concurrent processing.")
            # with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            #     futures = [executor.submit(process_pdf, pdf_file, args.output_dir) for pdf_file in pdf_files]
            #     for i, future in enumerate(concurrent.futures.as_completed(futures)):
            #         try:
            #             future.result() # Wait for completion, can retrieve return value if any
            #             print(f"Completed processing for: {pdf_files[i]}", flush=True) # This index might not be correct if order changes
            #         except Exception as e:
            #             print(f"Error processing {pdf_files[i]}: {e}", flush=True)
            # --- Sequential Processing (Current) --- #
            for i, pdf_file in enumerate(pdf_files):
                print(f"\n--- Starting processing for PDF {i+1}/{len(pdf_files)}: {pdf_file} ---", flush=True)
                try:
                    pdf_filename_without_ext = os.path.splitext(os.path.basename(pdf_file))[0]
                    individual_output_dir = os.path.join(args.output_dir, pdf_filename_without_ext)
                    # The extract_reference_sections function (called by process_pdf) 
                    # already creates the output_dir if it doesn't exist.
                    process_pdf(pdf_file, individual_output_dir)
                    print(f"--- Finished processing for PDF {i+1}/{len(pdf_files)}: {pdf_file} ---", flush=True)
                except Exception as e:
                    print(f"--- Error processing PDF {i+1}/{len(pdf_files)}: {pdf_file} --- Error: {e}", flush=True)
                    # Optionally, log detailed traceback for errors
                    # import traceback
                    # traceback.print_exc()
    else:
        print(f"Error: Input path '{args.input_path}' is not a valid PDF file or directory.", flush=True)
        sys.exit(1)

    print("--- get_bib_pages.py script finished ---", flush=True)
