import google.generativeai as genai
from google.api_core import exceptions as google_exceptions
import pikepdf
import json
import os
from collections import defaultdict
import sys
import argparse
import tempfile
import shutil
import concurrent.futures
from dotenv import load_dotenv
import glob
from .utils import ServiceRateLimiter


print("--- Script Top --- ", flush=True)

print("--- Imports Done --- ", flush=True)

# --- Added: Configure Google API Key ---
print("--- Configuring GenAI --- ", flush=True)
load_dotenv()
api_key = os.getenv('GOOGLE_API_KEY')
if not api_key:
    print("Error: GOOGLE_API_KEY environment variable not set.", flush=True)
    # Consider exiting or raising an error if the key is essential
    # sys.exit(1) # Uncomment to exit if key is missing
else:
    try:
        genai.configure(api_key=api_key)
        print("Successfully configured Google Generative AI with API key.")
    except Exception as e:
        print(f"Error configuring Google Generative AI: {e}")
        # Consider exiting or raising an error
        # sys.exit(1)
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

def find_reference_section_pages(pdf_path: str, model, uploaded_pdf, total_physical_pages: int, rate_limiter: ServiceRateLimiter):
    """Use GenAI to find reference section pages in the PDF."""
    print(f"Attempting to find reference sections in PDF: {pdf_path}", flush=True)
    print("Using pre-uploaded PDF for section finding.", flush=True)
    
    # Prompt asking for 1-based physical page indices
    prompt = f"""
Your task is to identify ALL bibliography or reference sections in this PDF document.

CONTEXT:
- The document has a total of {total_physical_pages} physical pages, numbered 1 to {total_physical_pages}.

INSTRUCTIONS:
1. Find sections containing lists of references, citations, or bibliographic entries. These sections might be titled 'References', 'Bibliography', 'Works Cited', or similar, or they might be untitled but recognizable by their format (lists of authors, years, titles, DOIs, etc.).
2. **IMPORTANT: You MUST use 1-based physical page indices. The first page of the document is page 1, the second is page 2, and so on, regardless of any printed page numbers.**
3. **'start_page' MUST be the 1-based physical index of the page containing the section's title (e.g., "References", "Bibliography"). If the title is on a preceding page, you must use the page with the title as the start_page.** This is critical to avoid cutting off the beginning of the section.
4. 'end_page' is the 1-based physical index of the page where the last entry of that specific reference section ends. Do not include subsequent pages that are empty or contain unrelated content.
5. Identify ALL such reference sections. Pay close attention to the end of chapters or major parts, as these often have their own reference lists.
6. Return ONLY a JSON array as shown below. Do not include any explanatory text or markdown formatting outside the JSON structure.

FORMAT:
[
    {{
        "start_page": 250,  // 1-based physical page index where the section starts
        "end_page": 255     // 1-based physical page index where the section's last entry ends
    }}
]

RULES:
- Always include 'start_page' and 'end_page' for each identified section.
- **When in doubt about the start page, be inclusive. It is better to include the page before the first reference item than to miss the section title.**
- Use INTEGER numbers only for page indices.
- Ensure 'start_page' correctly identifies the beginning of the section (title or first entry) by its 1-based physical index.
- Ensure 'end_page' correctly identifies the end of the last entry of that section by its 1-based physical index.
- Include EVERY distinct bibliography/reference section found throughout the document.
- The output must be ONLY the valid JSON array.
"""

    try:
        rate_limiter.wait_if_needed('gemini')
        print("Sending request to GenAI...", flush=True)
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

def extract_reference_sections(pdf_path: str, output_dir: str = "~/Nextcloud/DT/papers"):
    """Main function to find, adjust, validate, and extract reference sections, with chunking for large PDFs."""
    print("--- Entered extract_reference_sections --- ", flush=True)
    if not api_key:
        print("Skipping extraction: GOOGLE_API_KEY not configured.", flush=True)
        return

    model = genai.GenerativeModel("gemini-2.5-flash")
    service_config = {
        'gemini': {'limit': 10, 'window': 60}  # 10 requests per 60 seconds
    }
    rate_limiter = ServiceRateLimiter(service_config)
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
                    print(f"Uploading chunk: {chunk_display_name}")
                    uploaded_chunk = genai.upload_file(chunk_path, display_name=chunk_display_name)
                    files_to_clean_up.append(uploaded_chunk)
                    print(f"Uploaded as {uploaded_chunk.name}. Finding reference sections in chunk...")

                    chunk_page_count = end_page - start_page
                    chunk_sections = find_reference_section_pages(chunk_path, model, uploaded_chunk, total_physical_pages=chunk_page_count, rate_limiter=rate_limiter)
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
                print("Uploading full PDF...")
                uploaded_pdf = genai.upload_file(pdf_path)
                files_to_clean_up.append(uploaded_pdf)
                print(f"Uploaded as {uploaded_pdf.name}. Finding reference sections...")
                all_sections = find_reference_section_pages(pdf_path, model, uploaded_pdf, total_physical_pages=total_physical_pages, rate_limiter=rate_limiter)

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

        # --- Extraction Logic (remains mostly the same) ---
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
                try:
                    new_pdf = pikepdf.Pdf.new()
                    for page_num in range(start_idx, end_idx + 1):
                        new_pdf.pages.append(pdf_doc.pages[page_num])
                    expanded_output_dir = os.path.expanduser(output_dir)
                    os.makedirs(expanded_output_dir, exist_ok=True)
                    output_filename = f"{output_base_filename}_refs_physical_p{phys_start_1_based_for_naming}-{phys_end_1_based_for_naming}.pdf"
                    output_path = os.path.join(expanded_output_dir, output_filename)
                    new_pdf.save(output_path)
                    final_extracted_files.append(output_path)
                    print(f"    - Successfully extracted pages to {output_path}")
                except Exception as e:
                    print(f"    - Error extracting pages {start_idx}-{end_idx}: {e}. Skipping section.")

        print("\n--- Final Extracted Files ---")
        if final_extracted_files:
            for f_path in final_extracted_files:
                print(f"  - {f_path}")
        else:
            print("  No files were extracted.")

    except (FileNotFoundError, pikepdf.PasswordError, Exception) as e:
        print(f"An unexpected error occurred in extract_reference_sections: {e}", flush=True)
        import traceback
        traceback.print_exc()
    finally:
        # --- Centralized Cleanup --- 
        if files_to_clean_up:
            print(f"\nCleaning up {len(files_to_clean_up)} uploaded file(s)...")
            for f in files_to_clean_up:
                try:
                    genai.delete_file(f.name)
                    print(f"  - Successfully deleted {f.name}.")
                except Exception as e:
                    print(f"  - Failed to delete {f.name}: {e}")
        if temp_chunk_dir and os.path.exists(temp_chunk_dir):
            print(f"Cleaning up temporary chunk directory: {temp_chunk_dir}")
            shutil.rmtree(temp_chunk_dir)
        print("--- Exiting extract_reference_sections --- ", flush=True)

def process_pdf(pdf_path, output_dir):
    extract_reference_sections(pdf_path, output_dir)

if __name__ == "__main__":
    print("--- get_bib_pages.py script started ---", flush=True)
    # --- Argument Parsing Setup ---
    parser = argparse.ArgumentParser(description='Extract reference sections from PDF files using GenAI.')
    parser.add_argument('input_path', type=str, help='Path to a PDF file or a directory containing PDF files.')
    parser.add_argument('--output_dir', type=str, default=os.path.join(os.path.expanduser("~"), "Nextcloud/DT/papers_extracted_references"), 
                        help='Directory to save extracted reference PDFs. Defaults to ~/Nextcloud/DT/papers_extracted_references')
    # Add --version argument
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0_gemini_flash_batch_offset_v5_centralized_upload_and_config_fix"
    )
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
