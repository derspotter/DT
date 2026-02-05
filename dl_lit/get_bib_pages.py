from google import genai as google_genai
from google.genai import types
import pikepdf
import json
import os
import sys
import argparse
import tempfile
import shutil
import concurrent.futures
import time
from dotenv import load_dotenv
import glob

print("--- Script Top --- ", flush=True)

print("--- Imports Done --- ", flush=True)

print("--- Configuring GenAI --- ", flush=True)
load_dotenv()
api_key = os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_API_KEY')
api_client = None
if not api_key:
    print("Error: GEMINI_API_KEY (or GOOGLE_API_KEY) environment variable not set.", flush=True)
else:
    try:
        api_client = google_genai.Client(api_key=api_key)
        print("Successfully configured Google GenAI client with API key.")
    except Exception as e:
        print(f"Error configuring Google GenAI client: {e}")
print("--- GenAI Configured (or skipped) --- ", flush=True)


class GenAIModel:
    def __init__(self, model_name: str, temperature: float = 1.0):
        self.model_name = model_name
        self.temperature = temperature

    def generate_content(self, contents, request_options=None):
        if api_client is None:
            raise RuntimeError("GenAI client not configured.")
        config = types.GenerateContentConfig(
            temperature=self.temperature,
            response_mime_type="application/json",
        )

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


def upload_pdf(path: str):
    if api_client is None:
        raise RuntimeError("GenAI client not configured.")
    return api_client.files.upload(file=path)


def delete_uploaded(name: str):
    if api_client is None:
        return
    api_client.files.delete(name=name)


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


def find_reference_section_pages(pdf_path: str, model, uploaded_pdf, total_physical_pages: int):
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

    response = model.generate_content([prompt, uploaded_pdf], request_options={'timeout': 600})
    print("Raw find_reference_section_pages response: ```json")
    print(response.text)
    print("```", flush=True)

    try:
        import re
        json_match = re.search(r'```json\s*([\s\S]*?)\s*```', response.text or "")
        if json_match:
            json_text = json_match.group(1)
        else:
            json_text = response.text or ""
        sections = json.loads(clean_json_response(json_text))
        print(f"Found reference sections: {json.dumps(sections, indent=2)}")
        print("")
        return sections
    except Exception as e:
        print(f"Error in find_reference_section_pages: {e}", flush=True)
        return []


def _normalize_sections(sections, total_physical_pages: int):
    normalized = []
    for sec in sections:
        try:
            start_page = int(sec.get('start_page'))
            end_page = int(sec.get('end_page'))
        except (TypeError, ValueError):
            print(f"Warning: Invalid section values: {sec}. Skipping.")
            continue
        if start_page <= 0 or end_page <= 0 or start_page > end_page:
            print(f"Warning: Invalid page range from AI: {sec}. Skipping.")
            continue
        if start_page > total_physical_pages or end_page > total_physical_pages:
            print(f"Warning: Page range out of bounds: {sec}. Skipping.")
            continue
        normalized.append({"start_page": start_page, "end_page": end_page})
    return normalized


def extract_reference_sections(pdf_path: str, output_dir: str = "~/Nextcloud/DT/papers"):
    """Main function to find and extract reference sections, with chunking for large PDFs."""
    print("--- Entered extract_reference_sections --- ", flush=True)
    if not api_key:
        print("Skipping extraction: GOOGLE_API_KEY not configured.", flush=True)
        return []

    model = GenAIModel("gemini-3-flash-preview", temperature=1.0)
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
                return []
            print(f"PDF has {total_physical_pages} pages.")

            if total_physical_pages > MAX_PAGES_FOR_API:
                print(f"PDF exceeds {MAX_PAGES_FOR_API} pages. Splitting into chunks of {CHUNK_SIZE} pages.")
                temp_chunk_dir = tempfile.mkdtemp()

                for i in range(0, total_physical_pages, CHUNK_SIZE):
                    start_page = i
                    end_page = min(i + CHUNK_SIZE, total_physical_pages)
                    print(f"\nProcessing chunk: pages {start_page + 1}-{end_page}")

                    chunk_pdf = pikepdf.Pdf.new()
                    for page_num in range(start_page, end_page):
                        chunk_pdf.pages.append(pdf_doc.pages[page_num])

                    base_filename = os.path.splitext(os.path.basename(pdf_path))[0]
                    chunk_display_name = f"{base_filename}_chunk_pages_{start_page+1}-{end_page}.pdf"
                    chunk_path = os.path.join(temp_chunk_dir, chunk_display_name)
                    chunk_pdf.save(chunk_path)

                    print(f"Uploading chunk: {chunk_display_name}")
                    uploaded_chunk = upload_pdf(chunk_path)
                    files_to_clean_up.append(uploaded_chunk)
                    print(f"Uploaded as {uploaded_chunk.name}. Finding reference sections in chunk...")

                    chunk_page_count = end_page - start_page
                    chunk_sections = find_reference_section_pages(
                        chunk_path,
                        model,
                        uploaded_chunk,
                        total_physical_pages=chunk_page_count,
                    )
                    if chunk_sections:
                        for sec in chunk_sections:
                            sec['start_page'] += start_page
                            sec['end_page'] += start_page
                        all_sections.extend(chunk_sections)
                        print(f"Found {len(chunk_sections)} section(s) in chunk. Current total: {len(all_sections)}.")
                    else:
                        print("No reference sections found in this chunk.")
            else:
                print("PDF is within size limits, processing as a single file.")
                uploaded_pdf = upload_pdf(pdf_path)
                files_to_clean_up.append(uploaded_pdf)
                print(f"Uploaded as {uploaded_pdf.name}. Finding reference sections...")
                all_sections = find_reference_section_pages(
                    pdf_path,
                    model,
                    uploaded_pdf,
                    total_physical_pages=total_physical_pages,
                )

        if not all_sections:
            print("No reference sections found by the AI in the entire document.", flush=True)
            return []

        normalized_sections = _normalize_sections(all_sections, total_physical_pages)
        if not normalized_sections:
            print("No valid reference sections after normalization.", flush=True)
            return []

        output_dir_abs = os.path.expanduser(output_dir)
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        if os.path.basename(output_dir_abs) == base_name:
            specific_output_dir = output_dir_abs
        else:
            specific_output_dir = os.path.join(output_dir_abs, base_name)
        os.makedirs(specific_output_dir, exist_ok=True)
        print(f"Output directory for extracted pages: {specific_output_dir}")

        generated_page_pdf_filenames = []
        print("\n--- Extracting Bibliography Pages ---", flush=True)
        with pikepdf.Pdf.open(pdf_path) as pdf_doc:
            for i, section in enumerate(normalized_sections, 1):
                start_page_idx = section["start_page"] - 1
                end_page_idx = section["end_page"] - 1
                print(f"  - Extracting Section {i} (Physical Pages {start_page_idx} to {end_page_idx})...", flush=True)

                for page_num_idx in range(start_page_idx, end_page_idx + 1):
                    new_pdf_page = pikepdf.Pdf.new()
                    try:
                        if 0 <= page_num_idx < len(pdf_doc.pages):
                            new_pdf_page.pages.append(pdf_doc.pages[page_num_idx])
                            output_filename = f"{base_name}_section{i}_page{page_num_idx + 1}.pdf"
                            output_path = os.path.join(specific_output_dir, output_filename)
                            new_pdf_page.save(output_path)
                            generated_page_pdf_filenames.append(output_filename)
                            print(f"    - Saved: {output_path}", flush=True)
                        else:
                            print(f"    - Warning: Physical page index {page_num_idx} out of bounds. Skipping page.", flush=True)
                    except Exception as extract_err:
                        print(f"    - Error extracting physical page {page_num_idx} of section {i}: {extract_err}", flush=True)
                    finally:
                        new_pdf_page.close()

        print("--- Extraction Complete ---", flush=True)
        return generated_page_pdf_filenames

    except Exception as e:
        print(f"An unexpected error occurred in the main processing block: {e}", flush=True)
        return []
    finally:
        if files_to_clean_up:
            print("\n--- Cleaning up uploaded files ---", flush=True)
            for uploaded in files_to_clean_up:
                try:
                    print(f"    - Deleting {uploaded.name}...", flush=True)
                    delete_uploaded(uploaded.name)
                except Exception as del_err:
                    print(f"    - Warning: Failed to delete {uploaded.name}: {del_err}", flush=True)
        if temp_chunk_dir and os.path.exists(temp_chunk_dir):
            print(f"--- Cleaning up temporary chunk directory: {temp_chunk_dir} ---", flush=True)
            shutil.rmtree(temp_chunk_dir)
        print("--- extract_reference_sections function finished ---", flush=True)


def process_pdf(pdf_path, output_dir):
    return extract_reference_sections(pdf_path, output_dir)


if __name__ == "__main__":
    print("--- get_bib_pages.py script started ---", flush=True)
    parser = argparse.ArgumentParser(description='Extract bibliography pages from PDFs.')
    parser.add_argument('input', nargs='?', help='Path to a PDF file or directory containing PDFs')
    parser.add_argument('--input-pdf', dest='input_pdf', help='Path to a single PDF file')
    parser.add_argument('--input-dir', dest='input_dir', help='Path to a directory containing PDFs')
    parser.add_argument('--output-dir', default='bib_output', help='Output directory for extracted pages and JSON files')
    parser.add_argument(
        '--move-to-done',
        action='store_true',
        help="Move processed PDFs into OUTPUT_DIR/done_bibs/ after extraction. Disabled by default.",
    )
    args = parser.parse_args()

    if not api_key:
        print("Error: GEMINI_API_KEY (or GOOGLE_API_KEY) environment variable not set.", flush=True)
        sys.exit(1)

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory set to: {output_dir}", flush=True)

    input_source = args.input_pdf or args.input_dir or args.input
    if not input_source:
        print("Error: No input provided. Use --input-pdf, --input-dir, or the positional input.", flush=True)
        sys.exit(1)

    input_path = os.path.abspath(input_source)
    if os.path.isdir(input_path):
        pdf_files = glob.glob(os.path.join(input_path, '*.pdf'))
        print(f"Found {len(pdf_files)} PDF files in directory: {input_path}", flush=True)
    else:
        pdf_files = [input_path] if input_path.endswith('.pdf') else []
        if not pdf_files:
            print(f"Error: {input_path} is not a PDF file.", flush=True)
            sys.exit(1)
        print(f"Processing single PDF file: {input_path}", flush=True)

    print(f"Starting sequential processing of {len(pdf_files)} PDF(s)...", flush=True)
    start_time = time.time()

    total_bib_pages = 0
    processed_count = 0
    done_bibs_dir = None
    if args.move_to_done:
        done_bibs_dir = os.path.join(output_dir, 'done_bibs')
        os.makedirs(done_bibs_dir, exist_ok=True)

    for pdf_path in pdf_files:
        bib_pages = process_pdf(pdf_path, output_dir)
        total_bib_pages += len(bib_pages)
        processed_count += 1
        print(f"Completed {processed_count}/{len(pdf_files)} PDFs: {os.path.basename(pdf_path)} - {len(bib_pages)} bibliography pages found.", flush=True)

        if args.move_to_done and done_bibs_dir and bib_pages:
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
