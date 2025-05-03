#!/usr/bin/env python3
import os
import sys
import argparse
import json
import tempfile
import shutil
import pikepdf
from google import genai
import time
from dotenv import load_dotenv
import concurrent.futures
from pydantic import BaseModel, Field, RootModel
from google.genai.types import GenerateContentConfig
from typing import List

def main():
    parser = argparse.ArgumentParser(description="Detect pages containing references in a PDF.")
    parser.add_argument('--input-pdf', required=True, help='Path to the input PDF')
    parser.add_argument('--output-dir', required=True, help='Directory to save the output JSON')
    parser.add_argument('--max-pages', type=int, default=None, help='Limit processing to first N pages for testing')
    args = parser.parse_args()

    # Load environment variables from .env if present
    load_dotenv()
    api_key = os.getenv('GOOGLE_API_KEY')
    if not api_key:
        print('[ERROR] GOOGLE_API_KEY not set', file=sys.stderr)
        sys.exit(1)
    # Initialize GenAI client (reads API key from environment)
    client = genai.Client()

    pdf = pikepdf.Pdf.open(args.input_pdf)
    files = []
    entries = []
    tmp_dirs = []  # no longer used for cleanup
    page_infos = []
    # create directory for split pages in output dir
    pages_dir = os.path.join(args.output_dir, 'split_pages')
    os.makedirs(pages_dir, exist_ok=True)
    for i, page in enumerate(pdf.pages):
        if args.max_pages is not None and i >= args.max_pages:
            print(f"[INFO] Reached max-pages limit at page {i}, stopping split.", flush=True)
            break
        print(f"[INFO] Splitting and uploading page {i}...", flush=True)
        single = pikepdf.Pdf.new()
        single.pages.append(page)
        # save each split page to persistent directory
        page_path = os.path.join(pages_dir, f'page_{i}.pdf')
        single.save(page_path)
        single.close()
        print(f"[DEBUG] Saved single-page PDF at {page_path}", flush=True)
        page_infos.append({'idx': i, 'path': page_path})

    if args.max_pages is not None:
        print(f"[INFO] Test mode: limiting to first {args.max_pages} pages", flush=True)

    # Concurrently upload pages in batches of 10
    num_pages = len(page_infos)
    print(f"[INFO] Uploading {num_pages} pages concurrently with 10 workers...", flush=True)
    files = [None] * num_pages
    entries = [None] * num_pages
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Use keyword argument 'file' for upload per API signature
        future_to_idx = {executor.submit(client.files.upload, file=info['path']): info['idx'] for info in page_infos}
        for future in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[future]
            file_obj = future.result()
            files[idx] = file_obj
            # Store original index for later mapping
            entries[idx] = {'name': file_obj.name, 'has_references': False, 'index': idx}
            print(f"[DEBUG] Uploaded page {idx} -> name: {file_obj.name}", flush=True)

    # Schema only for the has_references flag
    class PageEntry(BaseModel):  # model for each page
        name: str = Field(..., description="Exact attachment file name")
        has_references: bool = Field(..., description="True if page contains references")
    class PageEntryList(RootModel[List[PageEntry]]):
        pass

    # Build names list for prompt
    attachment_names = [f.name for f in files]
    attachment_list_json = json.dumps(attachment_names)
    print(f"[DEBUG] Attachment list: {attachment_list_json}", flush=True)
    # Prompt: flag any reference-like content, ignoring page number headers/footers
    prompt = f"""
You have the following list of attachment filenames: {attachment_list_json}

For each page, examine its full text. Ignore any standalone numbers at the top or bottom of the page (page headers/footers).
If the remaining text contains any reference-like content—such as:
- Author–year citations (e.g., (Smith, 2020))
- Numbered lists of references ([1], 1., etc.)
- Bibliographic entries with authors, titles, publication details

then set "has_references": true; otherwise false.

Return strictly a JSON array of objects in the same order, each exactly like:
  {{ "name": "<attachment name>", "has_references": <true|false> }}
Do not include any extra keys, explanations, or formatting.
"""
    print(f"[DEBUG] Prompt text: {prompt}", flush=True)
    print(f"[DEBUG] Attachments count: {len(files)}", flush=True)

    # Now send single prompt with strict instructions
    start_time = time.time()
    print(f"[INFO] Sending strict prompt to model", flush=True)
    # Send prompt and attachments
    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=[prompt] + files,
        config=GenerateContentConfig(
            temperature=0,
            response_schema=PageEntryList,
            response_mime_type="application/json",
        ),
    )
    elapsed = time.time() - start_time
    print(f"[INFO] Received strict response in {elapsed:.2f}s", flush=True)
    print(f"[DEBUG] Response text: {response.text!r}", flush=True)
    print(f"[DEBUG] Response raw repr: {repr(response.text)}", flush=True)
    print(f"[DEBUG] Response length: {len(response.text)} chars", flush=True)

    # Parse structured response for has_references and build final mapping
    parsed = response.parsed
    if isinstance(parsed, tuple) and len(parsed) == 1:
        parsed = parsed[0]
    # get list of PageEntry
    entries_list = parsed.root if hasattr(parsed, 'root') else parsed
    # Map each parsed entry by name to original index
    name_to_index = {item['name']: item['index'] for item in entries}
    mapping = []
    for entry in entries_list:
        # entry has name and has_references
        name = entry.name if hasattr(entry, 'name') else entry.get('name')
        if name not in name_to_index:
            print(f"[ERROR] Unknown name in response: {name}", file=sys.stderr)
            sys.exit(1)
        has_refs = entry.has_references if hasattr(entry, 'has_references') else entry.get('has_references')
        mapping.append({'name': name, 'has_references': has_refs, 'index': name_to_index[name]})
    # Ensure output is ordered by original index
    if any(item['index'] != idx for idx, item in enumerate(mapping)):
        print("[WARNING] Model response out of order; reordering by index.", file=sys.stderr)
    mapping = sorted(mapping, key=lambda x: x['index'])

    # Validate sections
    validated_mapping = []
    for i, entry in enumerate(mapping):
        if entry['has_references']:
            print(f"Validating page {entry['index']}...")
            # Extract single page for validation
            with pikepdf.Pdf.open(args.input_pdf) as pdf:
                page_pdf = pikepdf.Pdf.new()
                page_pdf.pages.append(pdf.pages[entry['index']])
                # Save temporary single-page PDF
                temp_dir = tempfile.mkdtemp()
                temp_path = os.path.join(temp_dir, f"validate_page_{entry['index']}.pdf")
                page_pdf.save(temp_path)

            print(f"    - Saved temporary single-page PDF for validation.")

            # Upload temporary PDF
            uploaded_page = client.files.upload(file=temp_path)
            print(f"    - Uploaded validation page: {entry['index']}")

            # Validation prompt
            validation_prompt = f"""
You are tasked with validating if this page contains references.

Output Format:
  Return a JSON object with a single key, for example:
  {{
    "{uploaded_page.name}": {{"is_reference": true}}
  }}
  ONLY valid JSON. No markdown or extra text.
"""

            print(f"    --- Sending Validation Prompt for Page {entry['index']} ---")
            request_content = [validation_prompt, uploaded_page]
            try:
                response = client.models.generate_content(request_content, request_options={'timeout': 600})
                print(f"    --- Received Validation Response for Page {entry['index']} ---")
                validation_json = json.loads(response.text.strip())
                is_reference = validation_json.get(uploaded_page.name, {}).get("is_reference", False)
                print(f"    - Validation Results: is_reference={is_reference}")
            except Exception as e:
                print(f"    - Error during validation: {e}")
                is_reference = False
            finally:
                # Clean up
                try:
                    client.files.delete(uploaded_page.name)
                    shutil.rmtree(temp_dir)
                except Exception as e:
                    print(f"    - Cleanup error: {e}")

            if is_reference:
                validated_mapping.append(entry)
            else:
                print(f"  - Page {entry['index']} failed validation and will be skipped.")

    os.makedirs(args.output_dir, exist_ok=True)
    out_path = os.path.join(args.output_dir, 'reference_pages.json')
    with open(out_path, 'w') as f:
        json.dump(validated_mapping, f, indent=2)
    print(f'Saved reference presence mapping to {out_path}')

if __name__ == '__main__':
    main()
