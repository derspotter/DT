import os
import argparse
import json
import google.generativeai as genai
import pikepdf
import shutil
import tempfile

# Configure API key (replace with your actual key or use environment variable)
api_key = os.environ.get("GOOGLE_API_KEY")
if not api_key:
    raise ValueError("GOOGLE_API_KEY environment variable not set.")

genai.configure(api_key=api_key)

# Define the prompt for identifying bibliography sections
prompt = """
Your task is to identify ALL bibliography or reference sections in this PDF document.

INSTRUCTIONS:
1. Identify sections that contain lists of references, citations, or bibliographic entries. These may be titled 'References', 'Bibliography', 'Works Cited', 'Citations', 'Notes', 'Endnotes', or similar, but a title is not required—look for content patterns like lists of authors, publication years, titles, and publishers even if untitled.
2. For each section found, determine its start and end page numbers using a strict 0-based index (i.e., the first page of the PDF is page 0, the second page is page 1, and so on).
3. **CRITICAL: Use a 0-based index for page numbering. The first page of the PDF is page 0, regardless of any printed numbers on the pages. Count pages sequentially from the beginning of the PDF.**
4. **CRITICAL: 'start_page' MUST be the page where the bibliography SECTION begins (e.g., where the title or heading like 'References' appears, or where the section clearly starts), even if the actual bibliography entries start later on that page or if the page includes other content. 'end_page' MUST be the page where the LAST bibliography entry of the section ends. DO NOT include pages with unrelated content or empty pages after the last entry. Ensure 'end_page' is precisely the page with the final reference entry.**
5. **CRITICAL: Find ALL bibliography sections in the document, especially those that appear after each chapter or major section. Do not skip any potential reference lists, even if they are short or appear in unexpected locations.**
6. Return ONLY a JSON array with the format shown below.
7. Do not include any explanatory text, markdown, or additional content outside the JSON.

REQUIRED FORMAT:
[
    {
        "start_page": 16,  // The 0-based index of the page where the bibliography SECTION starts (even if entries start later on the page)
        "end_page": 19     // The 0-based index of the page where the LAST bibliography entry of the section ends (exclude empty or unrelated pages)
    }
]

IMPORTANT RULES:
- ALWAYS include both 'start_page' AND 'end_page' for each section.
- Use INTEGER numbers only.
- **You MUST use a strict 0-based index for page numbering. The first page of the PDF is page 0, the second is page 1, and so on, regardless of any printed numbers or other indexing systems.**
- **Ensure 'start_page' is the exact page where the bibliography SECTION starts (e.g., title or clear beginning of section) and 'end_page' is the exact page with the LAST bibliography entry of the section. Exclude any trailing empty pages or pages with unrelated content after the last entry. 'end_page' must be the page containing the final reference entry, not beyond it.**
- **Include EVERY bibliography section, particularly those following individual chapters or major sections. Thoroughly check the end of each chapter for reference lists.**
- Include ALL reference sections if multiple exist, even if they are in different parts of the document.
- If there is only one section (e.g., at the end), return only that one.
- Return ONLY valid JSON. No explanations, no markdown, no additional text.
"""

def find_bibliography_sections(pdf_path):
    """Upload the PDF and prompt the model to find all bibliography sections."""
    print("Uploading PDF...")
    model = genai.GenerativeModel("gemini-2.5-flash-preview-04-17", generation_config=genai.types.GenerationConfig(temperature=0.0))
    uploaded_pdf = genai.upload_file(pdf_path)
    print("PDF uploaded successfully.")

    print("Sending prompt to identify bibliography sections...")
    response = model.generate_content([prompt, uploaded_pdf], request_options={'timeout': 600})
    print("Received response.")

    # Clean and parse the JSON response
    response_text = response.text.strip()
    if response_text.startswith("```json"):
        response_text = response_text[7:]
    if response_text.endswith("```"):
        response_text = response_text[:-3]
    sections = json.loads(response_text)
    return sections, uploaded_pdf, model

def validate_section(pdf_path, section, uploaded_pdf, model):
    """Validate the start and end pages of a bibliography section."""
    reported_start = section['start_page']
    reported_end = section['end_page']
    print(f"  - Validating Section: Start={reported_start}, End={reported_end}")

    # Extract single pages for validation
    with pikepdf.Pdf.open(pdf_path) as pdf:
        start_page_pdf = pikepdf.Pdf.new()
        start_page_pdf.pages.append(pdf.pages[reported_start])
        end_page_pdf = pikepdf.Pdf.new()
        end_page_pdf.pages.append(pdf.pages[reported_end])

        # Save temporary single-page PDFs
        temp_start_dir = tempfile.mkdtemp()
        temp_start_path = os.path.join(temp_start_dir, f"validate_start_{reported_start}.pdf")
        start_page_pdf.save(temp_start_path)
        temp_end_dir = tempfile.mkdtemp()
        temp_end_path = os.path.join(temp_end_dir, f"validate_end_{reported_end}.pdf")
        end_page_pdf.save(temp_end_path)

    print(f"    - Saved temporary single-page PDFs for validation.")

    # Upload temporary PDFs
    uploaded_start = genai.upload_file(temp_start_path)
    uploaded_end = genai.upload_file(temp_end_path)
    print(f"    - Uploaded validation pages: Start (page {reported_start}), End (page {reported_end})")

    # Validation prompt
    validation_prompt = f"""
You are tasked with validating the start and end pages of a bibliography or reference section in a PDF document.

For the START PAGE:
  - Confirm if this page marks the BEGINNING of a bibliography section (e.g., contains a title like 'References' or the clear start of the section, even if actual entries start later on the page).
  - The page may contain other content before the section starts, but it should be the page where the section is introduced.

For the END PAGE:
  - Confirm if this page marks the END of a bibliography section (e.g., contains the last reference entry or related content).
  - The page may contain other content after the last entry, but it should be the page with the final bibliography content.

Output Format:
  Return a JSON object with two keys, for example:
  {{
    "{uploaded_start.name}": {{"is_start": true}},
    "{uploaded_end.name}":   {{"is_end": true}}
  }}
  ONLY valid JSON. No markdown or extra text.
"""

    print(f"    --- Sending Validation Prompt for Section (Checking pages {reported_start}-{reported_end}) ---")
    request_content = [validation_prompt, uploaded_start, uploaded_end]
    try:
        response = model.generate_content(request_content, request_options={'timeout': 600})
        print(f"    --- Received Validation Response for Section ---")
        response_text = response.text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        if not response_text:
            print(f"    - Error: Empty response received.")
            is_valid_start = False
            is_valid_end = False
        else:
            try:
                validation_json = json.loads(response_text)
                start_info = validation_json.get(uploaded_start.name, {"is_start": False})
                end_info = validation_json.get(uploaded_end.name, {"is_end": False})
                is_valid_start = start_info.get("is_start", False)
                is_valid_end = end_info.get("is_end", False)
                print(f"    - Validation Results: Start={is_valid_start}, End={is_valid_end}")
            except json.JSONDecodeError as jde:
                print(f"    - Error parsing JSON response: {jde}")
                print(f"    - Response content: {response_text[:100]}...")
                is_valid_start = False
                is_valid_end = False
    except Exception as e:
        print(f"    - Error during validation: {e}")
        is_valid_start = False
        is_valid_end = False
    finally:
        # Clean up
        try:
            genai.delete_file(uploaded_start.name)
            genai.delete_file(uploaded_end.name)
            shutil.rmtree(temp_start_dir)
            shutil.rmtree(temp_end_dir)
        except Exception as e:
            print(f"    - Cleanup error: {e}")

    return is_valid_start and is_valid_end

def extract_bibliography_pages(pdf_path, sections, output_dir):
    """Extract pages from validated sections and save as separate PDFs."""
    with pikepdf.Pdf.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
        extracted_count = 0

        for i, section in enumerate(sections):
            start_page = section['start_page']
            end_page = section['end_page']

            if start_page < 0 or end_page >= total_pages or start_page > end_page:
                print(f"  - Skipping invalid section {i+1}: Start={start_page}, End={end_page} (out of bounds)")
                continue

            output_pdf = pikepdf.Pdf.new()
            for page_num in range(start_page, end_page + 1):
                output_pdf.pages.append(pdf.pages[page_num])

            output_filename = f"{pdf_name}_bib_section_{i+1}_pages_{start_page+1}-{end_page+1}.pdf"
            output_path = os.path.join(output_dir, output_filename)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            output_pdf.save(output_path)
            print(f"  - Saved bibliography section {i+1} to {output_path}")
            extracted_count += (end_page - start_page + 1)

        return extracted_count

def main():
    parser = argparse.ArgumentParser(description="Simple script to find and extract bibliography sections from PDFs.")
    parser.add_argument("input_dir", help="Directory containing PDF files to process.")
    parser.add_argument("--output-dir", default="~/Nextcloud/DT/papers", help="Directory to save output.")
    args = parser.parse_args()

    input_dir = os.path.expanduser(args.input_dir)
    output_dir = os.path.expanduser(args.output_dir)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Find PDF files in the input directory
    pdf_files = [f for f in os.listdir(input_dir) if f.endswith(".pdf")]
    if not pdf_files:
        print(f"No PDF files found in {input_dir}")
        return

    print(f"Found {len(pdf_files)} PDF file(s) in {input_dir}")
    for pdf_file in pdf_files:
        pdf_path = os.path.join(input_dir, pdf_file)
        print(f"Processing {pdf_file}...")
        sections, uploaded_pdf, model = find_bibliography_sections(pdf_path)
        print(f"Found bibliography sections: {sections}")

        # Select sections to validate: first, last, and two middle sections
        if not sections:
            print("No sections found to validate.")
            continue

        total_sections = len(sections)
        indices_to_validate = []
        if total_sections == 1:
            indices_to_validate = [0]
        elif total_sections == 2:
            indices_to_validate = [0, 1]
        elif total_sections == 3:
            indices_to_validate = [0, 1, 2]
        else:
            # For 4 or more sections, validate first, last, and two middle
            middle_index1 = total_sections // 3
            middle_index2 = (2 * total_sections) // 3
            indices_to_validate = [0, middle_index1, middle_index2, total_sections - 1]

        print(f"Validating {len(indices_to_validate)} sections: indices {indices_to_validate}")
        validated_count = 0
        for i in indices_to_validate:
            print(f"Validating section {i+1}...")
            if validate_section(pdf_path, sections[i], uploaded_pdf, model):
                validated_count += 1
            else:
                print(f"  - Section {i+1} failed validation.")

        # If at least 3 out of 4 validated, extract all sections
        should_extract = validated_count >= 3 if len(indices_to_validate) == 4 else validated_count >= len(indices_to_validate) * 0.75
        print(f"Validation result: {validated_count}/{len(indices_to_validate)} sections validated. Will extract all sections: {should_extract}")

        # Save the results (all sections, validated or not, for reference)
        output_file = os.path.join(output_dir, f"{os.path.splitext(pdf_file)[0]}_bib_sections.json")
        with open(output_file, "w") as f:
            json.dump(sections, f, indent=2)
        print(f"Saved results to {output_file}")

        # Extract bibliography pages if validation threshold met
        if should_extract and sections:
            extracted_count = extract_bibliography_pages(pdf_path, sections, output_dir)
            print(f"Extracted {extracted_count} bibliography pages from {len(sections)} section(s).")
        else:
            print("Validation threshold not met or no sections to extract.")

        # Clean up uploaded PDF
        try:
            genai.delete_file(uploaded_pdf.name)
            print("Cleaned up uploaded PDF.")
        except Exception as e:
            print(f"Error cleaning up uploaded PDF: {e}")

if __name__ == "__main__":
    main()
