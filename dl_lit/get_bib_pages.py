import google.generativeai as genai
import json
import pikepdf
import os
from collections import defaultdict

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

def find_reference_section_pages(pdf_path):
    """First pass: Get reference section page ranges from the full PDF."""
    try:
        model = genai.GenerativeModel("gemini-2.0-flash-exp")
        uploaded_pdf = genai.upload_file(pdf_path)

        prompt = """
Your task is to find ALL bibliography/reference sections in this PDF.

INSTRUCTIONS:
1. Look for sections titled: References, Bibliography, Works Cited, Citations, Notes, Endnotes
2. For each section found, note its exact start and end page numbers
3. Return ONLY a JSON array with the format shown below
4. Do not include any explanatory text

REQUIRED FORMAT:
[
    {
        "start_page": 211,
        "end_page": 278
    }
]

IMPORTANT RULES:
- ALWAYS include both start_page AND end_page for each section
- Use INTEGER numbers only
- Use the actual printed page numbers visible on the pages
- Return ONLY valid JSON, no markdown, no explanations
- Include ALL reference sections if multiple exist. 
- And if there is only one at the end, only return only this one.
- Be sure to include every whole section!
""" 

        response = model.generate_content(
            [prompt, uploaded_pdf],
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json"
            )
        )
        
        if response.text:
            try:
                cleaned_response = clean_json_response(response.text)
                sections = json.loads(cleaned_response)
                # Validate that all sections have both start_page and end_page
                valid_sections = []
                for section in sections:
                    if 'start_page' in section and 'end_page' in section:
                        valid_sections.append(section)
                    else:
                        print(f"Warning: Skipping incomplete section: {section}")
                
                if not valid_sections:
                    print("No valid sections found (missing start_page or end_page)")
                    return None
                
                print(f"Found reference sections: {json.dumps(valid_sections, indent=2)}")
                
                # Store the model and uploaded PDF in function attributes for reuse
                find_reference_section_pages.model = model
                find_reference_section_pages.uploaded_pdf = uploaded_pdf
                
                return valid_sections
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON response: {e}")
                print(f"Raw response: {response.text}")
                return None
        else:
            print("Empty response from model")
            return None

    except Exception as e:
        print(f"Error finding reference sections: {str(e)}")
        return None

def detect_page_number_offset(pdf_path: str, model: genai.GenerativeModel, uploaded_pdf) -> int | None:
    """
    Sample pages at 20%, 40%, 60%, and 80% of the document to detect offset.
    """
    temp_pdf_path = "temp_sample_pages.pdf"
    try:
        # Create sampled PDF
        with pikepdf.Pdf.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            fifth = total_pages // 5
            # Sample at 1/5, 2/5, 3/5, and 4/5 of document
            new_pdf = pikepdf.Pdf.new()
            for i in range(1, 5):  # Will give us pages at 20%, 40%, 60%, 80%
                page_num = i * fifth
                new_pdf.pages.append(pdf.pages[page_num - 1])  # -1 for 0-based indexing
            
            # Save the sampled pages to a temporary file
            new_pdf.save(temp_pdf_path)
        
        # Upload the sampled pages
        sampled_pdf = genai.upload_file(temp_pdf_path)
        
        prompt = """
TASK: Extract the printed page numbers from this PDF sample.

INSTRUCTIONS:
1. For each page in this sample (numbered 1-4):
   - Find the actual printed page number visible on the page
   - Return both the sample page number (1-4) and the printed number

REQUIRED FORMAT:
[
    {
        "physical_page": 1,
        "printed_number": 20
    },
    {
        "physical_page": 2,
        "printed_number": 40
    }
]

IMPORTANT:
- Use only integers
- physical_page must be 1-4
- printed_number should be the actual number printed on the page
- Return ONLY valid JSON, no explanations
"""
        
        response = model.generate_content(
            [prompt, sampled_pdf],
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json"
            )
        )
        
        if response.text:
            try:
                cleaned_response = clean_json_response(response.text)
                page_numbers = json.loads(cleaned_response)
                print(f"Detected page numbers: {json.dumps(page_numbers, indent=2)}")
                
                offsets = defaultdict(int)
                for entry in page_numbers:
                    try:
                        phys_page = int(entry.get('physical_page', 0))
                        print(phys_page)
                        printed_num = entry.get('printed_number', '0')
                        if phys_page > 0:
                            actual_page = phys_page * fifth
                            offset = printed_num - actual_page
                            print(f"Sample #{phys_page}: Physical page {actual_page} shows number {printed_num} -> offset {offset}")
                            offsets[offset] += 1
                    except (ValueError, TypeError, IndexError) as e:
                        continue
                
                if offsets:
                    print(f"Detected offsets and their frequencies: {dict(offsets)}")
                    most_common_offset = max(offsets.items(), key=lambda x: x[1])
                    if most_common_offset[1] >= 2:  # At least 2 pages agree on offset
                        return most_common_offset[0]
            
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON response for page numbers: {e}")
                print(f"Raw response: {response.text}")
    
    except Exception as e:
        print(f"Error in pagination detection: {e}")
    
    finally:
        # Clean up temporary file
        if os.path.exists(temp_pdf_path):
            try:
                os.remove(temp_pdf_path)
            except Exception as e:
                print(f"Error removing temporary file: {e}")
    
    return None

def extract_reference_sections(pdf_path: str, output_dir: str = "~/Nextcloud/DT/papers"):
    """Main function to find, adjust, and extract reference sections."""
    # First, find reference sections
    sections = find_reference_section_pages(pdf_path)
    if not sections:
        print("No reference sections found.")
        return
    
    # Then detect page number offset using the same model and uploaded PDF
    offset = detect_page_number_offset(pdf_path, find_reference_section_pages.model, find_reference_section_pages.uploaded_pdf)
    if offset is None:
        print("Warning: Could not detect page number offset. Using physical page numbers.")
        offset = 0
    else:
        print(f"Detected page number offset: {offset}")
    
    # Adjust section page numbers
    adjusted_sections = []
    with pikepdf.Pdf.open(pdf_path) as doc:  # Changed to pikepdf for consistency
        total_pages = len(doc.pages)
        for section in sections:
            try:
                start_page = section.get('start_page', 0)
                end_page = section.get('end_page', 0)
                
                if start_page > 0 and end_page > 0:
                    # Adjust for offset and 0-based indexing
                    physical_start = start_page - offset - 1  # Subtract 1 for 0-based indexing
                    physical_end = end_page - offset - 1     # Subtract 1 for 0-based indexing
                    
                    if (0 <= physical_start < total_pages and  # Changed to 0-based comparison
                        0 <= physical_end < total_pages and    # Changed to 0-based comparison
                        physical_start <= physical_end):
                        
                        adjusted_sections.append({
                            "start_page": physical_start,  # Keep 0-based for pikepdf
                            "end_page": physical_end       # Keep 0-based for pikepdf
                        })
            except (ValueError, TypeError) as e:
                print(f"Skipping invalid section {section}: {e}")
                continue
    
    if not adjusted_sections:
        print("No valid sections after adjustment.")
        return
    
    print("\nAdjusted sections to extract:", json.dumps(adjusted_sections, indent=2))
    
    # Ask for user confirmation before extraction
    confirmation = input("\nWould you like to extract these sections? (yes/no): ").lower().strip()
    if confirmation != 'yes':
        print("Extraction cancelled.")
        return
    
    # Extract the sections after confirmation
    output_dir = os.path.expanduser(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print(f"\nExtracting to directory: {output_dir}")
    with pikepdf.Pdf.open(pdf_path) as doc:
        for i, section in enumerate(adjusted_sections, 1):
            start_page = section["start_page"]  # Already 0-based from adjustment
            end_page = section["end_page"]      # Already 0-based from adjustment

            base_name = os.path.splitext(os.path.basename(pdf_path))[0]
            for page_num in range(start_page, end_page + 1):  # +1 for inclusive range
                new_pdf = pikepdf.Pdf.new()
                new_pdf.pages.append(doc.pages[page_num])  # Use directly with 0-based index
                output_path = os.path.join(
                    output_dir,
                    f"{base_name}_section{i}_page{page_num+1}.pdf"  # +1 for human-readable naming
                )
                try:
                    new_pdf.save(output_path)
                    print(f"Extracted page {page_num+1} of section {i} to {output_path}")
                except Exception as err:
                    print(f"Error extracting page {page_num+1} of section {i}: {err}")
                finally:
                    new_pdf.close()

if __name__ == "__main__":
    pdf_path = "/home/jay/Nextcloud/DT/misc/Katharina Pistor - The Code of Capital_ How the Law Creates Wealth and Inequality (2019, Princeton University Press) [10.1515_9780691189437] - libgen.li.pdf"
    extract_reference_sections(pdf_path)