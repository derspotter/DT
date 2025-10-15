#!/usr/bin/env python3
"""
Extract Anhörung (hearing) and Bescheid (decision) pages from legal documents (Akten).
Uses GPT-5-mini to identify relevant pages and extracts them as separate PDFs.
"""

import os
import sys
import argparse
import pikepdf
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple
from dotenv import load_dotenv
import pydantic

# OpenAI imports
from openai import OpenAI

# Tenacity for retry logic
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type
)

# Load environment variables
load_dotenv()

# Pydantic models for structured responses
class PageRange(pydantic.BaseModel):
    """A range of pages identified in the document."""
    start_page: int = pydantic.Field(description="1-based physical page index where section starts")
    end_page: int = pydantic.Field(description="1-based physical page index where section ends")
    document_type: str = pydantic.Field(description="Type of document: 'Anhörung' or 'Bescheid'")
    confidence: float = pydantic.Field(ge=0.0, le=1.0, description="Confidence in identification")
    evidence: str = pydantic.Field(description="Text evidence supporting the identification")


class DocumentSections(pydantic.BaseModel):
    """Identified sections in the legal document."""
    sections: List[PageRange] = pydantic.Field(
        default_factory=list,
        description="List of identified Anhörung and Bescheid sections"
    )


@retry(
    wait=wait_random_exponential(min=1, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((Exception,))
)
def identify_sections_with_gpt5(client: OpenAI, pdf_path: str, total_pages: int) -> DocumentSections:
    """
    Use GPT-5-mini to identify Anhörung and Bescheid sections in the PDF.

    Args:
        client: OpenAI client instance
        pdf_path: Path to the PDF file
        total_pages: Total number of pages in the PDF

    Returns:
        DocumentSections with identified page ranges
    """
    filename = os.path.basename(pdf_path)
    print(f"📁 {filename}: Uploading to OpenAI for analysis...")

    # Upload the full PDF
    with open(pdf_path, "rb") as f:
        uploaded_file = client.files.create(file=f, purpose="user_data")

    print(f"✅ {filename}: Uploaded as {uploaded_file.id}")

    try:
        # Construct the prompt for GPT-5-mini
        prompt = f"""
Analyze this legal document (Akte) and identify all pages that contain:
1. **Anhörung** (hearing notice/hearing document)
2. **Bescheid** (official decision/ruling)

The document has {total_pages} total pages, numbered 1 to {total_pages}.

**INSTRUCTIONS:**
1. Look for documents titled or containing:
   - "Anhörung" / "Anhörungsschreiben" / "Hearing"
   - "Bescheid" / "Verwaltungsakt" / "Decision" / "Ruling"
2. **IMPORTANT: Use 1-based physical page indices** (first page = 1, second page = 2, etc.)
3. For each identified section, provide:
   - start_page: The 1-based page number where the document starts
   - end_page: The 1-based page number where the document ends
   - document_type: Either "Anhörung" or "Bescheid"
   - confidence: Your confidence level (0.0 to 1.0)
   - evidence: Brief text excerpt or description that confirms the identification

**WHAT TO LOOK FOR:**
- **Anhörung**: Usually contains phrases like:
  - "Anhörung gemäß..."
  - "Sie haben die Möglichkeit, sich zu äußern"
  - "Anhörungsschreiben"
  - References to legal provisions for hearing rights (e.g., "§ 28 VwVfG")
  - Often shorter, inviting statement or response

- **Bescheid**: Has a very specific formal structure:
  - **CRITICAL: Large, centered heading "BESCHEID" or "B E S C H E I D"**
  - Official letterhead (e.g., "Bundesamt für Migration und Flüchtlinge")
  - File reference numbers (Geschäfts-Z., AZR-Nummer)
  - Addressed to specific persons with birth details (geb. am...)
  - Formal decision phrase: "ergeht folgende Entscheidung" (the following decision is issued)
  - Numbered decision points (1., 2., 3., etc.) with specific outcomes:
    - "offensichtlich unbegründet abgelehnt" (manifestly unfounded rejected)
    - "wird zuerkannt/abgelehnt" (is granted/rejected)
    - References to specific laws (§ 60 Abs. 5, Aufenthaltsgesetz, etc.)
  - Legal remedies section ("Rechtsbehelfsbelehrung")
  - Information about deportation possibilities
  - Formal signatures and official stamps
  - Multiple pages typical

**OUTPUT FORMAT:**
Return a JSON structure with all identified sections. If no Anhörung or Bescheid is found, return an empty sections list.

**IMPORTANT:**
- Be conservative - only identify pages you're confident about (confidence > 0.7)
- If a document spans multiple pages, include all of them in the range
- The document might contain multiple Anhörungen or Bescheide - identify all of them
"""

        # Make API request using Responses API
        print(f"🔍 {filename}: Analyzing document with GPT-5-mini...")

        response = client.responses.parse(
            model="gpt-5-mini",
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_file", "file_id": uploaded_file.id},
                        {"type": "input_text", "text": prompt}
                    ]
                }
            ],
            text_format=DocumentSections,
            service_tier="flex"  # Use flex for cost savings
        )

        # Parse the structured response
        if hasattr(response, 'output_parsed') and response.output_parsed:
            sections = response.output_parsed
        elif hasattr(response, 'output_text') and response.output_text:
            sections = DocumentSections.model_validate_json(response.output_text)
        else:
            raise ValueError("No structured output found in response")

        # Display token usage if available
        if hasattr(response, 'usage') and response.usage:
            usage = response.usage
            input_tokens = getattr(usage, 'input_tokens', 0)
            output_tokens = getattr(usage, 'output_tokens', 0)
            total_tokens = getattr(usage, 'total_tokens', 0)

            # Calculate cost (GPT-5-mini flex pricing per 1M tokens)
            input_cost = (input_tokens / 1_000_000) * 0.125
            output_cost = (output_tokens / 1_000_000) * 1.00
            total_cost = input_cost + output_cost

            print(f"📊 {filename}: Token usage:")
            print(f"  ├─ Input tokens: {input_tokens:,}")
            print(f"  ├─ Output tokens: {output_tokens:,}")
            print(f"  ├─ Total tokens: {total_tokens:,}")
            print(f"  └─ Cost: ${total_cost:.4f}")

        print(f"✅ {filename}: Found {len(sections.sections)} section(s)")

        return sections

    finally:
        # Cleanup uploaded file
        try:
            client.files.delete(uploaded_file.id)
            print(f"🗑️ {filename}: Cleaned up uploaded file")
        except Exception as e:
            print(f"⚠️ {filename}: Warning - could not delete uploaded file: {e}")


def extract_pages(pdf_path: str, sections: DocumentSections, output_dir: str) -> List[str]:
    """
    Extract identified sections to separate PDF files.

    Args:
        pdf_path: Path to source PDF
        sections: Identified sections to extract
        output_dir: Directory to save extracted PDFs

    Returns:
        List of paths to extracted PDF files
    """
    if not sections.sections:
        print("ℹ️ No sections to extract")
        return []

    filename_base = Path(pdf_path).stem
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    extracted_files = []

    print(f"\n📄 Extracting sections from {os.path.basename(pdf_path)}...")

    with pikepdf.Pdf.open(pdf_path) as pdf_doc:
        total_pages = len(pdf_doc.pages)

        for i, section in enumerate(sections.sections, 1):
            # Convert 1-based to 0-based indices
            start_idx = section.start_page - 1
            end_idx = section.end_page - 1

            # Validate page ranges
            if start_idx < 0 or end_idx >= total_pages or start_idx > end_idx:
                print(f"⚠️ Section {i}: Invalid page range {section.start_page}-{section.end_page}, skipping")
                continue

            # Create output filename
            doc_type_clean = section.document_type.replace(" ", "_")
            output_filename = f"{filename_base}_{doc_type_clean}_p{section.start_page}-{section.end_page}.pdf"
            output_file = output_path / output_filename

            # Extract pages
            try:
                new_pdf = pikepdf.Pdf.new()
                for page_idx in range(start_idx, end_idx + 1):
                    new_pdf.pages.append(pdf_doc.pages[page_idx])

                new_pdf.save(output_file)
                extracted_files.append(str(output_file))

                print(f"  ✅ Section {i} ({section.document_type}):")
                print(f"     ├─ Pages: {section.start_page}-{section.end_page}")
                print(f"     ├─ Confidence: {section.confidence:.2f}")
                print(f"     ├─ Evidence: {section.evidence[:80]}...")
                print(f"     └─ Saved to: {output_filename}")

            except Exception as e:
                print(f"  ❌ Section {i}: Error extracting pages: {e}")

    return extracted_files


def process_akte(pdf_path: str, output_dir: str, api_key: str) -> Tuple[DocumentSections, List[str]]:
    """
    Process a legal document (Akte) to identify and extract Anhörung and Bescheid sections.

    Args:
        pdf_path: Path to the PDF file
        output_dir: Directory to save extracted PDFs
        api_key: OpenAI API key

    Returns:
        Tuple of (identified sections, list of extracted file paths)
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    filename = os.path.basename(pdf_path)
    print(f"\n{'='*60}")
    print(f"📋 Processing: {filename}")
    print(f"{'='*60}")

    start_time = time.time()

    # Get total page count
    with pikepdf.Pdf.open(pdf_path) as pdf_doc:
        total_pages = len(pdf_doc.pages)

    print(f"📄 Total pages: {total_pages}")

    # Initialize OpenAI client
    client = OpenAI(api_key=api_key, timeout=600.0)

    # Identify sections using GPT-5-mini
    sections = identify_sections_with_gpt5(client, pdf_path, total_pages)

    # Extract identified sections
    extracted_files = extract_pages(pdf_path, sections, output_dir)

    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"✅ Processing complete!")
    print(f"⏱️ Time: {elapsed:.1f} seconds")
    print(f"📁 Extracted {len(extracted_files)} file(s) to: {output_dir}")
    print(f"{'='*60}\n")

    return sections, extracted_files


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Extract Anhörung and Bescheid pages from legal documents using GPT-5-mini"
    )
    parser.add_argument(
        "pdf_path",
        help="Path to the PDF file (Akte) to process"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="extracted_documents",
        help="Output directory for extracted PDFs (default: extracted_documents)"
    )

    args = parser.parse_args()

    # Get API key from environment
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("❌ Error: OPENAI_API_KEY environment variable not set")
        print("Please set it in your .env file or environment")
        sys.exit(1)

    try:
        # Process the document
        sections, extracted_files = process_akte(
            args.pdf_path,
            args.output_dir,
            api_key
        )

        # Summary
        if extracted_files:
            print("📋 Summary of extracted documents:")
            for filepath in extracted_files:
                print(f"  ✓ {filepath}")
        else:
            print("ℹ️ No Anhörung or Bescheid sections found in the document")
            if sections.sections:
                print("\n⚠️ Sections were identified but had low confidence or invalid page ranges:")
                for section in sections.sections:
                    print(f"  • {section.document_type} (pages {section.start_page}-{section.end_page}, confidence: {section.confidence:.2f})")

    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
