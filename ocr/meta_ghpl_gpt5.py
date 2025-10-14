import os
from openai import OpenAI
import json
import pikepdf
import tempfile
import pydantic
from enum import Enum
import time
import logging
import traceback
import argparse
import glob
import pandas as pd
import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Any
import random
import threading

# Add tenacity for robust retry logic
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type
)

# Import rate limiting utilities (may not be needed with flex processing)
from utils import RateLimiter, wait_for_rate_limit

# Environment variables are loaded from shell (bashrc)

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Suppress httpx INFO logs (only show warnings and errors)
logging.getLogger("httpx").setLevel(logging.WARNING)
# Suppress OpenAI INFO logs
logging.getLogger("openai").setLevel(logging.WARNING)

# Global rate limiter for OpenAI API calls (GPT-5-mini: 500 RPM, 200K TPM)
OPENAI_RATE_LIMITER = RateLimiter(max_requests_per_minute=500, max_tokens_per_minute=200000)

# --- Pydantic Models Based on GHPL Glossary ---

# Import the proper enums and structured models from meta.py
from meta import (
    DocType, Creator, HealthTopic, GovernanceLevel,
    DocumentMetadata, StringFieldMetadata, IntFieldMetadata,
    DocTypeFieldMetadata, CreatorFieldMetadata, HealthTopicFieldMetadata, GovernanceLevelFieldMetadata
)

class RelevanceAssessment(pydantic.BaseModel):
    """Assessment of document relevance to GHPL scope using two boolean values."""
    is_health_policy_related: bool = pydantic.Field(description="True if document is from an authoritative health source (government, WHO, official health authorities, professional medical societies)")
    fits_ghpl_categories: bool = pydantic.Field(description="True if document fits one of the 6 GHPL categories (Policy, Law, National Health Strategy, National Control Plan, Action Plan, Guideline)")
    health_explanation: str = pydantic.Field(description="Brief explanation focusing on document authority and policy content")
    category_explanation: str = pydantic.Field(description="Brief explanation of which GHPL category it fits (or why it doesn't fit any)")
    health_confidence: float = pydantic.Field(ge=0.0, le=1.0, description="Confidence in health policy relevance assessment")
    category_confidence: float = pydantic.Field(ge=0.0, le=1.0, description="Confidence in GHPL category fit assessment")

class GHPLMetadataField(pydantic.BaseModel):
    """Metadata field following GHPL standards."""
    value: Optional[str] = pydantic.Field(None, description="The extracted value")
    confidence: float = pydantic.Field(0.0, ge=0.0, le=1.0, description="Confidence score")
    evidence: str = pydantic.Field("", description="Evidence supporting the extraction")
    source_page: Optional[int] = pydantic.Field(None, description="Source page number")
    alternatives: Optional[List[str]] = pydantic.Field(default_factory=list, description="Alternative values considered")

class GHPLDocumentMetadata(pydantic.BaseModel):
    """Enhanced metadata following GHPL glossary definitions."""
    # Core GHPL classification
    document_type: GHPLMetadataField = pydantic.Field(description="Document type according to GHPL glossary")
    health_focus: GHPLMetadataField = pydantic.Field(description="Primary health focus or disease area")
    
    # Administrative metadata
    title: GHPLMetadataField = pydantic.Field(description="Document title")
    country: GHPLMetadataField = pydantic.Field(description="Country of origin")
    year: GHPLMetadataField = pydantic.Field(description="Publication year")
    language: GHPLMetadataField = pydantic.Field(description="Primary language")
    
    # Authority and governance
    issuing_authority: GHPLMetadataField = pydantic.Field(description="Government body that issued/endorsed the document")
    governance_level: GHPLMetadataField = pydantic.Field(description="National, Regional, or Local level")
    
    # GHPL compliance
    officially_endorsed: GHPLMetadataField = pydantic.Field(description="Whether document is officially endorsed by government")
    
    # Quality scores
    overall_confidence: Optional[float] = pydantic.Field(None, ge=0.0, le=1.0)
    metadata_completeness: Optional[float] = pydantic.Field(None, ge=0.0, le=1.0)


def upload_pdf_subset(client, pdf_path, first_pages=10, last_pages=5):
    """Extract and upload a subset of PDF pages to OpenAI."""
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"The file '{pdf_path}' was not found.")
    
    # Get filename for consistent logging
    filename = os.path.basename(pdf_path)
    
    # Get file size for logging
    file_size = os.path.getsize(pdf_path)
    file_size_mb = file_size / (1024 * 1024)
    print(f"📁 {filename}: Extracting subset from PDF ({file_size_mb:.2f} MB)")
    print(f"   {filename}: Taking first {first_pages} and last {last_pages} pages...")
    
    temp_first_path = None
    temp_last_path = None
    uploaded_first = None
    uploaded_last = None
    
    try:
        # Create temporary files
        with tempfile.NamedTemporaryFile(delete=False, suffix="_first.pdf") as temp_f:
            temp_first_path = temp_f.name
        with tempfile.NamedTemporaryFile(delete=False, suffix="_last.pdf") as temp_f:
            temp_last_path = temp_f.name
        
        # Extract pages using pikepdf
        with pikepdf.open(pdf_path) as source_pdf:
            total_pages = len(source_pdf.pages)
            print(f"   {filename}: Total pages in PDF: {total_pages}")
            
            if total_pages == 0:
                print(f"❌ {filename}: PDF has 0 pages")
                return None, None
            
            # Create first pages PDF
            first_pdf = pikepdf.Pdf.new()
            pages_to_extract = min(total_pages, first_pages)
            for i in range(pages_to_extract):
                first_pdf.pages.append(source_pdf.pages[i])
            first_pdf.save(temp_first_path)
            print(f"   ✅ {filename}: Extracted first {pages_to_extract} pages")
            
            # Create last pages PDF (if document is long enough)
            last_pdf = pikepdf.Pdf.new()
            if total_pages > first_pages:
                start_index = max(first_pages, total_pages - last_pages)  # Avoid overlap
                pages_extracted = 0
                for i in range(start_index, total_pages):
                    last_pdf.pages.append(source_pdf.pages[i])
                    pages_extracted += 1
                last_pdf.save(temp_last_path)
                print(f"   ✅ {filename}: Extracted last {pages_extracted} pages")
            else:
                # Document too short - no separate last pages needed
                temp_last_path = None
                print(f"   📝 {filename}: Document too short - using only first pages")
        
        # Upload both files using tenacity for retries
        print(f"📤 {filename}: Uploading PDF subsets to OpenAI...")
        
        try:
            uploaded_first, uploaded_last = upload_files_with_retry(client, temp_first_path, temp_last_path, filename)
        except Exception as e:
            print(f"❌ {filename}: Failed to upload PDF subsets: {e}")
            return None, None
        
        return uploaded_first, uploaded_last
        
    except Exception as e:
        print(f"❌ {filename}: Error during PDF processing: {e}")
        logger.error(f"PDF processing error for {filename}: {e}")
        return None, None
        
    finally:
        # Cleanup temporary files
        if temp_first_path and os.path.exists(temp_first_path):
            os.remove(temp_first_path)
        if temp_last_path and os.path.exists(temp_last_path):
            os.remove(temp_last_path)


@retry(
    wait=wait_random_exponential(min=1, max=60),  # Wait 1-60 seconds with exponential backoff
    stop=stop_after_attempt(6),  # Try up to 6 times
    retry=retry_if_exception_type((Exception,))  # Retry on any exception
)
def api_call_with_tenacity(client, request_params, filename="", fallback_to_standard=False):
    """
    Make API call with tenacity retry logic for robust error handling.
    Handles: Rate limits (429), timeouts, resource unavailable, etc.
    Returns tuple of (parsed_response, rate_limit_headers)
    """
    # Apply rate limiting before making the API call
    wait_for_rate_limit(OPENAI_RATE_LIMITER, f"OpenAI API call ({filename})" if filename else "OpenAI API call")
    
    # Modify request for fallback
    if fallback_to_standard:
        request_params["service_tier"] = "auto"  # Standard processing
        print(f"   {filename}: Using standard processing for reliability..." if filename else "   Using standard processing for reliability...")
    
    # Use with_raw_response to capture headers
    raw_response = client.with_options(timeout=900.0).responses.with_raw_response.parse(**request_params)
    
    # Extract rate limit headers
    rate_headers = {}
    for header_key in ['x-ratelimit-limit-requests', 'x-ratelimit-remaining-requests', 
                       'x-ratelimit-reset-requests', 'x-ratelimit-limit-tokens',
                       'x-ratelimit-remaining-tokens', 'x-ratelimit-reset-tokens']:
        if header_key in raw_response.headers:
            rate_headers[header_key] = raw_response.headers[header_key]
    
    # Parse the response body
    parsed_response = raw_response.parse()
    
    return parsed_response, rate_headers


def make_api_request_with_retry(client, request_params, filename=""):
    """Wrapper for API requests with tenacity retry logic and rate limiting.
    Returns tuple of (parsed_response, rate_limit_headers)"""
    try:
        return api_call_with_tenacity(client, request_params, filename)
    except Exception as e:
        # If all retries fail, try once with standard processing (no additional retries)
        print(f"⚡ {filename}: Final fallback: trying standard processing once after all flex retries failed" if filename else "⚡ Final fallback: trying standard processing once after all flex retries failed")
        try:
            # Direct API call without tenacity decorator for fallback
            wait_for_rate_limit(OPENAI_RATE_LIMITER, f"OpenAI API call (fallback - {filename})" if filename else "OpenAI API call (fallback)")
            request_params_fallback = request_params.copy()
            request_params_fallback["service_tier"] = "auto"  # Standard processing
            print(f"   {filename}: Using standard processing for reliability..." if filename else "   Using standard processing for reliability...")
            
            # Use with_raw_response for fallback too
            raw_response = client.with_options(timeout=900.0).responses.with_raw_response.parse(**request_params_fallback)
            
            # Extract rate limit headers
            rate_headers = {}
            for header_key in ['x-ratelimit-limit-requests', 'x-ratelimit-remaining-requests', 
                               'x-ratelimit-reset-requests', 'x-ratelimit-limit-tokens',
                               'x-ratelimit-remaining-tokens', 'x-ratelimit-reset-tokens']:
                if header_key in raw_response.headers:
                    rate_headers[header_key] = raw_response.headers[header_key]
            
            # Parse the response body
            parsed_response = raw_response.parse()
            
            return parsed_response, rate_headers
        except Exception as final_error:
            print(f"❌ {filename}: All retry attempts failed, including fallback: {final_error}" if filename else f"❌ All retry attempts failed, including fallback: {final_error}")
            raise final_error


def process_document_with_chat(client, first_pages_file, last_pages_file, pdf_path, use_flex=True):
    """
    Process document with GPT-5-mini using OpenAI Responses API with flex processing.
    """
    model_name = 'gpt-5-mini'
    pdf_filename = os.path.basename(pdf_path)
    
    # Start timing the entire document processing
    processing_start_time = time.time()
    
    try:
        print(f"🔍 {pdf_filename}: Processing document with GPT-5-mini (flex)")
        
        # QUESTION 1: Health policy relevance and GHPL category fit
        print(f"❓ {pdf_filename}: Question 1: Assessing health policy relevance...")
        
        question_1 = """
        I need you to answer two separate boolean questions about this document:

        **Question A: Is this document from an authoritative health source?**
        - TRUE if from: Government agencies, ministries, parliaments, national public health institutes, official health authorities, professional medical societies/associations that set standards
        - FALSE if from:  WHO, UN agencies, Commercial companies, individual hospitals/clinics, pure academic institutions without policy mandate, news outlets, blogs, individual authors
        - Edge cases: NGOs/foundations can be TRUE if they work closely with government or have quasi-official status

        **Question B: Does it fit into one of the 6 GHPL document categories?**
        
        ⚠️ **IMPORTANT**: The document title/filename is NOT a definitive indicator of document type. Focus on the actual content, structure, and purpose of the document, not just its name.
        
        Look for these SPECIFIC document types and characteristics based on the official GHPL Glossary:
        
        1. **POLICY**: A formal, high-level statement by a government body that:
           - Defines goals, priorities, and parameters for action
           - Establishes principles to guide decisions
           - Sets a vision for the future
           - Outlines priorities and the general roles of different stakeholders
           - **Synonyms**: Policy framework, policy statement, strategic policy document
           - **Examples**: "National Policy on...", "Policy Framework for...", "Health Policy..."
           - **NOT**: Research papers discussing policy, announcements about policies, policy briefs, implementation plans
           - **This document type should only be considered, when none of the others seems fitting**
        
        2. **LAW**: Legal instruments that:
           - Create binding rules or regulations with legal force
           - Are acts, statutes, decrees, regulations, bylaws, or legal codes
           - Have enforcement mechanisms and penalties
           - Are passed by legislative bodies or authorized agencies
           - **Synonyms**: Act, statute, decree, regulation, bylaw, legal code, ordinance
           - **Examples**: "Public Health Act", "Tobacco Control Regulations", "Health Insurance Law"
           - **NOT**: Explanations of laws, legal analysis papers, draft legislation
        
        3. **NATIONAL HEALTH STRATEGY**: Comprehensive strategic documents that:
           - Provide a model for the entire health sector of a country
           - Cover broad, long-term strategic lines of action
           - Address the whole health system or major components thereof
           - Set overarching goals and strategic directions
           - **Synonyms**: Health strategic plan, national health plan, health sector strategy
           - **Examples**: "National Health Strategic Plan 2020-2025", "Vision 2030 Health Strategy"
           - **NOT**: Disease-specific strategies, narrow topic strategies, research agendas without implementation
        
        4. **NATIONAL CONTROL PLAN**: Strategic plans that:
           - Focus on controlling a specific disease or health problem at national/regional level
           - Have clear goals, targets, and implementation strategies
           - Include specific interventions and resource allocation
           - Are developed for disease control or health problem management
           - **Synonyms**: Control program, strategic plan for [disease], elimination plan, prevention plan
           - **Examples**: "National Cancer Control Programme", "National HIV/AIDS Strategic Plan", "Malaria Elimination Strategy"
           - **NOT**: Clinical studies, disease surveillance reports without strategy, research protocols
        
        5. **ACTION PLAN**: Implementation-focused documents that:
           - Outline specific steps and activities to implement a higher-level policy or strategy
           - Include detailed timelines, responsibilities, and resource allocation
           - Have concrete, measurable actions and deliverables
           - Serve as operational blueprints for implementation
           - **Synonyms**: Implementation plan, operational plan, work plan, execution plan
           - **Examples**: "Implementation Plan for...", "Operational Plan", "Action Plan for..."
           - **NOT**: High-level strategies, project proposals, organizational work plans
        
        6. **GUIDELINE**: Evidence-based documents that:
           - Provide formal advisory statements for health interventions
           - Guide clinical practice or public health practice
           - Are systematically developed with evidence review processes
           - Offer recommendations based on scientific evidence
           - **Synonyms**: Clinical guidelines, practice guidelines, standard treatment guidelines, clinical protocols, standard operating procedures
           - **Examples**: "Clinical Practice Guidelines", "Standard Treatment Guidelines", "WHO Guidelines"
           - **NOT**: Patient education materials, training manuals, fact sheets, FAQs, informal guidance

        **CRITICAL: Documents that do NOT qualify:**
        - Pure data reports/briefs (even from CDC/government) without policy content
        - Research papers/editorials/commentaries (even if discussing policy)
        - Educational materials/brochures for patients or public
        - Assessment/evaluation reports (unless they contain the actual policy/strategy)
        - Meeting reports, conference proceedings, presentations
        - Newsletters, bulletins, announcements
        - Product information, technical specifications
        - Training materials, toolkits, frameworks (unless official guidelines)
        - FAQs, fact sheets, information sheets
        
        **Real examples of documents that should be REJECTED:**
        - "Data Brief No. 50" → Statistical report, not policy
        - "Guest Editorial" in journal → Commentary, not policy
        - "Blood pressure brochure" → Patient education, not guideline
        - "FAQ on Delta-8 THC" → Information sheet, not policy
        - "Institutional Capacity Assessment" → Assessment report, not the strategy itself
        - "CovidConnect slides" → Presentation, not official document
        
        **Real examples of documents that should be ACCEPTED:**
        - "Rwanda National Cancer Control Plan" → National Control Plan
        - "Guidelines for the Use of Antiretroviral Agents" → Guideline
        - "Cardiovascular Disease Outcomes Strategy" → National Control Plan
        - "National Technical Guidelines for Integrated Disease Surveillance" → Guideline
        - "Kenya National Strategy for NCDs" → National Control Plan

        **Look for these positive indicators:**
        - Official seal, logo, or letterhead from government/authority
        - Formal approval statement or ministerial foreword
        - Version control, official reference numbers
        - Clear effective dates or implementation timelines
        - Structured format typical of official documents
        - Legal or regulatory language

        Return your assessment using the RelevanceAssessment schema.
        """
        
        # Build content array for OpenAI API
        content = [
            {"type": "input_file", "file_id": first_pages_file.id},
        ]
        if last_pages_file:
            content.append({"type": "input_file", "file_id": last_pages_file.id})
        content.append({"type": "input_text", "text": question_1})
        
        # Build request parameters for Question 1 (Responses API with PDF files)
        content_items = [
            {"type": "input_file", "file_id": first_pages_file.id},
            {"type": "input_text", "text": question_1}
        ]
        
        # Add last pages if exists
        if last_pages_file:
            content_items.insert(1, {"type": "input_file", "file_id": last_pages_file.id})
        
        request_params_q1 = {
            "model": model_name,
            "input": [
                {
                    "role": "user",
                    "content": content_items
                }
            ],
            "text_format": RelevanceAssessment
        }
        
        # Add service tier if using flex
        if use_flex:
            request_params_q1["service_tier"] = "flex"
        
        # Make API request with retry logic (now returns tuple)
        response_1, rate_headers_1 = make_api_request_with_retry(client, request_params_q1, pdf_filename)
        
        # Display rate limit headers - COMMENTED OUT due to unreliable values
        # The headers appear to show per-second limits with very short reset windows (120ms)
        # rather than the expected per-minute limits, so we rely on local tracking instead
        # if rate_headers_1:
        #     print(f"\n📊 OpenAI Rate Limit Headers (Question 1):")
        #     if 'x-ratelimit-limit-requests' in rate_headers_1:
        #         print(f"  ├─ Request limit: {rate_headers_1['x-ratelimit-limit-requests']}")
        #     if 'x-ratelimit-remaining-requests' in rate_headers_1:
        #         print(f"  ├─ Remaining requests: {rate_headers_1['x-ratelimit-remaining-requests']}")
        #     if 'x-ratelimit-reset-requests' in rate_headers_1:
        #         print(f"  ├─ Requests reset: {rate_headers_1['x-ratelimit-reset-requests']}")
        #     if 'x-ratelimit-limit-tokens' in rate_headers_1:
        #         print(f"  ├─ Token limit: {rate_headers_1['x-ratelimit-limit-tokens']}")
        #     if 'x-ratelimit-remaining-tokens' in rate_headers_1:
        #         print(f"  ├─ Remaining tokens: {rate_headers_1['x-ratelimit-remaining-tokens']}")
        #     if 'x-ratelimit-reset-tokens' in rate_headers_1:
        #         print(f"  └─ Tokens reset: {rate_headers_1['x-ratelimit-reset-tokens']}")
        # else:
        #     print("\n⚠️ No rate limit headers captured")
        
        # Track token usage for Question 1
        q1_tokens = {
            'prompt_tokens': 0,
            'output_tokens': 0,
            'reasoning_tokens': 0,
            'total_tokens': 0,
            'cost': 0.0
        }
        
        # Token usage tracking for Question 1
        
        if hasattr(response_1, 'usage') and response_1.usage:
            usage = response_1.usage
            # Use correct Responses API attribute names
            q1_tokens['prompt_tokens'] = getattr(usage, 'input_tokens', 0)
            q1_tokens['output_tokens'] = getattr(usage, 'output_tokens', 0)
            q1_tokens['total_tokens'] = getattr(usage, 'total_tokens', 0)
            
            # Get reasoning tokens from output_tokens_details if available
            if hasattr(usage, 'output_tokens_details') and usage.output_tokens_details:
                q1_tokens['reasoning_tokens'] = getattr(usage.output_tokens_details, 'reasoning_tokens', 0)
            else:
                q1_tokens['reasoning_tokens'] = 0
            
            # Calculate cost (GPT-5-mini with flex/standard processing)
            # Official GPT-5-mini pricing (per 1M tokens):
            if use_flex:
                input_rate = 0.125   # $0.125 per 1M tokens (flex)
                output_rate = 1.00   # $1.00 per 1M tokens (flex)
            else:
                input_rate = 0.25    # $0.25 per 1M tokens (standard)
                output_rate = 2.00   # $2.00 per 1M tokens (standard)
            reasoning_rate = output_rate  # Reasoning tokens billed as output tokens
            
            input_cost = (q1_tokens['prompt_tokens'] / 1_000_000) * input_rate
            output_cost = (q1_tokens['output_tokens'] / 1_000_000) * output_rate
            reasoning_cost = (q1_tokens['reasoning_tokens'] / 1_000_000) * reasoning_rate
            q1_tokens['cost'] = input_cost + output_cost + reasoning_cost
            
            print(f"\n📊 {pdf_filename}: Question 1 Token Usage (GPT-5-mini flex):")
            print(f"  ├─ Input tokens: {q1_tokens['prompt_tokens']:,}")
            print(f"  ├─ Output tokens: {q1_tokens['output_tokens']:,}")
            if q1_tokens['reasoning_tokens'] > 0:
                print(f"  ├─ Reasoning tokens: {q1_tokens['reasoning_tokens']:,}")
            print(f"  ├─ Total tokens: {q1_tokens['total_tokens']:,}")
            print(f"  └─ Cost: ${q1_tokens['cost']:.6f} ({'flex' if use_flex else 'standard'} pricing)")
            
            # Record token usage for rate limiting
            OPENAI_RATE_LIMITER.record_token_usage(q1_tokens['total_tokens'])
        
        # Parse structured response from Responses API
        try:
            # responses.parse() returns structured data in output_parsed
            if hasattr(response_1, 'output_parsed') and response_1.output_parsed:
                assessment = response_1.output_parsed
            else:
                # Fallback to parsing from output_text if available
                if hasattr(response_1, 'output_text') and response_1.output_text:
                    assessment = RelevanceAssessment.model_validate_json(response_1.output_text)
                else:
                    raise ValueError("No structured output found in response")
            
            print(f"🔍 {pdf_filename}: Response to Question 1:")
            print(f"  ├─ Health Policy Related: {'✅ YES' if assessment.is_health_policy_related else '❌ NO'}")
            print(f"  │  └─ {assessment.health_explanation} (confidence: {assessment.health_confidence:.2f})")
            print(f"  ├─ Fits GHPL Categories: {'✅ YES' if assessment.fits_ghpl_categories else '❌ NO'}")
            print(f"  │  └─ {assessment.category_explanation} (confidence: {assessment.category_confidence:.2f})")
            
            # Both must be TRUE to proceed
            if not assessment.is_health_policy_related or not assessment.fits_ghpl_categories:
                print(f"\n{pdf_filename}: " + "=" * 60)
                print(f"🚫 {pdf_filename}: DOCUMENT NOT SUITABLE FOR GHPL PROCESSING")
                print(f"{pdf_filename}: " + "=" * 60)
                if not assessment.is_health_policy_related:
                    print(f"{pdf_filename}: Reason: Not health policy related")
                elif not assessment.fits_ghpl_categories:
                    print(f"{pdf_filename}: Reason: Health policy related but doesn't fit GHPL categories")
                print(f"{pdf_filename}: No metadata extraction will be performed.")
                
                # Calculate timing even for rejected documents
                processing_end_time = time.time()
                total_duration = processing_end_time - processing_start_time
                
                print(f"\n⏱️ {pdf_filename}: PERFORMANCE METRICS (Rejected Document):")
                print(f"  ├─ Processing time: {total_duration:.2f} seconds")
                print(f"  ├─ API calls made: 1 (relevance assessment only)")
                print(f"  └─ API calls per minute: {60/total_duration:.1f}")
                
                # Return structured JSON for CSV export
                assessment_json = {
                    'is_health_policy_related': assessment.is_health_policy_related,
                    'health_confidence': assessment.health_confidence,
                    'health_explanation': assessment.health_explanation,
                    'fits_ghpl_categories': assessment.fits_ghpl_categories,
                    'category_confidence': assessment.category_confidence,
                    'category_explanation': assessment.category_explanation
                }
                
                # Return with only Q1 cost if Q2 failed
                return None, json.dumps(assessment_json), q1_tokens['cost']
                
        except Exception as e:
            print(f"❌ {pdf_filename}: Failed to parse structured response: {e}")
            raw_response = getattr(response_1, 'output_text', str(response_1))
            print(f"{pdf_filename}: Raw response: {raw_response}")
            # For parsing errors, return the raw text (will be handled in CSV export)
            return None, str(raw_response), q1_tokens['cost']
        
        # QUESTION 2: Extract detailed metadata
        print(f"\n❓ {pdf_filename}: Question 2: Extracting detailed metadata...")
        
        question_2 = f"""
        Great! Since this document is health policy related and fits GHPL categories, please extract detailed metadata using the proper enum-based structure.

        For each metadata field, provide the appropriate field type with:
        1. **value**: The extracted information (or null if not found)
        2. **confidence**: A score from 0.0 to 1.0 based on:
           - 1.0: Explicitly stated with clear labeling (e.g., "Title: [value]")
           - 0.8-0.9: Clearly visible but not explicitly labeled
           - 0.6-0.7: Inferred from prominent placement or formatting
           - 0.4-0.5: Inferred from context clues
           - 0.2-0.3: Educated guess based on document structure
           - 0.0-0.1: No evidence found
        3. **evidence**: Specific text or location that supports your extraction
        4. **source_page**: Which page contained this information
        5. **alternatives**: Other possible values you considered (as a list)

        ⚠️ CRITICAL ENUM REQUIREMENTS - THESE MUST BE FOLLOWED EXACTLY:
        
        **doc_type** MUST be EXACTLY one of these values (no other values allowed):
        - "Policy"
        - "Law" 
        - "National Health Strategy"
        - "National Control Plan"
        - "Action Plan"
        - "Health Guideline"
        
        ⚠️ **CRITICAL**: The document title/filename is NOT a definitive indicator of document type. You must analyze the actual content, structure, and purpose of the document, not just its name. A document called "Policy" might actually be a guideline, and a document called "Guidelines" might actually be a policy.
        
        **DETAILED DEFINITIONS FOR doc_type (from official GHPL Glossary):**
        
        1. **POLICY**: A formal, high-level statement by a government body that:
           - Defines goals, priorities, and parameters for action
           - Establishes principles to guide decisions
           - Sets a vision for the future
           - Outlines priorities and the general roles of different stakeholders
           - **Synonyms**: Policy framework, policy statement, strategic policy document
           - **Examples**: "National Policy on...", "Policy Framework for...", "Health Policy..."
           - **NOT**: Research papers discussing policy, announcements about policies, policy briefs, implementation plans

        2. **LAW**: Legal instruments that:
           - Create binding rules or regulations with legal force
           - Are acts, statutes, decrees, regulations, bylaws, or legal codes
           - Have enforcement mechanisms and penalties
           - Are passed by legislative bodies or authorized agencies
           - **Synonyms**: Act, statute, decree, regulation, bylaw, legal code, ordinance
           - **Examples**: "Public Health Act", "Tobacco Control Regulations", "Health Insurance Law"
           - **NOT**: Explanations of laws, legal analysis papers, draft legislation

        3. **NATIONAL HEALTH STRATEGY**: Comprehensive strategic documents that:
           - Provide a model for the entire health sector of a country
           - Cover broad, long-term strategic lines of action
           - Address the whole health system or major components thereof
           - Set overarching goals and strategic directions
           - **Synonyms**: Health strategic plan, national health plan, health sector strategy
           - **Examples**: "National Health Strategic Plan 2020-2025", "Vision 2030 Health Strategy"
           - **NOT**: Disease-specific strategies, narrow topic strategies, research agendas without implementation

        4. **NATIONAL CONTROL PLAN**: Strategic plans that:
           - Focus on controlling a specific disease or health problem at national/regional level
           - Have clear goals, targets, and implementation strategies
           - Include specific interventions and resource allocation
           - Are developed for disease control or health problem management
           - **Synonyms**: Control program, strategic plan for [disease], elimination plan, prevention plan
           - **Examples**: "National Cancer Control Programme", "National HIV/AIDS Strategic Plan", "Malaria Elimination Strategy"
           - **NOT**: Clinical studies, disease surveillance reports without strategy, research protocols

        5. **ACTION PLAN**: Implementation-focused documents that:
           - Outline specific steps and activities to implement a higher-level policy or strategy
           - Include detailed timelines, responsibilities, and resource allocation
           - Have concrete, measurable actions and deliverables
           - Serve as operational blueprints for implementation
           - **Synonyms**: Implementation plan, operational plan, work plan, execution plan
           - **Examples**: "Implementation Plan for...", "Operational Plan", "Action Plan for..."
           - **NOT**: High-level strategies, project proposals, organizational work plans

        6. **HEALTH GUIDELINE**: Evidence-based documents that:
           - Provide formal advisory statements for health interventions
           - Guide clinical practice or public health practice
           - Are systematically developed with evidence review processes
           - Offer recommendations based on scientific evidence
           - **Synonyms**: Clinical guidelines, practice guidelines, standard treatment guidelines, clinical protocols, standard operating procedures
           - **Examples**: "Clinical Practice Guidelines", "Standard Treatment Guidelines", "WHO Guidelines"
           - **NOT**: Patient education materials, training manuals, fact sheets, FAQs, informal guidance
        
        **health_topic** MUST be EXACTLY one of these values (no other values allowed):
        - "Cancer": for cancer-related documents (oncology, cancer screening, cancer treatment)
        - "Cardiovascular Health": for heart and blood vessel diseases (hypertension, heart disease, stroke, etc.)
        - "Non-Communicable Disease": ONLY if the document covers BOTH cancer AND cardiovascular diseases together
        
        **creator** MUST be EXACTLY one of these values (no other values allowed):
        - "Parliament"
        - "Ministry"
        - "Agency"
        - "Foundation"
        - "Association"
        - "Society"
        
        **level** MUST be EXACTLY one of these values (no other values allowed):
        - "National": Covers an entire sovereign nation
        - "Regional": Covers a state, province, or constituent country within a nation (e.g., Wales, Scotland, California, Ontario)
        - "International": Covers multiple sovereign nations
        
        **Examples for governance level classification:**
        - **National**: UK Parliament law, US Federal policy, German Bundestag policy
        - **Regional**: Welsh Government policy, Scottish Government policy, California state policy, Ontario provincial policy
        - **International**: WHO guidelines, EU directives, UN conventions, NAACCR standards (North America), PAHO policies (Americas)
        
        **CRITICAL: Documents that do NOT qualify:**
        - Pure data reports/briefs (even from CDC/government) without policy content
        - Research papers/editorials/commentaries (even if discussing policy)
        - Educational materials/brochures for patients or public
        - Assessment/evaluation reports (unless they contain the actual policy/strategy)
        - Meeting reports, conference proceedings, presentations
        - Newsletters, bulletins, announcements
        - Product information, technical specifications
        - Training materials, toolkits, frameworks (unless official guidelines)
        - FAQs, fact sheets, information sheets
        
        **Real examples of documents that should be REJECTED:**
        - "Data Brief No. 50" → Statistical report, not policy
        - "Guest Editorial" in journal → Commentary, not policy
        - "Blood pressure brochure" → Patient education, not guideline
        - "FAQ on Delta-8 THC" → Information sheet, not policy
        - "Institutional Capacity Assessment" → Assessment report, not the strategy itself
        - "CovidConnect slides" → Presentation, not official document
        
        **Real examples of documents that should be ACCEPTED:**
        - "Rwanda National Cancer Control Plan" → National Control Plan
        - "Guidelines for the Use of Antiretroviral Agents" → Guideline
        - "Cardiovascular Disease Outcomes Strategy" → National Control Plan
        - "National Technical Guidelines for Integrated Disease Surveillance" → Guideline
        - "Kenya National Strategy for NCDs" → National Control Plan

        **Look for these positive indicators:**
        - Official seal, logo, or letterhead from government/authority
        - Formal approval statement or ministerial foreword
        - Version control, official reference numbers
        - Clear effective dates or implementation timelines
        - Structured format typical of official documents
        - Legal or regulatory language
        
        If you cannot determine which enum value applies, set the value to null rather than guessing or creating new values.
        
        Other fields (title, country, language, year) can contain any appropriate values.
        
        ⚠️ TITLE FORMATTING REQUIREMENT:
        For the **title** field, convert ALL CAPS text to proper title case. For example:
        - "UPDATED MANAGEMENT OF HYPERTENSION IN ADULTS" → "Updated Management of Hypertension in Adults"
        - "NATIONAL HEALTH STRATEGY FOR CANCER" → "National Health Strategy for Cancer"
        Extract the meaningful title content, not just formatting artifacts.

        The source filename is: {pdf_filename}
        
        Return the complete DocumentMetadata object using the correct field types.
        """
        
        # Build request parameters for Question 2 (Responses API with PDF files)
        content_items_q2 = [
            {"type": "input_file", "file_id": first_pages_file.id},
            {"type": "input_text", "text": question_2}
        ]
        
        # Add last pages if exists
        if last_pages_file:
            content_items_q2.insert(1, {"type": "input_file", "file_id": last_pages_file.id})
        
        request_params_q2 = {
            "model": model_name,
            "input": [
                {
                    "role": "user",
                    "content": content_items_q2
                }
            ],
            "text_format": DocumentMetadata
        }
        
        # Add service tier if using flex
        if use_flex:
            request_params_q2["service_tier"] = "flex"
        
        # Make API request with retry logic (now returns tuple)
        response_2, rate_headers_2 = make_api_request_with_retry(client, request_params_q2, pdf_filename)
        
        # Display rate limit headers for Question 2 - COMMENTED OUT (see above)
        # if rate_headers_2:
        #     print(f"\n📊 OpenAI Rate Limit Headers (Question 2):")
        #     if 'x-ratelimit-remaining-requests' in rate_headers_2:
        #         print(f"  ├─ Remaining requests: {rate_headers_2['x-ratelimit-remaining-requests']}")
        #     if 'x-ratelimit-remaining-tokens' in rate_headers_2:
        #         print(f"  └─ Remaining tokens: {rate_headers_2['x-ratelimit-remaining-tokens']}")
        
        # Track token usage for Question 2
        q2_tokens = {
            'prompt_tokens': 0,
            'output_tokens': 0,
            'reasoning_tokens': 0,
            'total_tokens': 0,
            'cost': 0.0
        }
        
        if hasattr(response_2, 'usage') and response_2.usage:
            usage = response_2.usage
            # Use correct Responses API attribute names
            q2_tokens['prompt_tokens'] = getattr(usage, 'input_tokens', 0)
            q2_tokens['output_tokens'] = getattr(usage, 'output_tokens', 0)
            q2_tokens['total_tokens'] = getattr(usage, 'total_tokens', 0)
            
            # Get reasoning tokens from output_tokens_details if available
            if hasattr(usage, 'output_tokens_details') and usage.output_tokens_details:
                q2_tokens['reasoning_tokens'] = getattr(usage.output_tokens_details, 'reasoning_tokens', 0)
            else:
                q2_tokens['reasoning_tokens'] = 0
            
            # Calculate cost (GPT-5-mini with flex/standard processing)
            # Official GPT-5-mini pricing (per 1M tokens):
            if use_flex:
                input_rate = 0.125   # $0.125 per 1M tokens (flex)
                output_rate = 1.00   # $1.00 per 1M tokens (flex)
            else:
                input_rate = 0.25    # $0.25 per 1M tokens (standard)
                output_rate = 2.00   # $2.00 per 1M tokens (standard)
            reasoning_rate = output_rate  # Reasoning tokens billed as output tokens
            
            input_cost = (q2_tokens['prompt_tokens'] / 1_000_000) * input_rate
            output_cost = (q2_tokens['output_tokens'] / 1_000_000) * output_rate
            reasoning_cost = (q2_tokens['reasoning_tokens'] / 1_000_000) * reasoning_rate
            q2_tokens['cost'] = input_cost + output_cost + reasoning_cost
            
            print(f"\n📊 {pdf_filename}: Question 2 Token Usage (GPT-5-mini flex):")
            print(f"  ├─ Input tokens: {q2_tokens['prompt_tokens']:,}")
            print(f"  ├─ Output tokens: {q2_tokens['output_tokens']:,}")
            if q2_tokens['reasoning_tokens'] > 0:
                print(f"  ├─ Reasoning tokens: {q2_tokens['reasoning_tokens']:,}")
            print(f"  ├─ Total tokens: {q2_tokens['total_tokens']:,}")
            print(f"  └─ Cost: ${q2_tokens['cost']:.6f} ({'flex' if use_flex else 'standard'} pricing)")
            
            # Record token usage for rate limiting
            OPENAI_RATE_LIMITER.record_token_usage(q2_tokens['total_tokens'])
        
        # Calculate and display combined totals
        combined_tokens = {
            'prompt_tokens': q1_tokens['prompt_tokens'] + q2_tokens['prompt_tokens'],
            'output_tokens': q1_tokens['output_tokens'] + q2_tokens['output_tokens'],
            'reasoning_tokens': q1_tokens['reasoning_tokens'] + q2_tokens['reasoning_tokens'],
            'total_tokens': q1_tokens['total_tokens'] + q2_tokens['total_tokens'],
            'cost': q1_tokens['cost'] + q2_tokens['cost']
        }
        
        print(f"\n📊 {pdf_filename}: Combined Total Usage (GPT-5-mini flex):")
        print(f"  ├─ Input tokens: {combined_tokens['prompt_tokens']:,}")
        print(f"  ├─ Output tokens: {combined_tokens['output_tokens']:,}")
        if combined_tokens['reasoning_tokens'] > 0:
            print(f"  ├─ Reasoning tokens: {combined_tokens['reasoning_tokens']:,}")
        print(f"  ├─ Total tokens: {combined_tokens['total_tokens']:,}")
        print(f"  ├─ Total cost: ${combined_tokens['cost']:.6f} ({'flex' if use_flex else 'standard'})")
        if use_flex:
            # Calculate what standard pricing would cost (2x input, 2x output)
            standard_cost = combined_tokens['cost'] * 2  # Approximate 2x multiplier
            print(f"  └─ (~{combined_tokens['cost'] * 100:.2f} cents vs ~{standard_cost * 100:.2f} cents standard)")
        else:
            # Calculate what flex pricing would cost (0.5x input, 0.5x output)  
            flex_cost = combined_tokens['cost'] * 0.5  # Approximate 0.5x multiplier
            print(f"  └─ (~{combined_tokens['cost'] * 100:.2f} cents vs ~{flex_cost * 100:.2f} cents flex)")
        
        # Parse metadata using Pydantic model validation
        try:
            # responses.parse() returns structured data in output_parsed
            if hasattr(response_2, 'output_parsed') and response_2.output_parsed:
                metadata = response_2.output_parsed
            else:
                # Fallback to parsing from output_text if available
                if hasattr(response_2, 'output_text') and response_2.output_text:
                    metadata = DocumentMetadata.model_validate_json(response_2.output_text)
                else:
                    raise ValueError("No structured output found in response")
            print(f"\n✅ {pdf_filename}: Successfully extracted structured metadata")
            
            # Calculate overall scores
            from meta import calculate_overall_confidence, calculate_metadata_completeness
            metadata.overall_confidence = calculate_overall_confidence(metadata)
            metadata.metadata_completeness = calculate_metadata_completeness(metadata)
            
            # Calculate and display total processing time
            processing_end_time = time.time()
            total_duration = processing_end_time - processing_start_time
            
            print(f"\n⏱️ {pdf_filename}: PERFORMANCE METRICS:")
            print(f"  ├─ Total processing time: {total_duration:.2f} seconds")
            print(f"  ├─ Docs per minute (this worker): {60/total_duration:.1f}")
            print(f"  ├─ API calls made: 2 (upload + questions)")
            print(f"  ├─ API calls per minute: {120/total_duration:.1f}")
            print(f"  └─ Worker scaling estimate: For 126 RPM → {126/(120/total_duration):.1f} workers needed")
            
            # Return structured JSON for CSV export  
            assessment_json = {
                'is_health_policy_related': assessment.is_health_policy_related,
                'health_confidence': assessment.health_confidence,
                'health_explanation': assessment.health_explanation,
                'fits_ghpl_categories': assessment.fits_ghpl_categories,
                'category_confidence': assessment.category_confidence,
                'category_explanation': assessment.category_explanation
            }
            
            # Include token costs in the return
            total_cost = q1_tokens['cost'] + q2_tokens['cost']
            return metadata, json.dumps(assessment_json), total_cost
        except Exception as e:
            print(f"\n❌ {pdf_filename}: Failed to parse structured metadata: {e}")
            raw_response = getattr(response_2, 'output_text', str(response_2))
            print(f"{pdf_filename}: Raw response: {str(raw_response)[:500]}...")
            # For parsing errors in Question 2, still return Question 1 assessment  
            assessment_json = {
                'is_health_policy_related': assessment.is_health_policy_related,
                'health_confidence': assessment.health_confidence,
                'health_explanation': assessment.health_explanation,
                'fits_ghpl_categories': assessment.fits_ghpl_categories,
                'category_confidence': assessment.category_confidence,
                'category_explanation': assessment.category_explanation
            }
            # Return with only Q1 cost if Q2 failed
            return None, json.dumps(assessment_json), q1_tokens['cost']
            
    except Exception as e:
        print(f"❌ {pdf_filename}: Error in multi-turn conversation: {e}")
        logger.error(f"Chat processing error for {pdf_filename}: {traceback.format_exc()}")
        return None, None


def calculate_overall_confidence(metadata: GHPLDocumentMetadata) -> float:
    """Calculate overall confidence for GHPL metadata."""
    # Weight different fields by importance for health policy classification
    weights = {
        'document_type': 0.30,  # Most important for GHPL classification
        'title': 0.20,
        'issuing_authority': 0.15,
        'officially_endorsed': 0.15,
        'health_focus': 0.10,
        'country': 0.05,
        'year': 0.05
    }
    
    weighted_sum = 0.0
    total_weight = 0.0
    
    for field_name, weight in weights.items():
        field = getattr(metadata, field_name)
        if field.value is not None:
            weighted_sum += field.confidence * weight
            total_weight += weight
    
    # Calculate completeness
    fields_found = sum(1 for f in weights.keys() if getattr(metadata, f).value is not None)
    completeness = fields_found / len(weights)
    
    # Overall confidence adjusted by completeness
    if total_weight > 0:
        confidence = (weighted_sum / total_weight) * (0.7 + 0.3 * completeness)
    else:
        confidence = 0.0
    
    return round(confidence, 3)


def calculate_metadata_completeness(metadata: GHPLDocumentMetadata) -> float:
    """Calculate metadata completeness for GHPL standards."""
    fields = ['document_type', 'title', 'country', 'year', 'issuing_authority', 
             'officially_endorsed', 'governance_level', 'health_focus', 'language']
    fields_found = sum(1 for f in fields if getattr(metadata, f).value is not None)
    return round(fields_found / len(fields), 3)


@retry(
    wait=wait_random_exponential(min=1, max=30),  # Wait 1-30 seconds with exponential backoff
    stop=stop_after_attempt(4),  # Try up to 4 times (3 retries)
    retry=retry_if_exception_type((Exception,))  # Retry on any exception
)
def upload_files_with_retry(client, temp_first_path, temp_last_path, filename):
    """Upload PDF files with tenacity retry logic."""
    uploaded_first = None
    uploaded_last = None
    
    # Upload first pages
    with open(temp_first_path, "rb") as f:
        uploaded_first = client.files.create(file=f, purpose="user_data")
    print(f"✅ {filename}: Uploaded first pages: {uploaded_first.id}")
    
    # Upload last pages if exists
    if temp_last_path:
        with open(temp_last_path, "rb") as f:
            uploaded_last = client.files.create(file=f, purpose="user_data")
        print(f"✅ {filename}: Uploaded last pages: {uploaded_last.id}")
    
    return uploaded_first, uploaded_last


def display_ghpl_field(name: str, field):
    """Display a GHPL metadata field with its confidence and evidence."""
    if field.value is not None:
        print(f"{name}: {field.value}")
        print(f"  ├─ Confidence: {field.confidence:.2f}")
        if field.evidence:
            print(f"  ├─ Evidence: {field.evidence[:100]}...")
        if field.source_page:
            print(f"  ├─ Source: Page {field.source_page}")
        if field.alternatives:
            print(f"  └─ Alternatives: {', '.join(field.alternatives[:3])}")
    else:
        print(f"{name}: Not found")
        if field.evidence:
            print(f"  └─ Note: {field.evidence}")
    print()


def process_document_worker(pdf_path: str, client, worker_id: int, total_files: int, completed_count: dict, use_flex: bool = True):
    """Worker function to process a single document."""
    start_time = time.time()
    
    try:
        # Upload PDF subsets
        first_pages_file, last_pages_file = upload_pdf_subset(client, pdf_path)
        
        if not first_pages_file:
            print(f"❌ Worker-{worker_id}: Failed to upload {Path(pdf_path).name}")
            return None
        
        # Multi-turn chat conversation
        result = process_document_with_chat(client, first_pages_file, last_pages_file, pdf_path, use_flex=use_flex)
        
        # Handle both old (2-value) and new (3-value) return formats
        if isinstance(result, tuple) and len(result) == 3:
            metadata, relevance_response, api_cost = result
        elif isinstance(result, tuple) and len(result) == 2:
            # Old format without cost
            metadata, relevance_response = result
            api_cost = 0.0
        else:
            raise ValueError(f"Unexpected return format from process_document_with_chat: {result}")
        
        # Calculate processing time
        processing_time = time.time() - start_time
        
        # Update progress counter
        completed_count[0] += 1
        current_progress = completed_count[0]
        print(f"🏃‍♂️ Worker-{worker_id}: Completed {Path(pdf_path).name} ({current_progress}/{total_files})")
        
        # Cleanup uploaded files
        try:
            if first_pages_file:
                client.files.delete(first_pages_file.id)
            if last_pages_file:
                client.files.delete(last_pages_file.id)
        except Exception as cleanup_error:
            print(f"⚠️ Worker-{worker_id}: Cleanup warning: {cleanup_error}")
        
        return {
            'pdf_path': pdf_path,
            'filename': Path(pdf_path).name,
            'metadata': metadata,
            'relevance_response': relevance_response,
            'success': metadata is not None,
            'processed': True,  # Successfully processed, even if rejected
            'processing_time': processing_time,
            'api_cost': api_cost
        }
        
    except Exception as e:
        print(f"❌ Worker-{worker_id}: Error processing {Path(pdf_path).name}: {e}")
        processing_time = time.time() - start_time
        
        return {
            'pdf_path': pdf_path,
            'filename': Path(pdf_path).name,
            'metadata': None,
            'relevance_response': None,
            'success': False,
            'processed': False,  # Failed to process
            'processing_time': processing_time,
            'error': str(e),
            'api_cost': 0.0
        }


def parse_relevance_assessment(relevance_response: str) -> Dict[str, Any]:
    """Parse the relevance assessment JSON response to extract structured data."""
    try:
        # Parse JSON from relevance response
        relevance_data = json.loads(relevance_response)
        
        return {
            'question_1a_health_policy': relevance_data.get('is_health_policy_related', None),
            'question_1a_confidence': relevance_data.get('health_confidence', None),
            'question_1a_explanation': relevance_data.get('health_explanation', ''),
            'question_1b_ghpl_categories': relevance_data.get('fits_ghpl_categories', None),
            'question_1b_confidence': relevance_data.get('category_confidence', None),
            'question_1b_explanation': relevance_data.get('category_explanation', '')
        }
    except Exception as e:
        print(f"⚠️ Warning: Could not parse relevance response: {e}")
        return {
            'question_1a_health_policy': None,
            'question_1a_confidence': None,
            'question_1a_explanation': 'Parse error',
            'question_1b_ghpl_categories': None,
            'question_1b_confidence': None,
            'question_1b_explanation': 'Parse error'
        }


# Global lock for thread-safe CSV writing
csv_lock = threading.Lock()


def append_result_to_csv(result: Dict[str, Any], csv_filename: str) -> None:
    """Thread-safe append of a single result to CSV file."""
    
    # Parse relevance assessment data
    relevance_data = {}
    if result.get('relevance_response'):
        relevance_data = parse_relevance_assessment(result['relevance_response'])
    else:
        # Default values for failed processing
        relevance_data = {
            'question_1a_health_policy': None,
            'question_1a_confidence': None,
            'question_1a_explanation': 'Processing failed' if not result.get('processed', True) else 'No assessment',
            'question_1b_ghpl_categories': None,
            'question_1b_confidence': None,
            'question_1b_explanation': 'Processing failed' if not result.get('processed', True) else 'No assessment'
        }
    
    # Extract metadata if available (using same logic as export_results_to_csv)
    metadata = result.get('metadata')
    if metadata:
        # Helpers: robustly access metadata fields regardless of object/dict and alt names
        def get_meta_field(meta_obj, *names):
            """Try attribute, then dict key, for a list of candidate names."""
            for name in names:
                # Attribute access (Pydantic BaseModel)
                if hasattr(meta_obj, name):
                    val = getattr(meta_obj, name)
                    if val is not None:
                        return val
                # Dict-like access
                if isinstance(meta_obj, dict) and name in meta_obj and meta_obj[name] is not None:
                    return meta_obj[name]
            return None

        def extract_field_value(field, field_name: str = ""):
            """Return underlying value for nested field objects, enums, or dicts."""
            # Debug for the problematic fields
            if field_name in ['doc_type', 'health_topic', 'creator']:
                try:
                    hv = hasattr(field, 'value') if field is not None else False
                    print(f"DEBUG {field_name}: field_type={type(field)}, has_value_attr={hv}")
                    if hv:
                        print(f"DEBUG {field_name}: nested_value_type={type(getattr(field, 'value', None))}")
                except Exception:
                    pass

            # Handle Pydantic-style nested field with .value
            if field is not None and hasattr(field, 'value'):
                inner = getattr(field, 'value')
                # Enum with .value
                if hasattr(inner, 'value'):
                    return inner.value
                return inner
            # Handle dict field shapes like {"value": ...}
            if isinstance(field, dict):
                inner = field.get('value', None)
                if inner is None:
                    # Some responses might use alternate key
                    inner = field.get('name', None)
                # Enum-like nested dict {"value": {"value": "..."}}
                if isinstance(inner, dict) and 'value' in inner:
                    return inner.get('value')
                return inner
            return None
        
        metadata_fields = {
            'metadata_extracted': True,
            'title': extract_field_value(get_meta_field(metadata, 'title')),
            # Use DocumentMetadata field names
            'doc_type': extract_field_value(get_meta_field(metadata, 'doc_type'), 'doc_type'),
            'health_topic': extract_field_value(get_meta_field(metadata, 'health_topic'), 'health_topic'),
            'creator': extract_field_value(get_meta_field(metadata, 'creator'), 'creator'),
            'year': extract_field_value(get_meta_field(metadata, 'year')),
            'country': extract_field_value(get_meta_field(metadata, 'country')),
            'language': extract_field_value(get_meta_field(metadata, 'language')),
            # Use DocumentMetadata field name
            'governance_level': extract_field_value(get_meta_field(metadata, 'level')),
            'overall_confidence': getattr(metadata, 'overall_confidence', None) if not isinstance(metadata, dict) else metadata.get('overall_confidence'),
            'metadata_completeness': getattr(metadata, 'metadata_completeness', None) if not isinstance(metadata, dict) else metadata.get('metadata_completeness')
        }
    else:
        metadata_fields = {
            'metadata_extracted': False,
            'title': '',
            'doc_type': '',
            'health_topic': '',
            'creator': '',
            'year': '',
            'country': '',
            'language': '',
            'governance_level': '',
            'overall_confidence': None,
            'metadata_completeness': None
        }
    
    # Create CSV row (combining all data like export_results_to_csv does)
    csv_row = {
        'filename': result['filename'],
        **relevance_data,
        **metadata_fields,
        'processing_time_seconds': round(result.get('processing_time', 0.0), 2),
        'processed': result.get('processed', True),
        'error_message': result.get('error', '') if not result.get('processed', True) else ''
    }
    
    # Column order for consistency
    column_order = [
        'filename',
        'question_1a_health_policy', 'question_1a_confidence', 'question_1a_explanation',
        'question_1b_ghpl_categories', 'question_1b_confidence', 'question_1b_explanation',
        'metadata_extracted',
        'title', 'doc_type', 'health_topic', 'creator', 'year', 'country', 'language', 'governance_level',
        'overall_confidence', 'metadata_completeness',
        'processing_time_seconds', 'processed', 'error_message'
    ]
    
    # Thread-safe file writing
    with csv_lock:
        file_exists = os.path.exists(csv_filename)
        
        # Create DataFrame for this single row
        df_row = pd.DataFrame([csv_row])
        
        # Reorder columns
        for col in column_order:
            if col not in df_row.columns:
                df_row[col] = None
        df_row = df_row[column_order]
        
        # Write to CSV
        df_row.to_csv(csv_filename, mode='a', header=not file_exists, index=False)


def export_results_to_csv(results: List[Dict[str, Any]], output_path: str) -> None:
    """Export all results to a comprehensive CSV file."""
    print(f"\n📄 Exporting results to CSV: {output_path}")
    
    csv_rows = []
    
    for result in results:
        filename = result['filename']
        processing_time = result.get('processing_time', 0.0)
        
        # Parse relevance assessment data
        relevance_data = {}
        if result.get('relevance_response'):
            relevance_data = parse_relevance_assessment(result['relevance_response'])
        else:
            # Default values for failed processing
            relevance_data = {
                'question_1a_health_policy': None,
                'question_1a_confidence': None,
                'question_1a_explanation': 'Processing failed' if not result.get('processed', True) else 'No assessment',
                'question_1b_ghpl_categories': None,
                'question_1b_confidence': None,
                'question_1b_explanation': 'Processing failed' if not result.get('processed', True) else 'No assessment'
            }
        
        # Extract metadata if available
        metadata = result.get('metadata')
        if metadata:
            # Helper function to safely extract field values and handle alt names
            def get_meta_field(meta_obj, *names):
                for name in names:
                    if hasattr(meta_obj, name):
                        v = getattr(meta_obj, name)
                        if v is not None:
                            return v
                    if isinstance(meta_obj, dict) and name in meta_obj and meta_obj[name] is not None:
                        return meta_obj[name]
                return None

            def extract_field_value(field, field_name: str = ""):
                if field_name in ['doc_type', 'health_topic', 'creator']:
                    try:
                        hv = hasattr(field, 'value') if field is not None else False
                        print(f"DEBUG {field_name}: field_type={type(field)}, has_value_attr={hv}")
                    except Exception:
                        pass
                if field is not None and hasattr(field, 'value'):
                    inner = getattr(field, 'value')
                    if hasattr(inner, 'value'):
                        return inner.value
                    return inner
                if isinstance(field, dict):
                    inner = field.get('value', None)
                    if inner is None:
                        inner = field.get('name', None)
                    if isinstance(inner, dict) and 'value' in inner:
                        return inner.get('value')
                    return inner
                return None
            
            # DEBUG: Check what fields actually exist on metadata object
            print(f"\nDEBUG: metadata object type: {type(metadata)}")
            try:
                print(f"DEBUG: metadata fields: {[attr for attr in dir(metadata) if not attr.startswith('_')]}")
                print(f"DEBUG: hasattr doc_type: {hasattr(metadata, 'doc_type')}")
                print(f"DEBUG: hasattr health_topic: {hasattr(metadata, 'health_topic')}")
                print(f"DEBUG: hasattr creator: {hasattr(metadata, 'creator')}")
                print(f"DEBUG: hasattr level: {hasattr(metadata, 'level')}")
            except Exception:
                pass
            
            metadata_row = {
                'metadata_extracted': True,
                'title': extract_field_value(get_meta_field(metadata, 'title')),
                'doc_type': extract_field_value(get_meta_field(metadata, 'doc_type', 'document_type'), 'doc_type'),
                'health_topic': extract_field_value(get_meta_field(metadata, 'health_topic'), 'health_topic'),
                'creator': extract_field_value(get_meta_field(metadata, 'creator'), 'creator'),
                'year': extract_field_value(get_meta_field(metadata, 'year')),
                'country': extract_field_value(get_meta_field(metadata, 'country')),
                'language': extract_field_value(get_meta_field(metadata, 'language')),
                'governance_level': extract_field_value(get_meta_field(metadata, 'level', 'governance_level')),
                'overall_confidence': getattr(metadata, 'overall_confidence', None) if not isinstance(metadata, dict) else metadata.get('overall_confidence'),
                'metadata_completeness': getattr(metadata, 'metadata_completeness', None) if not isinstance(metadata, dict) else metadata.get('metadata_completeness')
            }
        else:
            # No metadata extracted
            metadata_row = {
                'metadata_extracted': False,
                'title': '',
                'doc_type': '',
                'health_topic': '',
                'creator': '',
                'year': '',
                'country': '',
                'language': '',
                'governance_level': '',
                'overall_confidence': None,
                'metadata_completeness': None
            }
        
        # Combine all data into one row
        csv_row = {
            'filename': filename,
            **relevance_data,
            **metadata_row,
            'processing_time_seconds': round(processing_time, 2),
            'processed': result.get('processed', True),
            'error_message': result.get('error', '') if not result.get('processed', True) else ''
        }
        
        csv_rows.append(csv_row)
    
    # Create DataFrame and export to CSV
    df = pd.DataFrame(csv_rows)
    
    # Reorder columns for better readability
    column_order = [
        'filename',
        'question_1a_health_policy', 'question_1a_confidence', 'question_1a_explanation',
        'question_1b_ghpl_categories', 'question_1b_confidence', 'question_1b_explanation',
        'metadata_extracted',
        'title', 'doc_type', 'health_topic', 'creator', 'year', 'country', 'language', 'governance_level',
        'overall_confidence', 'metadata_completeness',
        'processing_time_seconds', 'processed', 'error_message'
    ]
    
    # Ensure all columns exist (in case some are missing)
    for col in column_order:
        if col not in df.columns:
            df[col] = None
    
    df = df[column_order]
    df.to_csv(output_path, index=False)
    
    print(f"✅ CSV export complete: {len(csv_rows)} rows written to {output_path}")


def batch_process_documents(docs_dir: str, api_key: str, workers: int = 80, limit: Optional[int] = None, use_flex: bool = True, fresh_start: bool = False):
    """Process multiple documents using flex processing with OpenAI GPT-5-mini."""
    print(f"🚀 Starting batch processing with {workers} workers (flex processing)")
    print(f"📁 Directory: {docs_dir}")
    print("⚡ Using GPT-5-mini with flex processing for optimal cost/performance")
    
    # Find all PDF files
    pdf_files = []
    for pattern in ['*.pdf', '*.PDF']:
        pdf_files.extend(glob.glob(os.path.join(docs_dir, pattern)))
    
    if not pdf_files:
        print(f"❌ No PDF files found in {docs_dir}")
        return
    
    # Check for existing CSV to resume from (unless fresh start requested)
    existing_csvs = glob.glob("meta_gpt5_results_*.csv")
    processed_files = set()
    
    if existing_csvs and not fresh_start:
        # Find the most recent CSV file
        csv_filename = max(existing_csvs, key=os.path.getctime)
        print(f"📄 Found existing CSV: {csv_filename}")
        
        try:
            # Load existing CSV and get processed filenames
            existing_df = pd.read_csv(csv_filename)
            processed_files = set(existing_df['filename'].tolist())
            print(f"📊 Resuming: {len(processed_files)} files already processed")
            
            # Filter out already processed files
            original_count = len(pdf_files)
            pdf_files = [f for f in pdf_files if os.path.basename(f) not in processed_files]
            remaining_count = len(pdf_files)
            
            if remaining_count == 0:
                print(f"✅ All files already processed! Nothing to do.")
                return
            elif remaining_count < original_count:
                print(f"📋 Skipping {original_count - remaining_count} already-processed files")
                print(f"📋 {remaining_count} files remaining to process")
            
        except Exception as e:
            print(f"⚠️ Could not load existing CSV: {e}")
            print(f"📄 Starting fresh...")
            # Create new timestamped filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"meta_gpt5_results_{timestamp}.csv"
    else:
        # Create new timestamped filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"meta_gpt5_results_{timestamp}.csv"
        if fresh_start and existing_csvs:
            print(f"🔄 Fresh start requested - ignoring {len(existing_csvs)} existing CSV files")
        print(f"📄 Starting new CSV: {csv_filename}")
    
    # Apply limit if specified (after resume filtering)
    if limit:
        pdf_files = pdf_files[:limit]
        print(f"📊 Limited to {limit} files for testing")
    
    total_files = len(pdf_files)
    print(f"📋 Found {total_files} PDF files to process")
    
    # Initialize OpenAI client with extended timeout for flex processing
    client = OpenAI(api_key=api_key, timeout=900.0)  # 15 minute timeout
    print("📈 Using flex processing (slower but much cheaper)")
    
    # Shared progress counter (thread-safe with list)
    completed_count = [0]
    results = []
    start_time = time.time()
    
    print(f"🏃‍♂️ Starting {workers} workers...")
    
    # Process files using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all files to workers
        future_to_file = {}
        for i, pdf_path in enumerate(pdf_files):
            worker_id = (i % workers) + 1  # Worker IDs 1-80 (or configured workers)
            future = executor.submit(process_document_worker, pdf_path, client, worker_id, total_files, completed_count, use_flex)
            future_to_file[future] = pdf_path
        
        # Collect results as they complete and write to CSV immediately
        for future in as_completed(future_to_file):
            result = future.result()
            if result:
                # Append to CSV immediately (thread-safe)
                append_result_to_csv(result, csv_filename)
                results.append(result)
    
    # Calculate final statistics
    elapsed_time = time.time() - start_time
    processed = sum(1 for r in results if r.get('processed', True))  # Successfully processed
    extracted = sum(1 for r in results if r['success'])  # Metadata extracted  
    rejected = processed - extracted  # Processed but rejected in relevance assessment
    failed = len(results) - processed  # Actually failed to process
    total_cost = sum(r.get('api_cost', 0.0) for r in results)  # Total API cost
    
    print(f"\n🎯 BATCH PROCESSING COMPLETE")
    print(f"{'='*50}")
    print(f"📊 Total files: {total_files}")
    print(f"✅ Metadata extracted: {extracted}")
    print(f"🚫 Rejected (not health policy): {rejected}")
    print(f"❌ Processing failed: {failed}")
    print(f"⏱️ Total time: {elapsed_time/60:.1f} minutes")
    print(f"🚀 Throughput: {total_files/(elapsed_time/60):.1f} files/minute")
    print(f"💰 Total API cost: ${total_cost:.2f}")
    
    if failed > 0:
        print(f"\n❌ PROCESSING FAILURES:")
        for result in results:
            if not result.get('processed', True):
                error_msg = result.get('error', 'Unknown error')
                print(f"  • {result['filename']}: {error_msg}")
    
    if rejected > 0:
        print(f"\n🚫 REJECTED FILES (detailed breakdown):")
        print(f"{'─'*60}")
        for result in results:
            if result.get('processed', True) and not result['success']:
                filename = result['filename'][:50]  # Truncate long filenames
                
                # Parse relevance assessment to get Q1a and Q1b values
                relevance_data = parse_relevance_assessment(result.get('relevance_response', '{}'))
                q1a = relevance_data.get('question_1a_health_policy', False)
                q1b = relevance_data.get('question_1b_ghpl_categories', False)
                
                # Determine rejection reason with clear A/B status
                q1a_emoji = "✅" if q1a else "❌"
                q1b_emoji = "✅" if q1b else "❌"
                
                if not q1a and not q1b:
                    reason = f"❌ Q A failed, ❌ Q B failed (not health related, doesn't fit in category)"
                elif not q1a:
                    reason = f"❌ Q A failed, ✅ Q B passed (not from health authority)"
                else:  # not q1b
                    reason = f"✅ Q A passed, ❌ Q B failed (health related but doesn't fit in category)"
                
                print(f"  • {filename:<50} {reason}")
    
    # CSV already written continuously during processing
    print(f"\n📄 Results continuously written to: {csv_filename}")
    print(f"✅ CSV contains {len(results)} processed documents")
    
    return results


def main():
    """Main function implementing the GHPL two-stage approach."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='GHPL metadata extraction with rolling workers')
    parser.add_argument('pdf_path', nargs='?', help='Single PDF file to process (if no --docs-dir)')
    parser.add_argument('--docs-dir', help='Directory containing PDF files for batch processing')
    parser.add_argument('--workers', type=int, default=80, help='Number of concurrent workers (default: 80 for 500 RPM limit)')
    parser.add_argument('--no-flex', action='store_true', help='Disable flex processing (costs more, may be faster)')
    parser.add_argument('--limit', type=int, help='Maximum number of files to process (for testing)')
    parser.add_argument('--fresh-start', action='store_true', help='Start fresh run, ignore existing CSV files')
    
    args = parser.parse_args()
    
    # Get API key from environment
    API_KEY = os.environ.get("OPENAI_API_KEY")
    
    if not API_KEY:
        print("❌ Please set the OPENAI_API_KEY environment variable.")
        return
    
    # Determine processing mode
    if args.docs_dir:
        # Batch processing mode
        if not os.path.exists(args.docs_dir):
            print(f"❌ Directory not found: {args.docs_dir}")
            return
        
        print(f"🚀 BATCH PROCESSING MODE")
        print(f"📁 Directory: {args.docs_dir}")
        print(f"👥 Workers: {args.workers}")
        if args.limit:
            print(f"📊 Limit: {args.limit} files")
        
        batch_process_documents(args.docs_dir, API_KEY, args.workers, args.limit, use_flex=not args.no_flex, fresh_start=args.fresh_start)
        return
    
    # Single file processing mode
    pdf_path = args.pdf_path or "docs_correct/2017-27762.pdf"  # Default test file
    
    if not os.path.exists(pdf_path):
        print(f"❌ File not found: {pdf_path}")
        return
    
    print(f"📄 SINGLE FILE PROCESSING MODE")
    print(f"📁 File: {pdf_path}")
    
    try:
        # Initialize client with extended timeout for flex processing
        print("🔧 Initializing OpenAI client (GPT-5-mini with flex)...")
        client = OpenAI(api_key=API_KEY, timeout=900.0)
        
        # Stage 1: Upload PDF subsets
        first_pages_file, last_pages_file = upload_pdf_subset(client, pdf_path)
        
        if not first_pages_file:
            print("❌ Failed to upload PDF subsets")
            return
        
        # Multi-turn chat conversation
        result = process_document_with_chat(client, first_pages_file, last_pages_file, pdf_path, use_flex=not args.no_flex)
        
        # Handle both old (2-value) and new (3-value) return formats
        if isinstance(result, tuple) and len(result) == 3:
            metadata, relevance_response, api_cost = result
        elif isinstance(result, tuple) and len(result) == 2:
            # Old format without cost
            metadata, relevance_response = result
            api_cost = 0.0
        else:
            raise ValueError(f"Unexpected return format from process_document_with_chat: {result}")
        
        if metadata:
            print("\n" + "=" * 60)
            print("📋 GHPL METADATA EXTRACTION RESULTS")
            print("=" * 60)
            print(f"Overall Confidence: {metadata.overall_confidence:.2f}")
            print(f"Metadata Completeness: {metadata.metadata_completeness:.1%}")
            print("-" * 60)
            
            # Import display function from meta.py
            from meta import display_field
            
            # Display each field using the structured display function
            display_field("Title", metadata.title)
            display_field("Document Type", metadata.doc_type)
            display_field("Health Topic", metadata.health_topic)
            display_field("Creator", metadata.creator)
            display_field("Year", metadata.year)
            display_field("Country", metadata.country)
            display_field("Language", metadata.language)
            display_field("Governance Level", metadata.level)
            
            print("-" * 60)
            
            # Recommendations based on confidence
            if metadata.overall_confidence > 0.8:
                print("✅ High confidence GHPL classification")
            elif metadata.overall_confidence > 0.6:
                print("⚠️ Medium confidence - review recommended") 
            else:
                print("❌ Low confidence - manual review required")
                
        else:
            if relevance_response:
                print("\n" + "=" * 60)
                print("📋 RELEVANCE ASSESSMENT RESPONSE")
                print("=" * 60)
                print(relevance_response)
        
        # Cleanup uploaded files
        try:
            print("\n🗑️ Cleaning up uploaded files...")
            if first_pages_file:
                client.files.delete(first_pages_file.id)
                print("✅ Deleted first pages file")
            if last_pages_file:
                client.files.delete(last_pages_file.id)
                print("✅ Deleted last pages file")
            print("✅ Cleanup complete")
        except Exception as e:
            logger.warning(f"Warning: Could not clean up uploaded files: {e}")
                
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        logger.error(f"Full error: {traceback.format_exc()}")


if __name__ == "__main__":
    main()
