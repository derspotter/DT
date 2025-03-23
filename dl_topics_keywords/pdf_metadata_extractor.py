#!/usr/bin/env python3
import os
import sys
import json
import re
import argparse
import logging
import pikepdf
import requests
import tempfile
import time
import base64
from pathlib import Path
from datetime import datetime
from google import genai
import slugify
import random

# Add these imports for rate limiting
from datetime import datetime, timedelta
import collections
import shutil
from pikepdf.models.metadata import PdfMetadata
from rapidfuzz import fuzz  # You'll need to install this: pip install rapidfuzz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('pdf_metadata_extractor')

# Rate limiter for API calls
class RateLimiter:
    def __init__(self, max_calls=15, period=60, initial_backoff=10, max_backoff=300):
        """Initialize a rate limiter
        
        Args:
            max_calls: Maximum number of calls allowed in the period
            period: Time period in seconds
            initial_backoff: Initial backoff time in seconds when hitting quota limits
            max_backoff: Maximum backoff time in seconds
        """
        self.max_calls = max_calls
        self.period = period  # in seconds
        self.calls = collections.deque()
        self.current_backoff = initial_backoff
        self.max_backoff = max_backoff
        
    def wait_if_needed(self):
        """Wait if we've exceeded the rate limit"""
        now = datetime.now()
        
        # Remove timestamps older than the period
        while self.calls and now - self.calls[0] > timedelta(seconds=self.period):
            self.calls.popleft()
            
        # If we've hit the limit, wait
        if len(self.calls) >= self.max_calls:
            sleep_time = (self.calls[0] + timedelta(seconds=self.period) - now).total_seconds()
            if sleep_time > 0:
                logger.info(f"Rate limit reached. Waiting for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                
        # Record this call
        self.calls.append(now)
    
    def handle_quota_error(self):
        """Handle a quota error with exponential backoff"""
        logger.warning(f"API quota exhausted. Backing off for {self.current_backoff} seconds")
        time.sleep(self.current_backoff)
        
        # Increase backoff for next time (exponential backoff with jitter)
        next_backoff = min(self.current_backoff * 2, self.max_backoff)
        self.current_backoff = next_backoff + random.uniform(0, 5)  # Add jitter
        
    def reset_backoff(self):
        """Reset the backoff time after successful calls"""
        self.current_backoff = 10

# Create a rate limiter instance for Gemini (adjusted to 15 requests per minute)
gemini_rate_limiter = RateLimiter(max_calls=15, period=60)  # 15 calls per minute

def initialize_apis():
    """Initialize the Gemini API with robust API key detection"""
    # Try multiple methods to get the API key
    gemini_api_key = None
    
    # Method 1: Standard environment variable
    gemini_api_key = os.getenv('GOOGLE_API_KEY')
    
    # Method 2: Try reading from bashrc if not found in environment
    if not gemini_api_key:
        logger.info("API key not found in environment, trying to read from .bashrc")
        try:
            bashrc_path = os.path.expanduser("~/.bashrc")
            if os.path.exists(bashrc_path):
                with open(bashrc_path, 'r') as f:
                    bashrc_content = f.read()
                    match = re.search(r'GOOGLE_API_KEY\s*=\s*["\']?([^"\'\n]+)["\']?', bashrc_content)
                    if match:
                        gemini_api_key = match.group(1).strip()
                        logger.info("Found API key in .bashrc")
        except Exception as e:
            logger.error(f"Error reading .bashrc: {e}")
    
    # Method 3: Check for API key file
    if not gemini_api_key:
        api_key_file = os.path.expanduser("~/.google_api_key")
        if os.path.exists(api_key_file):
            try:
                with open(api_key_file, 'r') as f:
                    gemini_api_key = f.read().strip()
                    logger.info("Found API key in ~/.google_api_key file")
            except Exception as e:
                logger.error(f"Error reading API key file: {e}")
    
    # Method 4: Direct specification as a last resort
    if not gemini_api_key:
        # Uncomment and set this for testing only, then remove
        # gemini_api_key = "YOUR_API_KEY_HERE"
        pass
    
    # Final check
    if not gemini_api_key:
        print("\n=== GOOGLE API KEY NOT FOUND ===")
        print("Your API key was not detected. Debug information:")
        print(f"- Shell environment variable: {os.getenv('GOOGLE_API_KEY') or 'Not set'}")
        print("- Attempted to read from ~/.bashrc")
        print("- Attempted to read from ~/.google_api_key")
        print("\nPossible solutions:")
        print("1. Export the key directly before running the script:")
        print("   export GOOGLE_API_KEY=your_api_key")
        print("2. Create a file at ~/.google_api_key containing only your API key")
        print("3. Modify this script to include your API key directly (for testing)")
        print("\nNOTE: Sometimes environment variables in .bashrc aren't available")
        print("      to scripts run from certain contexts.\n")
        sys.exit(1)
    
    # Successfully found the API key, now initialize the client
    try:
        logger.info("Initializing Gemini client with API key")
        gemini = genai.Client(api_key=gemini_api_key)
        # Test the API key with a minimal request
        _ = gemini.models.list()
        logger.info("Gemini API client initialized successfully")
        return gemini
    except Exception as e:
        logger.error(f"Error initializing Gemini client: {e}")
        print(f"\nError initializing Gemini API: {e}")
        print("The API key was found but may not be valid. Please check the key.")
        sys.exit(1)

def is_valid_pdf(pdf_path):
    """Check if a PDF file is valid and readable"""
    try:
        with pikepdf.open(pdf_path, allow_overwriting_input=False) as pdf:
            # Try to access basic PDF properties
            num_pages = len(pdf.pages)
            return True, num_pages, None
    except pikepdf.PdfError as e:
        # This is a pikepdf-specific error for PDF issues
        return False, 0, f"PDF validation error: {e}"
    except Exception as e:
        return False, 0, f"General error validating PDF: {e}"

def attempt_pdf_repair(pdf_path):
    """Attempt to repair a corrupted PDF file"""
    logger.info(f"Attempting to repair corrupted PDF: {pdf_path}")
    
    try:
        # Create a temporary file for the repaired PDF
        repaired_path = os.path.join(tempfile.gettempdir(), f"repaired_{os.path.basename(pdf_path)}")
        
        # Attempt repair using pikepdf's recovery mode
        with pikepdf.open(pdf_path, suppress_warnings=True, attempt_recovery=True) as pdf:
            pdf.save(repaired_path)
        
        logger.info(f"PDF repair attempt successful, saved to: {repaired_path}")
        return True, repaired_path
    except Exception as e:
        logger.error(f"PDF repair attempt failed: {e}")
        return False, None

def extract_first_pages_with_pikepdf(pdf_path, num_pages=5, attempt_repair=True):
    """Extract the first few pages from a PDF file using pikepdf"""
    logger.info(f"Extracting first {num_pages} pages from {pdf_path} using pikepdf")
    
    # First check if the PDF is valid
    is_valid, page_count, error_message = is_valid_pdf(pdf_path)
    
    if not is_valid:
        logger.error(f"Invalid PDF file: {error_message}")
        
        # Attempt repair if requested
        if attempt_repair:
            repair_success, repaired_path = attempt_pdf_repair(pdf_path)
            if repair_success:
                logger.info(f"Using repaired PDF: {repaired_path}")
                pdf_path = repaired_path
                is_valid = True
            else:
                logger.error("PDF repair failed, cannot proceed with extraction")
                return {"pdf_path": None, "num_pages": 0, "error": error_message}
        else:
            return {"pdf_path": None, "num_pages": 0, "error": error_message}
    
    try:
        # Create a temporary file for the extracted pages
        temp_pdf_path = os.path.join(tempfile.gettempdir(), f"temp_pdf_pages_{int(time.time())}.pdf")
        
        # Open the source PDF
        with pikepdf.open(pdf_path) as pdf:
            # Create a new PDF with only the first few pages
            dst = pikepdf.new()
            
            # Get the actual number of pages (not exceeding num_pages)
            actual_pages = min(num_pages, len(pdf.pages))
            
            # Add the pages to the new PDF
            for i in range(actual_pages):
                dst.pages.append(pdf.pages[i])
            
            # Save the new PDF with the first few pages
            dst.save(temp_pdf_path)
        
        logger.info(f"Successfully extracted {actual_pages} pages to {temp_pdf_path}")
        return {
            "pdf_path": temp_pdf_path,
            "num_pages": actual_pages
        }
    except Exception as e:
        logger.error(f"Error extracting PDF pages: {e}")
        return {
            "pdf_path": None,
            "num_pages": 0,
            "error": str(e)
        }

def extract_metadata_with_gemini_multimodal(gemini_client, pdf_content, filename):
    """Use Gemini's multimodal capabilities to extract metadata from PDF with rate limiting"""
    logger.info("Extracting metadata with Gemini using multimodal analysis")
    
    # Apply rate limiting
    gemini_rate_limiter.wait_if_needed()
    
    try:
        # Check if we have a valid PDF extract
        if not pdf_content['pdf_path'] or not os.path.exists(pdf_content['pdf_path']):
            logger.error("No valid PDF extract available for analysis")
            return get_default_metadata()
        
        # Prepare content parts for Gemini - first the text prompt
        text_prompt = f"""You are a scholarly metadata extraction expert analyzing a PDF document.

The filename of this document is: {filename}
Examine the filename carefully as it may contain valuable metadata clues (like year, author organization, or document type).

Please extract the following metadata from this PDF in JSON format:

- title: The document's full title. This is CRITICALLY IMPORTANT to extract correctly.
- abstract: A brief summary if present, or "N/A" if not available
- publication_year: The year of publication (just the 4-digit year). This is CRITICALLY IMPORTANT and may be found in the document header/footer or filename.
- authors: A list of all author names. IMPORTANT: If no specific individuals are listed as authors, look for the ISSUING ORGANIZATION (like European Commission, EU, EFRAG, IPSF, etc). Never leave this empty - at minimum include the organization that published the document.
- type: One of these: "article", "book", "book chapter", "pre-print", or "other"
- journal: Journal name if it's an article, or "N/A" if not applicable
- book_title: Book title if it's a book or chapter, or "N/A" if not applicable

These are the first few pages of a document. For European policy documents, look carefully for:
- European Commission documents often have a date in the header/footer
- Document numbers/references may appear in headers (like COM(2023) 123)
- The issuing body (Commission, Parliament, EFRAG) should be listed as author if no individuals are named

Return ONLY a valid JSON object with these fields. Do not include any explanatory text.
"""

        # Read the PDF file
        with open(pdf_content['pdf_path'], "rb") as pdf_file:
            pdf_data = pdf_file.read()
        
        # Create request with text prompt and PDF
        parts = [
            text_prompt,
            {
                "inline_data": {
                    "data": base64.b64encode(pdf_data).decode('utf-8'),
                    "mime_type": "application/pdf"
                }
            }
        ]
        
        # Send the multimodal request to Gemini
        model = "gemini-2.0-flash-exp"  # Ensure this model supports PDF input
        logger.info(f"Sending multimodal request with PDF to Gemini {model}")
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = gemini_client.models.generate_content(
                    model=model,
                    contents=parts,
                )
                # Reset backoff on successful call
                gemini_rate_limiter.reset_backoff()
                break
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or "quota" in str(e).lower():
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(f"Failed after {max_retries} attempts: {e}")
                        raise
                    
                    # Handle quota error with backoff
                    gemini_rate_limiter.handle_quota_error()
                else:
                    # For other errors, just raise immediately
                    raise
        
        # Extract JSON from the response
        json_text = response.text.strip()
        
        # Remove any markdown formatting that might be around the JSON
        json_text = re.sub(r'^```json', '', json_text)
        json_text = re.sub(r'^```', '', json_text)
        json_text = re.sub(r'```$', '', json_text)
        json_text = json_text.strip()
        
        logger.info(f"Raw JSON response: {json_text[:200]}...")
        
        metadata = json.loads(json_text)
        logger.info("Successfully extracted metadata with Gemini multimodal analysis")
        
        # Ensure all required fields are present
        required_fields = ["title", "abstract", "publication_year", "authors", "type"]
        for field in required_fields:
            if field not in metadata:
                metadata[field] = "N/A"
        
        if "journal" not in metadata:
            metadata["journal"] = "N/A"
            
        if "book_title" not in metadata:
            metadata["book_title"] = "N/A"
        
        return metadata
        
    except Exception as e:
        logger.error(f"Error extracting metadata with Gemini multimodal: {e}")
        return get_default_metadata()

def get_default_metadata():
    """Return default metadata structure if extraction fails"""
    return {
        "title": "Unknown Title",
        "abstract": "N/A",
        "publication_year": datetime.now().year,
        "authors": ["Unknown Author"],
        "type": "other",
        "journal": "N/A",
        "book_title": "N/A"
    }

def clean_search_term(term):
    """Clean up search term for OpenAlex API based on OpenAlexScraper.py"""
    if not term or not isinstance(term, str):
        return ""
    
    # Remove special characters that could cause search issues
    term = re.sub(r'[\\/*?:"<>|]', ' ', term)
    # Remove excessive whitespace
    term = re.sub(r'\s+', ' ', term).strip()
    # Limit length to prevent URL issues
    return term[:150] if len(term) > 150 else term

def normalize_name(name):
    """Split academic name into first name(s) and last name, standardize format"""
    if not name:
        return None, None

    name = name.lower().strip()
    
    # Check if name is in "LastName, FirstNames" format
    if ',' in name:
        parts = [part.strip() for part in name.split(',')]
        if len(parts) == 2:
            last_name = parts[0]
            first_names = parts[1]
        else:
            return None, None
    else:
        # Assume "FirstNames LastName" format
        parts = name.split()
        if len(parts) == 0:
            return None, None
        last_name = parts[-1]
        first_names = ' '.join(parts[:-1])

    # Remove non-alphabetic characters from first names (like initials)
    first_names = ''.join(c for c in first_names if c.isalpha() or c.isspace()).strip()

    return first_names, last_name

def match_author_name(ref_author, result_author):
    """Match academic author names with tolerance for initials and fuzzy logic"""
    ref_first, ref_last = normalize_name(ref_author)
    result_first, result_last = normalize_name(result_author)

    if not ref_first or not ref_last or not result_first or not result_last:
        return 0.0

    # Last name must match at a high similarity level (fuzzy match with a high threshold)
    last_name_score = fuzz.ratio(ref_last, result_last)
    if last_name_score < 90:
        return 0.0

    # For first names, we're more lenient
    if ref_first[0] == result_first[0]:  # At least first initial matches
        return 0.8
    
    # Use fuzzy matching for first names
    first_name_score = fuzz.partial_ratio(ref_first, result_first)
    if first_name_score >= 70:
        return 0.7

    return 0.0

def calculate_author_match_score(gemini_authors, openalex_authors):
    """Calculate author match score between metadata and OpenAlex result"""
    if not gemini_authors or not openalex_authors:
        return 0.0
    
    # Only attempt author matching if we have actual author names
    if gemini_authors == ["Unknown Author"]:
        return 0.1  # Small default score
    
    matches = 0
    for gemini_author in gemini_authors:
        best_score = 0.0
        for openalex_author in openalex_authors:
            score = match_author_name(gemini_author, openalex_author)
            best_score = max(best_score, score)
        if best_score > 0:
            matches += best_score
    
    # Calculate final score - normalize by the smaller of the two author list lengths
    return matches / min(len(gemini_authors), len(openalex_authors))

def get_doi_and_metadata_from_openalex(metadata):
    """Try to find DOI and complete metadata using OpenAlex API"""
    logger.info("Searching for DOI and metadata on OpenAlex")
    
    # Extract metadata fields
    title = metadata.get('title', '')
    if title == 'Unknown Title' or not title:
        logger.warning("No valid title to search with")
        return None, None
    
    # Clean up the title for searching
    search_title = clean_search_term(title)
    
    # Get author and year info
    authors = metadata.get('authors', [])
    year = metadata.get('publication_year', '')
    journal = metadata.get('journal', '')
    if journal == 'N/A':
        journal = None
    
    # Store all unique results across search strategies
    all_results = {}
    
    # Define a reusable function for making OpenAlex API calls
    def call_openalex_api(params, description):
        try:
            logger.info(f"OpenAlex strategy: {description}")
            if 'filter' in params:
                logger.info(f"OpenAlex filter: {params['filter']}")
            
            # Add your email to comply with OpenAlex usage policies
            params['mailto'] = 'your.name@tu-berlin.de'  # Update with your TU Berlin email
            
            # Request only necessary fields to minimize data transfer
            fields = 'id,doi,display_name,publication_year,type,authorships'
            if 'select' in params:
                params['select'] = fields
            
            response = requests.get(
                "https://api.openalex.org/works", 
                params=params,
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            results = data.get('results', [])
            
            if not results:
                logger.info(f"No results found with strategy: {description}")
                return []
            
            logger.info(f"Found {len(results)} results with strategy: {description}")
            return results
            
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenAlex API request error with strategy {description}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error searching OpenAlex with strategy {description}: {e}")
            return []
    
    try:
        # STEP 1: Title exact match + year (if available)
        if search_title:
            filter_parts = [f'display_name.search:"{search_title}"']
            if year and year != 'N/A':
                try:
                    year_int = int(year)
                    filter_parts.append(f'publication_year:{year_int}')
                except (ValueError, TypeError):
                    pass
                
            params = {
                'filter': ','.join(filter_parts),
                'per-page': 5
            }
            
            results = call_openalex_api(params, "Step 1: Title exact + year")
            for result in results:
                if result is None:
                    continue
                    
                result_id = result.get('id')
                if result_id and result_id not in all_results:
                    all_results[result_id] = {
                        'result': result,
                        'search_step': 1
                    }
        
        # STEP 2: Title fuzzy search + year (if available)
        if search_title and len(all_results) < 3:  # Only proceed if we don't have enough results
            filter_parts = [f'title.search:{search_title}']
            if year and year != 'N/A':
                try:
                    year_int = int(year)
                    filter_parts.append(f'publication_year:{year_int}')
                except (ValueError, TypeError):
                    pass
                
            params = {
                'filter': ','.join(filter_parts),
                'per-page': 5
            }
            
            results = call_openalex_api(params, "Step 2: Title fuzzy search + year")
            for result in results:
                if result is None:
                    continue
                    
                result_id = result.get('id')
                if result_id and result_id not in all_results:
                    all_results[result_id] = {
                        'result': result,
                        'search_step': 2
                    }
        
        # STEP 3: Search parameter (more flexible than filter)
        if search_title and len(all_results) < 3:  # Only proceed if we don't have enough results
            params = {
                'search': search_title,
                'per-page': 5
            }
            
            results = call_openalex_api(params, "Step 3: General search")
            for result in results:
                if result is None:
                    continue
                    
                result_id = result.get('id')
                if result_id and result_id not in all_results:
                    all_results[result_id] = {
                        'result': result,
                        'search_step': 3
                    }
        
        # If we have no results, return early
        if not all_results:
            logger.warning("No results found on OpenAlex after trying all strategies")
            return None, None
        
        # Calculate match scores for all results
        for result_id, result_info in all_results.items():
            result = result_info['result']
            
            # Get OpenAlex authors
            openalex_authors = []
            for authorship in result.get('authorships', []) or []:
                author_data = authorship.get('author', {}) or {}
                display_name = author_data.get('display_name')
                if display_name:
                    openalex_authors.append(display_name)
            
            # Calculate title similarity
            original_title = metadata.get('title', '').lower() if metadata.get('title') else ''
            openalex_title = result.get('display_name', '').lower() if result.get('display_name') else ''
            
            # Use fuzzy matching for title similarity
            if original_title and openalex_title:
                title_score = fuzz.ratio(original_title, openalex_title) / 100.0
            else:
                title_score = 0.0
                
            # Calculate author score
            author_score = calculate_author_match_score(authors, openalex_authors)
            
            # Calculate year match (exact match = 1.0, off by one = 0.8, off by two = 0.5, else 0)
            year_score = 0.0
            if year and year != 'N/A':
                try:
                    gemini_year = int(year)
                    openalex_year = result.get('publication_year')
                    if openalex_year:
                        year_diff = abs(gemini_year - openalex_year)
                        if year_diff == 0:
                            year_score = 1.0
                        elif year_diff == 1:
                            year_score = 0.8
                        elif year_diff <= 2:
                            year_score = 0.5
                except (ValueError, TypeError):
                    pass
                    
            # Weight and combine scores
            # Title is most important, then authors, then year, then search step (earlier = better)
            total_score = (
                title_score * 0.6 + 
                author_score * 0.3 + 
                year_score * 0.1
            )
            
            result_info['title_score'] = title_score
            result_info['author_score'] = author_score
            result_info['year_score'] = year_score
            result_info['total_score'] = total_score
        
        # Sort results by total score (descending)
        sorted_results = sorted(
            all_results.values(), 
            key=lambda x: x.get('total_score', 0), 
            reverse=True
        )
        
        # Debug the top matches
        for i, result_info in enumerate(sorted_results[:3]):
            if i >= len(sorted_results):
                break
            result = result_info.get('result', {})
            score = result_info.get('total_score', 0)
            logger.info(f"Match #{i+1}: {result.get('display_name', 'Unknown')} (Score: {score:.2f})")
        
        # Get the best match if score is high enough
        if sorted_results and sorted_results[0].get('total_score', 0) >= 0.6:  # Threshold for accepting match
            best_match = sorted_results[0]
            logger.info(f"Selected best match with score {best_match.get('total_score', 0):.2f}")
            
            # Get the DOI and metadata
            best_result = best_match.get('result', {}) or {}
            doi = best_result.get('doi')
            
            if doi:
                logger.info(f"Found DOI: {doi} (Score: {best_match.get('total_score', 0):.2f})")
                
                # Create enhanced metadata with fields from OpenAlex
                enhanced_metadata = {}
                
                # Title
                enhanced_metadata["title"] = best_result.get('display_name') or metadata.get('title', 'Unknown Title')
                
                # Publication Year
                enhanced_metadata["publication_year"] = best_result.get('publication_year') or metadata.get('publication_year', 'N/A')
                
                # Authors - extract from authorships
                authors = []
                for authorship in best_result.get('authorships', []) or []:
                    author_data = authorship.get('author', {}) or {}
                    display_name = author_data.get('display_name')
                    if display_name:
                        authors.append(display_name)
                enhanced_metadata["authors"] = authors if authors else metadata.get('authors', [])
                
                # Type
                enhanced_metadata["type"] = best_result.get('type') or metadata.get('type', 'other')
                
                # Journal or venue
                enhanced_metadata["journal"] = metadata.get('journal', 'N/A')
                primary_location = best_result.get('primary_location', {})
                if primary_location:
                    source = primary_location.get('source', {})
                    if source and source.get('type') == 'journal':
                        enhanced_metadata["journal"] = source.get('display_name') or metadata.get('journal', 'N/A')
                    
                # Abstract - OpenAlex abstracts can be very comprehensive
                enhanced_metadata["abstract"] = metadata.get('abstract', 'N/A')
                if 'abstract_inverted_index' in best_result:
                    try:
                        abstract_words = []
                        inverted_index = best_result['abstract_inverted_index']
                        for word, positions in inverted_index.items():
                            for pos in positions:
                                while len(abstract_words) <= pos:
                                    abstract_words.append("")
                                abstract_words[pos] = word
                        abstract_text = " ".join(abstract_words)
                        if abstract_text:
                            enhanced_metadata["abstract"] = abstract_text
                    except Exception as e:
                        logger.error(f"Error processing abstract inverted index: {e}")
                
                # Book title (OpenAlex doesn't clearly distinguish this)
                enhanced_metadata["book_title"] = metadata.get('book_title', 'N/A')
                
                # DOI as ID
                enhanced_metadata["id"] = doi
                
                return doi, enhanced_metadata
            else:
                logger.warning("Best match has no DOI")
        else:
            if sorted_results:
                logger.warning(f"Best match score ({sorted_results[0].get('total_score', 0):.2f}) below threshold")
            else:
                logger.warning("No results to score")
        
    except Exception as e:
        logger.error(f"Error in OpenAlex processing: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return None, None

def load_combined_metadata(output_dir):
    """Load existing combined metadata file if it exists"""
    combined_json_path = os.path.join(output_dir, "combined_metadata.json")
    if os.path.exists(combined_json_path):
        try:
            with open(combined_json_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading existing metadata file: {e}")
    
    # Return empty dict if file doesn't exist or can't be loaded
    return {}

def save_combined_metadata(metadata_dict, output_dir):
    """Save the combined metadata dictionary to a JSON file"""
    combined_json_path = os.path.join(output_dir, "combined_metadata.json")
    try:
        with open(combined_json_path, 'w') as f:
            json.dump(metadata_dict, f, indent=2)
        logger.info(f"Combined metadata saved to {combined_json_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving combined metadata: {e}")
        return False

def process_pdf(pdf_path, output_dir=None, combined_metadata=None):
    """Process a PDF file to extract metadata"""
    logger.info(f"Processing PDF: {pdf_path}")
    
    # Initialize APIs
    gemini = initialize_apis()
    
    # Set output directory to the same as input if not specified
    if not output_dir:
        output_dir = os.path.dirname(pdf_path)
    
    # Check if file is accessible
    if not os.path.exists(pdf_path):
        logger.error(f"PDF file not found: {pdf_path}")
        return False, None
    
    # Extract first pages from PDF using pikepdf
    pdf_content = extract_first_pages_with_pikepdf(pdf_path)
    
    # Get the original filename to use as the key
    original_filename = os.path.basename(pdf_path)
    
    if not pdf_content['pdf_path']:
        error_msg = pdf_content.get('error', 'Unknown error')
        logger.error(f"Could not extract pages from PDF: {error_msg}")
        
        # Create basic metadata for corrupted files so we at least record them
        if combined_metadata is not None:
            # Add an entry to the combined metadata for the corrupted file
            error_metadata = {
                "title": f"[CORRUPT PDF] {original_filename}",
                "abstract": f"This PDF file could not be processed due to corruption: {error_msg}",
                "publication_year": datetime.now().year,
                "authors": ["Unknown - Corrupted File"],
                "type": "other",
                "journal": "N/A",
                "book_title": "N/A",
                "id": "https://doi.org/unknown",
                "processing_error": error_msg,
                "original_filename": original_filename
            }
            
            return True, {original_filename: error_metadata}
        
        return False, None
    
    try:
        # Extract metadata with Gemini using multimodal analysis on PDF
        max_retries = 3
        retry_count = 0
        metadata = None
        
        while retry_count < max_retries:
            try:
                # Pass the original filename to the extraction function
                metadata = extract_metadata_with_gemini_multimodal(gemini, pdf_content, original_filename)
                break
            except Exception as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or "quota" in str(e).lower():
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(f"Failed metadata extraction after {max_retries} attempts")
                        metadata = get_default_metadata()
                        break
                    
                    # Wait with longer delay before retrying
                    delay = 60 * retry_count  # Progressively longer delays
                    logger.warning(f"Rate limit hit. Waiting {delay} seconds before retry {retry_count+1}/{max_retries}")
                    time.sleep(delay)
                else:
                    # For other errors, use default metadata
                    logger.error(f"Error in metadata extraction: {e}")
                    metadata = get_default_metadata()
                    break
        
        # Store the original Gemini metadata before trying to enhance with OpenAlex
        original_metadata = metadata.copy()
        
        # Try to get DOI and enhanced metadata from OpenAlex
        doi, enhanced_metadata = get_doi_and_metadata_from_openalex(metadata)
        
        if doi and enhanced_metadata:
            # Use the enhanced OpenAlex metadata when a good match is found
            logger.info(f"Found DOI and enhanced metadata in OpenAlex: {doi}")
            metadata = enhanced_metadata
        else:
            # If no DOI found, use a placeholder and stick with Gemini metadata
            metadata['id'] = "https://doi.org/unknown"
            logger.info("No DOI found in OpenAlex, using original Gemini metadata")
        
        # Add the original filename to the metadata
        metadata['original_filename'] = original_filename
        logger.info(f"Extracted metadata for {original_filename}")
        
        # Return the metadata with the full original filename as key
        return True, {original_filename: metadata}
            
    except Exception as e:
        logger.error(f"Error processing PDF: {e}")
        return False, None
    finally:
        # Clean up the temporary PDF file
        if pdf_content['pdf_path'] and os.path.exists(pdf_content['pdf_path']):
            try:
                os.remove(pdf_content['pdf_path'])
                logger.debug(f"Removed temporary PDF file: {pdf_content['pdf_path']}")
            except Exception as e:
                logger.warning(f"Failed to remove temporary PDF file: {e}")

def main():
    parser = argparse.ArgumentParser(description='Extract metadata from PDF files using Gemini API')
    parser.add_argument('pdf_path', help='Path to the PDF file or directory containing PDFs')
    parser.add_argument('--output-dir', help='Directory to save output files (default: same as input)')
    parser.add_argument('--skip-repair', action='store_true', help='Skip attempting to repair corrupted PDFs')
    
    args = parser.parse_args()
    
    # Set output directory
    output_dir = args.output_dir
    if not output_dir and os.path.isdir(args.pdf_path):
        output_dir = args.pdf_path
    elif not output_dir:
        output_dir = os.path.dirname(args.pdf_path)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Load existing combined metadata if available
    combined_metadata = load_combined_metadata(output_dir)
    metadata_updated = False
    
    # Process files with the repair option unless --skip-repair is specified
    repair_option = not args.skip_repair
    
    # Check if input is a file or directory
    if os.path.isfile(args.pdf_path):
        # Use full filename with extension to check if it already exists in metadata
        filename = os.path.basename(args.pdf_path)
        
        # Only check for full filename with extension
        if filename in combined_metadata:
            logger.info(f"Skipping {args.pdf_path} - already in metadata")
        else:
            # Process single file
            success, new_metadata = process_pdf(args.pdf_path, output_dir, combined_metadata)
            if success and new_metadata:
                combined_metadata.update(new_metadata)
                metadata_updated = True
    elif os.path.isdir(args.pdf_path):
        # Process all PDFs in directory
        for filename in os.listdir(args.pdf_path):
            if filename.lower().endswith('.pdf'):
                pdf_path = os.path.join(args.pdf_path, filename)
                
                # Only check for full filename with extension
                if filename in combined_metadata:
                    logger.info(f"Skipping {pdf_path} - already in metadata")
                else:
                    success, new_metadata = process_pdf(pdf_path, output_dir, combined_metadata)
                    if success and new_metadata:
                        combined_metadata.update(new_metadata)
                        metadata_updated = True
    else:
        logger.error(f"Input path does not exist: {args.pdf_path}")
        return
    
    # Save the combined metadata if it was updated
    if metadata_updated:
        save_combined_metadata(combined_metadata, output_dir)
        logger.info(f"Combined metadata contains {len(combined_metadata)} documents")

if __name__ == "__main__":
    main()