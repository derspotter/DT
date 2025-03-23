import json
import time
import requests
import os
import threading
from urllib.parse import urljoin
from pathlib import Path
from collections import deque
import fitz  # PyMuPDF
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

import google.cloud.aiplatform as aiplatform
from google import genai
from google.genai.types import (
    FunctionDeclaration,
    GenerateContentConfig,
    GoogleSearch,
    Part,
    Retrieval,
    SafetySetting,
    Tool,
    VertexAISearch,
)


# ANSI escape codes for colors
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"  # Resets the color to default

class ServiceRateLimiter:
    def __init__(self, rps_limit=None, rpm_limit=None, tpm_limit=None, quota_window_minutes=60):
        self.RPS_LIMIT = rps_limit
        self.RPM_LIMIT = rpm_limit
        self.TPM_LIMIT = tpm_limit
        self.QUOTA_WINDOW = quota_window_minutes
        
        # For RPS tracking (if needed)
        self.second_requests = deque() if rps_limit else None
        
        # For RPM tracking (if needed)
        self.minute_requests = deque() if rpm_limit else None
        
        # For token tracking (if needed)
        self.token_counts = deque() if tpm_limit else None
        
        self.quota_reset_time = None
        self.quota_exceeded = False
        self.lock = threading.Lock()
        self.backoff_time = 1

    def wait_if_needed(self, estimated_tokens=0):
        """Thread-safe rate limiting for both RPS and RPM with token tracking."""
        with self.lock:
            now = time.time()
            
            # Check quota exceeded state
            if self.quota_exceeded and self.quota_reset_time:
                if now < self.quota_reset_time:
                    wait_time = self.quota_reset_time - now
                    print(f"Quota exceeded, waiting {wait_time:.1f} seconds before retry...")
                    time.sleep(wait_time)
                    now = time.time()  # Update time after waiting
                self.quota_exceeded = False
                self.quota_reset_time = None
                self.backoff_time = 1

            # Handle RPS limiting if configured
            if self.RPS_LIMIT:
                second_ago = now - 1.0
                while self.second_requests and self.second_requests[0] < second_ago:
                    self.second_requests.popleft()
                
                if len(self.second_requests) >= self.RPS_LIMIT:
                    wait_time = self.second_requests[0] - second_ago
                    if wait_time > 0:
                        time.sleep(wait_time)
                        now = time.time()  # Update time after waiting

            # Handle RPM limiting if configured
            if self.RPM_LIMIT:
                minute_ago = now - 60.0
                while self.minute_requests and self.minute_requests[0] < minute_ago:
                    self.minute_requests.popleft()
                
                if len(self.minute_requests) >= self.RPM_LIMIT:
                    wait_time = self.minute_requests[0] - minute_ago
                    if wait_time > 0:
                        print(f"Rate limit approached, waiting {wait_time:.1f} seconds...")
                        time.sleep(wait_time)
                        now = time.time()  # Update time after waiting

            # Handle token limiting if configured
            if self.TPM_LIMIT and estimated_tokens > 0:
                minute_ago = now - 60.0
                while self.token_counts and self.token_counts[0][0] < minute_ago:
                    self.token_counts.popleft()
                
                total_tokens = sum(count[1] for count in self.token_counts)
                if total_tokens + estimated_tokens > self.TPM_LIMIT:
                    wait_time = self.token_counts[0][0] - minute_ago
                    if wait_time > 0:
                        print(f"Token limit approached, waiting {wait_time:.1f} seconds...")
                        time.sleep(wait_time)
                        now = time.time()  # Update time after waiting
                
                self.token_counts.append((now, estimated_tokens))

            # Record the request with final timestamp after all potential waits
            if self.second_requests is not None:
                self.second_requests.append(now)
            if self.minute_requests is not None:
                self.minute_requests.append(now)

    def handle_response_error(self, error):
        """Handle quota exceeded errors with exponential backoff."""
        if "RESOURCE_EXHAUSTED" in str(error) or "429" in str(error):
            with self.lock:
                wait_time = self.backoff_time
                self.backoff_time = min(self.backoff_time * 2, 3600)  # Max 1 hour
                self.quota_exceeded = True
                self.quota_reset_time = time.time() + wait_time
                return wait_time
        return None

    def reset_backoff(self):
        """Reset backoff time after successful request."""
        with self.lock:
            self.backoff_time = 1
            self.quota_exceeded = False

def is_reference_downloaded(reference: dict, enhanced_results_dir: str) -> bool:
    """
    Standalone function to check if a reference has already been downloaded.
    """
    
    def normalize_string(s):
        """Normalize string for comparison by removing spaces and converting to lowercase."""
        if not s:
            return ""
        return "".join(s.lower().split())
    
    def references_match(ref1, ref2):
        """Compare two references for matching based on key fields."""
        # DOI is the most reliable identifier if available
        if ref1.get('doi') and ref2.get('doi'):
            return ref1['doi'] == ref2['doi']
        
        # Check title, authors, and year
        title_match = normalize_string(ref1.get('title', '')) == normalize_string(ref2.get('title', ''))
        year_match = ref1.get('year') == ref2.get('year')
        
        # Compare authors lists
        authors1 = [normalize_string(a) for a in ref1.get('authors', [])]
        authors2 = [normalize_string(a) for a in ref2.get('authors', [])]
        authors_match = sorted(authors1) == sorted(authors2)
        
        return title_match and year_match and authors_match
    
    enhanced_results_dir = Path(enhanced_results_dir)
    
    # Find all JSON files recursively
    json_files = enhanced_results_dir.glob('**/*.json')
    
    # Check each file for matching references
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle both array and object formats
            references = data['references'] if isinstance(data, dict) else data
            
            # Check each reference in the file
            for ref in references:
                if 'downloaded_file' in ref and references_match(reference, ref):
                    return True
                    
        except Exception as e:
            print(f"Error checking file {file_path}: {e}")
            continue
    
    return False


class BibliographyEnhancer:
    def __init__(self, email):
        self.email = email
        self.unpaywall_headers = {'email': email}
        
        # Service-specific rate limiters
        self.gemini_limiter = ServiceRateLimiter(
            rpm_limit=100, 
            tpm_limit=4_000_000,
            quota_window_minutes=60
        )
        self.openalex_limiter = ServiceRateLimiter(
            rps_limit=10  # 10 requests per second
        )

        # PROJECT_ID = "your-project-id"
        # vertexai.init()
        
        # Initialize credentials
        #credentials_path = "decision-theater-ffc892202832.json"
        #if not os.path.exists(credentials_path):
        #   raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
        #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
                
        self.script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        
        # Initialize Vertex AI client
        #self.client = genai.Client(
        #    vertexai=True, project='decision-theater', location='europe-west2'
        #)

        # Set model ID
        self.MODEL_ID = "gemini-2.0-flash-exp"

        self.client = genai.Client(api_key=os.getenv('GOOGLE_API_KEY'))
        # Use Google Search for grounding
        self.google_search_tool = Tool(google_search=GoogleSearch())

    def search_and_evaluate_with_gemini(self, ref):
        """
        Perform a combined search and evaluation using Gemini with enhanced rate limiting.
        """
        title = ref.get('title', '')
        authors = ref.get('authors', [])
        year = ref.get('year', '')
        container = ref.get('container_title', '')

        query = f'"{title}" {", ".join(authors) if authors else ""} {year} (pdf OR filetype:pdf)'
        print(f"Searching and evaluating with Gemini for: {query}")

        # Define the search and evaluation prompt
        combined_prompt = f'''You are helping find exact PDF matches for an academic publication. Perform the following tasks:
        1. Search for the publication using Google Search.
        2. Evaluate the results to ensure they are complete PDFs, not partial content.

        Publication to find:   
        Title: {title}
        Authors: {', '.join(authors)}
        Year: {year}
        Container: {container}

        STRICT MATCHING RULES:
        1. DO NOT include ANY of these under any circumstances:
        - Table of contents only
        - Preview PDFs
        - Sample chapters
        - Partial content
        - Book previews
        - URLs containing preview/sample in the path
        - URLs from publisher preview pages
        - PDFs marked as "preview" or "sample"
        - Any content that is not the complete work itself
        - Other academic publications, even if they are related or by the same authors

        2. ONLY return URLs if you are CERTAIN they lead to the EXACT publication being searched for.

        3. If you see ANY indication that a result might be partial content (preview, TOC, etc.), DO NOT include it.

        4. If a URL contains terms like "preview", "toc", "contents", "sample" - DO NOT include it.

        5. Ensure that the result matches ALL authors listed in the reference. If any author is missing, DO NOT include the result.

        6. DO NOT include any other academic publications, even if they are by the same authors or have similar titles. ONLY return the exact publication being searched for.

        Return matches in this JSON format:
        [
            {{
                "url": "url",
                "title": "title",
                "snippet": "snippet",
                "source": "source",
                "reason": "Verified to be the exact publication"
            }}
        ]

        If not 100% certain a result is the exact publication, DO NOT include it. Return [] instead.'''

        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Wait for rate limiting if needed
                self.gemini_limiter.wait_if_needed(estimated_tokens=1000)  # Conservative token estimate

                response = self.client.models.generate_content(
                    contents=combined_prompt,
                    model=self.MODEL_ID,
                    config=GenerateContentConfig(
                        temperature=0.0,
                        response_modalities=["TEXT"],
                        tools=[self.google_search_tool],
                    )
                )

                # Reset backoff on successful request
                self.gemini_limiter.reset_backoff()

                # Process response
                response_text = response.text.strip()
                response_text = response_text.replace('```json', '').replace('```', '').strip()

                try:
                    matches = json.loads(response_text)
                    if isinstance(matches, list):
                        print(f"Found {len(matches)} potential PDF matches after evaluation")
                        return matches
                    else:
                        print("Response was not a list of matches")
                        return []
                except json.JSONDecodeError as e:
                    print(f"JSON parse error: {e}")
                    print(f"Response was: {response_text}")
                    return []

            except Exception as e:
                wait_time = self.gemini_limiter.handle_response_error(e)
                if wait_time:
                    print(f"Attempt {retry_count + 1} failed. Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    retry_count += 1
                else:
                    print(f"Error searching and evaluating with Gemini: {e}")
                    return []

        print(f"Failed after {max_retries} attempts")
        return []

    def check_unpaywall(self, doi):
        if not doi:
            return None
            
        url = f'https://api.unpaywall.org/v2/{doi}?email={self.email}'
        
        try:
            response = requests.get(url)
            if response.status_code == 422:
                print(f"Invalid DOI format for Unpaywall: {doi}")
                return None
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error for {doi}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {doi}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error for {doi}: {e}")
            return None


    def search_with_scihub(self, doi):
            """
            Search for a paper on Sci-Hub using its DOI and retrieve the download link.
            Returns immediately after finding a working mirror.
            """
            if not doi:
                return None

            # Try multiple Sci-Hub mirrors
            scihub_mirrors = [
                "https://sci-hub.se",
                "https://sci-hub.st",
                "https://sci-hub.ru"
            ]

            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1'
            }

            for mirror in scihub_mirrors:
                scihub_url = f"{mirror}/{doi}"
                print(f"Searching Sci-Hub mirror: {mirror}")

                try:
                    response = requests.get(scihub_url, headers=headers, timeout=30)
                    if response.status_code == 404:
                        print(f"DOI not found on {mirror}")
                        continue
                    response.raise_for_status()

                    # Parse the HTML response to find the PDF link
                    soup = BeautifulSoup(response.text, 'html.parser')
                    pdf_link = None
                    
                    # Check embed tag first
                    embed = soup.find('embed', {'type': 'application/pdf'})
                    if embed and embed.get('src'):
                        pdf_link = embed['src']
                    
                    # If no embed, check iframe
                    if not pdf_link:
                        iframe = soup.find('iframe', {'id': 'pdf'})
                        if iframe and iframe.get('src'):
                            pdf_link = iframe['src']
                    
                    # If still no link, check for download button
                    if not pdf_link:
                        save_button = soup.find('button', string='↓ save')
                        if save_button:
                            onclick = save_button.get('onclick', '')
                            if "location.href='" in onclick:
                                pdf_link = onclick.split("location.href='")[1].split("'")[0]
                    
                    # If we found any kind of link, process it
                    if pdf_link:
                        # Handle relative URLs
                        if not pdf_link.startswith('http'):
                            if pdf_link.startswith('//'):
                                pdf_link = f"https:{pdf_link}"
                            elif pdf_link.startswith('/'):
                                pdf_link = f"{mirror}{pdf_link}"
                            else:
                                pdf_link = f"{mirror}/{pdf_link}"
                                
                        print(f"Found PDF link: {pdf_link}")
                        # Return immediately when we find a working mirror
                        return {
                            'url': pdf_link,
                            'source': 'Sci-Hub',
                            'reason': 'DOI resolved to PDF via Sci-Hub'
                        }
                    
                    print(f"No PDF link found on {mirror}")

                except requests.exceptions.ConnectionError:
                    print(f"Cannot connect to {mirror}")
                except requests.exceptions.Timeout:
                    print(f"Timeout connecting to {mirror}")
                except requests.exceptions.RequestException as e:
                    print(f"Error accessing {mirror}: {e}")

            # Only reaches here if all mirrors failed
            print("No working Sci-Hub mirrors found")
            return None
       
    def search_with_libgen(self, ref):
        """Search for publications on LibGen and extract working download links, prioritizing PDFs."""
        # Inside the search_with_libgen function, just change how we build the query:
        title = ref.get('title', '')
        authors = ref.get('authors', [])
        ref_type = ref.get('type', '').lower()  # Get reference type
        is_book = ref_type in ['book', 'monograph']

        # Get just the surname (last word) of first author
        author_surname = authors[0].split()[-1] if authors else ""

        query = f'{title} {author_surname}'
        query = query.replace(' ', '+')
        base_url = "https://libgen.la"
        libgen_url = f"{base_url}/index.php?req={query}&lg_topic=libgen&open=0&view=simple&res=25&phrase=1&column=def"
        
        print(f"Searching LibGen for: {title}")

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1'
            }
            response = requests.get(libgen_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            matches = []
            pdf_matches = []
            other_matches = []

            # Find the results table
            table = soup.find('table', {'id': 'tablelibgen'})
            if not table:
                return []

            # Process rows (skip header)
            rows = table.find_all('tr')[1:]  # Skip header row
            print(f"Found {len(rows)} results to process")
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 9:
                    continue
                    
                result_title = cells[0].get_text(strip=True)
                result_author = cells[1].get_text(strip=True)
                file_ext = cells[7].get_text(strip=True).lower()
                
                # Skip if it's a review
                if "Review by:" in result_author:
                    continue
                    
                # Skip if book title contains review markers
                if is_book:
                    review_markers = [
                        "vol.", "iss.", "pp.", "pages",
                        "Review of", "Book Review",
                        ") pp.", ") p.",
                    ]
                    if any(marker in result_title for marker in review_markers):
                        continue
                
                # Get mirror links
                mirrors_cell = cells[-1]
                for link in mirrors_cell.find_all('a'):
                    href = link.get('href', '')
                    source = link.get('data-original-title', '')
                    if href:
                        # Handle relative URLs
                        if not href.startswith('http'):
                            href = f"{base_url}{href if href.startswith('/') else '/' + href}"
                            
                        match_entry = {
                            'url': href,
                            'title': result_title,
                            'authors': result_author,
                            'source': f'LibGen ({source})',
                            'reason': f'{file_ext.upper()} download'
                        }
                        
                        # Separate PDF and non-PDF matches
                        if file_ext == 'pdf':
                            pdf_matches.append(match_entry)
                        else:
                            other_matches.append(match_entry)

            # Combine PDF matches first, then other formats
            matches = pdf_matches + other_matches
            print(f"Found {len(matches)} non-review download links ({len(pdf_matches)} PDFs)")
            return matches

        except Exception as e:
            print(f"Error searching LibGen: {e}")
            return []
        
    def is_likely_pdf_url(self, url):
        url_lower = url.lower()
        
        # General PDF checks
        if any([
            url_lower.endswith('.pdf'),
            'pdf' in url_lower and '/download' in url_lower,
            'pdf' in url_lower and '/view' in url_lower,
            '/pdf/' in url_lower,
            'pdf' in url_lower and 'download' in url_lower,
        ]):
            return True
        
        return False
        

    def try_extract_pdf_link(self, html_url):
        try:
            print(f"  Checking HTML page for PDF link: {html_url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1'
            }
            response = requests.get(html_url, headers=headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for PDF links first
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if self.is_likely_pdf_url(href):
                    print(f"  Found PDF link: {href}")
                    found_link = href
                    break
            else:
                # If no PDF links, try GET button
                get_button = soup.find('a', href=True, string='GET')
                if get_button:
                    found_link = get_button['href']
                    print(f"  Found GET button link: {found_link}")
                else:
                    return None

            # Handle relative URLs in one place
            if not found_link.startswith(('http://', 'https://')):
                found_link = urljoin(response.url, found_link)
                
            return found_link

        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error checking HTML page: {e}")
            return None
        except Exception as e:
            print(f"  ✗ Unexpected error checking HTML page: {e}")
            return None
            
    def is_valid_pdf(self, content, ref_type):
        """Validate PDF using PyMuPDF with improved error handling for CSS issues."""
        try:
            # Handle string/bytes conversion
            if isinstance(content, str):
                content = content.encode('utf-8')

            # Use context manager to ensure proper cleanup
            with fitz.open(stream=content, filetype="pdf") as doc:
                # Check if PDF is encrypted
                if doc.is_encrypted:
                    print("  ✗ PDF is encrypted")
                    return False

                # Basic page count validation
                num_pages = doc.page_count
                min_pages = 50 if ref_type.lower() == 'book' else 5
                
                if num_pages < min_pages:
                    print(f"  ✗ PDF too short ({num_pages} pages, minimum {min_pages})")
                    return False

                # Success - valid PDF
                print(f"  ✓ Valid PDF with {num_pages} pages")
                return True

        except fitz.FileDataError as e:
            error_msg = str(e).lower()
            # Ignore CSS-related errors as they don't affect PDF validity
            if "css syntax error" in error_msg:
                print("  ⚠ Ignoring CSS validation warnings")
                return True
            print(f"  ✗ PDF parsing error: {str(e)}")
            return False
        except Exception as e:
            print(f"  ✗ Unexpected error validating PDF: {str(e)}")
            return False

    def download_paper(self, url, filename, ref_type):
        """Enhanced paper downloader with better error handling"""
        if not url:
            return False
                        
        print(f"  Loading: {url}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1',
                'Accept': 'text/html,application/pdf,application/x-pdf,*/*'
            }
            
            # Add timeout to request
            response = requests.get(url, allow_redirects=True, headers=headers, timeout=30)
            
            # Check for specific HTTP error codes
            if response.status_code == 404:
                print("  ✗ URL not found (404)")
                return False
            elif response.status_code == 403:
                print("  ✗ Access forbidden (403)")
                return False
            elif response.status_code == 429:
                print("  ✗ Too many requests (429) - Rate limited")
                return False
            elif response.status_code != 200:
                print(f"  ✗ HTTP error {response.status_code}")
                return False
                    
            content = response.content
            
            # Enhanced PDF validation
            if self.is_valid_pdf(content, ref_type):
                with open(filename, 'wb') as f:
                    f.write(content)
                print("  ✓ Direct download successful - Valid complete PDF")
                return True
            else:
                print("  ✗ Downloaded file is not a valid complete PDF")
                
                # If it seems to be HTML, try extracting PDF link
                if 'text/html' in response.headers.get('Content-Type', '').lower():
                    pdf_url = self.try_extract_pdf_link(url)
                    if pdf_url and pdf_url != url:
                        return self.download_paper(pdf_url, filename, ref_type)
            
            return False
                
        except requests.exceptions.ConnectionError:
            print("  ✗ Connection error - Could not reach server")
            return False
        except requests.exceptions.Timeout:
            print("  ✗ Request timed out")
            return False
        except requests.exceptions.TooManyRedirects:
            print("  ✗ Too many redirects")
            return False
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Request failed: {str(e)}")
            return False
        except Exception as e:
            print(f"  ✗ Unexpected error: {str(e)}")
            return False

    def download_referenced_works(self, ref, downloads_dir):
        """
        Download referenced works by converting them to bibliography format and using enhance_bibliography.
        """
        referenced_works = ref.get('referenced_works', [])
        if not referenced_works:
            print("No referenced works found")
            return

        print(f"\nProcessing {len(referenced_works)} referenced works...")
        
        # Create a subdirectory for referenced works
        ref_dir = downloads_dir / 'referenced_works'
        ref_dir.mkdir(exist_ok=True)
        
        # Create a bibliography file for the referenced works
        bibliography = {'references': []}
        
        for work_id in referenced_works:
            print(f"\nProcessing referenced work: {work_id}")
            
            try:
                # Apply rate limiting before OpenAlex API call
                self.openalex_limiter.wait_if_needed()

                # Query OpenAlex for work details
                actual_id = work_id.split('/')[-1] if 'openalex.org/' in work_id else work_id
                url = f"https://api.openalex.org/{actual_id}?mailto={self.email}"
                response = requests.get(url, headers={'User-Agent': 'BibliographyEnhancer'})
                response.raise_for_status()
                work = response.json()
                
                # Only skip if we got no response at all
                if not work:
                    print(f"No data returned for work {work_id}")
                    continue

                # First safely extract the nested structures
                primary_location = work.get('primary_location') or {}      # If primary_location is None, use empty dict
                source = primary_location.get('source') or {}              # If source is None, use empty dict
                open_access = work.get('open_access') or {}                # If open_access is None, use empty dict
                authorships = work.get('authorships') or []                # If authorships is None, use empty list

                # Handle authors list separately since it's more complex
                authors = []
                for authorship in authorships:
                    if authorship:  # Check if authorship entry exists
                        author = authorship.get('author')
                        if author:  # Check if author exists
                            display_name = author.get('display_name')
                            if display_name:  # Check if display_name exists
                                authors.append(display_name)

                # Now build the reference work safely
                ref_work = {
                    'title': work.get('title',''),                 # Simple field - will be '' if missing
                    'year': work.get('publication_year'),          # Simple field - will be None if missing
                    'doi': work.get('doi'),                        # Simple field - will be None if missing
                    'type': work.get('type'),                      # Simple field - will be None if missing
                    'open_access_url': open_access.get('oa_url'),  # Using our safely extracted open_access
                    'authors': authors,                            # Using our safely built authors list
                    'container_title': source.get('display_name'),  # Using our safely extracted source
                    'url': open_access.get('oa_url')               # Using our safely extracted open_access
                }
                
                bibliography['references'].append(ref_work)
                            
            except Exception as e:
                print(f"Error processing referenced work {work_id}: {e}")
                continue
        
        if bibliography['references']:
            # Create the bibliography file named after the root work
            safe_title = "".join(c for c in ((ref or {}).get('title', '') or 'Untitled')[:50] if c.isalnum() or c in ' -_')
            referenced_works_file = ref_dir / f"{ref.get('year', 'unknown')}_{safe_title}_referenced_works.json"
            
            # Save the referenced works bibliography
            with open(referenced_works_file, 'w', encoding='utf-8') as f:
                json.dump(bibliography, f, indent=2)
                
            print(f"Saved referenced works bibliography to: {referenced_works_file}")
            
            # Use enhance_bibliography to download all referenced works
            enhanced_file = self.enhance_bibliography(referenced_works_file, output_dir=ref_dir)
            
            # Update the original reference with results
            with open(enhanced_file, 'r', encoding='utf-8') as f:
                enhanced_data = json.load(f)
            
            downloaded_refs = []
            for enhanced_ref in enhanced_data['references']:
                if 'downloaded_file' in enhanced_ref:
                    downloaded_refs.append({
                        'title': enhanced_ref.get('title','Untitled'),
                        'source': enhanced_ref.get('download_source'),
                        'file': enhanced_ref.get('downloaded_file')
                    })
            
            if downloaded_refs:
                ref['downloaded_referenced_works'] = downloaded_refs
                ref['referenced_works_bibliography'] = str(referenced_works_file)
                print(f"\nSuccessfully downloaded {len(downloaded_refs)} referenced works")
            else:
                print("\nNo referenced works could be downloaded")
        else:
            print("No valid referenced works to process")       

    def enhance_bibliography(self, json_file, output_dir=None):
        """
        Enhanced version of bibliography processor with the following order:
        1. Direct sources (direct URLs, DOI URLs, Unpaywall URLs, OpenAlex URLs).
        2. Combined search and evaluation with Gemini.
        3. Sci-Hub (using DOI).
        4. LibGen.
        """
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        references = data['references'] if isinstance(data, dict) and 'references' in data else data
            
        if output_dir:
            downloads_dir = Path(output_dir) / 'downloads'
            output_dir_path = Path(output_dir)
        else:
            downloads_dir = self.script_dir / 'downloads'
            output_dir_path = self.script_dir / 'enhanced_results'
        
        downloads_dir.mkdir(parents=True, exist_ok=True)
        output_dir_path.mkdir(parents=True, exist_ok=True)
        
        for ref in references:

            if is_reference_downloaded(ref, str(output_dir_path)):
                print(f"\nSkipping already downloaded: {ref.get('title','Untitled')}")
                continue

            print(f"\nProcessing: {ref.get('title','Untitled')}")
            print("DEBUG - Full reference content:", ref)  # Add it here
            print(f"Authors: {', '.join(ref.get('authors', []))}")
            print(f"Year: {ref.get('year')}")
            
            
            urls_to_try = []
            
            # 1. Try direct URL first if it exists
            if ref.get('url'):
                print("Found direct URL in reference")
                urls_to_try.append({
                    'url': ref['url'],
                    'title': ref.get('title', ''),
                    'snippet': '',
                    'source': 'Direct URL',
                    'reason': 'Direct URL from reference'
                })
            
            # 1. Try DOI resolution
            if ref.get('doi'):
                doi_url = self.clean_doi_url(ref['doi'])
                urls_to_try.append({
                    'url': doi_url,
                    'title': ref.get('title', ''),
                    'snippet': '',
                    'source': 'DOI',
                    'reason': 'DOI resolution'
                })
                
                # 1. Check Unpaywall for open access versions
                unpaywall_result = self.check_unpaywall(ref['doi'])
                if unpaywall_result and unpaywall_result.get('oa_locations'):
                    for location in unpaywall_result['oa_locations']:
                        pdf_url = location.get('url_for_pdf') or location.get('url')
                        if pdf_url:
                            urls_to_try.append({
                                'url': pdf_url,
                                'title': ref.get('title', ''),
                                'snippet': '',
                                'source': 'Unpaywall',
                                'reason': f"Open access version from {location.get('repository_institution', 'Unpaywall')}"
                            })

            # 1. Try OpenAlex URL
            if ref.get('open_access_url'):
                urls_to_try.append({
                    'url': ref['open_access_url'],
                    'title': ref.get('title', ''),
                    'snippet': '',
                    'source': 'OpenAlex',
                    'reason': 'Open access URL from OpenAlex'
                })
            
            # Try downloading from direct sources first
            download_success = False
            for url_info in urls_to_try:
                if self.try_download_url(url_info['url'], url_info['source'], ref, downloads_dir):
                    ref['download_source'] = url_info['source']
                    ref['download_reason'] = url_info['reason']
                    download_success = True
                    print(f"Downloaded from: {url_info['source']}")
                    break
            
            # 2. If direct methods fail, try combined search and evaluation with Gemini
            if not download_success:
                print("Direct methods failed. Trying combined search and evaluation with Gemini...")
                gemini_matches = self.search_and_evaluate_with_gemini(ref)
                
                if gemini_matches:
                    print("\nPotential matches found from Gemini:")
                    for match in gemini_matches:
                        print(f"- {match.get('title', 'No title')}")
                        print(f"  URL: {match.get('url', 'No URL')}")
                        print(f"  Reason: {match.get('reason', 'No reason provided')}")
                        print(f"  Source: {match.get('source', 'Unknown')}")
                        
                        if match.get('url'):  # Only try if URL exists
                            if self.try_download_url(match['url'], match.get('source', 'Gemini'), ref, downloads_dir):
                                ref['download_source'] = match.get('source', 'Gemini')
                                ref['download_reason'] = match.get('reason', 'Successfully downloaded')
                                download_success = True
                                print(f"Downloaded from: {match.get('source', 'Gemini')}")
                                break
            
            # 3. If Gemini fails, try Sci-Hub (using DOI)
            if not download_success and ref.get('doi'):
                print("Gemini failed. Trying Sci-Hub...")
                scihub_result = self.search_with_scihub(ref['doi'])
                
                if scihub_result:
                    print(f"\nFound PDF link on Sci-Hub: {scihub_result['url']}")
                    if self.try_download_url(scihub_result['url'], scihub_result['source'], ref, downloads_dir):
                        ref['download_source'] = scihub_result['source']
                        ref['download_reason'] = scihub_result['reason']
                        download_success = True
                        print(f"Downloaded from: {scihub_result['source']}")
            
            # 4. If Sci-Hub fails, try LibGen
            if not download_success:
                print("Sci-Hub failed. Trying LibGen...")
                libgen_matches = self.search_with_libgen(ref)
                
                if libgen_matches:
                    print("\nPotential matches found on LibGen:")
                    for match in libgen_matches:
                        print(f"- {match.get('title', 'No title')}")
                        print(f"  URL: {match.get('url', 'No URL')}")
                        print(f"  Reason: {match.get('reason', 'No reason provided')}")
                        print(f"  Source: {match.get('source', 'Unknown')}")
                        
                        if match.get('url'):  # Only try if URL exists
                            if self.try_download_url(match['url'], match.get('source', "LibGen"), ref, downloads_dir):
                                ref['download_source'] = match.get('source', "LibGen")
                                ref['download_reason'] = match.get('reason', 'Successfully downloaded')
                                download_success = True
                                print(f"Downloaded from: {match.get('source', 'LibGen')}")
                                break
            
            # If all methods fail, mark as not found
            if not download_success:
                print(f"  {RED}✗ No valid matches found through any method{RESET}")
                ref['download_status'] = 'not_found'
                ref['download_reason'] = 'No valid matches found through any method'

            if ref.get('referenced_works'):
                print("\nProcessing referenced works...")
                self.download_referenced_works(ref, downloads_dir)

            ##input("Press Enter to continue to the next reference...")

        # Save enhanced bibliography
        output_file = output_dir_path / f"enhanced_{json_file.name}"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Print statements for verification
        print(f"downloads_dir: {downloads_dir}")
        print(f"output_dir_path: {output_dir_path}")

        # Print summary
        total_refs = len(references)
        downloaded = sum(1 for ref in references if 'downloaded_file' in ref)
        print(f"\nSummary:")
        print(f"Total references: {total_refs}")
        print(f"Successfully downloaded: {downloaded}")
        print(f"Success rate: {(downloaded/total_refs)*100:.1f}%")
        
        return output_file

    def clean_doi_url(self, doi):
        """Clean and format DOI URL correctly"""
        # Remove any duplicate doi.org prefixes
        doi = doi.replace('https://doi.org/https://doi.org/', 'https://doi.org/')
        doi = doi.replace('http://doi.org/http://doi.org/', 'https://doi.org/')
        
        # If it's just the DOI number, add the prefix
        if not doi.startswith('http'):
            doi = f"https://doi.org/{doi}"
            
        return doi


    def try_download_url(self, url, source, ref, downloads_dir):
        safe_title = "".join(c for c in ((ref or {}).get('title', '') or 'Untitled')[:50] if c.isalnum() or c in ' -_')
        filename = downloads_dir / f"{ref.get('year', 'unknown')}_{safe_title}.pdf"
        ref_type = ref.get('type', '')
        
        print(f"\nTrying {source}: {url}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1',
                'Accept': 'text/html,application/pdf,application/x-pdf,*/*'
            }
            
            response = requests.get(url, allow_redirects=True, headers=headers, timeout=30)
            
            if response.status_code == 404:
                print("  ✗ URL not found (404)")
                return False
            elif response.status_code == 403:
                print("  ✗ Access forbidden (403)")
                return False
            elif response.status_code == 429:
                print("  ✗ Too many requests (429) - Rate limited")
                return False
            elif response.status_code != 200:
                print(f"  ✗ HTTP error {response.status_code}")
                return False
                        
            content = response.content

            if source == 'Direct URL' and url.lower().endswith(('.htm', '.html')):
                filename = filename.with_suffix('.html')  # Change extension to .html
                with open(filename, 'wb') as f:
                    f.write(content)
                print(f"  {GREEN}✓ Direct HTML download successful{RESET}")
                ref['downloaded_file'] = str(filename)
                ref['download_source'] = source
                return True
            
            # If it's a PDF, validate it
            if self.is_valid_pdf(content, ref_type):
                with open(filename, 'wb') as f:
                    f.write(content)
                print(f"  {GREEN}✓ Direct download successful - Valid PDF{RESET}")
                ref['downloaded_file'] = str(filename)
                ref['download_source'] = source
                return True
            else:
                print("  ✗ Downloaded file is not a valid PDF")
                
                # If it seems to be HTML, try extracting PDF link
                if 'text/html' in response.headers.get('Content-Type', '').lower():
                    pdf_url = self.try_extract_pdf_link(url)
                    if pdf_url and pdf_url != url:
                        return self.try_download_url(pdf_url, source, ref, downloads_dir)
            
            return False
        except requests.exceptions.ConnectionError:
            print("  ✗ Connection error - Could not reach server")
            return False
        except requests.exceptions.Timeout:
            print("  ✗ Request timed out")
            return False
        except requests.exceptions.TooManyRedirects:
            print("  ✗ Too many redirects")
            return False
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Request failed: {str(e)}")
            return False
        except Exception as e:
            print(f"  ✗ Unexpected error: {str(e)}")
            if filename.exists():
                filename.unlink()
            return False
    
def process_file(json_file, enhancer, done_dir):
    """Process a single JSON file and move it to the done directory."""
    print(f"\nEnhancing {json_file.name}")
    try:
        # Process the file
        enhanced_file = enhancer.enhance_bibliography(json_file)
        print(f"Saved enhanced bibliography to {enhanced_file}")
        
        # Move original file to done directory
        target_path = done_dir / json_file.name
        json_file.rename(target_path)
        print(f"Moved {json_file.name} to done directory")
    except Exception as e:
        print(f"Error processing {json_file.name}: {e}")
        # Optionally, you could move failed files to a separate 'failed' directory


def main():    
    enhancer = BibliographyEnhancer(email="spott@wzb.eu")
    search_results_dir = Path('new_search_results')
    done_dir = Path('done_search_results')
    done_dir.mkdir(exist_ok=True)

    processed_files = {f.name for f in done_dir.glob('*.json')}
    
    # Collect files that haven't been processed yet
    files_to_process = [json_file for json_file in search_results_dir.glob('*.json') if json_file.name not in processed_files]
    
    # Use ThreadPoolExecutor to process files concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers based on your system's capabilities
        futures = {executor.submit(process_file, json_file, enhancer, done_dir): json_file for json_file in files_to_process}
        
        for future in as_completed(futures):
            json_file = futures[future]
            try:
                future.result()  # This will raise any exceptions that occurred during processing
            except Exception as e:
                print(f"Error processing {json_file.name}: {e}")

if __name__ == "__main__":
    main()