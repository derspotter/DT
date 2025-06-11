import json
import time
import requests
import os
import threading
import sqlite3  
import copy
from urllib.parse import urljoin, urlparse
from pathlib import Path
from collections import deque
import fitz  
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from queue import Queue
import colorama
colorama.init(autoreset=True) # Makes colors reset automatically
GREEN = colorama.Fore.GREEN
RED = colorama.Fore.RED
YELLOW = colorama.Fore.YELLOW
RESET = colorama.Style.RESET_ALL # Use colorama's reset

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

# Add Pydantic for structured output with Gemini
try:
    from pydantic import BaseModel, Field
    from typing import List, Optional
    PYDANTIC_AVAILABLE = True
    
    # Define model for structured Gemini output
    class PdfMatch(BaseModel):
        url: str = Field(description="URL to the PDF file")
        title: str = Field(description="Title of the publication")
        snippet: Optional[str] = Field(default="", description="A short description or snippet")
        source: str = Field(default="Gemini", description="Source of the PDF")
        reason: str = Field(default="Verified to be the exact publication", description="Reason for including this match")
        
    class PdfMatchListContainer(BaseModel):
        matches: List[PdfMatch] = Field(description="List of PDF matches")
        
except ImportError:
    PYDANTIC_AVAILABLE = False
    print("Pydantic not available. Structured output for Gemini API will not be used.")

import datetime  # Added missing import

# ANSI escape codes for colors
# GREEN = "\033[92m"
# RED = "\033[91m"
# RESET = "\033[0m"  

class ServiceRateLimiter:
    def __init__(self, rps_limit=None, rpm_limit=None, tpm_limit=None, rpd_limit=None, quota_window_minutes=60):
        self.RPS_LIMIT = rps_limit
        self.RPM_LIMIT = rpm_limit
        self.TPM_LIMIT = tpm_limit
        self.RPD_LIMIT = rpd_limit  # New: Requests per day limit
        self.QUOTA_WINDOW = quota_window_minutes
        
        # For RPS tracking (if needed)
        self.second_requests = deque() if rps_limit else None
        
        # For RPM tracking (if needed)
        self.minute_requests = deque() if rpm_limit else None
        
        # For token tracking (if needed)
        self.token_counts = deque() if tpm_limit else None
        
        # For daily request tracking (if needed)
        self.daily_requests = deque() if rpd_limit else None
        self.current_day = None
        
        self.quota_reset_time = None
        self.quota_exceeded = False
        self.daily_limit_exceeded = False  # New: Track if daily limit exceeded
        self.lock = threading.Lock()
        self.backoff_time = 1

    def wait_if_needed(self, estimated_tokens=0):
        """Thread-safe rate limiting for RPS, RPM, TPM with token tracking and daily limits."""
        with self.lock:
            now = time.time()
            now_datetime = datetime.datetime.now() # Define datetime object
            today = now_datetime.date() # Use the datetime object for consistency
            
            # Use id(self) to distinguish limiter instances in debug output
            limiter_id = id(self)
            print(f"DEBUG RateLimiter ({limiter_id}): Entering wait_if_needed. Current day: {self.current_day}, Today: {today}") # DEBUG

            # Check and update current day for daily limits
            if self.RPD_LIMIT:
                if self.current_day != today:
                    print(f"DEBUG RateLimiter ({limiter_id}): New day detected. Resetting daily count ({len(self.daily_requests)}) and flag ({self.daily_limit_exceeded}). RPD_LIMIT={self.RPD_LIMIT}") # DEBUG
                    self.current_day = today
                    self.daily_requests.clear()
                    self.daily_limit_exceeded = False

                # Check if daily limit is exceeded
                print(f"DEBUG RateLimiter ({limiter_id}): Checking daily_limit_exceeded flag: {self.daily_limit_exceeded}") # DEBUG
                if self.daily_limit_exceeded:
                    print(f"DEBUG RateLimiter ({limiter_id}): Daily limit flag is True, returning False. RPD_LIMIT={self.RPD_LIMIT}") # DEBUG
                    return False

                # Remove timestamps from previous days
                removed_count = 0
                while self.daily_requests and (today - self.daily_requests[0].date()).days > 0:
                    self.daily_requests.popleft()
                    removed_count += 1
                if removed_count > 0:
                    print(f"DEBUG RateLimiter ({limiter_id}): Removed {removed_count} old daily timestamps.") # DEBUG

                # Check if the current count meets or exceeds the limit
                current_daily_count = len(self.daily_requests)
                print(f"DEBUG RateLimiter ({limiter_id}): Checking daily count: {current_daily_count} >= {self.RPD_LIMIT}?") # DEBUG
                if current_daily_count >= self.RPD_LIMIT:
                    print(f"DEBUG RateLimiter ({limiter_id}): Daily limit count reached ({current_daily_count}). Setting flag and returning False. RPD_LIMIT={self.RPD_LIMIT}") # DEBUG
                    self.daily_limit_exceeded = True
                    return False
            
            # Check quota exceeded state
            if self.quota_exceeded and self.quota_reset_time:
                if now < self.quota_reset_time:
                    wait_time = self.quota_reset_time - now
                    print(f"Quota exceeded, waiting {wait_time:.1f} seconds before retry...")
                    time.sleep(wait_time)
                    now = time.time()  
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
                        now = time.time()  

            # Handle RPM limiting if configured
            if self.RPM_LIMIT:
                minute_ago = now - 60.0
                while self.minute_requests and self.minute_requests[0] < minute_ago:
                    self.minute_requests.popleft()
                
                if len(self.minute_requests) >= self.RPM_LIMIT:
                    wait_time = 60.0 - (now - self.minute_requests[0])
                    print(f"RPM limit reached, waiting {wait_time:.1f} seconds...")
                    time.sleep(wait_time)
                    now = time.time()

            # Handle token limiting if configured
            if self.TPM_LIMIT and estimated_tokens > 0:
                current_minute = int(now / 60)
                while self.token_counts and self.token_counts[0][0] < current_minute - 1:
                    self.token_counts.popleft()
                
                current_tokens = sum(count for _, count in self.token_counts)
                if current_tokens + estimated_tokens > self.TPM_LIMIT:
                    wait_time = 60.0 - (now % 60.0) + 1.0  # Wait until next minute plus 1s buffer
                    print(f"TPM limit reached, waiting {wait_time:.1f} seconds...")
                    time.sleep(wait_time)
                    # Update the time and clear old entries after waiting
                    now = time.time()
                    current_minute = int(now / 60)
                    while self.token_counts and self.token_counts[0][0] < current_minute - 1:
                        self.token_counts.popleft()
                        
                # Add tokens to the current minute
                if self.token_counts and self.token_counts[-1][0] == current_minute:
                    self.token_counts[-1] = (current_minute, self.token_counts[-1][1] + estimated_tokens)
                else:
                    self.token_counts.append((current_minute, estimated_tokens))
                    
            # Update tracking for all request types
            #now_datetime = datetime.datetime.now() # Removed redundant line
            
            # Track requests per second
            if self.RPS_LIMIT:
                self.second_requests.append(now)
                
            # Track requests per minute
            if self.RPM_LIMIT:
                self.minute_requests.append(now)
                
            # Track requests per day
            if self.RPD_LIMIT:
                self.daily_requests.append(now_datetime) # Use the defined datetime object
            
            return True # Proceed with the request

    def handle_response_error(self, error):
        """Handle quota exceeded errors with exponential backoff."""
        if "RESOURCE_EXHAUSTED" in str(error) or "429" in str(error):
            with self.lock:
                wait_time = self.backoff_time
                self.backoff_time = min(self.backoff_time * 2, 3600)  
                self.quota_exceeded = True
                self.quota_reset_time = time.time() + wait_time
                return wait_time
        return None

    def reset_backoff(self):
        """Reset backoff time after successful request."""
        with self.lock:
            self.backoff_time = 1
            self.quota_exceeded = False
            self.quota_reset_time = None

def is_reference_downloaded(reference: dict, output_dir: Path, db_conn: sqlite3.Connection, check_input_table: bool = False) -> bool:
    """Check if a reference exists in current_references, previous_references, or optionally input_references."""
    doi = reference.get('doi', '').strip().lower() if reference.get('doi') else ''
    ref_id = reference.get('id', '')
    openalex_id_part = ref_id.split('/')[-1] if ref_id and 'openalex.org/' in ref_id else ref_id
    title = ''.join(reference.get('title', '').lower().split()) if reference.get('title') else ''
    authors = [''.join(a.lower().split()) for a in reference.get('authors', []) if a] if reference.get('authors') else []
    authors_str = ','.join(sorted(authors)) if authors else ''
    cursor = db_conn.cursor()
    tables_to_check = ['current_references', 'previous_references']
    if check_input_table:
        tables_to_check.append('input_references')

    for table in tables_to_check:
        try:
            if doi:
                cursor.execute(f"SELECT doi FROM {table} WHERE doi = ?", (doi,))
                if cursor.fetchone():
                    return True
            if openalex_id_part:
                # Query for 'id' field directly as it matches the JSON key
                cursor.execute(f"SELECT id FROM {table} WHERE id = ?", (openalex_id_part,))
                if cursor.fetchone():
                    return True
                # Fallback to extracting from metadata JSON if 'id' column doesn't exist or no match found
                cursor.execute(f"SELECT metadata FROM {table}")
                for row in cursor.fetchall():
                    try:
                        metadata = json.loads(row[0])
                        metadata_id = metadata.get('id', '')
                        if metadata_id and metadata_id.split('/')[-1] == openalex_id_part:
                            return True
                    except json.JSONDecodeError:
                        continue
            if title and authors_str:
                cursor.execute(f"SELECT title, authors FROM {table} WHERE title = ? AND authors = ?", (title, authors_str))
                if cursor.fetchone():
                    return True
        except sqlite3.OperationalError as e:
            print(f"Database error checking table {table}: {e}")
            continue
    return False

def add_reference_to_database(reference_data, db_conn, table='current_references'):
    try:
        metadata_json = json.dumps(reference_data, ensure_ascii=False)
    except TypeError as e:
        print(f"JSON serialization error: {str(e)}")
        return
    
    # Properly escape single quotes for SQLite
    metadata_json = metadata_json.replace("'", "''")
    
    doi = reference_data.get('doi', '').strip().lower() if reference_data.get('doi') else ''
    title = ''.join(reference_data.get('title', '').lower().split()) if reference_data.get('title') else ''
    year = str(reference_data.get('year', ''))
    authors = [''.join(a.lower().split()) for a in reference_data.get('authors', []) if a] if reference_data.get('authors') else []
    authors_str = ','.join(sorted(authors)) if authors else ''
    
    cursor = db_conn.cursor()
    cursor.execute(f"INSERT OR IGNORE INTO {table} (doi, title, year, authors, metadata) VALUES (?, ?, ?, ?, ?)",
                  (doi, title, year, authors_str, metadata_json))
    db_conn.commit()

def update_download_status(reference: dict, db_conn: sqlite3.Connection, table: str = 'current_references', downloaded: bool = True):
    """Update the downloaded status of a reference in the current_references table."""
    doi = reference.get('doi', '').strip().lower() if reference.get('doi') else ''
    title = ''.join(reference.get('title', '').lower().split()) if reference.get('title') else ''
    authors = [''.join(a.lower().split()) for a in reference.get('authors', []) if a] if reference.get('authors') else []
    authors_str = ','.join(sorted(authors)) if authors else ''
    downloaded_val = 1 if downloaded else 0
    
    cursor = db_conn.cursor()
    if doi:
        cursor.execute(f"UPDATE {table} SET downloaded = ? WHERE doi = ?", (downloaded_val, doi))
    elif title and authors_str:
        cursor.execute(f"UPDATE {table} SET downloaded = ? WHERE title = ? AND authors = ?", (downloaded_val, title, authors_str))
    db_conn.commit()

def load_metadata_to_database(metadata_dir: str, db_conn: sqlite3.Connection, table: str = 'previous_references'):
    """
    Load existing metadata.json files from previous runs into the database.
    Scans the directory recursively for JSON files and checks for duplicates before adding.
    """
    metadata_dir = Path(metadata_dir)
    json_files = metadata_dir.glob('**/metadata.json')
    count = 0
    skipped_count = 0
    
    print(f"Loading existing metadata files from {metadata_dir} into {table} table...")
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            references = []
            if isinstance(data, list):
                references = data # Input is a list of references
                print(f"  Loaded {len(references)} references from list in {file_path.name}")
            elif isinstance(data, dict):
                # First, try the 'references' key standard
                references = data.get('references', [])
                if references:
                    print(f"  Loaded {len(references)} references from 'references' key in {file_path.name}")
                else:
                    # If 'references' key missing/empty, assume values are references
                    references = list(data.values()) # Convert values view to list
                    if references:
                        print(f"  Loaded {len(references)} references from dictionary values in {file_path.name}")
                    else:
                       print(f"  Warning: Loaded dictionary from {file_path.name} but found no references in 'references' key or as values.")
            else:
                print(f"  Warning: Unexpected JSON format in {file_path.name}. Expected list or dict, got {type(data)}. Skipping file.")
                # references remains []

            # Reset counts for each file
            file_added_count = 0
            file_skipped_count = 0

            for ref in references:
                # Ensure the item from values() is actually a dictionary (reference object)
                if not isinstance(ref, dict):
                    print(f"  Warning: Skipping non-dictionary item found in {file_path.name}: {type(ref)}")
                    continue
                
                if 'downloaded_file' in ref:  # Only add if it was actually downloaded
                    # Check for duplicates in previous_references and current_references
                    doi = ref.get('doi', '').strip().lower() if ref.get('doi') else ''
                    ref_id = ref.get('id', '')
                    openalex_id_part = ref_id.split('/')[-1] if ref_id and 'openalex.org/' in ref_id else ref_id
                    title = ''.join(ref.get('title', '').lower().split()) if ref.get('title') else ''
                    authors = [''.join(a.lower().split()) for a in ref.get('authors', []) if a] if ref.get('authors') else []
                    authors_str = ','.join(sorted(authors)) if authors else ''
                    cursor = db_conn.cursor()
                    
                    if doi:
                        cursor.execute("SELECT doi FROM previous_references WHERE doi = ?", (doi,))
                        if cursor.fetchone():
                            skipped_count += 1
                            continue
                        cursor.execute("SELECT doi FROM current_references WHERE doi = ?", (doi,))
                        if cursor.fetchone():
                            skipped_count += 1
                            continue
                    
                    if title and authors_str:
                        cursor.execute("SELECT title, authors FROM previous_references WHERE title = ? AND authors = ?", (title, authors_str))
                        if cursor.fetchone():
                            skipped_count += 1
                            continue
                        cursor.execute("SELECT title, authors FROM current_references WHERE title = ? AND authors = ?", (title, authors_str))
                        if cursor.fetchone():
                            skipped_count += 1
                            continue
                    
                    add_reference_to_database(ref, db_conn, table)
                    count += 1
                    file_added_count += 1
                else:
                    # Optionally log skipped refs that weren't marked as downloaded
                    # print(f"  Skipping reference from metadata without 'downloaded_file': {ref.get('title', 'Untitled')}")
                    pass # Silently skip if not downloaded
            
            print(f"Processed {file_path} - added {file_added_count} new references, skipped {file_skipped_count} duplicates this file.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from {file_path}: {e}")
            continue
    
    print(f"Finished loading metadata. Added {count} references to the {table} table, skipped {skipped_count} duplicates.")
    return count

def load_input_jsons(input_dir: str, db_conn: sqlite3.Connection, table: str = 'input_references'):
    """
    Load all JSON files from the input directory into the database for processing.
    """
    input_dir = Path(input_dir)
    json_files = input_dir.glob('*.json')
    count = 0
    
    print(f"Loading input JSON files from {input_dir} into {table} table...")
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            references_in_file = [] # Initialize list for references from this specific file
            if isinstance(data, list):
                references_in_file = data
                # print(f"  Processing {len(references_in_file)} references from list in {file_path.name}")
            elif isinstance(data, dict):
                # Try 'references' key first
                potential_refs_list = data.get('references') # Use .get() to avoid KeyError
                if isinstance(potential_refs_list, list):
                    references_in_file = potential_refs_list
                    # print(f"  Processing {len(references_in_file)} references from 'references' key in {file_path.name}")
                else:
                    # If no 'references' key or it's not a list, assume values are the references
                    potential_refs_vals = list(data.values()) # Get values
                    # Filter to ensure they are actual reference dictionaries
                    references_in_file = [item for item in potential_refs_vals if isinstance(item, dict)]
                    if not references_in_file and potential_refs_list is not None:
                        # Handle edge case where 'references' key exists but isn't a list
                         print(f"  Warning: 'references' key in {file_path.name} exists but is not a list ({type(potential_refs_list)}). Also, dictionary values are not references.")
                    elif not references_in_file:
                         print(f"  Warning: Dictionary format in {file_path.name} has no 'references' list and its values are not reference objects.")
                    # else:
                        # print(f"  Processing {len(references_in_file)} references from dictionary values in {file_path.name}")
            else:
                print(f"  Warning: Unexpected JSON format in {file_path.name}. Expected list or dict, got {type(data)}. Skipping file.")
                continue # Skip this file

            file_added_count = 0 # Track adds for this file
            for ref in references_in_file:
                # Ensure the item is actually a dictionary (reference object)
                if not isinstance(ref, dict):
                    print(f"  Warning: Skipping non-dictionary item found while processing {file_path.name}: {type(ref)}")
                    continue
                    
                # Check if already in input_references to avoid duplicates from previous partial runs
                doi = ref.get('doi', '').strip().lower() if ref.get('doi') else ''
                title = ''.join(ref.get('title', '').lower().split()) if ref.get('title') else ''
                authors = [''.join(a.lower().split()) for a in ref.get('authors', []) if a] if ref.get('authors') else []
                authors_str = ','.join(sorted(authors)) if authors else ''
                cursor = db_conn.cursor()
                
                if doi:
                    cursor.execute(f"SELECT doi FROM {table} WHERE doi = ?", (doi,))
                    if cursor.fetchone():
                        continue
                if title and authors_str:
                    cursor.execute(f"SELECT title, authors FROM {table} WHERE title = ? AND authors = ?", (title, authors_str))
                    if cursor.fetchone():
                        continue
                
                add_reference_to_database(ref, db_conn, table)
                count += 1
                file_added_count += 1
            
            # Use file_added_count for the per-file message
            print(f"Processed {file_path.name} - added {file_added_count} new references to {table}.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from {file_path}: {e}")
            continue
        except Exception as e:
            print(f"Error processing {file_path}: {e}") # General exception catch
            continue
    
    print(f"Finished loading input JSONs. Added {count} references to the {table} table.")
    return count

def remove_reference_from_input(reference: dict, db_conn: sqlite3.Connection, table: str = 'input_references'):
    """Remove a processed reference from the input table."""
    doi = reference.get('doi', '').strip().lower() if reference.get('doi') else ''
    title = ''.join(reference.get('title', '').lower().split()) if reference.get('title') else ''
    authors = [''.join(a.lower().split()) for a in reference.get('authors', []) if a] if reference.get('authors') else []
    authors_str = ','.join(sorted(authors)) if authors else ''
    
    cursor = db_conn.cursor()
    if doi:
        cursor.execute(f"DELETE FROM {table} WHERE doi = ?", (doi,))
    elif title and authors_str:
        cursor.execute(f"DELETE FROM {table} WHERE title = ? AND authors = ?", (title, authors_str))
    db_conn.commit()

class BibliographyEnhancer:
    def __init__(self, db_file=None, email=None):
        self.email = email if email else 'spott@wzb.eu'
        self.unpaywall_headers = {'email': self.email}
        
        # Use threading.local() to store thread-specific database connections
        self.local_storage = threading.local()
        self.db_file = db_file if db_file else str(Path(__file__).parent / 'processed_references.db')
        
        # Ensure database is set up immediately
        self._setup_database()
        
        # Service-specific rate limiters
        self.gemini_limiter = ServiceRateLimiter(
            rpm_limit=15,  # 15 requests per minute
            tpm_limit=1_000_000,  # 1M tokens per minute
            rpd_limit=1_500,  # 1,500 requests per day
            quota_window_minutes=60
        )
        self.openalex_limiter = ServiceRateLimiter(
            rps_limit=10,  # 10 requests per second
            rpd_limit=100_000  # 100,000 requests per day
        )
        self.unpaywall_limiter = ServiceRateLimiter(
            rpm_limit=100,  # 100 requests per minute
            rpd_limit=100_000  # 100,000 requests per day
        )
        self.scihub_limiter = ServiceRateLimiter(
            rpm_limit=10
        )
        self.libgen_limiter = ServiceRateLimiter(
            rpm_limit=10
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
        self.proxies = None # Initialize proxies attribute
        
    @property
    def db_conn(self):
        # Get or create a database connection for the current thread
        if not hasattr(self.local_storage, 'db_conn') or self.local_storage.db_conn is None:
            self.local_storage.db_conn = sqlite3.connect(self.db_file, check_same_thread=False)
            # Enable foreign key constraints for this connection
            self.local_storage.db_conn.execute("PRAGMA foreign_keys = ON")
        return self.local_storage.db_conn

    def _setup_database(self):
        """Setup the SQLite database with necessary tables for previous, current, and input references."""
        cursor = self.db_conn.cursor()
        # Table for references from previous sessions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS previous_references (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doi TEXT UNIQUE,
                title TEXT,
                year TEXT,
                authors TEXT,
                metadata TEXT,
                processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Table for references from the current session
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS current_references (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doi TEXT UNIQUE,
                title TEXT,
                year TEXT,
                authors TEXT,
                metadata TEXT,
                processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Table for input references to be processed
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS input_references (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doi TEXT UNIQUE,
                title TEXT,
                year TEXT,
                authors TEXT,
                metadata TEXT,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.db_conn.commit()

    def __del__(self):
        """Destructor to close database connection."""
        if hasattr(self, 'db_conn'):
            self.db_conn.close()

    def search_and_evaluate_with_gemini(self, ref):
        """
        Perform a combined search and evaluation using Gemini with enhanced rate limiting.
        Uses structured output via Pydantic models if available.
        Will skip (return []) if daily request limit is exceeded.
        """
        title = ref.get('title', '')
        authors = ref.get('authors', [])
        year = ref.get('year', '')
        container = ref.get('container_title', '')

        query = f'"{title}" {", ".join(authors) if authors else ""} {year} (pdf OR filetype:pdf)'
        print(f"Searching and evaluating with Gemini for: {query}")

        # Check rate limits - return early if daily limit exceeded
        # This returns False if daily limit exceeded
        if not self.gemini_limiter.wait_if_needed(estimated_tokens=0):
            print("Gemini daily limit exceeded. Skipping Gemini search and proceeding with other methods.")
            return []
            
        # Define the search and evaluation prompt
        combined_prompt = f"""You are helping find exact PDF matches for an academic publication.
        
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

        7. You MUST return structured output as a JSON list of matches. If no matches are found, return an empty list [].
        """

        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Wait for rate limiting for tokens
                if not self.gemini_limiter.wait_if_needed(estimated_tokens=1000):
                    # If daily limit exceeded during retry, return empty results
                    print("Gemini daily limit exceeded during retry. Skipping and moving to next method.")
                    return []

                # Use structured output if Pydantic is available
                if PYDANTIC_AVAILABLE:
                    response = self.client.models.generate_content(
                        contents=combined_prompt,
                        model=self.MODEL_ID,
                        config=GenerateContentConfig(
                            temperature=0.0,
                            response_modalities=["TEXT"],
                            tools=[self.google_search_tool],
                            response_mime_type="application/json",
                            response_schema=PdfMatchListContainer, # Use the container class
                        )
                    )
                    
                    # Try to use the parsed response directly
                    try:
                        parsed_container = response.parsed
                        if parsed_container is not None:
                            # Access the 'matches' list within the container
                            parsed_matches = parsed_container.matches 
                            # Convert Pydantic models to dictionaries
                            matches = [match.dict() for match in parsed_matches]
                            print(f"Found {len(matches)} potential PDF matches after evaluation")
                            return matches
                    except Exception as parse_err:
                        print(f"Error parsing structured response: {parse_err}")
                        # Fall back to text parsing if structured parsing fails
                else:
                    # Fall back to original implementation if Pydantic not available
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

                # Process response text (used as fallback when structured parsing fails)
                response_text = response.text.strip()
                response_text = response_text.replace('```json', '').replace('```', '').strip()

                try:
                    matches = json.loads(response_text)
                    if isinstance(matches, list):
                        print(f"Found {len(matches)} potential PDF matches after text parsing")
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
        
        # Clean the DOI if it starts with doi: prefix
        if doi.lower().startswith('doi:'):
            doi = doi[4:].strip()
        
        # Extract the DOI part if it's a URL
        if doi.startswith(('http://', 'https://')):
            parsed = urlparse(doi)
            if 'doi.org' in parsed.netloc and parsed.path:
                doi = parsed.path[1:] if parsed.path.startswith('/') else parsed.path
            elif parsed.path.startswith('/10.'):
                doi = parsed.path[1:]
        
        # Check if the DOI starts with 10.
        if not doi.startswith('10.'):
            print(f"Invalid DOI format for Unpaywall: {doi} - DOI must start with '10.'")
            return None
        
        url = f'https://api.unpaywall.org/v2/{doi}?email={self.email}'
        
        try:
            # Check for rate limits including daily limit
            if not self.unpaywall_limiter.wait_if_needed():
                print(f"Unpaywall daily request limit exceeded. Skipping Unpaywall check for DOI {doi}.")
                return None
                
            response = requests.get(url, headers=self.unpaywall_headers, timeout=10)
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

    def try_download(self, url_info, ref, downloads_dir):
        url = url_info['url']
        print(f"Trying {url_info['source']}: {url}")
        try:
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30, allow_redirects=True)
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                if 'application/pdf' in content_type:
                    filename = self.save_pdf(response.content, ref, downloads_dir, url_info['source'])
                    if filename:
                        return {
                            'success': True,
                            'file': str(filename),
                            'source': url_info['source'],
                            'reason': url_info['reason']
                        }
                elif 'json' in content_type:
                    try:
                        data = response.json()
                        # Handle JSON response if it contains PDF links or other useful information
                        if 'pdf_url' in data:
                            url_info['url'] = data['pdf_url']
                            url_info['reason'] = f"{url_info['reason']} (via JSON response)"
                            return self.try_download(url_info, ref, downloads_dir)
                        return {'success': False, 'reason': 'JSON response did not contain PDF link'}
                    except json.JSONDecodeError:
                        print(f"JSON parse error for {url}: Response was not valid JSON")
                        return {'success': False, 'reason': 'JSON parse error'}
                else:
                    # Check if the response is HTML and look for PDF links
                    if 'html' in content_type or 'text' in content_type:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        pdf_links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.pdf')]
                        if pdf_links:
                            url_info['url'] = pdf_links[0]
                            url_info['reason'] = f"{url_info['reason']} (PDF link found in HTML)"
                            return self.try_download(url_info, ref, downloads_dir)
                    return {'success': False, 'reason': f'Unsupported content type: {content_type}'}
            else:
                return {'success': False, 'reason': f'HTTP {response.status_code}'}
        except requests.exceptions.RequestException as e:
            return {'success': False, 'reason': f'Request error: {str(e)}'}
        except Exception as e:
            return {'success': False, 'reason': f'Unexpected error: {str(e)}'}

    def search_with_scihub(self, doi):
        """
        Search for a paper on Sci-Hub using its DOI and retrieve the download link.
        Returns immediately after finding a working mirror.
        """
        if not doi:
            print("Sci-Hub: No DOI provided")
            return None
        
        # Clean and normalize the DOI first
        clean_doi = doi
        if clean_doi.startswith("doi:"):
            clean_doi = clean_doi[4:]  # Remove 'doi:' prefix
        if clean_doi.startswith("https://doi.org/") or clean_doi.startswith("http://doi.org/"):
            # Extract just the DOI part for Sci-Hub
            clean_doi = clean_doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
            
        print(f"Sci-Hub: Searching for DOI {clean_doi}")

        # Try multiple Sci-Hub mirrors
        scihub_mirrors = [
            "https://sci-hub.se",
            "https://sci-hub.st",
            "https://sci-hub.ru"
        ]

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1'
        }

        # Apply rate limiting before starting the loop
        self.scihub_limiter.wait_if_needed()

        for mirror in scihub_mirrors:
            scihub_url = f"{mirror}/{clean_doi}"
            print(f"  Trying Sci-Hub mirror: {mirror}")

            try:
                response = requests.get(scihub_url, headers=headers, timeout=30, proxies=self.proxies)
                if response.status_code == 404:
                    print(f"    DOI not found on {mirror}. Stopping search as content is likely the same across mirrors.")
                    return None
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
                    if not pdf_link.startswith(('http://', 'https://')):
                        if pdf_link.startswith('//'):
                            pdf_link = f"https:{pdf_link}"
                        elif pdf_link.startswith('/'):
                            pdf_link = f"{mirror}{pdf_link}"
                        else:
                            pdf_link = f"{mirror}/{pdf_link}"
                            
                    print(f"Found PDF link via {mirror}: {pdf_link}")
                    # Return dictionary when we find a working mirror
                    return {
                        'url': pdf_link,
                        'source': 'Sci-Hub',
                        'reason': f'DOI resolved to PDF via {mirror}'
                    }
                
                print(f"No PDF link found on {mirror}. Stopping search as content is likely the same across mirrors.")
                return None
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                print(f"Connection issue with {mirror}: {e}. Trying next mirror...")
            except requests.exceptions.RequestException as e:
                print(f"Error accessing {mirror}: {e}. Trying next mirror...")

        # Only reaches here if all mirrors failed due to connection issues
        print("No working Sci-Hub mirrors found due to connection issues")
        return None

    def search_with_libgen(self, ref):
        """Search for publications on LibGen and extract working download links, prioritizing PDFs."""
        # Inside the search_with_libgen function, just change how we build the query:
        title = ref.get('title', '')
        authors_data = ref.get('authorships', [])
        authors = [author.get('author', {}).get('display_name', '') for author in authors_data] if authors_data else []
        ref_type = ref.get('type', '').lower()  
        is_book = ref_type in ['book', 'monograph']

        # Get just the surname (last word) of first author
        author_surname = authors[0].split()[-1] if authors else ""

        query = f'{title} {author_surname}'
        query = query.replace(' ', '+')
        base_url = "https://libgen.la"
        libgen_url = f"{base_url}/index.php?req={query}&lg_topic=libgen&open=0&view=simple&res=25&phrase=1&column=def"
        
        print(f"Searching LibGen for: {title}")
        print(f"  Book type detection: is_book={is_book}, ref_type='{ref_type}'")
        print(f"  Using author surname: '{author_surname}'")
        print(f"  LibGen URL: {libgen_url}")

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
                print("  No results table found - checking for captcha/bot detection")
                if soup.find(text=re.compile('captcha|bot|automated|security check', re.IGNORECASE)):
                    print("  WARNING: Possible captcha/bot detection detected")
                return []

            # Process rows (skip header)
            rows = table.find_all('tr')[1:]  
            print(f"Found {len(rows)} results to process")
            
            for idx, row in enumerate(rows, start=1):
                cells = row.find_all('td')
                if len(cells) < 9:
                    print(f"  Row {idx}: Skipped - only {len(cells)} cells (need at least 9)")
                    continue
                    
                result_title = cells[0].get_text(strip=True)
                result_author = cells[1].get_text(strip=True)
                file_ext = cells[7].get_text(strip=True).lower()
                
                print(f"  Row {idx}: Title='{result_title[:60]}...', Authors='{result_author}', Ext='{file_ext}'")
                
                # Skip if it's a review
                if "Review by:" in result_author:
                    print("    Skipped - appears to be a review")
                    continue
                    
                # Skip if book title contains review markers
                if is_book:
                    review_markers = [
                        "vol.", "iss.", "pp.", "pages",
                        "Review of", "Book Review",
                        ") pp.", ") p.",
                    ]
                    if any(marker in result_title for marker in review_markers):
                        print(f"    Skipped - contains review marker")
                        continue
                
                # Get mirror links
                mirrors_cell = cells[-1]
                mirror_links = mirrors_cell.find_all('a')
                print(f"    Found {len(mirror_links)} mirror links")
                
                for link_idx, link in enumerate(mirror_links, start=1):
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
                            print(f"    Mirror {link_idx}: PDF link found - {href[:80]}...")
                        else:
                            other_matches.append(match_entry)
                            print(f"    Mirror {link_idx}: Non-PDF link found - {href[:80]}...")

            # Combine PDF matches first, then other formats
            matches = pdf_matches + other_matches
            print(f"Found {len(matches)} non-review download links ({len(pdf_matches)} PDFs)")
            return matches

        except Exception as e:
            print(f"Error searching LibGen: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
            
    def is_likely_pdf_url(self, url):
        """Checks if a URL is likely to point directly to a PDF based on patterns."""
        url_lower = url.lower()
        
        # General PDF checks
        if any([
            url_lower.endswith('.pdf'),
            'pdf' in url_lower and '/download' in url_lower,
            'pdf' in url_lower and '/view' in url_lower,
            '/pdf/' in url_lower,
            'pdf' in url_lower and 'download' in url_lower, # Added check
        ]):
            return True
        
        return False
        
    def try_extract_pdf_link(self, html_url):
        try:
            print(f"  Checking HTML page for PDF link: {html_url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15'
            }
            response = requests.get(html_url, headers=headers, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for PDF links first
            found_link = None # Initialize found_link
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
                    return None # Return None if no PDF link or GET button found

            # Return None if loop finished without finding a link and GET button wasn't found either
            if found_link is None:
                 return None
                 
            # Handle relative URLs in one place
            if not found_link.startswith(('http://', 'https://')):
                found_link = urljoin(response.url, found_link)
                
            return found_link

        except requests.exceptions.RequestException as e:
            print(f"  Error checking HTML page: {e}")
            return None
        except Exception as e:
            print(f"  Unexpected error checking HTML page: {e}")
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
                    print("  PDF is encrypted")
                    return False

                # Basic page count validation
                num_pages = doc.page_count
                min_pages = 50 if ref_type.lower() == 'book' else 5
                
                if num_pages < min_pages:
                    print(f"  PDF too short ({num_pages} pages, minimum {min_pages})")
                    return False

                # Success - valid PDF
                print(f"  Valid PDF with {num_pages} pages")
                return True

        except fitz.FileDataError as e:
            error_msg = str(e).lower()
            # Ignore CSS-related errors as they don't affect PDF validity
            if "css syntax error" in error_msg:
                print("  Ignoring CSS validation warnings")
                return True
            print(f"  PDF parsing error: {str(e)}")
            return False
        except Exception as e:
            print(f"  Unexpected error validating PDF: {str(e)}")
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
                print("  PDF not found (404)")
                return False
            elif response.status_code == 403:
                print("  Access forbidden (403)")
                return False
            elif response.status_code == 429:
                print("  Too many requests (429) - Rate limited")
                return False
            elif response.status_code != 200:
                print(f"  HTTP error {response.status_code}")
                return False
                    
            content = response.content
            
            # Enhanced PDF validation
            if self.is_valid_pdf(content, ref_type):
                with open(filename, 'wb') as f:
                    f.write(content)
                print("  Direct download successful - Valid complete PDF")
                return True
            else:
                print("  Downloaded file is not a valid complete PDF")
                
                # If it seems to be HTML, try extracting PDF link
                if 'text/html' in response.headers.get('Content-Type', '').lower():
                    pdf_url = self.try_extract_pdf_link(url)
                    if pdf_url and pdf_url != url:
                        return self.download_paper(pdf_url, filename, ref_type)
            
            return False
                
        except requests.exceptions.ConnectionError:
            print("  Connection error - Could not reach server")
            return False
        except requests.exceptions.Timeout:
            print("  Request timed out")
            return False
        except requests.exceptions.TooManyRedirects:
            print("  Too many redirects")
            return False
        except requests.exceptions.RequestException as e:
            print(f"  Request failed: {str(e)}")
            return False
        except Exception as e:
            print(f"  Unexpected error: {str(e)}")
            if filename.exists():
                filename.unlink()
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

                # Convert OpenAlex work data to our reference format
                parent_identifier = ref.get('doi', ref.get('title', 'Untitled'))
                enhanced_ref = {
                    'title': work.get('display_name', ''),
                    'authors': [auth.get('author', {}).get('display_name', auth.get('raw_author_name', '')) for auth in work.get('authorships', []) if auth.get('author') or auth.get('raw_author_name')],
                    'year': str(work.get('publication_year', '')),
                    'doi': work.get('doi', ''),
                    'openalex_id': work.get('id', ''),
                    'source': f"Referenced by {parent_identifier}"
                }
                
                # Check if already in input_references to avoid duplicates from previous partial runs
                doi = enhanced_ref.get('doi', '').strip().lower() if enhanced_ref.get('doi') else ''
                ref_id = enhanced_ref.get('id', '')
                openalex_id_part = ref_id.split('/')[-1] if ref_id and 'openalex.org/' in ref_id else ref_id
                title = ''.join(enhanced_ref.get('title', '').lower().split()) if enhanced_ref.get('title') else ''
                authors = [''.join(a.lower().split()) for a in enhanced_ref.get('authors', []) if a] if enhanced_ref.get('authors') else []
                authors_str = ','.join(sorted(authors)) if authors else ''
                cursor = self.db_conn.cursor()
                
                if doi:
                    cursor.execute("SELECT doi FROM input_references WHERE doi = ?", (doi,))
                    if cursor.fetchone():
                        continue
                    cursor.execute("SELECT doi FROM previous_references WHERE doi = ?", (doi,))
                    if cursor.fetchone():
                        continue
                    cursor.execute("SELECT doi FROM current_references WHERE doi = ?", (doi,))
                    if cursor.fetchone():
                        continue
                
                bibliography['references'].append(enhanced_ref)
                            
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

    def enhance_bibliography(self, ref, downloads_dir, force_download=False):
        """Enhances a single reference dictionary, attempts download, and returns the modified ref or None."""
        # Ensure downloads_dir is a Path object (it should be, but double-check)
        if not isinstance(downloads_dir, Path):
            print(f"ERROR: downloads_dir passed to enhance_bibliography is not a Path object: {type(downloads_dir)}")
            try:
                downloads_dir = Path(downloads_dir)
            except TypeError:
                print("ERROR: Cannot convert downloads_dir to Path. Aborting enhance_bibliography for this ref.")
                return None # Indicate failure

        # Ensure the directory exists (it should, but good practice)
        downloads_dir.mkdir(parents=True, exist_ok=True)
        
        # Removed duplicate-skip logic at download stage: always attempt download
        # --- Start of main processing logic for the single 'ref' --- 
        # Combine multiple prints into a single statement to avoid threading issues
        title = ref.get('title', ref.get('display_name', 'Untitled'))
        authors_list = ref.get('authors', [])
        ref_year = ref.get('year', ref.get('publication_year', None))
        print(f"\nProcessing: {title}\nAuthors: {', '.join(authors_list)}\nYear: {ref_year}")
        
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
            if unpaywall_result and unpaywall_result.get('best_oa_location'):
                oa_location = unpaywall_result['best_oa_location']
                if oa_location.get('url_for_pdf'):
                    print(f"  Unpaywall found OA PDF URL: {oa_location['url_for_pdf']}")
                    urls_to_try.append({
                        'url': oa_location['url_for_pdf'],
                        'title': ref.get('title', ''),
                        'snippet': '',
                        'source': oa_location.get('host_type', 'Unpaywall OA'),
                        'reason': 'Found via Unpaywall DOI lookup'
                    })
                else:
                    print(f"  Unpaywall found OA location but no direct PDF URL for DOI {ref['doi']}.")
            else:
                 print(f"  Unpaywall found no OA location for DOI {ref['doi']}.")

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
                file_path = ref.get('downloaded_file', 'UNKNOWN_PATH')
                print(f"{GREEN}✓ Downloaded from: {url_info['source']} as {Path(file_path).name}{RESET}")

        # 2. If direct methods fail, try Sci-Hub (using DOI)
        if not download_success and ref.get('doi'):
            print("Direct methods failed. Trying Sci-Hub...")
            self.scihub_limiter.wait_if_needed()
            scihub_result = self.search_with_scihub(ref['doi'])
            
            if scihub_result:
                print(f"\nFound PDF link on Sci-Hub: {scihub_result['url']}")
                if self.try_download_url(scihub_result['url'], scihub_result['source'], ref, downloads_dir):
                    ref['download_source'] = scihub_result['source']
                    ref['download_reason'] = scihub_result['reason'] # Keep Sci-Hub reason
                    download_success = True
                    file_path = ref.get('downloaded_file', 'UNKNOWN_PATH')
                    print(f"{GREEN}✓ Downloaded from: {scihub_result['source']} as {Path(file_path).name}{RESET}")

        # 3. If Sci-Hub fails (or no DOI), try LibGen
        if not download_success:
            print("Sci-Hub failed (or no DOI). Trying LibGen...")
            self.libgen_limiter.wait_if_needed()
            libgen_matches = self.search_with_libgen(ref)
            
            if libgen_matches:
                print("\nPotential matches found on LibGen:")
                for match in libgen_matches:
                    print(f"- {match.get('title', 'No title')}")
                    print(f"  URL: {match.get('url', 'No URL')}")
                    print(f"  Reason: {match.get('reason', 'No reason provided')}")
                    print(f"  Source: {match.get('source', 'LibGen')}")
                    
                    if match.get('url'):  
                        if self.try_download_url(match['url'], match.get('source', "LibGen"), ref, downloads_dir):
                            ref['download_source'] = match.get('source', "LibGen")
                            ref['download_reason'] = 'Successfully downloaded via LibGen' # More specific reason
                            download_success = True
                            file_path = ref.get('downloaded_file', 'UNKNOWN_PATH')
                            print(f"{GREEN}✓ Downloaded from: {match.get('source', 'LibGen')} as {Path(file_path).name}{RESET}")
                            break # Exit LibGen loop on success

        # If all methods fail, mark as not found
        if not download_success:
            print(f"  {RED}✗ No valid matches found through any method{RESET}")
            ref['download_status'] = 'not_found'
            ref['download_reason'] = 'No valid matches found through any method'

        # Return the modified ref dictionary if download was successful, otherwise None
        if 'downloaded_file' in ref and ref['downloaded_file']:
             # Optionally, update DB status here if enhance_bibliography is solely responsible
             # update_download_status(ref, self.db_conn, downloaded=True)
             return ref # Return the modified ref dict on success
        else:
             # Optionally, update DB status here if enhance_bibliography is solely responsible
             # update_download_status(ref, self.db_conn, downloaded=False)
             return None # Return None on failure or skip

    def extract_and_enhance_referenced_works(self, ref, db_conn: sqlite3.Connection):
        """
        Extract referenced works from a reference, enhance their metadata using OpenAlex API,
        and add them as separate entries to input_references for processing.
        """
        referenced_works = ref.get('referenced_works', [])
        if not referenced_works:
            print(f"No referenced works found for {ref.get('title', 'Untitled')}")
            return 0
        
        print(f"Processing {len(referenced_works)} referenced works for {ref.get('title', 'Untitled')}...")
        added_count = 0
        
        for work_id in referenced_works:
            try:
                # Apply rate limiting before OpenAlex API call
                self.openalex_limiter.wait_if_needed()

                # Query OpenAlex for work details
                actual_id = work_id.split('/')[-1] if 'openalex.org/' in work_id else work_id
                url = f"https://api.openalex.org/{actual_id}?mailto={self.email}"
                response = requests.get(url, headers={'User-Agent': 'BibliographyEnhancer'})
                response.raise_for_status()
                work = response.json()
                
                if not work:
                    print(f"No data returned for referenced work {work_id}")
                    continue

                # Convert OpenAlex work data to our reference format
                parent_identifier = ref.get('doi', ref.get('title', 'Untitled'))
                enhanced_ref = {
                    'title': work.get('display_name', ''),
                    'authors': [auth.get('author', {}).get('display_name', auth.get('raw_author_name', '')) for auth in work.get('authorships', []) if auth.get('author') or auth.get('raw_author_name')],
                    'year': str(work.get('publication_year', '')),
                    'doi': work.get('doi', ''),
                    'openalex_id': work.get('id', ''),
                    'source': f"Referenced by {parent_identifier}"
                }
                
                # Check if already in input_references or other tables to avoid duplicates
                doi = enhanced_ref.get('doi', '').strip().lower() if enhanced_ref.get('doi') else ''
                title = ''.join(enhanced_ref.get('title', '').lower().split()) if enhanced_ref.get('title') else ''
                authors = [''.join(a.lower().split()) for a in enhanced_ref.get('authors', []) if a] if enhanced_ref.get('authors') else []
                authors_str = ','.join(sorted(authors)) if authors else ''
                cursor = db_conn.cursor()
                
                if doi:
                    cursor.execute("SELECT doi FROM input_references WHERE doi = ?", (doi,))
                    if cursor.fetchone():
                        print(f"Referenced work {work_id} already in input_references, skipping.")
                        continue
                    cursor.execute("SELECT doi FROM previous_references WHERE doi = ?", (doi,))
                    if cursor.fetchone():
                        print(f"Referenced work {work_id} already in previous_references, skipping.")
                        continue
                    cursor.execute("SELECT doi FROM current_references WHERE doi = ?", (doi,))
                    if cursor.fetchone():
                        print(f"Referenced work {work_id} already in current_references, skipping.")
                        continue
                
                if title and authors_str:
                    cursor.execute("SELECT title, authors FROM input_references WHERE title = ? AND authors = ?", (title, authors_str))
                    if cursor.fetchone():
                        print(f"Referenced work {work_id} already in input_references, skipping.")
                        continue
                    cursor.execute("SELECT title, authors FROM previous_references WHERE title = ? AND authors = ?", (title, authors_str))
                    if cursor.fetchone():
                        print(f"Referenced work {work_id} already in previous_references, skipping.")
                        continue
                    cursor.execute("SELECT title, authors FROM current_references WHERE title = ? AND authors = ?", (title, authors_str))
                    if cursor.fetchone():
                        print(f"Referenced work {work_id} already in current_references, skipping.")
                        continue
                
                # Add to input_references for processing
                add_reference_to_database(enhanced_ref, db_conn, table='input_references')
                added_count += 1
                work_title = enhanced_ref.get('title', 'Untitled')
                work_id = enhanced_ref.get('id', 'Unknown ID').split('/')[-1]
                title_for_print = (work_title[:60] + '...') if work_title and len(work_title) > 60 else (work_title or 'Untitled')
                print(f"    Added referenced work to input_references: {title_for_print} (ID: {work_id or 'Unknown'})")
            except Exception as e:
                print(f"Error processing referenced work {work_id}: {e}")
                continue
        
        print(f"Added {added_count} referenced works to input_references for processing.")
        return added_count

    def _expand_inputs_with_referenced_works(self):
        """Pre-processes the input_references table. Finds referenced works, fetches their metadata (excluding *their* refs), checks for duplicates, and adds new ones back to input_references."""
        print("\n--- Expanding input references with referenced works ---")
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT metadata FROM input_references")
        current_input_refs_json = [row[0] for row in cursor.fetchall()]
        initial_input_count = len(current_input_refs_json)
        print(f"Checking {initial_input_count} initial input references for referenced works...")

        newly_added_count = 0
        processed_origin_refs = 0

        # Fields to select from OpenAlex for referenced works (exclude 'referenced_works')
        fields_for_referenced = "id,doi,display_name,authorships,publication_year,type,primary_location,open_access"

        for ref_json in current_input_refs_json:
            try:
                if not ref_json:
                    print("  Skipping empty metadata for a reference.")
                    continue
                ref = json.loads(ref_json)
                processed_origin_refs += 1

                # Debug the raw data to see what fields are available
                title = ref.get('display_name', ref.get('title', 'Untitled'))
                ref_id_raw = ref.get('openalex_id', ref.get('id', 'Unknown ID'))
                ref_id = ref_id_raw.split('/')[-1] if ref_id_raw and isinstance(ref_id_raw, str) and ref_id_raw.startswith('https://openalex.org/') else ref_id_raw
                if ref_id == 'Unknown ID':
                    print(f"  Warning: ID field missing or invalid in reference data.")
                if title == 'Untitled':
                    print(f"  Warning: Neither 'display_name' nor 'title' found in reference data.")
                print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Processing reference: {title[:60]} (ID: {ref_id})")

                referenced_works_urls = ref.get('referenced_works', [])
                if not referenced_works_urls:
                    print(f"    No referenced works found for this reference.")
                    continue

                print(f"    Found {len(referenced_works_urls)} referenced works to process...")

                # Filter out invalid URLs and extract work IDs
                valid_work_ids = []
                for work_url in referenced_works_urls:
                    if not work_url or not isinstance(work_url, str) or not work_url.startswith('https://openalex.org/'):
                        print(f"      Skipping invalid or non-OpenAlex URL: {str(work_url)[:50]}...")
                        continue
                    work_id = work_url.split('/')[-1]
                    valid_work_ids.append(work_id)

                if not valid_work_ids:
                    print(f"      No valid OpenAlex work IDs to fetch.")
                    continue

                # Check for duplicates before querying OpenAlex
                filtered_work_ids = []
                for work_id in valid_work_ids:
                    duplicate_found = False
                    for table in ['current_references', 'previous_references', 'input_references']:
                        try:
                            cursor.execute(f"SELECT metadata FROM {table}")
                            for row in cursor.fetchall():
                                metadata_json = row[0]
                                try:
                                    metadata = json.loads(metadata_json) if metadata_json else {}
                                    db_id_raw = metadata.get('openalex_id', metadata.get('id', ''))
                                    db_id = db_id_raw.split('/')[-1] if db_id_raw and isinstance(db_id_raw, str) and db_id_raw.startswith('https://openalex.org/') else db_id_raw
                                    if db_id and db_id == work_id:
                                        duplicate_found = True
                                        title_to_show = metadata.get('display_name', metadata.get('title', 'Untitled'))
                                        print(f"          Skipping referenced work already in database ({table}) based on ID match: {title_to_show[:60]} (ID: {work_id})")
                                        break
                                except json.JSONDecodeError:
                                    continue
                            if duplicate_found:
                                break
                        except sqlite3.OperationalError as e:
                            print(f"Database error checking table {table}: {e}")
                            continue
                        except json.JSONDecodeError:
                            print(f"          Warning: Failed to decode JSON metadata in table {table}.")
                            continue
                    if not duplicate_found:
                        filtered_work_ids.append(work_id)

                if not filtered_work_ids:
                    print(f"      All referenced work IDs are duplicates. Skipping API call.")
                    continue

                print(f"      Will query OpenAlex for {len(filtered_work_ids)} non-duplicate referenced works.")

                # Process in batches of 100 to avoid API limits
                batch_size = 100
                for i in range(0, len(filtered_work_ids), batch_size):
                    batch_ids = filtered_work_ids[i:i + batch_size]
                    print(f"      Fetching metadata for {len(batch_ids)} referenced works in a batch request (batch {i//batch_size + 1} of {len(filtered_work_ids)//batch_size + 1})...")
                    try:
                        self.openalex_limiter.wait_if_needed()
                        filter_param = "openalex:" + "|".join(batch_ids)
                        api_url = f"https://api.openalex.org/works?filter={filter_param}&select={fields_for_referenced}&per-page={len(batch_ids)}&mailto={self.email}"
                        response = requests.get(api_url, headers={'User-Agent': f'BibliographyEnhancer/{self.email}'}, timeout=60)
                        response.raise_for_status()
                        batch_data = response.json()
                        results = batch_data.get('results', [])
                        print(f"        Successfully fetched metadata for {len(results)} out of {len(batch_ids)} referenced works.")
                    except requests.exceptions.RequestException as api_err:
                        print(f"        API Error fetching batch metadata: {api_err}")
                        continue
                    except json.JSONDecodeError:
                        print(f"        API Error: Invalid JSON response for batch request")
                        continue

                    if not results:
                        print(f"        No data returned for any referenced works in this batch.")
                        continue

                    # Process each returned work
                    processed_ids = []
                    for referenced_work_data in results:
                        if not referenced_work_data:
                            print(f"        Skipping empty metadata for a referenced work.")
                            continue
                        work_id = referenced_work_data.get('openalex_id', '').split('/')[-1]
                        processed_ids.append(work_id)
                        print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Processing referenced work ID: {work_id}")

                        work_title = referenced_work_data.get('display_name', referenced_work_data.get('title', 'Untitled'))
                        title_for_print = (work_title[:60] + '...') if work_title and len(work_title) > 60 else (work_title or 'Untitled')
                        ref_doi = referenced_work_data.get('doi', '').strip().lower() if referenced_work_data.get('doi') else ''
                        duplicate_found = False

                        # Check DOI first
                        if ref_doi:
                            for table in ['current_references', 'previous_references', 'input_references']:
                                try:
                                    cursor.execute(f"SELECT title, metadata FROM {table} WHERE doi = ?", (ref_doi,))
                                    result = cursor.fetchone()
                                    if result:
                                        duplicate_found = True
                                        table_title, metadata_json = result
                                        title_to_show = metadata.get('display_name', metadata.get('title', 'Untitled')) if metadata_json else 'Untitled'
                                        print(f"          Skipping referenced work already in database ({table}) based on DOI: {title_to_show[:60]} (ID: {work_id})")
                                        break
                                except sqlite3.OperationalError as e:
                                    print(f"Database error checking table {table}: {e}")
                                    continue
                                except json.JSONDecodeError:
                                    print(f"          Warning: Failed to decode JSON metadata in table {table}.")
                                    continue
                            if duplicate_found:
                                continue

                        # If no DOI match, check title and author
                        if not duplicate_found and work_title and work_title != 'Untitled':
                            authorships = referenced_work_data.get('authorships', [])
                            if authorships:
                                # Extract author names
                                author_names = []
                                for authorship in authorships:
                                    author = authorship.get('author')
                                    if author:
                                        author_name = author.get('display_name')
                                        if author_name:
                                            author_names.append(author_name)

                                if author_names:
                                    for table in ['current_references', 'previous_references', 'input_references']:
                                        try:
                                            cursor.execute(f"SELECT title, metadata FROM {table}")
                                            for row in cursor.fetchall():
                                                table_title, metadata_json = row
                                                if table_title and table_title.lower() == work_title.lower():
                                                    try:
                                                        metadata = json.loads(metadata_json) if metadata_json else {}
                                                        db_authorships = metadata.get('authorships', [])
                                                        if db_authorships:
                                                            db_author_names = []
                                                            for db_authorship in db_authorships:
                                                                db_author = db_authorship.get('author')
                                                                if db_author:
                                                                    db_author_name = db_author.get('display_name')
                                                                    if db_author_name:
                                                                        db_author_names.append(db_author_name)
                                                            if db_author_names and set([n.lower() for n in author_names]) & set([n.lower() for n in db_author_names]):
                                                                duplicate_found = True
                                                                print(f"          Skipping referenced work already in database ({table}) based on title and author match: {work_title[:60]} (ID: {work_id})")
                                                                break
                                                    except json.JSONDecodeError:
                                                        print(f"          Warning: Failed to decode JSON metadata in table {table}.")
                                                        continue
                                            if duplicate_found:
                                                break
                                        except sqlite3.OperationalError as e:
                                            print(f"Database error checking table {table}: {e}")
                                            continue

                        if duplicate_found:
                            continue

                        # If no duplicate found, check using is_reference_downloaded for additional validation
                        if is_reference_downloaded(referenced_work_data, None, self.db_conn, check_input_table=True):
                            print(f"          Skipping referenced work already in database (via is_reference_downloaded check): {work_title[:60]} (ID: {work_id})")
                            continue

                        # If it passed all checks, add it to the input table for later processing
                        add_reference_to_database(referenced_work_data, self.db_conn, table='input_references')
                        newly_added_count += 1
                        print(f"          Added referenced work to input_references: {title_for_print} (ID: {work_id})")

                    # Note any IDs that were requested but not returned in the results
                    missing_ids = [wid for wid in batch_ids if wid not in processed_ids]
                    if missing_ids:
                        print(f"        Note: {len(missing_ids)} referenced work IDs were not returned by the API.")
            except json.JSONDecodeError:
                print(f"  Error decoding JSON for an input reference.")
                continue
            except Exception as e:
                import traceback
                print(f"  Error processing referenced works for an input reference: {e}")
                print(f"  Stack trace: {traceback.format_exc()}")
                continue

        print(f"--- Expansion complete: Processed {processed_origin_refs} origin refs, added {newly_added_count} new referenced works to input_references ---")

    def enhance_bibliography_thread_safe(self, ref, downloads_dir, force_download=False):
        """
        Thread-safe wrapper for enhance_bibliography.
        Creates a deep copy of the reference to avoid thread conflicts.
        """
        # Create a deep copy to avoid potential thread issues with shared data
        ref_copy = copy.deepcopy(ref)
        try:
            return self.enhance_bibliography(ref_copy, downloads_dir, force_download)
        except Exception as e:
            print(f"Error in thread processing {ref_copy.get('title', 'Unknown')}: {e}")
            # Re-raise to be caught by the executor
            raise
            
    def process_references_from_database(self, output_dir, max_concurrent=5, force_download=False, skip_referenced_works=False):
        """
        Process references from the input_references table with parallel downloads
        maintaining a constant worker pool.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        downloads_dir = output_dir / 'downloads'
        downloads_dir.mkdir(parents=True, exist_ok=True)
        
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT metadata FROM input_references")
        references_to_process = [json.loads(row[0]) for row in cursor.fetchall()]
        
        total_refs = len(references_to_process)
        updated_references = []
        
        # Create a work queue and results list
        work_queue = Queue()
        for ref in references_to_process:
            work_queue.put(ref)
        
        print(f"\nProcessing {total_refs} references from database with {max_concurrent} constant workers...")
        
        processed_count = 0
        active_futures = {}
        
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            # Initialize the worker pool with initial tasks
            for _ in range(min(max_concurrent, total_refs)):
                if not work_queue.empty():
                    ref = work_queue.get()
                    future = executor.submit(
                        self.enhance_bibliography_thread_safe, 
                        ref, 
                        downloads_dir,
                        force_download
                    )
                    active_futures[future] = ref
            
            # Process results and add new tasks as workers become available
            while active_futures:
                # Wait for any future to complete
                done, _ = wait(active_futures.keys(), return_when=FIRST_COMPLETED)
                
                for future in done:
                    ref = active_futures.pop(future)
                    
                    try:
                        updated_ref = future.result()
                        if updated_ref:
                            updated_references.append(updated_ref)
                    except Exception as e:
                        print(f"{RED}!!! Error processing reference {ref.get('title', 'Untitled')}: {e}{RESET}")
                        ref['download_status'] = 'error'
                        ref['error_message'] = str(e)
                        updated_references.append(ref)
                    
                    processed_count += 1
                    if processed_count % 5 == 0 or processed_count == total_refs:
                        print(f"Processed {processed_count}/{total_refs} references...")
                    
                    # Immediately move ref from input to current after download attempt
                    if 'downloaded_file' in ref:
                        add_reference_to_database(ref, self.db_conn, 'current_references')
                    remove_reference_from_input(ref, self.db_conn)
                    self.db_conn.commit()
                    
                    # Add a new task if there are more references to process
                    if not work_queue.empty():
                        next_ref = work_queue.get()
                        next_future = executor.submit(
                            self.enhance_bibliography_thread_safe, 
                            next_ref, 
                            downloads_dir,
                            force_download
                        )
                        active_futures[next_future] = next_ref

        print("\nFinished processing all references.")

        print("\nFinished processing all references.")
        return processed_count

    def save_final_metadata(self, output_dir):
        """Save metadata from current_references table to a single metadata.json file in the downloads directory."""
        output_dir = Path(output_dir)
        downloads_dir = output_dir / 'downloads'
        metadata_file = downloads_dir / 'metadata.json'
        
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT metadata FROM current_references")
        references = [json.loads(row[0]) for row in cursor.fetchall()]
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump({'references': references}, f, indent=2)
        
        print(f"Saved final metadata file with {len(references)} references to {metadata_file}")

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
        """Attempts to download a PDF from a given URL.
        Includes basic validation and HTML page handling.
        Returns True if successful, False otherwise. Sets ref['downloaded_file'] on success.
        """
        # Safely get title, provide fallback, ensure string
        title_val = (ref or {}).get('title', '') or 'Untitled'
        if not isinstance(title_val, str):
            print(f"{RED}ERROR try_download_url: Title is not a string for ref ID {ref.get('id', 'N/A')}! Using fallback.{RESET}")
            title_val = 'Untitled_check_source_data'

        print(f"DEBUG try_download_url called with source: {source}, url: {url}") # Debug print for source

        # Create safe filename from title
        safe_title = "".join(c for c in (title_val[:50]) if c.isalnum() or c in ' -_').strip()
        filename = downloads_dir / f"{ref.get('year', 'unknown')}_{safe_title}.pdf"
        ref_type = ref.get('type', '')
        
        print(f"\nTrying {source}: {url}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.1',
                'Accept': 'text/html,application/pdf,application/x-pdf,*/*'
            }
            
            # Add timeout to request
            response = requests.get(url, allow_redirects=True, headers=headers, timeout=30)
            
            # Check for specific HTTP error codes
            if response.status_code == 404:
                print("  PDF not found (404)")
                return False
            elif response.status_code == 403:
                print("  Access forbidden (403)")
                return False
            elif response.status_code == 429:
                print("  Too many requests (429) - Rate limited")
                return False
            elif response.status_code != 200:
                print(f"  HTTP error {response.status_code}")
                return False
                    
            content = response.content
            
            # Enhanced PDF validation
            if self.is_valid_pdf(content, ref_type):
                with open(filename, 'wb') as f:
                    f.write(content)
                print("  Direct download successful - Valid complete PDF")
                ref['downloaded_file'] = str(filename)
                ref['download_source'] = source
                return True
            else:
                print(f"  {YELLOW}Downloaded file is not a valid complete PDF{RESET}")

                # If it seems to be HTML, try extracting PDF link
                # --- MODIFICATION HERE ---
                pdf_url_from_html = self.try_extract_pdf_link(url) # Pass the original URL
                if pdf_url_from_html and pdf_url_from_html != url: # Avoid infinite loops
                    print(f"  Found potential PDF link in HTML, retrying download for: {pdf_url_from_html}")
                    # Recursively call try_download_url with the new link, potentially updating source
                    return self.try_download_url(pdf_url_from_html, source + " (via HTML)", ref, downloads_dir)
                else:
                     print(f"  {YELLOW}Could not extract a PDF link from the HTML page.{RESET}")
                # --- END MODIFICATION ---

            return False # Return False if download wasn't valid PDF and no link extracted/retried

        except requests.exceptions.ConnectionError:
            print("  Connection error - Could not reach server")
            return False
        except requests.exceptions.Timeout:
            print("  Request timed out")
            return False
        except requests.exceptions.TooManyRedirects:
            print("  Too many redirects")
            return False
        except requests.exceptions.RequestException as e:
            print(f"  Request failed: {str(e)}")
            return False
        except Exception as e:
            print(f"  Unexpected error: {str(e)}")
            if filename.exists():
                try:
                    filename.unlink()
                    print(f"  Removed potentially incomplete file: {filename.name}")
                except Exception as unlink_e:
                     print(f"  Error removing file during exception handling: {unlink_e}")
            return False

def process_file(json_file, enhancer, done_dir):
    """Process a single JSON file and move it to the done directory after loading to database."""
    try:
        # Move original file to done directory immediately after loading to database
        done_dir.mkdir(parents=True, exist_ok=True)
        target_path = done_dir / json_file.name
        #json_file.rename(target_path)
        print(f"Moved processed file to {target_path}")
    except Exception as e:
        print(f"Error moving {json_file}: {e}")

def test_scihub_search(doi_to_test):
    """
    Helper function to test the BibliographyEnhancer.search_with_scihub method.
    
    Args:
        doi_to_test (str): The DOI to search for on Sci-Hub.
    """
    print(f"\n--- Testing Sci-Hub Search for DOI: {doi_to_test} ---")
    enhancer = None  # Initialize enhancer to None
    try:
        # Instantiate the enhancer (using default db file and email)
        # Make sure BibliographyEnhancer is defined before this point
        enhancer = BibliographyEnhancer() 
        
        # Call the search function
        # Ensure the necessary rate limiters and network components are ready
        result = enhancer.search_with_scihub(doi_to_test)
        
        # Print the result
        if result and isinstance(result, str):
            print(f"{GREEN}Found potential download link:{RESET}")
            print(f"  - URL: {result}")
        else:
            print(f"{YELLOW}No download links found via Sci-Hub for this DOI.{RESET}")
            
    except Exception as e:
        print(f"{RED}An error occurred during the Sci-Hub test: {e}{RESET}")
    finally:
        # Explicitly clean up the enhancer instance if created, 
        # which helps ensure resources like DB connections are closed via __del__.
        if enhancer:
            del enhancer
    print("--- Sci-Hub Test Complete ---")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Enhance bibliography data and download PDFs.")
    # Add the test argument - mutually exclusive with regular operation
    parser.add_argument("--test-scihub", type=str, metavar='DOI', help="Run Sci-Hub test with the specified DOI and exit.")
    
    # Main operation arguments (conditionally required)
    parser.add_argument("--input", required=False, help="Input directory with JSON files (required for main operation)")
    parser.add_argument("--output", required=False, help="Output directory for enhanced results (required for main operation)")
    parser.add_argument("--done", default="done", help="Directory for processed input files")
    parser.add_argument("--force", action="store_true", help="Force download even if already downloaded")
    parser.add_argument("--load-metadata", help="Directory containing existing metadata.json files to load into database")
    parser.add_argument("--skip-referenced-works", action="store_true", help="Skip adding referenced works to input references")
    args = parser.parse_args()
    
    # --- Conditional Execution: Test or Main --- 
    if args.test_scihub:
        # Run the test and exit
        test_scihub_search(args.test_scihub)
        return 
    else:
        # --- Main Logic Pre-check --- 
        # Check if required args are present for main logic
        if not args.input or not args.output:
            parser.error("The following arguments are required for main operation: --input, --output")
        
        # --- Main Logic Execution --- 
        enhancer = BibliographyEnhancer()
        enhancer.results_dir = args.output
        input_dir = Path(args.input)
        done_dir = Path(args.done)
        
        # Load existing metadata if specified
        if args.load_metadata:
            load_metadata_to_database(args.load_metadata, enhancer.db_conn, 'previous_references')
        
        # Load input JSONs into database
        input_dir.mkdir(parents=True, exist_ok=True)
        load_input_jsons(str(input_dir), enhancer.db_conn, 'input_references')
        
        # Move input files to done directory after loading to database
        json_files = list(input_dir.glob("*.json"))
        for json_file in json_files:
            process_file(json_file, enhancer, done_dir)
        
        # Expand input references with their referenced works before processing
        if not args.skip_referenced_works:
            enhancer._expand_inputs_with_referenced_works()
        
        # Process references from database
        enhancer.process_references_from_database(args.output, force_download=args.force, skip_referenced_works=args.skip_referenced_works)
        
        # Save the final metadata file after all processing is complete
        enhancer.save_final_metadata(args.output)

if __name__ == "__main__":
    main()