"""
HOW GEMINI FUNCTION CALLING WORKS:
----------------------------------

1. DEFINE: We define functions that Gemini can "request" using FunctionDeclaration
   (This just tells Gemini the name and parameters of functions it can request)

2. REQUEST: When Gemini finds a PDF URL, it creates a "function_call" object in its response
   (This is just a structured JSON saying "please run this function with these arguments")

3. INTERCEPT: Our Python code looks at Gemini's response and finds these function_call objects
   
4. EXECUTE: For each function_call, our code then runs the ACTUAL local Python function
   (Gemini itself never executes code on your computer)

This is similar to a remote API: Gemini sends requests, your local code processes them.
"""

import requests
import os
import re
import sys
import json
import time
from urllib.parse import unquote
from google import genai
from google.genai.types import Tool, GenerateContentConfig, GoogleSearch, FunctionDeclaration
import builtins
import datetime
from google.genai.types import HttpOptions, Schema, AutomaticFunctionCallingConfig, ToolConfig, FunctionCallingConfig
from google.genai.types import Content, Part
import logging
import signal
import threading

# Add logging configuration
def setup_logging(level=logging.INFO):
    """Configure logging for the downloader script"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('downloader')

# Create logger
logger = setup_logging()

def download_pdf(url, output_dir):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Try to get filename from Content-Disposition header
        content_disposition = response.headers.get('Content-Disposition')
        if (content_disposition and 'filename=' in content_disposition):
            filename = re.findall('filename=(.+)', content_disposition)[0].strip('"\'')
        else:
            # Extract filename from URL
            url_path = unquote(url.split('/')[-1].split('?')[0])
            
            # Remove invalid filename characters
            filename = re.sub(r'[\\/*?:"<>|]', "", url_path)
        
        # Ensure the filename has .pdf extension
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
            
        output_path = os.path.join(output_dir, filename)
        
        with open(output_path, 'wb') as pdf_file:
            for chunk in response.iter_content(chunk_size=8192):
                pdf_file.write(chunk)
        print(f"Downloaded {url} to {output_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")

def download_pdfs_from_file(filepath, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    with open(filepath, 'r') as file:
        for line in file:
            url = line.strip()
            if url:
                download_pdf(url, output_dir)

def setup_gemini():
    # Initialize the Gemini client with v1alpha API version for more features
    client = genai.Client(
        api_key=os.getenv('GOOGLE_API_KEY'),
        http_options=HttpOptions(api_version='v1alpha')
    )
    
    # STEP 1: Define what functions Gemini can "request" to be called
    # This is just a description - Gemini doesn't get access to your system
    download_function = FunctionDeclaration(
        name="download_pdf_file",  # The name Gemini will use to request this action
        description="Tells the Python script which PDF URL to download",
        parameters={
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The URL of the PDF file that should be downloaded"
                }
            },
            "required": ["url"]
        }
    )
    
    # Create tools list with both search and function calling capabilities
    tools = [
        Tool(google_search=GoogleSearch()),
        Tool(function_declarations=[download_function])
    ]
    
    return client, tools

class RateLimiter:
    """Rate limiter for API calls with separate limits for search and other operations"""
    def __init__(self):
        self.last_request_time = 0
        self.last_search_time = 0
        self.min_delay = 4.0  # 4 seconds delay = 15 requests per minute (60/15=4)
        self.search_delay = 15.0  # 15 seconds delay = 4 requests per minute (60/4=15)
        self.backoff = 1.0
        self.request_count = 0
        self.search_count = 0

    def wait_if_needed(self, estimated_tokens=0, is_search=False):
        current_time = time.time()
        
        if is_search:
            # More conservative rate limiting for search operations
            elapsed = current_time - self.last_search_time
            delay = self.search_delay
            self.search_count += 1
            count = self.search_count
            
            if (elapsed < delay):
                sleep_time = delay - elapsed
                logger.info(f"Search rate limiting: Waiting for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
            
            self.last_search_time = time.time()
            logger.info(f"Search request #{count} sent at {time.strftime('%H:%M:%S')}")
        else:
            # Standard rate limiting for non-search operations
            elapsed = current_time - self.last_request_time
            delay = self.min_delay
            
            if (elapsed < delay):
                sleep_time = delay - elapsed
                logger.info(f"Rate limiting: Waiting for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
            self.request_count += 1
            logger.info(f"Request #{self.request_count} sent at {time.strftime('%H:%M:%S')}")
    
    def reset_backoff(self):
        self.backoff = 1.0
    
    def handle_response_error(self, error):
        logger.error(f"API error encountered: {str(error)}")
        if "quota" in str(error).lower() or "rate limit" in str(error).lower():
            wait_time = self.backoff * 5
            logger.info(f"Rate limit error detected. Using backoff time: {wait_time} seconds")
            self.backoff *= 2  # Exponential backoff
            return wait_time
        return None

# Enhance the TimeoutHandler to work with signals for more reliable handling
class TimeoutHandler:
    def __init__(self, seconds=30, error_message='API call timed out'):
        self.seconds = seconds
        self.error_message = error_message
        self.original_handler = None
        
    def handle_timeout(self, signum, frame):
        logger.error(f"TIMEOUT ALERT: {self.error_message} after {self.seconds} seconds")
        raise TimeoutError(self.error_message)
        
    def __enter__(self):
        # Use SIGALRM for more reliable timeouts
        self.original_handler = signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)  # Disable the alarm
        signal.signal(signal.SIGALRM, self.original_handler)  # Restore original handler

# Add a helper function to execute API calls with timeouts
def execute_with_timeout(func, timeout=120, error_message="API call timed out", *args, **kwargs):
    """Execute a function with a timeout"""
    try:
        with TimeoutHandler(seconds=timeout, error_message=error_message):
            return func(*args, **kwargs)
    except TimeoutError as e:
        logger.error(f"Timeout occurred: {e}")
        raise

def search_and_download_pdfs_with_function_calling(output_dir):
    if not os.getenv('GOOGLE_API_KEY'):
        print("Please set your GOOGLE_API_KEY environment variable")
        return
    
    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Setup Gemini client with tools
    client, tools = setup_gemini()
    rate_limiter = RateLimiter()
    
    # Get user input for search
    user_query = input("\nEnter your search query for finding PDF documents: ")
    
    # Define the prompt for Gemini with enhanced instructions for searching
    prompt = f'''You are a research assistant helping to find PDF documents on any topic.
    
    TASK: Search for PDF documents related to the following query and download them:
    "{user_query}"
    
    IMPORTANT INSTRUCTIONS:
    1. YOUR FIRST STEP MUST BE TO SEARCH USING THE GOOGLE SEARCH TOOL.
    2. BE AWARE that the search tool has limitations. If you encounter "SEARCH UNAVAILABLE", try to suggest direct URLs.
    3. Make your search very specific - add "filetype:pdf" to your search if not already included.
    4. Explicitly state EACH search query you try before you run it.
    5. Search for relevant documents available as PDFs.
    6. For each valid PDF you find, call the download_pdf_file function with the URL.
    7. After each search, briefly explain what you found or didn't find.
    8. If a search is unsuccessful, try a different search approach or more specific search terms.
    
    For each PDF you find, use the download_pdf_file function to download it.
    
    IMPORTANT: For transparency, whenever you perform a search:
    1. Start with "SEARCH QUERY: [your search query]"
    2. After searching, say "SEARCH RESULTS: [brief summary]"
    3. When calling the download function, say "DOWNLOADING: [url]"
    
    IF SEARCH DOESN'T WORK: If you cannot access search functionality for any reason, please 
    state this clearly as "SEARCH UNAVAILABLE" and then try to suggest the most likely PDF URLs 
    based on common patterns for the requested content. Focus on major institutions like BIS, IMF, 
    central banks, and academic repositories.
    '''
    
    print("\nSearching and downloading PDFs with Gemini...")
    print("(This search and reasoning process may take some time)")
    
    max_retries = 3
    retry_count = 0
    downloaded_count = 0
    search_failures = 0
    
    # Add a note about known search issues
    print("\nℹ️ NOTE: The search functionality in Gemini API is known to be unstable.")
    print("If search fails, the script will attempt to find PDFs using direct URL patterns.")
    
    # Reduce the API timeout for search operations based on observed behavior
    api_timeout = min(60, timeout)  # Shorter timeout for API calls
    
    while retry_count < max_retries:
        try:
            # Wait for rate limiting if needed - applying search-specific rate limiting
            logger.info(f"Starting search attempt #{retry_count + 1}")
            rate_limiter.wait_if_needed(estimated_tokens=1000, is_search=True)
            
            # Make the API call to Gemini with timeout
            logger.info("Sending request to Gemini API...")
            start_time = time.time()
            
            # Add a tighter timeout specifically for the API call
            try:
                def api_call():
                    return client.models.generate_content(
                        contents=prompt,
                        model="gemini-2.0-flash-exp",
                        config=GenerateContentConfig(
                            temperature=0.3,
                            tools=tools
                        )
                    )
                
                logger.info(f"Setting up API call with {api_timeout}s timeout")
                response = execute_with_timeout(
                    api_call,
                    timeout=api_timeout,
                    error_message="Gemini API search request timed out"
                )
                logger.info("API response received successfully")
            except TimeoutError:
                print(f"API request timed out after {api_timeout} seconds. Retrying with direct URL guessing...")
                
                # Try direct URL guessing as fallback
                term = user_query.replace('filetype:pdf', '').strip().strip('"').strip("'")
                fallback_urls = generate_fallback_urls(term)
                
                if fallback_urls:
                    print("\n--- DIRECT URL FALLBACK ---")
                    print(f"Trying direct URLs based on the search term: {term}")
                    downloaded = False
                    
                    for url in fallback_urls[:5]:  # Try the first 5 guessed URLs
                        print(f"Trying: {url}")
                        try:
                            download_pdf(url, output_dir)
                            downloaded_count += 1
                            downloaded = True
                        except Exception as e:
                            print(f"  Failed: {e}")
                            
                    if downloaded:
                        print(f"Successfully downloaded PDFs via direct URL guessing")
                        return
                
                retry_count += 1
                continue
                
            elapsed = time.time() - start_time
            logger.info(f"Received response from Gemini API in {elapsed:.2f} seconds")
            
            # Reset backoff on successful request
            rate_limiter.reset_backoff()
            
            # Process the response
            print("\n--- GEMINI SEARCH PROCESS ---\n")
            
            # Extract and display search queries from response text with more flexible pattern matching
            full_response = response.text
            logger.info(f"Response length: {len(full_response)} characters")
            print(full_response)
            
            # More lenient search detection patterns
            search_queries = re.findall(r"(?:SEARCH QUERY:|search(?:ing)? (?:for|query))[:\s]*(.*?)(?=SEARCH |search|\n\n|RESULTS|$)", 
                                        full_response, re.DOTALL | re.IGNORECASE)
            
            # Alternative pattern if the first one doesn't work
            if not search_queries:
                search_queries = re.findall(r"(?:searching for|I('ll| will) search for|search query)[:\s]*(.*?)(?=\n\n|\.|\n)", 
                                            full_response, re.DOTALL | re.IGNORECASE)
            
            if search_queries:
                print("\n--- SEARCH QUERIES PERFORMED ---")
                for i, query in enumerate(search_queries, 1):
                    print(f"{i}. {query.strip()}")
            
            # Check if search is unavailable with more explicit detection
            if ("SEARCH UNAVAILABLE" in full_response or 
                "cannot access search" in full_response.lower() or
                "don't have access to search" in full_response.lower() or
                "unable to search" in full_response.lower()):
                
                logger.warning("Gemini indicated search functionality is unavailable")
                print("\n⚠️ SEARCH FUNCTIONALITY UNAVAILABLE")
                search_failures += 1
                
                if search_failures > 1:
                    print("Search functionality appears to be consistently unavailable.")
                    print("This is a known limitation with the Gemini API - search quota may be exhausted.")
                
                print("Attempting to find PDFs using direct URL patterns...")
                
                # Generate fallback URLs for direct download attempt
                fallback_urls = generate_fallback_urls(user_query)
                fallback_success = try_fallback_urls(fallback_urls, output_dir)
                
                if fallback_success:
                    downloaded_count += fallback_success
                    return
                else:
                    # If this was the final retry with no success via fallback
                    if retry_count == max_retries - 1:
                        print("\nCould not find relevant PDFs via search or direct URL patterns.")
                        return
            
            # Track found URLs to avoid duplicates
            found_urls = set()
            
            # Process function calls - THIS is where the actual downloading happens
            # Gemini just identifies the URLs, but your local Python code does the downloading
            print("\n--- PDF DOCUMENTS FOUND ---")
            function_calls_made = 0
            
            # This loop looks at Gemini's response for function_call objects
            for candidate in response.candidates:
                for part in candidate.content.parts:
                    # Check if this part of the response contains a function call request
                    if hasattr(part, 'function_call') and part.function_call:
                        function_calls_made += 1
                        function_call = part.function_call
                        
                        # STEP 4: Execute the corresponding local function
                        if function_call.name == "download_pdf_file":
                            # Extract the arguments Gemini wants to pass to the function
                            args = json.loads(function_call.args)
                            url = args.get("url")
                            
                            if url and url not in found_urls:
                                found_urls.add(url)
                                print(f"PDF #{len(found_urls)}: {url}")
                                try:
                                    # THIS is where we actually execute the local function
                                    # (Gemini just requested it, your code actually runs it)
                                    download_pdf(url, output_dir)
                                    downloaded_count += 1
                                except Exception as e:
                                    print(f"Error downloading {url}: {e}")
            
            # If no function calls were made but URLs present in text, extract them
            if not found_urls:
                text_response = response.text
                potential_urls = re.findall(r'https?://\S+?(?:\.pdf|/pdf|/download)\S*', text_response)
                
                if potential_urls:
                    print("\nFound potential PDF URLs in response text:")
                    for url in potential_urls:
                        # Clean up URL (remove trailing punctuation, etc.)
                        url = re.sub(r'[.,;:)"\']+$', '', url)
                        if url not in found_urls:
                            found_urls.add(url)
                            print(f"Found potential PDF: {url}")
                            try:
                                download_pdf(url, output_dir)
                                downloaded_count += 1
                            except Exception as e:
                                print(f"Error downloading {url}: {e}")
            
            # Print summary of the operation
            print("\n--- SEARCH SUMMARY ---")
            if function_calls_made > 0:
                print(f"Function calls made: {function_calls_made}")
            
            if downloaded_count > 0:
                print(f"Successfully downloaded {downloaded_count} PDF documents to {output_dir}")
                return
            else:
                # If this was the last retry and still no PDFs
                if retry_count == max_retries - 1:
                    print("\nNo PDF documents matching your query were found.")
                    print("\nSuggestions:")
                    print("1. Try more specific terms related to your topic")
                    print("2. Try adding 'filetype:pdf' to your search")
                    print("3. Try searching in a different language if applicable")
                    return
                else:
                    # Try another approach automatically
                    print("\nRetrying with a different search approach...")
                    retry_count += 1
                    
        except Exception as e:
            logger.exception(f"Error during search attempt #{retry_count + 1}")
            wait_time = rate_limiter.handle_response_error(e)
            if wait_time:
                print(f"Attempt {retry_count + 1} failed. Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                retry_count += 1
            else:
                print(f"Error with Gemini: {e}")
                logger.error(f"Unrecoverable error: {str(e)}")
                return
    
    print(f"Failed to complete the operation after {max_retries} attempts")

def try_fallback_urls(urls, output_dir):
    """Try downloading PDFs from a list of potential URLs"""
    # Since the user doesn't want fallback approaches, we'll make this a no-op
    print("\nSkipping fallback URL approach as requested.")
    return 0

def generate_fallback_urls(search_term):
    """Generate likely PDF URLs based on the search term when API search fails"""
    # Since the user doesn't want fallback URLs, just return an empty list
    return []

def test_api_connectivity():
    """Simple test to verify basic Gemini API connectivity"""
    print("\n=== Testing Basic Gemini API Connectivity ===")
    
    try:
        # Initialize the client directly
        client = genai.Client(api_key=os.getenv('GOOGLE_API_KEY'))
        
        # Very simple prompt with no tools
        print("Sending a minimal test request to the API...")
        simple_response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents="Respond with only the word 'CONNECTED' if you can see this message."
        )
        
        if "CONNECTED" in simple_response.text:
            print("\n✓ BASIC API CONNECTION SUCCESSFUL")
            print("Your API key is valid and the Gemini service is reachable.\n")
            return True
        else:
            print("\n⚠ API RESPONDED BUT WITH UNEXPECTED CONTENT:")
            print(simple_response.text[:100])
            print("\nThis suggests the API is reachable but might be behaving unusually.\n")
            return False
            
    except Exception as e:
        print("\n✗ BASIC API CONNECTION FAILED")
        print(f"Error: {e}")
        print("\nThis indicates a fundamental connection issue with the Gemini API.")
        print("Please check your API key and internet connection.\n")
        return False

# Global variables to track state across function calls
_current_session_dir = ""
topics_processed = []

def gemini_driven_multi_search(output_dir):
    """
    Have Gemini generate optimized search queries from a research request,
    then process each query one by one.
    """
    if not os.getenv('GOOGLE_API_KEY'):
        print("Please set your GOOGLE_API_KEY environment variable")
        return
    
    # Setup global state
    global _current_session_dir, _topics_processed
    _current_session_dir = os.path.join(output_dir, f"multi_search_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}")
    _topics_processed = []
    os.makedirs(_current_session_dir, exist_ok=True)
    
    # Setup Gemini client
    client = genai.Client(
        api_key=os.getenv('GOOGLE_API_KEY'),
        http_options=HttpOptions(api_version='v1alpha')
    )
    rate_limiter = RateLimiter()
    
    print("\n=== Multi-Topic PDF Search ===\n")
    print("Enter your research text with bullet points.")
    print("Type 'END' on a new line when finished:")
    
    # Collect multi-line input
    text_lines = []
    while True:
        line = input()
        if line.strip() == "END":
            break
        text_lines.append(line)
    
    research_text = "\n".join(text_lines)
    
    # Save the original search text
    with open(os.path.join(_current_session_dir, "research_request.txt"), "w") as f:
        f.write(research_text)
    
    print("\nGenerating optimized search queries with Gemini...")
    
    # Create prompt for generating search queries
    query_generation_prompt = f'''You are a research librarian expert at creating effective search queries.

TASK: Create a list of focused, granular search queries based on the following research request.

RESEARCH REQUEST:
{research_text}

IMPORTANT INSTRUCTIONS:
1. Break down the research request into 10-20 FOCUSED, GRANULAR search topics
2. Each search topic should:
   - Focus on ONE specific concept or document type
   - Be concise (5-10 words is ideal)
   - Include specific keywords that would appear in relevant documents
   - Include "filetype:pdf" to specifically find PDF documents
   - Use quotes for exact phrases when helpful

3. Format your response as a numbered list of queries ONLY, one per line.
   Example:
   1. "BIS exchange rate systems" filetype:pdf
   2. "central bank capital flow analysis" filetype:pdf 
   3. "purchasing power parity report" filetype:pdf

DO NOT include any explanations or additional text - ONLY provide the numbered list of search queries.
'''

    try:
        # Generate the search queries
        logger.info("Requesting search query generation from Gemini API")
        rate_limiter.wait_if_needed()
        
        start_time = time.time()
        
        # Add timeout handling for query generation
        try:
            with TimeoutHandler(seconds=60, error_message="Query generation request timed out"):
                response = client.models.generate_content(
                    model="gemini-2.0-flash-exp",
                    contents=query_generation_prompt,
                    config=GenerateContentConfig(temperature=0.2)
                )
                logger.info("Query generation response received successfully")
        except TimeoutError as e:
            logger.error(f"Timeout during query generation: {e}")
            print("The query generation request timed out. Please try again or use --simple-test to check API connectivity.")
            return
            
        elapsed = time.time() - start_time
        logger.info(f"Received query generation response in {elapsed:.2f} seconds")
        
        # Extract the search queries
        logger.info(f"Raw response text length: {len(response.text)} characters")
        search_queries = []
        for line in response.text.strip().split('\n'):
            # Extract query from numbered list format (e.g., "1. query text")
            match = re.match(r'^\s*\d+\.\s*(.*)\s*$', line)
            if match and match.group(1).strip():
                search_queries.append(match.group(1).strip())
        
        # If we couldn't parse the queries properly, try a different approach
        if not search_queries:
            logger.warning("Failed to parse queries from numbered list format, attempting alternative parsing")
            search_queries = [line.strip() for line in response.text.strip().split('\n') if line.strip()]
        
        logger.info(f"Extracted {len(search_queries)} search queries")
        
        # Save the search queries
        with open(os.path.join(_current_session_dir, "search_queries.txt"), "w") as f:
            for i, query in enumerate(search_queries, 1):
                f.write(f"{i}. {query}\n")
        
        print(f"\nGenerated {len(search_queries)} search queries:")
        for i, query in enumerate(search_queries, 1):
            print(f"{i}. {query}")
            
        # Process each query sequentially
        print("\nProcessing search queries sequentially...")
        
        skipped_queries = []  # FIXED: Added this missing variable
        
        for i, query in enumerate(search_queries, 1):  # FIXED: Using better loop with index
            logger.info(f"Starting processing of query #{i}: {query}")
            # Create a directory for this query
            safe_name = re.sub(r'[\\/*?:"<>|]', '_', query)[:50]
            if i > 1:  # Add number to ensure uniqueness
                safe_name = f"{safe_name}_{i}"
                
            query_dir = os.path.join(_current_session_dir, safe_name)
            os.makedirs(query_dir, exist_ok=True)
            
            # Save query to file
            with open(os.path.join(query_dir, "query.txt"), "w") as f:
                f.write(query)
            
            print(f"\n{'='*80}")
            print(f"PROCESSING QUERY {i}/{len(search_queries)}: {query}")
            print(f"{'='*80}")
            print(f"Directory: {query_dir}")
            
            # Override input for search
            original_input = builtins.input
            
            class QueryInput:
                def __init__(self, query_text):
                    self.query = query_text
                
                def input(self, prompt):
                    print(f"{prompt}{self.query}")
                    return self.query
            
            try:
                # Replace input function
                builtins.input = QueryInput(query).input
                
                # Add timeout handling for each individual query
                start_time = time.time()
                logger.info(f"Beginning search for query #{i}")
                
                # Set a maximum time for each query (5 minutes)
                query_timeout = 300  # 5 minutes in seconds
                
                try:
                    # Use the signal-based timeout for the entire search process
                    with TimeoutHandler(seconds=query_timeout, 
                                       error_message=f"Search for query #{i} timed out completely"):
                        search_and_download_pdfs_with_function_calling(query_dir)
                except TimeoutError as e:
                    logger.error(f"Query #{i} timed out after {query_timeout} seconds: {e}")
                    print(f"⚠️ Search for query #{i} timed out after {query_timeout} seconds. Skipping to next query.")
                    skipped_queries.append({"query": query, "reason": "timeout", "query_num": i})
                    continue
                    
                elapsed = time.time() - start_time
                logger.info(f"Completed search for query #{i} in {elapsed:.2f} seconds")
                
                # Count PDFs downloaded and add to topics_processed for summary
                pdf_count = len([f for f in os.listdir(query_dir) if f.lower().endswith('.pdf')])
                logger.info(f"Query #{i} resulted in {pdf_count} PDFs")
                _topics_processed.append({"query": query, "pdf_count": pdf_count})
                
            except Exception as e:
                logger.exception(f"Error processing query #{i}")
                print(f"Error processing query #{i}: {e}")
                skipped_queries.append({"query": query, "reason": str(e), "query_num": i})
            finally:
                # Restore original input function
                builtins.input = original_input
            
            # Add a short delay between queries to ensure any lingering processes are terminated
            logger.info(f"Adding a 2-second buffer between queries")
            time.sleep(2)
        
        # Generate a summary
        print(f"\n{'='*80}")
        print("MULTI-TOPIC SEARCH COMPLETE")
        print(f"{'='*80}")
        
        # Calculate statistics
        total_queries = len(_topics_processed)
        total_skipped = len(skipped_queries)
        total_pdfs = sum(topic["pdf_count"] for topic in _topics_processed)
        queries_with_pdfs = sum(1 for topic in _topics_processed if topic["pdf_count"] > 0)
        
        print(f"Total queries processed: {total_queries}")
        print(f"Queries that found PDFs: {queries_with_pdfs}")
        print(f"Queries skipped due to errors: {total_skipped}")
        print(f"Total PDFs downloaded: {total_pdfs}")
        
        # Write a summary file
        with open(os.path.join(_current_session_dir, "search_summary.txt"), "w") as f:
            f.write("Multi-Topic Search Summary\n")
            f.write("========================\n\n")
            f.write(f"Search performed: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Queries processed: {total_queries}\n")
            f.write(f"Queries that found PDFs: {queries_with_pdfs}\n")
            f.write(f"Total PDFs downloaded: {total_pdfs}\n\n")
            f.write("Queries and results:\n")
            
            for i, topic in enumerate(_topics_processed, 1):
                f.write(f"{i}. {topic['query']} ({topic['pdf_count']} PDFs)\n")
            
            # Add information about skipped queries
            if skipped_queries:
                f.write("\nSkipped queries:\n")
                for sq in skipped_queries:
                    f.write(f"- Query #{sq['query_num']}: {sq['query']} (Reason: {sq['reason']})\n")
        
        print(f"Search results saved to: {_current_session_dir}/")
        
    except Exception as e:
        logger.exception("Error during multi-topic search")
        print(f"Error during multi-topic search: {e}")
        print(f"Error details: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        print("For more details, try running with --simple-test first to check API connectivity")

# Add a new command line option for timeout settings
if __name__ == "__main__":
    # Specify the output directory
    output_dir = 'downloaded_pdfs'
    
    # Add command line options
    if "--verbose" in sys.argv:
        logger = setup_logging(level=logging.DEBUG)
        sys.argv.remove("--verbose")
    
    # Add timeout setting option
    timeout = 120  # Default timeout in seconds
    for i, arg in enumerate(sys.argv):
        if arg == "--timeout" and i + 1 < len(sys.argv):
            try:
                timeout = int(sys.argv[i + 1])
                sys.argv.remove("--timeout")
                sys.argv.remove(str(timeout))
                print(f"Using custom timeout of {timeout} seconds")
            except (ValueError, IndexError):
                pass
    
    # Command line argument parsing
    if len(sys.argv) > 1:
        if sys.argv[1] == "--search":
            search_and_download_pdfs_with_function_calling(output_dir)
        elif sys.argv[1] == "--file" and len(sys.argv) > 2:
            filepath = sys.argv[2]
            download_pdfs_from_file(filepath, output_dir)
        elif sys.argv[1] == "--multi-search":
            gemini_driven_multi_search(output_dir)
        elif sys.argv[1] == "--test":
            # First test basic connectivity without complex tools
            if not test_api_connectivity():
                print("Skipping full test due to API connectivity issues.")
                sys.exit(1)
            
            # Quick test to verify search functionality works
            print("\n=== Testing PDF Search Functionality ===")
            print("Running a test search that should reliably find PDFs...")
            
            # Override output directory for test
            test_dir = os.path.join(output_dir, 'test')
            if not os.path.exists(test_dir):
                os.makedirs(test_dir)
            
            class TestQuery:
                def __init__(self, query):
                    self.query = query
                
                def input(self, prompt):
                    print(f"{prompt}{self.query}")
                    return self.query
            
            # Save original input function
            original_input = builtins.input
            
            try:
                # Replace input with our test version that returns a predefined query
                test_query = "BIS documents on balance of payments / capital flows filetype:pdf"
                builtins.input = TestQuery(test_query).input
                
                # Call the search function that we know works
                print("\nUsing predefined test query that should find PDFs:")
                
                # Set up an intercept to track downloads
                original_download_pdf = download_pdf
                downloaded_files = []
                
                def test_download_pdf(url, dir):
                    print(f"TEST: Would download {url} to {dir}")
                    downloaded_files.append(url)
                    # Don't actually download in test mode
                    return
                
                # Replace download function for testing
                download_pdf = test_download_pdf
                
                # Call the search function
                search_and_download_pdfs_with_function_calling(test_dir)
                
                # Evaluate results
                if downloaded_files:
                    print(f"\n✓ TEST PASSED: Found {len(downloaded_files)} PDF links")
                    print("The PDF search functionality is working correctly.")
                    test_passed = True
                    # Ask if user wants to actually download test PDFs
                    builtins.input = original_input  # Restore original input first
                    response = input("\nDo you want to actually download these PDFs? (y/n): ")
                    if response.lower() == 'y':
                        print("\nDownloading test PDFs...")
                        for url in downloaded_files:
                            original_download_pdf(url, test_dir)
                        print(f"Downloaded {len(downloaded_files)} PDFs to {test_dir}/")
                else:
                    print("\n✗ TEST FAILED: No PDF links were found.")
                    print("There might be an issue with the API or search functionality.")
                
            except Exception as e:
                print(f"\n✗ TEST FAILED: Error occurred: {e}")
                print("Please check your API key and internet connection.")
            finally:
                # Ensure we restore the original functions
                builtins.input = original_input
                if 'original_download_pdf' in locals():
                    download_pdf = original_download_pdf
        
        elif sys.argv[1] == "--simple-test":
            # Just run the basic connectivity test
            test_api_connectivity()
        
        else:
            print("Usage:")
            print("  python downloader.py --search            # Search and download PDFs using Gemini")
            print("  python downloader.py --file file_path    # Download PDFs from file")
            print("  python downloader.py --multi-search      # Run Gemini-driven multi-topic search")
            print("  python downloader.py --test              # Run a test of search functionality")
            print("  python downloader.py --simple-test       # Test basic API connectivity")
    else:
        # Default to original behavior for backward compatibility
        filepath = 'pdf_links.txt'
        download_pdfs_from_file(filepath, output_dir)
