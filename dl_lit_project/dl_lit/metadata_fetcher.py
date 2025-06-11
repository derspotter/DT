import requests
import time
from collections import deque
from datetime import datetime, timedelta
import threading
from urllib.parse import quote_plus
import json

# ANSI escape codes for colors
YELLOW = '\033[93m'
RESET = '\033[0m'

class RateLimiter:
    """A simple thread-safe rate limiter."""
    def __init__(self, max_per_second):
        self.max_per_second = max_per_second
        self.request_times = deque()
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        now = datetime.now()
        with self.lock:
            while self.request_times and (now - self.request_times[0]) > timedelta(seconds=1):
                self.request_times.popleft()
            
            if len(self.request_times) >= self.max_per_second:
                wait_time = 1 - (now - self.request_times[0]).total_seconds()
                if wait_time > 0:
                    time.sleep(wait_time)
            
            self.request_times.append(datetime.now())

class MetadataFetcher:
    """Fetches and normalizes metadata from external APIs like OpenAlex."""

    def __init__(self, mailto="your.email@example.com"):
        """
        Initializes the fetcher.
        Args:
            mailto: An email address for API politeness headers.
        """
        self.openalex_url = 'https://api.openalex.org/works'
        self.headers = {
            'Accept': 'application/json',
            'User-Agent': f'dl-lit/0.1 (mailto:{mailto})'
        }
        self.rate_limiter = RateLimiter(max_per_second=5) # Conservative rate limit
        self.select_fields = 'id,doi,display_name,authorships,publication_year,type,referenced_works,abstract_inverted_index,keywords,cited_by_api_url,primary_location'

    def _clean_search_term(self, term):
        """Removes commas and extra spaces from a search term."""
        if term is None:
            return None
        return term.replace(',', ' ').strip()

    def search_by_doi(self, doi: str) -> dict | None:
        """Fetches a single work from OpenAlex by its DOI."""
        if not doi:
            return None
        
        # DOIs can be just the identifier or a full URL
        doi_identifier = doi.split('doi.org/')[-1]
        
        url = f"{self.openalex_url}/doi:{doi_identifier}?select={self.select_fields}"
        print(f"[Fetcher] Querying by DOI: {url}")
        
        try:
            self.rate_limiter.wait_if_needed()
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"{YELLOW}[Fetcher] No result found for DOI: {doi}{RESET}")
            else:
                print(f"HTTP error fetching DOI {doi}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request error fetching DOI {doi}: {e}")
            return None

    def search_by_title(self, title: str, authors: list[str] | None = None) -> list[dict] | None:
        """
        Searches OpenAlex by title and optionally filters by author.
        Returns a list of potential matches.
        """
        if not title:
            return None

        cleaned_title = self._clean_search_term(title)
        
        params = {
            'filter': f'title.search:{cleaned_title}',
            'select': self.select_fields,
            'per-page': 5 # Limit to top 5 results for title search
        }

        # If authors are provided, add them to the filter for better matching
        # Note: OpenAlex author filtering can be complex. This is a simple approach.
        if authors:
            author_searches = [f"authorships.author.display_name.search:{self._clean_search_term(author)}" for author in authors]
            params['filter'] += ',' + ','.join(author_searches)

        print(f"[Fetcher] Querying by title: {cleaned_title}")

        try:
            self.rate_limiter.wait_if_needed()
            response = requests.get(self.openalex_url, headers=self.headers, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            return data.get('results', [])
        except requests.exceptions.RequestException as e:
            print(f"Request error searching for title '{title}': {e}")
            return None

    def fetch_metadata(self, entry_to_process: dict) -> dict | None:
        """
        Main method to fetch metadata for an entry from the 'to_download' queue.
        It uses DOI first, then falls back to title search.

        Args:
            entry_to_process: A dictionary representing the row from the database.

        Returns:
            A dictionary with the best-matched, enriched metadata, or None if no match found.
        """
        doi = entry_to_process.get('doi')
        title = entry_to_process.get('title')
        
        # Strategy 1: Use DOI if available (most reliable)
        if doi:
            result = self.search_by_doi(doi)
            if result:
                print(f"[Fetcher] Found match for DOI {doi}")
                return result # Return the single, exact match

        # Strategy 2: Fallback to title search
        if title:
            authors = json.loads(entry_to_process.get('authors', '[]'))
            results = self.search_by_title(title, authors)
            if results:
                # Here you would implement logic to pick the BEST match from the list.
                # For now, we'll just take the first one as a simple approach.
                print(f"[Fetcher] Found {len(results)} potential match(es) for title '{title}'. Taking the first one.")
                return results[0]

        print(f"{YELLOW}[Fetcher] Could not find any match for entry: {title or doi}{RESET}")
        return None
