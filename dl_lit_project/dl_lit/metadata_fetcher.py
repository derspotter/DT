import requests
import json
from .utils import ServiceRateLimiter

# ANSI escape codes for colors
YELLOW = '\033[93m'
RESET = '\033[0m'

class MetadataFetcher:
    """Fetches and normalizes metadata from external APIs like OpenAlex and Unpaywall."""

    def __init__(self, rate_limiter: ServiceRateLimiter, mailto: str = "your.email@example.com"):
        """
        Initializes the fetcher.
        Args:
            rate_limiter: A configured ServiceRateLimiter instance.
            mailto: An email address for API politeness headers.
        """
        self.openalex_url = 'https://api.openalex.org/works'
        self.unpaywall_url = 'https://api.unpaywall.org/v2/'
        self.headers = {
            'Accept': 'application/json',
            'User-Agent': f'dl-lit/0.1 (mailto:{mailto})'
        }
        self.rate_limiter = rate_limiter
        self.select_fields = 'id,doi,display_name,authorships,publication_year,type,referenced_works,abstract_inverted_index,keywords,cited_by_api_url,primary_location,open_access'
        self.mailto = mailto

    def _clean_search_term(self, term):
        """Removes commas and extra spaces from a search term."""
        if term is None:
            return None
        return term.replace(',', ' ').strip()

    def search_openalex_by_doi(self, doi: str) -> dict | None:
        """Fetches a single work from OpenAlex by its DOI."""
        if not doi:
            return None
        
        doi_identifier = doi.split('doi.org/')[-1]
        
        url = f"{self.openalex_url}/doi:{doi_identifier}?select={self.select_fields}"
        print(f"[Fetcher] Querying OpenAlex by DOI: {url}")
        
        try:
            self.rate_limiter.wait_if_needed('openalex')
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"{YELLOW}[Fetcher] No OpenAlex result found for DOI: {doi}{RESET}")
            else:
                print(f"HTTP error fetching DOI {doi} from OpenAlex: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request error fetching DOI {doi} from OpenAlex: {e}")
            return None

    def search_unpaywall(self, doi: str) -> dict | None:
        """Fetches metadata from Unpaywall for a given DOI to find OA sources."""
        if not doi:
            return None
        
        doi_identifier = doi.split('doi.org/')[-1]
        url = f"{self.unpaywall_url}{doi_identifier}?email={self.mailto}"
        print(f"[Fetcher] Querying Unpaywall for DOI: {doi_identifier}")
        
        try:
            self.rate_limiter.wait_if_needed('unpaywall')
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request error fetching DOI {doi} from Unpaywall: {e}")
            return None

    def fetch_metadata(self, entry_to_process: dict) -> dict | None:
        """
        Main method to fetch and consolidate metadata for an entry.
        It queries OpenAlex and Unpaywall.
        """
        doi = entry_to_process.get('doi')
        if not doi:
            print(f"{YELLOW}[Fetcher] Entry has no DOI, cannot fetch metadata. Entry: {entry_to_process.get('title')}{RESET}")
            return None

        openalex_data = self.search_openalex_by_doi(doi)
        unpaywall_data = self.search_unpaywall(doi)

        if not openalex_data:
            print(f"{YELLOW}[Fetcher] Could not find any match for DOI: {doi}{RESET}")
            return None
        
        # Combine results. OpenAlex is the primary source, Unpaywall supplements OA info.
        if unpaywall_data:
            openalex_data['unpaywall'] = unpaywall_data
        
        return openalex_data
