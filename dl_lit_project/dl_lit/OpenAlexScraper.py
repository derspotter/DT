import json
import re
from urllib.parse import quote_plus
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import argparse
import concurrent.futures
import os
from .utils import get_global_rate_limiter

try:
    from rapidfuzz import fuzz
except ImportError:
    raise ImportError("Module 'rapidfuzz' not found. Install with 'pip install rapidfuzz'")


def _build_retry_session(
    *,
    total_retries: int = 5,
    backoff_factor: float = 0.5,
    pool_connections: int = 64,
    pool_maxsize: int = 64,
):
    """Create a requests session with retry/backoff tuned for OpenAlex/Crossref."""
    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_connections, pool_maxsize=pool_maxsize)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


_missing_api_key_warned = False


def clean_search_term(term):
    """Remove commas and clean up the search term."""
    if term is None:
        return None
    # Replace commas with spaces and normalize multiple spaces to single spaces
    return ' '.join(term.replace(',', ' ').split())

def convert_inverted_index_to_text(inverted_index):
    """Convert an abstract_inverted_index to readable text"""
    if not inverted_index:
        return ""

    # Create a list of (word, position) tuples
    word_positions = []
    for word, positions in inverted_index.items():
        for position in positions:
            word_positions.append((word, position))

    # Sort by position
    word_positions.sort(key=lambda x: x[1])

    # Join words
    text = " ".join(word for word, _ in word_positions)
    return text

def fetch_citing_work_ids(
    cited_by_url,
    rate_limiter,
    mailto="spott@wzb.eu",
    max_citations=100,
    include_links=False,
    session=None,
    timeout=20,
    api_key=None,
):
    """
    Fetches the OpenAlex IDs of works that cite the work at the given URL.
    Handles pagination to get all citing work IDs.
    """
    if not cited_by_url:
        return []

    citing_work_ids = []
    select_fields = "id,title,display_name,authorships,publication_year,doi,type"
    if include_links:
        select_fields += ",referenced_works,cited_by_api_url"
    url = f"{cited_by_url}&select={select_fields}&per-page=100&mailto={mailto}"
    if api_key:
        url = f"{url}&api_key={quote_plus(api_key)}"
    page = 1

    print(f"Fetching citing works from: {cited_by_url} (max: {max_citations})")

    while url and len(citing_work_ids) < max_citations:
        try:
            # Apply rate limiting
            rate_limiter.wait_if_needed('openalex')
            http = session or requests
            response = http.get(url, headers={"Accept": "application/json"}, timeout=timeout)
            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            for work in results:
                if len(citing_work_ids) >= max_citations:
                    break
                    
                # Extract work details, not just ID
                work_details = {
                    'openalex_id': work.get("id"),
                    'title': work.get("display_name"),
                    'authors': [a.get('author', {}).get('display_name') for a in work.get('authorships', [])],
                    'year': work.get("publication_year"),
                    'doi': work.get("doi"),
                    'type': work.get("type")
                }
                if include_links:
                    work_details['referenced_works'] = work.get("referenced_works") or []
                    work_details['cited_by_api_url'] = work.get("cited_by_api_url")
                citing_work_ids.append(work_details)

            # Check for next page
            meta = data.get("meta", {})
            if meta.get("next_page") and len(citing_work_ids) < max_citations:
                url = meta["next_page"]
                page += 1
                print(f"Fetching next page of citing works: Page {page}")
            else:
                url = None # No more pages or reached limit

        except requests.exceptions.RequestException as e:
            print(f"Error fetching citing works from {url}: {str(e)}")
            # Stop fetching if there's an error
            url = None
        except Exception as e:
            print(f"Unexpected error fetching citing works from {url}: {str(e)}")
            url = None

    print(f"Finished fetching citing works. Found {len(citing_work_ids)}")
    return citing_work_ids

def fetch_referenced_work_details(
    referenced_work_ids,
    rate_limiter,
    mailto="spott@wzb.eu",
    include_links: bool = False,
    session=None,
    timeout=20,
    api_key=None,
):
    """
    Fetches detailed metadata for a list of referenced work OpenAlex IDs.
    Uses concurrent batch requests to efficiently get details for multiple works.
    """
    if not referenced_work_ids:
        return []

    # Process in batches of 50 (OpenAlex limit for pipe queries)
    batch_size = 50
    batches = []
    
    for i in range(0, len(referenced_work_ids), batch_size):
        batch = referenced_work_ids[i:i+batch_size]
        work_ids = []
        for work_id in batch:
            if isinstance(work_id, str):
                work_ids.append(work_id)
            elif isinstance(work_id, dict) and work_id.get('id'):
                work_ids.append(work_id['id'])
        if work_ids:
            batches.append(work_ids)

    if not batches:
        return []

    def _fetch_batch(work_ids):
        ids_query = "|".join(work_ids)
        select_fields = "id,title,display_name,authorships,publication_year,doi,type"
        if include_links:
            select_fields += ",referenced_works,cited_by_api_url"
        url = f"https://api.openalex.org/works?filter=openalex_id:{ids_query}&select={select_fields}&per-page=50&mailto={mailto}"
        if api_key:
            url = f"{url}&api_key={quote_plus(api_key)}"
        
        try:
            rate_limiter.wait_if_needed('openalex')
            http = session or requests
            response = http.get(url, headers={"Accept": "application/json"}, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            
            batch_results = []
            for work in data.get("results", []):
                work_details = {
                    'openalex_id': work.get("id"),
                    'title': work.get("display_name"),
                    'authors': [a.get('author', {}).get('display_name') for a in work.get('authorships', [])],
                    'year': work.get("publication_year"),
                    'doi': work.get("doi"),
                    'type': work.get("type"),
                }
                if include_links:
                    work_details['referenced_works'] = work.get("referenced_works") or []
                    work_details['cited_by_api_url'] = work.get("cited_by_api_url")
                batch_results.append(work_details)
            return batch_results
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching referenced work details: {str(e)}")
            return []
        except Exception as e:
            print(f"Unexpected error fetching referenced work details: {str(e)}")
            return []

    referenced_works = []
    
    # We use ThreadPoolExecutor to run batches concurrently.
    # Max workers clamped to 8 to avoid overwhelming the single rate limiter suddenly.
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(batches), 8)) as executor:
        futures = [executor.submit(_fetch_batch, batch) for batch in batches]
        for future in concurrent.futures.as_completed(futures):
            referenced_works.extend(future.result())
            
    print(f"Fetched details for {len(referenced_works)} referenced works")
    return referenced_works

def get_authors_from_ref(ref):
    """
    Collect all authors and editors from a reference entry into a single list.
    OpenAlex only has authors, so we treat editors as additional authors.
    """
    authors = []
    
    # Get authors array if present
    if isinstance(ref.get('authors'), list):
        authors.extend(ref['authors'])
    
    # Get editors array if present
    if isinstance(ref.get('editors'), list):
        authors.extend(ref['editors'])
    
    # Get single editor if present
    elif isinstance(ref.get('editor'), str):
        authors.append(ref['editor'])
    
    return authors

def normalize_name(name):
    """Split academic name into first name(s) and last name, standardize format"""
    if not name:
        return None, None

    name = name.lower().strip()
    
    # Handle nobility particles that can appear in different positions
    particles = ['von', 'van', 'de', 'du', 'der', 'la', 'le', 'da', 'dos', 'del']
    
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
        
        # Handle particles: move them to the last name
        name_parts = []
        particle_parts = []
        
        for part in parts:
            if part in particles:
                particle_parts.append(part)
            else:
                name_parts.append(part)
        
        if len(name_parts) == 0:
            return None, None
        
        # Last name includes particles + actual last name
        last_name_parts = particle_parts + [name_parts[-1]]
        last_name = ' '.join(last_name_parts)
        first_names = ' '.join(name_parts[:-1])

    # Remove non-alphabetic characters from first names (like initials)
    first_names = ''.join(c for c in first_names if c.isalpha() or c.isspace()).strip()

    return first_names, last_name

def match_author_name(ref_author, result_author):
    """Match academic author names with tolerance for initials and fuzzy logic"""
    ref_first, ref_last = normalize_name(ref_author)
    res_first, res_last = normalize_name(result_author)

    # Last names must match with high similarity
    if not ref_last or not res_last:
        return 0.0
    last_name_similarity = fuzz.ratio(ref_last.lower(), res_last.lower())
    if last_name_similarity < 85: # Strict threshold for last names
        return 0.0

    # If last names match, check first names
    if not ref_first or not res_first:
        # If one has no first name, rely on last name match score
        score = last_name_similarity / 100.0
        return score

    # Check for initialism (e.g., "J. R. R." vs "John Ronald Reuel")
    ref_initials = "".join([name[0] for name in ref_first.split()])
    res_initials = "".join([name[0] for name in res_first.split()])

    if ref_initials.lower() == res_initials.lower():
        return 1.0 # Very likely a match

    # Fuzzy match on full first names
    first_name_similarity = fuzz.partial_ratio(ref_first.lower(), res_first.lower())

    # Combine scores, giving more weight to last name
    combined_score = (0.7 * last_name_similarity + 0.3 * first_name_similarity) / 100.0
    
    return combined_score

def find_best_author_match(ref_authors, result, ref_editors=None):
    """
    Match authors and editors against result authors.
    If either authors OR editors match well (>0.85), return the best score.
    """
    if ref_editors is None:
        ref_editors = []

    result_authors = [
        author['author']['display_name']
        for author in result.get('authorships', [])
        if author.get('author') and author['author'].get('display_name')
    ]

    if not result_authors:
        # If no authors in result, we can't perform a match.
        return 0.0

    scores = []
    
    # Match reference authors separately
    if ref_authors:
        author_scores = []
        for ref_author in ref_authors:
            for res_author in result_authors:
                author_scores.append(match_author_name(ref_author, res_author))
        
        if author_scores:
            # Use the average of the top N scores, where N is the number of reference authors
            author_scores.sort(reverse=True)
            top_author_scores = author_scores[:len(ref_authors)]
            author_match_score = sum(top_author_scores) / len(top_author_scores)
            scores.append(author_match_score)
    
    # Match reference editors separately  
    if ref_editors:
        editor_scores = []
        for ref_editor in ref_editors:
            for res_author in result_authors:
                editor_scores.append(match_author_name(ref_editor, res_author))
        
        if editor_scores:
            # Use the average of the top N scores, where N is the number of reference editors
            editor_scores.sort(reverse=True)
            top_editor_scores = editor_scores[:len(ref_editors)]
            editor_match_score = sum(top_editor_scores) / len(top_editor_scores)
            scores.append(editor_match_score)

    if not scores:
        return 0.0

    # Return the BEST score (either authors or editors)
    return max(scores)

def get_authors_and_editors(ref):
    """Get separate lists of authors and editors from reference, expanding multi-author strings."""

    def _normalize_people_field(raw_value):
        """Normalize person fields that may arrive as list, JSON text, or plain string."""
        if not raw_value:
            return []

        if isinstance(raw_value, str):
            candidate = raw_value.strip()
            if not candidate:
                return []
            if candidate.startswith("[") and candidate.endswith("]"):
                try:
                    parsed = json.loads(candidate)
                except json.JSONDecodeError:
                    parsed = [raw_value]
                else:
                    if isinstance(parsed, list):
                        raw_value = parsed
                    elif isinstance(parsed, str):
                        raw_value = [parsed]
                    else:
                        raw_value = [raw_value]
            else:
                raw_value = [raw_value]
        elif not isinstance(raw_value, list):
            raw_value = [raw_value]

        normalized = []
        for item in raw_value:
            if isinstance(item, str):
                item = item.strip()
                if item:
                    normalized.append(item)
                continue

            if isinstance(item, dict):
                display = item.get("display_name") or item.get("name") or item.get("full_name")
                if isinstance(display, str) and display.strip():
                    normalized.append(display.strip())
                    continue

                first = item.get("given") or item.get("first_name") or item.get("first")
                last = item.get("family") or item.get("last_name") or item.get("last")
                first = first.strip() if isinstance(first, str) else ""
                last = last.strip() if isinstance(last, str) else ""
                if first or last:
                    normalized.append(f"{last}, {first}".strip(", "))

        return normalized

    def expand_author_list(author_list_in):
        author_list_in = _normalize_people_field(author_list_in)
        if not author_list_in:
            return []

        expanded_list = []
        for item in author_list_in:
            # Heuristic: if a string contains multiple commas, it might be a list of authors
            # in the format "Last1, F1., Last2, F2."
            if item.count(',') > 1:
                # This is a brittle parser for "Last, F., Last, F." format
                # It assumes an even number of comma-separated parts.
                sub_parts = [p.strip() for p in item.split(',')]
                if len(sub_parts) % 2 == 0:
                    it = iter(sub_parts)
                    for last, first in zip(it, it):
                        expanded_list.append(f"{last}, {first}")
                    continue # Go to next item in author_list_in
            
            # If not a multi-author string, or if parsing failed, add the original item
            expanded_list.append(item)
            
        return expanded_list

    authors = expand_author_list(ref.get('authors', []))
    editors = expand_author_list(ref.get('editors', []))
    # Also check for singular 'editor' field for backward compatibility
    if ref.get('editor') and not editors:
        editors = expand_author_list(ref.get('editor', []))
        
    return authors, editors


class OpenAlexCrossrefSearcher:
    def __init__(self, mailto='spott@wzb.eu', rate_limiter=None):
        self.base_url = 'https://api.openalex.org/works'
        self.headers = {
            'Accept': 'application/json',
            'User-Agent': f'OpenAlexScraper/{mailto}'
        }
        self.crossref_base_url = 'https://api.crossref.org/works'
        self.crossref_headers = {
            'Accept': 'application/json',
            'User-Agent': f'OpenAlexScraper/1.0 (mailto:{mailto})'
        }
        self.semantic_scholar_base_url = 'https://api.semanticscholar.org/graph/v1'
        self.semantic_scholar_headers = {
            'Accept': 'application/json',
            'User-Agent': f'OpenAlexScraper/1.0 (mailto:{mailto})'
        }
        semantic_scholar_api_key = os.getenv('SEMANTIC_SCHOLAR_API_KEY') or os.getenv('RAG_FEEDER_SEMANTIC_SCHOLAR_API_KEY')
        if semantic_scholar_api_key:
            self.semantic_scholar_headers['x-api-key'] = semantic_scholar_api_key
        self.rate_limiter = rate_limiter or get_global_rate_limiter()
        self.fields = 'id,doi,display_name,authorships,open_access,title,publication_year,type,referenced_works,abstract_inverted_index,keywords,cited_by_api_url'
        self.crossref_fields = 'DOI,title,author,container-title,published-print,published-online,published'
        self.api_key = os.getenv('OPENALEX_API_KEY') or os.getenv('RAG_FEEDER_OPENALEX_API_KEY')
        self.enable_openalex_fallback = os.getenv('RAG_FEEDER_ENABLE_OPENALEX_FALLBACK', '0').strip().lower() in {'1', 'true', 'yes', 'on'}
        global _missing_api_key_warned
        if not self.api_key and not _missing_api_key_warned:
            print(
                "[WARN] OPENALEX_API_KEY not set. OpenAlex daily credits may be severely limited without an API key."
            )
            _missing_api_key_warned = True
        self.openalex_timeout = float(os.getenv('RAG_FEEDER_OPENALEX_TIMEOUT', '20'))
        self.crossref_timeout = float(os.getenv('RAG_FEEDER_CROSSREF_TIMEOUT', '20'))
        self.semantic_scholar_timeout = float(os.getenv('RAG_FEEDER_SEMANTIC_SCHOLAR_TIMEOUT', '20'))
        self.openalex_session = _build_retry_session()
        self.crossref_session = _build_retry_session()
        self.semantic_scholar_session = _build_retry_session()

    def _with_openalex_auth(self, params):
        if self.api_key:
            params['api_key'] = self.api_key
        return params

    @staticmethod
    def _semantic_scholar_to_openalex_like(item):
        if not item:
            return None
        external_ids = item.get('externalIds') or {}
        doi = normalize_doi(external_ids.get('DOI') or external_ids.get('Doi'))
        paper_id = item.get('paperId') or item.get('paper_id')
        authorships = []
        for author in item.get('authors') or []:
            name = author.get('name') if isinstance(author, dict) else None
            if name:
                authorships.append({'author': {'display_name': name}})
        open_access_url = ((item.get('openAccessPdf') or {}).get('url') if isinstance(item.get('openAccessPdf'), dict) else None)
        return {
            'id': f"semantic_scholar:{paper_id}" if paper_id else 'semantic_scholar:',
            'doi': doi,
            'display_name': item.get('title'),
            'publication_year': item.get('year'),
            'type': ((item.get('publicationTypes') or [None])[0] or item.get('publicationType')),
            'authorships': authorships,
            'referenced_works': [],
            'abstract_inverted_index': None,
            'keywords': [],
            'cited_by_api_url': None,
            'primary_location': {
                'source': {
                    'display_name': item.get('venue'),
                    'publisher': None,
                }
            } if item.get('venue') else {},
            'biblio': {},
            'open_access': {'oa_url': open_access_url},
            'semantic_scholar_url': item.get('url'),
            'semantic_scholar_json': item,
        }

    def search_semantic_scholar_match(self, title):
        if not title:
            return {"step": "s2_match", "results": [], "success": False, "error": "Title required for Semantic Scholar match"}
        params = {
            'query': title,
            'fields': 'title,year,authors,externalIds,venue,url,publicationTypes,openAccessPdf,paperId',
        }
        try:
            print("\nStep s2_match: Semantic Scholar title match")
            print(f"Query: {title}")
            self.rate_limiter.wait_if_needed('semantic_scholar')
            response = self.semantic_scholar_session.get(
                f'{self.semantic_scholar_base_url}/paper/search/match',
                params=params,
                headers=self.semantic_scholar_headers,
                timeout=self.semantic_scholar_timeout,
            )
            if response.status_code == 404:
                return {"step": "s2_match", "results": [], "success": True}
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and isinstance(data.get('data'), list):
                raw_item = data['data'][0] if data['data'] else None
            else:
                raw_item = data
            result = self._semantic_scholar_to_openalex_like(raw_item)
            return {"step": "s2_match", "results": [result] if result else [], "success": True}
        except requests.exceptions.RequestException as e:
            return {"step": "s2_match", "results": [], "success": False, "error": str(e)}
        except Exception as e:
            return {"step": "s2_match", "results": [], "success": False, "error": f"Unexpected Semantic Scholar error: {str(e)}"}

    def search_crossref(self, title, authors, year):
        """Search Crossref for matching works."""
        query = []
        if title:
            query.append(f'title:{quote_plus(title)}')
        if authors:
            authors_str = '+'.join(quote_plus(author) for author in authors)
            query.append(f'author:{authors_str}')
        if year:
            query.append(f'published:{year}')
            
        if not query:
            return []
            
        url = f'https://api.crossref.org/works?query={"+".join(query)}&rows=5'
        
        try:
            self.rate_limiter.wait_if_needed('crossref')
            response = self.crossref_session.get(url, headers=self.crossref_headers, timeout=self.crossref_timeout)
            response.raise_for_status()
            data = response.json()
            
            if data['message']['items']:
                # Convert Crossref results to OpenAlex-like format for compatibility
                results = []
                for item in data['message']['items']:
                    result = {
                        'id': f"crossref:{item.get('DOI', '')}",
                        'doi': item.get('DOI'),
                        'display_name': item.get('title', [''])[0],
                        'publication_year': item.get('published-print', {}).get('date-parts', [[None]])[0][0],
                        'type': item.get('type'),
                        'authorships': [
                            {
                                'author': {
                                    'display_name': f"{author.get('given', '')} {author.get('family', '')}"
                                }
                            }
                            for author in item.get('author', [])
                        ]
                    }
                    results.append(result)
                return results
            return []
                
        except Exception as e:
            print(f"Crossref error: {e}")
            return []

    def search(self, title, year, container_title, abstract, keywords, step, rate_limiter=None, doi=None):
        """
        Search OpenAlex and Crossref with different strategies based on step number.
        Step 0: DOI-first search (if DOI available)
        Steps 1-7: use OpenAlex filter queries
        Step 8: use Crossref search
        Step 9: use OpenAlex search parameter for container_title
        """
        rate_limiter = rate_limiter or self.rate_limiter
        # Step 0: DOI-first search - highest priority
        if step == 0:
            doi = normalize_doi(doi)
            if not doi:
                return {"step": step, "results": [], "success": False, "error": "DOI required for step 0"}
            
            # Direct DOI lookup in OpenAlex
            params = {
                'filter': f'doi:{doi}',
                'select': self.fields,
                'per-page': 1
            }
            params = self._with_openalex_auth(params)
            
            try:
                print(f"\nStep {step}: DOI Direct Search")
                print(f"DOI: {doi}")
                rate_limiter.wait_if_needed('openalex')
                response = self.openalex_session.get(
                    self.base_url,
                    headers=self.headers,
                    params=params,
                    timeout=self.openalex_timeout,
                )
                response.raise_for_status()
                data = response.json()
                
                results = data.get('results', [])
                if results:
                    print(f"DOI match found: {results[0].get('display_name', 'N/A')}")
                    return {"step": step, "results": results, "success": True, "meta": data.get('meta')}
                else:
                    print(f"No DOI match found in OpenAlex")
                    return {"step": step, "results": [], "success": True} # Success but no results
                    
            except requests.exceptions.RequestException as e:
                return {"step": step, "results": [], "success": False, "error": str(e)}
            except Exception as e:
                return {"step": step, "results": [], "success": False, "error": f"Unexpected DOI search error: {str(e)}"}
        
        if not title and not container_title:
            return {"step": step, "results": [], "success": False}

        # Clean up the terms
        title = clean_search_term(title)
        container = clean_search_term(container_title)

        # Steps 1-7: use filter parameter
        if step in [1, 4]:
            if container:
                query = f'display_name:"{title}"|"{container}"'
            else:
                query = f'display_name:"{title}"'
        elif step in [2, 5]:
            if container:
                query = f'title.search:"{title}"|"{container}"'
            else:
                query = f'title.search:"{title}"'
        elif step in [3, 6]:
            if container:
                query = f'title.search:{title}|{container}'
            else:
                query = f'title.search:{title}'
        elif step == 7:
            search_query = f'{title}'
            params = {
                'search': search_query,
                'select': self.fields,
                'per-page': 10
            }
            params = self._with_openalex_auth(params)
        
        # Step 8: Crossref search
        elif step == 8:
            try:
                print(f"\nStep {step}: Crossref Search")
                query = []
                if title:
                    query.append(f'title:{quote_plus(title)}')
                if container:
                    query.append(f'container-title:{quote_plus(container)}')
                if year:
                    query.append(f'published:{year}')
                    
                if not query:
                    return {"step": step, "results": [], "success": False}
                    
                query_string = '+'.join(query)
                url = f'https://api.crossref.org/works?query={query_string}&rows=10'
                
                print(f"Crossref Query: {url}")
                
                rate_limiter.wait_if_needed('crossref')
                response = self.crossref_session.get(url, headers=self.crossref_headers, timeout=self.crossref_timeout)
                response.raise_for_status()
                data = response.json()
                
                if data['message']['items']:
                    # Convert Crossref results to OpenAlex-like format
                    results = []
                    for item in data['message']['items']:
                        # Get publication year from the most specific date available
                        pub_year = None
                        for date_field in ['published-print', 'published-online', 'published']:
                            if date_parts := item.get(date_field, {}).get('date-parts', [[None]])[0]:
                                pub_year = date_parts[0]
                                if pub_year:
                                    break

                        result = {
                            'id': f"crossref:{item.get('DOI', '')}",
                            'doi': item.get('DOI'),
                            'display_name': item.get('title', [''])[0],
                            'publication_year': pub_year,
                            'type': item.get('type'),
                            'authorships': [
                                {
                                    'author': {
                                        'display_name': f"{author.get('given', '')} {author.get('family', '')}"
                                    }
                                }
                                for author in item.get('author', [])
                            ],
                            'referenced_works': [], # Crossref doesn't provide this directly
                            'abstract_inverted_index': None, # Crossref doesn't provide this
                            'keywords': [], # Crossref doesn't provide this
                            'cited_by_api_url': item.get('is-referenced-by-count', 0) > 0 and f"https://api.crossref.org/works/{item.get('DOI')}/references" or None
                        }
                        results.append(result)
                    return {"step": step, "results": results, "success": True}
                else:
                    return {"step": step, "results": [], "success": True} # Success but no results
            except requests.exceptions.RequestException as e:
                return {"step": step, "results": [], "success": False, "error": str(e)}
            except Exception as e:
                return {"step": step, "results": [], "success": False, "error": f"Unexpected Crossref error: {str(e)}"}

        # Step 9: use search parameter for container_title
        elif step == 9:
            if not container:
                return {"step": step, "results": [], "success": False, "error": "Container title needed for step 9"}
            search_query = f'{container}'
            params = {
                'search': search_query,
                'select': self.fields,
                'per-page': 10
            }
            params = self._with_openalex_auth(params)
        else:
            return {"step": step, "results": [], "success": False, "error": "Invalid step"}

        # Common OpenAlex request logic for steps 1-7 and 9
        if step != 8:
            if step in [1,2,3,4,5,6]: # Filter based queries
                 params = {
                    'filter': query + (f",publication_year:{year}" if year and step in [1,2,3] else ""),
                    'select': self.fields,
                    'per-page': 10
                }
                 params = self._with_openalex_auth(params)

            try:
                print(f"\nStep {step}: OpenAlex Query")
                print(f"Params: {params}")
                rate_limiter.wait_if_needed('openalex')
                response = self.openalex_session.get(
                    self.base_url,
                    headers=self.headers,
                    params=params,
                    timeout=self.openalex_timeout,
                )
                response.raise_for_status()
                data = response.json()
                return {"step": step, "results": data.get('results', []), "success": True, "meta": data.get('meta')}
            except requests.exceptions.RequestException as e:
                return {"step": step, "results": [], "success": False, "error": str(e)}
            except Exception as e:
                return {"step": step, "results": [], "success": False, "error": f"Unexpected OpenAlex error: {str(e)}"}

def normalize_doi(raw_doi: str | None) -> str | None:
    """Normalize DOI strings to a bare, lowercase form."""
    if not raw_doi:
        return None
    doi = raw_doi.strip().lower()
    doi = re.sub(r'^(https?://)?(dx\.)?doi\.org/', '', doi)
    doi = re.sub(r'^doi:\s*', '', doi)
    doi = doi.strip().strip(' .;,')
    return doi or None


def coerce_year_int(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        try:
            return int(value)
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"\b(\d{4})\b", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None

def normalize_title_for_match(title: str | None) -> str | None:
    """Normalize titles for exact-ish comparisons."""
    if not title:
        return None
    normalized = title.lower()
    normalized = re.sub(r'[\u2010-\u2015]', '-', normalized)
    normalized = re.sub(r'[^a-z0-9]+', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized or None


def title_token_similarity(left: str | None, right: str | None) -> float:
    left_norm = normalize_title_for_match(left)
    right_norm = normalize_title_for_match(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 1.0
    token_score = fuzz.token_set_ratio(left_norm, right_norm) / 100.0
    partial_score = fuzz.partial_ratio(left_norm, right_norm) / 100.0
    left_tokens = set(left_norm.split())
    right_tokens = set(right_norm.split())
    jaccard = len(left_tokens & right_tokens) / max(1, len(left_tokens | right_tokens)) if left_tokens and right_tokens else 0.0
    return max(token_score, 0.85 * partial_score + 0.15 * jaccard)

def process_single_file(json_file, output_dir, searcher, fetch_citations=True):
    """Process a single JSON bibliography file."""
    output_path = Path(output_dir) / Path(json_file).name
    enriched_references = []
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            references = json.load(f)
    except Exception as e:
        print(f"Error reading {json_file}: {e}")
        return

    for ref in references:
        enriched_ref = process_single_reference(ref, searcher, searcher.rate_limiter)
        if enriched_ref and fetch_citations and enriched_ref.get('best_match_result', {}).get('cited_by_api_url'):
            print(f"Fetching citations for: {enriched_ref.get('best_match_result', {}).get('display_name')}")
            enriched_ref['citing_works_ids'] = fetch_citing_work_ids(
                enriched_ref['best_match_result']['cited_by_api_url'], 
                searcher.rate_limiter, 
                searcher.headers['User-Agent'].split('/')[-1], # Extract mailto from User-Agent
                session=searcher.openalex_session,
                timeout=searcher.openalex_timeout,
                api_key=searcher.api_key,
            )
        enriched_references.append(enriched_ref if enriched_ref else ref)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(enriched_references, f, indent=2)
    print(f"Enriched data saved to {output_path}")

def get_abstract(inverted_index):
    """Converts an OpenAlex inverted index to a string."""
    if not inverted_index:
        return None
    
    word_positions = {}
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions[pos] = word
            
    # Sort by position and join the words
    sorted_words = [word_positions[i] for i in sorted(word_positions.keys())]
    return " ".join(sorted_words)

def _build_enrichment_payload(ref, best_result):
    """Build a consistent enrichment payload for downstream consumers."""
    # Extract best-effort source/biblio metadata
    primary_location = best_result.get('primary_location') or {}
    source_info = primary_location.get('source') or {}
    biblio = best_result.get('biblio') or {}

    pages = None
    first_page = biblio.get('first_page')
    last_page = biblio.get('last_page')
    if first_page and last_page:
        pages = f"{first_page}--{last_page}"

    return {
        'title': best_result.get('display_name') or best_result.get('title'),
        'authors': [a.get('author', {}).get('display_name') for a in best_result.get('authorships', [])],
        'year': best_result.get('publication_year'),
        'doi': best_result.get('doi'),
        'openalex_id': best_result.get('id') if str(best_result.get('id') or '').startswith('https://openalex.org/') else None,
        'abstract': get_abstract(best_result.get('abstract_inverted_index')),
        'keywords': [kw.get('display_name') for kw in best_result.get('keywords', []) if kw.get('display_name')],
        'source': source_info.get('display_name') or best_result.get('venue'),
        'volume': biblio.get('volume'),
        'issue': biblio.get('issue'),
        'pages': pages,
        'publisher': source_info.get('publisher'),
        'type': best_result.get('type'),
        'url': best_result.get('semantic_scholar_url') or best_result.get('id'),
        'open_access_url': best_result.get('open_access', {}).get('oa_url'),
        'referenced_work_ids': list(best_result.get('referenced_works') or []),
        'cited_by_api_url': best_result.get('cited_by_api_url'),
        'openalex_json': best_result if str(best_result.get('id') or '').startswith('https://openalex.org/') else None,
        'crossref_json': best_result if str(best_result.get('id') or '').startswith('crossref:') else None,
        'source_work_id': ref.get('source_work_id'),
        'relationship_type': ref.get('relationship_type'),
        'ingest_source': ref.get('ingest_source'),
        'run_id': ref.get('run_id'),
    }


def process_single_reference(
    ref,
    searcher,
    rate_limiter,
    fetch_references=True,
    fetch_citations=False,
    max_citations=100,
    related_depth: int = 1,
    related_depth_upstream: int = 1,
    max_related_per_reference: int = 40,
    return_diagnostics: bool = False,
):
    """Process a single reference to find its OpenAlex entry and potentially citing works."""
    diagnostics = {
        "had_api_error": False,
        "api_errors": [],
        "attempted_steps": [],
        "status": "unknown",
    }
    non_api_errors = {
        "DOI required for step 0",
        "Container title needed for step 9",
        "Invalid step",
    }

    def _record_api_error(step_name, result_payload):
        diagnostics["had_api_error"] = True
        diagnostics["api_errors"].append(
            {
                "step": step_name,
                "error": (result_payload or {}).get("error"),
            }
        )

    def _select_best_result(unique_results, ref, ref_authors, ref_editors, ref_year):
        ref_people_present = bool(ref_authors or ref_editors)
        matched_results = [
            (
                lambda result: {
                    'result': result,
                    'provider': 'crossref' if str(result.get('id') or '').startswith('crossref:') else 'other',
                    'author_match_score': find_best_author_match(ref_authors, result, ref_editors),
                    'title_similarity': title_token_similarity(ref.get('title'), result.get('display_name') or result.get('title')),
                    'combined_score': 0.0,
                    'first_found_in_step': result.get('first_found_in_step', 999),
                    'passes_author_gate': True,
                }
            )(result)
            for result in unique_results
        ]

        for match in matched_results:
            author_bonus = 0.2 if match['author_match_score'] > 0.85 else 0.0
            result_year = match['result'].get('publication_year')
            year_bonus = 0.0
            if ref_year and result_year:
                try:
                    if int(ref_year) == int(result_year):
                        year_bonus = 0.1
                    elif abs(int(ref_year) - int(result_year)) <= 1:
                        year_bonus = 0.05
                except Exception:
                    year_bonus = 0.0
            match['combined_score'] = match['title_similarity'] + author_bonus + year_bonus
            match['min_title_similarity'] = 0.88
            if match['provider'] == 'crossref' and ref_people_present:
                match['passes_author_gate'] = match['author_match_score'] > 0.85

        matched_results.sort(key=lambda x: (-x['combined_score'], -x['title_similarity'], -x['author_match_score'], x['first_found_in_step']))

        print("DEBUG: Evaluating potential matches...")
        for match in matched_results:
            original_authors = ref.get('authors', 'N/A')
            result_authors = [a.get('author', {}).get('display_name', 'N/A') for a in match['result'].get('authorships', [])]
            print(f"  - Provider: {match['provider']} | Combined: {match['combined_score']:.2f} | TitleSim: {match['title_similarity']:.2f} | Author: {match['author_match_score']:.2f} | AuthorGate: {match['passes_author_gate']} | Step: {match['first_found_in_step']} | Title: {match['result'].get('display_name', 'N/A')[:60] if match['result'].get('display_name') else 'N/A'}")
            print(f"    Original Ref Authors: {original_authors}")
            print(f"    API Result Authors:   {result_authors}")

        eligible_matches = [m for m in matched_results if m['passes_author_gate']]
        if not eligible_matches:
            return None
        if eligible_matches[0]['title_similarity'] < eligible_matches[0]['min_title_similarity'] or eligible_matches[0]['combined_score'] < 0.9:
            return None

        best_result = eligible_matches[0]['result']
        top_score = eligible_matches[0]['combined_score']
        tie_candidates = [m for m in eligible_matches if top_score - m['combined_score'] <= 0.05]
        if len(tie_candidates) > 1:
            ref_title_norm = normalize_title_for_match(ref.get('title'))

            def tie_key(match):
                result_title = match['result'].get('display_name') or match['result'].get('title')
                result_title_norm = normalize_title_for_match(result_title)
                title_match = 1 if ref_title_norm and result_title_norm and ref_title_norm == result_title_norm else 0
                result_year = coerce_year_int(match['result'].get('publication_year'))
                ref_year_int = coerce_year_int(ref_year)
                year_distance = abs(ref_year_int - result_year) if ref_year_int and result_year else 999
                return (-match['combined_score'], -match['title_similarity'], -title_match, year_distance, match['first_found_in_step'])

            tie_candidates.sort(key=tie_key)
            best_result = tie_candidates[0]['result']

        return best_result

    title = ref.get('title')
    year = ref.get('year')
    container_title = ref.get('container-title') # Journal or book title
    abstract = ref.get('abstract') # For future use, not directly in search now
    keywords = ref.get('keywords') # For future use
    doi = ref.get('doi')

    if not title and not doi:
        # This print is useful, so we keep it.
        print(f"Skipping reference due to missing title and DOI: {ref.get('id', 'Unknown ID')}")
        diagnostics["status"] = "invalid_input"
        if return_diagnostics:
            return None, diagnostics
        return None # Return None on failure

    all_results = {} # Store unique results by ID
    first_success_step = None
    
    # Step 0: DOI-first search if DOI is available
    normalized_doi = normalize_doi(doi)
    if normalized_doi:
        print(f"\nProcessing reference with DOI: {normalized_doi}")
        diagnostics["attempted_steps"].append(0)
        results = searcher.search(title, year, container_title, abstract, keywords, 0, rate_limiter=rate_limiter, doi=normalized_doi)
        if not results.get("success") and str(results.get("error") or "") not in non_api_errors:
            _record_api_error(0, results)
        
        if results['success'] and results['results']:
            # DOI match found - accept immediately without author matching
            best_result = results['results'][0]
            print(f"DOI match accepted: {best_result.get('display_name', 'N/A')}")
            
            final_enrichment = _build_enrichment_payload(ref, best_result)
            
            # Fetch related works if requested
            final_enrichment = _fetch_related_works(
                final_enrichment,
                best_result,
                searcher,
                rate_limiter,
                fetch_references,
                fetch_citations,
                max_citations,
                related_depth_upstream=related_depth_upstream,
                related_depth=related_depth,
                max_related_per_reference=max_related_per_reference,
            )
            diagnostics["status"] = "matched"
            if return_diagnostics:
                return final_enrichment, diagnostics
            return final_enrichment
        else:
            print(f"DOI search failed or no results, proceeding with regular search")
    
    # If no DOI or DOI search failed, use the primary title-based fallback chain:
    # Semantic Scholar title match -> Crossref -> optional OpenAlex.
    if not title:
        print(f"Skipping reference due to missing title: {ref.get('id', 'Unknown ID')}")
        diagnostics["status"] = "invalid_input"
        if return_diagnostics:
            return None, diagnostics
        return None

    ref_authors, ref_editors = get_authors_and_editors(ref)
    ref_year = ref.get('year')

    diagnostics["attempted_steps"].append("s2_match")
    s2_results = searcher.search_semantic_scholar_match(title)
    if not s2_results.get("success") and str(s2_results.get("error") or "") not in non_api_errors:
        _record_api_error("s2_match", s2_results)
    if s2_results.get('success') and s2_results.get('results'):
        s2_unique_results = []
        for result in s2_results['results']:
            result_id = result.get('id')
            if not result_id:
                continue
            result['first_found_in_step'] = -1
            s2_unique_results.append(result)
        best_s2_result = _select_best_result(s2_unique_results, ref, ref_authors, ref_editors, ref_year)
        if best_s2_result is not None:
            final_enrichment = _build_enrichment_payload(ref, best_s2_result)
            final_enrichment = _fetch_related_works(
                final_enrichment,
                best_s2_result,
                searcher,
                rate_limiter,
                fetch_references,
                fetch_citations,
                max_citations,
                related_depth_upstream=related_depth_upstream,
                related_depth=related_depth,
                max_related_per_reference=max_related_per_reference,
            )
            diagnostics["status"] = "matched"
            if return_diagnostics:
                return final_enrichment, diagnostics
            return final_enrichment

    regular_steps = [8]
    if searcher.enable_openalex_fallback:
        regular_steps.extend(range(1, 8))
        regular_steps.append(9)

    for step in regular_steps:
        diagnostics["attempted_steps"].append(step)
        if step == "s2_match":
            results = searcher.search_semantic_scholar_match(title)
        else:
            results = searcher.search(title, year, container_title, abstract, keywords, step, rate_limiter=rate_limiter)
        if not results.get("success") and str(results.get("error") or "") not in non_api_errors:
            _record_api_error(step, results)
        
        if results['success']:
            if first_success_step is None and len(results['results']) > 0:
                first_success_step = step
            
            for result in results['results']:
                result_id = result.get('id')
                if result_id and result_id not in all_results:
                    all_results[result_id] = result
                    all_results[result_id]['first_found_in_step'] = -1 if step == "s2_match" else step
    
    unique_results = list(all_results.values())

    # Crossref DOI resolution back into OpenAlex is optional and disabled by default.
    if searcher.enable_openalex_fallback:
        crossref_dois = {
            normalize_doi(result.get('doi'))
            for result in unique_results
            if result.get('id', '').startswith('crossref:') and result.get('doi')
        }
        for crossref_doi in sorted({d for d in crossref_dois if d}):
            diagnostics["attempted_steps"].append("crossref_doi_resolve")
            doi_lookup = searcher.search(None, None, None, None, None, 0, rate_limiter=rate_limiter, doi=crossref_doi)
            if not doi_lookup.get("success") and str(doi_lookup.get("error") or "") not in non_api_errors:
                _record_api_error("crossref_doi_resolve", doi_lookup)
            if doi_lookup.get('success') and doi_lookup.get('results'):
                for result in doi_lookup['results']:
                    result_id = result.get('id')
                    if result_id and result_id not in all_results:
                        result['first_found_in_step'] = 0
                        all_results[result_id] = result
        unique_results = list(all_results.values())
    
    if unique_results:
        best_result = _select_best_result(unique_results, ref, ref_authors, ref_editors, ref_year)
        if best_result is not None:
            # CRITICAL FIX: Construct a clean dict with all necessary data.
            # The original `best_result` is the raw JSON, which is good.
            # We need to ensure all keys the DB expects are present, even if None.
            final_enrichment = _build_enrichment_payload(ref, best_result)
            
            # Fetch related works if requested
            final_enrichment = _fetch_related_works(
                final_enrichment,
                best_result,
                searcher,
                rate_limiter,
                fetch_references,
                fetch_citations,
                max_citations,
                related_depth_upstream=related_depth_upstream,
                related_depth=related_depth,
                max_related_per_reference=max_related_per_reference,
            )
            diagnostics["status"] = "matched"
            if return_diagnostics:
                return final_enrichment, diagnostics
            return final_enrichment

    # If no high-confidence match is found or no results at all, return None.
    diagnostics["status"] = "api_error" if diagnostics["had_api_error"] else "no_match"
    if return_diagnostics:
        return None, diagnostics
    return None

def _normalize_openalex_id(value: str | None) -> str | None:
    if not value:
        return None
    if value.startswith("http"):
        return value.rstrip("/").split("/")[-1]
    return value


def _fetch_related_works(
    enrichment_data,
    openalex_result,
    searcher,
    rate_limiter,
    fetch_references=True,
    fetch_citations=False,
    max_citations=100,
    related_depth_upstream: int = 1,
    related_depth: int = 1,
    max_related_per_reference: int = 40,
):
    """Helper function to fetch referenced works and citing works for an enriched entry."""
    
    # Keep raw parent graph pointers even when related expansion is disabled.
    enrichment_data['referenced_work_ids'] = list(openalex_result.get('referenced_works') or [])
    enrichment_data['cited_by_api_url'] = openalex_result.get('cited_by_api_url')

    # Initialize expanded related works fields
    enrichment_data['referenced_works'] = []
    enrichment_data['citing_works'] = []
    
    # Fetch referenced works if requested
    if fetch_references and openalex_result.get('referenced_works'):
        print(f"Fetching details for {len(openalex_result['referenced_works'])} referenced works...")
        enrichment_data['referenced_works'] = fetch_referenced_work_details(
            openalex_result['referenced_works'], 
            rate_limiter,
            searcher.headers.get('User-Agent', 'spott@wzb.eu').split('/')[-1],
            include_links=related_depth > 1,
            session=searcher.openalex_session,
            timeout=searcher.openalex_timeout,
            api_key=searcher.api_key,
        )

        if related_depth > 1 and enrichment_data['referenced_works']:
            ref_map: dict[str, list[str]] = {}
            secondary_ids: set[str] = set()
            for ref in enrichment_data['referenced_works']:
                ref_id = _normalize_openalex_id(ref.get('openalex_id'))
                if not ref_id:
                    continue
                raw_ids = ref.get('referenced_works') or []
                cleaned_ids = []
                for item in raw_ids:
                    if isinstance(item, str):
                        cleaned_ids.append(_normalize_openalex_id(item))
                    elif isinstance(item, dict) and item.get('id'):
                        cleaned_ids.append(_normalize_openalex_id(item.get('id')))
                cleaned_ids = [cid for cid in cleaned_ids if cid]
                if max_related_per_reference:
                    cleaned_ids = cleaned_ids[:max_related_per_reference]
                if not cleaned_ids:
                    continue
                ref_map[ref_id] = cleaned_ids
                secondary_ids.update(cleaned_ids)

            if secondary_ids:
                secondary_details = fetch_referenced_work_details(
                    list(secondary_ids),
                    rate_limiter,
                    searcher.headers.get('User-Agent', 'spott@wzb.eu').split('/')[-1],
                    include_links=False,
                    session=searcher.openalex_session,
                    timeout=searcher.openalex_timeout,
                    api_key=searcher.api_key,
                )
                detail_map = {
                    _normalize_openalex_id(item.get('openalex_id')): item for item in secondary_details
                }
                for ref in enrichment_data['referenced_works']:
                    ref_id = _normalize_openalex_id(ref.get('openalex_id'))
                    if not ref_id or ref_id not in ref_map:
                        continue
                    ref['referenced_works_expanded'] = [
                        detail_map[child_id]
                        for child_id in ref_map[ref_id]
                        if child_id in detail_map
                    ]
    
    # Fetch citing works if requested
    if fetch_citations and openalex_result.get('cited_by_api_url'):
        print(f"Fetching citing works (max: {max_citations})...")
        enrichment_data['citing_works'] = fetch_citing_work_ids(
            openalex_result['cited_by_api_url'],
            rate_limiter,
            searcher.headers.get('User-Agent', 'spott@wzb.eu').split('/')[-1],
            max_citations,
            include_links=related_depth_upstream > 1,
            session=searcher.openalex_session,
            timeout=searcher.openalex_timeout,
            api_key=searcher.api_key,
        )
        if related_depth_upstream > 1 and enrichment_data['citing_works']:
            print(f"Fetching second-level citing works (depth: {related_depth_upstream})...")
            for citing_work in enrichment_data['citing_works']:
                cited_by_url = citing_work.get('cited_by_api_url')
                if not cited_by_url:
                    continue
                citing_work['citing_works_expanded'] = fetch_citing_work_ids(
                    cited_by_url,
                    rate_limiter,
                    searcher.headers.get('User-Agent', 'spott@wzb.eu').split('/')[-1],
                    min(max_related_per_reference, max_citations),
                    include_links=False,
                    session=searcher.openalex_session,
                    timeout=searcher.openalex_timeout,
                    api_key=searcher.api_key,
                )
    
    return enrichment_data

def process_bibliography_files(bib_dir, output_dir, searcher, fetch_citations=True):
    """Process all JSON bibliography files in a directory using ThreadPoolExecutor."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    json_files = list(Path(bib_dir).rglob('*.json'))

    if not json_files:
        print(f"No JSON files found in {bib_dir}")
        return

    # Using ThreadPoolExecutor to process files in parallel
    # Adjust max_workers based on your system and API limits
    # Since RateLimiter is thread-safe, this should be fine.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for json_file in json_files:
            futures.append(executor.submit(process_single_file, json_file, output_dir, searcher, fetch_citations))
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # To raise exceptions if any occurred in the thread
            except Exception as e:
                print(f"Error processing a file: {e}")

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Scrape OpenAlex for references and citations.')
    parser.add_argument('--bib-dir', type=str, help='Directory containing bibliography JSON files')
    parser.add_argument('--input-file', type=str, help='Single bibliography JSON file to process')
    parser.add_argument('--output-dir', type=str, required=True, help='Directory to save enriched JSON files')
    parser.add_argument('--mailto', type=str, required=True, help='Your email address for OpenAlex API politeness')
    parser.add_argument('--fetch-citations', action='store_true', default=True, help='Fetch citing work IDs (default: True)')
    parser.add_argument('--no-fetch-citations', action='store_false', dest='fetch_citations', help='Do not fetch citing work IDs')
    
    args = parser.parse_args(argv)

    if not args.bib_dir and not args.input_file:
        parser.error('Either --bib-dir or --input-file must be specified.')
    if args.bib_dir and args.input_file:
        parser.error('Specify either --bib-dir or --input-file, not both.')

    searcher = OpenAlexCrossrefSearcher(mailto=args.mailto)

    if args.input_file:
        process_single_file(args.input_file, args.output_dir, searcher, args.fetch_citations)
    elif args.bib_dir:
        process_bibliography_files(args.bib_dir, args.output_dir, searcher, args.fetch_citations)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
