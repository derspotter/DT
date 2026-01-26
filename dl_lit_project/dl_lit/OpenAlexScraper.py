import json
import re
from urllib.parse import quote_plus
import requests
import time
from pathlib import Path
from collections import deque
from datetime import datetime, timedelta
import argparse
import concurrent.futures
import threading
import os
from .utils import get_global_rate_limiter

try:
    from rapidfuzz import fuzz
except ImportError:
    raise ImportError("Module 'rapidfuzz' not found. Install with 'pip install rapidfuzz'")


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

def fetch_citing_work_ids(cited_by_url, rate_limiter, mailto="spott@wzb.eu", max_citations=100):
    """
    Fetches the OpenAlex IDs of works that cite the work at the given URL.
    Handles pagination to get all citing work IDs.
    """
    if not cited_by_url:
        return []

    citing_work_ids = []
    url = f"{cited_by_url}&select=id,title,display_name,authorships,publication_year,doi,type&per-page=100&mailto={mailto}"
    page = 1

    print(f"Fetching citing works from: {cited_by_url} (max: {max_citations})")

    while url and len(citing_work_ids) < max_citations:
        try:
            # Apply rate limiting
            rate_limiter.wait_if_needed('openalex')

            response = requests.get(url, headers={"Accept": "application/json"})
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

def fetch_referenced_work_details(referenced_work_ids, rate_limiter, mailto="spott@wzb.eu"):
    """
    Fetches detailed metadata for a list of referenced work OpenAlex IDs.
    Uses batch requests to efficiently get details for multiple works.
    """
    if not referenced_work_ids:
        return []

    referenced_works = []
    
    # Process in batches of 50 (OpenAlex limit for pipe queries)
    batch_size = 50
    for i in range(0, len(referenced_work_ids), batch_size):
        batch = referenced_work_ids[i:i+batch_size]
        
        # Extract just the work IDs for the batch query
        work_ids = []
        for work_id in batch:
            if isinstance(work_id, str):
                work_ids.append(work_id)
            elif isinstance(work_id, dict) and work_id.get('id'):
                work_ids.append(work_id['id'])
        
        if not work_ids:
            continue
            
        # Create pipe-separated query for batch request
        ids_query = "|".join(work_ids)
        url = f"https://api.openalex.org/works?filter=openalex_id:{ids_query}&select=id,title,display_name,authorships,publication_year,doi,type&per-page=50&mailto={mailto}"
        
        try:
            rate_limiter.wait_if_needed('openalex')
            response = requests.get(url, headers={"Accept": "application/json"})
            response.raise_for_status()
            data = response.json()
            
            results = data.get("results", [])
            for work in results:
                work_details = {
                    'openalex_id': work.get("id"),
                    'title': work.get("display_name"),
                    'authors': [a.get('author', {}).get('display_name') for a in work.get('authorships', [])],
                    'year': work.get("publication_year"),
                    'doi': work.get("doi"),
                    'type': work.get("type")
                }
                referenced_works.append(work_details)
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching referenced work details: {str(e)}")
        except Exception as e:
            print(f"Unexpected error fetching referenced work details: {str(e)}")
    
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
    
    def expand_author_list(author_list_in):
        if not author_list_in:
            return []
        
        # Ensure it's a list to start with
        if isinstance(author_list_in, str):
            author_list_in = [author_list_in]

        expanded_list = []
        for item in author_list_in:
            if not isinstance(item, str):
                continue

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
        self.rate_limiter = rate_limiter or get_global_rate_limiter()
        self.fields = 'id,doi,display_name,authorships,open_access,title,publication_year,type,referenced_works,abstract_inverted_index,keywords,cited_by_api_url'
        self.crossref_fields = 'DOI,title,author,container-title,published-print,published-online,published'

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
            response = requests.get(url, headers=self.crossref_headers)
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
            
            try:
                print(f"\nStep {step}: DOI Direct Search")
                print(f"DOI: {doi}")
                rate_limiter.wait_if_needed('openalex')
                response = requests.get(self.base_url, headers=self.headers, params=params)
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
                response = requests.get(url, headers=self.crossref_headers)
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

            try:
                print(f"\nStep {step}: OpenAlex Query")
                print(f"Params: {params}")
                rate_limiter.wait_if_needed('openalex')
                response = requests.get(self.base_url, headers=self.headers, params=params)
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

def normalize_title_for_match(title: str | None) -> str | None:
    """Normalize titles for exact-ish comparisons."""
    if not title:
        return None
    normalized = title.lower()
    normalized = re.sub(r'[\u2010-\u2015]', '-', normalized)
    normalized = re.sub(r'[^a-z0-9]+', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized or None

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
                searcher.headers['User-Agent'].split('/')[-1] # Extract mailto from User-Agent
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
        'title': best_result.get('display_name'),
        'authors': [a.get('author', {}).get('display_name') for a in best_result.get('authorships', [])],
        'year': best_result.get('publication_year'),
        'doi': best_result.get('doi'),
        'openalex_id': best_result.get('id'),
        'abstract': get_abstract(best_result.get('abstract_inverted_index')),
        'keywords': [kw.get('display_name') for kw in best_result.get('keywords', []) if kw.get('display_name')],
        'source': source_info.get('display_name'),
        'volume': biblio.get('volume'),
        'issue': biblio.get('issue'),
        'pages': pages,
        'publisher': source_info.get('publisher'),
        'type': best_result.get('type'),
        'url': best_result.get('id'),
        'open_access_url': best_result.get('open_access', {}).get('oa_url'),
        'openalex_json': best_result,
        'crossref_json': None,
        'source_work_id': ref.get('source_work_id'),
        'relationship_type': ref.get('relationship_type'),
        'ingest_source': ref.get('ingest_source'),
        'run_id': ref.get('run_id'),
    }


def process_single_reference(ref, searcher, rate_limiter, fetch_references=True, fetch_citations=False, max_citations=100):
    """Process a single reference to find its OpenAlex entry and potentially citing works."""
    title = ref.get('title')
    year = ref.get('year')
    container_title = ref.get('container-title') # Journal or book title
    abstract = ref.get('abstract') # For future use, not directly in search now
    keywords = ref.get('keywords') # For future use
    doi = ref.get('doi')

    if not title and not doi:
        # This print is useful, so we keep it.
        print(f"Skipping reference due to missing title and DOI: {ref.get('id', 'Unknown ID')}")
        return None # Return None on failure

    all_results = {} # Store unique results by ID
    first_success_step = None
    
    # Step 0: DOI-first search if DOI is available
    normalized_doi = normalize_doi(doi)
    if normalized_doi:
        print(f"\nProcessing reference with DOI: {normalized_doi}")
        results = searcher.search(title, year, container_title, abstract, keywords, 0, rate_limiter=rate_limiter, doi=normalized_doi)
        
        if results['success'] and results['results']:
            # DOI match found - accept immediately without author matching
            best_result = results['results'][0]
            print(f"DOI match accepted: {best_result.get('display_name', 'N/A')}")
            
            final_enrichment = _build_enrichment_payload(ref, best_result)
            
            # Fetch related works if requested
            final_enrichment = _fetch_related_works(final_enrichment, best_result, searcher, rate_limiter, 
                                                  fetch_references, fetch_citations, max_citations)
            return final_enrichment
        else:
            print(f"DOI search failed or no results, proceeding with regular search")
    
    # If no DOI or DOI search failed, proceed with regular steps 1-9
    if not title:
        print(f"Skipping reference due to missing title: {ref.get('id', 'Unknown ID')}")
        return None
    
    # Try all steps 1-9 in order, collecting all possible results
    for step in range(1, 10):
        results = searcher.search(title, year, container_title, abstract, keywords, step, rate_limiter=rate_limiter)
        
        if results['success']:
            if first_success_step is None and len(results['results']) > 0:
                first_success_step = step
            
            for result in results['results']:
                result_id = result.get('id')
                if result_id and result_id not in all_results:
                    all_results[result_id] = result
                    all_results[result_id]['first_found_in_step'] = step
    
    unique_results = list(all_results.values())

    # If Crossref returned a DOI, try resolving to OpenAlex via DOI lookup.
    crossref_dois = {
        normalize_doi(result.get('doi'))
        for result in unique_results
        if result.get('id', '').startswith('crossref:') and result.get('doi')
    }
    for crossref_doi in sorted({d for d in crossref_dois if d}):
        doi_lookup = searcher.search(None, None, None, None, None, 0, rate_limiter=rate_limiter, doi=crossref_doi)
        if doi_lookup.get('success') and doi_lookup.get('results'):
            for result in doi_lookup['results']:
                result_id = result.get('id')
                if result_id and result_id not in all_results:
                    result['first_found_in_step'] = 0
                    all_results[result_id] = result
    unique_results = list(all_results.values())
    
    if unique_results:
        ref_authors, ref_editors = get_authors_and_editors(ref)
        
        matched_results = [
            {
                'result': result,
                'author_match_score': find_best_author_match(ref_authors, result, ref_editors),
                'first_found_in_step': all_results[result['id']]['first_found_in_step']
            }
            for result in unique_results
        ]

        matched_results.sort(key=lambda x: (-x['author_match_score'], x['first_found_in_step']))

        # --- DEBUG: Print all match scores ---
        print("DEBUG: Evaluating potential matches...")
        for match in matched_results:
            original_authors = ref.get('authors', 'N/A')
            result_authors = [a.get('author', {}).get('display_name', 'N/A') for a in match['result'].get('authorships', [])]
            print(f"  - Score: {match['author_match_score']:.2f} | Step: {match['first_found_in_step']} | Title: {match['result'].get('display_name', 'N/A')[:60] if match['result'].get('display_name') else 'N/A'}")
            print(f"    Original Ref Authors: {original_authors}")
            print(f"    API Result Authors:   {result_authors}")
        # --- END DEBUG ---

        # Accept the result only if the best score is high enough.
        if matched_results and matched_results[0]['author_match_score'] > 0.85:
            best_result = matched_results[0]['result']
            top_score = matched_results[0]['author_match_score']
            tie_candidates = [m for m in matched_results if top_score - m['author_match_score'] <= 0.05]

            if len(tie_candidates) > 1:
                ref_title_norm = normalize_title_for_match(ref.get('title'))
                ref_year = ref.get('year')

                def tie_key(match):
                    result_title = match['result'].get('display_name') or match['result'].get('title')
                    result_title_norm = normalize_title_for_match(result_title)
                    title_match = 1 if ref_title_norm and result_title_norm and ref_title_norm == result_title_norm else 0
                    result_year = match['result'].get('publication_year')
                    year_distance = abs(ref_year - result_year) if ref_year and result_year else 999
                    return (-match['author_match_score'], -title_match, year_distance, match['first_found_in_step'])

                tie_candidates.sort(key=tie_key)
                best_result = tie_candidates[0]['result']

            # CRITICAL FIX: Construct a clean dict with all necessary data.
            # The original `best_result` is the raw JSON, which is good.
            # We need to ensure all keys the DB expects are present, even if None.
            final_enrichment = _build_enrichment_payload(ref, best_result)
            
            # Fetch related works if requested
            final_enrichment = _fetch_related_works(final_enrichment, best_result, searcher, rate_limiter, 
                                                  fetch_references, fetch_citations, max_citations)
            return final_enrichment

    # If no high-confidence match is found or no results at all, return None.
    return None

def _fetch_related_works(enrichment_data, openalex_result, searcher, rate_limiter, 
                        fetch_references=True, fetch_citations=False, max_citations=100):
    """Helper function to fetch referenced works and citing works for an enriched entry."""
    
    # Initialize related works fields
    enrichment_data['referenced_works'] = []
    enrichment_data['citing_works'] = []
    
    # Fetch referenced works if requested
    if fetch_references and openalex_result.get('referenced_works'):
        print(f"Fetching details for {len(openalex_result['referenced_works'])} referenced works...")
        enrichment_data['referenced_works'] = fetch_referenced_work_details(
            openalex_result['referenced_works'], 
            rate_limiter,
            searcher.headers.get('User-Agent', 'spott@wzb.eu').split('/')[-1]
        )
    
    # Fetch citing works if requested
    if fetch_citations and openalex_result.get('cited_by_api_url'):
        print(f"Fetching citing works (max: {max_citations})...")
        enrichment_data['citing_works'] = fetch_citing_work_ids(
            openalex_result['cited_by_api_url'],
            rate_limiter,
            searcher.headers.get('User-Agent', 'spott@wzb.eu').split('/')[-1],
            max_citations
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

def main():
    parser = argparse.ArgumentParser(description='Scrape OpenAlex for references and citations.')
    parser.add_argument('--bib-dir', type=str, help='Directory containing bibliography JSON files')
    parser.add_argument('--input-file', type=str, help='Single bibliography JSON file to process')
    parser.add_argument('--output-dir', type=str, required=True, help='Directory to save enriched JSON files')
    parser.add_argument('--mailto', type=str, required=True, help='Your email address for OpenAlex API politeness')
    parser.add_argument('--fetch-citations', action='store_true', default=True, help='Fetch citing work IDs (default: True)')
    parser.add_argument('--no-fetch-citations', action='store_false', dest='fetch_citations', help='Do not fetch citing work IDs')
    
    args = parser.parse_args()

    if not args.bib_dir and not args.input_file:
        parser.error('Either --bib-dir or --input-file must be specified.')
    if args.bib_dir and args.input_file:
        parser.error('Specify either --bib-dir or --input-file, not both.')

    searcher = OpenAlexCrossrefSearcher(mailto=args.mailto)

    if args.input_file:
        process_single_file(args.input_file, args.output_dir, searcher, args.fetch_citations)
    elif args.bib_dir:
        process_bibliography_files(args.bib_dir, args.output_dir, searcher, args.fetch_citations)

if __name__ == '__main__':
    main()
