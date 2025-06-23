import json
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

try:
    from rapidfuzz import fuzz
except ImportError:
    raise ImportError("Module 'rapidfuzz' not found. Install with 'pip install rapidfuzz'")

class RateLimiter:
    def __init__(self, max_per_second):
        self.max_per_second = max_per_second
        self.request_times = deque()
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        now = datetime.now()
        
        with self.lock:
            # Remove timestamps older than 1 second
            while self.request_times and (now - self.request_times[0]) > timedelta(seconds=1):
                self.request_times.popleft()
            
            # If we've hit the limit, wait
            if len(self.request_times) >= self.max_per_second:
                wait_time = 1 - (now - self.request_times[0]).total_seconds()
                if wait_time > 0:
                    time.sleep(wait_time)
            
            # Record this request
            self.request_times.append(now)

def clean_search_term(term):
    """Remove commas and clean up the search term."""
    if term is None:
        return None
    return term.replace(',', ' ')

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

def fetch_citing_work_ids(cited_by_url, rate_limiter, mailto="spott@wzb.eu"):
    """
    Fetches the OpenAlex IDs of works that cite the work at the given URL.
    Handles pagination to get all citing work IDs.
    """
    if not cited_by_url:
        return []

    citing_work_ids = []
    url = f"{cited_by_url}&select=id&per-page=100&mailto={mailto}" # Request only ID, increase per-page limit
    page = 1

    print(f"Fetching citing work IDs from: {cited_by_url}")

    while url:
        try:
            # Apply rate limiting
            rate_limiter.wait_if_needed()

            response = requests.get(url, headers={"Accept": "application/json"})
            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            for work in results:
                work_id = work.get("id")
                if work_id:
                    citing_work_ids.append(work_id)

            # Check for next page
            meta = data.get("meta", {})
            if meta.get("next_page"):
                url = meta["next_page"]
                page += 1
                print(f"Fetching next page of citing works: Page {page}")
            else:
                url = None # No more pages

        except requests.exceptions.RequestException as e:
            print(f"Error fetching citing works from {url}: {str(e)}")
            # Stop fetching if there's an error
            url = None
        except Exception as e:
            print(f"Unexpected error fetching citing works from {url}: {str(e)}")
            url = None

    print(f"Finished fetching citing work IDs. Found {len(citing_work_ids)}")
    return citing_work_ids

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
    ref_editors is the list of editors from the reference, if available.
    """
    if ref_editors is None:
        ref_editors = []

    result_authors = [
        author['author']['display_name']
        for author in result.get('authorships', [])
        if author.get('author') and author['author'].get('display_name')
    ]

    # Combine authors and editors for matching
    all_ref_authors = ref_authors + ref_editors

    if not all_ref_authors:
        # If no authors in reference, we can't perform a match.
        return 0.0

    if not result_authors:
        # If no authors in result, we can't perform a match.
        return 0.0
    
    # Calculate match scores for all pairs of authors
    scores = []
    for ref_author in all_ref_authors:
        for res_author in result_authors:
            scores.append(match_author_name(ref_author, res_author))

    if not scores:
        return 0.0

    # Use the average of the top N scores, where N is the number of authors in the reference.
    # This rewards results that match multiple authors.
    scores.sort(reverse=True)
    num_authors_to_consider = len(all_ref_authors)
    top_scores = scores[:num_authors_to_consider]
    
    # Check for empty top_scores to avoid division by zero
    if not top_scores:
        return 0.0

    average_score = sum(top_scores) / len(top_scores)
    
    return average_score

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
    editors = expand_author_list(ref.get('editor', []))
        
    return authors, editors


class OpenAlexCrossrefSearcher:
    def __init__(self, mailto='spott@wzb.eu'):
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
        self.rate_limiter = RateLimiter(max_per_second=5)  # Very conservative rate limit
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
            self.rate_limiter.wait_if_needed()
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

    def search(self, title, year, container_title, abstract, keywords, step, rate_limiter):
        """
        Search OpenAlex and Crossref with different strategies based on step number.
        Steps 1-7: use OpenAlex filter queries
        Step 8: use Crossref search
        Step 9: use OpenAlex search parameter for container_title
        """
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
                
                rate_limiter.wait_if_needed()
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
                    'filter': query + (f",publication_year:{year}" if year and step in [4,5,6] else ""),
                    'select': self.fields,
                    'per-page': 10
                }

            try:
                print(f"\nStep {step}: OpenAlex Query")
                print(f"Params: {params}")
                rate_limiter.wait_if_needed()
                response = requests.get(self.base_url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()
                return {"step": step, "results": data.get('results', []), "success": True, "meta": data.get('meta')}
            except requests.exceptions.RequestException as e:
                return {"step": step, "results": [], "success": False, "error": str(e)}
            except Exception as e:
                return {"step": step, "results": [], "success": False, "error": f"Unexpected OpenAlex error: {str(e)}"}

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

def process_single_reference(ref, searcher, rate_limiter):
    """Process a single reference to find its OpenAlex entry and potentially citing works."""
    title = ref.get('title')
    year = ref.get('year')
    container_title = ref.get('container-title') # Journal or book title
    abstract = ref.get('abstract') # For future use, not directly in search now
    keywords = ref.get('keywords') # For future use

    if not title:
        # This print is useful, so we keep it.
        print(f"Skipping reference due to missing title: {ref.get('id', 'Unknown ID')}")
        return None # Return None on failure

    all_results = {} # Store unique results by ID
    first_success_step = None
    
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

        # Accept the result only if the best score is high enough.
        if matched_results and matched_results[0]['author_match_score'] > 0.85:
            best_match = matched_results[0]
            
            # Add abstract if available from OpenAlex
            if best_match['result'].get('abstract_inverted_index'):
                best_match['result']['abstract'] = convert_inverted_index_to_text(best_match['result']['abstract_inverted_index'])
            
            return {
                "original_reference": ref,
                "best_match_result": best_match['result'],
                "author_match_score": best_match['author_match_score'],
                "search_steps_tried": list(range(1,10)),
                "first_successful_step": first_success_step,
                "all_potential_matches_count": len(unique_results)
            }

    # If no high-confidence match is found or no results at all, return None.
    return None

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
