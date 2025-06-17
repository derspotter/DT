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
    result_first, result_last = normalize_name(result_author)

    if not ref_first or not ref_last or not result_first or not result_last:
        return 0.0

    # Last name must match at a high similarity level (fuzzy match with a high threshold)
    last_name_score = fuzz.ratio(ref_last, result_last)
    if last_name_score < 90:
        return 0.0

    # Handle initials and full name comparison for first names
    ref_initials = ''.join([name[0] for name in ref_first.split() if name])
    result_initials = ''.join([name[0] for name in result_first.split() if name])

    # If the reference first name only has one part, only compare initials
    if len(ref_first.split()) == 1:
        if ref_initials and result_initials and ref_initials[0] == result_initials[0]: # check if initials are not empty
            return 1.0
    else:
        # If both names have more than one part, compare the initials fully
        if ref_initials == result_initials:
            return 1.0

    # Use fuzzy matching to see if the first names are similar enough
    first_name_score = fuzz.partial_ratio(ref_first, result_first)
    if first_name_score >= 70:
        return 1.0

    return 0.0

def find_best_author_match(ref_authors, result, ref_editors=None):
    """
    Match authors and editors against result authors.
    ref_editors is the list of editors from the reference, if available.
    """
    result_authors = [
        authorship['author'].get('display_name', '') 
        for authorship in result.get('authorships', [])
        if 'author' in authorship
    ]
    
    if not result_authors:
        return 0.0

    # Create two lists: confirmed authors and potential editor-authors
    confirmed_authors = ref_authors if ref_authors else []
    editor_authors = ref_editors if ref_editors else []
    
    # First try to match confirmed authors in position
    score = 0.0
    for ref_author in confirmed_authors:
        # Try to match this author with any result author
        for result_author in result_authors:
            if match_author_name(ref_author, result_author) == 1.0:
                score += 1
                break
    
    # Then try to match editors in any position
    for editor in editor_authors:
        for result_author in result_authors:
            if match_author_name(editor, result_author) == 1.0:
                score += 1
                break
    
    # Calculate final score based on maximum possible matches
    total_people = len(confirmed_authors) + len(editor_authors)
    if total_people == 0:
        return 0.0
        
    return score / max(total_people, len(result_authors))

def get_authors_and_editors(ref):
    """Get separate lists of authors and editors from reference"""
    authors = []
    editors = []
    
    if isinstance(ref.get('authors'), list):
        authors.extend(ref['authors'])
    
    if isinstance(ref.get('editors'), list):
        editors.extend(ref['editors'])
    elif isinstance(ref.get('editor'), str):
        editors.append(ref['editor'])
    
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
        print(f"Skipping reference due to missing title: {ref.get('id', 'Unknown ID')}")
        return ref # Return original if no title

    all_results = {} # Store unique results by ID
    first_success_step = None
    
    # Try all steps 1-9 in order
    print("\nTrying all search steps:")
    for step in range(1, 10):
        # Pass abstract and keywords to search
        results = searcher.search(title, year, container_title, abstract, keywords, step, rate_limiter=rate_limiter)
        
        if results['success']:
            print(f"Step {step}: Found {len(results['results'])} results")
            if first_success_step is None:
                first_success_step = step
            
            # Add new results to our collection
            for result in results['results']:
                result_id = result.get('id')
                if result_id and result_id not in all_results:
                    all_results[result_id] = result
                    all_results[result_id]['first_found_in_step'] = step
        else:
            error_msg = results.get('error', 'Unknown error')
            if error_msg == 'Unknown error' and results.get('meta', {}).get('count', 0) == 0:
                print(f"Step {step}: No results found for this query")
            else:
                print(f"Step {step}: Failed - {error_msg}")
                print(f"[DEBUG] Detailed error info for step {step}: {results}")
    
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

        accept_result = matched_results[0]['author_match_score'] > 0.0
        
        if accept_result:
            best_match = matched_results[0]
            print(f"Best match for '{title[:50]}...': {best_match['result'].get('display_name', 'N/A')[:50]}... (Score: {best_match['author_match_score']:.2f}, Step: {best_match['first_found_in_step']})")
            
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
        else:
            print(f"No acceptable match found for '{title[:50]}...' after trying all steps.")
            return {
                "original_reference": ref,
                "best_match_result": None,
                "author_match_score": 0.0,
                "search_steps_tried": list(range(1,10)),
                "first_successful_step": first_success_step,
                "all_potential_matches_count": len(unique_results),
                "note": "No good author match found among OpenAlex/Crossref results."
            }
    else:
        print(f"No results found for '{title[:50]}...' in any step.")
        return {
            "original_reference": ref,
            "best_match_result": None,
            "search_steps_tried": list(range(1,10)),
            "first_successful_step": None,
            "all_potential_matches_count": 0,
            "note": "No results returned from OpenAlex/Crossref search steps."
        }

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
