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
        if ref_initials[0] == result_initials[0]:
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
                    
                url = f'https://api.crossref.org/works?query={"+".join(query)}&rows=10'
                
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
                            ]
                        }
                        results.append(result)
                    
                    return {
                        "step": step,
                        "results": results,
                        "success": len(results) > 0,
                        "query": url,
                        "meta": data.get('message', {}).get('total-results'),
                        "url": url
                    }
                
                return {"step": step, "results": [], "success": False}
                    
            except Exception as e:
                print(f"Error in Crossref search: {e}")
                return {"step": step, "results": [], "success": False, "error": str(e)}
        
        # Step 9: Container title search
        elif step == 9:
            if container:
                search_query = f'{container}'
                params = {
                    'search': search_query,
                    'select': self.fields,
                    'per-page': 10
                }
            else:
                return {"step": step, "results": [], "success": False}
        
        # OpenAlex API calls (for steps 1-7 and 9)
        if step not in [8]:  # All steps except Crossref
            # Add year for steps 1-3
            if step <= 3 and year:
                query = f'{query},publication_year:{year}'
            
            # Set up parameters if not already set (for steps 1-6)
            if step <= 6:
                params = {
                    'filter': query,
                    'select': self.fields,
                    'per-page': 10
                }

            try:
                print(f"\nStep {step} Search")
                if step in [7, 9]:
                    print(f"Search query: {params['search']}")
                else:
                    print(f"Filter Query: {query}")
                
                # Apply rate limiting
                rate_limiter.wait_if_needed()
                
                response = requests.get(self.base_url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                results = data.get('results', [])
                
                return {
                    "step": step,
                    "results": results,
                    "success": len(results) > 0,
                    "query": params.get('filter') or params.get('search'),
                    "meta": data.get('meta', {}),
                    "url": response.url
                }
                
            except Exception as e:
                print(f"Error in step {step}: {e}")
                return {"step": step, "results": [], "success": False, "error": str(e)}
           
def process_single_file(json_file, output_dir, searcher, fetch_citations=True):
    print(f"\n=== Processing {json_file} ===")
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    references = data.get('references', data) if isinstance(data, dict) else data
    results_per_file = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:  # Process one reference at a time
        future_to_ref = {executor.submit(process_single_reference, ref, searcher, RateLimiter(max_per_second=5)): ref for ref in references}
        for future in concurrent.futures.as_completed(future_to_ref):
            ref = future_to_ref[future]
            try:
                processed_ref = future.result()
                if processed_ref:
                    results_per_file.append(processed_ref)
                print(f"[DEBUG] Completed processing for reference: {ref.get('title', 'Untitled')}")
            except Exception as e:
                print(f"[ERROR] Processing failed for {ref.get('title', 'Untitled')}: {e}")
    
    if results_per_file:
        output_file = Path(output_dir) / f"{Path(json_file).stem}_openalex_results.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results_per_file, f, indent=2)
        print(f"\nSaved search results to {output_file}")

def process_single_reference(ref, searcher, rate_limiter):
    title = ref.get('title')
    year = ref.get('year')
    if year:
        try:
            year = int(year)
        except (ValueError, TypeError):
            year = None
    container_title = ref.get('journal') or ref.get('container_title')
    abstract = ref.get('abstract')  # Get abstract
    keywords = ref.get('keywords')  # Get keywords
    
    print(f"\n--- Searching for: {title} ---")
    if year:
        print(f"Year: {year}")
    if container_title:
        print(f"Container: {container_title}")
    
    # Use dictionary to store unique results by ID
    all_results = {}
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
    
    # Assuming further processing of results continues here...
    # Return processed results for this reference
    unique_results = list(all_results.values())
    
    if unique_results:
        ref_authors = get_authors_from_ref(ref)
        
        # Match authors for unique results
        matched_results = [
            {
                'result': result,
                'author_match_score': find_best_author_match(ref_authors, result),
                'first_found_in_step': all_results[result['id']]['first_found_in_step']
            }
            for result in unique_results
        ]

        # Sort by author match score and then by step number (earlier steps preferred)
        matched_results.sort(key=lambda x: (-x['author_match_score'], x['first_found_in_step']))

        # Accept the result if there's a good author match
        accept_result = matched_results[0]['author_match_score'] > 0.0
        
        if accept_result:
            best_match = matched_results[0]
            result = best_match['result']
            
            print("\nBest match:")
            print(f"Title: {result.get('display_name')}")
            print(f"Authors: {', '.join(a['author'].get('display_name', '') for a in result.get('authorships', []))}")
            print(f"Year: {result.get('publication_year')}")
            print(f"Match score: {best_match['author_match_score']:.2f}")
            print(f"First found in step: {best_match['first_found_in_step']}")
            print("[DEBUG] All keys in result dictionary:", list(result.keys()))
            
            # Extract cited_by_api_url
            cited_by_url = result.get('cited_by_api_url')
            if 'cited_by_api_url' not in result:
                print("[DEBUG] cited_by_api_url not found in result. Checking for similar keys.")
                for key in result.keys():
                    if 'cited' in key.lower() or 'cite' in key.lower():
                        print(f"[DEBUG] Found potential citation key: {key} = {result.get(key)}")
            ref['cited_by_api_url'] = cited_by_url  # Keep the URL for reference if needed later

            if cited_by_url:
                print(f"Extracted cited_by_api_url: {cited_by_url}")
                # Fetch citing work IDs using the new function
                citing_work_ids = fetch_citing_work_ids(cited_by_url, rate_limiter, mailto="spott@wzb.eu")
                print(f"Added {len(citing_work_ids)} citing work IDs.")
                # Save citing work IDs if we fetched them
                if citing_work_ids:
                    ref['citing_work_ids'] = citing_work_ids
            else:
                ref['cited_by_api_url'] = None
                ref['citing_work_ids'] = []
                print("No cited_by_api_url found in OpenAlex result, no citing work IDs fetched.")

            # Extract and convert abstract
            abstract_inverted_index = result.get('abstract_inverted_index')
            if abstract_inverted_index:
                ref['abstract'] = convert_inverted_index_to_text(abstract_inverted_index)
                print(f"Extracted abstract ({len(ref['abstract'])} characters)")
            else:
                ref['abstract'] = None
                print("No abstract found in OpenAlex result")

            # Extract keywords
            keywords = result.get('keywords')
            if keywords:
                # Keywords are returned as a list of dicts like [{'keyword': '...', 'score': ...}]
                # We want just a list of strings
                ref['keywords'] = [k.get('keyword') for k in keywords if k.get('keyword')]
                print(f"Extracted {len(ref['keywords'])} keywords")
            else:
                ref['keywords'] = []
                print("No keywords found in OpenAlex result")

        else:
            # No acceptable matches
            print("\nNo acceptable matches found:")
            print(f"Best author match score: {matched_results[0]['author_match_score']:.2f}")
            if ref_authors:
                print("Reference authors:", ref_authors)
                print("Best match authors:", [
                    a['author'].get('display_name', '')
                    for a in matched_results[0]['result']['authorships']
                ])
            ref['match_found'] = False
        
        return ref
    else:
        # No results at all
        print("\nNo results found in any step")
        ref['match_found'] = False
        return ref

def process_bibliography_files(bib_dir, output_dir, searcher, fetch_citations=True):
    rate_limiter = RateLimiter(max_per_second=5)
    bib_dir_path = Path(bib_dir)
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Output directory set to: {output_dir_path}")

    json_files = list(bib_dir_path.glob('*.json'))
    print(f"[INFO] Found {len(json_files)} JSON files in {bib_dir_path}")

    def process_file(json_file):
        print(f"\n=== Processing {json_file.name} ===")
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        references = data.get('references', data) if isinstance(data, dict) else data
        print(f"[INFO] Loaded {len(references)} references from {json_file.name}")

        results = []
        for ref in references:
            result = process_single_reference(ref, searcher, rate_limiter)
            if result:
                results.append(result)
            print("\n" + "="*50)

        output_file = output_dir_path / f"{json_file.stem}_openalex_results.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"\nSaved search results to {output_file}")
        return json_file.name

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_file, json_file) for json_file in json_files]
        for future in concurrent.futures.as_completed(futures):
            print(f"[INFO] Completed processing {future.result()}")

    print(f"[SUCCESS] Processed all files in {bib_dir_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape OpenAlex for references and citations.')
    parser.add_argument('--bib-dir', type=str, help='Directory containing bibliography JSON files')
    parser.add_argument('--input-file', type=str, help='Single bibliography JSON file to process')
    parser.add_argument('--output-dir', type=str, default='openalex_output', help='Directory to save OpenAlex results')
    parser.add_argument('--fetch-citations', action='store_true', help='Fetch citation data for each reference')
    args = parser.parse_args()
    
    if not args.bib_dir and not args.input_file:
        parser.error("Either --bib-dir or --input-file must be specified")
    if args.bib_dir and args.input_file:
        parser.error("Only one of --bib-dir or --input-file can be specified")

    os.makedirs(args.output_dir, exist_ok=True)
    print(f"[INFO] Output directory set to: {args.output_dir}")
    
    searcher = OpenAlexCrossrefSearcher()
    
    if args.input_file:
        process_single_file(args.input_file, args.output_dir, searcher, fetch_citations=args.fetch_citations)
    else:
        process_bibliography_files(args.bib_dir, args.output_dir, searcher, fetch_citations=args.fetch_citations)
