import json
import requests
import time
from pathlib import Path
from collections import deque
from datetime import datetime, timedelta
from rapidfuzz import fuzz

class RateLimiter:
    def __init__(self, max_per_second=10):
        self.max_per_second = max_per_second
        self.request_times = deque()
    
    def wait_if_needed(self):
        now = datetime.now()
        
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

class OpenAlexSearcher:
    def __init__(self, email="spott@wzb.eu"):
        self.base_url = "https://api.openalex.org/works"
        self.headers = {'User-Agent': f'mailto:{email}'}
        self.fields = 'id,doi,display_name,authorships,open_access,title,publication_year,type,referenced_works'
        self.rate_limiter = RateLimiter(max_per_second=10)

    def search(self, title, year, container_title, step):
        """
        Search OpenAlex with different strategies based on step number.
        Steps 1-6 use filter queries.
        Steps 7-8 use the 'search' parameter for title and container_title separately.
        """
        if not title and not container_title:
            return {"step": step, "results": [], "success": False}

        # Clean up the terms
        title = clean_search_term(title)
        container = clean_search_term(container_title)

        # Steps 1-6: use filter parameter
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
        
        # Steps 7-8: use search parameter separately for title and container
        if step in [7, 8]:
            if step == 7 and title:
                search_query = f'{title}'
            elif step == 8 and container:
                search_query = f'{container}'
            else:
                return {"step": step, "results": [], "success": False}
            
            params = {
                'search': search_query,
                'select': self.fields,
                'per-page': 100
            }
        else:
            # Add year for steps 1-3
            if year and step <= 3:
                query = f'{query},publication_year:{year}'
            
            params = {
                'filter': query,
                'select': self.fields,
                'per-page': 100
            }

        try:
            print(f"\nStep {step} Search")
            if step in [7, 8]:
                print(f"Search query: {params['search']}")
            else:
                print(f"Filter Query: {query}")
            
            # Apply rate limiting
            self.rate_limiter.wait_if_needed()
            
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
        
def process_bibliography_files():
    searcher = OpenAlexSearcher()
    bib_dir = Path('bibliographies')
    output_dir = Path('search_results')
    output_dir.mkdir(exist_ok=True)
    
    for json_file in bib_dir.glob('*_bibliography.json'):
        print(f"\n=== Processing {json_file.name} ===")
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        references = data.get('references', data) if isinstance(data, dict) else data
        
        for ref in references:
            title = ref.get('title')
            year = ref.get('year')
            if year:
                try:
                    year = int(year)
                except (ValueError, TypeError):
                    year = None
            container_title = ref.get('journal') or ref.get('container_title')
            
            print(f"\n--- Searching for: {title} ---")
            if year:
                print(f"Year: {year}")
            if container_title:
                print(f"Container: {container_title}")
                
            ref['search_results'] = []
            all_results = []
            first_success_step = None
            
            # Try steps 1-5
            print("\nTrying steps 1-5:")
            for step in range(1, 6):
                results = searcher.search(title, year, container_title, step)
                ref['search_results'].append(results)
                
                if results['success']:
                    print(f"Step {step}: Found {len(results['results'])} results")
                    if first_success_step is None:
                        first_success_step = step
                        if step == 1:
                            print("Step 1 successful - using only these results")
                            all_results = results['results']
                            break
                        else:
                            print(f"First success at step {step} - will continue through step 5")
                    if step > 1:
                        all_results = results['results']
                else:
                    print(f"Step {step}: No results")
            
            # Try step 6 if less than 100 results
            if len(all_results) < 100:
                print("\nTrying step 6 (less than 100 total results):")
                results = searcher.search(title, year, container_title, 6)
                ref['search_results'].append(results)
                if results['success']:
                    print(f"Step 6: Found {len(results['results'])} results")
                    all_results.extend(results['results'])
                else:
                    print("Step 6: No results")
            
            # If still no results, try steps 7 and 8
            if not all_results:
                # Try step 7 (title search)
                if title:
                    print("\nTrying step 7 (title search):")
                    results = searcher.search(title, year, container_title, 7)
                    ref['search_results'].append(results)
                    if results['success']:
                        print(f"Step 7: Found {len(results['results'])} results")
                        all_results = results['results']
                    else:
                        print("Step 7: No results")
                
                # Try step 8 (container title search) if still no results
                if container_title:
                    print("\nTrying step 8 (container title search):")
                    results = searcher.search(title, year, container_title, 8)
                    ref['search_results'].append(results)
                    if results['success']:
                        print(f"Step 8: Found {len(results['results'])} results")
                        all_results.extend(results['results'])
                    else:
                        print("Step 8: No results")
            
            print(f"\nTotal results collected: {len(all_results)}")
            
            # Do author matching if we have results
            if all_results:
                ref_authors = get_authors_from_ref(ref)
    
                # Always use find_best_author_match
                matched_results = [
                    {
                        'result': result,
                        'author_match_score': find_best_author_match(ref_authors, result)
                    }
                    for result in all_results
                ]

                # Sort by author match score
                matched_results.sort(key=lambda x: x['author_match_score'], reverse=True)

                # Accept the result if:
                # 1. Either there's a good author match (score > 0.0)
                # 2. Or if there's exactly one result after step 6
                accept_result = (
                    (matched_results[0]['author_match_score'] > 0.0) or 
                    (first_success_step is not None and first_success_step <= 3)
                )

                if accept_result:
                    best_match = matched_results[0]
                    result = best_match['result']

                    # Print best match details
                    print("\nBest match:")
                    print(f"Title: {result.get('display_name')}")
                    print(f"Authors: {', '.join(a['author'].get('display_name', '') for a in result.get('authorships', []))}")
                    print(f"Year: {result.get('publication_year')}")
                    print(f"Match score: {best_match['author_match_score']:.2f}")
                    if len(all_results) == 1 and ref['search_results'][-1]['step'] >= 6:
                        print("Accepted due to single result after step 6")

                    # Assign ref fields
                    ref['match_found'] = True
                    ref['doi'] = result.get('doi')
                    ref['is_open_access'] = result.get('open_access', {}).get('is_oa', False)
                    ref['open_access_url'] = result.get('open_access', {}).get('oa_url')
                    ref['author_match_score'] = best_match.get('author_match_score')
                    ref['openalex_id'] = result.get('id')
                    ref['matched_title'] = result.get('display_name')
                    ref['matched_authors'] = [
                        a.get('author', {}).get('display_name') 
                        for a in result.get('authorships', [])
                    ]

                    # Determine match_step
                    if first_success_step is not None:
                        ref['match_step'] = first_success_step
                    else:
                        for r in ref['search_results']:
                            if r['success']:
                                ref['match_step'] = r['step']
                                break
                else:
                    # No acceptable matches
                    print("\nNo acceptable matches found:")
                    print(f"Number of results: {len(all_results)}")
                    print(f"Best author match score: {matched_results[0]['author_match_score']:.2f}")
                    if ref_authors:
                        print("Reference authors:", ref_authors)
                        if matched_results and matched_results[0]['result'].get('authorships'):
                            print("Best match authors:", [
                                a['author'].get('display_name', '')
                                for a in matched_results[0]['result']['authorships']
                            ])
                    ref['match_found'] = False
                    ##input("\nPress Enter to continue...")

            else:
                # No results at all
                print("\nNo results found in any step")
                ref['match_found'] = False
                ##input("\nPress Enter to continue...")
                ##continue

            # Remove the detailed search results
            ref.pop('search_results', None)
            print("\n" + "="*50)

        # Save results
        output_file = output_dir / f"search_results_{json_file.name}"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"\nSaved search results to {output_file}")

if __name__ == "__main__":
    process_bibliography_files()