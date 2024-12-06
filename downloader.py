import json
import time
import requests
import os
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import quote_plus

class RateLimiter:
    def __init__(self, requests_per_second=2):
        self.delay = 1.0 / requests_per_second
        self.last_request = 0
        
    def wait(self):
        elapsed = time.time() - self.last_request
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request = time.time()

class BibliographyEnhancer:
    def __init__(self, email):
        self.email = email
        self.crossref_headers = {'mailto': email}
        self.unpaywall_headers = {'email': email}
        self.rate_limiter = RateLimiter()
        self.script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        
    def search_crossref(self, entry):
        self.rate_limiter.wait()
        
        query = []
        if entry.get('title'):
            query.append(f'title:{quote_plus(entry["title"])}')
        if entry.get('authors'):
            authors = '+'.join(quote_plus(author) for author in entry['authors'])
            query.append(f'author:{authors}')
        if entry.get('year'):
            query.append(f'published:{entry["year"]}')
            
        if not query:
            return None
            
        url = f'https://api.crossref.org/works?query={"+".join(query)}&rows=1'
        
        try:
            response = requests.get(url, headers=self.crossref_headers)
            response.raise_for_status()
            data = response.json()
            
            if data['message']['items']:
                return data['message']['items'][0]
        except Exception as e:
            print(f"Crossref error for {entry.get('title')}: {e}")
        
        return None

    def check_unpaywall(self, doi):
        if not doi:
            return None
            
        self.rate_limiter.wait()
        url = f'https://api.unpaywall.org/v2/{doi}?email={self.email}'
        
        try:
            response = requests.get(url)
            if response.status_code == 422:
                print(f"Invalid DOI format for Unpaywall: {doi}")
                return None
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Unpaywall error for {doi}: {e}")
            return None

    def download_paper(self, url, filename):
        if not url:
            return False
                
        print(f"  Loading: {url}")
        
        # Try direct download first
        try:
            response = requests.get(url, allow_redirects=True, headers={
                'User-Agent': 'Mozilla/5.0',
                'Accept': 'application/pdf,*/*'
            })
            
            content_type = response.headers.get('Content-Type', '').lower()
            if response.status_code == 200 and ('pdf' in content_type or url.lower().endswith('.pdf')):
                with open(filename, 'wb') as f:
                    f.write(response.content)
                print("  ✓ Direct download successful")
                return True
                
            # Try browser if direct download fails
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            driver = webdriver.Chrome(options=options)
            
            try:
                driver.get(url)
                time.sleep(3)  # Wait for redirects
                final_url = driver.current_url
                print(f"  Redirected to: {final_url}")
                
                # Try downloading from final URL
                response = requests.get(final_url, headers={
                    'User-Agent': 'Mozilla/5.0',
                    'Accept': 'application/pdf,*/*'
                })
                if response.status_code == 200:
                    with open(filename, 'wb') as f:
                        f.write(response.content)
                    return True
                    
                return False
            finally:
                driver.quit()
                
        except Exception as e:
            print(f"  Error: {str(e)}")
            return False




    def enhance_bibliography(self, json_file):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        references = data['references'] if isinstance(data, dict) and 'references' in data else data
        downloads_dir = self.script_dir / 'downloads'
        downloads_dir.mkdir(exist_ok=True)
        
        for ref in references:
            print(f"\nProcessing: {ref.get('title')}")
            print(f"Authors: {', '.join(ref.get('authors', []))}")
            print(f"Year: {ref.get('year')}")
            
            urls = []
            # Process DOI URL first if it exists
            if ref.get('crossref_doi'):
                doi_url = f"https://doi.org/{ref['crossref_doi']}"
                urls.append(('DOI', doi_url))
            
            # Process Unpaywall URLs
            crossref_result = self.search_crossref(ref)
            if crossref_result:
                ref['crossref_doi'] = crossref_result.get('DOI')
                unpaywall_result = self.check_unpaywall(ref['crossref_doi'])
                if unpaywall_result and unpaywall_result.get('oa_locations'):
                    for location in unpaywall_result['oa_locations']:
                        pdf_url = location.get('url_for_pdf') or location.get('url')
                        if pdf_url:
                            urls.append(('Unpaywall', pdf_url))
            
            # Process OpenAlex URL
            if ref.get('open_access_url'):
                urls.append(('OpenAlex', ref['open_access_url']))
            
            if urls:
                print(f"Found URLs to try: {urls}")
                for source, url in urls:
                    safe_title = "".join(c for c in ref.get('title', '')[:50] if c.isalnum() or c in ' -_')
                    filename = downloads_dir / f"{ref.get('year', 'unknown')}_{safe_title}.pdf"
                    print(f"\nTrying {source}: {url}")
                    if self.download_paper(url, filename):
                        ref['downloaded_file'] = str(filename)
                        ref['download_source'] = source
                        print(f"✓ Downloaded from {source}")
                        break
                    print(f"✗ Failed to download from {source}")
            else:
                print("No URLs found from any source")
        
        output_file = self.script_dir / f"enhanced_{json_file.name}"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        return output_file

def main():
    enhancer = BibliographyEnhancer(email="spott@wzb.eu")
    search_results_dir = Path('search_results')
    
    for json_file in search_results_dir.glob('*.json'):
        print(f"\nEnhancing {json_file.name}")
        enhanced_file = enhancer.enhance_bibliography(json_file)
        print(f"Saved enhanced bibliography to {enhanced_file}")

if __name__ == "__main__":
    main()