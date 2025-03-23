import requests
from bs4 import BeautifulSoup
import os
import time
from urllib.parse import urljoin, urlparse, parse_qs

# Ask the user for the main URL to scrape
main_url = input("Please enter the main URL to scrape: ")

# Extract the base URL from the main URL
parsed_url = urlparse(main_url)
base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

print(f"Base URL: {base_url}")
print(f"Main URL: {main_url}")

# Create a folder to store the downloaded PDFs
download_folder = "pdf_downloads"
os.makedirs(download_folder, exist_ok=True)

# Sets to track visited subpages and downloaded PDFs
visited_subpages = set()
downloaded_pdfs = set()

def make_request(url):
    """Make a request with retry logic for 429 errors and a delay after success."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            time.sleep(0.5)  # Delay after successful request
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"429 error for {url}, retrying in 10 seconds...")
                time.sleep(10)
            else:
                raise
    else:
        print(f"Failed to fetch {url} after {max_retries} attempts")
        raise

def get_soup(url):
    """Fetch the webpage and return a BeautifulSoup object."""
    response = make_request(url)
    return BeautifulSoup(response.text, 'html.parser')

def is_subpage(link, main_url):
    """Check if a link is a subpage on the same domain."""
    full_link = urljoin(main_url, link)
    return full_link.startswith(base_url)

def extract_pdf_links(soup):
    """Extract all PDF links from a BeautifulSoup object."""
    pdf_links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if href.lower().endswith('.pdf'):
            pdf_links.append(href)
    return pdf_links

def download_pdf(pdf_url, folder):
    """Download a PDF file if not already downloaded and save it to the specified folder."""
    full_pdf_url = urljoin(base_url, pdf_url)
    if full_pdf_url in downloaded_pdfs:
        print(f"Already downloaded: {full_pdf_url}")
        return
    
    response = make_request(full_pdf_url)
    
    # Determine the filename
    parsed_url = urlparse(full_pdf_url)
    query_params = parse_qs(parsed_url.query)
    if 'filename' in query_params:
        filename = query_params['filename'][0]
    else:
        filename = os.path.basename(parsed_url.path)
    
    filename = os.path.join(folder, filename)
    with open(filename, 'wb') as f:
        f.write(response.content)
    print(f"Downloaded: {filename}")
    downloaded_pdfs.add(full_pdf_url)

def main():
    """Main function to orchestrate the PDF downloading process."""
    print(f"Fetching main page: {main_url}")
    main_soup = get_soup(main_url)
    visited_subpages.add(main_url)  # Mark main page as visited
    
    # Extract all links from the main page
    all_links = [a['href'] for a in main_soup.find_all('a', href=True)]
    
    # Identify unique subpages
    subpages = set()
    for link in all_links:
        if is_subpage(link, main_url):
            full_link = urljoin(main_url, link)
            if full_link != main_url and full_link not in visited_subpages:
                subpages.add(full_link)
    
    # Download PDFs from the main page
    pdf_links_main = extract_pdf_links(main_soup)
    for pdf in pdf_links_main:
        download_pdf(pdf, download_folder)
    
    # Process each unique subpage and download its PDFs
    for subpage in subpages:
        try:
            print(f"Fetching subpage: {subpage}")
            subpage_soup = get_soup(subpage)
            visited_subpages.add(subpage)
            pdf_links_sub = extract_pdf_links(subpage_soup)
            for pdf in pdf_links_sub:
                download_pdf(pdf, download_folder)
        except Exception as e:
            print(f"Could not access subpage: {subpage} - {e}")

if __name__ == "__main__":
    main()
