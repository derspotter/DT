import requests
import hashlib
from pathlib import Path
from .utils import ServiceRateLimiter

# ANSI escape codes for colors
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

class PDFDownloader:
    """Manages the downloading of PDF files from various sources."""

    def __init__(self, download_dir: str | Path, rate_limiter: ServiceRateLimiter, mailto: str = "your.email@example.com"):
        """
        Initializes the downloader.
        Args:
            download_dir: The base directory where PDFs will be saved.
            rate_limiter: A configured ServiceRateLimiter instance.
            mailto: An email address for API politeness headers.
        """
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limiter = rate_limiter
        self.headers = {
            'User-Agent': f'dl-lit/0.1 (mailto:{mailto})'
        }

    def _calculate_checksum(self, content: bytes) -> str:
        """Calculates the SHA256 checksum of the file content."""
        return hashlib.sha256(content).hexdigest()

    def _download_and_save(self, pdf_url: str, destination_filename: str, source: str) -> tuple[Path | None, str | None, str | None]:
        """
        Generic internal method to download a PDF from a URL and save it.
        """
        if not pdf_url:
            return None, None, f"No PDF URL provided by {source}."

        try:
            print(f"[Downloader] Attempting to download from {source}: {pdf_url}")
            # Use a generic rate limit for downloads, as we don't know the host in advance
            self.rate_limiter.wait_if_needed('default') 
            response = requests.get(pdf_url, headers=self.headers, timeout=60, allow_redirects=True)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', '')
            if 'application/pdf' not in content_type:
                return None, None, f"URL from {source} did not point to a PDF. Content-Type: {content_type}"

            pdf_content = response.content
            if not pdf_content:
                 return None, None, f"Downloaded empty file from {source}."

            checksum = self._calculate_checksum(pdf_content)
            
            safe_filename = "".join(c for c in destination_filename if c.isalnum() or c in ('.', '_', '-')).rstrip()
            destination_path = self.download_dir / safe_filename
            
            with open(destination_path, 'wb') as f:
                f.write(pdf_content)
            
            print(f"{GREEN}[Downloader] Successfully downloaded from {source} and saved to: {destination_path}{RESET}")
            return destination_path.resolve(), checksum, None

        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed from {source}: {e}"
            print(f"{RED}[Downloader] {error_msg}{RESET}")
            return None, None, error_msg

    def attempt_download(self, metadata: dict) -> tuple[Path | None, str | None, str | None]:
        """
        Tries to download a PDF using various sources based on the provided metadata.
        The metadata should be the enriched data from OpenAlexScraper.
        
        Returns:
            A tuple of (path_to_file, checksum, source_of_download).
            Returns (None, None, None) if all attempts fail.
        """
        doi = metadata.get('doi')
        if not doi:
             # Create a fallback filename if no DOI is present
            openalex_id = metadata.get('id', '').split('/')[-1]
            title_slug = metadata.get('display_name', 'untitled')[:20].replace(' ', '_')
            filename = f"{openalex_id or title_slug}.pdf"
        else:
            filename = f"{doi.replace('/', '_')}.pdf"

        # Strategy 1: Try Unpaywall's best OA location
        unpaywall_meta = metadata.get('unpaywall')
        if unpaywall_meta and unpaywall_meta.get('best_oa_location'):
            pdf_url = unpaywall_meta['best_oa_location'].get('url_for_pdf')
            if pdf_url:
                path, checksum, err = self._download_and_save(pdf_url, filename, "Unpaywall")
                if path:
                    return path, checksum, "Unpaywall"
                else:
                    print(f"{YELLOW}[Downloader] Unpaywall download failed: {err}{RESET}")

        # Strategy 2: Try OpenAlex's open_access.oa_url
        oa_url = metadata.get('open_access', {}).get('oa_url')
        if oa_url:
            path, checksum, err = self._download_and_save(oa_url, filename, "OpenAlex OA URL")
            if path:
                return path, checksum, "OpenAlex OA URL"
            else:
                print(f"{YELLOW}[Downloader] OpenAlex OA URL download failed: {err}{RESET}")

        # Strategy 3: Try OpenAlex primary_location's pdf_url
        pdf_url = metadata.get('primary_location', {}).get('pdf_url')
        if pdf_url:
            path, checksum, err = self._download_and_save(pdf_url, filename, "OpenAlex PDF URL")
            if path:
                return path, checksum, "OpenAlex PDF URL"
            else:
                print(f"{YELLOW}[Downloader] OpenAlex PDF URL download failed: {err}{RESET}")

        print(f"{RED}[Downloader] All download attempts failed for DOI: {doi}{RESET}")
        return None, None, None
