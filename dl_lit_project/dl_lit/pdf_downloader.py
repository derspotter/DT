import requests
import hashlib
from pathlib import Path

# ANSI escape codes for colors
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

class PDFDownloader:
    """Manages the downloading of PDF files."""

    def __init__(self, download_dir: str | Path, mailto: str = "your.email@example.com"):
        """
        Initializes the downloader.

        Args:
            download_dir: The base directory where PDFs will be saved.
            mailto: An email address for API politeness headers.
        """
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.headers = {
            'User-Agent': f'dl-lit/0.1 (mailto:{mailto})'
        }

    def _calculate_checksum(self, content: bytes) -> str:
        """Calculates the SHA256 checksum of the file content."""
        return hashlib.sha256(content).hexdigest()

    def download_pdf(self, pdf_url: str, destination_filename: str) -> tuple[Path | None, str | None, str | None]:
        """
        Downloads a PDF from a URL and saves it.

        Args:
            pdf_url: The URL of the PDF to download.
            destination_filename: The desired filename for the saved PDF.

        Returns:
            A tuple containing (path_to_file, checksum, error_message).
            If successful, path_to_file is the Path object, checksum is the SHA256 hash,
            and error_message is None.
            If failed, path_to_file and checksum are None, and error_message contains the reason.
        """
        if not pdf_url:
            return None, None, "No PDF URL provided."

        try:
            print(f"[Downloader] Attempting to download from: {pdf_url}")
            response = requests.get(pdf_url, headers=self.headers, timeout=30, allow_redirects=True)
            response.raise_for_status()

            # Verify content type
            content_type = response.headers.get('Content-Type', '')
            if 'application/pdf' not in content_type:
                return None, None, f"URL did not point to a PDF. Content-Type: {content_type}"

            pdf_content = response.content
            checksum = self._calculate_checksum(pdf_content)
            
            # Ensure filename is safe and create the destination path
            safe_filename = "".join(c for c in destination_filename if c.isalnum() or c in ('.', '_', '-')).rstrip()
            destination_path = self.download_dir / safe_filename
            
            with open(destination_path, 'wb') as f:
                f.write(pdf_content)
            
            print(f"{GREEN}[Downloader] Successfully downloaded and saved to: {destination_path}{RESET}")
            return destination_path.resolve(), checksum, None

        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {e}"
            print(f"{RED}[Downloader] {error_msg}{RESET}")
            return None, None, error_msg
